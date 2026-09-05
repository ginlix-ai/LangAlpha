"""Restore a workspace's manifest into a sandbox that lost its files.

Blob-backed rows are pulled by the sandbox itself from presigned URLs when
transfer is ``direct``; everything else is uploaded from this process.
"""

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.database.workspace_file import (
    WorkspaceSyncBusy,
    datetime_to_micros,
    get_files_for_workspace,
    workspace_sync_lock,
)
from src.server.database.workspace import (
    ANY_SANDBOX,
    files_restore_incomplete,
    set_files_restore_incomplete,
    workspace_owner,
)
from src.server.database.blob_keys import blob_key
from src.server.database.workspace_file_blobs import fetch_blob
from src.server.services.persistence._rows import (
    _has_inline_bytes,
    _mode_int,
    _transfer_mode,
)
from src.server.services.persistence.resolve import (
    FileBytesUnavailable,
    resolve_file_bytes,
)
from src.server.services.persistence.transfer import (
    all_unreachable,
    pull_direct,
    transfer_timeout_s,
)
from src.utils.storage import get_signed_url

# Relayed restore uploads in flight. Higher than RELAY_CONCURRENCY because an
# upload costs this process nothing but a socket, where a relayed backup also
# holds the file's bytes in memory while it hashes and stores them.
RESTORE_UPLOAD_CONCURRENCY = 16

logger = logging.getLogger(__name__)


# Two separate facts, deliberately kept in separate places because they have
# different lifetimes and different readers:
#
#   .file_sync_marker (sandbox filesystem)
#       "this sandbox has been populated". Dies with the sandbox, which is
#       exactly what makes it the right signal for `maybe_restore` — its
#       absence is how a recreated sandbox announces itself.
#
#   workspaces.files_restore_incomplete_at (Postgres)
#       "a restore left files unrecovered". Gates pruning in `sync_to_db`.
#
# The second is not a fact about the sandbox, so it does not belong in one. A
# restore fails per file for reasons the sandbox is not party to — most often a
# blob that object storage would not hand back — and in that case the sandbox is
# healthy and would happily store a marker saying otherwise. Postgres is where
# this project keeps cross-worker truth, the next sync may run on a worker that
# never saw the failed restore, and keeping the flag beside the manifest means
# the flag and the rows it protects fail together rather than independently.
def _sync_marker_path(work_dir: str) -> str:
    """Return the sync marker file path for the given working directory."""
    return f"{work_dir}/.file_sync_marker"


_FLAG_CLEAR_ATTEMPTS = 3
_FLAG_CLEAR_BACKOFF_S = 0.2


class RestoreGuardUnavailable(Exception):
    """The completeness flag could not be raised, so no restore was attempted."""


class RestoreIdentityLost(RestoreGuardUnavailable):
    """The row no longer names the sandbox this restore expected to replace:
    another provisioner bound first, so no restore was attempted."""


def _identity_of(sandbox: Any) -> Any:
    return getattr(sandbox, "sandbox_id", None) or ANY_SANDBOX


async def _clear_restore_flag(workspace_id: str, sandbox: Any, *, conn=None) -> None:
    """Clear the flag for this sandbox only: a restore into a provisional
    sandbox that then loses the identity race must not vouch for the winner.
    Before the bind the row names a different sandbox and the clear is a
    no-op; the post-bind reconcile repeats it once the row names this one."""
    await set_files_restore_incomplete(
        workspace_id, False, conn=conn, sandbox_id=_identity_of(sandbox)
    )


async def restore_to_sandbox(
    workspace_id: str, sandbox: Any, *, expected_sandbox_id: Any = ANY_SANDBOX
) -> dict[str, Any]:
    """
    Restore workspace files from the manifest into the sandbox.

    Blob-backed files are fetched by the sandbox itself from presigned
    URLs when transfer is ``direct``; rows that still carry inline bytes
    (or every file under ``relay``) are uploaded from here. Directories
    and symlinks always go through the sandbox runtime, which needs no
    network for them. Modes and mtimes are set from the manifest, so no
    second pass is needed to learn what the sandbox now has.

    Serialized with ``sync_to_db`` on the workspace's lock: a sync that
    scanned the sandbox while a restore was still filling it, then read
    the completeness flag after the restore cleared it, would prune every
    row its stale scan had not seen arrive.

    Returns:
        Restore result summary
    """
    # Raised before the lock is even requested, cleared only once the whole
    # restore came back clean. Both callers swallow every raise as a warning,
    # a lock wait that times out included, and an unflagged sandbox is then
    # an empty mirror of a full manifest, which the next sync prunes. Raising
    # it after the timeout instead would let it land on top of a restore
    # that another worker completed while this one waited: that worker
    # clears the flag as its last step before releasing the lock, which is
    # after any wait on that lock began, so a write made before the wait
    # is always the older of the two.
    # The raise names the sandbox the row is expected to hold, the same one
    # the caller's identity CAS will expect to replace. A provisioner slow
    # enough to reach this after another has bound and reconciled would
    # otherwise leave a flag standing on the winner's sandbox that nothing
    # clears until the next start, with pruning withheld all the while.
    try:
        landed = await set_files_restore_incomplete(
            workspace_id, True, sandbox_id=expected_sandbox_id
        )
    except Exception as e:
        # Without the guard an empty sandbox is indistinguishable from an
        # emptied workspace, so no restore may begin: the caller has to abort
        # provisioning rather than bind a sandbox the next backup would read
        # as the user having deleted everything.
        raise RestoreGuardUnavailable(workspace_id) from e
    if not landed:
        raise RestoreIdentityLost(workspace_id)
    try:
        async with workspace_sync_lock(workspace_id) as conn:
            return await _restore_locked(workspace_id, sandbox, conn)
    except WorkspaceSyncBusy:
        logger.warning(f"File restore for workspace {workspace_id} timed out waiting for the sync lock")
        raise
    except Exception as e:
        logger.error(f"File restore failed for workspace {workspace_id}: {e}")
        raise


async def _restore_locked(
    workspace_id: str, sandbox: Any, conn: Any
) -> dict[str, Any]:
    result = {"restored": 0, "errors": 0}

    rows = await get_files_for_workspace(
        workspace_id, include_content=True, all_kinds=True, conn=conn
    )

    if not rows:
        # An empty manifest is mirrored completely by any sandbox.
        logger.info(f"No files to restore for workspace {workspace_id}")
        await _clear_restore_flag(workspace_id, sandbox, conn=conn)
        return result

    logger.info(f"Restoring {len(rows)} entries for workspace {workspace_id}")

    # Object keys are scoped to the owner; read once for the whole restore.
    user_id = await workspace_owner(workspace_id, conn=conn)

    mode = _transfer_mode(sandbox)
    structural: list[dict[str, Any]] = []
    direct: list[dict[str, Any]] = []
    packs: dict[str, list[dict[str, Any]]] = {}
    relay: list[dict[str, Any]] = []
    total_bytes = 0

    for row in rows:
        if row.get("kind", "file") != "file":
            structural.append(_pull_item(row, url=None))
            continue
        pointer = row.get("pack_sha256") or row.get("blob_sha256")
        if mode == "direct" and not _has_inline_bytes(row) and pointer:
            total_bytes += int(row.get("file_size") or 0)
            if row.get("pack_sha256"):
                packs.setdefault(row["pack_sha256"], []).append(row)
            direct.append(row)
        else:
            relay.append(row)

    if direct:
        items, unsigned = await _signed_pull_items(
            user_id, direct, packs, transfer_timeout_s(total_bytes) + 60
        )
        relay += unsigned
        direct_paths: set[str] = set()
        for i in items:
            if i.get("kind") == "pack":
                direct_paths.update(m["path"] for m in i["members"])
            else:
                direct_paths.add(i["path"])
        # Directories stay open while a relay pass still has files to
        # place under them; that pass closes them. A store the sandbox
        # turns out not to reach reopens them on the same terms.
        results = await pull_direct(
            sandbox, structural + items, defer_dir_modes=bool(relay)
        )
        direct_results = {p: r for p, r in results.items() if p in direct_paths}
        unreachable_paths = {
            p for p, r in direct_results.items() if r.get("status") == "unreachable"
        }
        if unreachable_paths:
            if all_unreachable(direct_results):
                logger.warning(
                    f"Sandbox for workspace {workspace_id} could not reach "
                    f"object storage; restoring {len(items)} file(s) through "
                    f"the server"
                )
            else:
                logger.warning(
                    f"{len(unreachable_paths)} of {len(items)} direct "
                    f"download(s) for workspace {workspace_id} could not "
                    f"reach object storage; restoring those through the server"
                )
            results = {p: r for p, r in results.items() if p not in unreachable_paths}
        # The store would not sign it, or the sandbox could not reach it:
        # both end in the same place, so they join the relay list together.
        relay += [r for r in direct if r["file_path"] in unreachable_paths]
        _tally_pull(workspace_id, results, result)
    elif structural:
        results = await pull_direct(
            sandbox, structural, defer_dir_modes=bool(relay)
        )
        _tally_pull(workspace_id, results, result)

    if relay:
        dirs = [i for i in structural if i.get("kind") == "dir"]
        await _restore_relay(user_id, workspace_id, sandbox, relay, result, dirs)

    complete = result["errors"] == 0

    # The marker only claims "this sandbox has been populated", so it
    # goes in the sandbox and is withheld on a partial restore to make
    # the next start retry. A sandbox failure here propagates: every
    # file just restored through this same sandbox, so one failing now
    # is a real condition, and swallowing it is what leaves a recreated
    # sandbox looking populated with no attributable reason. False is
    # the only outcome left to check — path validation rejected it.
    if complete:
        marker_written = await sandbox.aupload_file_bytes(
            _sync_marker_path(sandbox.working_dir),
            datetime.now(timezone.utc).isoformat().encode("utf-8"),
        )
        if not marker_written:
            # Costs one redundant restore next start. Safe in the
            # direction that matters: nothing is deleted on its account.
            logger.warning(
                f"Could not write the sync marker for workspace "
                f"{workspace_id}; the next start will restore again"
            )
        # Last, on the lock connection: a worker whose wait on this lock
        # timed out flagged the workspace before it began waiting, and
        # this clear has to be the later write (see restore_to_sandbox).
        await _clear_restore_flag(workspace_id, sandbox, conn=conn)
    else:
        logger.warning(
            f"Restore for workspace {workspace_id} left {result['errors']} "
            f"file(s) unrestored; the workspace stays flagged so the next "
            f"start retries and sync leaves the manifest alone"
        )

    logger.info(
        f"File restore completed for workspace {workspace_id}: "
        f"restored={result['restored']}, errors={result['errors']}"
    )

    return result


async def _signed_pull_items(
    user_id: str,
    direct: list[dict[str, Any]],
    packs: dict[str, list[dict[str, Any]]],
    expires: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Presign one URL per object and per chunk, largest item first.

    Returns the items the sandbox can pull and the rows the store would not
    sign, which the caller relays alongside anything the sandbox then fails
    to reach.
    """
    items: list[dict[str, Any]] = []
    unsigned: list[dict[str, Any]] = []
    for row in direct:
        if row.get("pack_sha256"):
            continue
        url = await asyncio.to_thread(
            get_signed_url, blob_key(user_id, row["blob_sha256"]), expires
        )
        if url is None:
            unsigned.append(row)
            continue
        items.append(_pull_item(row, url=url))
    # One item per chunk; the runtime slices the members out.
    for pack_sha256, members in packs.items():
        url = await asyncio.to_thread(
            get_signed_url, blob_key(user_id, pack_sha256), expires
        )
        if url is None:
            unsigned.extend(members)
            continue
        items.append(
            {
                "kind": "pack",
                "sha256": pack_sha256,
                "size": sum(int(m.get("file_size") or 0) for m in members),
                "url": url,
                "members": [_pack_member_item(m) for m in members],
            }
        )
    # Largest first: a big object that starts last is the tail.
    items.sort(key=lambda i: int(i.get("size") or 0), reverse=True)
    return items, unsigned


def _pull_item(row: dict[str, Any], *, url: str | None) -> dict[str, Any]:
    modified = row.get("sandbox_modified_at")
    micros = datetime_to_micros(modified)
    mtime_ns = micros * 1000 if micros is not None else None
    return {
        "path": row["file_path"],
        "kind": row.get("kind", "file"),
        "sha256": row.get("blob_sha256"),
        "size": int(row.get("file_size") or 0),
        "url": url,
        "mode": _mode_int(row.get("permissions"), row.get("kind", "file")),
        "mtime_ns": mtime_ns,
        "symlink_target": row.get("symlink_target"),
    }


def _pack_member_item(row: dict[str, Any]) -> dict[str, Any]:
    item = _pull_item(row, url=None)
    return {
        "path": item["path"],
        "offset": int(row.get("pack_offset") or 0),
        "size": item["size"],
        "sha256": row.get("content_hash"),
        "mode": item["mode"],
        "mtime_ns": item["mtime_ns"],
    }


def _tally_pull(
    workspace_id: str, results: dict[str, dict[str, Any]], result: dict[str, Any]
) -> None:
    for path, r in results.items():
        if r.get("status") == "ok":
            result["restored"] += 1
        else:
            result["errors"] += 1
            logger.warning(
                f"Failed to restore {path} for workspace {workspace_id}: "
                f"{r.get('status')} http={r.get('http')} {r.get('error')}"
            )


async def _restore_relay(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    rows: list[dict[str, Any]],
    result: dict[str, Any],
    dirs: list[dict[str, Any]] | None = None,
) -> None:
    """Upload file rows from this process and let the runtime place them.

    Nothing is uploaded to its final path. A plain file lands under a
    scan-excluded staging name and a pack travels whole; one pull op then
    verifies each against the manifest, moves it into place, and stamps
    modes and mtimes. An upload cut short (a full disk) therefore fails
    verification instead of becoming the file's next content at the next
    backup. ``dirs`` are the structure pass's directory items, carried again
    so their modes and mtimes are applied after the last file is in.
    """
    sem = asyncio.Semaphore(RESTORE_UPLOAD_CONCURRENCY)

    async def _stage(row: dict) -> tuple[dict, tuple[str, str, int] | None]:
        async with sem:
            try:
                return (row, await _stage_relayed_file(user_id, sandbox, row))
            except Exception as e:
                logger.warning(f"Failed to restore {row['file_path']}: {e}")
                return (row, None)

    # Packed rows travel as whole chunks, one at a time, and are sliced
    # in the sandbox; see _relayed_pack_items.
    packs: dict[str, list[dict[str, Any]]] = {}
    plain: list[dict[str, Any]] = []
    for r in rows:
        if r.get("pack_sha256") and not _has_inline_bytes(r):
            packs.setdefault(r["pack_sha256"], []).append(r)
        else:
            plain.append(r)

    items: list[dict[str, Any]] = []
    for row, staged in await asyncio.gather(*(_stage(r) for r in plain)):
        if staged is None:
            result["errors"] += 1
            continue
        name, digest, n = staged
        if digest != row.get("content_hash"):
            # The row's own digest no longer decides placement, so a row that
            # cannot reproduce it would otherwise pass through unremarked.
            logger.warning(
                f"Row for {row['file_path']} in workspace {workspace_id} "
                f"stores bytes that do not reproduce its own content_hash; "
                f"restoring the bytes the row stores"
            )
        item = _pull_item(row, url=None)
        item.update({"file": name, "sha256": digest, "size": n})
        items.append(item)
    if packs:
        items += await _relayed_pack_items(user_id, workspace_id, sandbox, packs, result)
    if not items and not dirs:
        return

    file_paths: set[str] = set()
    for i in items:
        if i.get("kind") == "pack":
            file_paths.update(m["path"] for m in i["members"])
        else:
            file_paths.add(i["path"])
    try:
        outcomes = await pull_direct(sandbox, items + list(dirs or []))
    except Exception as e:
        logger.warning(
            f"Could not place {len(file_paths)} relayed file(s) for workspace "
            f"{workspace_id}: {e}"
        )
        result["errors"] += len(file_paths)
        return
    _tally_pull(
        workspace_id, {p: r for p, r in outcomes.items() if p in file_paths}, result
    )
    for path, r in outcomes.items():
        if path not in file_paths and r.get("status") != "ok":
            # The directory itself was counted by the structure pass; a
            # failed final mode or mtime is still a restore error, or the
            # next backup records the wrong metadata as the user's.
            result["errors"] += 1
            logger.warning(
                f"Could not stamp {path} for workspace {workspace_id}: "
                f"{r.get('status')} {r.get('error')}"
            )


def _staging_name() -> str:
    """A root-level name the scan skips and the sandbox file API accepts.

    The pack directory would be the natural home, but the file API keeps
    everything under ``_internal`` off limits; the ``.wsfiles-`` prefix is
    excluded from the scan, so a restore that dies here leaves nothing the
    next backup would record as a user's file.
    """
    return f".wsfiles-relay-{uuid.uuid4().hex}"


async def _stage_relayed_file(
    user_id: str, sandbox: Any, file_record: dict
) -> tuple[str, str, int] | None:
    """Upload one row's bytes; returns the staging name, their digest and length.

    Byte resolution happens here rather than in the caller so blob fetches
    run under the restore semaphore: concurrency is bounded for free.

    The digest and length describe the bytes actually sent, not the row's
    ``content_hash`` and ``file_size``. The check they feed asks whether the
    upload arrived whole, which only a measurement of what was sent can
    answer, and a row whose ``file_size`` came from a stat rather than from
    the content can state a length its own bytes disagree with.
    """
    try:
        content = await resolve_file_bytes(file_record, user_id=user_id)
    except FileBytesUnavailable as e:
        logger.warning(f"Cannot restore {file_record['file_path']}: {e}")
        return None
    if content is None:
        return None
    staged = _staging_name()
    if not await sandbox.aupload_file_bytes(f"{sandbox.working_dir}/{staged}", content):
        return None
    return staged, hashlib.sha256(content).hexdigest(), len(content)


async def _relayed_pack_items(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    packs: dict[str, list[dict[str, Any]]],
    result: dict[str, Any],
) -> list[dict[str, Any]]:
    """Relay each chunk whole; returns the pull items that slice them in place.

    Uploading members one by one costs a call per file and loses what
    the runtime's extractor keeps: names the upload API cannot carry
    (a trailing space, a newline), modes and mtimes. One upload per
    chunk keeps the relay path at the direct path's fidelity, and memory
    at one chunk at a time.
    """
    work_dir = sandbox.working_dir
    items: list[dict[str, Any]] = []
    for pack_sha256, members in packs.items():
        rel = f".wsfiles-relay-{pack_sha256}"
        try:
            data = await fetch_blob(user_id, pack_sha256)
            size = len(data)
            ok = await sandbox.aupload_file_bytes(f"{work_dir}/{rel}", data)
            del data
        except Exception as e:
            ok = False
            logger.warning(f"Could not relay pack {pack_sha256} for workspace {workspace_id}: {e}")
        if not ok:
            result["errors"] += len(members)
            continue
        items.append(
            {
                "kind": "pack",
                "file": rel,
                "sha256": pack_sha256,
                "size": size,
                "members": [_pack_member_item(m) for m in members],
            }
        )
    return items


async def _reconcile_flag_beside_marker(workspace_id: str, sandbox: Any) -> None:
    """Clear a flag left standing beside a marker, retrying a failed write.

    This is the last chance on the provisioning path: a warm session is
    never reconciled again, so a failure here withholds pruning until the
    sandbox is next recreated, and that recreation restores files the user
    had deleted. Each attempt checks out its own connection, so a failed
    statement does not poison the next one."""
    for attempt in range(1, _FLAG_CLEAR_ATTEMPTS + 1):
        try:
            if await files_restore_incomplete(workspace_id):
                await _clear_restore_flag(workspace_id, sandbox)
            return
        except Exception as e:
            if attempt == _FLAG_CLEAR_ATTEMPTS:
                # Returning, not raising: the sandbox this runs on is
                # complete and healthy, and the next cold start reconciles
                # again from the same marker. Failing provisioning here
                # would destroy it over a database that is briefly down.
                logger.error(
                    f"Could not clear the completeness flag for workspace "
                    f"{workspace_id} after {attempt} attempts; pruning stays "
                    f"withheld until the next restore: {e}"
                )
                return
            await asyncio.sleep(_FLAG_CLEAR_BACKOFF_S * attempt)


async def maybe_restore(workspace_id: str, sandbox: Any) -> None:
    """
    Restore files from DB if sandbox was recreated (files lost).

    Checks for sync marker file. If absent, files were lost and need restore.
    """
    try:
        work_dir = sandbox.working_dir
        sync_marker = _sync_marker_path(work_dir)
        marker = await sandbox.adownload_file_bytes(sync_marker)
        if marker is not None:
            # The marker is written only by a restore that came back clean
            # and then clears the flag; a flag still standing beside it is
            # a restore that died between those two writes, and left alone
            # it would withhold pruning on every backup from here on.
            await _reconcile_flag_beside_marker(workspace_id, sandbox)
            return

        # Every kind, not just ``kind='file'``: a workspace of directories
        # and symlinks is not an empty one, and reading it as empty writes
        # the marker and clears the flag, after which the next backup prunes
        # the structural rows it never saw restored.
        try:
            files = await get_files_for_workspace(
                workspace_id, include_content=False, all_kinds=True
            )
        except Exception as e:
            # Not knowing the manifest is the same hazard as not raising the
            # flag: the sandbox is empty, nothing marks it as unrestored,
            # and the next backup reads the emptiness as deletions.
            raise RestoreGuardUnavailable(
                f"Could not read the manifest for workspace {workspace_id}: {e}"
            ) from e
        if not files:
            # Nothing to restore, so the sandbox trivially matches the
            # manifest — record it, or every start repeats this check.
            # The flag goes first: it is the half that gates deletion, and
            # an empty manifest has nothing left to protect either way,
            # whereas a sandbox failure on the marker write belongs to the
            # caller and is left to propagate.
            await _clear_restore_flag(workspace_id, sandbox)
            await sandbox.aupload_file_bytes(
                sync_marker,
                datetime.now(timezone.utc).isoformat().encode("utf-8"),
            )
            return

        logger.info(
            f"Sync marker missing for workspace {workspace_id}. "
            f"Restoring {len(files)} files from DB."
        )
        await restore_to_sandbox(
            workspace_id, sandbox, expected_sandbox_id=_identity_of(sandbox)
        )

    except RestoreGuardUnavailable:
        # The caller aborts provisioning on this one; on the lazy-start and
        # reconnect paths this is the only restore, and swallowing it here
        # would publish an empty, unflagged sandbox for the next sync to
        # read as an emptied workspace.
        raise
    except (SandboxGoneError, SandboxTransientError):
        # Let a sandbox condition reach the caller as itself. The marker
        # probe answering with a failure means we never learned whether the
        # files are there, and flattening that into a generic warning here
        # reads downstream as "checked, nothing to do" — which is how a
        # recreated sandbox stays empty with no attributable reason.
        raise
    except Exception as e:
        logger.warning(f"Error in maybe_restore for workspace {workspace_id}: {e}")
