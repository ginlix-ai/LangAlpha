"""Restore a workspace's manifest into a sandbox that lost its files.

Blob-backed rows are pulled by the sandbox itself from presigned URLs when
transfer is ``direct``; everything else is uploaded from this process.
"""

import asyncio
import base64
import hashlib
import logging
import shlex
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.middleware.skills.lock import (
    LOCK_FILENAME,
    is_shared_tier,
    parse_skills_lock,
)
from ptc_agent.core.paths import MOUNTED_AGENT_SUBDIRS, THREAD_DIR_NAME, WorkspaceLayout
from ptc_agent.core.sandbox.assets import delivered_skill_names
from src.server.database.workspace_file import (
    SYNC_LOCK_WAIT,
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
from src.server.database.blob_keys import RELAY_MAX_BYTES, blob_key
from src.server.database.user_skills import (
    SkillSyncLockBusy,
    hold_workspace_skill_sync,
    list_user_skills,
    list_workspace_skill_disables,
)
from src.server.database.workspace_file_blobs import fetch_blob
from src.server.services.persistence._rows import (
    _has_inline_bytes,
    _mode_int,
)
from src.server.services.persistence.resolve import (
    FileBytesUnavailable,
    resolve_file_bytes,
)
from src.server.services.user_skills.reconcile import RECONCILE_TIMEOUT_SECONDS
from src.server.services.persistence.transfer import (
    DEFERRED_LEDGER,
    DEFERRED_MARKER,
    DEFERRED_RESTORE_DIRS,
    SYNC_MARKER_NAME,
    transfer_mode,
    INPROCESS_MAX_INFLIGHT_BYTES,
    PACK_MAX_BYTES,
    ByteBudget,
    all_unreachable,
    is_deferred,
    scratchpad_note_thread,
    pull_direct,
    transfer_timeout_s,
)
from src.utils.storage import get_signed_url

if TYPE_CHECKING:
    from src.server.database.conversation import ThreadPrefixes

# Relayed restore uploads in flight. A restore holds each file's bytes in this
# process exactly as a relayed backup does: _stage_relayed_file resolves the
# whole row before it uploads. So this is a count only, and what those files
# weigh is bounded by INPROCESS_MAX_INFLIGHT_BYTES alongside it.
RESTORE_UPLOAD_CONCURRENCY = 16

# The deferred pass takes the sync lock per batch, so a backup or a strict
# caller waits for one batch rather than for every deferred file.
DEFERRED_BATCH_BYTES = 64 * 1024 * 1024
DEFERRED_BATCH_ROWS = 256
# A batch looks again at its own paths, passed to one ``find`` in the command
# string; a command string past 128 KiB is refused by the kernel, so a batch
# whose quoted paths pass this looks at the whole deferred dirs instead.
_RECHECK_ARGS_MAX = 64 * 1024

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
def _sync_marker_path(layout: WorkspaceLayout) -> str:
    """The marker for one project folder, not for the machine it sits on.

    Several projects share a computer root now. A marker at that root is a
    claim about all of them, so the first restore to finish would answer for
    every sibling, and each of those would skip its own restore and stay
    empty. The claim belongs where the files it describes are.
    """
    return layout.join(SYNC_MARKER_NAME)


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


# Past the longest a reconcile pass can hold the skill lock, so a restore
# waits out any pass that is still running rather than failing behind it.
_SKILL_SYNC_WAIT_S = RECONCILE_TIMEOUT_SECONDS + 30


async def restore_to_sandbox(
    workspace_id: str,
    sandbox: Any,
    *,
    expected_sandbox_id: Any = ANY_SANDBOX,
    layout: WorkspaceLayout,
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
    row its stale scan had not seen arrive. The skill reconcile is held off
    the same way, for the same reason: a pass that read the skill folder
    half-filled would delete the rows whose dirs had not landed yet.

    Returns:
        Restore result summary
    """
    # Raised before the lock is even requested, cleared only once the whole
    # restore came back clean. Both restore paths end every raise in a warning,
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
        # The skill lock first, on a session of its own that the sync lock
        # then joins: the restore waits on a pass without a pool slot and
        # without the sync lock that this workspace's backups queue on.
        async with hold_workspace_skill_sync(
            workspace_id, wait_s=_SKILL_SYNC_WAIT_S
        ) as session:
            async with workspace_sync_lock(workspace_id, conn=session) as conn:
                return await _restore_locked(workspace_id, sandbox, conn, layout)
    except WorkspaceSyncBusy:
        logger.warning(f"File restore for workspace {workspace_id} timed out waiting for the sync lock")
        raise
    except SkillSyncLockBusy:
        logger.warning(
            f"File restore for workspace {workspace_id} timed out waiting for "
            f"a skill reconcile pass"
        )
        raise
    except Exception as e:
        logger.error(f"File restore failed for workspace {workspace_id}: {e}")
        raise


async def _restore_locked(
    workspace_id: str, sandbox: Any, conn: Any, layout: WorkspaceLayout
) -> dict[str, Any]:
    # Evicted results and the rest of the scratchpads come afterwards, in
    # restore_deferred.
    rows = await get_files_for_workspace(
        workspace_id,
        include_content=True,
        all_kinds=True,
        outside=(*DEFERRED_RESTORE_DIRS, *MOUNTED_AGENT_SUBDIRS),
        conn=conn,
    )
    notes = await _kept_notes(workspace_id, conn)
    if notes:
        rows = sorted([*rows, *notes], key=lambda row: row["file_path"])

    if rows:
        # Object keys are scoped to the owner; read once for the whole restore.
        user_id = await workspace_owner(workspace_id, conn=conn)
        rows = await _without_shared_skill_copies(
            workspace_id, rows, user_id, sandbox, conn=conn
        )
        logger.info(f"Restoring {len(rows)} entries for workspace {workspace_id}")
        result = await _transfer_rows(
            workspace_id, sandbox, rows, user_id=user_id, layout=layout
        )
    else:
        # An empty manifest is mirrored completely by any sandbox.
        logger.info(f"No files to restore for workspace {workspace_id}")
        result = {"restored": 0, "errors": 0}

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
            _sync_marker_path(layout),
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


async def _transfer_rows(
    workspace_id: str,
    sandbox: Any,
    rows: list[dict[str, Any]],
    *,
    user_id: str,
    layout: WorkspaceLayout,
    placed: set[str] | None = None,
    made: set[str] | None = None,
) -> dict[str, Any]:
    """Put manifest rows into the sandbox: direct pulls first, relay for the
    rest. ``placed`` collects each path the sandbox reported in place, and
    ``made`` each ``keep_existing`` dir it reported making."""
    result = {"restored": 0, "errors": 0}
    results: dict[str, dict[str, Any]] = {}

    mode = transfer_mode(sandbox)
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
            sandbox,
            structural + items,
            layout=layout,
            defer_dir_modes=bool(relay),
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
        _tally_pull(workspace_id, results, result, placed)
    elif structural:
        results = await pull_direct(
            sandbox, structural, layout=layout, defer_dir_modes=bool(relay)
        )
        _tally_pull(workspace_id, results, result, placed)

    ours = {p for p, r in results.items() if r.get("made")}
    if made is not None:
        made |= ours
    if relay:
        # A deferred dir standing before the op above is the turn's, and
        # keeps its own mode and mtime.
        dirs = [
            dict(i, made=True) if i["keep_existing"] else i
            for i in structural
            if i.get("kind") == "dir" and (not i["keep_existing"] or i["path"] in ours)
        ]
        await _restore_relay(
            user_id, workspace_id, sandbox, relay, result, dirs, layout=layout, placed=placed
        )

    return result


# A skill ledger runs to a few kilobytes, and the filter reads it whole into
# this process. One past this is not read at all.
_LEDGER_MAX_BYTES = 1 << 20


async def _without_shared_skill_copies(
    workspace_id: str,
    rows: list[dict[str, Any]],
    user_id: str,
    sandbox: Any,
    *,
    conn: Any,
) -> list[dict[str, Any]]:
    """Leave out the skills the computer's shared tier serves.

    A backup from when the workspace lived at the computer root carries the
    platform and user-tier skills it was served there. The server delivers
    those, so a restored copy can only be older, and left as a real dir in a
    workspace folder it outranks the shared skill for good. The backup's own
    ledger says which skills those were. A name it says nothing about, or
    all of them when it cannot be read, is shared if the computer's last
    asset sync delivered it and the workspace has not switched it off: an
    agent-installed skill is ledgered as ``local`` the first time a turn
    discovers it, so an unledgered copy under a name the folder is linked to
    is a delivery that lost its entry. With no record of the
    sync, such a copy is restored: a stale copy pinned in the folder costs
    less than a lost one. A name the workspace owns as a row is never left
    out: the skill sync decides its bytes, and its linked entry over a
    missing copy would read to the reconcile as the workspace deleting it.
    """
    names = {n for r in rows if (n := _skill_copy(r))} - {LOCK_FILENAME}
    if not names:
        return rows
    lock_path = f"{WorkspaceLayout.SKILLS_DIR}/{LOCK_FILENAME}"
    lock_row = next(
        (
            r
            for r in rows
            if r["file_path"] == lock_path and r.get("kind", "file") == "file"
        ),
        None,
    )
    entries = (
        await _backup_ledger(workspace_id, lock_row, user_id) if lock_row else {}
    )
    shared = {
        n
        for n in names
        if isinstance(entries.get(n), dict) and is_shared_tier(entries[n])
    }
    unledgered = {n for n in names if not isinstance(entries.get(n), dict)}
    if unledgered:
        delivered = await delivered_skill_names(sandbox)
        if delivered is None:
            logger.warning(
                f"Restore for workspace {workspace_id} cannot read what the "
                f"shared tier was delivered; restoring {len(unledgered)} "
                f"unledgered skill(s) as the workspace's own"
            )
        elif linked := unledgered & delivered:
            shared |= linked - await list_workspace_skill_disables(
                workspace_id, conn=conn
            )
    if shared:
        shared -= {
            r["name"]
            for r in await list_user_skills(
                user_id, workspace_id=workspace_id, conn=conn
            )
        }
    if not shared:
        return rows
    kept = [r for r in rows if _skill_copy(r) not in shared]
    logger.info(
        f"Restore for workspace {workspace_id} leaves out {len(rows) - len(kept)} "
        f"entries under {len(shared)} shared skill(s)"
    )
    return kept


def _skill_copy(row: dict[str, Any]) -> str | None:
    """The skill whose copy ``row`` is part of, if any.

    A link at a skill's own path is the folder's view of the shared tier, not
    a copy of it, and restoring one is harmless.
    """
    prefix = f"{WorkspaceLayout.SKILLS_DIR}/"
    path = row["file_path"]
    if not path.startswith(prefix):
        return None
    name, _, below = path[len(prefix):].partition("/")
    if not below and row.get("kind") == "symlink":
        return None
    return name or None


async def _backup_ledger(
    workspace_id: str, lock_row: dict[str, Any], user_id: str
) -> dict[str, Any]:
    """The backup's skill ledger entries, empty when they cannot be read.

    A ledger too large to be the skill sync's is not read at all, and names
    no skill, like a corrupt or unfetchable one.
    """
    if int(lock_row.get("file_size") or 0) > _LEDGER_MAX_BYTES:
        logger.warning(
            f"Skill ledger for workspace {workspace_id} is "
            f"{lock_row.get('file_size')} bytes; not read"
        )
        return {}
    try:
        raw = await resolve_file_bytes(lock_row, user_id=user_id) or b""
    except FileBytesUnavailable as e:
        logger.warning(f"Could not read the skill ledger for workspace {workspace_id}: {e}")
        return {}
    if len(raw) > _LEDGER_MAX_BYTES:
        # A legacy row can understate its own size.
        return {}
    return parse_skills_lock(raw.decode("utf-8", errors="replace"))


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
                # The whole chunk lands on disk, and the rows still naming it
                # can be a sliver of it once siblings were repacked, so it is
                # weighed at the most a chunk can hold.
                "size": PACK_MAX_BYTES,
                "url": url,
                "members": [_pack_member_item(m) for m in members],
            }
        )
    # Largest first: a big object that starts last is the tail.
    items.sort(key=lambda i: int(i.get("size") or 0), reverse=True)
    return items, unsigned


async def _kept_notes(workspace_id: str, conn: Any) -> list[dict[str, Any]]:
    """The scratchpad notes that stay, by the rule the deferred rows follow
    (``_kept``): a kept thread's, and any under a folder no thread names.

    These come in the first pass, not the deferred one: a resumed turn reads
    its notes by the path its summary names, and a note missing then is
    written afresh, which the deferred pass would keep over the backup's copy.
    The deferred pass leaves them alone too, since one the turn deletes before
    that pass looks would otherwise come back.
    """
    from src.server.database.conversation import get_workspace_thread_prefixes

    notes = [
        row["file_path"]
        for row in await get_files_for_workspace(
            workspace_id, all_kinds=True, under=WorkspaceLayout.SCRATCHPAD_DIR, conn=conn
        )
        if scratchpad_note_thread(row["file_path"]) is not None
    ]
    if not notes:
        return []
    kept = await get_workspace_thread_prefixes(workspace_id, conn=conn)
    paths = [path for path in notes if _kept(path, kept)]
    if not paths:
        return []
    return await get_files_for_workspace(
        workspace_id, include_content=True, all_kinds=True, paths=paths, conn=conn
    )


def _pull_item(row: dict[str, Any], *, url: str | None) -> dict[str, Any]:
    modified = row.get("sandbox_modified_at")
    micros = datetime_to_micros(modified)
    mtime_ns = micros * 1000 if micros is not None else None
    return {
        "path": row["file_path"],
        "kind": row.get("kind", "file"),
        "sha256": row.get("blob_sha256"),
        # Unknown stays unknown: the runtime then charges the item its whole
        # byte budget, as the relay does, and verifies it by digest alone. A
        # zero would admit it free and then reject its real bytes as a mismatch.
        "size": None if row.get("file_size") is None else int(row["file_size"]),
        "url": url,
        "mode": _mode_int(row.get("permissions"), row.get("kind", "file")),
        "mtime_ns": mtime_ns,
        "symlink_target": row.get("symlink_target"),
        "keep_existing": is_deferred(row["file_path"]),
    }


def _pack_member_item(row: dict[str, Any]) -> dict[str, Any]:
    item = _pull_item(row, url=None)
    return {
        "path": item["path"],
        "offset": int(row.get("pack_offset") or 0),
        "size": int(item["size"] or 0),
        "sha256": row.get("content_hash"),
        "mode": item["mode"],
        "mtime_ns": item["mtime_ns"],
        "keep_existing": item["keep_existing"],
    }


def _tally_pull(
    workspace_id: str,
    results: dict[str, dict[str, Any]],
    result: dict[str, Any],
    placed: set[str] | None = None,
) -> None:
    for path, r in results.items():
        if r.get("status") == "ok":
            result["restored"] += 1
            if placed is not None:
                placed.add(path)
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
    *,
    layout: WorkspaceLayout,
    placed: set[str] | None = None,
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
    budget = ByteBudget(INPROCESS_MAX_INFLIGHT_BYTES, RESTORE_UPLOAD_CONCURRENCY)

    async def _stage(row: dict) -> tuple[dict, tuple[str, str, int] | None]:
        size = row.get("file_size")
        if size is not None and int(size) > RELAY_MAX_BYTES:
            # The direct path caps nothing, so a blob this large is normal
            # until the store turns out to be unreachable and the restore
            # lands here instead. Resolving it would pull the whole thing
            # into this process; the budget cannot help, since an item over
            # the whole budget is admitted anyway rather than deadlocking.
            # Reported as an error, so the file is retried when the store is
            # reachable again rather than silently skipped.
            logger.error(
                f"Cannot relay {row['file_path']} for workspace "
                f"{workspace_id}: {size} bytes exceeds the {RELAY_MAX_BYTES} "
                f"byte relay limit, and object storage was unreachable from "
                f"the sandbox. The file is left unrestored"
            )
            return (row, None)
        async with budget.hold(size):
            try:
                return (
                    row,
                    await _stage_relayed_file(user_id, sandbox, row, layout),
                )
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
        items += await _relayed_pack_items(
            user_id, workspace_id, sandbox, packs, result, layout
        )
    if not items and not dirs:
        return

    file_paths: set[str] = set()
    for i in items:
        if i.get("kind") == "pack":
            file_paths.update(m["path"] for m in i["members"])
        else:
            file_paths.add(i["path"])
    try:
        outcomes = await pull_direct(
            sandbox, items + list(dirs or []), layout=layout
        )
    except Exception as e:
        logger.warning(
            f"Could not place {len(file_paths)} relayed file(s) for workspace "
            f"{workspace_id}: {e}"
        )
        result["errors"] += len(file_paths)
        return
    _tally_pull(
        workspace_id, {p: r for p, r in outcomes.items() if p in file_paths}, result, placed
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
    user_id: str, sandbox: Any, file_record: dict, layout: WorkspaceLayout
) -> tuple[str, str, int] | None:
    """Upload one row's bytes; returns the staging name, their digest and length.

    Byte resolution happens here rather than in the caller so blob fetches
    run under the caller's byte budget: what this holds is bounded for free.

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
    # The staging name is excluded at the root of the walk, which is the
    # project folder, so that is also where it has to land.
    if not await sandbox.aupload_file_bytes(layout.join(staged), content):
        return None
    return staged, hashlib.sha256(content).hexdigest(), len(content)


async def _relayed_pack_items(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    packs: dict[str, list[dict[str, Any]]],
    result: dict[str, Any],
    layout: WorkspaceLayout,
) -> list[dict[str, Any]]:
    """Relay each chunk whole; returns the pull items that slice them in place.

    Uploading members one by one costs a call per file and loses what
    the runtime's extractor keeps: names the upload API cannot carry
    (a trailing space, a newline), modes and mtimes. One upload per
    chunk keeps the relay path at the direct path's fidelity, and memory
    at one chunk at a time.
    """
    items: list[dict[str, Any]] = []
    for pack_sha256, members in packs.items():
        rel = f".wsfiles-relay-{pack_sha256}"
        try:
            data = await fetch_blob(user_id, pack_sha256)
            size = len(data)
            ok = await sandbox.aupload_file_bytes(layout.join(rel), data)
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


async def maybe_restore(
    workspace_id: str, sandbox: Any, *, layout: WorkspaceLayout
) -> None:
    """
    Restore files from DB if sandbox was recreated (files lost).

    Checks for sync marker file. If absent, files were lost and need restore.
    Every failure reaches the caller as itself: flattened into a warning here,
    a restore that never ran reads downstream as checked with nothing to do,
    and the caller records the workspace as attached over missing files.
    """
    sync_marker = _sync_marker_path(layout)
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
        workspace_id,
        sandbox,
        expected_sandbox_id=_identity_of(sandbox),
        layout=layout,
    )


async def _restamp_dirs(
    workspace_id: str,
    sandbox: Any,
    items: list[dict[str, Any]],
    *,
    layout: WorkspaceLayout,
) -> None:
    """Stamp the dirs the first batch made, once the last batch has written
    beneath them, without making again any the turn has removed.

    A failure is logged rather than counted: a retried pass finds these dirs
    in place and would not stamp them either.
    """
    try:
        outcomes = await pull_direct(sandbox, items, layout=layout)
    except Exception as e:  # noqa: BLE001 - the files are in place either way
        logger.warning(
            f"Could not stamp {len(items)} deferred dir(s) for workspace "
            f"{workspace_id}: {e}"
        )
        return
    for path, r in outcomes.items():
        if r.get("status") != "ok":
            logger.warning(
                f"Could not stamp {path} for workspace {workspace_id}: "
                f"{r.get('status')} {r.get('error')}"
            )


def _deferred_batches(due: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    dirs = [r for r in due if r.get("kind") == "dir"]
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    size = 0
    for row in due:
        if row.get("kind") == "dir":
            continue
        n = int(row.get("file_size") or 0)
        if current and (
            size + n > DEFERRED_BATCH_BYTES or len(current) >= DEFERRED_BATCH_ROWS
        ):
            batches.append(current)
            current, size = [], 0
        current.append(row)
        size += n
    if current:
        batches.append(current)
    if dirs:
        batches = [dirs + batches[0], *batches[1:]] if batches else [dirs]
    return batches


@dataclass(frozen=True)
class DeferredInventory:
    """What the sandbox held under the deferred dirs, by path, when probed,
    with what an earlier batch placed there.

    ``present`` is None when the marker was there: an earlier pass finished
    on this sandbox. ``ledgered`` is the part of it the ledger lists.
    """

    present: frozenset[str] | None
    ledgered: frozenset[str] = frozenset()


def deferred_probe_script(layout: WorkspaceLayout) -> str:
    """The shell that takes a ``DeferredInventory``, for a caller to run inside
    a script of its own and hand back through ``parse_deferred``."""
    # Each find prints its own dir's workspace-relative name, which has no
    # ``%``, before the path beneath it.
    finds = "; ".join(
        f"find {shlex.quote(layout.join(d))} -printf '{d}/%P\\0' 2>/dev/null"
        for d in DEFERRED_RESTORE_DIRS
    )
    return _probe_script(layout, f"{finds}; {_cat_ledger(layout)}")


def _probe_script(layout: WorkspaceLayout, listing: str) -> str:
    """``listing`` unless the marker is there, its NUL-ended paths sent as one
    line of base64: a name may hold a newline, which a line per path would
    read as two paths, and the exec output is text a NUL may not survive."""
    marker = shlex.quote(layout.join(DEFERRED_MARKER))
    return (
        f"if [ -e {marker} ]; then echo '#done'; "
        f"else {{ {listing}; }} | base64 -w0; echo; fi; true"
    )


# Printed before the ledger's paths, which, like the finds', all start with
# the deferred dirs' names, so no path can spell it.
_LEDGER_START = "#ledger"


def _cat_ledger(layout: WorkspaceLayout) -> str:
    """The shell that prints every path the ledger lists, workspace-relative
    and NUL-ended, after ``_LEDGER_START``."""
    return (
        f"printf '{_LEDGER_START}\\0'; "
        f"cat {shlex.quote(layout.join(DEFERRED_LEDGER))}/* 2>/dev/null"
    )


def _probed(stdout: str) -> list[str] | None:
    """The paths a ``_probe_script`` listed, or None when it found the marker."""
    text = stdout.strip()
    if text == "#done":
        return None
    listed = base64.b64decode(text, validate=True).decode("utf-8", "surrogateescape")
    return [path for path in listed.split("\0") if path]


def parse_deferred(stdout: str) -> DeferredInventory:
    listed = _probed(stdout)
    if listed is None:
        return DeferredInventory(None)
    cut = listed.index(_LEDGER_START) if _LEDGER_START in listed else len(listed)
    # A dir itself prints with an empty tail: ``.agents/scratchpad/``.
    found = frozenset(path.removesuffix("/") for path in listed[:cut])
    ledgered = frozenset(path.removesuffix("/") for path in listed[cut + 1 :])
    return DeferredInventory(found | ledgered, ledgered)


async def _probe(sandbox: Any, command: str) -> str:
    """A probe's output. The probes end in ``true``, so only a failed exec (a
    timeout) exits otherwise, and its empty output would read as every path
    missing: the pass would send backup bytes over what the sandbox holds."""
    probe = await sandbox.runtime.exec(command)
    if probe.exit_code != 0:
        raise RuntimeError(f"deferred restore probe failed (exit {probe.exit_code})")
    return probe.stdout or ""


async def _deferred_inventory(sandbox: Any, layout: WorkspaceLayout) -> DeferredInventory:
    return parse_deferred(await _probe(sandbox, deferred_probe_script(layout)))


async def _deferred_present(
    sandbox: Any, layout: WorkspaceLayout
) -> frozenset[str] | None:
    return (await _deferred_inventory(sandbox, layout)).present


async def _batch_present(
    sandbox: Any, layout: WorkspaceLayout, batch: list[dict[str, Any]]
) -> frozenset[str] | None:
    """What the sandbox holds at one batch's own paths, or placed there in an
    earlier batch; None when the marker is there, because another worker's
    pass finished meanwhile.

    The inventory taken before the batches holds for every path no other pass
    touched, and another pass writes only rows it found missing, so looking
    again at the batch's own paths, and at the ledger, is enough to see its
    work.
    """
    paths = {row["file_path"] for row in batch}
    by_abs = {layout.join(path): path for path in paths}
    args = " ".join(shlex.quote(path) for path in by_abs)
    if len(args) > _RECHECK_ARGS_MAX:
        return await _deferred_present(sandbox, layout)
    listed = _probed(
        await _probe(
            sandbox,
            _probe_script(
                layout,
                f"find {args} -maxdepth 0 -printf '%p\\0' 2>/dev/null; {_cat_ledger(layout)}",
            ),
        )
    )
    if listed is None:
        return None
    # The finds print absolute paths and the ledger relative ones.
    return frozenset(by_abs.get(path, path) for path in listed if path in by_abs or path in paths)


async def _record_placed(
    sandbox: Any, layout: WorkspaceLayout, rows: list[dict[str, Any]]
) -> None:
    """List paths this sandbox has held since the backup, placed by the pass
    or found there, so a later look reads one the turn deleted since as placed
    rather than missing. A batch lists its own under its lock; the ones the
    pass's first look found need none, since that look took none either."""
    ledger = layout.join(DEFERRED_LEDGER)
    body = "".join(f"{row['file_path']}\0" for row in rows).encode("utf-8")
    if not (
        await sandbox.acreate_directory(ledger)
        and await sandbox.aupload_file_bytes(f"{ledger}/{uuid.uuid4().hex}", body)
    ):
        logger.warning(
            f"Could not record {len(rows)} deferred restore(s) in {ledger}; "
            f"another pass may send one the turn has since deleted"
        )


def _kept(path: str, kept: "ThreadPrefixes") -> bool:
    """Whether a deferred row's thread keeps it; a row under no thread is kept."""
    for base in DEFERRED_RESTORE_DIRS:
        if path == base or path.startswith(base + "/"):
            head = path[len(base) :].lstrip("/").split("/", 1)[0]
            return not THREAD_DIR_NAME.match(head) or head in kept.keeps(base)
    return True


async def restore_deferred(
    workspace_id: str,
    sandbox: Any,
    *,
    layout: WorkspaceLayout,
    kept: "ThreadPrefixes",
    lock_wait: str = SYNC_LOCK_WAIT,
    inventory: DeferredInventory | None = None,
) -> dict[str, Any]:
    """The second restore pass: the evicted results and scratchpads, notes
    aside, of kept threads this sandbox lacks. Each batch reads which threads keep them
    again, so one archived or deleted while the pass runs gets nothing more.

    A row whose path already holds anything is skipped: the pass runs beside
    the first turn, and a restore places only whole copies, so whatever is
    there, of any kind or size, was put there after the backup. One the turn
    writes after a batch looks is kept too: the sandbox places a deferred
    entry only where nothing is (``keep_existing``). Each batch looks again,
    under its lock, at its own paths and at the ledger of what earlier
    batches placed: another worker's bring-up may be restoring the same
    folder, and a second copy would undo an edit made in between or bring
    back a file deleted since. Rows found in place go in the ledger too: a
    pass that does not finish runs again at the next bring-up, and a file the
    turn deleted in between has to stay deleted. A batch with errors, or
    whose transfer failed outright, lists each of its paths the sandbox
    reported in place, and looks once more at them for any that landed
    unreported.

    The marker is written only when every row came back, and ``done`` says
    the sandbox has it; until then backups keep these rows (see
    DEFERRED_RESTORE_DIRS) and the next bring-up resumes. A lock held past
    ``lock_wait`` raises WorkspaceSyncBusy with the batches before it kept.
    ``inventory`` is a probe the caller already took.
    """
    from src.server.database.conversation import get_workspace_thread_prefixes

    result = {"restored": 0, "errors": 0, "skipped": 0, "done": False}
    if inventory is None:
        inventory = await _deferred_inventory(sandbox, layout)
    present = inventory.present
    if present is None:
        result["done"] = True
        return result

    rows = [
        row
        for base in DEFERRED_RESTORE_DIRS
        for row in await get_files_for_workspace(
            workspace_id, all_kinds=True, under=base
        )
        if is_deferred(row["file_path"])
    ]
    due, there = [], []
    for row in rows:
        if _kept(row["file_path"], kept):
            (there if row["file_path"] in present else due).append(row)
    result["skipped"] = len(there)
    # A retry finds in place all an earlier pass listed, and listing it again
    # would add another copy of the ledger at every retry.
    if unlisted := [r for r in there if r["file_path"] not in inventory.ledgered]:
        await _record_placed(sandbox, layout, unlisted)

    if due:
        logger.info(
            f"Restoring {len(due)} deferred entries for workspace {workspace_id}"
        )
        user_id = await workspace_owner(workspace_id)
    batches = _deferred_batches(due)
    made: set[str] = set()
    dir_items: dict[str, dict[str, Any]] = {}
    for batch in batches:
        async with workspace_sync_lock(workspace_id, wait=lock_wait) as conn:
            present = await _batch_present(sandbox, layout, batch)
            if present is None:
                result["done"] = True
                return result
            kept = await get_workspace_thread_prefixes(workspace_id, conn=conn)
            todo, there = [], []
            for r in batch:
                if _kept(r["file_path"], kept):
                    (there if r["file_path"] in present else todo).append(r)
            result["skipped"] += len(batch) - len(todo)
            if todo:
                full = await get_files_for_workspace(
                    workspace_id,
                    include_content=True,
                    all_kinds=True,
                    paths=[r["file_path"] for r in todo],
                    conn=conn,
                )
                dir_items.update(
                    (r["file_path"], _pull_item(r, url=None))
                    for r in full
                    if r.get("kind") == "dir"
                )
                placed: set[str] = set()
                try:
                    part = await _transfer_rows(
                        workspace_id,
                        sandbox,
                        full,
                        user_id=user_id,
                        layout=layout,
                        placed=placed,
                        made=made,
                    )
                except Exception as e:  # noqa: BLE001 - some of the batch may be in place
                    logger.warning(
                        f"Deferred restore batch of {len(todo)} for workspace "
                        f"{workspace_id} failed: {e}"
                    )
                    part = {"restored": 0, "errors": len(todo)}
                result["restored"] += part["restored"]
                result["errors"] += part["errors"]
                if not part["errors"]:
                    there += todo
                else:
                    # What the sandbox reported in place, which the turn may
                    # have deleted since, and what is there now, which covers
                    # a runtime that died before it reported. A look that
                    # fails too leaves the report to be listed alone.
                    try:
                        placed |= await _batch_present(sandbox, layout, todo) or frozenset()
                    except Exception as e:  # noqa: BLE001 - the report still lists
                        logger.warning(
                            f"Could not look again at a failed deferred batch "
                            f"for workspace {workspace_id}: {e}"
                        )
                    there += [r for r in todo if r["file_path"] in placed]
            if there:
                await _record_placed(sandbox, layout, there)

    if len(batches) > 1 and made:
        await _restamp_dirs(
            workspace_id,
            sandbox,
            [dict(dir_items[p], made=True) for p in made if p in dir_items],
            layout=layout,
        )

    if result["errors"]:
        logger.warning(
            f"Deferred restore for workspace {workspace_id} left "
            f"{result['errors']} deferred file(s) unrestored; the next "
            f"bring-up retries and backups keep their rows until then"
        )
        return result
    result["done"] = bool(
        await sandbox.aupload_file_bytes(
            layout.join(DEFERRED_MARKER),
            datetime.now(timezone.utc).isoformat().encode("utf-8"),
        )
    )
    if not result["done"]:
        logger.warning(
            f"Could not write the deferred restore marker for workspace "
            f"{workspace_id}; the next bring-up checks again"
        )
    return result
