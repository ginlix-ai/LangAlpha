"""Get a scanned entry's bytes into object storage, or into the row itself.

Digests the registry already knows cost nothing. The rest go straight from
the sandbox to the store under a presigned PUT, or through this process when
that path is unavailable. Small files travel as members of shared chunks.
"""

import asyncio
import hashlib
import logging
import mimetypes
from dataclasses import dataclass
from typing import Any

from ptc_agent.core.paths import WorkspaceLayout
from src.server.database.blob_keys import (
    BLOB_CONTENT_TYPE,
    RELAY_MAX_BYTES,
    blob_key,
)
from src.server.database.workspace_file import bulk_upsert_files
from src.server.database.workspace_file_blobs import (
    BlobUploadError,
    register_blobs,
    registered_blobs,
    store_blob,
)
from src.server.services.persistence._rows import (
    _blob_row,
    _content_matches,
    _detect_is_binary,
    _pack_row,
    _row_base,
    _stamp_matches,
)
from src.server.services.persistence.sync_result import UnsavedFile, UnsavedReason
from src.server.services.persistence.transfer import (
    transfer_mode,
    INPROCESS_MAX_INFLIGHT_BYTES,
    MULTIPART_THRESHOLD_BYTES,
    PACK_MAX_BYTES,
    PACK_STAGE_MAX_BYTES,
    SINGLE_PUT_MAX_BYTES,
    ByteBudget,
    pack_direct,
    unlink_direct,
    ScanEntry,
    all_unreachable,
    push_direct,
    transfer_timeout_s,
)
from src.utils.storage import (
    abort_multipart_upload,
    complete_multipart_upload,
    create_signed_multipart_upload,
    delete_object,
    get_signed_upload_url,
    sha256_object,
)

# Files moved through this process (inline rows, or blobs when direct
# transfer is unavailable). The count keeps the gather from fanning out
# unboundedly; INPROCESS_MAX_INFLIGHT_BYTES bounds what those files weigh, which
# is the bound that matters now that the direct path caps no file.
RELAY_CONCURRENCY = 8


def _entry_abs_path(entry: ScanEntry, layout: WorkspaceLayout) -> str:
    """Where a scanned entry lives in the sandbox.

    A user's file is relative to the project folder it was scanned under.
    A pack chunk is the machine's own scratch, staged beside the computer
    root rather than inside any one folder, so it arrives already absolute.
    """
    if entry.path.startswith("/"):
        return entry.path
    return f"{layout.workspace}/{entry.path}"


def _machine_layout(layout: WorkspaceLayout) -> WorkspaceLayout:
    """The computer's own root, which is where pack chunks are staged."""
    return WorkspaceLayout(layout.root)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DirectPush:
    """What the store made of one direct-upload pass, per digest."""

    registered: set[str]
    changed: set[str]
    unreachable: set[str]


async def _persist_blobs(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    entries: list[ScanEntry],
    *,
    unlink_after: bool = False,
    layout: WorkspaceLayout,
) -> tuple[list[dict[str, Any]], list[UnsavedFile]]:
    """Make sure every entry's digest has an object, then build its row.

    Digests the registry already knows under the owner are done before any
    byte moves. The rest go direct when the store and sandbox allow it, and
    through this process otherwise. An entry whose digest still has no
    object at the end is dropped from the batch and returned as unsaved:
    its old row survives, and the next sync retries.
    """
    wanted = {e.sha256 for e in entries if e.sha256}
    have = await registered_blobs(user_id, list(wanted))
    need = wanted - have
    mode = transfer_mode(sandbox)
    if unlink_after and have:
        # A chunk the registry already holds is never pushed, so nothing
        # downstream would remove it; it would sit until the age sweep.
        await _unlink_chunks(
            sandbox,
            [e.path for e in entries if e.sha256 in have],
            workspace_id,
            layout,
        )

    # The store answering at all makes a rejection final this pass, so only
    # the digests it never reached fall through to the relay.
    outcome = (
        await _push_direct(
            user_id,
            workspace_id,
            sandbox,
            entries,
            need,
            unlink_after=unlink_after,
            layout=layout,
        )
        if need and mode == "direct"
        else None
    )
    relay_need = need if outcome is None else outcome.unreachable
    registered, changed = (
        (set(), set()) if outcome is None else (outcome.registered, outcome.changed)
    )
    if relay_need:
        relayed, relay_changed = await _relay_blobs(
            user_id,
            workspace_id,
            sandbox,
            entries,
            relay_need,
            unlink_after=unlink_after,
            layout=layout,
        )
        registered |= relayed
        changed |= relay_changed

    available = have | registered
    rows: list[dict[str, Any]] = []
    unsaved: list[UnsavedFile] = []
    for entry in entries:
        if entry.sha256 in available:
            rows.append(_blob_row(entry))
        elif entry.sha256 in changed:
            unsaved.append(UnsavedFile(entry.path, "changed", entry.size))
        else:
            unsaved.append(UnsavedFile(entry.path, "failed", entry.size))
            logger.error(
                f"Blob upload failed for {entry.path} "
                f"(workspace {workspace_id}, sha {entry.sha256}); "
                f"keeping the previous manifest row"
            )
    if changed:
        logger.info(
            f"{len(changed)} file(s) in workspace {workspace_id} changed "
            f"during sync and will be picked up next pass"
        )
    return rows, unsaved


async def _sign_push_items(
    user_id: str,
    workspace_id: str,
    representative: dict[str, ScanEntry],
    *,
    expires: int,
    unlink_after: bool,
) -> (
    tuple[
        list[dict[str, Any]], dict[str, tuple[str, int]], dict[str, dict[str, Any]]
    ]
    | None
):
    """Presign an upload per digest; returns the runtime's items, open uploads,
    and the results of digests no upload could carry.

    Each open upload maps its digest to (upload id, part count).

    ``None`` means the store cannot presign at all, so nothing was opened and
    the caller relays. Signing is local work on a thread each; a large first
    backup has thousands of digests, and one at a time serializes what has no
    order.

    A digest large enough that one PUT is a long time to hold a socket open
    gets a multipart upload *as well as* its single PUT: a failed part costs
    that part rather than the whole file. Nothing resumes across syncs; the
    upload id lives for this call only. The two signatures are alternatives,
    not stages, because a sandbox running a transfer runtime that predates
    parts ignores them and sends the whole file, and the result says which
    happened.
    """
    signatures = await asyncio.gather(
        *(
            asyncio.to_thread(
                get_signed_upload_url,
                blob_key(user_id, sha),
                sha256_hex=sha,
                content_length=entry.size,
                content_type=BLOB_CONTENT_TYPE,
                expires_in=expires,
            )
            for sha, entry in representative.items()
        )
    )
    if any(s is None for s in signatures):
        logger.info(
            f"Store cannot presign uploads; relaying "
            f"{len(representative)} blob(s) for workspace {workspace_id}"
        )
        return None

    large = {
        sha: entry
        for sha, entry in representative.items()
        if entry.size >= MULTIPART_THRESHOLD_BYTES
    }
    creating = asyncio.gather(
        *(
            asyncio.to_thread(
                create_signed_multipart_upload,
                blob_key(user_id, sha),
                content_length=entry.size,
                content_type=BLOB_CONTENT_TYPE,
                expires_in=expires,
            )
            for sha, entry in large.items()
        ),
        return_exceptions=True,
    )
    try:
        created = await asyncio.shield(creating)
    except asyncio.CancelledError:
        # The store calls run on in their threads and open uploads nobody
        # would hold the ids of, so they are aborted once they return.
        creating.add_done_callback(
            lambda done: _abort_orphans(user_id, large, done)
        )
        raise
    uploads, parts, failure = _opened_uploads(large, created)
    if failure is not None:
        # The caller never sees uploads from a call that raised, so the ones
        # that did open are settled here rather than left to the lifecycle rule.
        await _settle_multipart(user_id, uploads, {})
        raise failure

    items: list[dict[str, Any]] = []
    withheld: dict[str, dict[str, Any]] = {}
    for (sha, entry), signed in zip(representative.items(), signatures):
        if sha not in parts and entry.size > SINGLE_PUT_MAX_BYTES:
            # The store refuses a single PUT this size after the whole body
            # has crossed the link, so sending one spends the transfer to
            # learn what the size already says. Next sync asks for parts again.
            withheld[sha] = {
                "status": "failed",
                "error": "too large for one upload and the store would not split it",
            }
            continue
        url, headers = signed
        item: dict[str, Any] = {
            "path": entry.path,
            "sha256": sha,
            "size": entry.size,
            "url": url,
            "headers": headers,
            "unlink": unlink_after,
        }
        if sha in parts:
            item["parts"] = parts[sha]
        items.append(item)
    items.sort(key=lambda i: int(i.get("size") or 0), reverse=True)
    return items, uploads, withheld


def _opened_uploads(
    large: dict[str, ScanEntry], created: list[Any]
) -> tuple[
    dict[str, tuple[str, int]], dict[str, list[dict[str, Any]]], BaseException | None
]:
    uploads: dict[str, tuple[str, int]] = {}
    parts: dict[str, list[dict[str, Any]]] = {}
    failure: BaseException | None = None
    for sha, opened in zip(large, created):
        if isinstance(opened, BaseException):
            failure = failure or opened
        # A store that will not open a multipart upload still takes the
        # single PUT this digest is already signed for.
        elif opened is not None:
            upload_id, parts[sha] = opened
            uploads[sha] = (upload_id, len(parts[sha]))
    return uploads, parts, failure


_orphan_aborts: set[asyncio.Task[Any]] = set()


def _abort_orphans(
    user_id: str, large: dict[str, ScanEntry], done: asyncio.Future[list[Any]]
) -> None:
    if done.cancelled():
        return
    uploads, _, _ = _opened_uploads(large, done.result())
    if uploads:
        task = asyncio.get_running_loop().create_task(
            _settle_multipart(user_id, uploads, {})
        )
        _orphan_aborts.add(task)
        task.add_done_callback(_orphan_aborts.discard)


def _assembly_refusal(
    sha: str, part_count: int, result: dict[str, Any]
) -> dict[str, Any] | None:
    """Why a multipart result must not be assembled, as the status to record.

    Parts are signed for their length only, so the store would assemble
    whatever same-sized bytes arrived under the digest's key. The runtime's
    hash of what it sent catches a file that changed mid-upload before any
    assembly, and a result without one (a runtime from before the hash) is
    not trusted. It is the runtime's word, though, so the assembled object is
    still read back before it counts as stored.
    """
    etags = result.get("etags") or []
    if sorted(int(n) for n, _ in etags) != list(range(1, part_count + 1)):
        return {**result, "status": "failed", "error": "multipart upload is missing parts"}
    sent = result.get("sent_sha256")
    if sent != sha:
        return {
            **result,
            "status": "changed" if sent else "failed",
            "error": "sent bytes do not match the digest"
            if sent
            else "runtime reported no digest for its parts",
        }
    return None


async def _settle_multipart(
    user_id: str,
    uploads: dict[str, tuple[str, int]],
    results: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Assemble the uploads whose parts landed as the digest's bytes, discard the rest.

    Returns ``results`` with any digest whose object did not make it into the
    store no longer saying ``ok``, so one status stays the whole truth about
    a digest. An assembled object counts only once the server has hashed it
    back: a digest names its bytes for every later sync and download link,
    and the code that uploaded the parts runs as the workspace's user. Aborting is best effort and this reports when it fails: an
    upload left open holds its parts as storage nothing lists and no registry
    row references, which only the bucket's lifecycle rule reaps.
    """
    settled = dict(results)

    async def _one(sha: str, upload: tuple[str, int]) -> None:
        upload_id, part_count = upload
        key = blob_key(user_id, sha)
        result = settled.get(sha) or {}
        etags = result.get("etags") if result.get("status") == "ok" else None
        refusal = _assembly_refusal(sha, part_count, result) if etags else None
        if refusal is not None:
            settled[sha] = refusal
            etags = None
        if etags:
            done = await asyncio.to_thread(
                complete_multipart_upload,
                key,
                upload_id,
                [(int(number), str(etag)) for number, etag in etags],
            )
            if done:
                held = await asyncio.to_thread(sha256_object, key, transfer_timeout_s)
                if held == sha:
                    return
                if held is not None:
                    # Wrong bytes under a digest's name are worse than none:
                    # a missing object is re-sent, a wrong one is trusted.
                    logger.error(
                        f"Assembled {key} hashes to {held}, not its digest; deleting it"
                    )
                    await asyncio.to_thread(delete_object, key)
                # Unreadable is left unregistered, so the next sync sends it again.
                settled[sha] = {
                    **result,
                    "status": "failed",
                    "error": "assembled object does not match its digest"
                    if held is not None
                    else "assembled object could not be read back",
                }
                return
            settled[sha] = {
                **result,
                "status": "failed",
                "error": "multipart upload could not be assembled",
            }
        if not await asyncio.to_thread(abort_multipart_upload, key, upload_id):
            logger.error(
                f"Leaked multipart upload {upload_id} on {key}: its parts are "
                f"stored and billed, and no listing shows them. The bucket's "
                f"AbortIncompleteMultipartUpload rule is what will reap it"
            )

    # An upload whose sibling raised still has to be settled, so a failure
    # here is reported rather than allowed to cancel the rest.
    outcomes = await asyncio.gather(
        *(_one(sha, uid) for sha, uid in uploads.items()),
        return_exceptions=True,
    )
    for (sha, (upload_id, _)), outcome in zip(uploads.items(), outcomes):
        if isinstance(outcome, BaseException):
            logger.error(
                f"Leaked multipart upload {upload_id} on "
                f"{blob_key(user_id, sha)}: settling it raised {outcome!r}"
            )
            settled[sha] = {
                **(settled.get(sha) or {}),
                "status": "failed",
                "error": f"multipart settle failed: {outcome!r}",
            }
    return settled


async def _push_direct(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    entries: list[ScanEntry],
    need: set[str],
    *,
    unlink_after: bool = False,
    layout: WorkspaceLayout,
) -> DirectPush | None:
    """Presign an upload per missing digest and let the sandbox move the bytes.

    ``None`` means the direct path was not usable at all (no presigning, or
    the store unreachable from the sandbox); otherwise the digests the store
    never answered for come back in ``unreachable``. Either way the caller
    owns the fallback.
    """
    representative: dict[str, ScanEntry] = {}
    for entry in entries:
        if entry.sha256 in need and entry.sha256 not in representative:
            representative[entry.sha256] = entry
    total = sum(e.size for e in representative.values())
    expires = transfer_timeout_s(total) + 60

    # Every upload opened by the signing has to be settled whether the push
    # returned, raised, or was cancelled, so the settle is the exit path
    # rather than a step on it, and the signing sits inside it too: a
    # cancellation landing between the two would otherwise strand them.
    uploads: dict[str, tuple[str, int]] = {}
    results: dict[str, dict[str, Any]] = {}
    try:
        signed = await _sign_push_items(
            user_id,
            workspace_id,
            representative,
            expires=expires,
            unlink_after=unlink_after,
        )
        if signed is None:
            return None
        items, uploads, withheld = signed
        results = {
            **withheld,
            **(await push_direct(sandbox, items, layout=layout) if items else {}),
        }
    finally:
        results = await _settle_multipart(user_id, uploads, results)

    if all_unreachable(results):
        logger.warning(
            f"Sandbox for workspace {workspace_id} could not reach object "
            f"storage; relaying {len(items)} blob(s) through the server"
        )
        return None
    unreachable = {
        sha for sha, r in results.items() if r.get("status") == "unreachable"
    }
    if unreachable:
        # The store answered for the rest, so this is a flaky egress path
        # rather than a missing route, and the caller relays just these in
        # the same pass instead of leaving them for the next sync.
        logger.warning(
            f"{len(unreachable)} of {len(items)} direct upload(s) for "
            f"workspace {workspace_id} could not reach object storage; "
            f"relaying those through the server"
        )
    ok = [
        (sha, representative[sha].size)
        for sha, r in results.items()
        if r.get("status") == "ok" and sha in representative
    ]
    await register_blobs(user_id, ok)
    for sha, r in results.items():
        if r.get("status") not in ("ok", "changed", "unreachable"):
            logger.warning(
                f"Direct upload of {representative.get(sha).path if sha in representative else sha} "
                f"for workspace {workspace_id} {r.get('status')}: "
                f"http={r.get('http')} {r.get('error')}"
            )
    return DirectPush(
        registered={sha for sha, _ in ok},
        changed={
            sha for sha, r in results.items() if r.get("status") == "changed"
        },
        unreachable=unreachable,
    )


async def _relay_blobs(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    entries: list[ScanEntry],
    need: set[str],
    *,
    unlink_after: bool = False,
    layout: WorkspaceLayout,
) -> tuple[set[str], set[str]]:
    """Copy missing digests through this process: download, hash, PUT."""
    representative: dict[str, ScanEntry] = {}
    for entry in entries:
        if entry.sha256 in need and entry.sha256 not in representative:
            representative[entry.sha256] = entry

    registered: set[str] = set()
    changed: set[str] = set()
    budget = ByteBudget(INPROCESS_MAX_INFLIGHT_BYTES, RELAY_CONCURRENCY)

    async def _one(sha: str, entry: ScanEntry) -> None:
        if entry.size > RELAY_MAX_BYTES:
            # The direct path caps nothing, so a file this large is normal
            # until the store turns out to be unreachable and the push lands
            # here instead. Downloading it would pull the whole thing into
            # this process. Left unregistered, so the caller reports it
            # unsaved as ``failed``, keeps the previous row, and the next sync
            # retries: unlike a
            # scan-time rejection this is a passing condition, not a limit
            # the file will always exceed. Checked before the budget, since
            # holding weight for bytes we refuse to move helps nobody.
            logger.error(
                f"Cannot relay {entry.path} for workspace {workspace_id}: "
                f"{entry.size} bytes exceeds the {RELAY_MAX_BYTES} byte "
                f"relay limit, and object storage was unreachable from "
                f"the sandbox. Keeping the previous manifest row"
            )
            return
        async with budget.hold(entry.size):
            try:
                content = await sandbox.adownload_file_bytes(
                    _entry_abs_path(entry, layout)
                )
                if content is None:
                    changed.add(sha)
                    return
                # Length as well as digest: the scan stats a file before it
                # hashes it, so a size the digest does not cover is a change.
                if len(content) != entry.size or hashlib.sha256(content).hexdigest() != sha:
                    changed.add(sha)
                    return
                await store_blob(user_id, sha, content)
                registered.add(sha)
            except BlobUploadError as e:
                logger.error(
                    f"Blob upload failed for {entry.path} "
                    f"(workspace {workspace_id}, sha {sha}): {e}"
                )
            except Exception as e:
                logger.warning(
                    f"Error relaying {entry.path} for workspace "
                    f"{workspace_id}: {e}"
                )

    await asyncio.gather(*(_one(s, e) for s, e in representative.items()))
    if unlink_after:
        await _unlink_chunks(
            sandbox, [e.path for e in entries], workspace_id, layout
        )
    return registered, changed


async def _unlink_chunks(
    sandbox: Any, paths: list[str], workspace_id: str, layout: WorkspaceLayout
) -> None:
    try:
        await unlink_direct(sandbox, paths, layout=layout)
    except Exception as e:
        # Cosmetic: the next pack op sweeps what is left by age.
        logger.info(f"Could not remove pack chunks for {workspace_id}: {e}")


async def _persist_packed(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    members: list[ScanEntry],
    existing: dict[str, dict[str, Any]],
    *,
    may_prune: bool = True,
    withheld: frozenset[str] = frozenset(),
    layout: WorkspaceLayout,
) -> tuple[list[dict[str, Any]], list[UnsavedFile], int]:
    """Rows for every small file, via the pack set. Returns (rows, unsaved, skipped).

    The pack set is rewritten whole whenever a member changed, appeared or
    left, so a workspace never trails half-dead chunks and a restore stays
    a handful of GETs. It is staged a run of chunks at a time
    (``_pack_runs``), so a backup never needs more free disk than
    ``PACK_STAGE_MAX_BYTES``. When nothing changed and every member already
    points at a pack, the only work is a metadata refresh for rows whose
    stamp moved. A file that only recently shrank below the cutoff, or
    was stored per object before packs existed, reads as a set change
    and is packed on this pass. A member that is absent from the sandbox
    counts as having left only when this pass may prune its row: while
    pruning is withheld the row stays and the file is expected back, so
    repacking around it would repeat on every sync until the restore; a
    ``withheld`` member (a deferred restore still due) is expected back the
    same way. The
    same pass leaves a moved stamp unrecorded, for the reason ``sync_to_db``
    gives: it may be the failed restore's own doing.
    """
    by_path = {e.path: e for e in members}

    def packed_unchanged(e: ScanEntry) -> bool:
        db = existing.get(e.path)
        return _content_matches(db, e) and bool(db.get("pack_sha256"))

    previously_packed = {
        path
        for path, m in existing.items()
        if m.get("kind", "file") == "file" and m.get("pack_sha256")
    }
    left = previously_packed - set(by_path) - withheld if may_prune else set()
    if not left and all(packed_unchanged(e) for e in members):
        rows: list[dict[str, Any]] = []
        for e in members:
            db = existing[e.path]
            if may_prune and not _stamp_matches(db, e):
                rows.append(
                    _pack_row(
                        e, db["pack_sha256"], db["pack_offset"], is_binary=db.get("is_binary")
                    )
                )
        return rows, [], len(members)

    def unsaved_member(path: str, reason: UnsavedReason) -> UnsavedFile:
        e = by_path.get(path)
        return UnsavedFile(path, reason, e.size if e else None)

    # The chunks themselves belong to the machine, not to the project folder
    # the members came from, so they push and unlink at the computer root;
    # ``_entry_abs_path`` reads their absolute paths as such.
    machine = _machine_layout(layout)
    rows = []
    failed: list[UnsavedFile] = []
    changed: set[str] = set()
    chunk_count = 0
    staged: list[str] = []
    for run in _pack_runs(members):
        try:
            # Each run is staged in the room the previous one held, so its pack
            # removes those chunks first and fails rather than pack beside one.
            out = await pack_direct(
                sandbox,
                [{"path": e.path, "sha256": e.sha256, "size": e.size} for e in run],
                layout=layout,
                release=staged,
            )
            chunks = out["chunks"]
            staged = [c["path"] for c in chunks]
            changed.update(out["changed"])
            chunk_count += len(chunks)
            # A chunk is pushed exactly like a file: same presigning, same direct
            # path with relay fallback, same registry. It just is not a file the
            # user has, so it is removed from the sandbox once pushed.
            chunk_entries = [
                ScanEntry(
                    path=c["path"],
                    kind="file",
                    size=int(c["size"]),
                    mtime_ns=0,
                    mode=0,
                    sha256=c["sha256"],
                    symlink_target=None,
                    is_binary=True,
                )
                for c in chunks
            ]
            chunk_rows, _ = await _persist_blobs(
                user_id,
                workspace_id,
                sandbox,
                chunk_entries,
                unlink_after=True,
                layout=machine,
            )
        except (Exception, asyncio.CancelledError):
            # No later pack will release this run's chunks, or the previous
            # run's when this pack failed before releasing them. A client
            # dropping or a shutdown cancels a backup the same way.
            await asyncio.shield(_unlink_chunks(sandbox, staged, workspace_id, machine))
            raise
        available = {r["blob_sha256"] for r in chunk_rows}
        stranded = [c["path"] for c in chunks if c["sha256"] not in available]
        if stranded:
            # Only a stored chunk leaves with its push. The next run's pack
            # would release a rejected one, but the last run has no next.
            await _unlink_chunks(sandbox, stranded, workspace_id, machine)
        for c in chunks:
            if c["sha256"] not in available:
                # The old rows survive, still pointing at the previous chunk,
                # which stays referenced and restorable; next sync retries.
                failed.extend(unsaved_member(m["path"], "failed") for m in c["members"])
                continue
            for m in c["members"]:
                e = by_path.get(m["path"])
                if e is None:
                    continue
                db = existing.get(e.path)
                is_binary = db.get("is_binary") if _content_matches(db, e) else None
                rows.append(_pack_row(e, c["sha256"], m["offset"], is_binary=is_binary))

    unsaved = [unsaved_member(path, "changed") for path in sorted(changed)] + failed
    if changed:
        logger.info(
            f"{len(changed)} small file(s) in workspace {workspace_id} changed "
            f"during packing and will be picked up next pass"
        )
    logger.info(
        f"Packed {len(rows)} file(s) into {chunk_count} chunk(s) "
        f"for workspace {workspace_id}"
    )
    return rows, unsaved, 0


def _pack_runs(members: list[ScanEntry]) -> list[list[ScanEntry]]:
    """Split the pack set into runs of whole chunks, staged one run at a time.

    The cuts fall where the pack op closes a chunk anyway: path order, with a
    chunk closing before the member that would overflow it. Each run then packs
    into the chunks one pass over the whole set would have written, so an
    unchanged chunk still finds its digest in the registry and is not uploaded.
    """
    per_run = max(PACK_STAGE_MAX_BYTES // PACK_MAX_BYTES, 1)
    runs: list[list[ScanEntry]] = [[]]
    closed = 0
    open_bytes: int | None = None
    for e in sorted(members, key=lambda e: e.path):
        if open_bytes is not None and open_bytes + e.size > PACK_MAX_BYTES:
            closed += 1
            open_bytes = None
            if closed == per_run:
                runs.append([])
                closed = 0
        runs[-1].append(e)
        open_bytes = (open_bytes or 0) + e.size
    return [run for run in runs if run]


def _batched_by_weight(
    entries: list[ScanEntry], max_bytes: int, max_count: int
) -> list[list[ScanEntry]]:
    """Split entries into runs bounded by combined size and by count.

    An entry heavier than the whole allowance gets a run to itself rather
    than one that can never be filled.
    """
    batches: list[list[ScanEntry]] = []
    run: list[ScanEntry] = []
    weight = 0
    for entry in entries:
        size = max(int(entry.size or 0), 0)
        if run and (len(run) >= max_count or weight + size > max_bytes):
            batches.append(run)
            run, weight = [], 0
        run.append(entry)
        weight += size
    if run:
        batches.append(run)
    return batches


async def _persist_inline(
    workspace_id: str,
    sandbox: Any,
    entries: list[ScanEntry],
    *,
    layout: WorkspaceLayout,
    conn: Any,
) -> tuple[int, list[UnsavedFile]]:
    """No object store: bytes go into the manifest row itself.

    Written a batch at a time rather than gathered and upserted once, because
    an inline row *is* its bytes: holding every row for one final write would
    make the peak the whole changed working set, which is exactly what
    bounding in-flight bytes is supposed to prevent. Each batch's rows are
    released as soon as they are stored, so the bound is the batch.
    """
    synced = 0
    unsaved: list[UnsavedFile] = []

    async def _one(entry: ScanEntry) -> dict[str, Any] | UnsavedFile:
        try:
            content = await sandbox.adownload_file_bytes(
                _entry_abs_path(entry, layout)
            )
            if content is None:
                return UnsavedFile(entry.path, "changed", entry.size)
            # The row describes the bytes it carries, so hash and size
            # come from the download even if the scan saw an earlier
            # version of the file.
            content_hash = hashlib.sha256(content).hexdigest()
            is_binary = _detect_is_binary(entry.path, content)
            content_text = None
            content_binary = None
            if is_binary:
                content_binary = content
            else:
                try:
                    content_text = content.decode("utf-8")
                except UnicodeDecodeError:
                    is_binary = True
                    content_binary = content
            row = _row_base(entry)
            mime, _ = mimetypes.guess_type(entry.path)
            row.update(
                {
                    "file_size": len(content),
                    "content_hash": content_hash,
                    "content_text": content_text,
                    "content_binary": content_binary,
                    "mime_type": mime,
                    "is_binary": is_binary,
                }
            )
            return row
        except Exception as e:
            logger.warning(
                f"Error downloading file {entry.path} "
                f"for workspace {workspace_id}: {e}"
            )
            return UnsavedFile(entry.path, "failed", entry.size)

    for batch in _batched_by_weight(
        entries, INPROCESS_MAX_INFLIGHT_BYTES, RELAY_CONCURRENCY
    ):
        rows: list[dict[str, Any]] = []
        for payload in await asyncio.gather(*(_one(e) for e in batch)):
            if isinstance(payload, UnsavedFile):
                unsaved.append(payload)
            else:
                rows.append(payload)
        if rows:
            synced += await bulk_upsert_files(workspace_id, rows, conn=conn)
    return synced, unsaved
