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

from src.server.database.blob_keys import BLOB_CONTENT_TYPE, blob_key
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
    _transfer_mode,
)
from src.server.services.persistence.transfer import (
    pack_direct,
    unlink_direct,
    ScanEntry,
    all_unreachable,
    push_direct,
    transfer_timeout_s,
)
from src.utils.storage import get_signed_upload_url

# Files moved through this process (inline rows, or blobs when direct
# transfer is unavailable). The sandbox's own download semaphore is the
# tighter bound; this one keeps the gather from fanning out unboundedly.
RELAY_CONCURRENCY = 8

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
) -> tuple[list[dict[str, Any]], int]:
    """Make sure every entry's digest has an object, then build its row.

    Digests the registry already knows under the owner are done before any
    byte moves. The rest go direct when the store and sandbox allow it, and
    through this process otherwise. An entry whose digest still has no
    object at the end is dropped from the batch and counted as an error:
    its old row survives, and the next sync retries.
    """
    wanted = {e.sha256 for e in entries if e.sha256}
    have = await registered_blobs(user_id, list(wanted))
    need = wanted - have
    mode = _transfer_mode(sandbox)
    if unlink_after and have:
        # A chunk the registry already holds is never pushed, so nothing
        # downstream would remove it; it would sit until the age sweep.
        await _unlink_chunks(
            sandbox, [e.path for e in entries if e.sha256 in have], workspace_id
        )

    # The store answering at all makes a rejection final this pass, so only
    # the digests it never reached fall through to the relay.
    outcome = (
        await _push_direct(
            user_id, workspace_id, sandbox, entries, need, unlink_after=unlink_after
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
        )
        registered |= relayed
        changed |= relay_changed

    available = have | registered
    rows: list[dict[str, Any]] = []
    errors = 0
    for entry in entries:
        if entry.sha256 in available:
            rows.append(_blob_row(entry))
        else:
            errors += 1
            if entry.sha256 not in changed:
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
    return rows, errors


async def _push_direct(
    user_id: str,
    workspace_id: str,
    sandbox: Any,
    entries: list[ScanEntry],
    need: set[str],
    *,
    unlink_after: bool = False,
) -> DirectPush | None:
    """Presign one PUT per missing digest and let the sandbox upload.

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

    # Signing is local work on a thread each; a large first backup has
    # thousands of digests, and one at a time serializes what has no order.
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
    items: list[dict[str, Any]] = []
    for (sha, entry), signed in zip(representative.items(), signatures):
        if signed is None:
            logger.info(
                f"Store cannot presign uploads; relaying {len(need)} "
                f"blob(s) for workspace {workspace_id}"
            )
            return None
        url, headers = signed
        items.append(
            {
                "path": entry.path,
                "sha256": sha,
                "size": entry.size,
                "url": url,
                "headers": headers,
                "unlink": unlink_after,
            }
        )

    items.sort(key=lambda i: int(i.get("size") or 0), reverse=True)
    results = await push_direct(sandbox, items)
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
) -> tuple[set[str], set[str]]:
    """Copy missing digests through this process: download, hash, PUT."""
    representative: dict[str, ScanEntry] = {}
    for entry in entries:
        if entry.sha256 in need and entry.sha256 not in representative:
            representative[entry.sha256] = entry

    registered: set[str] = set()
    changed: set[str] = set()
    sem = asyncio.Semaphore(RELAY_CONCURRENCY)

    async def _one(sha: str, entry: ScanEntry) -> None:
        async with sem:
            try:
                content = await sandbox.adownload_file_bytes(
                    f"{sandbox.working_dir}/{entry.path}"
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
        await _unlink_chunks(sandbox, [e.path for e in entries], workspace_id)
    return registered, changed


async def _unlink_chunks(sandbox: Any, paths: list[str], workspace_id: str) -> None:
    try:
        await unlink_direct(sandbox, paths)
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
) -> tuple[list[dict[str, Any]], int, int]:
    """Rows for every small file, via the pack set. Returns (rows, errors, skipped).

    The pack set is rewritten whole whenever a member changed, appeared or
    left, so a workspace never trails half-dead chunks and a restore stays
    a handful of GETs. When nothing changed and every member already
    points at a pack, the only work is a metadata refresh for rows whose
    stamp moved. A file that only recently shrank below the cutoff, or
    was stored per object before packs existed, reads as a set change
    and is packed on this pass. A member that is absent from the sandbox
    counts as having left only when this pass may prune its row: while
    pruning is withheld the row stays and the file is expected back, so
    repacking around it would repeat on every sync until the restore. The
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
    left = previously_packed - set(by_path) if may_prune else set()
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
        return rows, 0, len(members)

    out = await pack_direct(
        sandbox,
        [{"path": e.path, "sha256": e.sha256, "size": e.size} for e in members],
    )
    chunks = out["chunks"]
    changed = set(out["changed"])
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
        user_id, workspace_id, sandbox, chunk_entries, unlink_after=True
    )
    available = {r["blob_sha256"] for r in chunk_rows}

    rows = []
    errors = len(changed)
    for c in chunks:
        if c["sha256"] not in available:
            # The old rows survive, still pointing at the previous chunk,
            # which stays referenced and restorable; next sync retries.
            errors += len(c["members"])
            continue
        for m in c["members"]:
            e = by_path.get(m["path"])
            if e is None:
                continue
            db = existing.get(e.path)
            is_binary = db.get("is_binary") if _content_matches(db, e) else None
            rows.append(_pack_row(e, c["sha256"], m["offset"], is_binary=is_binary))
    if changed:
        logger.info(
            f"{len(changed)} small file(s) in workspace {workspace_id} changed "
            f"during packing and will be picked up next pass"
        )
    logger.info(
        f"Packed {len(rows)} file(s) into {len(chunks)} chunk(s) "
        f"for workspace {workspace_id}"
    )
    return rows, errors, 0


async def _persist_inline(
    workspace_id: str, sandbox: Any, entries: list[ScanEntry]
) -> tuple[list[dict[str, Any]], int]:
    """No object store: bytes go into the manifest row itself."""
    rows: list[dict[str, Any]] = []
    errors = 0
    sem = asyncio.Semaphore(RELAY_CONCURRENCY)

    async def _one(entry: ScanEntry) -> dict[str, Any] | None:
        async with sem:
            try:
                content = await sandbox.adownload_file_bytes(
                    f"{sandbox.working_dir}/{entry.path}"
                )
                if content is None:
                    return None
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
                return None

    for payload in await asyncio.gather(*(_one(e) for e in entries)):
        if payload is None:
            errors += 1
        else:
            rows.append(payload)
    return rows, errors
