"""Snapshot a workspace's sandbox tree into the manifest.

The sandbox walks and hashes its own tree; this side diffs the result
against the manifest and moves only the bytes whose digest is not yet
registered. Getting those bytes into storage is ``blobs``; deciding which
ones move, and what the manifest then says, is here.
"""

import errno
import logging
import os
from datetime import datetime
from typing import Any

from src.server.database.workspace_file import (
    bulk_update_file_stamps,
    bulk_upsert_files,
    delete_file_rows,
    delete_removed_files,
    manifest_clock,
    get_file_metadata_for_sync,
    get_workspace_total_size,
    workspace_sync_lock,
)
from src.server.database.workspace import files_restore_incomplete, workspace_owner
from src.server.database.blob_keys import MAX_BLOB_BYTES
from src.server.services.persistence._rows import (
    _blob_row,
    _content_matches,
    _entry_mode_string,
    _ns_to_datetime,
    _row_base,
    _stamp_matches,
)
from src.server.services.persistence.blobs import (
    _persist_blobs,
    _persist_inline,
    _persist_packed,
)
from src.server.services.persistence.transfer import (
    PACK_CUTOFF,
    ScanEntry,
    scan_workspace,
)
from src.utils.storage import is_storage_enabled

# Same number as the per-blob storage cap, and derived from it rather than
# restated: a file this path accepts must be storable.
MAX_FILE_SIZE = MAX_BLOB_BYTES
MAX_WORKSPACE_SIZE = 1024 * 1024 * 1024  # 1GB total per workspace

logger = logging.getLogger(__name__)


async def list_sandbox_files(
    sandbox: Any, *, prior: dict[str, tuple[int, int, str]] | None = None
) -> dict[str, dict[str, Any]]:
    """Listing of regular files for the backup-status route.

    ``prior`` (path -> (size, mtime_ns, sha256)) lets the scan reuse
    hashes for unchanged files instead of re-reading the whole tree.
    """
    scan = await scan_workspace(
        sandbox, prior or {}, max_file_bytes=MAX_FILE_SIZE
    )
    work_dir = sandbox.working_dir
    return {
        e.path: {
            "abs_path": f"{work_dir}/{e.path}",
            "file_name": os.path.basename(e.path),
            "file_size": e.size,
            "mtime": e.mtime_ns / 1e9,
            "content_hash": e.sha256,
        }
        for e in scan.entries
        if e.kind == "file"
    }


def prior_from_meta(
    existing: dict[str, dict[str, Any]],
) -> dict[str, tuple[int, int, str]]:
    return {
        path: (m["file_size"], m["mtime_ns"], m["content_hash"])
        for path, m in existing.items()
        if m.get("kind", "file") == "file"
        and m.get("mtime_ns") is not None
        and m.get("content_hash")
    }


def _has_ancestor_in(path: str, names: set[str]) -> bool:
    cut = path.rfind("/")
    while cut > 0:
        if path[:cut] in names:
            return True
        cut = path.rfind("/", 0, cut)
    return False


async def sync_to_db(workspace_id: str, sandbox: Any) -> dict[str, Any]:
    """
    Snapshot workspace files from the sandbox into the manifest.

    The sandbox walks and hashes its own tree; this side diffs the result
    against the manifest and moves only the bytes whose digest is not yet
    registered. Under ``direct`` transfer those bytes never pass through
    this process.

    Serialized per workspace across workers: the diff decides what to write
    from a read taken before the scan, so two overlapping passes would let
    the older one's upsert land last and reinstate what the newer pass had
    already recorded.

    Returns:
        Sync result summary
    """
    try:
        async with workspace_sync_lock(workspace_id) as conn:
            return await _sync_locked(workspace_id, sandbox, conn)
    except Exception as e:
        logger.error(f"File sync failed for workspace {workspace_id}: {e}")
        raise


async def _sync_locked(
    workspace_id: str, sandbox: Any, conn: Any
) -> dict[str, Any]:
    """One sync pass, holding this workspace's lock on ``conn``.

    Every manifest read and write rides that same session: acquiring a
    second pooled connection underneath a lock holder doubles the slots a
    sync costs and can stall on the pool's own timeout.
    """
    result = {
        "synced": 0, "skipped": 0, "deleted": 0, "errors": 0,
        "oversized": 0, "total_size": 0,
    }

    # Taken before the scan, on the database's clock: rows written by
    # anyone after this instant are newer than what this pass saw.
    started_at = await manifest_clock(conn=conn)
    existing = await get_file_metadata_for_sync(workspace_id, conn=conn)
    scan = await scan_workspace(
        sandbox,
        prior_from_meta(existing),
        max_file_bytes=MAX_FILE_SIZE,
    )
    result["oversized"] = len(scan.oversized)
    for item in scan.oversized:
        logger.warning(
            f"Skipping {item.get('path')} in workspace {workspace_id}: "
            f"{item.get('size')} bytes exceeds the {MAX_FILE_SIZE} "
            f"per-file limit"
        )
    read_errors = 0
    for item in scan.errors:
        # ENOENT below the root is a file removed between the listing
        # and the read: absent for the right reason, so it is neither
        # data at risk nor a reason to withhold pruning. Anything else
        # (EACCES, EIO, the root itself) is data at risk, and a strict
        # backup must refuse to tear the sandbox down over it.
        if item.get("errno") == errno.ENOENT and item.get("path") != ".":
            logger.info(
                f"{item.get('path')} in workspace {workspace_id} vanished "
                f"during the scan; treating it as deleted"
            )
            continue
        logger.warning(
            f"Could not read {item.get('path')} in workspace "
            f"{workspace_id}: {item.get('error')}"
        )
        result["errors"] += 1
        read_errors += 1

    # Pruning treats "in the manifest but not the sandbox" as a user
    # deletion. That inference only holds if the sandbox actually has
    # everything it should — a failed restore produces the identical
    # signature. A read failure counts as incomplete: the cost of
    # skipping a prune is a stale row, and the cost of the opposite
    # guess is the only surviving record of a file.
    try:
        may_prune = not await files_restore_incomplete(workspace_id, conn=conn)
    except Exception as e:
        logger.warning(
            f"Could not read the restore-completeness flag for workspace "
            f"{workspace_id} ({e}); treating the sandbox as an incomplete "
            f"mirror and skipping deletions this pass"
        )
        may_prune = False

    if read_errors and may_prune:
        # A path the scan could not read is absent from the listing
        # for the wrong reason. The root itself failing looks like
        # an emptied workspace, and pruning on that erases the file
        # list; one unreadable directory is the same mistake in
        # miniature. Stale rows are the cheaper error.
        logger.warning(
            f"Scan of workspace {workspace_id} hit {read_errors} read "
            f"error(s); keeping every manifest row this pass"
        )
        may_prune = False

    if not scan.entries:
        logger.info(f"No files found for workspace {workspace_id}")
        if not may_prune:
            logger.warning(
                f"Sandbox for workspace {workspace_id} listed no files and "
                f"is a known-incomplete mirror; keeping every manifest row. "
                f"A restore that failed for all files looks exactly like "
                f"this, and pruning here would erase the whole file list."
            )
            return result
        deleted = await delete_removed_files(
            workspace_id, set(), untouched_since=started_at, conn=conn
        )
        result["deleted"] = deleted
        return result

    total_size = sum(e.size for e in scan.entries if e.kind == "file")
    if total_size > MAX_WORKSPACE_SIZE:
        logger.warning(
            f"Workspace {workspace_id} total size ({total_size}) exceeds limit "
            f"({MAX_WORKSPACE_SIZE}). Syncing anyway but this may be slow."
        )

    active_paths: set[str] = set()
    rows: list[dict[str, Any]] = []
    stamp_updates: list[tuple[str, datetime, str | None]] = []
    needs_bytes: list[ScanEntry] = []
    pack_members: list[ScanEntry] = []
    blobs_on = is_storage_enabled()
    # Object keys are scoped to the owner; read once for the whole pass.
    user_id = await workspace_owner(workspace_id, conn=conn) if blobs_on else None

    # While the mirror is known incomplete a stamp that moved on an
    # unchanged entry may be the restore's own failed chmod or utime, and
    # recording it would make the wrong mode the one the retry restores.
    # The manifest stays the authority on metadata until a restore completes.
    refresh_stamps = may_prune

    # A directory that came back as a file or symlink still has its old
    # children in the manifest, and the scan never lists them (it does not
    # descend into either). They are not "absent for an unknown reason",
    # which is what the prune gate protects: the parent's new kind proves
    # they cannot exist, and a restore that keeps them makes the child
    # create a directory where the symlink or file has to land. Keyed on
    # the scan alone: a manifest written before directories had rows of
    # their own holds the children with no parent row to compare against.
    non_dir_paths: set[str] = set()

    for entry in scan.entries:
        active_paths.add(entry.path)
        db = existing.get(entry.path)

        if entry.kind != "dir":
            non_dir_paths.add(entry.path)

        if entry.kind != "file":
            if (
                db is not None
                and db.get("kind") == entry.kind
                and db.get("symlink_target") == entry.symlink_target
                and (_stamp_matches(db, entry) or not refresh_stamps)
            ):
                result["skipped"] += 1
            else:
                rows.append(_row_base(entry))
            continue

        if entry.sha256 is None:
            # The scan reports why in ``errors`` and already counted it.
            continue

        if blobs_on and entry.size <= PACK_CUTOFF:
            # Small files are decided as a set, below: whether the
            # pack they share needs rewriting depends on all of them.
            pack_members.append(entry)
            continue

        if _content_matches(db, entry):
            result["skipped"] += 1
            if _stamp_matches(db, entry) or not refresh_stamps:
                continue
            if db.get("blob_sha256"):
                # Pointer row: a metadata refresh costs no bytes.
                rows.append(
                    _blob_row(entry, is_binary=db.get("is_binary"))
                )
            else:
                # Inline row: the stamps can be refreshed without
                # rewriting content the row does not carry.
                stamp_updates.append(
                    (
                        entry.path,
                        _ns_to_datetime(entry.mtime_ns),
                        _entry_mode_string(entry),
                    )
                )
            continue

        needs_bytes.append(entry)

    if needs_bytes:
        if blobs_on:
            persisted, errors = await _persist_blobs(
                user_id, workspace_id, sandbox, needs_bytes
            )
        else:
            persisted, errors = await _persist_inline(
                workspace_id, sandbox, needs_bytes
            )
        rows.extend(persisted)
        result["errors"] += errors

    if pack_members:
        packed, errors, skipped = await _persist_packed(
            user_id, workspace_id, sandbox, pack_members, existing, may_prune=may_prune
        )
        rows.extend(packed)
        result["errors"] += errors
        result["skipped"] += skipped

    if stamp_updates:
        # A mode or mtime change on unchanged bytes is persisted only here.
        # Left uncounted, a failed write reads as a clean backup, and a strict
        # caller would tear the sandbox down with the new stamps unrecorded.
        try:
            await bulk_update_file_stamps(workspace_id, stamp_updates, conn=conn)
        except Exception as e:
            logger.error(
                f"Stamp update failed for {len(stamp_updates)} files in "
                f"workspace {workspace_id}: {e}"
            )
            result["errors"] += len(stamp_updates)

    if rows:
        result["synced"] = await bulk_upsert_files(
            workspace_id, rows, conn=conn
        )

    orphaned = [
        p for p in existing
        if p not in active_paths and _has_ancestor_in(p, non_dir_paths)
    ]
    if orphaned:
        result["deleted"] += await delete_file_rows(
            workspace_id, orphaned, conn=conn
        )

    if may_prune:
        deleted = await delete_removed_files(
            workspace_id, active_paths, untouched_since=started_at, conn=conn
        )
        result["deleted"] += deleted
    else:
        withheld = len(set(existing) - active_paths)
        if withheld:
            logger.warning(
                f"Keeping {withheld} manifest row(s) for workspace "
                f"{workspace_id} whose files are absent from the sandbox: "
                f"it is a known-incomplete mirror, so a missing file means "
                f"a failed restore, not a user deletion. The next workspace "
                f"start retries the restore and pruning resumes."
            )

    result["total_size"] = await get_workspace_total_size(
        workspace_id, conn=conn
    )

    logger.debug(
        f"File sync completed for workspace {workspace_id}: "
        f"synced={result['synced']}, skipped={result['skipped']}, "
        f"deleted={result['deleted']}, errors={result['errors']}, "
        f"hashed={scan.hashed}, reused={scan.reused}"
    )

    return result
