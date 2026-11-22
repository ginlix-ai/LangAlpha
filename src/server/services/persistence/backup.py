"""Snapshot a workspace's sandbox tree into the manifest.

The sandbox walks and hashes its own tree; this side diffs the result
against the manifest and moves only the bytes whose digest is not yet
registered. Getting those bytes into storage is ``blobs``; deciding which
ones move, and what the manifest then says, is here.
"""

import errno
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ptc_agent.core.paths import WorkspaceLayout
from src.server.database.workspace_file import (
    bulk_update_file_stamps,
    bulk_upsert_files,
    delete_file_rows,
    delete_gone_rows,
    delete_removed_files,
    manifest_clock,
    get_file_metadata_for_sync,
    get_workspace_total_size,
    path_fits_manifest,
    mark_files_backed_up,
    set_files_scan_mark,
    workspace_sync_lock,
)
from src.server.database.workspace import (
    computer_backs_up_files,
    files_restore_incomplete,
    workspace_owner,
)
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
from src.server.services.persistence.repos import hold_repos, is_transient, repo_of
from src.server.services.persistence.restore import folder_waits_on_owner
from src.server.services.persistence.sync_result import (
    SyncResult,
    UnsavedFile,
    UnsavedReason,
)
from src.server.services.persistence.transfer import (
    history_cap_bytes,
    scan_cap_bytes,
    PACK_CUTOFF,
    ScanEntry,
    ScanMark,
    ScanResult,
    ScanRules,
    is_deferred,
    scan_workspace,
)
from src.utils.storage import is_storage_enabled

logger = logging.getLogger(__name__)


async def list_sandbox_files(
    sandbox: Any,
    *,
    layout: WorkspaceLayout,
) -> dict[str, dict[str, Any]]:
    """Listing of regular files for the backup-status route, by size and mtime.

    Nothing is hashed: the route compares against the manifest by size and
    mtime, and it runs on every file-list refresh.
    """
    # A listing reports what is in the sandbox; whether a file is storable
    # is the sync's question, so nothing is withheld here.
    scan = await scan_workspace(
        sandbox, {}, max_file_bytes=None, layout=layout, hash_files=False, histories=False
    )
    work_dir = layout.workspace
    return {
        e.path: {
            "abs_path": f"{work_dir}/{e.path}",
            "file_name": os.path.basename(e.path),
            "file_size": e.size,
            "mtime": e.mtime_ns / 1e9,
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


async def sync_to_db(
    workspace_id: str, sandbox: Any, *, layout: WorkspaceLayout, routine: bool = False
) -> SyncResult:
    """
    Snapshot workspace files from the sandbox into the manifest.

    ``layout`` names the project folder to mirror. It is required, and its
    folder name is fenced against the row inside the prune statement: a scan
    of the wrong directory would otherwise read every sibling's file as this
    project's and delete the manifest it was meant to update.

    The sandbox walks and hashes its own tree; this side diffs the result
    against the manifest and moves only the bytes whose digest is not yet
    registered. Under ``direct`` transfer those bytes never pass through
    this process.

    Serialized per workspace across workers: the diff decides what to write
    from a read taken before the scan, so two overlapping passes would let
    the older one's upsert land last and reinstate what the newer pass had
    already recorded.

    ``routine`` is a backup the computer's switch governs (after a turn, at a
    stop, before a delete). It reads the switch here, under the lock, rather
    than where its pass was planned: a machine-wide pass runs for as long as
    its projects take, and the owner may turn backups off meanwhile.

    Returns:
        What the pass saved, and every file it could not
    """
    try:
        async with workspace_sync_lock(workspace_id) as conn:
            # Only an owner's machine can have its backups off.
            if (
                routine
                and not sandbox.recreatable
                and not await computer_backs_up_files(workspace_id, conn=conn)
            ):
                return SyncResult(backups_off=True)
            # A folder gone from its owner's machine, or emptied, would be
            # mirrored as deleted, pruning the files they may still ask back.
            if not sandbox.recreatable and await folder_waits_on_owner(
                workspace_id, sandbox, layout=layout, conn=conn
            ):
                return SyncResult(held=True)
            result = await _sync_locked(workspace_id, sandbox, conn, layout)
            # Last, under the same lock: a mark says everything changed before
            # it is saved, so it moves only once the writes above landed, the
            # prune ran, and nothing a retry could save was left behind. A pass
            # that withheld its prune must be repeated once the gate lifts,
            # and a mark would let the sweep skip that repeat. A file
            # unsaved for good (too large, path too long) does not hold it
            # back: rescanning cannot save it, and it is reported either way.
            if result.pruned and not result.errors:
                try:
                    await mark_files_backed_up(workspace_id, conn=conn)
                except Exception as e:
                    # Only what the owner is shown before a restore.
                    logger.warning(
                        f"Could not record the backup time of workspace "
                        f"{workspace_id}: {e}"
                    )
            if result.scan_mark and result.pruned and not result.errors:
                try:
                    await set_files_scan_mark(
                        workspace_id, result.scan_mark.as_json(), conn=conn
                    )
                except Exception as e:
                    # A hint the next sweep would have skipped on: without it
                    # that sweep reads the project as changed, which is safe.
                    # Failing here would report saved files as unsaved.
                    logger.warning(
                        f"Could not record the scan mark for workspace "
                        f"{workspace_id}: {e}"
                    )
            return result
    except Exception as e:
        logger.error(f"File sync failed for workspace {workspace_id}: {e}")
        raise


# The scan's errno is the sandbox's, which is always Linux; the server's own
# ``errno.ENAMETOOLONG`` differs on macOS. ENOENT is 2 everywhere.
_SANDBOX_ENAMETOOLONG = 36


def _unsaved(entry: ScanEntry, reason: UnsavedReason) -> UnsavedFile:
    return UnsavedFile(entry.path, reason, entry.size if entry.kind == "file" else None)


def _git_file(item: dict[str, Any]) -> ScanEntry:
    """A file the scan named without reading, as an entry ``hold_repos``
    can time."""
    return ScanEntry(
        str(item["path"]), "file", int(item.get("size") or 0), int(item["mtime_ns"]), 0, None, None, None
    )


@dataclass(frozen=True, slots=True)
class _Triage:
    """What a scan could not hand over as entries, and what the prune must
    keep because of it."""

    #: Present on the sandbox and never storable here (over the cap, a path
    #: the manifest cannot key): the prune reads them as present, and as
    #: files, or it takes the gap for a deletion.
    oversized_paths: set[str]
    #: The directories among them, whose children may still have rows.
    unkeyable_dirs: set[str]
    #: Rows of repositories held at their last complete backup. Not
    #: ``oversized_paths``: read as files, a held repository's directory
    #: would orphan every row under it.
    kept_rows: set[str]
    #: Paths the scan could not read, which make every absence suspect.
    read_errors: int


def _triage_scan(
    scan: ScanResult,
    existing: dict[str, dict[str, Any]],
    result: SyncResult,
    *,
    workspace_id: str,
    scan_cap: int | None,
    restore_incomplete: bool,
    withheld: frozenset[str],
    max_history_bytes: int,
) -> _Triage:
    """Files what ``scan`` could not take in ``result.unsaved`` (and
    ``result.root_missing``), and drops from ``scan.entries`` what no row may
    be written for this pass."""
    # Paths this deployment can never store. They are not entries, so nothing
    # downstream builds a row for them, but they are still present in the
    # sandbox: the prune has to see them or it reads the gap as a deletion
    # and removes the last good row the file had.
    # Git's own files the scan saw by name only: one over the transfer cap, or
    # in a history too large to read. Never stored, but whether git is
    # writing to the repository right now is told by them too.
    git_files = [
        _git_file(item)
        for item in (
            *scan.oversized,
            *(t for h in scan.whole_too_large for t in h.get("transient") or ()),
        )
        if is_transient(str(item.get("path")))
    ]
    oversized = [item for item in scan.oversized if not is_transient(str(item.get("path")))]
    oversized_paths = {
        str(item.get("path")) for item in oversized if item.get("path")
    }
    for item in oversized:
        result.unsaved.append(
            UnsavedFile(str(item.get("path")), "too_large", item.get("size"))
        )
        logger.warning(
            f"Cannot store {item.get('path')} in workspace {workspace_id}: "
            f"{item.get('size')} bytes exceeds this transfer path's "
            f"{scan_cap} byte limit. Its existing manifest row is kept; the "
            f"file itself is not backed up."
        )
    result.unsaved.extend(
        UnsavedFile(str(item["path"]), "history_too_large", item.get("size"))
        for item in scan.whole_too_large
    )
    result.unsaved.extend(_unsaved(e, "bad_name") for e in scan.unnamed)
    oversized_paths |= scan.unnamed_links
    if scan.unnamed:
        logger.warning(
            f"Workspace {workspace_id} has {len(scan.unnamed)} name(s) that are "
            f"not UTF-8, not backed up: {scan.unnamed[0].path[:200]}"
        )
    # A path the manifest cannot key would fail the batch insert and take
    # every other file in the pass down with it. It can still have a row: the
    # cutoff counts bytes and is stricter than the column, so a multibyte path
    # recorded earlier is protected from the prune like an oversized file.
    unkeyable = [e for e in scan.entries if not path_fits_manifest(e.path)]
    unkeyable_dirs = {e.path for e in unkeyable if e.kind == "dir"}
    if unkeyable:
        scan.entries = [e for e in scan.entries if path_fits_manifest(e.path)]
        oversized_paths |= {e.path for e in unkeyable}
        result.unsaved.extend(_unsaved(e, "path_too_long") for e in unkeyable)
        logger.warning(
            f"Workspace {workspace_id} has {len(unkeyable)} path(s) too long "
            f"to record, not backed up: {unkeyable[0].path[:200]}..."
        )
    read_errors = 0
    for item in scan.errors:
        # ENOENT on the root is a folder this sandbox generation never
        # had: a sibling that has not rejoined since the machine was
        # recreated. Nothing on the sandbox can be lost, so it is not an
        # unsaved file, but the scan saw nothing either, so no row may be
        # pruned on its account.
        if item.get("errno") == errno.ENOENT and item.get("path") == ".":
            logger.info(
                f"Workspace {workspace_id} has no folder on this sandbox; "
                f"nothing to mirror this pass"
            )
            result.root_missing = True
            continue
        # ENOENT below the root is a file removed between the listing
        # and the read: absent for the right reason, so it is neither
        # data at risk nor a reason to withhold pruning. Anything else
        # (EACCES, EIO) is data at risk, and a strict backup must refuse
        # to tear the sandbox down over it.
        # Inside a repository it is git at work (a ref packed, an object
        # repacked), so the repository waits for a pass that sees it settled.
        if item.get("errno") == errno.ENOENT and repo_of(str(item.get("path"))):
            result.unsaved.append(UnsavedFile(str(item.get("path")), "changed"))
            continue
        if item.get("errno") == errno.ENOENT:
            logger.info(
                f"{item.get('path')} in workspace {workspace_id} vanished "
                f"during the scan; treating it as deleted"
            )
            continue
        # Past PATH_MAX the sandbox cannot open the entry at all. Everything
        # under it is longer still, so no manifest row can exist there and
        # the prune loses nothing by going ahead.
        if item.get("errno") == _SANDBOX_ENAMETOOLONG:
            result.unsaved.append(
                UnsavedFile(str(item.get("path")), "path_too_long")
            )
            continue
        logger.warning(
            f"Could not read {item.get('path')} in workspace "
            f"{workspace_id}: {item.get('error')}"
        )
        result.unsaved.append(UnsavedFile(str(item.get("path")), "unreadable"))
        read_errors += 1

    # A repository this pass cannot take whole keeps the rows of the last pass
    # that could: none of its entries are stored, and its rows are kept from
    # the prune as if every file in it were still on the sandbox (they are,
    # only not readable as one state this pass).
    hold = hold_repos(
        result.unsaved,
        [*scan.entries, *git_files],
        now_ns=scan.started_ns,
        hold_all=restore_incomplete or read_errors > 0,
        # Rows the deferred restore has yet to bring back are kept, so new
        # ones beside them would store two states as one.
        pending={repo for p in withheld if (repo := repo_of(p))},
        max_bytes=max_history_bytes,
    )
    scan.entries = [e for e in scan.entries if hold.stores(e.path)]
    if hold.repos:
        result.unsaved = hold.unsaved
        # A history too large to carry is not kept, so nothing in it is
        # protected from the prune as a file still on the sandbox either.
        oversized_paths = {
            p for p in oversized_paths if not hold.covers(p) or repo_of(p) in hold.kept
        }
        logger.info(
            f"Workspace {workspace_id} stores none of {len(hold.repos)} git "
            f"repositor(ies) this pass: " + ", ".join(sorted(hold.repos))
        )
    return _Triage(
        oversized_paths=oversized_paths,
        unkeyable_dirs=unkeyable_dirs,
        kept_rows=hold.rows_of(existing),
        read_errors=read_errors,
    )


class _RepoNotWhole(Exception):
    def __init__(self, unsaved: list[UnsavedFile]) -> None:
        self.unsaved = unsaved


async def _persist_inline_by_repo(
    workspace_id: str,
    sandbox: Any,
    entries: list[ScanEntry],
    *,
    layout: WorkspaceLayout,
    conn: Any,
    gone: dict[str, list[str]],
    untouched_since: datetime,
) -> tuple[int, int, list[UnsavedFile]]:
    """``_persist_inline``, with each repository's files in one transaction.

    Inline rows are written as they are read, so a repository that fails
    halfway would otherwise keep the half that landed: a newer ref beside
    the old objects, naming a commit the backup lacks. Each repository's
    files are taken only as the scan saw them, and all or none are stored,
    together with removing its ``gone`` rows: committed apart, a pass cut
    short between the two kept the loose refs a ``pack-refs`` removed beside
    the new ``packed-refs``, and a loose ref outranks a packed one. A row the
    prune's fences keep holds the repository, rather than standing beside
    the new ones.
    """
    by_repo: dict[str | None, list[ScanEntry]] = {}
    for entry in entries:
        by_repo.setdefault(repo_of(entry.path), []).append(entry)
    synced, unsaved = await _persist_inline(
        workspace_id, sandbox, by_repo.pop(None, []), layout=layout, conn=conn
    )
    deleted = 0
    for repo, members in by_repo.items():
        try:
            async with conn.transaction():
                stored, missed = await _persist_inline(
                    workspace_id, sandbox, members, layout=layout, conn=conn, exact=True
                )
                if missed:
                    raise _RepoNotWhole(missed)
                if repo in gone:
                    removed = await delete_gone_rows(
                        workspace_id,
                        gone[repo],
                        walked_dir_name=layout.dir_name,
                        untouched_since=untouched_since,
                        conn=conn,
                    )
                    if removed < len(gone[repo]):
                        raise _RepoNotWhole([UnsavedFile(repo, "changed")])
                    deleted += removed
            synced += stored
        except _RepoNotWhole as e:
            unsaved.extend(e.unsaved)
    return synced, deleted, unsaved


async def _sync_locked(
    workspace_id: str, sandbox: Any, conn: Any, layout: WorkspaceLayout
) -> SyncResult:
    """One sync pass, holding this workspace's lock on ``conn``.

    Every manifest read and write rides that same session: acquiring a
    second pooled connection underneath a lock holder doubles the slots a
    sync costs and can stall on the pool's own timeout.
    """
    # Taken before the scan, on the database's clock: rows written by
    # anyone after this instant are newer than what this pass saw.
    started_at = await manifest_clock(conn=conn)
    existing = await get_file_metadata_for_sync(workspace_id, conn=conn)
    blobs_on = is_storage_enabled()
    rules = ScanRules.of(
        scan_cap_bytes(sandbox, blobs_on=blobs_on), history_cap_bytes(blobs_on=blobs_on)
    )
    scan_cap = rules.max_file_bytes
    result = SyncResult(max_file_bytes=scan_cap)
    scan = await scan_workspace(
        sandbox,
        prior_from_meta(existing),
        max_file_bytes=scan_cap,
        layout=layout,
        max_history_bytes=rules.max_history_bytes,
    )
    result.scan_mark = ScanMark.of(scan, sandbox, rules)
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
    # Files the deferred restore has not brought back yet are on their way,
    # not deleted; see DEFERRED_RESTORE_DIRS.
    withheld = (
        frozenset()
        if scan.deferred_restored
        else frozenset(p for p in existing if is_deferred(p))
    )

    triage = _triage_scan(
        scan,
        existing,
        result,
        workspace_id=workspace_id,
        scan_cap=scan_cap,
        restore_incomplete=not may_prune,
        withheld=withheld,
        max_history_bytes=rules.max_history_bytes,
    )
    oversized_paths, unkeyable_dirs, kept_rows = (
        triage.oversized_paths,
        triage.unkeyable_dirs,
        triage.kept_rows,
    )

    if result.root_missing:
        return result

    if triage.read_errors and may_prune:
        # A path the scan could not read is absent from the listing
        # for the wrong reason. The root itself failing looks like
        # an emptied workspace, and pruning on that erases the file
        # list; one unreadable directory is the same mistake in
        # miniature. Stale rows are the cheaper error.
        logger.warning(
            f"Scan of workspace {workspace_id} hit {triage.read_errors} read "
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
        else:
            result.deleted = await delete_removed_files(
                workspace_id,
                oversized_paths | withheld | kept_rows,
                walked_dir_name=layout.dir_name,
                untouched_since=started_at,
                conn=conn,
            )
            # A kept deferred row is a prune this pass could not make.
            result.pruned = not withheld
        # Rows kept for oversized files, or by a skipped prune, still count.
        result.total_size = await get_workspace_total_size(workspace_id, conn=conn)
        return result

    active_paths: set[str] = set(oversized_paths) | kept_rows
    rows: list[dict[str, Any]] = []
    stamp_updates: list[tuple[str, datetime, str | None]] = []
    needs_bytes: list[ScanEntry] = []
    pack_members: list[ScanEntry] = []
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
    non_dir_paths: set[str] = oversized_paths - unkeyable_dirs

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
                result.skipped += 1
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
            result.skipped += 1
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

    # Unsaved files reported from here on are the transfer's, after the scan.
    scanned_unsaved = len(result.unsaved)

    if needs_bytes:
        if blobs_on:
            persisted, unsaved = await _persist_blobs(
                user_id, workspace_id, sandbox, needs_bytes, layout=layout
            )
            rows.extend(persisted)
            result.unsaved.extend(unsaved)
        else:
            # Inline rows carry their own bytes, so they are written in
            # bounded batches rather than joining ``rows`` and being held
            # until the single upsert below.
            gone: dict[str, list[str]] = {}
            for path in existing if may_prune else ():
                repo = repo_of(path)
                if repo is not None and path not in active_paths and path not in withheld:
                    gone.setdefault(repo, []).append(path)
            inline_synced, inline_deleted, unsaved = await _persist_inline_by_repo(
                workspace_id,
                sandbox,
                needs_bytes,
                layout=layout,
                conn=conn,
                gone=gone,
                untouched_since=started_at,
            )
            result.synced += inline_synced
            result.deleted += inline_deleted
            result.unsaved.extend(unsaved)

    if pack_members:
        packed, unsaved, skipped = await _persist_packed(
            user_id,
            workspace_id,
            sandbox,
            pack_members,
            existing,
            may_prune=may_prune,
            # A held repository's packed rows are kept, not gone: counted as
            # left, they would rewrite the pack set on every pass it is held.
            withheld=withheld | kept_rows,
            layout=layout,
        )
        rows.extend(packed)
        result.unsaved.extend(unsaved)
        result.skipped += skipped

    # A file in a repository that failed to upload holds the repository too,
    # now that the rest of it is stored: its new rows are dropped (inline
    # ones were rolled back) and the old ones kept, which is the last
    # complete history.
    late = hold_repos(result.unsaved[scanned_unsaved:])
    if late.repos:
        result.unsaved[scanned_unsaved:] = late.unsaved
        rows = [r for r in rows if not late.covers(r["file_path"])]
        stamp_updates = [u for u in stamp_updates if not late.covers(u[0])]
        active_paths |= late.rows_of(existing)

    # One transaction from here to the prune: a pass cut short between a
    # repository's new rows and the removal of its old ones would store two
    # states as one (a ``pack-refs`` leaves loose refs that outrank the new
    # ``packed-refs``), which no file-by-file mirror can tell apart.
    async with conn.transaction():
        if stamp_updates:
            # A mode or mtime change on unchanged bytes is persisted only here.
            # Left uncounted, a failed write reads as a clean backup, and a strict
            # caller would tear the sandbox down with the new stamps unrecorded.
            try:
                # A savepoint: the failure is recorded and the pass goes on.
                async with conn.transaction():
                    await bulk_update_file_stamps(workspace_id, stamp_updates, conn=conn)
            except Exception as e:
                logger.error(
                    f"Stamp update failed for {len(stamp_updates)} files in "
                    f"workspace {workspace_id}: {e}"
                )
                result.unsaved.extend(
                    UnsavedFile(path, "failed") for path, _, _ in stamp_updates
                )

        if rows:
            result.synced += await bulk_upsert_files(
                workspace_id, rows, conn=conn
            )

        orphaned = [
            p for p in existing
            if p not in active_paths and _has_ancestor_in(p, non_dir_paths)
        ]
        if orphaned:
            result.deleted += await delete_file_rows(
                workspace_id, orphaned, conn=conn
            )

        if may_prune:
            deleted = await delete_removed_files(
                workspace_id,
                active_paths | withheld,
                walked_dir_name=layout.dir_name,
                untouched_since=started_at,
                conn=conn,
            )
            result.deleted += deleted
            result.pruned = not (withheld - active_paths)
        else:
            kept = len(set(existing) - active_paths)
            if kept:
                logger.warning(
                    f"Keeping {kept} manifest row(s) for workspace "
                    f"{workspace_id} whose files are absent from the sandbox: "
                    f"it is a known-incomplete mirror, so a missing file means "
                    f"a failed restore, not a user deletion. The next workspace "
                    f"start retries the restore and pruning resumes."
                )

    result.total_size = await get_workspace_total_size(
        workspace_id, conn=conn
    )

    logger.debug(
        f"File sync completed for workspace {workspace_id}: "
        f"synced={result.synced}, skipped={result.skipped}, "
        f"deleted={result.deleted}, errors={result.errors}, "
        f"hashed={scan.hashed}, reused={scan.reused}"
    )

    return result
