"""Contracts for the restore-completeness flag's control over manifest pruning.

``sync_to_db`` treats "in the manifest but not the sandbox" as a user deletion.
A restore that failed produces the identical signature, so something has to
tell the two apart, durably and across processes: the worker that observes the
failed restore is not the one that runs the next sync.

The flag lives in Postgres rather than in the sandbox's ``.file_sync_marker``
because it is not a fact about the sandbox: a restore usually fails per file on
something the sandbox is not party to — a blob object storage would not hand
back — and that sandbox would happily store a marker claiming otherwise. It
also has to outlive the sandbox, which the marker by design does not.

Verified live before these were written (partial restore, then sync, then
repair) against a real sandbox and bucket; these pin the decision table.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from datetime import datetime, timezone

import pytest

from ptc_agent.core.paths import SandboxLayout
from src.server.services.persistence import backup
from src.server.services.persistence.transfer import ScanEntry, ScanResult

WS = "ws-prune-gate"
PATH = "reports/gone.txt"
MTIME_NS = 1_700_000_000_000_000_000

ROOT = "/workspace"
DIR_NAME = "prune-ab12"
# The folder the scan walks. It rides into the prune statement as
# ``walked_dir_name``, which is what keeps a scan of the wrong directory from
# reading a sibling's files as this workspace's deletions.
LAYOUT = SandboxLayout.for_root(ROOT).for_workspace(DIR_NAME)


CLOCK = datetime(2026, 9, 3, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _clock():
    with patch("src.server.services.persistence.backup.manifest_clock", new=AsyncMock(return_value=CLOCK)):
        yield


@pytest.fixture(autouse=True)
def _sync_lock():
    """``sync_to_db`` serializes on a Postgres advisory lock; there is no DB here."""

    @asynccontextmanager
    async def _lock(_workspace_id):
        yield None

    with patch.object(backup, "workspace_sync_lock", _lock):
        yield


def _scan(*entries: ScanEntry, errors=None) -> ScanResult:
    return ScanResult(entries=list(entries), oversized=[], errors=errors or [], hashed=0, reused=len(entries))


def _sandbox() -> MagicMock:
    sandbox = MagicMock()
    sandbox.working_dir = ROOT
    sandbox.adownload_file_bytes = AsyncMock(return_value=None)
    return sandbox


# --- the empty-listing branch (highest consequence) ------------------------


async def _sync_with_empty_sandbox(incomplete):
    """``incomplete`` is the flag's value, or an exception to raise reading it."""
    flag = (
        AsyncMock(side_effect=incomplete)
        if isinstance(incomplete, Exception)
        else AsyncMock(return_value=incomplete)
    )
    with (
        patch(
            "src.server.services.persistence.backup.scan_workspace",
            new=AsyncMock(return_value=_scan()),
        ),
        patch(
            "src.server.services.persistence.backup.get_file_metadata_for_sync",
            new=AsyncMock(return_value={}),
        ),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=flag),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch(
            "src.server.services.persistence.backup.delete_removed_files",
            new=AsyncMock(return_value=7),
        ) as deleter,
        patch(
            "src.server.services.persistence.backup.get_workspace_total_size",
            new=AsyncMock(return_value=0),
        ),
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    return result, deleter


@pytest.mark.asyncio
async def test_empty_listing_while_flagged_deletes_nothing():
    """A restore that failed for every file leaves an empty sandbox. Pruning
    against it erases the entire manifest — the whole file list, not one row."""
    result, deleter = await _sync_with_empty_sandbox(True)
    deleter.assert_not_awaited()
    assert result.deleted == 0


@pytest.mark.asyncio
async def test_empty_listing_when_not_flagged_still_prunes():
    """A user who really did delete everything must still see it reflected."""
    result, deleter = await _sync_with_empty_sandbox(False)
    deleter.assert_awaited_once()
    assert result.deleted == 7


@pytest.mark.asyncio
async def test_unreadable_flag_suppresses_pruning():
    """The read can fail on its own. A stale row is recoverable; a deleted row
    whose file only existed in the manifest is not."""
    _, deleter = await _sync_with_empty_sandbox(RuntimeError("db down"))
    deleter.assert_not_awaited()


# --- the populated branch --------------------------------------------------


async def _sync_with_one_unchanged_file(incomplete: bool):
    """One file present and unchanged, one manifest row whose file is gone."""
    listing = _scan(
        ScanEntry("keep.txt", "file", 10, MTIME_NS, 0o644, "x", None, None)
    )
    existing = {
        "keep.txt": {
            "kind": "file",
            "file_size": 10,
            "mtime_ns": MTIME_NS,
            "content_hash": "x",
            "permissions": "0644",
        },
        PATH: {"kind": "file", "file_size": 5, "mtime_ns": MTIME_NS, "content_hash": "y"},
    }
    with (
        patch("src.server.services.persistence.backup.PACK_CUTOFF", -1),
        patch(
            "src.server.services.persistence.backup.scan_workspace",
            new=AsyncMock(return_value=listing),
        ),
        patch(
            "src.server.services.persistence.backup.files_restore_incomplete",
            new=AsyncMock(return_value=incomplete),
        ),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch(
            "src.server.services.persistence.backup.get_file_metadata_for_sync",
            new=AsyncMock(return_value=existing),
        ),
        patch(
            "src.server.services.persistence.backup.get_workspace_total_size",
            new=AsyncMock(return_value=10),
        ),
        patch(
            "src.server.services.persistence.backup.delete_removed_files",
            new=AsyncMock(return_value=1),
        ) as deleter,
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    return result, deleter


@pytest.mark.asyncio
async def test_flagged_workspace_keeps_the_row_for_a_missing_file():
    result, deleter = await _sync_with_one_unchanged_file(True)
    deleter.assert_not_awaited()
    assert result.deleted == 0
    assert result.skipped == 1


@pytest.mark.asyncio
async def test_unflagged_workspace_prunes_the_row_for_a_missing_file():
    result, deleter = await _sync_with_one_unchanged_file(False)
    deleter.assert_awaited_once()
    assert deleter.await_args.args[1] == {"keep.txt"}
    assert result.deleted == 1


async def _sync_with_one_restamped_file(incomplete: bool, stamp_failure=None):
    """One file whose bytes match the manifest and whose mode moved."""
    listing = _scan(
        ScanEntry("keep.txt", "file", 10, MTIME_NS, 0o600, "x", None, None)
    )
    existing = {
        "keep.txt": {
            "kind": "file",
            "file_size": 10,
            "mtime_ns": MTIME_NS,
            "content_hash": "x",
            "permissions": "0644",
        },
    }
    with (
        patch("src.server.services.persistence.backup.PACK_CUTOFF", -1),
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=incomplete)),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value=existing)),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=10)),
        patch("src.server.services.persistence.backup.bulk_upsert_files", new=AsyncMock(return_value=0)),
        patch("src.server.services.persistence.backup.bulk_update_file_stamps", new=AsyncMock(side_effect=stamp_failure)) as stamps,
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=0)),
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    return result, stamps


@pytest.mark.asyncio
async def test_a_stamp_write_that_fails_is_counted_as_an_error():
    """The moved mode is persisted only by that write. Uncounted, a strict
    stop reads the backup as clean and tears the sandbox down with the new
    mode unrecorded."""
    result, _ = await _sync_with_one_restamped_file(False, RuntimeError("db away"))
    assert result.errors == 1


@pytest.mark.asyncio
async def test_flagged_workspace_leaves_a_moved_stamp_unrecorded():
    """A restore that placed the file but failed its chmod leaves a moved
    stamp. Recording it makes the wrong mode the one the retry restores."""
    result, stamps = await _sync_with_one_restamped_file(True)
    stamps.assert_not_awaited()
    assert result.skipped == 1


@pytest.mark.asyncio
async def test_unflagged_workspace_records_a_moved_stamp():
    result, stamps = await _sync_with_one_restamped_file(False)
    stamps.assert_awaited_once()
    assert stamps.await_args.args[1][0][0] == "keep.txt"


# --- the prune fence and the read-error gate --------------------------------


async def _sync(listing: ScanResult, incomplete=False):
    with (
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value={PATH: {"kind": "file", "content_hash": "x", "file_size": 1, "mtime_ns": MTIME_NS}})),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=incomplete)),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch("src.server.services.persistence.backup.bulk_upsert_files", new=AsyncMock(return_value=0)),
        patch("src.server.services.persistence.backup.bulk_update_file_stamps", new=AsyncMock()),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=1)) as deleter,
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    return result, deleter


@pytest.mark.asyncio
async def test_prune_is_fenced_to_rows_untouched_since_the_scan_began():
    """An overlapping sync that upserted a row after this scan started must
    not have that row pruned by this pass, which never saw the file."""
    result, deleter = await _sync(_scan())
    deleter.assert_awaited_once()
    assert deleter.await_args.kwargs["untouched_since"] == CLOCK
    # And fenced to the folder this scan actually walked.
    assert deleter.await_args.kwargs["walked_dir_name"] == DIR_NAME
    assert result.deleted == 1


@pytest.mark.asyncio
async def test_unreadable_root_does_not_wipe_the_manifest():
    """A root the scan could not read yields an empty listing, which is the
    same signature as an emptied workspace. It must keep every row."""
    result, deleter = await _sync(_scan(errors=[{"path": ".", "error": "EIO"}]))
    deleter.assert_not_awaited()
    assert result.deleted == 0 and result.errors == 1


@pytest.mark.asyncio
async def test_any_scan_read_error_withholds_pruning():
    entry = ScanEntry(path="reports", kind="dir", size=0, mtime_ns=MTIME_NS, mode=0o755, sha256=None, symlink_target=None, is_binary=None)
    result, deleter = await _sync(_scan(entry, errors=[{"path": "reports/q3", "error": "EACCES"}]))
    deleter.assert_not_awaited()
    assert result.errors == 1


@pytest.mark.asyncio
async def test_a_file_that_vanished_mid_scan_does_not_withhold_pruning():
    """ENOENT below the root is a file removed between the listing and the
    read: absent for the right reason. It is not data at risk either."""
    result, deleter = await _sync(_scan(errors=[{"path": "reports/q3", "error": "ENOENT", "errno": 2}]))
    deleter.assert_awaited_once()
    assert result.errors == 0


@pytest.mark.asyncio
async def test_a_missing_root_still_withholds_pruning():
    """ENOENT on the root is a folder this sandbox never had, not a vanished
    file: nothing there can be lost, so it is not an unsaved file, but the
    empty listing it yields must not prune anything."""
    result, deleter = await _sync(_scan(errors=[{"path": ".", "error": "ENOENT", "errno": 2}]))
    deleter.assert_not_awaited()
    assert result.deleted == 0 and result.errors == 0
    assert result.root_missing is True and result.unsaved == []


# --- a directory that stopped being one -------------------------------------


async def _sync_dir_turned_symlink(incomplete: bool):
    """``a/`` with a child in the manifest; the sandbox now has symlink ``a``."""
    listing = _scan(
        ScanEntry("a", "symlink", 0, MTIME_NS, 0o777, None, "elsewhere", None)
    )
    existing = {
        "a": {"kind": "dir", "file_size": 0, "mtime_ns": MTIME_NS, "permissions": "0755"},
        "a/child": {"kind": "file", "content_hash": "x", "file_size": 1, "mtime_ns": MTIME_NS},
        "ab": {"kind": "file", "content_hash": "y", "file_size": 1, "mtime_ns": MTIME_NS},
    }
    with (
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value=existing)),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=incomplete)),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch("src.server.services.persistence.backup.bulk_upsert_files", new=AsyncMock(return_value=1)),
        patch("src.server.services.persistence.backup.bulk_update_file_stamps", new=AsyncMock()),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch("src.server.services.persistence.backup.delete_file_rows", new=AsyncMock(return_value=1)) as rows_deleter,
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=1)) as deleter,
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    return result, rows_deleter, deleter


@pytest.mark.asyncio
async def test_a_flagged_workspace_still_drops_children_of_a_decayed_directory():
    """The withheld prune keeps ``ab`` (absent for an unknown reason) but
    ``a/child`` cannot exist under a symlink, and a restore that kept it
    would create a directory where the symlink has to land."""
    result, rows_deleter, deleter = await _sync_dir_turned_symlink(True)
    deleter.assert_not_awaited()
    rows_deleter.assert_awaited_once()
    assert rows_deleter.await_args.args[1] == ["a/child"]
    assert result.deleted == 1


@pytest.mark.asyncio
async def test_an_unflagged_workspace_counts_both_prunes():
    result, rows_deleter, deleter = await _sync_dir_turned_symlink(False)
    rows_deleter.assert_awaited_once()
    deleter.assert_awaited_once()
    assert result.deleted == 2


async def _sync_legacy_child_under_new_symlink():
    """A manifest written before directories had rows: ``a/child`` exists,
    ``a`` has no row, and the sandbox now has symlink ``a``."""
    listing = _scan(
        ScanEntry("a", "symlink", 0, MTIME_NS, 0o777, None, "elsewhere", None)
    )
    existing = {
        "a/child": {"kind": "file", "content_hash": "x", "file_size": 1, "mtime_ns": MTIME_NS},
        "a/sub/deep": {"kind": "file", "content_hash": "y", "file_size": 1, "mtime_ns": MTIME_NS},
        "ab/child": {"kind": "file", "content_hash": "z", "file_size": 1, "mtime_ns": MTIME_NS},
    }
    with (
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value=existing)),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=True)),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch("src.server.services.persistence.backup.bulk_upsert_files", new=AsyncMock(return_value=1)),
        patch("src.server.services.persistence.backup.bulk_update_file_stamps", new=AsyncMock()),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch("src.server.services.persistence.backup.delete_file_rows", new=AsyncMock(return_value=2)) as rows_deleter,
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=1)) as deleter,
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    return result, rows_deleter, deleter


@pytest.mark.asyncio
async def test_children_with_no_parent_row_are_still_dropped_under_a_new_symlink():
    result, rows_deleter, deleter = await _sync_legacy_child_under_new_symlink()
    deleter.assert_not_awaited()
    assert sorted(rows_deleter.await_args.args[1]) == ["a/child", "a/sub/deep"]
    assert result.deleted == 2


# --- paths the manifest cannot key -----------------------------------------


@pytest.mark.asyncio
async def test_a_path_too_long_to_record_is_unsaved_and_never_written():
    """One such path used to fail the batch insert with "value too long for
    type character varying(1024)", taking every other file in the pass down
    and leaving a strict backup with no file to name."""
    deep = "d/" * 600
    listing = _scan(
        ScanEntry("reports", "dir", 4096, MTIME_NS, 0o755, None, None, None),
        ScanEntry(deep.rstrip("/"), "dir", 4096, MTIME_NS, 0o755, None, None, None),
        ScanEntry(deep + "f.txt", "file", 3, MTIME_NS, 0o644, "f", None, None),
    )
    with (
        patch("src.server.services.persistence.backup.PACK_CUTOFF", -1),
        patch("src.server.services.persistence.backup.is_storage_enabled", return_value=False),
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value={})),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=3)),
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=0)),
        patch("src.server.services.persistence.backup.bulk_upsert_files", new=AsyncMock(return_value=1)) as upsert,
        patch("src.server.services.persistence.blobs.bulk_upsert_files", new=AsyncMock(return_value=0)),
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)

    written = [row["file_path"] for call in upsert.await_args_list for row in call.args[1]]
    assert written == ["reports"]
    assert sorted((f.path, f.reason, f.size) for f in result.unsaved) == [
        (deep.rstrip("/"), "path_too_long", None),
        (deep + "f.txt", "path_too_long", 3),
    ]
    assert result.oversized == 2 and result.errors == 0


@pytest.mark.asyncio
async def test_a_path_over_the_byte_cutoff_keeps_the_row_it_already_has():
    """The cutoff counts bytes and the column counts characters, so a
    multibyte path can have a row from before the cutoff. Dropping it from
    the listing must not read as a deletion and prune that row."""
    wide = "文" * 700 + ".txt"
    listing = _scan(
        ScanEntry("keep.txt", "file", 10, MTIME_NS, 0o644, "x", None, None),
        ScanEntry(wide, "file", 10, MTIME_NS, 0o644, "w", None, None),
    )
    with (
        patch("src.server.services.persistence.backup.PACK_CUTOFF", -1),
        patch("src.server.services.persistence.backup.is_storage_enabled", return_value=False),
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value={})),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=10)),
        patch("src.server.services.persistence.backup._persist_inline", new=AsyncMock(return_value=(1, []))),
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=0)) as prune,
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)

    assert wide in prune.await_args.args[1]
    assert [(f.path, f.reason) for f in result.unsaved] == [(wide, "path_too_long")]


def test_the_manifest_bound_counts_bytes_as_well_as_characters():
    from src.server.database.workspace_file import path_fits_manifest

    assert path_fits_manifest("a" * 1024)
    assert not path_fits_manifest("a" * 1025)
    # 700 CJK characters are well under 1024 but 2100 bytes of key.
    assert not path_fits_manifest("文" * 700)


@pytest.mark.asyncio
async def test_a_directory_past_path_max_is_path_too_long_and_does_not_stop_the_prune():
    """The sandbox cannot even open it (Linux ENAMETOOLONG). Reported as
    unreadable, it told the user to check permissions and held every
    deletion back, though nothing under it could ever have had a row."""
    listing = _scan(
        ScanEntry("keep.txt", "file", 10, MTIME_NS, 0o644, "x", None, None),
        errors=[{"path": "deep/" + "a" * 4200, "error": "File name too long", "errno": 36}],
    )
    existing = {
        "keep.txt": {"kind": "file", "file_size": 10, "mtime_ns": MTIME_NS, "content_hash": "x", "permissions": "0644"},
        PATH: {"kind": "file", "file_size": 5, "mtime_ns": MTIME_NS, "content_hash": "y"},
    }
    with (
        patch("src.server.services.persistence.backup.PACK_CUTOFF", -1),
        patch("src.server.services.persistence.backup.scan_workspace", new=AsyncMock(return_value=listing)),
        patch("src.server.services.persistence.backup.files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch("src.server.services.persistence.backup.workspace_owner", new=AsyncMock(return_value="user-prune")),
        patch("src.server.services.persistence.backup.get_file_metadata_for_sync", new=AsyncMock(return_value=existing)),
        patch("src.server.services.persistence.backup.get_workspace_total_size", new=AsyncMock(return_value=10)),
        patch("src.server.services.persistence.backup.delete_removed_files", new=AsyncMock(return_value=1)) as deleter,
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)

    assert [f.reason for f in result.unsaved] == ["path_too_long"]
    deleter.assert_awaited_once()


# --- a restore waiting on its owner's choice --------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("recreatable", [False, True], ids=["owners", "rebuilt"])
async def test_a_folder_waiting_on_its_owner_holds_the_whole_pass(recreatable):
    """The folder is gone or emptied, so a pass over it reads every file as
    deleted and prunes the backup the owner may still ask for. Not even the
    scan runs. Every backup asks, under its lock, as the owner may empty the
    folder long after the attach looked. A machine the server rebuilds lays
    its folders again unasked, so it is never asked."""
    sandbox = _sandbox()
    sandbox.recreatable = recreatable
    waits = AsyncMock(return_value=True)
    scan = AsyncMock(return_value=_scan())
    with (
        patch.object(backup, "folder_waits_on_owner", waits),
        patch.object(backup, "scan_workspace", scan),
        patch.object(backup, "get_file_metadata_for_sync", AsyncMock(return_value={})) as manifest,
        patch.object(backup, "files_restore_incomplete", AsyncMock(return_value=False)),
        patch.object(backup, "workspace_owner", AsyncMock(return_value="user-prune")),
        patch.object(backup, "get_workspace_total_size", AsyncMock(return_value=0)),
        patch.object(backup, "delete_removed_files", AsyncMock(return_value=0)) as deleter,
        patch.object(backup, "mark_files_backed_up", AsyncMock()) as stamp,
    ):
        result = await backup.sync_to_db(WS, sandbox, layout=LAYOUT)

    if recreatable:
        waits.assert_not_awaited()
        scan.assert_awaited_once()
        return
    waits.assert_awaited_once_with(WS, sandbox, layout=LAYOUT, conn=None)
    assert result.held is True
    assert result.pruned is False
    scan.assert_not_awaited()
    manifest.assert_not_awaited()
    deleter.assert_not_awaited()
    stamp.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("routine", "recreatable", "reads"),
    [(True, False, False), (False, False, True), (True, True, True)],
    ids=["routine", "strict", "rebuilt"],
)
async def test_a_routine_pass_reads_its_computers_switch_under_the_lock(
    routine, recreatable, reads
):
    """A machine-wide pass reaches its later projects long after it was
    planned, and the owner may turn backups off meanwhile: each project's
    pass asks again, so none starts once the switch is off. A move, a
    duplicate or a downgrade copies the files whatever the switch says, and
    a machine the server rebuilds has no switch."""
    sandbox = _sandbox()
    sandbox.recreatable = recreatable
    switch = AsyncMock(return_value=False)
    scan = AsyncMock(return_value=_scan())
    with (
        patch.object(backup, "computer_backs_up_files", switch),
        patch.object(backup, "folder_waits_on_owner", AsyncMock(return_value=False)),
        patch.object(backup, "scan_workspace", scan),
        patch.object(backup, "get_file_metadata_for_sync", AsyncMock(return_value={})),
        patch.object(backup, "files_restore_incomplete", AsyncMock(return_value=False)),
        patch.object(backup, "workspace_owner", AsyncMock(return_value="user-prune")),
        patch.object(backup, "get_workspace_total_size", AsyncMock(return_value=0)),
        patch.object(backup, "delete_removed_files", AsyncMock(return_value=0)) as deleter,
        patch.object(backup, "mark_files_backed_up", AsyncMock()) as stamp,
    ):
        result = await backup.sync_to_db(WS, sandbox, layout=LAYOUT, routine=routine)

    assert result.backups_off is not reads
    assert scan.await_count == (1 if reads else 0)
    if not reads:
        switch.assert_awaited_once_with(WS, conn=None)
        deleter.assert_not_awaited()
        stamp.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("incomplete", "stamped"), [(False, True), (True, False)], ids=["pruned", "withheld"]
)
async def test_only_a_complete_pass_stamps_the_backup_time(incomplete, stamped):
    """The time is what the owner is shown before choosing to restore, so it
    names a pass that mirrored the folder whole; a withheld prune left rows
    for files the folder no longer holds."""
    with patch.object(backup, "mark_files_backed_up", AsyncMock()) as stamp:
        await _sync_with_empty_sandbox(incomplete)

    assert stamp.await_count == (1 if stamped else 0)

