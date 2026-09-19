"""One computer, several workspace folders: what each transfer op is rooted at.

The walk root, the sync marker and the relay staging area all moved from the
computer root into the folder a workspace owns. The pack directory did not:
chunks are the machine's own scratch and stay under the computer's
``_internal``, which is the one place the scan never walks.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout
from src.server.services.persistence import backup, restore, transfer

ROOT = "/home/workspace"
DIR_A = "alpha-1111"
DIR_B = "beta-2222"


def _layout(dir_name: str = DIR_A):
    """The folder a project owns on the computer at ``ROOT``.

    An empty name is the root owner: the one project on an unsplit computer
    whose folder is the machine itself.
    """
    return SandboxLayout.for_root(ROOT).for_workspace(dir_name)


def _sandbox() -> MagicMock:
    sb = MagicMock()
    sb.working_dir = ROOT
    sb.config.sandbox.provider = "daytona"
    return sb


def _manager() -> MagicMock:
    manager = MagicMock()
    manager.config.to_core_config.return_value = SimpleNamespace(
        filesystem=SimpleNamespace(working_directory=ROOT)
    )
    return manager


def _captured(op_mock, op: str) -> dict:
    """The spec the server handed the runtime for ``op``."""
    for call in op_mock.await_args_list:
        if call.args[1] == op:
            return call.args[2]
    raise AssertionError(f"no {op} op was run")


# --- walk root ------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("dir_name,expected", [(DIR_A, f"{ROOT}/{DIR_A}"), ("", ROOT)])
async def test_the_scan_walks_the_workspace_folder_not_the_computer(
    dir_name, expected
):
    """A named folder is the walk root; only the root owner walks the machine."""
    run = AsyncMock(return_value={"entries": []})
    with patch.object(transfer, "run_transfer_op", run):
        await transfer.scan_workspace(
            _sandbox(), {}, max_file_bytes=10, layout=_layout(dir_name)
        )
    assert _captured(run, "scan")["root"] == expected


@pytest.mark.asyncio
async def test_two_folders_on_one_computer_are_scanned_separately():
    """A walk from the computer root would have each workspace claim the
    others' files and prune their manifests on the next sync."""
    run = AsyncMock(return_value={"entries": []})
    sb = _sandbox()
    with patch.object(transfer, "run_transfer_op", run):
        await transfer.scan_workspace(sb, {}, max_file_bytes=10, layout=_layout(DIR_A))
        await transfer.scan_workspace(sb, {}, max_file_bytes=10, layout=_layout(DIR_B))
    roots = [c.args[2]["root"] for c in run.await_args_list]
    assert roots == [f"{ROOT}/{DIR_A}", f"{ROOT}/{DIR_B}"]


@pytest.mark.asyncio
async def test_pull_and_push_are_rooted_at_the_folder_and_pack_at_the_computer():
    run = AsyncMock(return_value={"results": {}})
    items = [{"path": "a.txt", "sha256": "d", "size": 1, "url": "u", "headers": {}}]
    with patch.object(transfer, "run_transfer_op", run):
        await transfer.push_direct(_sandbox(), items, layout=_layout())
        await transfer.pull_direct(_sandbox(), items, layout=_layout())
    for op in ("push", "pull"):
        spec = _captured(run, op)
        assert spec["root"] == f"{ROOT}/{DIR_A}"
        # The chunk staging area is the machine's, shared by every folder.
        assert spec["pack_root"] == ROOT


@pytest.mark.asyncio
async def test_the_pack_op_keeps_its_chunks_at_the_computer_root():
    run = AsyncMock(return_value={"chunks": [], "changed": []})
    with patch.object(transfer, "run_transfer_op", run):
        await transfer.pack_direct(
            _sandbox(), [{"path": "a.txt", "sha256": "d", "size": 1}], layout=_layout()
        )
    spec = _captured(run, "pack")
    assert spec["root"] == f"{ROOT}/{DIR_A}"
    assert spec["pack_root"] == ROOT
    # Kept for a warm sandbox still running a runtime that predates pack_root.
    assert spec["out_dir"] == transfer.PACK_DIR


@pytest.mark.asyncio
async def test_unlink_carries_both_roots_so_a_chunk_path_still_resolves():
    run = AsyncMock(return_value={"removed": 1})
    with patch.object(transfer, "run_transfer_op", run):
        await transfer.unlink_direct(
            _sandbox(), [f"{transfer.PACK_DIR}/chunk-x"], layout=_layout()
        )
    spec = _captured(run, "unlink")
    assert spec["root"] == f"{ROOT}/{DIR_A}"
    assert spec["pack_root"] == ROOT


# --- the sync marker ------------------------------------------------------


def test_the_sync_marker_lives_in_the_folder_it_speaks_for():
    """A marker at the computer root answers for every workspace on it, so the
    first restore to finish would leave every sibling empty."""
    assert restore._sync_marker_path(_layout(DIR_A)) == (
        f"{ROOT}/{DIR_A}/{transfer.SYNC_MARKER_NAME}"
    )
    assert restore._sync_marker_path(_layout(DIR_B)) == (
        f"{ROOT}/{DIR_B}/{transfer.SYNC_MARKER_NAME}"
    )
    # The root owner's folder is the machine, so its marker sits at the root.
    assert restore._sync_marker_path(_layout("")) == (
        f"{ROOT}/{transfer.SYNC_MARKER_NAME}"
    )


@pytest.mark.asyncio
async def test_a_siblings_marker_does_not_answer_for_this_workspace():
    """B's restore probes B's folder even though A's marker is already there."""
    present = {f"{ROOT}/{DIR_A}/{transfer.SYNC_MARKER_NAME}": b"2026-01-01"}
    sandbox = MagicMock()
    sandbox.working_dir = ROOT
    sandbox.sandbox_id = "sb-1"
    sandbox.adownload_file_bytes = AsyncMock(side_effect=lambda p: present.get(p))
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)

    with (
        patch.object(
            restore, "get_files_for_workspace", AsyncMock(return_value=[{"x": 1}])
        ),
        patch.object(restore, "restore_to_sandbox", AsyncMock()) as restored,
        patch.object(restore, "files_restore_incomplete", AsyncMock(return_value=False)),
    ):
        await restore.maybe_restore("ws-b", sandbox, layout=_layout(DIR_B))

    sandbox.adownload_file_bytes.assert_awaited_once_with(
        f"{ROOT}/{DIR_B}/{transfer.SYNC_MARKER_NAME}"
    )
    restored.assert_awaited_once()
    assert restored.await_args.kwargs["layout"] == _layout(DIR_B)


@pytest.mark.asyncio
async def test_a_workspace_with_its_own_marker_is_not_restored_again():
    present = {f"{ROOT}/{DIR_A}/{transfer.SYNC_MARKER_NAME}": b"2026-01-01"}
    sandbox = MagicMock()
    sandbox.working_dir = ROOT
    sandbox.sandbox_id = "sb-1"
    sandbox.adownload_file_bytes = AsyncMock(side_effect=lambda p: present.get(p))

    with (
        patch.object(restore, "restore_to_sandbox", AsyncMock()) as restored,
        patch.object(restore, "files_restore_incomplete", AsyncMock(return_value=False)),
    ):
        await restore.maybe_restore("ws-a", sandbox, layout=_layout(DIR_A))

    restored.assert_not_awaited()


@pytest.mark.asyncio
async def test_relayed_bytes_stage_inside_the_folder_the_scan_walks():
    """The ``.wsfiles-`` prefix is reserved at the root of the walk, which is
    now the workspace folder, so a staged file outside it would be recorded as
    a sibling's user file on the next backup."""
    sandbox = MagicMock()
    sandbox.working_dir = ROOT
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)
    with patch.object(restore, "resolve_file_bytes", AsyncMock(return_value=b"hi")):
        staged = await restore._stage_relayed_file(
            "user-1", sandbox, {"file_path": "a.txt"}, _layout()
        )
    assert staged is not None
    path = sandbox.aupload_file_bytes.await_args.args[0]
    assert path.startswith(f"{ROOT}/{DIR_A}/.wsfiles-relay-")


# --- the serve root -------------------------------------------------------


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"computer_id": "c-1", "dir_name": DIR_A}, f"{ROOT}/{DIR_A}"),
        ({"computer_id": "c-1", "dir_name": DIR_B}, f"{ROOT}/{DIR_B}"),
    ],
)
def test_the_serve_root_is_the_workspaces_own_folder(row, expected):
    from src.server.app.workspace_files import _shared

    assert _shared.work_dir_for({"workspace_id": "ws-1", **row}, manager=_manager()) == expected


@pytest.mark.parametrize(
    "row",
    [
        # Half-bound: the bind committed the machine and not the folder.
        {"computer_id": "c-1", "dir_name": None},
        # Unbound: migration 046 leaves a row that shared a sandbox with a
        # sibling on the pre-computer path, so the sandbox this row still
        # names is a machine those siblings are already laid out on.
        {"computer_id": None, "dir_name": None},
    ],
)
def test_a_row_that_names_no_folder_is_refused_rather_than_widened(row):
    """A workspace that names no folder cannot be served: the only wider
    answer available is its neighbours' files. The route turns that into a
    503, because the placement is a fact the row is expected to carry, not a
    workspace that has no files."""
    from fastapi import HTTPException

    from src.server.app.workspace_files import _shared
    from src.server.services.workspace_layout import WorkspaceLayoutUnavailable

    unplaced = {"workspace_id": "ws-1", **row}
    with pytest.raises(WorkspaceLayoutUnavailable):
        _shared.work_dir_for(unplaced, manager=_manager())
    with pytest.raises(HTTPException) as raised:
        _shared.owner_work_dir(unplaced, manager=_manager())
    assert raised.value.status_code == 503


# --- the folder reaches the ops through every entry point -----------------


@pytest.mark.asyncio
async def test_a_backup_scans_the_folder_the_caller_named():
    with (
        patch.object(backup, "workspace_sync_lock") as lock,
        patch.object(backup, "_sync_locked", AsyncMock(return_value={})) as locked,
    ):
        lock.return_value.__aenter__ = AsyncMock(return_value="conn")
        lock.return_value.__aexit__ = AsyncMock(return_value=False)
        await backup.sync_to_db("ws-a", _sandbox(), layout=_layout())
    assert locked.await_args.args[3] == _layout()


@pytest.mark.asyncio
async def test_the_backup_status_listing_is_rooted_at_the_folder():
    scan = AsyncMock(
        return_value=transfer.ScanResult(
            entries=[
                transfer.ScanEntry("report.html", "file", 3, 1, 0o644, "d", None, False)
            ],
            oversized=[],
            errors=[],
            hashed=1,
            reused=0,
        )
    )
    with patch.object(backup, "scan_workspace", scan):
        listed = await backup.list_sandbox_files(_sandbox(), layout=_layout())
    assert scan.await_args.kwargs["layout"] == _layout()
    assert listed["report.html"]["abs_path"] == f"{ROOT}/{DIR_A}/report.html"
