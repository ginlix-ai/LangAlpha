"""
Integration-style tests for the ``starting`` status routing in
``src/server/app/workspace_files.py``.

These pin the incident-day invariant: while a workspace is in the
intermediate ``starting`` state (lazy init in flight, or Phase 2 failed),
a concurrent ``GET /files`` call must route to the DB fallback. Before
Fix 1 the DB read side only checked ``stopped``/``stopping`` so a
concurrent request during lazy init went straight to live sandbox
acquisition and collided with the in-flight Daytona restore — 503 storm.

Plan item #8 (``investigate-backend-1-info-zesty-sketch.md``): request A
fails lazy init, request B calls ``/files`` while status is
``starting`` → B returns DB fallback, not 503.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.app.workspace_files.crud import list_workspace_files

# Every listing also carries the user's data files, read from their rows.
# These tests pin the routing; test_workspace_files_virtual.py pins those.
_DATA_FILES = "src.server.app.workspace_files.crud._virtual_files_in_scope"


def _workspace(ws_id: str, user_id: str, status: str) -> dict:
    return {
        "workspace_id": ws_id,
        "user_id": user_id,
        "status": status,
        "config": None,
        "sandbox_id": "sb-existing",
    }


@pytest.mark.asyncio
@patch(_DATA_FILES, new=AsyncMock(return_value=[]))
@patch("src.server.app.workspace_files.crud.owner_work_dir", return_value="/home/workspace")
@patch("src.server.app.workspace_files.crud.FilePersistenceService")
@patch("src.server.app.workspace_files.crud.db_get_workspace")
async def test_starting_status_routes_to_db_fallback(
    mock_get_ws, mock_fp, _mock_wd,
):
    """Happy path: status='starting' → handler returns the DB file tree
    with ``source: 'database'``, never touching the sandbox."""
    ws_id = "ws-starting"
    mock_get_ws.return_value = _workspace(ws_id, "user-1", status="starting")
    mock_fp.get_file_tree = AsyncMock(
        return_value=[
            {"path": "results/summary.md"},
            {"path": "data/daily.csv"},
        ]
    )

    result = await list_workspace_files(
        workspace_id=ws_id,
        x_user_id="user-1",
        path=".",
        include_system=False,
        pattern="**/*",
        wait_for_sandbox=False,
        auto_start=False,
    )

    assert result["source"] == "database"
    assert result["sandbox_ready"] is False
    assert set(result["files"]) == {"results/summary.md", "data/daily.csv"}
    mock_fp.get_file_tree.assert_awaited_once_with(ws_id)


@pytest.mark.asyncio
@patch(_DATA_FILES, new=AsyncMock(return_value=[]))
@patch("src.server.app.workspace_files.crud.owner_work_dir", return_value="/home/workspace")
@patch("src.server.app.workspace_files.crud.FilePersistenceService")
@patch("src.server.app.workspace_files.crud.db_get_workspace")
async def test_files_during_concurrent_failing_lazy_init(
    mock_get_ws, mock_fp, _mock_wd,
):
    """Plan item #8: A (``get_session_for_workspace`` with failing Phase 2)
    races against B (``list_workspace_files``). As long as the DB row reads
    ``status='starting'`` when B arrives, B must route to the DB fallback
    and not raise 503 — even if A is mid-failure."""
    ws_id = "ws-racing"
    mock_get_ws.return_value = _workspace(ws_id, "user-1", status="starting")
    mock_fp.get_file_tree = AsyncMock(return_value=[{"path": "results/x.txt"}])

    async def failing_request_a() -> Exception:
        """Simulate ``WorkspaceManager.get_session_for_workspace`` raising
        a SandboxTransientError from Phase 2 while B is reading /files."""
        await asyncio.sleep(0.005)
        raise SandboxTransientError("phase 2 init exhausted retries")

    async def request_b() -> dict:
        # B intentionally has no knowledge of A — it just reads /files.
        return await list_workspace_files(
            workspace_id=ws_id,
            x_user_id="user-1",
            path=".",
            include_system=False,
            pattern="**/*",
            wait_for_sandbox=False,
            auto_start=False,
        )

    outcomes = await asyncio.gather(
        failing_request_a(),
        request_b(),
        return_exceptions=True,
    )

    a_outcome, b_outcome = outcomes
    assert isinstance(a_outcome, SandboxTransientError)  # A failed, as expected
    assert isinstance(b_outcome, dict), f"B should not raise 503 / error: {b_outcome!r}"
    assert b_outcome["source"] == "database"
    assert b_outcome["files"] == ["results/x.txt"]


@pytest.mark.asyncio
@patch("src.server.database.workspace_file.get_workspace_total_size", new_callable=AsyncMock, return_value=7)
@patch("src.server.database.workspace_file.get_file_metadata_for_sync", new_callable=AsyncMock)
@patch("src.server.app.workspace_files.crud.db_get_workspace")
async def test_backup_status_of_a_stopped_workspace_lists_files_only(mock_get_ws, mock_meta, _size):
    """The sync metadata carries directory and symlink rows; the status is
    about files, and the running branch only ever sees files."""
    from src.server.app.workspace_files.crud import get_backup_status

    mock_get_ws.return_value = _workspace("ws-stopped", "user-1", status="stopped")
    mock_meta.return_value = {
        "data": {"kind": "directory", "file_size": 0},
        "link": {"kind": "symlink", "file_size": 0},
        "data/a.csv": {"kind": "file", "file_size": 7},
        "legacy.txt": {"file_size": 3},
    }

    result = await get_backup_status(workspace_id="ws-stopped", x_user_id="user-1")

    assert set(result["backed_up"]) == {"data/a.csv", "legacy.txt"}
    assert result["total_backed_up_size"] == 7


@pytest.mark.asyncio
async def test_mutation_rejects_shared_agent_root_after_canonicalization():
    from types import SimpleNamespace
    from fastapi import HTTPException
    from src.server.services.workspace_files import contained_target

    sandbox = SimpleNamespace(validate_path=lambda _path: True)
    with patch(
        "src.server.services.workspace_files.contained_sandbox_path",
        AsyncMock(return_value="/home/workspace/.agents/tools/docs/server.md"),
    ):
        with pytest.raises(HTTPException) as error:
            await contained_target(sandbox, "linked-doc.md", "/home/workspace/project")
    assert error.value.status_code == 404


@pytest.mark.asyncio
async def test_backup_scans_the_folder_read_under_the_folder_hold():
    """A settle on another worker can land the folder after the acquisition
    read the row. Scanned at the old path, it reads as missing, which the sync
    reports as a clean pass."""
    from contextlib import asynccontextmanager

    from src.server.app.workspace_files.crud import backup_workspace_files
    from src.server.services.persistence.sync_result import SyncResult

    row = {**_workspace("ws-renamed", "user-1", status="running"), "computer_root_dir": "/home/workspace"}
    events = []

    @asynccontextmanager
    async def hold(_workspace_id):
        events.append("hold")
        yield
        events.append("release")

    async def scan(_workspace_id, _sandbox, *, layout):
        events.append(f"sync {layout.workspace}")
        return SyncResult(synced=1)

    acquire = AsyncMock(return_value=(object(), {**row, "dir_name": "Research"}))
    sync = AsyncMock(side_effect=scan)
    with (
        patch("src.server.app.workspace_files.crud.db_get_workspace", AsyncMock(return_value=row)),
        patch("src.server.services.workspace_files.acquire_sandbox", acquire),
        patch("src.server.services.workspace_files.workspace_folder_in_use", hold),
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value={**row, "dir_name": "Macro"}),
        ),
        patch("src.server.app.workspace_files.crud.FilePersistenceService.sync_to_db", sync),
    ):
        result = await backup_workspace_files(workspace_id="ws-renamed", x_user_id="user-1")

    assert events == ["hold", "sync /home/workspace/Macro", "release"]
    assert result["synced"] == 1


@pytest.mark.asyncio
async def test_a_backup_that_found_the_folder_gone_answers_as_a_standing_offer():
    """The pass offered the restore instead of mirroring the emptied folder, so
    nothing was saved; a 200 with nothing synced would read as a backup made."""
    from fastapi import HTTPException

    from src.server.app.workspace_files.crud import backup_workspace_files
    from src.server.services.persistence.sync_result import SyncResult

    from contextlib import nullcontext

    row = {**_workspace("ws-1", "user-1", status="running"), "computer_root_dir": "/home/u"}
    with (
        patch("src.server.app.workspace_files.crud.db_get_workspace", AsyncMock(return_value=row)),
        patch(
            "src.server.app.workspace_files.crud.get_computer",
            AsyncMock(return_value={"computer_id": "comp-1", "files_backup": True}),
        ),
        patch(
            "src.server.services.workspace_files.workspace_folder_in_use",
            lambda _workspace_id: nullcontext(),
        ),
        patch(
            "src.server.services.workspace_files.acquire_sandbox",
            AsyncMock(return_value=(object(), {**row, "dir_name": "Research"})),
        ),
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value={**row, "dir_name": "Research"}),
        ),
        patch(
            "src.server.app.workspace_files.crud.FilePersistenceService.sync_to_db",
            AsyncMock(return_value=SyncResult(held=True)),
        ),
        pytest.raises(HTTPException) as refused,
    ):
        await backup_workspace_files(workspace_id="ws-1", x_user_id="user-1")

    assert refused.value.status_code == 409
    assert "Restore it from the backup" in refused.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("offered_at", "files_backup", "detail"),
    [
        (
            "2026-10-01T00:00:00Z",
            True,
            "This workspace's folder is missing on its computer. Restore it "
            "from the backup or keep it as it is first.",
        ),
        (None, False, "File backup is off for this computer."),
    ],
    ids=["restore-offered", "backups-off"],
)
async def test_a_backup_asked_for_by_hand_is_refused_before_the_machine_is_reached(
    offered_at, files_backup, detail
):
    """A pass over a folder that is gone prunes the backup the owner may yet
    restore, and one on a machine with backups off replaces the backup its
    owner chose to keep. Refused before the acquire, which would start the
    machine for nothing."""
    from fastapi import HTTPException

    from src.server.app.workspace_files.crud import backup_workspace_files

    row = {
        **_workspace("ws-1", "user-1", status="running"),
        "computer_id": "comp-1",
        "files_restore_offered_at": offered_at,
    }
    acquire = AsyncMock()
    sync = AsyncMock()
    with (
        patch("src.server.services.workspace_files.db_get_workspace", AsyncMock(return_value=row)),
        patch(
            "src.server.app.workspace_files.crud.get_computer",
            AsyncMock(return_value={"computer_id": "comp-1", "files_backup": files_backup}),
        ),
        patch("src.server.services.workspace_files.acquire_sandbox", acquire),
        patch("src.server.app.workspace_files.crud.FilePersistenceService.sync_to_db", sync),
        pytest.raises(HTTPException) as refused,
    ):
        await backup_workspace_files(workspace_id="ws-1", x_user_id="user-1")

    assert (refused.value.status_code, refused.value.detail) == (409, detail)
    acquire.assert_not_awaited()
    sync.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("moving", ["hold_times_out", "row_staged"])
async def test_backup_status_falls_back_when_the_folder_is_moving(moving):
    """A folder a settle is moving cannot be scanned yet, and the status is
    polled with every file-list refresh: it answers from the DB, as it does for
    a sandbox that is not ready, rather than failing the panel."""
    from contextlib import asynccontextmanager

    from src.server.app.workspace_files.crud import get_backup_status
    from src.server.database.workspace_folders import WorkspaceFolderMoving, moving_path

    row = {
        **_workspace("ws-moving", "user-1", status="running"),
        "computer_root_dir": "/home/workspace",
        "dir_name": "Research",
    }
    held_row = row if moving == "hold_times_out" else {**row, "dir_name": moving_path("ws-moving")}

    @asynccontextmanager
    async def hold(workspace_id):
        if moving == "hold_times_out":
            raise WorkspaceFolderMoving(workspace_id)
        yield

    manager = MagicMock()
    manager.get_instance.return_value.get_session_for_workspace = AsyncMock(
        return_value=SimpleNamespace(sandbox=object())
    )
    scan = AsyncMock(return_value={})
    with (
        patch("src.server.app.workspace_files.crud.db_get_workspace", AsyncMock(return_value=row)),
        patch("src.server.services.workspace_files.WorkspaceManager", manager),
        patch("src.server.services.workspace_files.workspace_folder_in_use", hold),
        patch("src.server.services.workspace_files.db_get_workspace", AsyncMock(return_value=held_row)),
        patch("src.server.app.workspace_files.crud.FilePersistenceService.list_sandbox_files", scan),
        patch(
            "src.server.database.workspace_file.get_file_metadata_for_sync",
            AsyncMock(return_value={"data/a.csv": {"kind": "file", "file_size": 7}}),
        ),
        patch("src.server.database.workspace_file.get_workspace_total_size", AsyncMock(return_value=7)),
    ):
        result = await get_backup_status(workspace_id="ws-moving", x_user_id="user-1")

    scan.assert_not_awaited()
    assert result == {
        "workspace_id": "ws-moving",
        "backed_up": ["data/a.csv"],
        "modified": [],
        "untracked": [],
        "total_backed_up_size": 7,
        "files_restore_incomplete": False,
        "files_backup": True,
        "files_restore_offered_at": None,
    }
