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
@patch("src.server.app.workspace_files.backup_reads.FilePersistenceService")
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
@patch("src.server.app.workspace_files.backup_reads.FilePersistenceService")
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
@patch("src.server.database.workspace_file.get_workspace_total_size", new_callable=AsyncMock, return_value=0)
@patch("src.server.database.workspace_file.get_file_metadata_for_sync", new_callable=AsyncMock, return_value={})
@patch("src.server.app.workspace_files.crud._computer_backs_up_files", new_callable=AsyncMock, return_value=False)
@patch("src.server.app.workspace_files.crud.acquire_sandbox_to_change")
@patch("src.server.app.workspace_files.crud.db_get_workspace")
async def test_backup_status_with_backup_off_never_walks_the_folder(mock_get_ws, acquire, *_):
    """The file panel asks after every list; with nothing to save, the walk
    would only cross the owner's link."""
    from src.server.app.workspace_files.crud import get_backup_status

    mock_get_ws.return_value = _workspace("ws-off", "user-1", status="running")

    result = await get_backup_status(workspace_id="ws-off", x_user_id="user-1")

    assert result["files_backup"] is False
    assert (result["backed_up"], result["modified"], result["untracked"]) == ([], [], [])
    acquire.assert_not_called()


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


def _mac_row(ws_id: str, *, tunnel: str | None = "tun-1") -> dict:
    """A running workspace on the owner's own machine, as ``get_workspace``
    reads it: the computer's id, kind and provider config joined in. With a
    ``tunnel`` the machine connects out through the gateway; without one the
    server dials it directly."""
    config = {"host": "mac-mini.local", "user": "me"}
    if tunnel:
        config["tunnel"] = tunnel
    return {
        **_workspace(ws_id, "user-1", status="running"),
        "computer_id": "comp-mac",
        "computer_root_dir": "/Users/me/langalpha",
        "computer_kind": "ssh",
        "computer_provider_config": config,
    }


def _tunnel_down() -> AsyncMock:
    """The one Postgres read that finds a machine's tunnel down."""
    return AsyncMock(return_value=True)


def _away_machine_manager() -> MagicMock:
    """A workspace manager whose acquisition finds the owner's machine's tunnel down."""
    from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

    manager = MagicMock()
    manager.get_instance.return_value.get_session_for_workspace = AsyncMock(
        side_effect=SSHConnectError("not_connected", "mac-mini is not connected.")
    )
    return manager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backs_up", "has_backup"),
    [(True, True), (False, True), (True, False)],
    ids=["backup-kept", "backups-off", "no-backup"],
)
@pytest.mark.parametrize("tunnel", ["tun-1", None], ids=["tunneled", "dialed"])
async def test_a_running_workspace_whose_machine_is_away_lists_its_backup(
    backs_up, has_backup, tunnel
):
    """A sleeping machine's workspace still reads as running, so the stopped
    check never routes it to the backup. The machine not answering does,
    read-only and marked, where a backup is kept; with none, the 503 and its
    code stand. A tunnel that is down is known from Postgres before any dial;
    a machine the server dials is known not to answer only by trying."""
    from fastapi import HTTPException

    row = _mac_row("ws-away", tunnel=tunnel)
    tunnel_down = _tunnel_down()
    manager = _away_machine_manager()
    with (
        patch("src.server.services.ssh_computers.computer_tunnel_down", tunnel_down),
        patch(_DATA_FILES, new=AsyncMock(return_value=[])),
        patch(
            "src.server.app.workspace_files.crud.db_get_workspace",
            AsyncMock(return_value=row),
        ),
        patch(
            "src.server.app.workspace_files.crud.owner_work_dir",
            return_value="/Users/me/langalpha/ws",
        ),
        patch("src.server.services.workspace_files.WorkspaceManager", manager),
        patch(
            "src.server.services.workspace_files.computer_backs_up_files",
            AsyncMock(return_value=backs_up),
        ),
        patch(
            "src.server.services.workspace_files.workspace_has_backup",
            AsyncMock(return_value=has_backup),
        ),
        patch(
            "src.server.app.workspace_files.backup_reads.FilePersistenceService.get_file_tree",
            AsyncMock(return_value=[{"path": "notes.md"}]),
        ),
    ):
        call = list_workspace_files(
            workspace_id="ws-away",
            x_user_id="user-1",
            path=".",
            include_system=False,
            pattern="**/*",
            wait_for_sandbox=False,
            auto_start=False,
            backup_ok=True,
        )
        if backs_up and has_backup:
            result = await call
        else:
            with pytest.raises(HTTPException) as refused:
                await call

    if tunnel:
        tunnel_down.assert_awaited_once_with("comp-mac", conn=None)
    else:
        tunnel_down.assert_not_awaited()
    dialed = manager.get_instance.return_value.get_session_for_workspace
    if backs_up and has_backup:
        assert result["files"] == ["notes.md"]
        assert (result["source"], result["unreachable"]) == ("database", "not_connected")
        # A tunnel known down sends the read to the backup without a dial.
        assert dialed.await_count == (0 if tunnel else 1)
    else:
        assert refused.value.status_code == 503
        assert refused.value.headers == {"X-Computer-Unreachable": "not_connected"}


@pytest.mark.asyncio
@pytest.mark.parametrize("tunnel", ["tun-1", None], ids=["tunneled", "dialed"])
async def test_a_client_that_has_not_said_it_shows_a_backup_gets_the_away_machine(tunnel):
    """An older client ignores the marks and edits what a read answers, and a
    running workspace takes its saves: older backed-up text would be saved
    over the file once the machine is back. Only a client that opts in is
    answered from the backup."""
    from fastapi import HTTPException

    from src.server.app.workspace_files.crud import read_workspace_file

    row = _mac_row("ws-away", tunnel=tunnel)
    backup_read = AsyncMock()
    with (
        patch("src.server.services.ssh_computers.computer_tunnel_down", _tunnel_down()),
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=row),
        ),
        patch(
            "src.server.app.workspace_files.crud.owned_workspace_on_computer",
            AsyncMock(return_value=row),
        ),
        patch(
            "src.server.app.workspace_files.crud.owner_work_dir",
            return_value="/Users/me/langalpha/ws",
        ),
        patch(
            "src.server.services.workspace_files.WorkspaceManager",
            _away_machine_manager(),
        ),
        patch(
            "src.server.services.workspace_files.computer_backs_up_files",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.services.workspace_files.workspace_has_backup",
            AsyncMock(return_value=True),
        ),
        patch("src.server.app.workspace_files.crud.read_from_backup", backup_read),
        pytest.raises(HTTPException) as refused,
    ):
        await read_workspace_file(
            workspace_id="ws-away",
            x_user_id="user-1",
            path="notes.md",
            offset=0,
            limit=100,
            unlimited=True,
        )

    assert refused.value.status_code == 503
    assert refused.value.headers == {"X-Computer-Unreachable": "not_connected"}
    backup_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("away", [True, False], ids=["machine-away", "stopped"])
async def test_bytes_from_the_backup_say_by_header_that_the_machine_is_away(away):
    """An image or a PDF has no field to carry the marks a text read does, so
    the viewer would show the backup's bytes as the live file's. A stopped
    workspace's backup is no news and carries nothing."""
    from fastapi.responses import Response

    from src.server.app.workspace_files.crud import download_workspace_file

    row = _mac_row("ws-away") if away else _workspace("ws-away", "user-1", status="stopped")
    with (
        patch("src.server.services.ssh_computers.computer_tunnel_down", _tunnel_down()),
        patch(
            "src.server.app.workspace_files.crud.owned_workspace_on_computer",
            AsyncMock(return_value=row),
        ),
        patch(
            "src.server.app.workspace_files.crud.owner_work_dir",
            return_value="/Users/me/langalpha/ws",
        ),
        patch(
            "src.server.services.workspace_files.computer_backs_up_files",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.services.workspace_files.workspace_has_backup",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.app.workspace_files.crud.download_from_backup",
            AsyncMock(return_value=Response(b"%PDF-1.7", media_type="application/pdf")),
        ),
    ):
        response = await download_workspace_file(
            workspace_id="ws-away",
            x_user_id="user-1",
            request=SimpleNamespace(headers={}),
            path="report.pdf",
            attachment=False,
            backup_ok=True,
        )

    assert response.body == b"%PDF-1.7"
    assert response.headers.get("X-Computer-Unreachable") == ("not_connected" if away else None)


@pytest.mark.asyncio
async def test_a_file_the_backup_lacks_answers_as_the_away_machine_not_as_missing():
    """Written since the last backup, it may well be on the machine: a 404
    would say it is gone, so the machine's 503 stands."""
    from fastapi import HTTPException

    from src.server.app.workspace_files.crud import read_workspace_file

    row = _mac_row("ws-away")
    with (
        patch("src.server.services.ssh_computers.computer_tunnel_down", _tunnel_down()),
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=row),
        ),
        patch(
            "src.server.app.workspace_files.crud.owner_work_dir",
            return_value="/Users/me/langalpha/ws",
        ),
        patch(
            "src.server.services.workspace_files.WorkspaceManager",
            _away_machine_manager(),
        ),
        patch(
            "src.server.services.workspace_files.computer_backs_up_files",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.services.workspace_files.workspace_has_backup",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.app.workspace_files.backup_reads.get_vault_secrets_for_redaction",
            AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.app.workspace_files.backup_reads.FilePersistenceService.get_file_content",
            AsyncMock(return_value=None),
        ),
        pytest.raises(HTTPException) as refused,
    ):
        await read_workspace_file(
            workspace_id="ws-away",
            x_user_id="user-1",
            path="new-draft.md",
            offset=0,
            limit=100,
            unlimited=False,
            backup_ok=True,
        )

    assert refused.value.status_code == 503
    assert refused.value.headers == {"X-Computer-Unreachable": "not_connected"}


class _LocalShell:
    """A machine that is this one: the commands the file routes send run here."""

    async def exec(self, command: str, timeout: int = 60):
        import subprocess

        done = subprocess.run(["/bin/sh", "-c", command], capture_output=True, text=True)
        return SimpleNamespace(stdout=done.stdout, stderr=done.stderr, exit_code=done.returncode)


class _LocalSandbox:
    """The sandbox surface the read and the guarded write use, over a folder here."""

    sandbox_id = "sb-local"

    def __init__(self, root: str) -> None:
        self.runtime = _LocalShell()
        self.config = SimpleNamespace(
            filesystem=SimpleNamespace(enable_path_validation=False, denied_directories=[])
        )

    async def _wait_ready(self) -> None:
        return None

    async def _runtime_call(self, func, *args, retry_policy, **kwargs):
        return await func(*args, **kwargs)

    def normalize_path(self, path: str) -> str:
        return path

    def validate_path(self, path: str) -> bool:
        return True

    async def awrite_file_text_guarded(self, filepath, content, guard):
        from ptc_agent.core.sandbox.files import awrite_file_text_guarded

        return await awrite_file_text_guarded(self, filepath, content, guard)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("since", "lands"),
    [(None, True), ("# Notes, written by the agent", False), ("", False)],
    ids=["unchanged", "changed-since", "gone-since"],
)
async def test_a_save_names_the_version_it_edited_and_never_overwrites_a_newer_file(
    tmp_path, since, lands
):
    """A save made from text read earlier (a backup shown while the machine
    slept, or a live read the agent has since written over) would replace
    what the file now holds unseen. Naming the version it began from, it
    lands only on the file it was read from, checked in the write's own
    script on the machine."""
    import os
    from contextlib import ExitStack, asynccontextmanager
    from pathlib import Path

    from fastapi import HTTPException

    from src.server.app.workspace_files.crud import (
        WriteFileRequest,
        read_workspace_file,
        write_workspace_file,
    )

    root = os.path.realpath(tmp_path)
    notes = Path(root, "notes.md")
    notes.write_text("# Notes")
    sandbox = _LocalSandbox(root)
    row = {**_workspace("ws-live", "user-1", status="running")}

    @asynccontextmanager
    async def acquire(*_args, **_kwargs):
        yield sandbox, row

    crud = "src.server.app.workspace_files.crud"
    with ExitStack() as stack:
        for p in (
            patch(f"{crud}.owned_workspace_on_computer", AsyncMock(return_value=row)),
            patch(f"{crud}.owner_work_dir", return_value=root),
            patch(f"{crud}.acquire_sandbox_to_change", acquire),
            patch(f"{crud}.contained_target", AsyncMock(return_value=str(notes))),
            patch(f"{crud}.to_client_path", return_value="notes.md"),
            patch(f"{crud}.get_vault_secrets_for_redaction", AsyncMock(return_value=[])),
            patch(f"{crud}.keep_updated_after_change"),
        ):
            stack.enter_context(p)
        read = await read_workspace_file(
            workspace_id="ws-live", x_user_id="user-1", path="notes.md",
            offset=0, limit=100, unlimited=True,
        )
        if since == "":
            notes.unlink()
        elif since is not None:
            notes.write_text(since)
        call = write_workspace_file(
            workspace_id="ws-live",
            x_user_id="user-1",
            path="notes.md",
            body=WriteFileRequest(content="# Notes, edited", base_version=read["version"]),
        )
        if lands:
            result = await call
        else:
            with pytest.raises(HTTPException) as refused:
                await call

    assert read["content"] == "# Notes" and read["version"]
    if lands:
        assert notes.read_text() == "# Notes, edited"
        assert result["version"] and result["version"] != read["version"]
    else:
        assert refused.value.status_code == 409
        assert refused.value.detail["code"] == "file_changed"
        assert (notes.read_text() if notes.exists() else None) == (since or None)
    # Nothing staged for the write is left beside the file.
    assert sorted(os.listdir(root)) == (["notes.md"] if since != "" else [])


class _AsyncLocalShell(_LocalShell):
    """``_LocalShell`` whose commands run beside the event loop, so two saves
    and a lock held from outside actually overlap, and which takes uploads."""

    async def exec(self, command: str, timeout: int = 60):
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", command,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        return SimpleNamespace(
            stdout=out.decode(), stderr=err.decode(), exit_code=proc.returncode
        )

    async def upload_file(self, content: bytes, path: str) -> None:
        with open(path, "wb") as f:
            f.write(content)


def _save_patches(sandbox, row: dict, root: str, notes) -> list:
    """The routes' reach, narrowed to ``sandbox`` over a folder here."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def acquire(*_args, **_kwargs):
        yield sandbox, row

    crud = "src.server.app.workspace_files.crud"
    return [
        patch(f"{crud}.owned_workspace_on_computer", AsyncMock(return_value=row)),
        patch(f"{crud}.owner_work_dir", return_value=root),
        patch(f"{crud}.acquire_sandbox_to_change", acquire),
        patch(f"{crud}.contained_target", AsyncMock(return_value=str(notes))),
        patch(f"{crud}.to_client_path", return_value="notes.md"),
        patch(f"{crud}.get_vault_secrets_for_redaction", AsyncMock(return_value=[])),
        patch(f"{crud}.keep_updated_after_change"),
    ]


async def _read_then_save(sandbox, root, notes, between=None, *, with_sha=True, content="# Notes, edited"):
    """Read ``notes`` as the editor does, run ``between``, then save over it."""
    import hashlib
    from contextlib import ExitStack

    from src.server.app.workspace_files.crud import (
        WriteFileRequest,
        read_workspace_file,
        write_workspace_file,
    )

    row = _workspace("ws-live", "user-1", status="running")
    with ExitStack() as stack:
        for p in _save_patches(sandbox, row, root, notes):
            stack.enter_context(p)
        read = await read_workspace_file(
            workspace_id="ws-live", x_user_id="user-1", path="notes.md",
            offset=0, limit=100, unlimited=True,
        )
        if between is not None:
            between()
        digest = hashlib.sha256(read["content"].encode()).hexdigest()
        return await write_workspace_file(
            workspace_id="ws-live",
            x_user_id="user-1",
            path="notes.md",
            body=WriteFileRequest(
                content=content,
                base_version=read["version"],
                base_sha256=digest if with_sha else None,
            ),
        )


def _touched(path):
    """A change of the file's stamps alone, as ``touch`` makes."""
    import os

    def touch() -> None:
        st = os.stat(path)
        os.utime(path, (st.st_atime + 5, st.st_mtime + 5))

    return touch


@pytest.mark.asyncio
@pytest.mark.parametrize("with_sha", [True, False], ids=["content-named", "version-only"])
async def test_a_save_over_a_file_only_touched_since_lands_when_it_names_the_content(
    tmp_path, with_sha
):
    """A ``touch``, a rewrite of the same text or a restore moves the version
    and not one byte. A save that names the content it began from lands on
    it; the only other way past would be throwing the draft away."""
    import os
    from pathlib import Path

    from fastapi import HTTPException

    root = os.path.realpath(tmp_path)
    notes = Path(root, "notes.md")
    notes.write_text("# Notes")
    call = _read_then_save(
        _LocalSandbox(root), root, notes, _touched(notes), with_sha=with_sha
    )
    if with_sha:
        result = await call
        assert result["version"]
        assert notes.read_text() == "# Notes, edited"
    else:
        with pytest.raises(HTTPException) as refused:
            await call
        assert refused.value.status_code == 409
        assert notes.read_text() == "# Notes"
    assert os.listdir(root) == ["notes.md"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "base_version",
    ["not-a-version", "12-1.5-1.5-9-extra"],
    ids=["garbage", "too-many-parts"],
)
async def test_a_base_version_no_read_gave_is_a_client_error_not_a_concurrent_edit(
    base_version,
):
    """A 409 ``file_changed`` sends the person to reread over an edit that
    never happened; the client's bug says so, before the machine is reached."""
    from fastapi import HTTPException

    from src.server.app.workspace_files.crud import (
        WriteFileRequest,
        write_workspace_file,
    )

    row = _workspace("ws-live", "user-1", status="running")
    acquire = MagicMock(side_effect=AssertionError("the machine is not reached"))
    crud = "src.server.app.workspace_files.crud"
    with (
        patch(f"{crud}.owned_workspace_on_computer", AsyncMock(return_value=row)),
        patch(f"{crud}.owner_work_dir", return_value="/w"),
        patch(f"{crud}.acquire_sandbox_to_change", acquire),
        pytest.raises(HTTPException) as refused,
    ):
        await write_workspace_file(
            workspace_id="ws-live",
            x_user_id="user-1",
            path="notes.md",
            body=WriteFileRequest(content="x", base_version=base_version),
        )

    assert refused.value.status_code == 422
    assert refused.value.detail["code"] == "invalid_base_version"


def test_a_base_version_is_bounded_and_a_content_digest_is_hex():
    from pydantic import ValidationError

    from src.server.app.workspace_files.crud import WriteFileRequest

    with pytest.raises(ValidationError):
        WriteFileRequest(content="x", base_version="1" * 129)
    with pytest.raises(ValidationError):
        WriteFileRequest(content="x", base_sha256="Z" * 64)


class _AnswerLostSandbox(_LocalSandbox):
    """A machine whose first answer to the write script is lost on the way
    back after the script ran, so the runtime runs it again, as a retried
    exec does."""

    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.runtime = _AsyncLocalShell()
        self.lost = 0

    async def _runtime_call(self, func, *args, retry_policy, **kwargs):
        answer = await func(*args, **kwargs)
        if args and "mv -f" in str(args[0]) and not self.lost:
            self.lost += 1
            answer = await func(*args, **kwargs)
        return answer


class _ScriptUnreachedSandbox(_LocalSandbox):
    """A machine that takes the staged upload and then stops answering."""

    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.runtime = _AsyncLocalShell()

    async def _runtime_call(self, func, *args, retry_policy, **kwargs):
        if args and "mv -f" in str(args[0]):
            raise SandboxTransientError("connection dropped")
        return await func(*args, **kwargs)


@pytest.mark.asyncio
async def test_a_large_save_run_again_after_it_landed_reports_it_landed(tmp_path, monkeypatch):
    """Past the inline limit the bytes are staged beside the file, and the
    rename takes the staged copy away. A run of the script repeated after the
    first landed found nothing to rename and failed a save that had landed."""
    import os
    from pathlib import Path

    monkeypatch.setattr("ptc_agent.core.sandbox.files._INLINE_TEXT_WRITE_LIMIT", 8)
    root = os.path.realpath(tmp_path)
    notes = Path(root, "notes.md")
    notes.write_text("# Notes")
    sandbox = _AnswerLostSandbox(root)

    result = await _read_then_save(sandbox, root, notes, content="# Notes, a long edit")

    assert sandbox.lost == 1
    assert result["version"] and result["size"] == len("# Notes, a long edit")
    assert notes.read_text() == "# Notes, a long edit"
    assert os.listdir(root) == ["notes.md"]


@pytest.mark.asyncio
async def test_a_large_save_that_never_reached_its_script_leaves_nothing_staged(
    tmp_path, monkeypatch
):
    """The staged copy sits in the workspace folder, where the file panel lists
    it and a backup keeps it, so a write cut short removes it."""
    import os
    from pathlib import Path

    monkeypatch.setattr("ptc_agent.core.sandbox.files._INLINE_TEXT_WRITE_LIMIT", 8)
    root = os.path.realpath(tmp_path)
    notes = Path(root, "notes.md")
    notes.write_text("# Notes")

    with pytest.raises(SandboxTransientError):
        await _read_then_save(_ScriptUnreachedSandbox(root), root, notes)

    assert notes.read_text() == "# Notes"
    assert os.listdir(root) == ["notes.md"]


@pytest.mark.asyncio
async def test_a_save_waits_for_another_saves_check_and_rename(tmp_path):
    """Two saves of one file through different workers both passed the check
    before either renamed, and the second wrote over the first unseen. The
    check and the rename hold a lock on the machine."""
    import os
    from pathlib import Path

    from ptc_agent.core.sandbox.files import _guard_lock

    root = os.path.realpath(tmp_path)
    notes = Path(root, "notes.md")
    notes.write_text("# Notes")
    sandbox = _LocalSandbox(root)
    sandbox.runtime = _AsyncLocalShell()
    lock = _guard_lock(str(notes))
    os.mkdir(lock)
    try:
        save = asyncio.create_task(_read_then_save(sandbox, root, notes))
        await asyncio.sleep(0.5)
        assert not save.done()
        assert notes.read_text() == "# Notes"
    finally:
        os.rmdir(lock)
    result = await asyncio.wait_for(save, 10)

    assert result["version"]
    assert notes.read_text() == "# Notes, edited"
    assert not os.path.exists(lock)
