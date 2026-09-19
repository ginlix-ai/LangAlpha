"""API integration test fixtures — real PTCSandbox + mocked DB/auth.

Self-contained conftest that wires a real PTCSandbox (backed by MemoryProvider)
to FastAPI workspace endpoint routers via httpx.AsyncClient. Database and auth
layers are mocked so tests exercise the full sandbox-to-HTTP path without
external infrastructure.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app
from tests.integration.sandbox.conftest import _make_core_config
from tests.integration.sandbox.memory_provider import MemoryProvider
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TEST_USER_ID = "test-user-123"
TEST_WS_ID = "ws-test-001"
TEST_COMPUTER_ID = "cmp-test-001"
TEST_PROJECT = ProjectContext(TEST_WS_ID, "api-test-ab12")


def _make_workspace(status="running", **overrides):
    ws = {
        "id": TEST_WS_ID,
        "user_id": TEST_USER_ID,
        "workspace_id": TEST_WS_ID,
        "computer_id": TEST_COMPUTER_ID,
        "dir_name": TEST_PROJECT.dir_name,
        "status": status,
        "sandbox_id": "sb-123",
        "created_at": "2026-01-01T00:00:00Z",
    }
    ws.update(overrides)
    return ws


@pytest.fixture(autouse=True)
def _no_secret_db_lookup(monkeypatch):
    """Keep DB reads mocked while exercising redaction on real sandbox files."""
    monkeypatch.setattr(
        "src.server.utils.secret_redactor._connector_secret_literals",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        "src.server.database.vault_secrets.get_effective_secrets",
        AsyncMock(return_value={}),
    )


@pytest.fixture
def sandbox_base_dir(tmp_path):
    d = tmp_path / "sandboxes"
    d.mkdir()
    return str(d)


@pytest_asyncio.fixture
async def sandbox(sandbox_base_dir):
    """Self-contained PTCSandbox backed by MemoryProvider."""
    provider = MemoryProvider(base_dir=sandbox_base_dir)
    config = _make_core_config(working_directory=sandbox_base_dir)
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=provider,
    ):
        sb = PTCSandbox(config)
        await sb.setup_sandbox_workspace(dir_name=TEST_PROJECT.dir_name)
        actual_work_dir = await sb.runtime.fetch_working_dir()
        sb.config.filesystem.working_directory = actual_work_dir
        sb.config.filesystem.allowed_directories = [actual_work_dir, "/tmp"]
        yield sb
        try:
            await sb.cleanup()
        except Exception:
            pass


@pytest_asyncio.fixture
async def mock_session(sandbox):
    """Mock session object with real sandbox."""
    session = MagicMock()
    session.sandbox = sandbox
    session.mcp_registry = MagicMock()
    session.mcp_registry.connectors = MagicMock()
    session.mcp_registry.connectors.keys.return_value = ["fmp", "sec"]
    return session


@pytest_asyncio.fixture
async def files_client(mock_session, sandbox):
    """httpx client wired to workspace_files router with real sandbox."""
    from src.server.app.workspace_files import router

    app = create_test_app(router)

    mock_manager = MagicMock()
    mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)
    mock_manager._sessions = {TEST_WS_ID: mock_session}
    mock_manager.config = MagicMock()
    mock_manager.config.to_core_config.return_value = sandbox.config

    # The dual-router package resolves sessions from three modules — patch
    # WorkspaceManager at every import site or the real singleton gets hit.
    with (
        patch(
            "src.server.app.workspace_files.crud.db_get_workspace",
            AsyncMock(return_value=_make_workspace()),
        ),
        patch("src.server.app.workspace_files.crud.WorkspaceManager") as MockWM,
        patch(
            "src.server.app.workspace_files._shared.WorkspaceManager"
        ) as MockWMShared,
        patch(
            "src.server.app.workspace_files.serve.WorkspaceManager"
        ) as MockWMServe,
    ):
        MockWM.get_instance.return_value = mock_manager
        MockWMShared.get_instance.return_value = mock_manager
        MockWMServe.get_instance.return_value = mock_manager
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, sandbox


def _make_computer(status="running", **overrides):
    comp = {
        "computer_id": TEST_COMPUTER_ID,
        "user_id": TEST_USER_ID,
        "kind": "daytona",
        "provider_ref": "sb-123",
        "name": "My computer",
        "is_primary": True,
        "status": status,
        "resource_tier": "standard",
        "is_always_on": False,
        "root_dir": "/home/workspace",
        "last_activity_at": None,
        "stopped_at": None,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "config": {},
    }
    comp.update(overrides)
    return comp


@pytest_asyncio.fixture
async def computers_client():
    """Exercise mounted lifecycle routes with a mocked computer manager."""
    from src.server.app.computers import router

    app = create_test_app(router)

    session = MagicMock()
    session.sandbox = MagicMock()
    session.sandbox.sandbox_id = "sb-123"

    manager = MagicMock()
    manager.get_session_for_computer = AsyncMock(return_value=session)
    manager.start_computer = AsyncMock(return_value=_make_computer())
    manager.stop_computer = AsyncMock(return_value=_make_computer("stopped"))
    manager.archive_computer = AsyncMock(return_value=_make_computer("stopped"))
    manager.set_computer_spec = AsyncMock(return_value=_make_computer())
    manager.set_computer_always_on = AsyncMock(return_value=_make_computer())

    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=_make_computer()),
        ),
        patch(
            "src.server.app.computers.get_computers_for_user",
            AsyncMock(return_value=[_make_computer()]),
        ),
        patch(
            "src.server.app.computers._computer_manager", return_value=manager
        ),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, manager


@pytest_asyncio.fixture
async def sandbox_client(mock_session, sandbox):
    """httpx client wired to workspace_sandbox router with real sandbox."""
    from src.server.app.workspace_sandbox import router

    app = create_test_app(router)

    mock_manager = MagicMock()
    mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)
    mock_manager._sessions = {TEST_WS_ID: mock_session}
    mock_manager.config = MagicMock()
    mock_manager.config.to_core_config.return_value = sandbox.config

    with (
        patch(
            "src.server.app.workspace_sandbox.db_get_workspace",
            AsyncMock(return_value=_make_workspace()),
        ),
        patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
    ):
        MockWM.get_instance.return_value = mock_manager
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, sandbox
