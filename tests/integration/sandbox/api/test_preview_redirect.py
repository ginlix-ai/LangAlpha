"""Integration tests for preview redirect endpoint and sandbox preview methods.

Tests the unauthenticated preview redirect router (``preview_redirect_router``)
wired to a real PTCSandbox (MemoryProvider), plus direct sandbox preview method
tests (start/stop/logs for preview servers and background commands).

Database, auth, and Redis cache layers are mocked so tests exercise the full
sandbox-to-HTTP path without external infrastructure.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.core.sandbox.runtime import PreviewInfo, SessionCommandResult
from tests.conftest import create_test_app
from tests.integration.sandbox.conftest import _make_core_config
from tests.integration.sandbox.memory_provider import MemoryProvider

from .conftest import TEST_USER_ID, TEST_WS_ID, _make_workspace

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

PREVIEW_BASE = f"/api/v1/preview/{TEST_WS_ID}"
FAKE_SIGNED_URL = "https://test-preview.example.com/proxy/8080"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sandbox_base_dir(tmp_path):
    d = tmp_path / "sandboxes"
    d.mkdir()
    return str(d)


@pytest_asyncio.fixture
async def sandbox(sandbox_base_dir):
    """Self-contained PTCSandbox backed by MemoryProvider."""
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    provider = MemoryProvider(base_dir=sandbox_base_dir)
    config = _make_core_config(working_directory=sandbox_base_dir)
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=provider,
    ):
        sb = PTCSandbox(config)
        await sb.setup_sandbox_workspace()
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
async def preview_client(mock_session, sandbox):
    """httpx client wired to preview_redirect_router with real sandbox.

    Unlike ``sandbox_client``, this mounts the unauthenticated
    ``preview_redirect_router`` which serves the GET /api/v1/preview/...
    endpoints.
    """
    from src.server.app.workspace_sandbox import preview_redirect_router

    app = create_test_app(preview_redirect_router)

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
        patch(
            "src.server.app.workspace_sandbox._resolve_preview",
            AsyncMock(return_value=FAKE_SIGNED_URL),
        ),
    ):
        MockWM.get_instance.return_value = mock_manager
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            follow_redirects=False,
        ) as client:
            yield client, sandbox


# ---------------------------------------------------------------------------
# Endpoint tests: Preview Redirect
# ---------------------------------------------------------------------------


class TestPreviewRedirectWorkspaceNotFound:
    """GET /api/v1/preview/{workspace_id}/{port} when workspace does not exist."""

    async def test_returns_404(self, mock_session, sandbox):
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=None),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Preview not available"


class TestPreviewRedirectStoppedWorkspace:
    """GET /api/v1/preview/{workspace_id}/{port} when workspace is stopped."""

    async def test_returns_404_for_stopped(self, mock_session, sandbox):
        """Stopped workspaces return 404 (same as missing) to avoid leaking existence."""
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="stopped")),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Preview not available"


class TestPreviewRedirectRunningWorkspace:
    """GET /api/v1/preview/{workspace_id}/{port} for a running workspace."""

    async def test_returns_302_redirect(self, preview_client):
        client, _sandbox = preview_client

        resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 302
        assert resp.headers["location"] == FAKE_SIGNED_URL
        assert "no-store" in resp.headers.get("cache-control", "")
        assert "no-cache" in resp.headers.get("cache-control", "")

    async def test_redirect_has_must_revalidate(self, preview_client):
        client, _sandbox = preview_client

        resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 302
        assert "must-revalidate" in resp.headers.get("cache-control", "")


class TestPreviewRedirectWithPath:
    """GET /api/v1/preview/{workspace_id}/{port}/{path} with path suffix."""

    async def test_path_appended_to_redirect_url(self, mock_session, sandbox):
        """The path suffix is appended to the signed URL before redirecting."""
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        signed_url = "https://preview.example.com/proxy/8080"

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(return_value=signed_url),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080/timeline.html")

        assert resp.status_code == 302
        location = resp.headers["location"]
        assert location.endswith("/timeline.html")

    async def test_nested_path_appended_to_redirect_url(self, mock_session, sandbox):
        """Nested paths like assets/style.css are appended correctly."""
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        signed_url = "https://preview.example.com/proxy/8080"

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(return_value=signed_url),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080/assets/style.css")

        assert resp.status_code == 302
        location = resp.headers["location"]
        assert "/assets/style.css" in location


class TestPreviewRedirectPathTraversal:
    """GET /api/v1/preview/{workspace_id}/{port}/{path} with path traversal.

    HTTP clients and Starlette normalise bare ``..`` segments in URL paths
    before the handler sees them.  The realistic attack vector is
    percent-encoded dots (``%2e%2e``) which Starlette decodes into the
    ``{path:path}`` parameter as literal ``..``.  We use that encoding here
    so the check in ``_preview_redirect`` actually fires.
    """

    async def test_encoded_double_dot_returns_400(self, mock_session, sandbox):
        """URL-encoded '..' (%2e%2e) at the start of the path is rejected."""
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(return_value=FAKE_SIGNED_URL),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                # %2e%2e decodes to ".." inside the {path:path} parameter
                resp = await client.get(f"{PREVIEW_BASE}/8080/%2e%2e/etc/passwd")

        assert resp.status_code == 400
        assert resp.json()["detail"] == "Invalid path"

    async def test_mid_path_traversal_returns_400(self, mock_session, sandbox):
        """URL-encoded '..' in the middle of the path is also rejected."""
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(return_value=FAKE_SIGNED_URL),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                # foo/%2e%2e/bar decodes to "foo/../bar" in the path param
                resp = await client.get(f"{PREVIEW_BASE}/8080/foo/%2e%2e/bar")

        assert resp.status_code == 400
        assert resp.json()["detail"] == "Invalid path"


class TestPreviewRedirectTimeout:
    """GET /api/v1/preview/{workspace_id}/{port} when resolution times out.

    ``_preview_redirect`` wraps the inner ``_resolve()`` coroutine in
    ``asyncio.wait_for(..., timeout=20)``.  To trigger the 504 path we make
    ``_resolve_preview`` block longer than the timeout, and shorten the
    timeout via a wrapper around ``asyncio.wait_for`` so the test finishes
    quickly.
    """

    async def test_timeout_returns_504(self, mock_session, sandbox):
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        async def slow_resolve(*_args, **_kwargs):
            """Simulate a preview URL resolution that hangs."""
            await asyncio.sleep(60)
            return FAKE_SIGNED_URL

        _real_wait_for = asyncio.wait_for

        async def _short_wait_for(coro, *, timeout=None):
            """Replace the 20s timeout with 0.05s so the test is fast."""
            if timeout == 20:
                timeout = 0.05
            return await _real_wait_for(coro, timeout=timeout)

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                side_effect=slow_resolve,
            ),
            patch(
                "src.server.app.workspace_sandbox.asyncio.wait_for",
                side_effect=_short_wait_for,
            ),
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 504
        assert resp.json()["detail"] == "Preview URL resolution timed out"


class TestPreviewRedirectNotImplemented:
    """GET /api/v1/preview/{workspace_id}/{port} when provider lacks preview support."""

    async def test_not_implemented_returns_501(self, mock_session, sandbox):
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(return_value=mock_session)

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(side_effect=NotImplementedError("not supported")),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 501
        assert "not supported" in resp.json()["detail"].lower()


class TestPreviewRedirectPortValidation:
    """Verify FastAPI path parameter validation on port range."""

    async def test_port_below_range_returns_422(self, preview_client):
        client, _sandbox = preview_client
        resp = await client.get(f"/api/v1/preview/{TEST_WS_ID}/80")
        assert resp.status_code == 422

    async def test_port_above_range_returns_422(self, preview_client):
        client, _sandbox = preview_client
        resp = await client.get(f"/api/v1/preview/{TEST_WS_ID}/99999")
        assert resp.status_code == 422

    async def test_port_at_lower_bound_succeeds(self, preview_client):
        client, _sandbox = preview_client
        resp = await client.get(f"/api/v1/preview/{TEST_WS_ID}/3000")
        assert resp.status_code == 302

    async def test_port_at_upper_bound_succeeds(self, preview_client):
        client, _sandbox = preview_client
        resp = await client.get(f"/api/v1/preview/{TEST_WS_ID}/9999")
        assert resp.status_code == 302


class TestPreviewRedirectSessionNotReady:
    """GET /api/v1/preview/{workspace_id}/{port} when session lookup fails."""

    async def test_session_error_returns_503(self, sandbox):
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)
        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            side_effect=RuntimeError("session init failed"),
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 503
        assert resp.json()["detail"] == "Sandbox not ready"

    async def test_sandbox_none_returns_503(self, sandbox):
        """Session exists but sandbox attribute is None."""
        from src.server.app.workspace_sandbox import preview_redirect_router

        app = create_test_app(preview_redirect_router)

        session_no_sandbox = MagicMock(spec=[])  # no sandbox attribute

        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            return_value=session_no_sandbox,
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace()),
            ),
            patch("src.server.app.workspace_sandbox.WorkspaceManager") as MockWM,
        ):
            MockWM.get_instance.return_value = mock_manager
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                follow_redirects=False,
            ) as client:
                resp = await client.get(f"{PREVIEW_BASE}/8080")

        assert resp.status_code == 503
        assert resp.json()["detail"] == "Sandbox not available"


# ---------------------------------------------------------------------------
# Sandbox method tests: Preview Server lifecycle
# ---------------------------------------------------------------------------


class TestStartPreviewServer:
    """PTCSandbox.start_preview_server with MemoryProvider.

    MemoryProvider's runtime does not implement sessions (raises
    NotImplementedError), so we mock the session methods on the runtime
    to verify the sandbox-level orchestration logic.
    """

    async def test_creates_per_port_session(self, sandbox):
        """start_preview_server creates a session named 'preview-{port}'."""
        created_sessions = []

        async def fake_create_session(session_id):
            created_sessions.append(session_id)

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            return SessionCommandResult(
                cmd_id="cmd-001", exit_code=None, stdout="", stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.session_execute = fake_session_execute

        cmd_id = await sandbox.start_preview_server("python -m http.server 8080", 8080)

        assert cmd_id == "cmd-001"
        assert "preview-8080" in created_sessions
        assert 8080 in sandbox._preview_sessions
        session_id, stored_cmd_id = sandbox._preview_sessions[8080]
        assert session_id == "preview-8080"
        assert stored_cmd_id == "cmd-001"

    async def test_replaces_existing_session_on_same_port(self, sandbox):
        """Starting on a port that already has a session tears down the old one."""
        deleted_sessions = []
        created_sessions = []
        call_count = 0

        async def fake_create_session(session_id):
            created_sessions.append(session_id)

        async def fake_delete_session(session_id):
            deleted_sessions.append(session_id)

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            nonlocal call_count
            call_count += 1
            return SessionCommandResult(
                cmd_id=f"cmd-{call_count:03d}",
                exit_code=None,
                stdout="",
                stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.delete_session = fake_delete_session
        sandbox.runtime.session_execute = fake_session_execute

        # First start
        cmd_id_1 = await sandbox.start_preview_server("python -m http.server 8080", 8080)
        assert cmd_id_1 == "cmd-001"
        assert 8080 in sandbox._preview_sessions

        # Second start on the same port
        cmd_id_2 = await sandbox.start_preview_server("python -m http.server 8080", 8080)
        assert cmd_id_2 == "cmd-002"

        # Old session should have been deleted
        assert "preview-8080" in deleted_sessions
        # New session entry should replace the old one
        _, stored_cmd_id = sandbox._preview_sessions[8080]
        assert stored_cmd_id == "cmd-002"

    async def test_multiple_ports_get_separate_sessions(self, sandbox):
        """Different ports get different sessions."""
        created_sessions = []
        call_count = 0

        async def fake_create_session(session_id):
            created_sessions.append(session_id)

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            nonlocal call_count
            call_count += 1
            return SessionCommandResult(
                cmd_id=f"cmd-{call_count:03d}",
                exit_code=None,
                stdout="",
                stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.session_execute = fake_session_execute

        await sandbox.start_preview_server("python -m http.server 3000", 3000)
        await sandbox.start_preview_server("python -m http.server 8080", 8080)

        assert "preview-3000" in created_sessions
        assert "preview-8080" in created_sessions
        assert 3000 in sandbox._preview_sessions
        assert 8080 in sandbox._preview_sessions
        assert sandbox._preview_sessions[3000][0] == "preview-3000"
        assert sandbox._preview_sessions[8080][0] == "preview-8080"


class TestStopPreviewServer:
    """PTCSandbox.stop_preview_server with mocked runtime sessions."""

    async def test_stop_deletes_session_and_cleans_up(self, sandbox):
        """Stopping a preview server deletes the session and removes tracking."""
        deleted_sessions = []

        async def fake_create_session(session_id):
            pass

        async def fake_delete_session(session_id):
            deleted_sessions.append(session_id)

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            return SessionCommandResult(
                cmd_id="cmd-001", exit_code=None, stdout="", stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.delete_session = fake_delete_session
        sandbox.runtime.session_execute = fake_session_execute

        await sandbox.start_preview_server("python -m http.server 8080", 8080)
        assert 8080 in sandbox._preview_sessions

        result = await sandbox.stop_preview_server(8080)

        assert result is True
        assert "preview-8080" in deleted_sessions
        assert 8080 not in sandbox._preview_sessions

    async def test_stop_nonexistent_port_returns_false(self, sandbox):
        """Stopping a port with no preview server returns False."""
        result = await sandbox.stop_preview_server(9999)
        assert result is False


class TestGetPreviewServerLogs:
    """PTCSandbox.get_preview_server_logs with mocked runtime sessions."""

    async def test_logs_for_running_server(self, sandbox):
        """Get logs for a running preview server."""
        async def fake_create_session(session_id):
            pass

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            return SessionCommandResult(
                cmd_id="cmd-001", exit_code=None, stdout="", stderr="",
            )

        async def fake_session_command_logs(session_id, cmd_id):
            return SessionCommandResult(
                cmd_id=cmd_id,
                exit_code=None,  # still running
                stdout="Serving HTTP on 0.0.0.0 port 8080\n",
                stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.session_execute = fake_session_execute
        sandbox.runtime.session_command_logs = fake_session_command_logs

        await sandbox.start_preview_server("python -m http.server 8080", 8080)

        logs = await sandbox.get_preview_server_logs(8080)

        assert logs["success"] is True
        assert logs["is_running"] is True
        assert logs["port"] == 8080
        assert "Serving HTTP" in logs["stdout"]

    async def test_logs_for_nonexistent_port(self, sandbox):
        """Getting logs for a non-tracked port returns a failure result."""
        logs = await sandbox.get_preview_server_logs(9999)

        assert logs["success"] is False
        assert logs["is_running"] is False
        assert logs["port"] == 9999
        assert "No preview session" in logs["stderr"]

    async def test_logs_for_exited_server(self, sandbox):
        """Logs show exit code when server has stopped."""
        async def fake_create_session(session_id):
            pass

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            return SessionCommandResult(
                cmd_id="cmd-001", exit_code=None, stdout="", stderr="",
            )

        async def fake_session_command_logs(session_id, cmd_id):
            return SessionCommandResult(
                cmd_id=cmd_id,
                exit_code=1,
                stdout="",
                stderr="Address already in use",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.session_execute = fake_session_execute
        sandbox.runtime.session_command_logs = fake_session_command_logs

        await sandbox.start_preview_server("python -m http.server 8080", 8080)

        logs = await sandbox.get_preview_server_logs(8080)

        assert logs["success"] is True
        assert logs["is_running"] is False
        assert logs["exit_code"] == 1
        assert "Address already in use" in logs["stderr"]


# ---------------------------------------------------------------------------
# Sandbox method tests: Background command lifecycle
# ---------------------------------------------------------------------------


class TestBackgroundCommandStop:
    """PTCSandbox.stop_background_command via execute_bash_command(background=True)."""

    async def test_run_and_stop_background_command(self, sandbox):
        """Start a background command, then stop it."""
        created_sessions = []
        deleted_sessions = []
        call_count = 0

        async def fake_create_session(session_id):
            created_sessions.append(session_id)

        async def fake_delete_session(session_id):
            deleted_sessions.append(session_id)

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            nonlocal call_count
            call_count += 1
            return SessionCommandResult(
                cmd_id=f"bg-cmd-{call_count:03d}",
                exit_code=None,
                stdout="",
                stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.delete_session = fake_delete_session
        sandbox.runtime.session_execute = fake_session_execute

        # Start a background command
        result = await sandbox.execute_bash_command(
            "sleep 999",
            background=True,
        )

        assert result["success"] is True
        assert "bg-cmd-001" in result["stdout"]

        # Verify the session was tracked
        cmd_id = "bg-cmd-001"
        assert cmd_id in sandbox._bg_sessions

        # Stop the background command
        stopped = await sandbox.stop_background_command(cmd_id)

        assert stopped is True
        assert cmd_id not in sandbox._bg_sessions
        # The session should have been deleted
        assert any("bg-" in s for s in deleted_sessions)

    async def test_stop_unknown_command_returns_false(self, sandbox):
        """Stopping a non-existent command ID returns False."""
        result = await sandbox.stop_background_command("nonexistent-cmd")
        assert result is False


class TestGetBackgroundCommandStatus:
    """PTCSandbox.get_background_command_status with mocked sessions."""

    async def test_status_of_running_command(self, sandbox):
        """Check status of a running background command."""
        async def fake_create_session(session_id):
            pass

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            return SessionCommandResult(
                cmd_id="bg-cmd-001", exit_code=None, stdout="", stderr="",
            )

        async def fake_session_command_logs(session_id, cmd_id):
            return SessionCommandResult(
                cmd_id=cmd_id,
                exit_code=None,  # still running
                stdout="working...\n",
                stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.session_execute = fake_session_execute
        sandbox.runtime.session_command_logs = fake_session_command_logs
        sandbox.runtime.delete_session = AsyncMock()

        result = await sandbox.execute_bash_command("sleep 999", background=True)
        cmd_id = "bg-cmd-001"

        status = await sandbox.get_background_command_status(cmd_id)

        assert status["is_running"] is True
        assert status["cmd_id"] == cmd_id
        assert "working" in status["stdout"]
        # Session should NOT be cleaned up yet
        assert cmd_id in sandbox._bg_sessions

    async def test_status_of_completed_command_auto_cleans(self, sandbox):
        """Completed command status auto-cleans the session."""
        deleted_sessions = []

        async def fake_create_session(session_id):
            pass

        async def fake_delete_session(session_id):
            deleted_sessions.append(session_id)

        async def fake_session_execute(session_id, command, *, run_async=False, timeout=None):
            return SessionCommandResult(
                cmd_id="bg-cmd-001", exit_code=None, stdout="", stderr="",
            )

        async def fake_session_command_logs(session_id, cmd_id):
            return SessionCommandResult(
                cmd_id=cmd_id,
                exit_code=0,  # completed
                stdout="done\n",
                stderr="",
            )

        sandbox.runtime.create_session = fake_create_session
        sandbox.runtime.delete_session = fake_delete_session
        sandbox.runtime.session_execute = fake_session_execute
        sandbox.runtime.session_command_logs = fake_session_command_logs

        await sandbox.execute_bash_command("echo hello", background=True)
        cmd_id = "bg-cmd-001"

        status = await sandbox.get_background_command_status(cmd_id)

        assert status["is_running"] is False
        assert status["success"] is True
        assert status["exit_code"] == 0
        # Session should be auto-cleaned
        assert cmd_id not in sandbox._bg_sessions
        assert len(deleted_sessions) > 0

    async def test_status_of_unknown_command(self, sandbox):
        """Querying status for an unknown command returns a failure dict."""
        status = await sandbox.get_background_command_status("nonexistent")

        assert status["success"] is False
        assert status["is_running"] is False
        assert "No background session" in status["stderr"]
