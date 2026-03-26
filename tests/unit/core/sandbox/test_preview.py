"""
Tests for PTCSandbox preview methods and workspace_sandbox preview redirect endpoint.

Part 1: PTCSandbox unit tests covering background session management,
         preview server lifecycle, reachability checks, and leak fixes.
Part 2: _preview_redirect endpoint tests covering error states, redirects,
         path handling, and timeouts.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.runtime import (
    PreviewInfo,
    SessionCommandResult,
    SandboxProvider,
    SandboxRuntime,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> CoreConfig:
    defaults = dict(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    defaults.update(overrides)
    return CoreConfig(**defaults)


def _make_sandbox(mock_provider, mock_runtime):
    """Create a ready PTCSandbox wired to mocks."""
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=mock_provider,
    ):
        sandbox = PTCSandbox(config=_make_config())
    sandbox.runtime = mock_runtime
    sandbox._ready_event = asyncio.Event()
    sandbox._ready_event.set()
    return sandbox


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_runtime():
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.id = "test-sandbox"
    runtime.working_dir = "/workspace"
    runtime.create_session = AsyncMock()
    runtime.delete_session = AsyncMock()
    runtime.session_execute = AsyncMock(
        return_value=SessionCommandResult(
            cmd_id="cmd-001", exit_code=None, stdout="", stderr=""
        )
    )
    runtime.session_command_logs = AsyncMock(
        return_value=SessionCommandResult(
            cmd_id="cmd-001", exit_code=None, stdout="some output", stderr=""
        )
    )
    runtime.exec = AsyncMock()
    runtime.upload_file = AsyncMock()
    runtime.get_preview_url = AsyncMock(
        return_value=PreviewInfo(url="https://preview.example.com/signed", token="tok")
    )
    runtime.get_preview_link = AsyncMock(
        return_value=PreviewInfo(
            url="https://preview.example.com/link",
            token="tok",
            auth_headers={"Authorization": "Bearer tok"},
        )
    )
    return runtime


@pytest.fixture
def mock_provider():
    provider = AsyncMock(spec=SandboxProvider)
    provider.is_transient_error = MagicMock(return_value=False)
    provider.close = AsyncMock()
    return provider


@pytest.fixture
def sandbox(mock_provider, mock_runtime):
    return _make_sandbox(mock_provider, mock_runtime)


# ===================================================================
# Part 1: PTCSandbox unit tests
# ===================================================================


class TestCreateBgSession:
    """Tests for PTCSandbox._create_bg_session."""

    @pytest.mark.asyncio
    async def test_happy_path_creates_session(self, sandbox, mock_runtime):
        session_id = await sandbox._create_bg_session("task-1")
        assert session_id == "bg-task-1"
        mock_runtime.create_session.assert_called_once()

    @pytest.mark.asyncio
    async def test_already_exists_triggers_delete_and_recreate(
        self, sandbox, mock_runtime
    ):
        mock_runtime.create_session.side_effect = [
            Exception("session already exists"),
            None,  # recreate succeeds
        ]
        mock_runtime.delete_session.return_value = None

        session_id = await sandbox._create_bg_session("task-2")

        assert session_id == "bg-task-2"
        assert mock_runtime.delete_session.call_count == 1
        assert mock_runtime.create_session.call_count == 2

    @pytest.mark.asyncio
    async def test_delete_recreate_failure_reuses_stale(
        self, sandbox, mock_runtime
    ):
        mock_runtime.create_session.side_effect = [
            Exception("session already exists"),
            Exception("still broken"),
        ]
        mock_runtime.delete_session.side_effect = Exception("delete also failed")

        # Should not raise — falls back to reusing stale session
        session_id = await sandbox._create_bg_session("task-3")
        assert session_id == "bg-task-3"

    @pytest.mark.asyncio
    async def test_non_already_exists_error_raises(self, sandbox, mock_runtime):
        mock_runtime.create_session.side_effect = Exception("permission denied")

        with pytest.raises(Exception, match="permission denied"):
            await sandbox._create_bg_session("task-4")


class TestStopBackgroundCommand:
    """Tests for PTCSandbox.stop_background_command."""

    @pytest.mark.asyncio
    async def test_found_and_deleted(self, sandbox, mock_runtime):
        sandbox._bg_sessions["cmd-abc"] = "bg-abc"
        result = await sandbox.stop_background_command("cmd-abc")
        assert result is True
        mock_runtime.delete_session.assert_called_once()
        assert "cmd-abc" not in sandbox._bg_sessions

    @pytest.mark.asyncio
    async def test_no_session_returns_false(self, sandbox):
        result = await sandbox.stop_background_command("nonexistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_fails_returns_false(self, sandbox, mock_runtime):
        sandbox._bg_sessions["cmd-fail"] = "bg-fail"
        mock_runtime.delete_session.side_effect = Exception("network error")

        result = await sandbox.stop_background_command("cmd-fail")
        assert result is False
        # Session should be cleaned up from _bg_sessions even on failure
        assert "cmd-fail" not in sandbox._bg_sessions


class TestStartPreviewServer:
    """Tests for PTCSandbox.start_preview_server."""

    @pytest.mark.asyncio
    async def test_happy_path_creates_per_port_session(self, sandbox, mock_runtime):
        mock_runtime.session_execute.return_value = SessionCommandResult(
            cmd_id="preview-cmd-1", exit_code=None, stdout="", stderr=""
        )

        cmd_id = await sandbox.start_preview_server("python -m http.server 8080", 8080)

        assert cmd_id == "preview-cmd-1"
        mock_runtime.create_session.assert_called_once()
        # Verify the session is stored correctly
        assert 8080 in sandbox._preview_sessions
        session_id, stored_cmd_id = sandbox._preview_sessions[8080]
        assert session_id == "preview-8080"
        assert stored_cmd_id == "preview-cmd-1"

    @pytest.mark.asyncio
    async def test_stale_session_teardown(self, sandbox, mock_runtime):
        # Pre-populate a stale session for port 8080
        sandbox._preview_sessions[8080] = ("preview-8080", "old-cmd")

        mock_runtime.session_execute.return_value = SessionCommandResult(
            cmd_id="new-cmd", exit_code=None, stdout="", stderr=""
        )

        cmd_id = await sandbox.start_preview_server("python app.py", 8080)

        assert cmd_id == "new-cmd"
        # delete_session should have been called for the stale session
        assert mock_runtime.delete_session.call_count >= 1
        # Verify updated preview sessions
        assert sandbox._preview_sessions[8080] == ("preview-8080", "new-cmd")

    @pytest.mark.asyncio
    async def test_stale_session_cleanup_failure_continues(
        self, sandbox, mock_runtime
    ):
        sandbox._preview_sessions[8080] = ("preview-8080", "old-cmd")
        # delete_session fails but we should continue
        mock_runtime.delete_session.side_effect = Exception("cleanup failed")

        mock_runtime.session_execute.return_value = SessionCommandResult(
            cmd_id="new-cmd", exit_code=None, stdout="", stderr=""
        )

        cmd_id = await sandbox.start_preview_server("python app.py", 8080)
        assert cmd_id == "new-cmd"

    @pytest.mark.asyncio
    async def test_already_exists_on_create_continues(self, sandbox, mock_runtime):
        mock_runtime.create_session.side_effect = Exception(
            "Session already exists for this sandbox"
        )
        mock_runtime.session_execute.return_value = SessionCommandResult(
            cmd_id="reused-cmd", exit_code=None, stdout="", stderr=""
        )

        cmd_id = await sandbox.start_preview_server("node server.js", 3000)
        assert cmd_id == "reused-cmd"


class TestIsPreviewReachable:
    """Tests for PTCSandbox._is_preview_reachable."""

    @pytest.mark.asyncio
    async def test_returns_true_for_200(self, sandbox, mock_runtime):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.head = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(
                return_value=client_instance
            )
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await sandbox._is_preview_reachable(8080)
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_true_for_404(self, sandbox, mock_runtime):
        """404 means server IS running but path not found."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.head = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(
                return_value=client_instance
            )
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await sandbox._is_preview_reachable(8080)
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_502(self, sandbox, mock_runtime):
        """502 means proxy can't reach the backend."""
        mock_response = MagicMock()
        mock_response.status_code = 502

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.head = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(
                return_value=client_instance
            )
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await sandbox._is_preview_reachable(8080)
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_for_503(self, sandbox, mock_runtime):
        mock_response = MagicMock()
        mock_response.status_code = 503

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.head = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(
                return_value=client_instance
            )
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await sandbox._is_preview_reachable(8080)
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self, sandbox, mock_runtime):
        mock_runtime.get_preview_link.side_effect = Exception("connection refused")

        result = await sandbox._is_preview_reachable(8080)
        assert result is False


class TestStopPreviewServer:
    """Tests for PTCSandbox.stop_preview_server."""

    @pytest.mark.asyncio
    async def test_found_and_deleted(self, sandbox, mock_runtime):
        sandbox._preview_sessions[8080] = ("preview-8080", "cmd-1")
        result = await sandbox.stop_preview_server(8080)
        assert result is True
        mock_runtime.delete_session.assert_called_once()
        assert 8080 not in sandbox._preview_sessions

    @pytest.mark.asyncio
    async def test_not_found_returns_false(self, sandbox):
        result = await sandbox.stop_preview_server(9999)
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_fails_still_cleans_up(self, sandbox, mock_runtime):
        sandbox._preview_sessions[8080] = ("preview-8080", "cmd-1")
        mock_runtime.delete_session.side_effect = Exception("network error")

        result = await sandbox.stop_preview_server(8080)
        # Returns True even if delete fails (session entry is still cleaned up)
        assert result is True
        assert 8080 not in sandbox._preview_sessions


class TestGetPreviewServerLogs:
    """Tests for PTCSandbox.get_preview_server_logs."""

    @pytest.mark.asyncio
    async def test_entry_exists_returns_logs(self, sandbox, mock_runtime):
        sandbox._preview_sessions[8080] = ("preview-8080", "cmd-1")
        mock_runtime.session_command_logs.return_value = SessionCommandResult(
            cmd_id="cmd-1", exit_code=None, stdout="server started", stderr=""
        )

        result = await sandbox.get_preview_server_logs(8080)
        assert result["success"] is True
        assert result["is_running"] is True
        assert result["stdout"] == "server started"
        assert result["port"] == 8080

    @pytest.mark.asyncio
    async def test_no_entry_returns_error_dict(self, sandbox):
        result = await sandbox.get_preview_server_logs(9999)
        assert result["success"] is False
        assert result["is_running"] is False
        assert "No preview session" in result["stderr"]
        assert result["port"] == 9999

    @pytest.mark.asyncio
    async def test_logs_api_failure_returns_error_dict(self, sandbox, mock_runtime):
        sandbox._preview_sessions[8080] = ("preview-8080", "cmd-1")
        mock_runtime.session_command_logs.side_effect = Exception("API error")

        result = await sandbox.get_preview_server_logs(8080)
        assert result["success"] is False
        assert "Failed to get logs" in result["stderr"]


class TestExecuteBashBackgroundSessionLeakFix:
    """Test that session_execute failure triggers session cleanup.

    Note: execute_bash_command has an outer try/except that catches all
    exceptions and returns an error dict, so the exception does not propagate.
    We verify the cleanup occurred by checking delete_session was called and
    the returned dict indicates failure.
    """

    @pytest.mark.asyncio
    async def test_session_execute_failure_cleans_up_session(
        self, sandbox, mock_runtime
    ):
        # Make create_session succeed but session_execute fail
        mock_runtime.create_session.return_value = None
        mock_runtime.session_execute.side_effect = Exception("execute failed")
        mock_runtime.delete_session.return_value = None

        result = await sandbox.execute_bash_command(
            "sleep 100", background=True
        )

        # The outer except catches and returns an error dict
        assert result["success"] is False
        assert result["exit_code"] == -1
        # Verify the session was cleaned up after execute failure
        mock_runtime.delete_session.assert_called()

    @pytest.mark.asyncio
    async def test_session_cleanup_failure_does_not_mask_original_error(
        self, sandbox, mock_runtime
    ):
        mock_runtime.create_session.return_value = None
        mock_runtime.session_execute.side_effect = Exception("execute failed")
        mock_runtime.delete_session.side_effect = Exception("cleanup also failed")

        result = await sandbox.execute_bash_command(
            "sleep 100", background=True
        )

        # Should still return error dict even when cleanup also fails
        assert result["success"] is False
        assert "execute failed" in result["stderr"] or result["exit_code"] == -1


# ===================================================================
# Part 2: _preview_redirect endpoint unit tests
# ===================================================================


def _make_workspace(status="running", **overrides):
    ws = {
        "id": "ws-test-001",
        "user_id": "test-user-123",
        "workspace_id": "ws-test-001",
        "status": status,
        "sandbox_id": "sb-123",
        "created_at": "2026-01-01T00:00:00Z",
    }
    ws.update(overrides)
    return ws


@pytest.fixture
def mock_sandbox_for_endpoint():
    sandbox = AsyncMock()
    sandbox.sandbox_id = "sb-123"
    sandbox.start_and_get_preview_url = AsyncMock(
        return_value=PreviewInfo(
            url="https://preview.example.com/signed?token=abc", token="abc"
        )
    )
    sandbox.get_preview_url = AsyncMock(
        return_value=PreviewInfo(
            url="https://preview.example.com/signed?token=abc", token="abc"
        )
    )
    return sandbox


@pytest.fixture
def mock_session_for_endpoint(mock_sandbox_for_endpoint):
    session = MagicMock()
    session.sandbox = mock_sandbox_for_endpoint
    return session


class TestPreviewRedirectEndpoint:
    """Tests for the _preview_redirect function via the GET endpoints."""

    @pytest.mark.asyncio
    async def test_workspace_not_found_returns_404(self):
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        with patch(
            "src.server.app.workspace_sandbox.db_get_workspace",
            AsyncMock(return_value=None),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-nonexistent/8080",
                    follow_redirects=False,
                )
                assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_workspace_stopped_returns_404(self):
        """Stopped workspaces return 404 (same as missing) to avoid leaking existence."""
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        with patch(
            "src.server.app.workspace_sandbox.db_get_workspace",
            AsyncMock(return_value=_make_workspace(status="stopped")),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080",
                    follow_redirects=False,
                )
                assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_happy_path_running_returns_302_redirect(
        self, mock_session_for_endpoint
    ):
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            return_value=mock_session_for_endpoint
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(
                    return_value="https://preview.example.com/signed?token=abc"
                ),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080",
                    follow_redirects=False,
                )
                assert resp.status_code == 302
                assert "preview.example.com" in resp.headers["location"]
                assert resp.headers.get("cache-control") == (
                    "no-store, no-cache, must-revalidate"
                )

    @pytest.mark.asyncio
    async def test_path_suffix_appended(self, mock_session_for_endpoint):
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            return_value=mock_session_for_endpoint
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(
                    return_value="https://preview.example.com/base?token=abc"
                ),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080/timeline.html",
                    follow_redirects=False,
                )
                assert resp.status_code == 302
                location = resp.headers["location"]
                assert "/timeline.html" in location

    @pytest.mark.asyncio
    async def test_path_traversal_blocked(self, mock_session_for_endpoint):
        """Test that '..' segments in the path are rejected with 400.

        httpx normalizes URLs before sending, so we call _preview_redirect
        directly to ensure the path traversal check is exercised.
        """
        from fastapi import HTTPException

        from src.server.app.workspace_sandbox import _preview_redirect

        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            return_value=mock_session_for_endpoint
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
            patch(
                "src.server.app.workspace_sandbox._resolve_preview",
                AsyncMock(
                    return_value="https://preview.example.com/base?token=abc"
                ),
            ),
        ):
            MockWM.get_instance.return_value = mock_manager

            with pytest.raises(HTTPException) as exc_info:
                await _preview_redirect(
                    "ws-test-001", 8080, "foo/../../../etc/passwd"
                )
            assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_timeout_returns_504(self):
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        mock_manager = MagicMock()

        async def slow_get_session(*args, **kwargs):
            await asyncio.sleep(60)

        mock_manager.get_session_for_workspace = slow_get_session

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
            # Patch the timeout to be very short for the test
            patch(
                "src.server.app.workspace_sandbox._preview_redirect",
                wraps=None,
            ) as mock_redirect,
        ):
            # Instead of wrapping, we directly test the timeout behavior
            # by calling the real function with a mocked slow inner resolve
            pass

        # Better approach: test via the actual endpoint with a patched timeout
        # We need to make _resolve() take longer than the asyncio.wait_for timeout

        async def slow_resolve_preview(*args, **kwargs):
            await asyncio.sleep(60)
            return "https://never.reached"

        mock_manager2 = MagicMock()
        mock_manager2.get_session_for_workspace = slow_get_session

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
            patch(
                "src.server.app.workspace_sandbox.asyncio.wait_for",
                side_effect=asyncio.TimeoutError,
            ),
        ):
            MockWM.get_instance.return_value = mock_manager2

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080",
                    follow_redirects=False,
                )
                assert resp.status_code == 504

    @pytest.mark.asyncio
    async def test_sandbox_not_ready_returns_503(self):
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            side_effect=Exception("sandbox not ready")
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
        ):
            MockWM.get_instance.return_value = mock_manager

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080",
                    follow_redirects=False,
                )
                assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_sandbox_attribute_none_returns_503(self):
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)

        mock_session = MagicMock()
        mock_session.sandbox = None

        mock_manager = MagicMock()
        mock_manager.get_session_for_workspace = AsyncMock(
            return_value=mock_session
        )

        with (
            patch(
                "src.server.app.workspace_sandbox.db_get_workspace",
                AsyncMock(return_value=_make_workspace(status="running")),
            ),
            patch(
                "src.server.app.workspace_sandbox.WorkspaceManager"
            ) as MockWM,
        ):
            MockWM.get_instance.return_value = mock_manager

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080",
                    follow_redirects=False,
                )
                assert resp.status_code == 503
