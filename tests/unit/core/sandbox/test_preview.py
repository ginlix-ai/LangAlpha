"""
Tests for PTCSandbox preview methods and workspace_sandbox preview redirect endpoint.

Part 1: PTCSandbox unit tests covering the preview server lifecycle,
         reachability checks, and leak fixes.
Part 2: preview URL resolution, and the legacy preview redirect to the
         app's ``/a/`` link.
"""

import asyncio
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import ANY, AsyncMock, MagicMock, patch

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
    runtime.session_execute = AsyncMock(return_value="cmd-001")
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


class TestStartPreviewServer:
    """Tests for PTCSandbox.start_preview_server."""

    @pytest.mark.asyncio
    async def test_happy_path_creates_per_port_session(self, sandbox, mock_runtime):
        mock_runtime.session_execute.return_value = "preview-cmd-1"

        cmd_id = await sandbox.start_preview_server("python -m http.server 8080", 8080)

        assert cmd_id == "preview-cmd-1"
        mock_runtime.create_session.assert_called_once()
        # Verify the session is stored correctly
        assert 8080 in sandbox._preview_sessions
        session_id, stored_cmd_id = sandbox._preview_sessions[8080]
        assert session_id.startswith("preview-8080-")
        assert stored_cmd_id == "preview-cmd-1"

    @pytest.mark.asyncio
    async def test_stale_session_teardown(self, sandbox, mock_runtime):
        # Pre-populate a stale session for port 8080
        sandbox._preview_sessions[8080] = ("preview-8080", "old-cmd")

        mock_runtime.session_execute.return_value = "new-cmd"

        cmd_id = await sandbox.start_preview_server("python app.py", 8080)

        assert cmd_id == "new-cmd"
        # delete_session should have been called for the stale session
        assert mock_runtime.delete_session.call_count >= 1
        # Verify updated preview sessions
        assert sandbox._preview_sessions[8080][0].startswith("preview-8080-")
        assert sandbox._preview_sessions[8080][1] == "new-cmd"

    @pytest.mark.asyncio
    async def test_stale_session_cleanup_failure_continues(
        self, sandbox, mock_runtime
    ):
        sandbox._preview_sessions[8080] = ("preview-8080", "old-cmd")
        # delete_session fails but we should continue
        mock_runtime.delete_session.side_effect = Exception("cleanup failed")

        mock_runtime.session_execute.return_value = "new-cmd"

        cmd_id = await sandbox.start_preview_server("python app.py", 8080)
        assert cmd_id == "new-cmd"

    @pytest.mark.asyncio
    async def test_already_exists_on_create_continues(self, sandbox, mock_runtime):
        mock_runtime.create_session.side_effect = Exception(
            "Session already exists for this sandbox"
        )
        mock_runtime.session_execute.return_value = "reused-cmd"

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


# ===================================================================
# Part 2: preview resolution and the legacy redirect
# ===================================================================


def _make_workspace(status="running", **overrides):
    ws = {
        "id": "ws-test-001",
        "user_id": "test-user-123",
        "workspace_id": "ws-test-001",
        "status": status,
        "sandbox_id": "sb-123",
        "computer_root_dir": "/home/workspace",
        "dir_name": "project-a",
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


class TestWorkspaceScopedPreviewCommands:
    @pytest.fixture(autouse=True)
    def _available_preview_coordination(self):
        cache = MagicMock()
        cache.acquire_lock = AsyncMock(return_value=True)
        cache.release_lock = AsyncMock()
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock(return_value=True)
        cache.delete = AsyncMock()
        with patch(
            "src.server.services.preview_links.get_cache_client",
            return_value=cache,
        ):
            yield

    @pytest.mark.asyncio
    async def test_stored_command_restarts_inside_the_workspace_folder(
        self, mock_sandbox_for_endpoint
    ):
        from src.server.services.preview_links import resolve_preview

        with patch(
            "src.server.services.preview_links._set_cached_signed_url",
            AsyncMock(),
        ):
            await resolve_preview(
                mock_sandbox_for_endpoint,
                "ws-test-001",
                8080,
                command="python -m http.server 8080",
                force=True,
                work_dir="/home/workspace/project-a",
            )

        mock_sandbox_for_endpoint.start_and_get_preview_url.assert_awaited_once_with(
            "cd /home/workspace/project-a && python -m http.server 8080",
            8080,
            expires_in=3600,
            owner="ws-test-001",
        )

    @staticmethod
    def _held_folder(sandbox, events, dir_name="project-a"):
        """The folder hold and the row read under it, each recorded, with every
        launch recorded by the folder it starts in."""

        @asynccontextmanager
        async def hold(_workspace_id):
            events.append("hold")
            try:
                yield
            finally:
                events.append("release")

        async def read(_workspace_id):
            events.append("read")
            return _make_workspace(dir_name=dir_name)

        def launched(returns=None):
            async def launch(cmd, *_args, **_kwargs):
                events.append(f"launch {cmd.split(' && ')[0]}")
                return returns
            return launch

        sandbox.start_preview_server.side_effect = launched()
        sandbox.start_and_get_preview_url.side_effect = launched(
            sandbox.start_and_get_preview_url.return_value
        )
        return (
            patch("src.server.app.workspace_sandbox._get_sandbox",
                  AsyncMock(return_value=(MagicMock(), sandbox))),
            patch("src.server.services.preview_links._set_cached_signed_url", AsyncMock()),
            patch("src.server.services.workspace_files.workspace_folder_in_use", hold),
            # Every read of the row, the owner check's included, goes through
            # here, so one outside the hold shows as out of order rather than
            # missing.
            patch("src.server.services.workspace_files.db_get_workspace", read),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("route", ["restart", "open"])
    async def test_a_launch_starts_in_the_folder_read_under_the_hold(
        self, mock_sandbox_for_endpoint, route
    ):
        """A settle on any worker moves only a folder it can hold. Read before
        the hold, the folder could move and a sibling land on its old name,
        and the server would start in the sibling's folder."""
        from src.server.app.workspace_sandbox import (
            PreviewRestartRequest,
            owner_preview_url,
            restart_preview_server,
        )

        events = []
        patches = self._held_folder(mock_sandbox_for_endpoint, events)
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            if route == "restart":
                response = await restart_preview_server(
                    "ws-test-001",
                    "test-user-123",
                    PreviewRestartRequest(port=8080, command="python -m http.server 8080"),
                )
                assert response.success is True
            else:
                await owner_preview_url(
                    "ws-test-001", "test-user-123", 8080,
                    command="python -m http.server 8080", force=True,
                )

        assert events == ["hold", "read", "launch cd /home/workspace/project-a", "release"]

    @pytest.mark.asyncio
    async def test_a_url_a_browser_cannot_open_is_cached_and_returned_as_published(
        self, mock_sandbox_for_endpoint
    ):
        """Whatever reaches the panel or the cache is publish's answer, never
        the raw sandbox host that needs a header."""
        from src.server.services.preview_links import resolve_preview

        private = PreviewInfo(
            url="https://8080-sb-123.e2b.app",
            token=None,
            auth_headers={"e2b-traffic-access-token": "tok"},
            browser_ready=False,
        )
        mock_sandbox_for_endpoint.get_preview_url = AsyncMock(return_value=private)
        capability = "https://8080-cap.preview.example.com/"
        with (
            patch(
                "src.server.services.preview_links.publish",
                AsyncMock(return_value=capability),
            ) as publish,
            patch(
                "src.server.services.preview_links._set_cached_signed_url", AsyncMock()
            ) as cache_url,
        ):
            url = await resolve_preview(
                mock_sandbox_for_endpoint,
                "ws-test-001",
                8080,
                command=None,
                work_dir="/home/workspace/project-a",
            )

        assert url == capability
        publish.assert_awaited_once_with(
            private,
            sandbox_id="sb-123",
            port=8080,
            workspace_id="ws-test-001",
            expires_in=3600,
        )
        assert cache_url.await_args.args[2] == capability

    @pytest.mark.asyncio
    async def test_a_computer_needing_the_proxy_where_there_is_none_is_501(
        self, mock_sandbox_for_endpoint, monkeypatch
    ):
        from fastapi import HTTPException

        from src.config import env
        from src.server.app.workspace_sandbox import owner_preview_url

        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "")
        mock_sandbox_for_endpoint.get_preview_url = AsyncMock(
            return_value=PreviewInfo(
                url="https://8080-sb-123.e2b.app",
                token=None,
                auth_headers={"e2b-traffic-access-token": "tok"},
                browser_ready=False,
            )
        )
        events = []
        with ExitStack() as stack:
            for p in self._held_folder(mock_sandbox_for_endpoint, events):
                stack.enter_context(p)
            with pytest.raises(HTTPException) as raised:
                await owner_preview_url(
                    "ws-test-001", "test-user-123", 8080, command=None
                )

        assert raised.value.status_code == 501
        assert "preview proxy" in raised.value.detail

    @pytest.mark.asyncio
    async def test_no_launch_starts_in_a_folder_left_mid_move(self, mock_sandbox_for_endpoint):
        from fastapi import HTTPException

        from src.server.app.workspace_sandbox import (
            PreviewRestartRequest,
            restart_preview_server,
        )

        events = []
        patches = self._held_folder(
            mock_sandbox_for_endpoint, events, dir_name="_internal/moving/ws-test-001"
        )
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            with pytest.raises(HTTPException) as raised:
                await restart_preview_server(
                    "ws-test-001",
                    "test-user-123",
                    PreviewRestartRequest(port=8080, command="python -m http.server 8080"),
                )

        assert raised.value.status_code == 503
        assert events == ["hold", "read", "release"]

    @pytest.mark.asyncio
    async def test_cache_miss_reuses_owned_preview_from_another_worker(
        self, mock_sandbox_for_endpoint
    ):
        from src.server.services.preview_links import resolve_preview

        mock_sandbox_for_endpoint._is_preview_reachable = AsyncMock(return_value=True)
        with (
            patch(
                "src.server.services.preview_links.get_preview_owner",
                AsyncMock(return_value="ws-test-001"),
            ),
            patch(
                "src.server.services.preview_links._get_cached_signed_url",
                AsyncMock(return_value=None),
            ),
            patch(
                "src.server.services.preview_links._set_cached_signed_url",
                AsyncMock(),
            ) as cache_url,
        ):
            url = await resolve_preview(
                mock_sandbox_for_endpoint,
                "ws-test-001",
                8080,
                command="python -m http.server 8080",
                work_dir="/home/workspace/project-a",
            )

        assert url == "https://preview.example.com/signed?token=abc"
        mock_sandbox_for_endpoint.start_and_get_preview_url.assert_not_awaited()
        cache_url.assert_awaited_once_with(
            "sb-123",
            8080,
            "https://preview.example.com/signed?token=abc",
            expires_in=60,
            owner_workspace_id="ws-test-001",
            url_expires_at=ANY,
        )

    @pytest.mark.asyncio
    async def test_cross_worker_launch_reserves_owner_before_provider_work(self):
        from src.server.services.preview_links import resolve_preview

        class Cache:
            def __init__(self):
                self.values = {}
                self.locks = {}

            async def acquire_lock(self, key, token, _ttl_ms):
                if key in self.locks:
                    return False
                self.locks[key] = token
                return True

            async def release_lock(self, key, token):
                if self.locks.get(key) == token:
                    self.locks.pop(key)

            async def get(self, key):
                return self.values.get(key)

            async def set(self, key, value, ttl=None):
                self.values[key] = value
                return True

            async def delete(self, key):
                self.values.pop(key, None)

        cache = Cache()
        launch_entered = asyncio.Event()
        finish_launch = asyncio.Event()
        first = AsyncMock()
        first.sandbox_id = "shared-sandbox"

        async def launch(*_args, **_kwargs):
            assert cache.values["preview:owner:shared-sandbox:8080"] == "workspace-a"
            launch_entered.set()
            await finish_launch.wait()
            return PreviewInfo(url="https://preview.example.com/a", token="a")

        first.start_and_get_preview_url = AsyncMock(side_effect=launch)
        second = AsyncMock()
        second.sandbox_id = "shared-sandbox"
        second.start_and_get_preview_url = AsyncMock()

        with (
            patch(
                "src.server.services.preview_links.get_cache_client",
                return_value=cache,
            ),
            patch(
                "src.server.services.preview_links._check_signed_url_healthy",
                AsyncMock(return_value=True),
            ),
        ):
            first_task = asyncio.create_task(resolve_preview(
                first,
                "workspace-a",
                8080,
                command="python -m http.server 8080",
                work_dir="/home/workspace/a",
            ))
            await launch_entered.wait()
            second_task = asyncio.create_task(resolve_preview(
                second,
                "workspace-b",
                8080,
                command="python -m http.server 8080",
                work_dir="/home/workspace/b",
            ))
            await asyncio.sleep(0.1)
            second.start_and_get_preview_url.assert_not_awaited()
            finish_launch.set()
            assert await first_task == "https://preview.example.com/a"
            with pytest.raises(RuntimeError, match="already in use"):
                await second_task

    @pytest.mark.asyncio
    async def test_failed_launch_releases_owner_reservation(
        self, mock_sandbox_for_endpoint
    ):
        from src.server.services.preview_links import resolve_preview

        cache = MagicMock()
        cache.acquire_lock = AsyncMock(return_value=True)
        cache.release_lock = AsyncMock()
        values = {}
        cache.get = AsyncMock(side_effect=lambda key: values.get(key))
        cache.set = AsyncMock(side_effect=lambda key, value, ttl=None: values.__setitem__(key, value) or True)
        cache.delete = AsyncMock(side_effect=lambda key: values.pop(key, None))
        mock_sandbox_for_endpoint.start_and_get_preview_url.side_effect = RuntimeError("boom")

        with patch(
            "src.server.services.preview_links.get_cache_client",
            return_value=cache,
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await resolve_preview(
                    mock_sandbox_for_endpoint,
                    "workspace-a",
                    8080,
                    command="python -m http.server 8080",
                    work_dir="/home/workspace/a",
                )

        assert "preview:owner:sb-123:8080" not in values
        cache.release_lock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_commandless_redirect_rejects_a_sibling_preview(
        self, mock_sandbox_for_endpoint
    ):
        from src.server.services.preview_links import resolve_preview

        mock_sandbox_for_endpoint._is_preview_reachable = AsyncMock(return_value=True)
        with (
            patch(
                "src.server.services.preview_links.get_preview_owner",
                AsyncMock(return_value="workspace-b"),
            ),
            patch(
                "src.server.services.preview_links._get_cached_signed_url",
                AsyncMock(return_value="https://preview.example.com/b"),
            ),
        ):
            with pytest.raises(RuntimeError, match="already in use"):
                await resolve_preview(
                    mock_sandbox_for_endpoint,
                    "workspace-a",
                    8080,
                    command=None,
                    work_dir="/home/workspace/a",
                )

        mock_sandbox_for_endpoint.get_preview_url.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_launch_fails_closed_without_preview_coordination(
        self, mock_sandbox_for_endpoint
    ):
        from src.server.services.preview_links import resolve_preview

        cache = MagicMock()
        cache.acquire_lock = AsyncMock(return_value=None)
        with patch(
            "src.server.services.preview_links.get_cache_client",
            return_value=cache,
        ):
            with pytest.raises(RuntimeError, match="coordination is unavailable"):
                await resolve_preview(
                    mock_sandbox_for_endpoint,
                    "workspace-a",
                    8080,
                    command="python -m http.server 8080",
                    work_dir="/home/workspace/a",
                )

        mock_sandbox_for_endpoint.start_and_get_preview_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_signed_urls_expiry_is_counted_from_its_mint_not_from_the_read():
    """The ``/a/`` page renews an app's URL off this, and a cached URL is older
    than the request that gets it back."""
    from src.server.services import preview_links as links

    store: dict = {}
    cache = MagicMock()
    cache.set = AsyncMock(side_effect=lambda key, value, ttl=None: store.__setitem__(key, value))
    cache.get = AsyncMock(side_effect=lambda key: store.get(key))
    with (
        patch.object(links, "get_cache_client", return_value=cache),
        patch.object(links.time, "time", return_value=1_000_000),
    ):
        await links._set_cached_signed_url("sb-1", 8080, "https://a", url_expires_at=1_003_600)
        await links._set_cached_signed_url("sb-1", 8081, "https://b")
        assert await links.signed_url_expires_at("https://a") == 1_003_600
        assert await links.signed_url_expires_at("https://b") == 1_000_000 + 3600
        # Cached by a worker that kept no record: the cache's own margin.
        assert await links.signed_url_expires_at("https://c") == 1_000_000 + 600


class TestPreviewRedirectEndpoint:
    """The legacy ``/api/v1/preview/{ws}/{port}`` route: a redirect to the
    app's ``/a/`` link that never touches a session or the sandbox."""

    _COMMAND = "src.server.app.workspace_sandbox.get_preview_command"
    _LINK = "src.server.app.workspace_sandbox.ensure_app_link"
    _WSMGR = "src.server.app.workspace_sandbox.WorkspaceManager"

    @staticmethod
    def _client():
        from httpx import ASGITransport, AsyncClient

        from src.server.app.workspace_sandbox import preview_redirect_router
        from tests.conftest import create_test_app

        app = create_test_app(preview_redirect_router)
        return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

    @pytest.mark.asyncio
    async def test_no_server_on_the_port_returns_404(self):
        """An unknown workspace and an unregistered port read the same."""
        with (
            patch(self._COMMAND, AsyncMock(return_value=None)),
            patch(self._LINK, AsyncMock()) as link,
        ):
            async with self._client() as client:
                resp = await client.get(
                    "/api/v1/preview/ws-test-001/8080", follow_redirects=False
                )
        assert resp.status_code == 404
        link.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("suffix", "query"),
        [
            ("", "?path=/"),
            ("/timeline.html", "?path=timeline.html"),
            ("/reports/q3.html", "?path=reports/q3.html"),
        ],
    )
    async def test_registered_port_redirects_to_the_link_without_a_session(self, suffix, query):
        """#378: the redirect never acquires a session, so a manager and a
        sandbox lookup that both blow up leave the answer untouched."""
        from src.config.env import PUBLIC_APP_URL
        from src.server.database.share_links import ShareLink

        link = MagicMock(spec=ShareLink, code="k3Vq9ZtR2mXa")
        with (
            patch(self._COMMAND, AsyncMock(return_value="python -m http.server 8080")),
            patch(self._LINK, AsyncMock(return_value=link)) as ensure,
            patch(self._WSMGR) as manager,
            patch(
                "src.server.app.workspace_sandbox._get_sandbox",
                AsyncMock(side_effect=RuntimeError("no session for you")),
            ),
        ):
            manager.get_instance.side_effect = RuntimeError("manager is down")
            async with self._client() as client:
                resp = await client.get(
                    f"/api/v1/preview/ws-test-001/8080{suffix}", follow_redirects=False
                )
        assert resp.status_code == 302
        # The old URL's suffix rides the redirect; it is never stored on the
        # link, which anyone holding the UUID could otherwise choose.
        assert resp.headers["location"] == f"{PUBLIC_APP_URL}/a/k3Vq9ZtR2mXa{query}"
        assert resp.headers["cache-control"] == "no-store"
        ensure.assert_awaited_once_with("ws-test-001", 8080)
        manager.get_instance.assert_not_called()


_SIGNED = "https://8050-sbx.proxy.test/?token=abc"


@pytest.mark.parametrize(
    ("page", "expected"),
    [
        (None, _SIGNED),
        ("/", _SIGNED),
        ("reports/q3.html", "https://8050-sbx.proxy.test/reports/q3.html?token=abc"),
        # The page's query follows the signed one, which is what admits the request.
        ("dashboard?tab=positions", "https://8050-sbx.proxy.test/dashboard?token=abc&tab=positions"),
        ("dashboard#top", "https://8050-sbx.proxy.test/dashboard?token=abc#top"),
        ("/?tab=a b", "https://8050-sbx.proxy.test/?token=abc&tab=a+b"),
    ],
)
def test_a_page_opens_inside_the_signed_url_without_displacing_its_token(page, expected):
    from src.server.app.workspace_sandbox import with_preview_path

    assert with_preview_path(_SIGNED, page) == expected


@pytest.mark.asyncio
async def test_occupied_preview_port_never_returns_sibling_url(sandbox, mock_runtime):
    mock_runtime.exec.return_value = MagicMock(exit_code=0)
    with pytest.raises(RuntimeError, match="already in use"):
        await sandbox.start_and_get_preview_url("python -m http.server 8080", 8080)
    mock_runtime.session_execute.assert_not_awaited()
    mock_runtime.get_preview_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_occupied_preview_port_reuses_the_owning_workspace_server(
    sandbox, mock_runtime
):
    sandbox._preview_sessions[8080] = ("preview-8080", "cmd-1")
    sandbox._preview_owners[8080] = "ws-1"
    mock_runtime.exec.return_value = MagicMock(exit_code=0)
    with patch.object(sandbox, "_is_preview_reachable", AsyncMock(return_value=True)):
        result = await sandbox.start_and_get_preview_url(
            "python -m http.server 8080", 8080, owner="ws-1"
        )

    assert result.url == "https://preview.example.com/signed"
    mock_runtime.session_execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_occupied_preview_port_rejects_a_sibling_workspace(
    sandbox, mock_runtime
):
    sandbox._preview_sessions[8080] = ("preview-8080", "cmd-1")
    sandbox._preview_owners[8080] = "ws-1"
    mock_runtime.exec.return_value = MagicMock(exit_code=0)
    with pytest.raises(RuntimeError, match="already in use"):
        await sandbox.start_and_get_preview_url(
            "python -m http.server 8080", 8080, owner="ws-2"
        )
    mock_runtime.get_preview_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_preview_command_never_returns_an_existing_server(sandbox, mock_runtime):
    mock_runtime.exec.side_effect = [MagicMock(exit_code=1), MagicMock(stdout="READY")]
    mock_runtime.session_command_logs.return_value = SessionCommandResult(
        cmd_id="cmd-001", exit_code=1, stdout="", stderr="Address already in use"
    )
    with pytest.raises(RuntimeError, match="Address already in use"):
        await sandbox.start_and_get_preview_url("python -m http.server 8080", 8080)
    mock_runtime.get_preview_url.assert_not_awaited()
