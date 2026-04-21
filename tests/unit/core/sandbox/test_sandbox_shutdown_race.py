"""
Tests for sandbox shutdown race condition fix.

Verifies that:
- cleanup_idle_workspaces skips workspaces with active workflows
- BackgroundTaskManager.has_active_tasks_for_workspace works correctly
- _init_task is cancelled during stop_sandbox() and cleanup()
"""

import asyncio
from datetime import datetime, timedelta, timezone
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
    CodeRunResult,
    ExecResult,
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
)
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)


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


@pytest.fixture
def mock_runtime():
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.id = "mock-runtime-1"
    runtime.working_dir = "/home/workspace"
    runtime.exec = AsyncMock(return_value=ExecResult("output", "", 0))
    runtime.upload_file = AsyncMock()
    runtime.upload_files = AsyncMock()
    runtime.download_file = AsyncMock(return_value=b"data")
    runtime.list_files = AsyncMock(
        return_value=[{"name": "file.txt", "is_dir": False}]
    )
    runtime.code_run = AsyncMock(return_value=CodeRunResult("result", "", 0, []))
    runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
    runtime.start = AsyncMock()
    runtime.stop = AsyncMock()
    runtime.delete = AsyncMock()
    return runtime


@pytest.fixture
def mock_provider(mock_runtime):
    provider = AsyncMock(spec=SandboxProvider)
    provider.create = AsyncMock(return_value=mock_runtime)
    provider.get = AsyncMock(return_value=mock_runtime)
    provider.close = AsyncMock()
    provider.is_transient_error = MagicMock(return_value=False)
    return provider


class TestHasActiveTasksForWorkspace:
    """BackgroundTaskManager.has_active_tasks_for_workspace returns correct results."""

    @pytest.mark.asyncio
    async def test_no_tasks(self):
        mgr = BackgroundTaskManager()
        assert await mgr.has_active_tasks_for_workspace("ws-1") is False

    @pytest.mark.asyncio
    async def test_running_task_matches(self):
        mgr = BackgroundTaskManager()
        mgr.tasks["thread-1"] = TaskInfo(
            thread_id="thread-1",
            status=TaskStatus.RUNNING,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1"},
        )
        assert await mgr.has_active_tasks_for_workspace("ws-1") is True

    @pytest.mark.asyncio
    async def test_queued_task_matches(self):
        mgr = BackgroundTaskManager()
        mgr.tasks["thread-1"] = TaskInfo(
            thread_id="thread-1",
            status=TaskStatus.QUEUED,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1"},
        )
        assert await mgr.has_active_tasks_for_workspace("ws-1") is True

    @pytest.mark.asyncio
    async def test_completed_task_does_not_match(self):
        mgr = BackgroundTaskManager()
        mgr.tasks["thread-1"] = TaskInfo(
            thread_id="thread-1",
            status=TaskStatus.COMPLETED,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1"},
        )
        assert await mgr.has_active_tasks_for_workspace("ws-1") is False

    @pytest.mark.asyncio
    async def test_soft_interrupted_task_matches(self):
        mgr = BackgroundTaskManager()
        mgr.tasks["thread-1"] = TaskInfo(
            thread_id="thread-1",
            status=TaskStatus.SOFT_INTERRUPTED,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1"},
        )
        assert await mgr.has_active_tasks_for_workspace("ws-1") is True

    @pytest.mark.asyncio
    async def test_different_workspace_does_not_match(self):
        mgr = BackgroundTaskManager()
        mgr.tasks["thread-1"] = TaskInfo(
            thread_id="thread-1",
            status=TaskStatus.RUNNING,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-other"},
        )
        assert await mgr.has_active_tasks_for_workspace("ws-1") is False


class TestCleanupIdleWorkspacesGuard:
    """cleanup_idle_workspaces skips workspaces with active workflows."""

    @pytest.mark.asyncio
    async def test_skips_workspace_with_active_task(self):
        from ptc_agent.config import AgentConfig

        config = MagicMock(spec=AgentConfig)
        from src.server.services.workspace_manager import WorkspaceManager

        mgr = WorkspaceManager(config=config, idle_timeout=1800)

        stale_time = datetime.now(timezone.utc) - timedelta(seconds=3600)
        workspace = {
            "workspace_id": "ws-active",
            "last_activity_at": stale_time,
        }

        # Patch DB query to return one "idle" workspace
        with (
            patch(
                "src.server.services.workspace_manager.get_workspaces_by_status",
                new_callable=AsyncMock,
                return_value=[workspace],
            ),
            patch.object(mgr, "stop_workspace", new_callable=AsyncMock) as mock_stop,
            patch(
                "src.server.services.background_task_manager.BackgroundTaskManager.get_instance"
            ) as mock_get_instance,
        ):
            mock_instance = MagicMock()
            mock_instance.has_active_tasks_for_workspace = AsyncMock(return_value=True)
            mock_get_instance.return_value = mock_instance

            stopped = await mgr.cleanup_idle_workspaces()

        assert stopped == 0
        mock_stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_stops_workspace_without_active_task(self):
        from ptc_agent.config import AgentConfig

        config = MagicMock(spec=AgentConfig)
        from src.server.services.workspace_manager import WorkspaceManager

        mgr = WorkspaceManager(config=config, idle_timeout=1800)

        stale_time = datetime.now(timezone.utc) - timedelta(seconds=3600)
        workspace = {
            "workspace_id": "ws-idle",
            "last_activity_at": stale_time,
        }

        with (
            patch(
                "src.server.services.workspace_manager.get_workspaces_by_status",
                new_callable=AsyncMock,
                return_value=[workspace],
            ),
            patch.object(mgr, "stop_workspace", new_callable=AsyncMock) as mock_stop,
            patch(
                "src.server.services.background_task_manager.BackgroundTaskManager.get_instance"
            ) as mock_get_instance,
        ):
            mock_instance = MagicMock()
            mock_instance.has_active_tasks_for_workspace = AsyncMock(return_value=False)
            mock_get_instance.return_value = mock_instance

            stopped = await mgr.cleanup_idle_workspaces()

        assert stopped == 1
        mock_stop.assert_called_once_with("ws-idle")


class TestSessionClosedIsTransient:
    """DaytonaProvider classifies closed-client errors as transient."""

    def test_session_is_closed(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        exc = Exception("DaytonaError: Session is closed: Daytona client is closed")
        assert provider.is_transient_error(exc) is True

    def test_client_is_closed(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        exc = Exception("client is closed")
        assert provider.is_transient_error(exc) is True

    def test_execute_command_with_closed_session_is_transient(self):
        """The 'failed to execute command' guard must not block closed-client errors."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        exc = Exception(
            "Failed to execute command: Session is closed: "
            "Daytona client is closed"
        )
        assert provider.is_transient_error(exc) is True

    def test_execution_error_still_not_transient(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        exc = Exception("Failed to execute command: exit code 1")
        assert provider.is_transient_error(exc) is False


class TestEnsureSandboxConnectedRecreatesProvider:
    """_ensure_sandbox_connected always recreates provider for reconnect."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_provider_recreated_on_reconnect(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        sandbox.sandbox_id = "existing-sandbox"

        # Fresh provider for the reconnect
        fresh_provider = AsyncMock(spec=SandboxProvider)
        fresh_provider.get = AsyncMock(return_value=mock_runtime)
        fresh_provider.close = AsyncMock()
        fresh_provider.is_transient_error = MagicMock(return_value=False)
        mock_create_provider.return_value = fresh_provider

        await sandbox._ensure_sandbox_connected()

        # Provider was recreated (once at __init__, once in _ensure_sandbox_connected)
        assert sandbox.provider is fresh_provider
        assert mock_create_provider.call_count == 2


class TestEnsureSandboxConnectedNoFutureLeak:
    """After Fix 3, _ensure_sandbox_connected has no asyncio.Future coalescing
    primitive, so a failing reconnect cannot produce a 'Future exception was
    never retrieved' warning. Serialization is still enforced via the lock."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_no_future_leak_on_reconnect_failure(
        self, mock_create_provider, mock_provider, mock_runtime, caplog
    ):
        import gc
        import logging
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        sandbox.sandbox_id = "existing-sandbox"

        fresh_provider = AsyncMock(spec=SandboxProvider)
        fresh_provider.close = AsyncMock()
        mock_create_provider.return_value = fresh_provider

        sandbox.reconnect = AsyncMock(
            side_effect=RuntimeError("reconnect bombed")
        )

        caplog.set_level(logging.WARNING, logger="asyncio")

        with pytest.raises(RuntimeError, match="reconnect bombed"):
            await sandbox._ensure_sandbox_connected()

        # Force GC; if there were a stranded Future with an unretrieved
        # exception, the asyncio debug machinery would log here.
        gc.collect()
        await asyncio.sleep(0)

        assert not any(
            "Future exception was never retrieved" in rec.getMessage()
            for rec in caplog.records
        ), "Fix 3 regression: Future coalescing primitive reintroduced"

        # No private state survives the failed attempt.
        assert not hasattr(sandbox, "_reconnect_inflight")

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_concurrent_callers_serialized_by_lock(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Two concurrent _ensure_sandbox_connected calls each invoke reconnect
        serially (the lock was the only real serializer — Fix 3 removed the
        dead Future that pretended to coalesce)."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        sandbox.sandbox_id = "existing-sandbox"

        fresh_provider = AsyncMock(spec=SandboxProvider)
        fresh_provider.close = AsyncMock()
        mock_create_provider.return_value = fresh_provider

        running = 0
        peak_concurrency = 0

        async def tracked_reconnect(sid):
            nonlocal running, peak_concurrency
            running += 1
            peak_concurrency = max(peak_concurrency, running)
            await asyncio.sleep(0.01)
            running -= 1

        sandbox.reconnect = AsyncMock(side_effect=tracked_reconnect)

        await asyncio.gather(
            sandbox._ensure_sandbox_connected(),
            sandbox._ensure_sandbox_connected(),
        )

        assert sandbox.reconnect.await_count == 2
        assert peak_concurrency == 1, (
            "Lock must serialize concurrent reconnects; saw "
            f"peak_concurrency={peak_concurrency}"
        )


class TestInitTaskCancellation:
    """_init_task is cancelled during stop_sandbox() and cleanup()."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_init_task_cancelled_on_stop(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        started = asyncio.Event()

        async def slow_reconnect(sid):
            started.set()
            await asyncio.sleep(60)

        sandbox._ready_event = asyncio.Event()
        sandbox._init_task = asyncio.create_task(slow_reconnect("test"))
        await started.wait()

        await sandbox.stop_sandbox()

        assert sandbox._init_task is None

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_init_task_cancelled_on_cleanup(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        started = asyncio.Event()

        async def slow_reconnect(sid):
            started.set()
            await asyncio.sleep(60)

        sandbox._ready_event = asyncio.Event()
        sandbox._init_task = asyncio.create_task(slow_reconnect("test"))
        await started.wait()

        await sandbox.cleanup()

        assert sandbox._init_task is None
