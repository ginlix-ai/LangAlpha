"""
Tests for sandbox shutdown race condition fix.

Verifies that:
- cleanup_idle_workspaces skips workspaces with active workflows
- LocalRunExecutor.has_active_tasks_for_workspace works correctly
- _init_task is cancelled during stop_sandbox() and cleanup()
"""

import asyncio
import contextlib
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
    ExecResult,
    RuntimeState,
    SandboxHostUnavailableError,
    SandboxRuntime,
)
from src.server.services.runs.executor import (
    LocalRunExecutor,
    LocalRunExecution,
    LocalRunStatus,
)
from tests.unit.core.sandbox.fake_provider import fake_provider


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
    # A real path, not the spec'd AsyncMock's return: reconnect stores this as
    # the computer root and everything downstream joins paths onto it.
    runtime.fetch_working_dir = AsyncMock(return_value="/home/workspace")
    runtime.exec = AsyncMock(return_value=ExecResult("output", "", 0))
    runtime.upload_file = AsyncMock()
    runtime.upload_files = AsyncMock()
    runtime.download_file = AsyncMock(return_value=b"data")
    runtime.list_files = AsyncMock(
        return_value=[{"name": "file.txt", "is_dir": False}]
    )
    runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
    runtime.start = AsyncMock()
    runtime.stop = AsyncMock()
    runtime.delete = AsyncMock()
    return runtime


@pytest.fixture
def mock_provider(mock_runtime):
    provider = fake_provider()
    provider.create = AsyncMock(return_value=mock_runtime)
    provider.get = AsyncMock(return_value=mock_runtime)
    provider.close = AsyncMock()
    provider.is_transient_error = MagicMock(return_value=False)
    return provider


@contextlib.contextmanager
def _durable_probes(*, root_active=False, open_task_runs=0, error=None):
    """Patch the guard's ledger reads (root-run rows + subagent runs)."""
    root = AsyncMock(return_value=root_active)
    if error is not None:
        root.side_effect = error
    with (
        patch(
            "src.server.database.runs.lifecycle.workspace_has_active_run",
            new=root,
        ),
        patch(
            "src.server.database.runs.subagent_runs.count_open_runs_for_workspace",
            new=AsyncMock(return_value=open_task_runs),
        ),
    ):
        yield


class TestHasActiveTasksForWorkspace:
    """LocalRunExecutor.has_active_tasks_for_workspace returns correct results."""

    @pytest.mark.asyncio
    async def test_no_tasks(self):
        mgr = LocalRunExecutor()
        with _durable_probes():
            assert await mgr.has_active_tasks_for_workspace("ws-1") is False

    @pytest.mark.asyncio
    async def test_running_task_matches(self):
        mgr = LocalRunExecutor()
        mgr.executions[("thread-1", "run-1")] = LocalRunExecution(
            run_id="run-1",
            thread_id="thread-1",
            status=LocalRunStatus.RUNNING,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1"},
        )
        assert await mgr.has_active_tasks_for_workspace("ws-1") is True

    @pytest.mark.asyncio
    async def test_completed_task_does_not_match(self):
        mgr = LocalRunExecutor()
        mgr.executions[("thread-1", "run-1")] = LocalRunExecution(
            run_id="run-1",
            thread_id="thread-1",
            status=LocalRunStatus.COMPLETED,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1"},
        )
        with _durable_probes():
            assert await mgr.has_active_tasks_for_workspace("ws-1") is False

    @pytest.mark.asyncio
    async def test_different_workspace_does_not_match(self):
        mgr = LocalRunExecutor()
        mgr.executions[("thread-1", "run-1")] = LocalRunExecution(
            run_id="run-1",
            thread_id="thread-1",
            status=LocalRunStatus.RUNNING,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-other"},
        )
        with _durable_probes():
            assert await mgr.has_active_tasks_for_workspace("ws-1") is False

    @pytest.mark.asyncio
    async def test_remote_root_run_counts(self):
        """An in_progress response row on another worker's thread keeps the
        workspace active even with an empty local task map."""
        mgr = LocalRunExecutor()
        with _durable_probes(root_active=True):
            assert await mgr.has_active_tasks_for_workspace("ws-1") is True

    @pytest.mark.asyncio
    async def test_open_subagent_run_counts(self):
        """A tail-mode subagent (no root run anywhere) still holds the
        sandbox via its in_progress ledger row."""
        mgr = LocalRunExecutor()
        with _durable_probes(open_task_runs=1):
            assert await mgr.has_active_tasks_for_workspace("ws-1") is True

    @pytest.mark.asyncio
    async def test_probe_failure_counts_as_active(self):
        """Fail closed: the callers gate sandbox teardown, so an unreadable
        ledger must defer, never destroy."""
        mgr = LocalRunExecutor()
        with _durable_probes(error=RuntimeError("db down")):
            assert await mgr.has_active_tasks_for_workspace("ws-1") is True


class TestCleanupIdleWorkspacesGuard:
    """cleanup_idle_workspaces skips machines with active workflows.

    The reaper walks computers, so the gate it consults is the machine-wide
    one: the sandbox belongs to the computer, and one idle project on it is
    not a reason to take it from a sibling that is mid-turn.
    """

    @staticmethod
    def _idle_machine(computer_id: str) -> dict:
        return {
            "computer_id": computer_id,
            "user_id": "user-1",
            "kind": "daytona",
            "is_always_on": False,
            "last_activity_at": datetime.now(timezone.utc) - timedelta(seconds=3600),
            "updated_at": datetime.now(timezone.utc) - timedelta(seconds=3600),
        }

    @pytest.mark.asyncio
    async def test_skips_machine_with_active_task(self):
        from ptc_agent.config import AgentConfig

        config = MagicMock(spec=AgentConfig)
        config.to_core_config.return_value.sandbox = SandboxConfig()
        from src.server.services.workspace_manager import WorkspaceManager

        mgr = WorkspaceManager(config=config)

        with (
            patch(
                "src.server.services.workspace_manager.get_computers_by_status",
                new_callable=AsyncMock,
                return_value=[self._idle_machine("comp-active")],
            ),
            patch.object(mgr, "_stop_machine", new_callable=AsyncMock) as mock_stop,
            patch(
                "src.server.services.runs.executor.LocalRunExecutor.get_instance"
            ) as mock_get_instance,
        ):
            mock_instance = MagicMock()
            mock_instance.has_active_tasks_for_computer = AsyncMock(return_value=True)
            mock_get_instance.return_value = mock_instance

            stopped = await mgr.cleanup_idle_workspaces()

        assert stopped == 0
        mock_stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_stops_machine_without_active_task(self):
        from ptc_agent.config import AgentConfig

        config = MagicMock(spec=AgentConfig)
        config.to_core_config.return_value.sandbox = SandboxConfig()
        from src.server.services.workspace_manager import WorkspaceManager

        mgr = WorkspaceManager(config=config)
        machine = self._idle_machine("comp-idle")

        with (
            patch(
                "src.server.services.workspace_manager.get_computers_by_status",
                new_callable=AsyncMock,
                return_value=[machine],
            ),
            patch.object(mgr, "_stop_machine", new_callable=AsyncMock) as mock_stop,
            patch(
                "src.server.services.runs.executor.LocalRunExecutor.get_instance"
            ) as mock_get_instance,
        ):
            mock_instance = MagicMock()
            mock_instance.has_active_tasks_for_computer = AsyncMock(return_value=False)
            mock_get_instance.return_value = mock_instance

            stopped = await mgr.cleanup_idle_workspaces()

        assert stopped == 1
        mock_stop.assert_called_once_with(
            "comp-idle", unchanged_since=machine["updated_at"], reason="idle"
        )


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
        fresh_provider = fake_provider()
        fresh_provider.get = AsyncMock(return_value=mock_runtime)
        fresh_provider.close = AsyncMock()
        fresh_provider.is_transient_error = MagicMock(return_value=False)
        mock_create_provider.return_value = fresh_provider

        await sandbox._ensure_sandbox_connected()

        # Provider was recreated (once at __init__, once in _ensure_sandbox_connected)
        assert sandbox.provider is fresh_provider
        assert mock_create_provider.call_count == 2


class TestEnsureSandboxConnectedSharesOneReconnect:
    """Callers arriving while a reconnect runs take its outcome rather than
    queue for their own: against a host that does not answer, each queued one
    paid its own connect timeout in turn."""

    @staticmethod
    def _sandbox(mock_create_provider, mock_provider, mock_runtime, reconnect):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        sandbox.sandbox_id = "existing-sandbox"
        fresh_provider = fake_provider()
        fresh_provider.close = AsyncMock()
        mock_create_provider.return_value = fresh_provider
        sandbox.reconnect = AsyncMock(side_effect=reconnect)
        return sandbox

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_concurrent_callers_share_one_reconnect(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        async def slow_reconnect(sid):
            await asyncio.sleep(0.01)

        sandbox = self._sandbox(
            mock_create_provider, mock_provider, mock_runtime, slow_reconnect
        )

        await asyncio.gather(*(sandbox._ensure_sandbox_connected() for _ in range(3)))

        assert sandbox.reconnect.await_count == 1

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_concurrent_callers_share_its_failure(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        verdict = SandboxHostUnavailableError("existing-sandbox", "did not answer")

        async def silent_host(sid):
            await asyncio.sleep(0.01)
            raise verdict

        sandbox = self._sandbox(
            mock_create_provider, mock_provider, mock_runtime, silent_host
        )

        outcomes = await asyncio.gather(
            *(sandbox._ensure_sandbox_connected() for _ in range(3)),
            return_exceptions=True,
        )

        assert outcomes == [verdict, verdict, verdict]
        assert sandbox.reconnect.await_count == 1

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_a_caller_after_it_ended_reconnects_anew(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """An outcome is news only to those who waited for it; the host may
        answer by the next call."""
        sandbox = self._sandbox(
            mock_create_provider,
            mock_provider,
            mock_runtime,
            [SandboxHostUnavailableError("existing-sandbox", "did not answer"), None],
        )

        with pytest.raises(SandboxHostUnavailableError):
            await sandbox._ensure_sandbox_connected()
        await sandbox._ensure_sandbox_connected()

        assert sandbox.reconnect.await_count == 2
        assert sandbox._reconnect_task is None

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_a_caller_giving_up_does_not_cancel_it_for_the_others(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        release = asyncio.Event()

        async def held_reconnect(sid):
            await release.wait()

        sandbox = self._sandbox(
            mock_create_provider, mock_provider, mock_runtime, held_reconnect
        )
        quitter = asyncio.create_task(sandbox._ensure_sandbox_connected())
        waiter = asyncio.create_task(sandbox._ensure_sandbox_connected())
        await asyncio.sleep(0)

        quitter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await quitter
        release.set()
        await waiter

        assert sandbox.reconnect.await_count == 1

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_a_failure_no_caller_waited_for_is_not_reported_unretrieved(
        self, mock_create_provider, mock_provider, mock_runtime, caplog
    ):
        import gc
        import logging

        release = asyncio.Event()

        async def failing_reconnect(sid):
            await release.wait()
            raise RuntimeError("reconnect bombed")

        sandbox = self._sandbox(
            mock_create_provider, mock_provider, mock_runtime, failing_reconnect
        )
        caplog.set_level(logging.ERROR, logger="asyncio")
        quitter = asyncio.create_task(sandbox._ensure_sandbox_connected())
        await asyncio.sleep(0)
        quitter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await quitter

        release.set()
        for _ in range(3):
            await asyncio.sleep(0)
        gc.collect()

        assert sandbox._reconnect_task is None
        assert not any("never retrieved" in rec.getMessage() for rec in caplog.records)


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
