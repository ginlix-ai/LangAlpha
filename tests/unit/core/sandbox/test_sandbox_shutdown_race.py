"""
Tests for sandbox shutdown race condition fix.

Verifies that after stop_sandbox() or cleanup():
- _runtime_call raises SandboxGoneError immediately (no retry)
- _ensure_sandbox_connected raises SandboxGoneError immediately
- _init_task is cancelled
- reconnect() resets the _stopped flag for session reuse
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
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    CodeRunResult,
    ExecResult,
    RuntimeState,
    SandboxGoneError,
    SandboxProvider,
    SandboxRuntime,
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


class TestStoppedFlag:
    """After stop_sandbox(), subsequent operations fail fast with SandboxGoneError."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_runtime_call_raises_after_stop(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.stop_sandbox()
        assert sandbox._stopped is True

        with pytest.raises(SandboxGoneError, match="intentionally stopped"):
            await sandbox._runtime_call(
                mock_runtime.exec,
                "ls",
                retry_policy=RetryPolicy.SAFE,
            )

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_ensure_connected_raises_after_stop(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        sandbox.sandbox_id = "test-sandbox"

        await sandbox.stop_sandbox()

        with pytest.raises(SandboxGoneError, match="intentionally stopped"):
            await sandbox._ensure_sandbox_connected()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_runtime_call_raises_after_cleanup(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.cleanup()
        assert sandbox._stopped is True

        with pytest.raises(SandboxGoneError, match="intentionally stopped"):
            await sandbox._runtime_call(
                mock_runtime.exec,
                "ls",
                retry_policy=RetryPolicy.SAFE,
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

        # Simulate a long-running lazy init task
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


class TestReconnectResetsFlag:
    """reconnect() clears _stopped so session reuse works."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_reconnect_clears_stopped(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        mock_runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
        mock_runtime.fetch_working_dir = AsyncMock(return_value="/home/workspace")

        sandbox = PTCSandbox(config=_make_config())
        sandbox._stopped = True

        # reconnect should clear the _stopped flag early
        # It will fail at some point during setup, but _stopped should be False
        try:
            await sandbox.reconnect("test-sandbox")
        except Exception:
            pass

        assert sandbox._stopped is False
