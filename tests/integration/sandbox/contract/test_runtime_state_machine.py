"""Contract tests for SandboxRuntime state transitions."""

from __future__ import annotations

import pytest

from ptc_agent.core.sandbox.runtime import RuntimeState

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestRuntimeStateMachine:
    """Full state machine: running -> stopped -> running -> deleted."""

    async def test_initial_state_is_running(self, sandbox_runtime, timed):
        async with timed("lifecycle", "get_state"):
            state = await sandbox_runtime.get_state()
        assert state == RuntimeState.RUNNING

    async def test_stop_transitions_to_stopped(self, sandbox_runtime, timed):
        async with timed("lifecycle", "stop"):
            await sandbox_runtime.stop()
        assert await sandbox_runtime.get_state() == RuntimeState.STOPPED

    async def test_start_from_stopped(self, sandbox_runtime, timed):
        await sandbox_runtime.stop()
        async with timed("lifecycle", "start_from_stopped"):
            await sandbox_runtime.start()
        assert await sandbox_runtime.get_state() == RuntimeState.RUNNING

    async def test_start_when_already_running_is_noop(self, sandbox_runtime, timed):
        async with timed("lifecycle", "start_when_running"):
            await sandbox_runtime.start()
        assert await sandbox_runtime.get_state() == RuntimeState.RUNNING

    async def test_full_stop_start_cycle(self, sandbox_runtime, timed):
        """running -> stop -> start -> stop -> start"""
        assert await sandbox_runtime.get_state() == RuntimeState.RUNNING

        async with timed("lifecycle", "stop_start_cycle"):
            await sandbox_runtime.stop()
            assert await sandbox_runtime.get_state() == RuntimeState.STOPPED

            await sandbox_runtime.start()
            assert await sandbox_runtime.get_state() == RuntimeState.RUNNING

            await sandbox_runtime.stop()
            assert await sandbox_runtime.get_state() == RuntimeState.STOPPED

            await sandbox_runtime.start()
            assert await sandbox_runtime.get_state() == RuntimeState.RUNNING


class TestStoppedRuntimeErrors:
    """Verify that exec/file_io raise when the runtime is stopped."""

    async def test_exec_on_stopped_runtime_raises(self, sandbox_runtime, timed):
        await sandbox_runtime.stop()
        async with timed("lifecycle", "exec_on_stopped"):
            with pytest.raises(Exception):
                await sandbox_runtime.exec("echo hello")
        await sandbox_runtime.start()

    async def test_file_io_on_stopped_runtime_raises(self, sandbox_runtime, timed):
        wd = await sandbox_runtime.fetch_working_dir()
        await sandbox_runtime.stop()
        async with timed("lifecycle", "file_io_on_stopped"):
            with pytest.raises(Exception):
                await sandbox_runtime.upload_file(b"data", f"{wd}/test.txt")
        await sandbox_runtime.start()
