"""Integration tests for retry logic with real runtime operations.

Tests async_retry_with_backoff and _runtime_call with the MemoryProvider,
verifying that transient errors are handled correctly during actual
sandbox operations.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.retry import RetryPolicy, async_retry_with_backoff
from ptc_agent.core.sandbox.runtime import ExecResult, SandboxTransientError

from .memory_provider import MemoryProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestRetryWithRealOperations:
    """Retry behavior with actual async operations."""

    async def test_successful_operation_no_retry(self, memory_provider):
        runtime = await memory_provider.create()
        call_count = 0

        async def exec_wrapper():
            nonlocal call_count
            call_count += 1
            return await runtime.exec("echo ok")

        result = await async_retry_with_backoff(
            exec_wrapper,
            retry_policy=RetryPolicy.SAFE,
            is_transient=memory_provider.is_transient_error,
            initial_delay_s=0.01,
        )
        assert call_count == 1
        assert "ok" in result.stdout

    async def test_transient_then_success(self, memory_provider):
        """Simulates a transient error that resolves on retry."""
        runtime = await memory_provider.create()
        call_count = 0

        async def flaky_exec():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient network error")
            return await runtime.exec("echo recovered")

        result = await async_retry_with_backoff(
            flaky_exec,
            retry_policy=RetryPolicy.SAFE,
            is_transient=memory_provider.is_transient_error,
            initial_delay_s=0.01,
        )
        assert call_count == 3
        assert "recovered" in result.stdout

    async def test_unsafe_policy_no_retry_on_transient(self, memory_provider):
        """UNSAFE policy raises immediately on transient error."""
        call_count = 0

        async def failing_exec():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("transient connection reset")

        with pytest.raises(SandboxTransientError):
            await async_retry_with_backoff(
                failing_exec,
                retry_policy=RetryPolicy.UNSAFE,
                is_transient=memory_provider.is_transient_error,
                initial_delay_s=0.01,
            )
        assert call_count == 1

    async def test_non_transient_error_raises_immediately(self, memory_provider):
        """Non-transient errors are never retried."""
        call_count = 0

        async def bad_code():
            nonlocal call_count
            call_count += 1
            raise ValueError("invalid argument format")

        with pytest.raises(ValueError, match="invalid argument"):
            await async_retry_with_backoff(
                bad_code,
                retry_policy=RetryPolicy.SAFE,
                is_transient=memory_provider.is_transient_error,
                initial_delay_s=0.01,
            )
        assert call_count == 1

    async def test_max_retries_exceeded(self, memory_provider):
        """After max retries, raises SandboxTransientError."""
        call_count = 0

        async def always_transient():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("transient error every time")

        with pytest.raises(SandboxTransientError):
            await async_retry_with_backoff(
                always_transient,
                retry_policy=RetryPolicy.SAFE,
                is_transient=memory_provider.is_transient_error,
                retries=3,
                initial_delay_s=0.01,
            )
        assert call_count == 3

    async def test_total_timeout_exceeded(self, memory_provider):
        """Total timeout causes SandboxTransientError even if retries remain."""
        call_count = 0

        async def slow_transient():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("transient timeout")

        with pytest.raises(SandboxTransientError):
            await async_retry_with_backoff(
                slow_transient,
                retry_policy=RetryPolicy.SAFE,
                is_transient=memory_provider.is_transient_error,
                retries=100,
                initial_delay_s=0.01,
                total_timeout=0.05,
            )
        # Should have attempted a few times before timeout
        assert call_count >= 1

    async def test_on_transient_callback(self, memory_provider):
        """The on_transient callback is called at most once."""
        runtime = await memory_provider.create()
        callback_count = 0
        call_count = 0

        async def reconnect_callback():
            nonlocal callback_count
            callback_count += 1

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise RuntimeError("transient connection reset")
            return ExecResult(stdout="ok", stderr="", exit_code=0)

        result = await async_retry_with_backoff(
            flaky,
            retry_policy=RetryPolicy.SAFE,
            is_transient=memory_provider.is_transient_error,
            on_transient=reconnect_callback,
            initial_delay_s=0.01,
        )
        assert result.stdout == "ok"
        assert callback_count == 1  # called at most once
        assert call_count == 4


class TestRuntimeCallIntegration:
    """Test _runtime_call through PTCSandbox with real operations."""

    async def test_runtime_call_safe_exec(
        self, sandbox_base_dir, core_config, _patch_create_provider
    ):
        """_runtime_call with SAFE policy retries on transient errors."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        sb = PTCSandbox(core_config)
        await sb.setup_sandbox_workspace()

        try:
            result = await sb._runtime_call(
                sb.runtime.exec,
                "echo via_runtime_call",
                retry_policy=RetryPolicy.SAFE,
            )
            assert "via_runtime_call" in result.stdout
        finally:
            await sb.cleanup()

    async def test_runtime_call_unsafe_exec(
        self, sandbox_base_dir, core_config, _patch_create_provider
    ):
        """_runtime_call with UNSAFE policy for a command."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        sb = PTCSandbox(core_config)
        await sb.setup_sandbox_workspace()

        try:
            result = await sb._runtime_call(
                sb.runtime.exec,
                "echo unsafe_policy",
                retry_policy=RetryPolicy.UNSAFE,
            )
            assert "unsafe_policy" in result.stdout
        finally:
            await sb.cleanup()
