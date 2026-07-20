"""Unit tests for the shared circuit breaker."""

from __future__ import annotations

import asyncio

import pytest

from src.tools.web.breaker import CircuitBreaker, CircuitState


class TestOnOpenCallback:
    @pytest.mark.asyncio
    async def test_on_open_runs_and_is_retained_then_discarded(self):
        """The detached on-open callback must run to completion and not be GC'd."""
        ran = asyncio.Event()

        async def on_open():
            await asyncio.sleep(0)
            ran.set()

        breaker = CircuitBreaker(failure_threshold=1)
        await breaker.record_failure(on_open)

        # Task is held while pending, so it survives to completion.
        assert breaker.state == CircuitState.OPEN
        assert len(breaker._open_callbacks) == 1
        await asyncio.wait_for(ran.wait(), timeout=1.0)
        # done-callback clears the strong ref once finished.
        await asyncio.sleep(0)
        assert len(breaker._open_callbacks) == 0

    @pytest.mark.asyncio
    async def test_no_callback_when_below_threshold(self):
        breaker = CircuitBreaker(failure_threshold=3)
        calls = []

        async def on_open():
            calls.append(1)

        await breaker.record_failure(on_open)
        assert breaker.state == CircuitState.CLOSED
        assert breaker._open_callbacks == set()
        assert calls == []
