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

    @pytest.mark.asyncio
    async def test_callback_fires_once_per_open_not_once_per_failure(self):
        """Calls already in flight when the circuit trips still report failures.
        Recovery work (a browser reset, for one) must not run again for each."""
        breaker = CircuitBreaker(failure_threshold=2)
        calls = []

        async def on_open():
            calls.append(1)

        for _ in range(5):
            await breaker.record_failure(on_open)
        await asyncio.sleep(0.05)  # on_open is detached; let it run

        assert breaker.state == CircuitState.OPEN
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_callback_fires_again_when_a_half_open_probe_fails(self):
        """The one case where re-firing is right: the probe failed, so the
        recovery work did not take and is owed another run. Without this, an
        'only on the CLOSED→OPEN edge' reading would reset the browser once
        ever, however many times the circuit re-opens."""
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        calls = []

        async def on_open():
            calls.append(1)

        await breaker.record_failure(on_open)
        await asyncio.sleep(0.05)
        assert breaker.state == CircuitState.OPEN
        assert len(calls) == 1

        await breaker.check_state()
        assert breaker.state == CircuitState.HALF_OPEN

        await breaker.record_failure(on_open)
        await asyncio.sleep(0.05)
        assert breaker.state == CircuitState.OPEN
        assert len(calls) == 2


class TestOpenLogging:
    @pytest.mark.asyncio
    async def test_open_logs_once_and_names_the_streak_cause(self, caplog):
        breaker = CircuitBreaker(name="fetch:acme", failure_threshold=2)

        with caplog.at_level("WARNING", logger="src.tools.web.breaker"):
            for _ in range(4):
                await breaker.record_failure(reason="[timeout] upstream stalled")

        opens = [r for r in caplog.records if "opening" in r.message]
        assert len(opens) == 1
        assert "[fetch:acme]" in opens[0].message
        assert "2x [timeout] upstream stalled" in opens[0].message
