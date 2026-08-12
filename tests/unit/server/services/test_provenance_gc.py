"""``ProvenanceGCService`` — the periodic orphan-body sweeper.

The loop sweeps on start, then sleeps an interval (via ``wait_for`` on a shutdown
event) before the next sweep; the service is a process-global singleton. Tests
reset ``ProvenanceGCService._instance`` around each case to avoid singleton bleed,
use a tiny interval so the loop cycles fast (no multi-second waits), and patch
``sweep_orphan_bodies`` at the service module's import path. Asserts:

* ``get_instance`` returns the same cached object,
* ``start`` schedules a task and ``stop`` cancels it cleanly,
* ``stop`` is idempotent (safe when never started, called twice),
* the loop invokes ``sweep_orphan_bodies`` with the configured grace,
* the first sweep fires on start, NOT after a full interval (so a process that
  restarts faster than the interval still sweeps),
* a sweep raising inside one cycle never kills the service.
"""

import asyncio

import pytest

from src.server.services.provenance_gc import ProvenanceGCService

MOD = "src.server.services.provenance_gc"


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Clear the process-global instance before and after each test."""
    ProvenanceGCService._instance = None
    yield
    ProvenanceGCService._instance = None


class TestSingleton:
    def test_get_instance_caches(self):
        a = ProvenanceGCService.get_instance()
        b = ProvenanceGCService.get_instance()
        assert a is b

    def test_constructor_params_injectable(self):
        svc = ProvenanceGCService(interval_seconds=5, grace_days=3)
        assert svc._interval == 5
        assert svc._grace_days == 3


class TestStartStop:
    @pytest.mark.asyncio
    async def test_start_schedules_task(self):
        svc = ProvenanceGCService(interval_seconds=3600)
        try:
            await svc.start()
            assert svc._task is not None
            assert not svc._task.done()
        finally:
            await svc.stop()

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        svc = ProvenanceGCService(interval_seconds=3600)
        try:
            await svc.start()
            first = svc._task
            await svc.start()  # already running → no new task
            assert svc._task is first
        finally:
            await svc.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self):
        svc = ProvenanceGCService(interval_seconds=3600)
        await svc.start()
        task = svc._task
        await svc.stop()
        assert task.done()

    @pytest.mark.asyncio
    async def test_stop_when_never_started_is_noop(self):
        svc = ProvenanceGCService(interval_seconds=3600)
        await svc.stop()  # no task → must not raise
        assert svc._task is None

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self):
        svc = ProvenanceGCService(interval_seconds=3600)
        await svc.start()
        await svc.stop()
        await svc.stop()  # second stop on a done task → must not raise


class TestLoopSweeps:
    @pytest.mark.asyncio
    async def test_loop_invokes_sweep_with_grace(self, monkeypatch):
        # Tiny interval so the first wait_for times out almost immediately and
        # the loop reaches the sweep without a real multi-second sleep.
        called = asyncio.Event()
        seen_grace = []

        async def fake_sweep(grace_days):
            seen_grace.append(grace_days)
            called.set()
            return 0

        monkeypatch.setattr(f"{MOD}.sweep_orphan_bodies", fake_sweep)
        svc = ProvenanceGCService(interval_seconds=0.01, grace_days=11)
        try:
            await svc.start()
            await asyncio.wait_for(called.wait(), timeout=2.0)
        finally:
            await svc.stop()
        assert seen_grace and seen_grace[0] == 11

    @pytest.mark.asyncio
    async def test_loop_survives_a_failing_sweep_cycle(self, monkeypatch):
        # One raising cycle must not kill the loop — it sweeps again next tick.
        calls = []
        twice = asyncio.Event()

        async def flaky_sweep(grace_days):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("sweep boom")
            twice.set()
            return 0

        monkeypatch.setattr(f"{MOD}.sweep_orphan_bodies", flaky_sweep)
        svc = ProvenanceGCService(interval_seconds=0.01)
        try:
            await svc.start()
            # If the loop died on the first raise, this wait times out.
            await asyncio.wait_for(twice.wait(), timeout=2.0)
        finally:
            await svc.stop()
        assert len(calls) >= 2

    @pytest.mark.asyncio
    async def test_sweeps_on_start_not_after_a_full_interval(self, monkeypatch):
        # Regression: the loop must sweep on start, not wait a full interval first
        # — else a process restarting faster than the interval never sweeps. A huge
        # interval means this would time out if the sweep waited for it.
        swept = asyncio.Event()

        async def fake_sweep(grace_days):
            swept.set()
            return 0

        monkeypatch.setattr(f"{MOD}.sweep_orphan_bodies", fake_sweep)
        svc = ProvenanceGCService(interval_seconds=3600)
        try:
            await svc.start()
            await asyncio.wait_for(swept.wait(), timeout=2.0)
        finally:
            await svc.stop()

    @pytest.mark.asyncio
    async def test_stop_during_sleep_exits_without_further_sweeping(self, monkeypatch):
        # Sweep runs once on start, then the loop parks in the long interval sleep;
        # stop() must exit that sleep without triggering a second sweep.
        swept = []
        first = asyncio.Event()

        async def fake_sweep(grace_days):
            swept.append(1)
            first.set()
            return 0

        monkeypatch.setattr(f"{MOD}.sweep_orphan_bodies", fake_sweep)
        svc = ProvenanceGCService(interval_seconds=3600)
        await svc.start()
        await asyncio.wait_for(first.wait(), timeout=2.0)  # startup sweep done
        await svc.stop()
        assert swept == [1]  # exactly the startup sweep, no second one
