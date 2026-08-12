"""body_store wait bounds: a wedged DB write must never pin the tool/model
path (schedule) or turn-end / SSE close (drain) — the store is best-effort."""

from __future__ import annotations

import asyncio

import pytest

from ptc_agent.agent.middleware.provenance import body_store
from ptc_agent.agent.middleware.provenance.body_store import (
    drain_body_writes,
    schedule_body_write,
)


async def _never() -> None:
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_saturated_stall_drops_write_instead_of_blocking(monkeypatch):
    monkeypatch.setattr(body_store, "BODY_WRITE_WAIT_TIMEOUT", 0.05)
    stuck = asyncio.create_task(_never())
    tasks = {stuck}

    await asyncio.wait_for(
        schedule_body_write(tasks, _never(), max_tasks=1), timeout=1.0
    )

    # Nothing new scheduled; the stalled write is untouched.
    assert tasks == {stuck}
    assert not stuck.done()
    stuck.cancel()


@pytest.mark.asyncio
async def test_saturated_wait_resumes_when_a_write_finishes():
    gate = asyncio.Event()

    async def _gated() -> None:
        await gate.wait()

    slot = asyncio.create_task(_gated())
    tasks = {slot}
    ran = asyncio.Event()

    async def _write() -> None:
        ran.set()

    sched = asyncio.create_task(schedule_body_write(tasks, _write(), max_tasks=1))
    await asyncio.sleep(0.01)
    gate.set()
    await asyncio.wait_for(sched, timeout=1.0)
    await asyncio.wait_for(ran.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_drain_times_out_instead_of_hanging(monkeypatch):
    monkeypatch.setattr(body_store, "BODY_WRITE_WAIT_TIMEOUT", 0.05)
    stuck = asyncio.create_task(_never())
    tasks = {stuck}

    await asyncio.wait_for(drain_body_writes(tasks), timeout=1.0)

    # Shield kept the in-flight write alive; it stays tracked for the next drain.
    assert stuck in tasks
    assert not stuck.done()
    stuck.cancel()
