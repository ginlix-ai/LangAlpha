"""Scheduling rules for background catalog discovery.

Two invariants live here, and neither is visible from a single call:

- newer work is never dropped. A write that lands while a probe is on the wire
  asks about a configuration the running probe has already stopped describing,
  so it queues one more pass instead of returning;
- the rate limit the self-heal path leans on is a row in Postgres, not a dict
  in this process. ``_in_flight``/``_rerun`` are execution context for one
  worker; ``claim_probe_kick`` is what every worker agrees on;
- the scheduler keys per row, so nothing bounds how many rows an import or a
  rotated secret kicks at once. The bound is on the network instead, and the
  last test pins that background discovery queues on the same ceiling every
  other probe holds.

The pass itself is stubbed: what is under test is which passes run, with what
throttle, in what order.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.server.services.mcp_oauth import discovery


@pytest.fixture(autouse=True)
def _clean_scheduler_state():
    """Module-level dicts outlive a test; a leaked key would silently make the
    next schedule call a no-op."""
    discovery._in_flight.clear()
    discovery._rerun.clear()
    yield
    discovery._in_flight.clear()
    discovery._rerun.clear()


@pytest.fixture
def passes(monkeypatch):
    """Records every ``_discovery_pass`` and lets a test park the first one.

    ``started`` fires once the first pass is actually awaiting, which is the
    only point at which a second kick is genuinely concurrent.
    """
    calls: list[tuple[str, str, str]] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def _pass(user_id: str, name: str, reason: str) -> None:
        calls.append((user_id, name, reason))
        if len(calls) == 1:
            started.set()
            await release.wait()

    monkeypatch.setattr(discovery, "_discovery_pass", _pass)
    return type(
        "Passes", (), {"calls": calls, "started": started, "release": release}
    )()


@pytest.fixture
def claim(monkeypatch):
    mock = AsyncMock(return_value=True)
    monkeypatch.setattr(discovery, "claim_probe_kick", mock)
    return mock


async def _settle(key: tuple[str, str]) -> None:
    task = discovery._in_flight.get(key)
    if task is not None:
        await task


@pytest.mark.asyncio
async def test_a_write_during_a_running_probe_gets_its_own_pass(passes, claim):
    """The regression: the second kick used to return and the edit stayed
    unprobed until a self-heal noticed it, two minutes later."""
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()

    discovery.schedule_catalog_discovery("u1", "authy", reason="edit")
    passes.release.set()
    await _settle(("u1", "authy"))

    assert len(passes.calls) == 2


@pytest.mark.asyncio
async def test_many_writes_during_one_probe_collapse_into_one_rerun(passes, claim):
    """A rerun re-reads the row, so three edits in one probe window all land in
    a single extra pass rather than three."""
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()

    for _ in range(3):
        discovery.schedule_catalog_discovery("u1", "authy", reason="edit")
    passes.release.set()
    await _settle(("u1", "authy"))

    assert len(passes.calls) == 2


@pytest.mark.asyncio
async def test_a_self_heal_kick_never_queues_a_rerun(passes, claim):
    """A throttled kick means "this row looks unprobed", which the probe now
    running is already answering. Queueing it would let the list route stack a
    rerun per poll."""
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()

    discovery.schedule_catalog_discovery("u1", "authy", reason="list", throttle=True)
    passes.release.set()
    await _settle(("u1", "authy"))

    assert len(passes.calls) == 1


@pytest.mark.asyncio
async def test_the_rerun_flag_is_drained_when_the_task_ends(passes, claim):
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()
    discovery.schedule_catalog_discovery("u1", "authy", reason="edit")
    passes.release.set()
    await _settle(("u1", "authy"))

    assert ("u1", "authy") not in discovery._rerun
    assert ("u1", "authy") not in discovery._in_flight


@pytest.mark.asyncio
async def test_a_throttled_kick_claims_against_the_self_heal_interval(passes, claim):
    passes.release.set()
    discovery.schedule_catalog_discovery("u1", "authy", reason="list", throttle=True)
    await _settle(("u1", "authy"))

    assert claim.await_args.args == ("u1", "authy")
    assert claim.await_args.kwargs == {"throttle_s": discovery.SELF_HEAL_INTERVAL_S}


@pytest.mark.asyncio
async def test_a_write_claims_with_no_throttle_at_all(passes, claim):
    """A write is not rate-limited: the user changed something and the answer
    on the row is now stale by definition."""
    passes.release.set()
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await _settle(("u1", "authy"))

    assert claim.await_args.kwargs == {"throttle_s": None}


@pytest.mark.asyncio
async def test_a_lost_claim_never_reaches_the_network(passes, claim):
    """Another worker stamped the row inside the window: this process drops the
    kick rather than dialling the same server again."""
    claim.return_value = False

    discovery.schedule_catalog_discovery("u1", "authy", reason="list", throttle=True)
    await _settle(("u1", "authy"))

    assert passes.calls == []


@pytest.mark.asyncio
async def test_two_rows_run_independently(passes, claim):
    """The key is per (user, server): one slow probe must not gate another
    row's kick."""
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()

    discovery.schedule_catalog_discovery("u1", "other", reason="create")
    await _settle(("u1", "other"))
    passes.release.set()
    await _settle(("u1", "authy"))

    assert {name for _, name, _ in passes.calls} == {"authy", "other"}


def test_the_throttle_clock_no_longer_lives_in_process_memory():
    """``_last_kick`` was a per-worker dict, so N workers meant N times the
    kicks the throttle promised. The stamp is a column now."""
    assert not hasattr(discovery, "_last_kick")


@pytest.mark.asyncio
async def test_background_discovery_shares_the_process_wide_probe_ceiling(
    monkeypatch,
):
    """One task per created row is what an import schedules, and one per
    referencing row is what a rotated secret schedules, so a background probe
    that skipped the gate could open a session per row at once. It queues
    behind the add form and the plugin fan-out instead."""
    from src.server.services import mcp_probe

    monkeypatch.setattr(mcp_probe, "_gate", asyncio.Semaphore(1))
    dialled: list[str] = []
    release = asyncio.Event()

    async def _probe(url, headers=None, *, timeout_s):
        dialled.append(url)
        await release.wait()
        return mcp_probe.ProbeOutcome(ok=True, auth="none")

    monkeypatch.setattr(mcp_probe, "probe_remote_server", _probe)
    # The call discovery makes, against the call the add form makes.
    background = asyncio.create_task(
        discovery.bounded_probe(
            "https://a.example.com/mcp", {}, timeout_s=30, background=True
        )
    )
    while not dialled:
        await asyncio.sleep(0)
    ad_hoc = asyncio.create_task(mcp_probe.bounded_probe("https://b.example.com/mcp"))
    await asyncio.sleep(0.01)

    assert dialled == ["https://a.example.com/mcp"]  # the second one is queued

    release.set()
    assert (await background).ok
    assert (await ad_hoc).ok


@pytest.mark.asyncio
async def test_background_discovery_cannot_fill_the_ceiling(monkeypatch):
    """The lane is narrower than the ceiling, so however many rows an import
    schedules, the add form's probe finds a free slot: the background probes
    queue on the lane while the ad-hoc one takes the gate straight away."""
    from src.server.services import mcp_probe

    monkeypatch.setattr(mcp_probe, "_gate", asyncio.Semaphore(2))
    monkeypatch.setattr(mcp_probe, "_background_lane", asyncio.Semaphore(1))
    dialled: list[str] = []
    release = asyncio.Event()

    async def _probe(url, headers=None, *, timeout_s):
        dialled.append(url)
        await release.wait()
        return mcp_probe.ProbeOutcome(ok=True, auth="none")

    monkeypatch.setattr(mcp_probe, "probe_remote_server", _probe)
    rows = [
        asyncio.create_task(
            mcp_probe.bounded_probe(f"https://r{i}.example.com/mcp", background=True)
        )
        for i in range(3)
    ]
    while not dialled:
        await asyncio.sleep(0)
    ad_hoc = asyncio.create_task(mcp_probe.bounded_probe("https://form.example.com/mcp"))
    await asyncio.sleep(0.01)

    # One background probe holds the lane; the other two wait on it, not on
    # the gate, which still has the slot the form takes.
    assert dialled == ["https://r0.example.com/mcp", "https://form.example.com/mcp"]

    release.set()
    assert all(o.ok for o in await asyncio.gather(*rows))
    assert (await ad_hoc).ok
