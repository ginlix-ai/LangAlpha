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
  other probe holds;
- and whichever kick asks, the pass dials only a row the user has switched on.

The pass itself is stubbed: what is under test is which passes run, with what
throttle, in what order.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.mcp_oauth import discovery


@pytest.fixture(autouse=True)
def _clean_scheduler_state():
    """Module-level dicts outlive a test; a leaked key would silently make the
    next schedule call a no-op."""
    discovery._in_flight.clear()
    discovery._rerun.clear()
    discovery._pending_stamp.clear()
    yield
    discovery._in_flight.clear()
    discovery._rerun.clear()
    discovery._pending_stamp.clear()


@pytest.fixture
def passes(monkeypatch):
    """Records every ``_discovery_pass`` and lets a test park the first one.

    ``started`` fires once the first pass is actually awaiting, which is the
    only point at which a second kick is genuinely concurrent.
    """
    calls: list[tuple[str, str, str]] = []
    stamps: list[datetime | None] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def _pass(user_id: str, name: str, reason: str, *, claimed_at=None) -> None:
        calls.append((user_id, name, reason))
        stamps.append(claimed_at)
        if len(calls) == 1:
            started.set()
            await release.wait()

    monkeypatch.setattr(discovery, "_discovery_pass", _pass)
    return type(
        "Passes",
        (),
        {"calls": calls, "stamps": stamps, "started": started, "release": release},
    )()


CLAIMED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.fixture
def claim(monkeypatch):
    mock = AsyncMock(return_value=CLAIMED)
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
async def test_a_write_during_a_running_probe_stamps_the_row_itself(passes, claim):
    """The rerun is not the only thing the write needs: the probe already on
    the wire is holding the old secret's verdict and can land it first. The
    write stamps a newer epoch as it queues, which fences that verdict out, and
    the rerun then claims an epoch newer still so its own write survives."""
    later = CLAIMED.replace(minute=5)
    latest = CLAIMED.replace(minute=6)
    claim.side_effect = [CLAIMED, later, latest]

    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()

    discovery.schedule_catalog_discovery("u1", "authy", reason="edit")
    passes.release.set()
    await _settle(("u1", "authy"))

    assert claim.await_count == 3
    assert all(c.kwargs == {"throttle_s": None} for c in claim.await_args_list)
    # The rerun carries the third claim, so the write's own stamp landed
    # between the two passes rather than after them.
    assert passes.stamps == [CLAIMED, latest]


@pytest.mark.parametrize("writes", [1, 2])
@pytest.mark.asyncio
async def test_the_rerun_claims_only_after_every_stamp_its_writes_sent(
    passes, monkeypatch, writes
):
    """The stamp is detached, so nothing used to order it against the rerun's
    own claim. A stamp slow to reach the row landed after that claim, left the
    epoch newer than the one the rerun holds, and the rerun's write was dropped
    as reprobed with no pass behind it: the row kept the pre-write snapshot
    until the self-heal. Two writes make the older stamp the slow one, which
    waiting on the newest alone would still let overtake the claim."""
    landed: list[str] = []
    row: dict[str, datetime] = {}
    gate = asyncio.Event()
    entered = 0

    async def _claim(user_id: str, name: str, *, throttle_s=None) -> datetime:
        nonlocal entered
        stamp = asyncio.current_task().get_name().startswith("mcp-kick-")
        if stamp:
            entered += 1
            if entered == 1:
                await gate.wait()
        # NOW() reads when the UPDATE runs, so whichever claim reaches the row
        # last is the epoch every earlier one is measured against.
        row["probe_kicked_at"] = CLAIMED + timedelta(seconds=len(landed))
        landed.append("stamp" if stamp else "claim")
        return row["probe_kicked_at"]

    monkeypatch.setattr(discovery, "claim_probe_kick", _claim)

    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()
    for _ in range(writes):
        discovery.schedule_catalog_discovery("u1", "authy", reason="edit")
    passes.release.set()
    # Every chance for the rerun to claim ahead of the stamp, which is what it
    # took before.
    await asyncio.sleep(0.01)
    gate.set()
    await _settle(("u1", "authy"))
    await asyncio.gather(*discovery._discovery_tasks)

    assert landed == ["claim", *["stamp"] * writes, "claim"]
    # The fence the rerun's own write will be read under: its epoch is the
    # row's, so nothing supersedes it.
    rerun = discovery._SnapshotWriter("u1", "authy", "fp", None, passes.stamps[-1])
    assert not rerun._reprobed_since(
        {"probe_kicked_at": row["probe_kicked_at"].isoformat()}
    )


@pytest.mark.asyncio
async def test_a_self_heal_kick_during_a_running_probe_stamps_nothing(passes, claim):
    """A throttled kick that finds a probe running has its answer coming. A
    stamp there would fence out the very probe that is about to answer it."""
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()

    discovery.schedule_catalog_discovery("u1", "authy", reason="list", throttle=True)
    passes.release.set()
    await _settle(("u1", "authy"))

    assert claim.await_count == 1


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
    assert ("u1", "authy") not in discovery._pending_stamp


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
    claim.return_value = None

    discovery.schedule_catalog_discovery("u1", "authy", reason="list", throttle=True)
    await _settle(("u1", "authy"))

    assert passes.calls == []


@pytest.mark.asyncio
async def test_the_pass_carries_the_stamp_its_claim_wrote(passes, claim):
    """The stamp is the fence the snapshot write lands under, and a rerun keeps
    it: no one restamped the row, so this probe is still the newest that asked.
    Without the stamp the write fences on the fingerprint alone, which a
    rotated vault value never moves."""
    discovery.schedule_catalog_discovery("u1", "authy", reason="create")
    await passes.started.wait()
    discovery.schedule_catalog_discovery("u1", "authy", reason="edit")
    passes.release.set()
    await _settle(("u1", "authy"))

    assert passes.stamps == [CLAIMED, CLAIMED]


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


def _catalog_row(**overrides) -> dict:
    row = {
        "name": "authy",
        "transport": "http",
        "url": "https://api.example.com/mcp",
        "headers": {"Authorization": "${vault:API_KEY}"},
        "env": {},
        "command": None,
        "args": [],
        "description": "",
        "instruction": "",
        "tool_exposure_mode": "summary",
        "discovery_uses_secrets": False,
        "enabled": True,
    }
    row.update(overrides)
    return row


def _writer_stub():
    """A ``_SnapshotWriter`` that lands nothing: these tests are about what
    reaches the network, and the write itself needs a pool."""

    class _Writer:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def settle(self, outcome):
            return None

        async def fail(self, result):
            return None

    return _Writer


class TestTheSwitchDecidesWhatIsDialled:
    """Which ROWS a pass dials, rather than which passes run. Every kick above
    lands in the same pass, so one refusal there covers create, import,
    promote, plugin install, a rotated secret and the self-heal alike."""

    @pytest.mark.parametrize("enabled,dials", [(False, 0), (True, 1)])
    @pytest.mark.asyncio
    async def test_a_row_is_dialled_only_once_the_user_switches_it_on(
        self, enabled, dials
    ):
        """An inert row's headers can hold a credential an import wrote into
        the vault a second ago. Putting that on the wire is the user's call,
        and the switch is where they make it -- until then the pass refuses
        before it resolves a single secret."""
        secrets = AsyncMock(return_value={"API_KEY": "EXAMPLE-OPAQUE-TOKEN-AAA"})
        probe = AsyncMock()
        with (
            patch.object(
                discovery,
                "get_catalog_server",
                new=AsyncMock(return_value=_catalog_row(enabled=enabled)),
            ),
            patch.object(
                discovery, "get_connection", new=AsyncMock(return_value=None)
            ),
            patch.object(discovery, "get_user_secrets_decrypted", new=secrets),
            patch.object(discovery, "bounded_probe", new=probe),
            patch.object(discovery, "_SnapshotWriter", new=_writer_stub()),
        ):
            await discovery.discover_catalog_server("u1", "authy")

        assert probe.await_count == dials
        assert secrets.await_count == dials

    @pytest.mark.asyncio
    async def test_a_row_of_a_disabled_plugin_is_not_dialled(self):
        """The plugin switch withholds its rows from every runtime while each
        row keeps ``enabled=True``, so the self-heal and a rotated secret still
        kick them; the pass refuses before a secret is resolved or a token
        looked up, the same as for a row switched off on its own."""
        secrets = AsyncMock(return_value={"API_KEY": "EXAMPLE-OPAQUE-TOKEN-AAA"})
        probe = AsyncMock()
        connection = AsyncMock(return_value=None)
        with (
            patch.object(
                discovery,
                "get_catalog_server",
                new=AsyncMock(
                    return_value=_catalog_row(
                        plugin_name="bundle", plugin_enabled=False
                    )
                ),
            ),
            patch.object(discovery, "get_connection", new=connection),
            patch.object(discovery, "get_user_secrets_decrypted", new=secrets),
            patch.object(discovery, "bounded_probe", new=probe),
            patch.object(discovery, "_SnapshotWriter", new=_writer_stub()),
        ):
            assert await discovery.discover_catalog_server("u1", "authy") is None

        assert probe.await_count == 0
        assert secrets.await_count == 0
        assert connection.await_count == 0
