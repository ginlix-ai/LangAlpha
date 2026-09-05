"""Runtime credit gate — the invariants the module promises in its docstring.

Fail-open is the load-bearing one: mid-run, inability to check must never stop
a run, so a metering read that raises has to be contained rather than escape
the model-boundary hook. The rest pin the stop decision (own-spend vs
own-ceiling, and only under a recorded denial), the monotone ceiling, the
gate's lifetime under every way a stream can end, the two lane behaviours, and
the pause payload's agreement with the status classifier that reads it.

One reservation per turn is the other load-bearing one, and it is the fixed
regression: a subagent must not be denied against its own parent's grant, and
the lease must outlive the main run for as long as any subagent is still
spending under it.

The port is duck-typed by design, which is what makes all of this testable
without a server; every test here fakes it and nothing touches a database.
Spend is asserted by comparison, never against a credit figure — the USD rate
and the infrastructure pricing table are configuration, not contract.
"""

from __future__ import annotations

import asyncio

import pytest

from ptc_agent.agent.middleware import credit_gate as credit_gate_module
from ptc_agent.agent.middleware.credit_gate import (
    CreditGateMiddleware,
    CreditGateState,
    CreditLease,
    CreditStopError,
    LeaseVerdict,
    build_pause_payload,
    current_credit_gate,
    run_with_credit_gate,
)
from langgraph.types import Interrupt

from src.server.contracts.status import classify_interrupt_reason

# Copy the platform would author; langalpha only ever relays it.
_DENIAL_COPY = "Out of credits. Top up to keep this run going."


class FakeTracker:
    """Token-side meter. Raises from ``platform_usd_total`` once armed."""

    def __init__(self, usd: float = 0.0) -> None:
        self.usd = usd
        self.boom = False

    def platform_usd_total(self) -> float:
        if self.boom:
            raise RuntimeError("tracker read failed")
        return self.usd


class FakeToolTracker:
    """Tool-usage meter. Armed, it fails the way a lock-free dict actually
    fails when a running tool mutates it mid-read."""

    def __init__(self, usage: dict | None = None) -> None:
        self.usage = dict(usage or {})
        self.boom = False

    def get_summary(self) -> dict:
        if self.boom:
            raise RuntimeError("dictionary changed size during iteration")
        return dict(self.usage)


class FakePort:
    """Records every call; answers with whatever verdicts the test queued
    (exhausted queue = None, the service's "no verdict" / fail-open shape)."""

    def __init__(
        self,
        verdicts: list | None = None,
        heartbeat_results: list | None = None,
    ) -> None:
        self.verdicts = list(verdicts or [])
        self.acquires: list[tuple] = []
        self.multipliers: list[float] = []
        self.byoks: list[bool] = []
        self.releases: list[tuple] = []
        self.heartbeats: list[tuple] = []
        self.heartbeat_results = list(heartbeat_results or [])

    async def acquire(
        self,
        user_id: str,
        run_ref: str,
        spent_credits: float,
        rate_multiplier: float = 1.0,
        byok: bool = False,
    ):
        self.acquires.append((user_id, run_ref, spent_credits))
        self.multipliers.append(rate_multiplier)
        self.byoks.append(byok)
        return self.verdicts.pop(0) if self.verdicts else None

    async def release(
        self, user_id: str, run_ref: str, generation: int | None = None
    ) -> None:
        self.releases.append((user_id, run_ref, generation))

    async def heartbeat(self, kind: str, run_ref: str, credits: float):
        self.heartbeats.append((kind, run_ref, credits))
        if not self.heartbeat_results:
            return True
        answer = self.heartbeat_results.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


async def join_quiet(lease: CreditLease, member: CreditGateState) -> None:
    """Join, then stand the lease's refresher back down. Membership is what
    every family-total assertion here needs; the loop is not — these tests
    drive its tick by hand, and a live one would race every assertion about
    what the port was asked."""
    await lease.join(member)
    await lease._stop_refresher()
    lease._closed = False


async def make_gate(**overrides) -> CreditGateState:
    port = overrides.pop("port", None) or FakePort()
    lease = overrides.pop("lease", None) or CreditLease(
        user_id="user-1", run_ref="run-1", port=port
    )
    params = {"run_ref": "run-1", "kind": "run", "port": port, "lease": lease}
    params.update(overrides)
    gate = CreditGateState(**params)
    await join_quiet(lease, gate)
    return gate


async def metered_gate(
    **overrides,
) -> tuple[CreditGateState, FakeTracker, FakeToolTracker]:
    tracker = FakeTracker(usd=2.0)
    tools = FakeToolTracker({"TavilySearchTool": 3})
    gate = await make_gate(tracker=tracker, tool_tracker=tools, **overrides)
    return gate, tracker, tools


async def _run_boundary(gate: CreditGateState):
    """Drive one model boundary through the middleware under the lane var."""
    token = current_credit_gate.set(gate)
    try:
        return await CreditGateMiddleware().abefore_model({}, None)
    finally:
        current_credit_gate.reset(token)


# -- fail open ------------------------------------------------------------


@pytest.mark.parametrize("failing", ["tracker", "tool_tracker"])
@pytest.mark.asyncio
async def test_spend_holds_the_last_good_value_when_a_meter_raises(failing):
    gate, tracker, tools = await metered_gate()
    good = gate.spend()
    assert good > 0, "fixture must actually meter, or the test proves nothing"

    setattr(tracker if failing == "tracker" else tools, "boom", True)

    assert gate.spend() == good
    assert gate.spend() == good, "the held value survives repeated failed reads"


@pytest.mark.parametrize("failing", ["tracker", "tool_tracker"])
@pytest.mark.asyncio
async def test_a_broken_meter_never_stops_a_run(failing):
    """The module's first-paragraph promise: inability to check fails open."""
    gate, tracker, tools = await metered_gate()
    good = gate.spend()
    setattr(tracker if failing == "tracker" else tools, "boom", True)

    assert await _run_boundary(gate) is None
    assert gate.spend() == good


# -- stop decision --------------------------------------------------------


@pytest.mark.asyncio
async def test_no_recorded_verdict_never_stops():
    lease = (await make_gate()).lease
    lease.ceiling_credits = 100.0
    assert lease.should_stop(1_000_000.0) is False


@pytest.mark.asyncio
async def test_spend_under_a_standing_grant_is_entitled_to_finish():
    lease = (await make_gate()).lease
    lease.ceiling_credits = 100.0
    lease.denial = {"message": _DENIAL_COPY}
    assert lease.should_stop(99.9) is False


@pytest.mark.asyncio
async def test_a_denial_stops_at_and_past_the_granted_ceiling():
    lease = (await make_gate()).lease
    lease.ceiling_credits = 100.0
    lease.denial = {"message": _DENIAL_COPY}
    assert lease.should_stop(100.0) is True
    assert lease.should_stop(100.1) is True


# -- verdict recording ----------------------------------------------------


@pytest.mark.asyncio
async def test_a_stale_grant_never_lowers_the_ceiling():
    lease = (await make_gate()).lease
    lease._record(LeaseVerdict(granted=True, ceiling_credits=500.0, ttl_seconds=60))
    assert lease.ceiling_credits == 500.0

    # A response that overtook a newer one on the wire.
    lease._record(LeaseVerdict(granted=True, ceiling_credits=200.0, ttl_seconds=60))
    assert lease.ceiling_credits == 500.0


@pytest.mark.asyncio
async def test_no_verdict_leaves_the_previous_one_standing():
    lease = (await make_gate()).lease
    lease._record(LeaseVerdict(granted=False, quota={"message": _DENIAL_COPY}))
    lease._record(None)
    assert lease.denial == {"message": _DENIAL_COPY}


# -- lifetime -------------------------------------------------------------


def count_closes(gate: CreditGateState) -> list:
    calls: list = []
    original = gate.aclose

    async def counted() -> None:
        calls.append(1)
        await original()

    gate.aclose = counted
    return calls


async def _events(*items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_gate_closes_once_when_the_stream_completes():
    gate = await make_gate()
    closes = count_closes(gate)

    seen = [e async for e in run_with_credit_gate(gate, _events("a", "b"))]

    assert seen == ["a", "b"]
    assert closes == [1]
    assert gate.port.releases == [("user-1", "run-1", None)]


@pytest.mark.asyncio
async def test_gate_closes_once_when_the_stream_raises():
    gate = await make_gate()
    closes = count_closes(gate)

    async def failing():
        yield "a"
        raise RuntimeError("upstream blew up")

    with pytest.raises(RuntimeError, match="upstream blew up"):
        async for _ in run_with_credit_gate(gate, failing()):
            pass

    assert closes == [1]
    assert gate.port.releases == [("user-1", "run-1", None)]


@pytest.mark.asyncio
async def test_gate_closes_once_when_the_driving_task_is_cancelled():
    gate = await make_gate()
    closes = count_closes(gate)
    started = asyncio.Event()

    async def hanging():
        yield "a"
        started.set()
        await asyncio.sleep(3600)

    async def drive():
        async for _ in run_with_credit_gate(gate, hanging()):
            pass

    task = asyncio.create_task(drive())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert closes == [1]
    # The lease is retired even on the cancellation path — aclose shields
    # each teardown step precisely so a cancel cannot strand a live lease.
    assert gate.port.releases == [("user-1", "run-1", None)]


@pytest.mark.asyncio
async def test_no_gate_passes_the_stream_through_untouched():
    assert [e async for e in run_with_credit_gate(None, _events("a", "b"))] == ["a", "b"]


# -- lane behaviour -------------------------------------------------------


@pytest.mark.asyncio
async def test_task_lane_raises_credit_stop_error():
    gate = await make_gate(kind="task")
    gate.lease.denial = {"message": _DENIAL_COPY}

    with pytest.raises(CreditStopError) as excinfo:
        await _run_boundary(gate)

    assert str(excinfo.value) == _DENIAL_COPY


@pytest.mark.asyncio
async def test_run_lane_pauses_instead_of_raising(monkeypatch):
    gate = await make_gate(kind="run")
    gate.lease.denial = {"message": _DENIAL_COPY}
    seen: list = []

    class _Paused(Exception):
        """Stands in for the interrupt langgraph raises on the first pass."""

    def fake_interrupt(payload):
        seen.append(payload)
        raise _Paused()

    monkeypatch.setattr(credit_gate_module, "interrupt", fake_interrupt)

    with pytest.raises(_Paused):
        await _run_boundary(gate)

    assert seen == [build_pause_payload(gate.lease)]


# -- one reservation per turn ---------------------------------------------


async def child_of(parent: CreditGateState, usd: float = 0.0) -> CreditGateState:
    child = parent.spawn_child(
        run_ref="task-1", tracker=FakeTracker(usd=usd), tool_tracker=None
    )
    await join_quiet(parent.lease, child)
    return child


@pytest.mark.asyncio
async def test_a_child_reserves_nothing_of_its_own():
    """The regression. A subagent asking for its own lease was denied against
    the parent's grant, because the parent's ceiling counts as reserved for
    everyone but the parent. A child must never ask."""
    port = FakePort([LeaseVerdict(granted=True, ceiling_credits=500.0, ttl_seconds=900)])
    parent = await make_gate(port=port)
    child = await child_of(parent)

    assert child.lease is parent.lease

    # Drive one refresh directly instead of starting the task. An acquire has
    # to actually happen for any of this to mean anything: assert on an empty
    # list and the test passes just as well against a gate that never asks.
    await parent.lease._tick()
    assert port.acquires, "the lease must actually ask, or this proves nothing"
    assert {ref for _, ref, _ in port.acquires} == {"run-1"}

    await child.aclose()
    assert {ref for _, ref, _ in port.acquires} == {"run-1"}, "the child asked too"
    assert port.releases == [], "a live parent still holds the lease"


@pytest.mark.asyncio
async def test_a_child_is_not_stopped_merely_for_having_no_ceiling_of_its_own():
    """Fresh child state used to mean ceiling 0, so any denial stopped it at
    spend 0 — before its first model call. It now answers to the family."""
    parent = await make_gate()
    child = await child_of(parent)
    parent.lease.ceiling_credits = 100.0
    parent.lease.denial = {"message": _DENIAL_COPY}

    assert child.spend() == 0.0
    assert parent.lease.should_stop(parent.lease.spend()) is False


@pytest.mark.asyncio
async def test_family_spend_sums_live_lanes_and_retains_retired_ones():
    parent = await make_gate(tracker=FakeTracker(usd=2.0))
    child = await child_of(parent, usd=3.0)
    lease = parent.lease

    both = lease.spend()
    assert both > parent.spend() > 0, "the family total includes the child"
    assert both > child.spend() > 0

    await lease.leave(child)
    assert lease.spend() == pytest.approx(both), "a finished lane's spend stays"


@pytest.mark.asyncio
async def test_the_lease_outlives_the_main_run_until_the_last_child_leaves():
    """``auto_wait=False`` means subagent writers keep working after the main
    stream ends. Releasing on the parent's close would leave them ungated."""
    parent = await make_gate()
    child = await child_of(parent)

    await parent.aclose()
    assert parent.port.releases == [], "a live child still holds the reservation"

    await child.aclose()
    assert parent.port.releases == [("user-1", "run-1", None)]


@pytest.mark.asyncio
async def test_a_lane_joining_during_a_release_never_waits_on_the_network():
    """A subagent's first step is scheduled independently of its spawn, so it
    can arrive after the last lane left and the release is already in flight.
    The join must not wait that out — nothing in this gate makes a lane wait on
    the network. The fence is what keeps the two apart: the release retires the
    generation it was granted, not the one the latecomer just took out."""
    port = FakePort(
        [
            LeaseVerdict(granted=True, ceiling_credits=500.0, generation=1),
            LeaseVerdict(granted=True, ceiling_credits=500.0, generation=2),
        ]
    )
    released = asyncio.Event()
    let_release_finish = asyncio.Event()
    real_release = port.release

    async def slow_release(*args):
        released.set()
        await let_release_finish.wait()
        await real_release(*args)

    port.release = slow_release
    parent = await make_gate(port=port)
    lease = parent.lease
    lease._record(await port.acquire("user-1", "run-1", 0.0))

    leaving = asyncio.create_task(lease.leave(parent))
    await released.wait()

    latecomer = parent.spawn_child(
        run_ref="task-late", tracker=FakeTracker(), tool_tracker=None
    )
    await asyncio.wait_for(lease.join(latecomer), timeout=1.0)
    assert latecomer in lease._members
    assert lease._refresher is not None, "the latecomer is spending under a dead lease"

    let_release_finish.set()
    await leaving
    assert port.releases == [("user-1", "run-1", 1)], "retired the wrong grant"

    # ...and the reopened lease is released on its own generation.
    await lease.leave(latecomer)
    assert port.releases[-1] == ("user-1", "run-1", 2)


@pytest.mark.asyncio
async def test_leaving_twice_does_not_double_count_or_double_release():
    parent = await make_gate(tracker=FakeTracker(usd=2.0))
    lease = parent.lease
    spent = parent.spend()

    await lease.leave(parent)
    await lease.leave(parent)

    assert lease._retired_credits == pytest.approx(spent)
    assert parent.port.releases == [("user-1", "run-1", None)]


@pytest.mark.asyncio
async def test_release_is_fenced_on_the_generation_it_was_granted():
    """Unfenced, a release retires whatever grant is current — including one a
    lane that rejoined took out while this teardown was in flight."""
    port = FakePort([LeaseVerdict(granted=True, ceiling_credits=10.0, generation=7)])
    gate = await make_gate(port=port)
    gate.lease._record(await port.acquire("user-1", "run-1", 0.0))

    await gate.lease.leave(gate)

    assert port.releases == [("user-1", "run-1", 7)]


@pytest.mark.asyncio
async def test_a_denial_still_refreshes_the_generation():
    """A denial refreshes the reservation's reported spend on the service, so
    its generation is newer than the last grant's — releasing on the older one
    would no-op and leave the reservation standing."""
    port = FakePort()
    gate = await make_gate(port=port)
    gate.lease._record(
        LeaseVerdict(granted=True, ceiling_credits=10.0, generation=3)
    )
    gate.lease._record(
        LeaseVerdict(granted=False, quota={"message": "no"}, generation=4)
    )

    await gate.lease.leave(gate)

    assert port.releases == [("user-1", "run-1", 4)]


# -- payload / classifier agreement ---------------------------------------


@pytest.mark.asyncio
async def test_pause_payload_classifies_as_a_credit_pause():
    lease = (await make_gate()).lease
    lease.denial = {"message": _DENIAL_COPY}
    payload = build_pause_payload(lease)

    assert classify_interrupt_reason([Interrupt(value=payload)]) == "credit_pause"
    assert payload["action_requests"][0]["message"] == _DENIAL_COPY


@pytest.mark.asyncio
async def test_a_denial_without_copy_still_classifies_and_still_explains():
    """``_record``'s no-copy denial shape. The classifier must still see a
    credit pause, and the card must never get an explanation-less box."""
    lease = (await make_gate()).lease
    lease.denial = {"message": None}
    payload = build_pause_payload(lease)

    assert classify_interrupt_reason([Interrupt(value=payload)]) == "credit_pause"
    assert payload["action_requests"][0]["message"]


# -- what the family reserves against -------------------------------------

@pytest.mark.asyncio
async def test_the_lease_relays_whose_key_the_turn_runs_on():
    """Relayed, never branched on here. The service folds the same billing row
    for admission and for this, so a lease that cannot see an own-key turn
    answers it out of a pool that does not apply to it."""
    port = FakePort([LeaseVerdict(granted=True, ceiling_credits=50.0, ttl_seconds=60)])
    lease = CreditLease(user_id="u", run_ref="r", port=port, is_byok=True)
    await lease._acquire(0.0)
    assert port.byoks == [True]

    # Default off, so a build that never resolved a credential source sends the
    # payload a platform-funded turn has always sent.
    plain = CreditLease(user_id="u", run_ref="r", port=FakePort([]))
    assert plain.is_byok is False



@pytest.mark.asyncio
async def test_the_family_reserves_for_its_priciest_live_lane():
    """One reservation covers every lane, so it has to be sized for the most
    expensive model running under it. A subagent pinned to a premium model
    under a cheap main run is the case that matters: budget the turn at the
    main run's rate and that lane spends the whole chunk on one call, then
    comes back to the service at every boundary after it."""
    port = FakePort([LeaseVerdict(granted=True, ceiling_credits=500.0, ttl_seconds=900)])
    parent = await make_gate(port=port)
    parent.rate_multiplier = 1.0
    child = parent.spawn_child(
        run_ref="task-1", tracker=None, tool_tracker=None, rate_multiplier=16.5
    )
    await join_quiet(parent.lease, child)

    await parent.lease._tick()

    assert port.multipliers == [16.5]
    await child.aclose()


@pytest.mark.asyncio
async def test_a_lane_keeps_the_turn_s_rate_unless_given_its_own():
    """The overwhelming majority of subagents run the model their turn does,
    so inheriting is the default and an override is the exception."""
    parent = await make_gate()
    parent.rate_multiplier = 5.0
    inherits = parent.spawn_child(run_ref="t1", tracker=None, tool_tracker=None)
    overrides = parent.spawn_child(
        run_ref="t2", tracker=None, tool_tracker=None, rate_multiplier=0.5
    )
    assert inherits.rate_multiplier == 5.0
    assert overrides.rate_multiplier == 0.5


@pytest.mark.asyncio
async def test_a_retired_lane_stops_setting_the_family_s_rate():
    """A lane that has finished is done spending, and holding its rate would
    keep the whole turn reserving against a model no longer running."""
    port = FakePort(
        [LeaseVerdict(granted=True, ceiling_credits=500.0, ttl_seconds=900)] * 2
    )
    parent = await make_gate(port=port)
    parent.rate_multiplier = 1.0
    child = parent.spawn_child(
        run_ref="task-1", tracker=None, tool_tracker=None, rate_multiplier=16.5
    )
    await join_quiet(parent.lease, child)
    await parent.lease._tick()

    await parent.lease.leave(child)
    # Drive a renewal rather than a fresh ask: the standing grant is still
    # inside its TTL, so a bare tick correctly declines to re-acquire, and
    # asserting on that tick would pass against a lease that never re-rates.
    parent.lease._next_acquire_at = 0.0
    parent.lease._lease_deadline = 0.0
    await parent.lease._tick()

    assert port.multipliers == [16.5, 1.0]


@pytest.mark.asyncio
async def test_a_run_denied_on_its_first_acquire_keeps_asking():
    """A denial carries no ceiling, so a run refused on its very first acquire
    still holds the 0.0 it started with. Reading that as "past its ceiling" and
    giving up withheld the top-up window from the user most likely to want it:
    the one who just ran out."""
    port = FakePort(
        [
            LeaseVerdict(
                granted=False,
                ceiling_credits=0.0,
                ttl_seconds=0,
                quota={"message": _DENIAL_COPY},
            ),
            LeaseVerdict(granted=True, ceiling_credits=50.0, ttl_seconds=900),
        ]
    )
    gate = await make_gate(port=port)
    lease = gate.lease

    await lease._tick()
    assert lease.denial is not None
    assert lease.ceiling_credits == 0.0

    # The top-up lands between two model boundaries. Clear the spacing floor so
    # the next tick is due; the point is whether it asks at all, not when.
    lease._next_acquire_at = 0.0
    await lease._tick()

    assert len(port.acquires) == 2, "a run denied at ceiling 0 stopped asking"
    assert lease.denial is None, "the green verdict has to clear the denial"
    assert lease.ceiling_credits == 50.0


# -- ledger heartbeat -----------------------------------------------------
#
# The cursor the unchanged-spend guard reads may only ever name spend the
# ledger accepted. Advancing it on an attempt instead left an idle lane
# (a long execute_code, a subagent waiting on a sandbox) absent from the
# in-flight aggregate until it spent again or closed.


def _make_due(gate: CreditGateState) -> None:
    """Age the lane past the flush interval so the next poll is due."""
    gate._last_flush_at -= credit_gate_module._FLUSH_INTERVAL_SECONDS


@pytest.mark.asyncio
async def test_a_failed_heartbeat_is_retried_on_the_next_due_poll():
    port = FakePort(heartbeat_results=[RuntimeError("ledger write failed")])
    gate, _, _ = await metered_gate(port=port)

    await gate._flush_if_due()
    assert len(port.heartbeats) == 1, "the first beat should have been attempted"

    # Spend has not moved. The retry is owed to the failure, not to new spend.
    _make_due(gate)
    await gate._flush_if_due()

    assert len(port.heartbeats) == 2, "a failed beat was never retried"
    assert port.heartbeats[0][2] == port.heartbeats[1][2]


@pytest.mark.asyncio
async def test_a_failed_heartbeat_does_not_retry_faster_than_the_interval():
    """The retry owes its spacing to the attempt, not the last accepted write.

    Keying the delta arm off the flush cursor would leave it permanently true
    for any lane more than the delta past its last good write, turning every
    poll into a database round trip for as long as the ledger stayed down.
    """
    port = FakePort(heartbeat_results=[RuntimeError("ledger write failed")])
    gate, _, _ = await metered_gate(port=port)

    await gate._flush_if_due()
    await gate._flush_if_due()
    await gate._flush_if_due()

    assert len(port.heartbeats) == 1, "a failing lane retried on every poll"


@pytest.mark.asyncio
async def test_an_accepted_write_is_not_resent_while_spend_is_flat():
    port = FakePort()
    gate, _, _ = await metered_gate(port=port)

    await gate._flush_if_due()
    _make_due(gate)
    await gate._flush_if_due()

    assert len(port.heartbeats) == 1, "an accepted beat was rewritten unchanged"


@pytest.mark.asyncio
async def test_a_closed_row_stops_the_lane_without_new_spend():
    port = FakePort(heartbeat_results=[False] * credit_gate_module._DEAD_ROW_STOPS)
    gate, _, _ = await metered_gate(port=port)

    for _ in range(credit_gate_module._DEAD_ROW_STOPS):
        _make_due(gate)
        await gate._flush_if_due()

    assert gate._closed is True, "a closed row never reached the stop count"


@pytest.mark.asyncio
async def test_an_errored_beat_does_not_clear_the_dead_row_count():
    """``None`` is the port's documented error shape, not an accepted write.

    Reading it as success reset the count that decides a row is gone, so a
    lane alternating error and refusal would never reach the stop.
    """
    port = FakePort(heartbeat_results=[False, None, False, False])
    gate, _, _ = await metered_gate(port=port)

    for _ in range(4):
        _make_due(gate)
        await gate._flush_if_due()

    assert gate._closed is True


@pytest.mark.asyncio
async def test_aclose_flushes_the_final_absolute_spend():
    port = FakePort()
    gate, _, _ = await metered_gate(port=port)

    await gate.aclose()

    assert len(port.heartbeats) == 1, "teardown skipped the final flush"
