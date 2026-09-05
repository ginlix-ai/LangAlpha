"""Runtime credit gate — per-model-boundary spend check against a turn ceiling.

The quota check otherwise runs once at admission and never again until
finalize, so a long turn can spend unbounded platform credits mid-run. This
middleware closes that window: each turn meters its own platform spend (LLM
records priced at record time + per-use infrastructure credits) and compares
it, at every model boundary, against a ceiling granted by the quota service.
The comparison is own-spend vs own-ceiling: a turn measures what it has spent
against what it was granted, never against a shared "remaining" figure that
every other turn is drawing down at the same time.

**One reservation per turn, not per lane.** A turn is one billable unit: the
main run and every subagent it spawns spend from the same budget, so they
share one lease. Splitting the reservation per lane is what made a subagent
unrunnable on a small balance: asking under its own ref put a child in
competition with its own parent for the one budget they both spend from.
Hence the two objects here:

- :class:`CreditLease` — one per turn family. Owns the reservation: the
  acquire loop, the ceiling, the verdict, and the members spending under it.
- :class:`CreditGateState` — one per lane. Owns that lane's metering and its
  own ledger heartbeat, and defers the stop decision to the lease.

The ledger stays per-run (a subagent's spend lands on its own row); only the
reservation is shared. Membership is refcounted and the last member out
releases, because subagent writers routinely outlive the main run: the
parent's stream ends while children are still working, so a parent-owned
release would leave them spending against nothing.

Enforcement is deliberately one verdict behind (accepted overshoot: one
model boundary): the gate never blocks a call waiting on the network. The
lease's refresher extends the reservation asynchronously and each lane
flushes its own spend heartbeat; the gate only enforces what the refresher
last learned. Mid-run, inability to check NEVER stops a turn — refresh
failures and unreachable services fail open with spend still metered and
heartbeated. The only stop is a healthy service answering "insufficient
credits":

- The main run pauses via ``interrupt()`` (a resumable HITL checkpoint,
  relaying the service's denial message verbatim). The resume arrives as a
  new POST with its own lease, so nothing here re-checks on the way back:
  admission refuses a still-short account before any model call is paid for.
- A subagent run raises :class:`CreditStopError` instead, settling terminal
  through the normal writer path with the stop reason in its failure
  payload; its checkpoint survives, so the parent can resume it later via
  ``Task(action="resume")``.

Everything turn-local hangs off the lane state reached through a ContextVar —
the middleware instance itself is stateless because compiled subagent graphs
share middleware instances across lanes. Deployments with no quota service
never set the ContextVar, so the whole gate is inert.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Literal, Optional

from langchain.agents.middleware.types import AgentMiddleware, AgentState
from langgraph.runtime import Runtime
from langgraph.types import interrupt

from src.config.env import USD_TO_CREDITS_RATE
from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
from src.utils.tracking.infrastructure_costs import calculate_infrastructure_credits

logger = logging.getLogger(__name__)

# Heartbeat flush triggers: elapsed time OR credits accrued since the last
# flush, whichever first. Each lane polls its own meter cheaply and only
# touches the DB when a trigger fires.
_FLUSH_INTERVAL_SECONDS = 10.0
# Written in USD because that is the unit it was reasoned about in: 50 credits
# reads like a threshold and is five cents, which any real model call clears,
# so the "or" arm fired on essentially every poll and the interval above never
# got to be the floor it was meant to be.
_FLUSH_CREDIT_DELTA = 1.0 * USD_TO_CREDITS_RATE
_POLL_SECONDS = 2.0
# Extend the lease when it is this close to expiry even if spend hasn't
# reached the ceiling — a lease that lapses mid-turn no longer covers the
# run, and the gate would go on enforcing a grant it does not hold.
# Public because the startup wiring check compares the service's granted TTL
# against it: a TTL at or below this margin puts every lease inside the renew
# window at the moment it is granted, which is a re-acquire loop wearing the
# costume of a working gate.
LEASE_RENEW_MARGIN_SECONDS = 120.0
# Consecutive "row no longer open" heartbeats before a lane accepts its run
# has settled elsewhere and stops its own heartbeat (backstop for any
# teardown path that missed aclose()).
_DEAD_ROW_STOPS = 3
# After an acquire that produced no verdict (unreachable / 5xx), wait this
# long before asking again — fail-open must not turn into a hot retry loop.
_ACQUIRE_RETRY_BACKOFF_SECONDS = 15.0
# Floor between two acquires that DID get an answer. A grant whose TTL is short
# or unreadable leaves the deadline permanently inside the renew margin, and a
# lane sitting at its ceiling re-asks on the same condition every poll: either
# way a healthy service gets asked every _POLL_SECONDS for the whole turn.
_ACQUIRE_MIN_SPACING_SECONDS = 5.0
# Last resort only. The quota service authors the denial copy and we relay it
# verbatim; this stands in for the one case where a denial arrives with none,
# so neither stop surface is left without an explanation.
_DEFAULT_STOP_MESSAGE = "Stopped by the credit gate."


class CreditStopError(Exception):
    """A subagent run stopped by the credit gate (healthy-service denial).

    Reaches the background writer's generic exception handler, so the run
    settles terminal with ``error_type: credit_stop``
    (:data:`~src.server.contracts.status.CREDIT_STOP_ERROR_TYPE`) in its
    failure payload — the marker the parent's resume-time injection looks for.
    """


@dataclass(frozen=True)
class LeaseVerdict:
    """One answered acquire, normalized by the port.

    The port is the only producer and this class is the only consumer, so the
    fields are guaranteed rather than defended: a body the port could not read
    becomes ``None`` in place of a verdict, which the gate treats as "keep
    enforcing the last one I trusted".
    """

    granted: bool
    ceiling_credits: float = 0.0
    ttl_seconds: float = 0.0
    generation: Optional[int] = None
    quota: Optional[dict] = None


class _GateLoop:
    """Shared lifecycle for this module's two background loops.

    Both the lease's refresher and a lane's heartbeat are one cancellable
    task that polls, must never let a tick's failure kill the loop, and tear
    down exactly once even when the teardown itself is being cancelled. Only
    the tick differs, so only the tick lives in the subclasses.
    """

    _refresher: Optional[asyncio.Task]
    _closed: bool

    async def _poll_forever(self, tick: Any, label: str) -> None:
        while not self._closed:
            await asyncio.sleep(_POLL_SECONDS)
            try:
                await tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "[CreditGate] %s tick failed", label, exc_info=True
                )

    async def _stop_refresher(self) -> bool:
        """Cancel and await the loop. False when teardown already ran, which
        is the caller's cue to skip the rest of its own teardown."""
        if self._closed and self._refresher is None:
            return False
        self._closed = True
        refresher, self._refresher = self._refresher, None
        if refresher is not None:
            refresher.cancel()
            with contextlib.suppress(BaseException):
                await asyncio.shield(refresher)
        return True


@dataclass(eq=False)
class CreditLease(_GateLoop):
    """One turn's reservation, shared by the main run and its subagents.

    Identity is the main run's ref, which is what the quota service reserves
    against. Members join as their lanes start and leave as they finish;
    spend from a departed member is retained in ``_retired_credits`` so the
    family total never drops. The last member out releases — subagent
    writers outlive the main run, so "the parent finished" is not the same
    question as "this turn is done spending".

    ``port`` is duck-typed (the server injects it; its absence is the OSS
    shape and disables the gate entirely):

    - ``await port.acquire(user_id, run_ref, spent_credits, rate_multiplier,
      byok)`` → :class:`LeaseVerdict`, or None for "no verdict" (unreachable
      / 5xx — fail open).
    - ``await port.release(user_id, run_ref, generation)`` → best-effort
      lease retirement, fenced on the generation of the grant being retired.
    """

    user_id: str
    run_ref: str
    port: Any

    # Whether this turn runs on a credential the user pays for themselves.
    # Relayed, never branched on here: which pools apply to an own key is the
    # quota service's rule, and admission already asks it the same question.
    # Dropping it is what let a lease answer a turn admission had allowed.
    is_byok: bool = False
    ceiling_credits: float = 0.0
    denial: Optional[dict] = None
    # The service's own version of this reservation, echoed back on release so
    # it only applies to the grant this lease actually holds. None until a
    # verdict carries one, which leaves that release unconditional.
    grant_generation: Optional[int] = None
    _lease_deadline: float = 0.0  # monotonic; 0 = no live lease
    _members: set["CreditGateState"] = field(default_factory=set)
    _retired_credits: float = 0.0
    _refresher: Optional[asyncio.Task] = None
    _closed: bool = False
    _next_acquire_at: float = 0.0
    # Makes one membership transition atomic. A lane can arrive after the last
    # one left — a subagent's first step is scheduled independently of its
    # spawn, so it can land past the parent's teardown — and interleaving that
    # arrival with the departure is what leaves a lease closed with members in
    # it, or open with none.
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # -- membership --------------------------------------------------------

    async def join(self, member: "CreditGateState") -> None:
        """Attach a lane and make sure the refresher is running, reopening the
        lease if the last lane already left."""
        async with self._lock:
            self._closed = False
            self._members.add(member)
            self._start_refresher()

    async def leave(self, member: "CreditGateState") -> None:
        """Retire a lane, keeping its spend in the family total, and release
        the reservation once the last member is gone."""
        async with self._lock:
            if member in self._members:
                self._members.discard(member)
                self._retired_credits += member.spend()
            if self._members or not await self._stop_refresher():
                return
            generation = self.grant_generation
        # Outside the lock: a release is a network call, and nothing here ever
        # makes a lane wait on the network. A lane joining while this is in
        # flight reopens the lease and acquires a grant of its own; the fence
        # is what keeps this release off it — it retires the generation it was
        # granted, not whatever is current. Best-effort and cancellation-safe:
        # the lease expires on its own if the release is lost.
        with contextlib.suppress(BaseException):
            await asyncio.shield(
                self.port.release(self.user_id, self.run_ref, generation)
            )

    def spend(self) -> float:
        """The whole turn's platform spend: every live lane plus everything
        already retired. Each lane read is O(1) and none of them await, so
        this is atomic against the event loop."""
        return self._retired_credits + sum(m.spend() for m in tuple(self._members))

    def rate_multiplier(self) -> float:
        """The priciest model boundary live on this turn right now.

        One reservation covers every lane, so it has to be sized for the most
        expensive of them: a chunk sized for a cheap main run would be spent by
        a single call on a subagent pinned to a premium model, and that lane
        would re-ask the service on every boundary. Retired lanes are excluded
        deliberately — they are done spending, and holding their rate would keep
        the whole turn reserving against a model no longer running.
        """
        members = tuple(self._members)
        return max((m.rate_multiplier for m in members), default=1.0)

    def should_stop(self, spent: float) -> bool:
        """Only a recorded healthy-service denial stops, and only past the
        ceiling the turn was already granted — spend under a standing grant
        is entitled to finish."""
        return self.denial is not None and spent >= self.ceiling_credits

    # -- verdict -----------------------------------------------------------

    def _record(self, verdict: Optional[LeaseVerdict]) -> None:
        if verdict is None:
            return  # no verdict — keep enforcing the previous one
        # Taken from any answered verdict, not only a grant: a denial still
        # refreshes the reservation's reported spend, so its generation is the
        # freshest thing the service has told us about this lease.
        if verdict.generation is not None:
            self.grant_generation = verdict.generation
        if verdict.granted:
            # Monotone on purpose: a stale response arriving out of order
            # must not shrink a granted ceiling.
            self.ceiling_credits = max(self.ceiling_credits, verdict.ceiling_credits)
            self.denial = None
            self._lease_deadline = time.monotonic() + verdict.ttl_seconds
        else:
            # Empty rather than None: ``should_stop`` reads the presence of a
            # denial, and the pause copy falls back on its own.
            self.denial = verdict.quota or {}

    async def _acquire(self, spent: float) -> Optional[LeaseVerdict]:
        try:
            return await self.port.acquire(
                self.user_id,
                self.run_ref,
                spent,
                self.rate_multiplier(),
                self.is_byok,
            )
        except Exception:
            logger.warning(
                "[CreditGate] lease acquire failed for %s; failing open",
                self.run_ref,
                exc_info=True,
            )
            return None

    # -- refresher ---------------------------------------------------------

    def _start_refresher(self) -> None:
        """Idempotent: every lane joins, the first one starts the loop. Call
        under the lock."""
        if self._refresher is None and not self._closed:
            self._refresher = asyncio.create_task(
                self._refresh_loop(), name=f"credit-lease-{self.run_ref[:8]}"
            )

    async def _refresh_loop(self) -> None:
        # First acquire immediately: the turn starts with ceiling 0, and the
        # sooner a real grant lands the sooner the gate has a verdict newer
        # than admission's. Guarded like every later tick, and for a sharper
        # reason: a tick that raises loses one verdict, but this one would kill
        # the task before the poll loop starts — and a dead refresher never
        # comes back, because ``start`` sees a non-None task and returns. The
        # turn would run to completion with no ceiling and no denial.
        try:
            self._record(await self._acquire(self.spend()))
        except Exception:
            logger.warning(
                "[CreditGate] lease priming acquire failed for %s",
                self.run_ref,
                exc_info=True,
            )
        await self._poll_forever(self._tick, f"lease {self.run_ref}")

    async def _tick(self) -> None:
        now = time.monotonic()
        if now < self._next_acquire_at:
            return
        credits = self.spend()
        if self.denial is None:
            needs_lease = (
                credits >= self.ceiling_credits
                or self._lease_deadline - now < LEASE_RENEW_MARGIN_SECONDS
            )
        else:
            # A denied turn under its ceiling keeps asking: a top-up in the
            # gap turns its next verdict green and it never stops at all.
            # Past its ceiling it stops at the next boundary — re-asking
            # is the resume path's job, not the refresher's.
            #
            # Ceiling 0 is the exception, and it is the common denial: a denial
            # carries no ceiling, so a run refused on its first acquire is
            # "past" one it was never granted. Excluding it would withhold the
            # top-up window from exactly the user most likely to use it.
            needs_lease = (
                credits < self.ceiling_credits or self.ceiling_credits <= 0.0
            )
        if needs_lease:
            verdict = await self._acquire(credits)
            self._record(verdict)
            # Space the next ask on every branch. Backing further off when
            # there was no verdict keeps an unreachable service from being
            # hammered; the shorter floor on an answered one costs at most a
            # boundary of enforcement lag, which this gate is built to absorb
            # anyway — it is one verdict behind by design.
            self._next_acquire_at = now + (
                _ACQUIRE_RETRY_BACKOFF_SECONDS
                if verdict is None
                else _ACQUIRE_MIN_SPACING_SECONDS
            )


@dataclass(eq=False)
class CreditGateState(_GateLoop):
    """One lane's metering and ledger heartbeat, gated by the turn's lease.

    The lane owns what is genuinely per-run — its trackers and its own
    ledger row — and nothing about the reservation, which is the lease's.
    ``port.heartbeat(kind, run_ref, credits)`` returns True when the run's
    ledger row accepted the absolute spend value, False when the row is no
    longer open, None on error.
    """

    run_ref: str
    kind: Literal["run", "task"]  # conversation response | subagent run
    port: Any
    lease: CreditLease
    tracker: Any = None
    tool_tracker: Any = None
    # How much one model boundary on this lane costs relative to the baseline
    # the reservation is sized in. 1.0 is "no opinion", which is what an
    # unpriced model and every OSS build get.
    rate_multiplier: float = 1.0

    _refresher: Optional[asyncio.Task] = None
    _closed: bool = False
    _last_flush_at: float = field(default_factory=time.monotonic)
    _last_flushed_credits: float = 0.0
    _last_attempted_credits: float = 0.0
    _dead_row_count: int = 0
    _last_spend: float = 0.0
    _spend_error_logged: bool = False

    def spend(self) -> float:
        """This lane's cumulative platform spend, in credits — the tracker's
        incremental platform-USD total (priced per record at record time, so
        peak-hour schedules are honored) plus per-use infrastructure credits.
        Advisory; billing truth stays the finalize-time batch pass.

        Never raises. Both reads are best-effort — the tool-usage map is
        mutated lock-free by concurrently running tools, and the pricing pass
        can reject a malformed entry — and this sits on the model-boundary
        path, where inability to check must never stop a run. On failure the
        last good value stands, so a broken meter costs the gate precision,
        never the run.
        """
        try:
            credits = 0.0
            if self.tracker is not None:
                credits += self.tracker.platform_usd_total() * USD_TO_CREDITS_RATE
            if self.tool_tracker is not None:
                usage = self.tool_tracker.get_summary()
                if usage:
                    credits += float(
                        calculate_infrastructure_credits(usage).get("total_credits")
                        or 0.0
                    )
        except Exception:
            if not self._spend_error_logged:
                # Once per lane: the heartbeat re-reads every couple of
                # seconds, and a persistent fault would drown the log.
                self._spend_error_logged = True
                logger.warning(
                    "[CreditGate] spend read failed for %s %s; holding %.2f",
                    self.kind,
                    self.run_ref,
                    self._last_spend,
                    exc_info=True,
                )
            return self._last_spend
        self._last_spend = credits
        return credits

    def spawn_child(
        self,
        *,
        run_ref: str,
        tracker: Any,
        tool_tracker: Any,
        rate_multiplier: Optional[float] = None,
    ) -> "CreditGateState":
        """A subagent lane: its own metering and ledger row, the same lease.
        Children spend from the turn's budget, so they reserve nothing of
        their own — asking separately is what got them denied against their
        own parent's grant.

        A lane keeps the parent's rate unless it was given one of its own: a
        subagent may be pinned to a pricier model than the turn it serves, and
        the reservation has to cover the boundary it will actually hit.
        """
        return CreditGateState(
            run_ref=run_ref,
            kind="task",
            port=self.port,
            lease=self.lease,
            tracker=tracker,
            tool_tracker=tool_tracker,
            rate_multiplier=(
                self.rate_multiplier if rate_multiplier is None else rate_multiplier
            ),
        )

    # -- heartbeat ---------------------------------------------------------

    async def start(self) -> None:
        """Join the turn's lease and start flushing this lane's spend."""
        if self._refresher is not None or self._closed:
            return
        await self.lease.join(self)
        # Self-stop (a closed ledger row) is not teardown: aclose() still owns
        # the final flush and leaving the lease.
        self._refresher = asyncio.create_task(
            self._poll_forever(
                self._flush_if_due, f"heartbeat {self.kind} {self.run_ref}"
            ),
            name=f"credit-lane-{self.run_ref[:8]}",
        )

    async def _flush_if_due(self) -> None:
        now = time.monotonic()
        credits = self.spend()
        if credits <= self._last_flushed_credits:
            # Nothing new to record. GREATEST already makes an unchanged write
            # a semantic no-op, but not a free one: in_flight_credits sits in
            # the predicate of both partial indexes, so no update of this row
            # can be HOT — every beat reinserts it into every index on the
            # table. An idle lane (a long execute_code, a subagent waiting on
            # a sandbox) would otherwise write a dead tuple every interval for
            # the life of the run. _last_flush_at is deliberately left alone,
            # so the moment spend resumes the next poll flushes immediately.
            return
        if (
            now - self._last_flush_at >= _FLUSH_INTERVAL_SECONDS
            or credits - self._last_attempted_credits >= _FLUSH_CREDIT_DELTA
        ):
            await self._flush_heartbeat(credits)

    async def _flush_heartbeat(self, credits: float) -> None:
        """Write this lane's absolute spend, advancing the cursor only if it lands.

        Both markers move before the await, but they answer different questions.
        ``_last_attempted_credits`` spaces retries, so a lane whose write failed
        backs off to the flush interval rather than re-asking on every poll,
        while ``_last_flushed_credits`` is what the unchanged-spend guard reads
        and so may only ever name spend the ledger has actually accepted.
        """
        self._last_flush_at = time.monotonic()
        self._last_attempted_credits = credits
        if credits <= 0:
            return
        try:
            accepted = await self.port.heartbeat(self.kind, self.run_ref, credits)
        except Exception:
            logger.warning(
                "[CreditGate] heartbeat failed for %s %s",
                self.kind,
                self.run_ref,
                exc_info=True,
            )
            return
        if accepted is False:
            self._dead_row_count += 1
            if self._dead_row_count >= _DEAD_ROW_STOPS:
                logger.info(
                    "[CreditGate] run row closed; heartbeat stopping for %s %s",
                    self.kind,
                    self.run_ref,
                )
                self._closed = True
            return
        if accepted is not True:
            # The port's documented error shape. It is not evidence the row is
            # still open, so the dead-row count stands rather than resetting.
            return
        self._last_flushed_credits = credits
        self._dead_row_count = 0

    async def aclose(self) -> None:
        """Teardown: stop the heartbeat, flush the final absolute spend, then
        leave the lease (which releases it if this was the last lane). Every
        step best-effort and cancellation-safe."""
        if not await self._stop_refresher():
            return
        with contextlib.suppress(BaseException):
            await asyncio.shield(self._flush_heartbeat(self.spend()))
        with contextlib.suppress(BaseException):
            await asyncio.shield(self.lease.leave(self))


current_credit_gate: contextvars.ContextVar[Optional[CreditGateState]] = (
    contextvars.ContextVar("current_credit_gate", default=None)
)


async def run_with_credit_gate(
    gate: Optional[CreditGateState], stream: AsyncIterator[Any]
) -> AsyncIterator[Any]:
    """Drive a turn's stream inside its gate's lifetime.

    Sets the lane ContextVar and joins the lease before the first event, and
    closes the lane when the stream ends however it ends — completion, error,
    interrupt, or the driving task being cancelled all funnel through the
    ``finally``. Closing the main lane does not necessarily release the
    lease: subagent lanes that outlive this stream hold it open. With
    ``gate=None`` the stream passes through untouched (the OSS shape).
    """
    if gate is None:
        async for event in stream:
            yield event
        return
    token = current_credit_gate.set(gate)
    await gate.start()
    try:
        async for event in stream:
            yield event
    finally:
        # A generator finalized from a task other than the one that started it
        # cannot reset the token; teardown must still run, or the lease and
        # both loops outlive the turn.
        with contextlib.suppress(ValueError):
            current_credit_gate.reset(token)
        await gate.aclose()


def build_pause_payload(lease: CreditLease) -> dict:
    """The interrupt payload for a credit pause — the service's denial copy
    relayed verbatim, with a bare fallback so the card can never render an
    explanation-less box (a denial without copy is the service's own edge
    case, not ours to word)."""
    denial = lease.denial or {}
    return {
        "action_requests": [
            {
                "type": INTERRUPT_REASON_CREDIT_PAUSE,
                "message": denial.get("message") or _DEFAULT_STOP_MESSAGE,
            }
        ]
    }


class CreditGateMiddleware(AgentMiddleware):
    """Model-boundary enforcement point. Stateless — shared across lanes."""

    async def abefore_model(
        self, state: AgentState, runtime: Runtime
    ) -> dict[str, Any] | None:
        gate = current_credit_gate.get()
        if gate is None:
            return None
        lease = gate.lease
        # One family-spend read per boundary, reused by the stop decision,
        # the log line, and (via recheck) the next acquire.
        spent = lease.spend()
        if not lease.should_stop(spent):
            return None
        if gate.kind == "task":
            denial = lease.denial or {}
            raise CreditStopError(denial.get("message") or _DEFAULT_STOP_MESSAGE)
        # Main run: pause at a clean checkpoint. Nothing here re-checks on the
        # way back — a resume arrives as a new POST with its own lease, and
        # admission (``enforce_credit_limit``) has already refused it if the
        # account is still short, before any model call is paid for.
        logger.info(
            "[CreditGate] pausing run %s: spent=%.2f ceiling=%.2f",
            gate.run_ref,
            spent,
            lease.ceiling_credits,
        )
        interrupt(build_pause_payload(lease))
        return None
