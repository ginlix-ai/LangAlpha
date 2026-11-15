"""Paid calls on the deployment's own vendor account: priced and held before
the relay sends them, settled from the vendor's reply after.

A ``platform_mcp`` grant spends a key the deployment pays for, so the relay is
the one place that sees every paid call, whether the model's code made it or a
hand-written frame did. The cost comes from the vendor's own reply, because
the price a catalog lists is not always what a call costs (some tools charge
per record), and is written to ``metered_vendor_calls`` and charged to the
thread the call ran for as it settles (``services/metered_charges.py``).
Nothing else can recover it: a Gateway account's own usage endpoints report
zero (checked 2026-10-28), so this ledger is the only per-user record of what
was spent.

Each vendor reports cost in its own places, so each platform account carries
its own ``Meter`` (``platform_accounts.py``).
"""

from __future__ import annotations

import asyncio
import enum
import logging
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

import anyio
from fastapi import HTTPException

from ptc_agent.agent.provenance.trace import RelayedSource
from src.server.database.metered_vendor_calls import mark_job, release, reserve
from src.server.database.relayed_sources import stage
from src.server.dependencies.principal import CALL_HEADER
from src.server.dependencies.usage_limits import spendable_credits
from src.server.services.egress import Dispatch, RelayError, RelayRejection, header
from src.server.services.egress.jsonrpc import is_tool_error, tool_result
from src.server.services.livefs import outcomes
from src.server.services.metered_charges import settle_call, user_credits

if TYPE_CHECKING:
    from src.server.services.egress.relay import PreparedRelay

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Charge:
    """What a reply says its call cost, in the vendor's credits. With
    ``job_ref`` set it is the job's running total, of which only the growth
    since the last poll is billed."""

    credits: float
    job_ref: str | None = None


@dataclass(frozen=True)
class MeteredSource:
    """A source a meter read off a reply, priced in the vendor's credits."""

    source_type: str
    identifier: str
    body: str
    title: str | None = None
    provider: str | None = None
    site: str | None = None
    args: dict[str, Any] | None = None
    vendor_credits: float | None = None

    def priced(self, per_unit: float) -> RelayedSource:
        """As the turn's provenance holds it, its cost in the user's credits."""
        cost = self.vendor_credits
        return RelayedSource(
            source_type=self.source_type,
            identifier=self.identifier,
            title=self.title,
            provider=self.provider,
            site=self.site,
            args=self.args,
            cost_credits=None if cost is None else round(cost * per_unit, 2),
            body=self.body,
        )


class Meter(Protocol):
    """How one vendor's calls are priced, charged and read for sources."""

    #: The ``vendor`` its ledger rows carry.
    vendor: str
    #: The tracking name its credits are priced under (``auxiliary_tools``).
    tracking: str
    #: What a command's result calls its credits.
    label: str
    #: The most a user's spend not yet on any usage row (calls in flight, and
    #: calls waiting on the next turn) may reach, the call being admitted
    #: included, when no balance bounds it; with one, it pauses paid calls
    #: once reached, whatever is left.
    owed_cap: float

    def is_paid(self, tool: str) -> bool: ...

    def refusal(self, tool: str, arguments: Mapping[str, Any]) -> RelayRejection | None: ...

    async def load_catalog(
        self, tool: str, arguments: Mapping[str, Any], *, url: str, headers: Mapping[str, str]
    ) -> Mapping[str, Any]: ...

    def estimate(
        self, tool: str, arguments: Mapping[str, Any], catalog: Mapping[str, Any]
    ) -> float: ...

    def charge(
        self, tool: str, arguments: Mapping[str, Any], result: Mapping[str, Any]
    ) -> Charge | None: ...

    def job_ref(
        self, tool: str, arguments: Mapping[str, Any], result: Mapping[str, Any]
    ) -> str | None: ...

    def sources(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        catalog: Mapping[str, Any],
        call_cost: float,
    ) -> tuple[list[MeteredSource], str | None]: ...


async def _thread_of(computer_id: str | None, call_id: str | None) -> str | None:
    """The thread a paid call is charged to: the one the host filed its call
    id under when the command started, kept for a day, which outlasts a
    handed-off command on any computer the idle sweep stops. Unknown, it is
    charged to the user's next turn, so a failed read costs only where the
    charge lands."""
    if not call_id or not computer_id:
        return None
    try:
        context = await outcomes.call_context(computer_id, call_id)
    except Exception:  # noqa: BLE001 - see docstring
        logger.warning("[egress_meter] could not find the thread of call %s", call_id)
        return None
    return str(context.thread_id) if context is not None and context.thread_id else None


class Settled(enum.Enum):
    """What a sent call is charged."""

    #: Answered with an error, or nothing readable came back whole.
    FREE = "free"
    #: Its cost could not be read, the reply was cut short, or none came
    #: after the request went out: the vendor charged the call all the same,
    #: so its hold is the charge.
    ESTIMATE = "estimate"
    #: What the reply says it cost.
    REPORTED = "reported"


#: The most the route waits for a call's recording. Its writes are shielded
#: from the request's cancellation, so without a bound a starved pool would
#: hold the route open; past it the recording carries on behind the response.
_FINISH_BUDGET_S = 20.0

# Recordings in flight on this worker, held so a task the route stopped
# waiting for is not collected before it ends. Execution context only.
_recording: set[asyncio.Task] = set()


def _detach(work, tool: str) -> asyncio.Task:
    task = asyncio.ensure_future(work)
    _recording.add(task)

    def _done(t: asyncio.Task) -> None:
        _recording.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.error(
                "[egress_meter] could not record a %s call", tool, exc_info=t.exception()
            )

    task.add_done_callback(_done)
    return task


# Waits between retries of a failed release. A hold left in place is charged
# at its estimate once stale (``metered_vendor_calls._STALE_HOLD``, 5
# minutes), so the retries end well inside that.
_RELEASE_RETRY_S = (0.5, 2.0, 8.0, 30.0)


async def _release_free(hold_id: int, tool: str) -> None:
    """Drop the hold of a call that cost nothing. One failed write would leave
    it for the sweep to charge at its estimate, so a failure is retried, on a
    task of its own so the route is not held up by the waits."""
    try:
        await release(hold_id)
    except Exception:  # noqa: BLE001 - retried below
        logger.warning(
            "[egress_meter] could not free hold %s; retrying", hold_id, exc_info=True
        )
        _detach(_retry_release(hold_id), tool)


async def _retry_release(hold_id: int) -> None:
    for delay in _RELEASE_RETRY_S:
        await asyncio.sleep(delay)
        try:
            await release(hold_id)
            return
        except Exception:  # noqa: BLE001 - tried again until the waits run out
            pass
    logger.error(
        "[egress_meter] could not free hold %s; the sweep charges it at its estimate",
        hold_id,
    )


@dataclass(frozen=True)
class _Read:
    """What a reply settles, read off the event loop."""

    settled: Settled
    result: Mapping[str, Any] | None = None
    charge: Charge | None = None
    job_ref: str | None = None
    sources: tuple[RelayedSource, ...] = ()
    #: The tool answered that it failed, which is how a vendor may report a
    #: key it no longer accepts.
    tool_error: bool = False


@dataclass
class MeteredCall:
    """One paid call from its hold to its charge. The route feeds it the
    reply's bytes as they pass and calls ``finish`` once, however the
    exchange ended."""

    meter: Meter
    prepared: PreparedRelay
    hold_id: int
    call_id: str | None
    thread_id: str | None
    catalog: Mapping[str, Any]
    #: What the call is held at, in vendor credits: its charge when no cost
    #: can be read.
    estimate: float = 0.0
    dispatch: Dispatch = field(default_factory=Dispatch)
    _chunks: list[bytes] = field(default_factory=list)
    _finished: bool = False

    @property
    def tool(self) -> str:
        return self.prepared.canonical.tool_name or ""

    @property
    def arguments(self) -> Mapping[str, Any]:
        return self.prepared.canonical.arguments or {}

    def observe(self, chunk: bytes) -> None:
        """Keep a chunk of the reply; the relay's response cap bounds them.
        Only a 200's: any other answer is free, so its body is never read."""
        if self.dispatch.status == 200:
            self._chunks.append(chunk)

    def _read(self, body: bytes, complete: bool) -> _Read:
        """Parse a 200's body and price what it returned. Synchronous and
        pure, so it runs in a thread: a reply is up to 16 MiB of JSON, parsed
        twice (the envelope, then the tool's text)."""
        # Read even when cut short: the sandbox got at most these bytes, so a
        # reply that still parses is one it could read whole.
        result = tool_result(body)
        if result is None:
            # A whole reply that does not read as a result is an error result
            # or a shape the meter cannot price, and is not charged: the user
            # pays for what a reply shows it returned. Cut short, it may have
            # been either, so it is held at its estimate.
            return _Read(
                Settled.FREE if complete else Settled.ESTIMATE, tool_error=is_tool_error(body)
            )
        del body
        charge = self.meter.charge(self.tool, self.arguments, result)
        # A kept estimate still names the job its reply reports on, so a
        # later poll of that job nets its running total against it.
        job_ref = (
            charge.job_ref
            if charge is not None
            else self.meter.job_ref(self.tool, self.arguments, result)
        )
        sources: tuple[RelayedSource, ...] = ()
        if self.call_id is not None:
            # Each source carries its share of what the call is charged,
            # rounded up as the charge is: what the reply said, else the
            # estimate it is held at.
            cost = math.ceil(charge.credits if charge is not None else self.estimate)
            try:
                items, _ = self.meter.sources(
                    self.tool, self.arguments, result, catalog=self.catalog, call_cost=cost
                )
                per_unit = user_credits(self.meter, 1)
                sources = tuple(item.priced(per_unit) for item in items)
            except Exception:  # noqa: BLE001 - a lost source costs a Sources entry, never a charge
                logger.warning(
                    "[egress_meter] could not read the sources of a %s call",
                    self.tool,
                    exc_info=True,
                )
        return _Read(
            Settled.ESTIMATE if charge is None else Settled.REPORTED,
            result=result,
            charge=charge,
            job_ref=job_ref,
            sources=sources,
        )

    async def _settle(self, status: int | None, body: bytes | None, complete: bool) -> None:
        if status is None:
            status = self.dispatch.status
            if status is None and not self.dispatch.sent:
                await _release_free(self.hold_id, self.tool)
                return
        if status is None:
            read = _Read(Settled.ESTIMATE)
        elif status != 200 or (body is None and complete):
            read = _Read(Settled.FREE)
        elif body is None:
            # A 200 cut before its first byte (an SSE reply sends its headers
            # before the tool runs): the vendor took the call all the same.
            read = _Read(Settled.ESTIMATE)
        else:
            read = await asyncio.to_thread(self._read, body, complete)
            del body
        if read.tool_error and self.prepared.platform is not None:
            # Detached, so a key check never holds up the charge or the reply.
            _detach(
                self.prepared.platform.account.recheck_key(self.prepared.grant["user_id"]),
                self.tool,
            )
        if read.settled is Settled.FREE:
            await _release_free(self.hold_id, self.tool)
            return
        if read.job_ref is not None:
            # On the hold first, so a hold the sweep takes because its settle
            # never landed is still netted against the job's polls, at the
            # running total this reply reported.
            await mark_job(
                self.hold_id,
                read.job_ref,
                read.charge.credits if read.charge is not None else None,
            )
        if read.sources:
            # Before the charge, so a command that sees its calls settled
            # finds their sources held too.
            await self._stage_sources(read)
        if read.settled is Settled.ESTIMATE:
            logger.warning(
                "[egress_meter] %s reply for %s %s; charged at its estimate",
                self.meter.vendor,
                self.tool,
                "carried no readable cost"
                if read.result is not None
                else "was cut short"
                if status is not None
                else "never came after the request went out",
            )
        await settle_call(
            self.prepared.grant["user_id"],
            self.hold_id,
            credits=read.charge.credits if read.charge is not None else None,
            job_ref=read.job_ref,
        )

    async def finish(self, status: int | None, *, complete: bool) -> None:
        """Charge what the reply settles. ``status`` None is an exchange the
        route gave up on before relaying a reply: the hold is released only
        if the request never started out (``dispatch``), and kept as the
        charge if it did and no answer came. Once only. Never raises: the
        call has already been delivered or refused, and a failure here is
        unbilled usage to log, not an error to hand the sandbox.

        The recording runs as a task of its own that nothing cancels, so no
        write is cut off midway (a free call's release, a transaction's
        rollback). The route waits for it at most ``_FINISH_BUDGET_S``; past
        that it finishes behind the response, bounded by the pool's own
        timeouts.
        """
        if self._finished:
            return
        self._finished = True
        body = b"".join(self._chunks) if self._chunks else None
        self._chunks.clear()
        task = _detach(self._settle(status, body, complete), self.tool)
        del body
        try:
            async with asyncio.timeout(_FINISH_BUDGET_S):
                await asyncio.shield(task)
        except TimeoutError:
            logger.error(
                "[egress_meter] recording a %s call took over %ss; it goes on behind "
                "the response (hold %s)",
                self.tool,
                _FINISH_BUDGET_S,
                self.hold_id,
            )
        except Exception:  # noqa: BLE001 - logged by the task itself
            pass

    async def _stage_sources(self, read: _Read) -> None:
        """Hold the reply's sources for the turn's provenance, under the
        grant's server. Apart from billing: a source lost here costs a
        Sources entry, never a charge."""
        grant = self.prepared.grant
        try:
            await stage(
                user_id=grant["user_id"],
                call_id=self.call_id,
                job_ref=read.job_ref,
                server=grant["server_name"],
                tool=self.tool,
                sources=read.sources,
            )
        except Exception:  # noqa: BLE001 - see docstring
            logger.warning(
                "[egress_meter] could not hold the sources of a %s call", self.tool, exc_info=True
            )


_UNAVAILABLE = "credit check unavailable"


def _quota_refusal(e: HTTPException) -> RelayRejection:
    """The relay's answer to a credit check that refused, carrying the auth
    service's own message. A 429 is a denial, and only a credit limit is the
    user being out of credits; the auth service's own outage (its
    ``service_unavailable``) and any non-429 are a check to retry."""
    detail = e.detail if isinstance(e.detail, Mapping) else {}
    kind = str(detail.get("type") or "")
    message = detail.get("message")
    message = message.strip() if isinstance(message, str) else ""
    if e.status_code != 429 or kind == "service_unavailable":
        retry = detail.get("retry_after")
        return RelayRejection(
            503,
            RelayError.CREDIT_CHECK_UNAVAILABLE,
            _UNAVAILABLE,
            retry_after=retry if isinstance(retry, int) and retry > 0 else 30,
        )
    if kind.endswith("credit_limit"):
        return RelayRejection(
            402,
            RelayError.CREDITS_EXHAUSTED,
            message or "the user has no credits left for paid calls",
        )
    # Any other denial (a failed payment, a surface the plan leaves out) is
    # one the user has to clear, so the agent is told to tell them, with the
    # auth service's reason.
    return RelayRejection(
        402,
        RelayError.CREDITS_EXHAUSTED,
        message or f"the user's account refuses paid calls ({kind or 'denied'})",
    )


async def spendable_for_paid_calls(user_id: str) -> float | None:
    """What ``user_id`` may still spend in our credits, None for no limit, or
    the relay's refusal when the auth service denies or cannot answer."""
    try:
        return await spendable_credits(user_id)
    except HTTPException as e:
        raise _quota_refusal(e) from e


async def admit(prepared: PreparedRelay, headers: Mapping[str, str]) -> MeteredCall | None:
    """Price a paid call before it is sent and hold that price while it runs;
    None for any call that is not paid.

    Admitted only if the price and everything the user has spent but not been
    charged for fit what they can still spend: the turn's credit gate looks
    only between model steps, the balance the auth service reports does not
    count calls not charged yet, and one call can run ten catalog tools. The
    hold is a ledger row, so calls made at once see each other's. Once this
    returns a call, the route owns its hold and must ``finish`` it.
    """
    grant, canonical = prepared.grant, prepared.canonical
    if prepared.platform is None or canonical.method != "tools/call":
        return None
    meter = prepared.platform.account.meter
    tool, arguments = canonical.tool_name or "", canonical.arguments or {}
    if not meter.is_paid(tool):
        return None
    refusal = meter.refusal(tool, arguments)
    if refusal is not None:
        raise refusal
    unavailable = RelayRejection(
        503, RelayError.CREDIT_CHECK_UNAVAILABLE, _UNAVAILABLE, retry_after=30
    )
    # Unpriced, every call would be charged nothing and the balance would
    # bound nothing, while the vendor bills the deployment for each.
    per_unit = user_credits(meter, 1)
    if not per_unit > 0:
        logger.error(
            "[egress_meter] %s has no price per unit; paid calls are refused", meter.tracking
        )
        raise unavailable
    # Before the read: a call charged after this instant may be missing from
    # the balance read, so the ledger counts it against the balance as well.
    balance_read_at = time.monotonic()
    # Independent reads, run together. The balance's refusal still comes
    # first, as it did when the catalog was read only after it.
    spendable, catalog = await asyncio.gather(
        spendable_for_paid_calls(grant["user_id"]),
        meter.load_catalog(
            tool, arguments, url=grant["destination_url"], headers=prepared.credential.headers
        ),
        return_exceptions=True,
    )
    if isinstance(spendable, BaseException):
        raise spendable
    if isinstance(catalog, BaseException):
        if not isinstance(catalog, Exception):
            raise catalog
        logger.warning("[egress_meter] could not price a %s call: %s", tool, catalog)
        raise unavailable from catalog
    estimate = meter.estimate(tool, arguments, catalog)
    if not (math.isfinite(estimate) and estimate >= 0):
        logger.error("[egress_meter] %s priced a %s call at %r", meter.vendor, tool, estimate)
        raise unavailable
    call_id = outcomes.valid_call_id(header(headers, CALL_HEADER))
    thread_id = await _thread_of(prepared.claims.computer_id, call_id)
    # The hold's id as soon as it is written, so a cancellation that lands
    # after its commit, before this returns a call to own it, can still free
    # it: nothing was sent.
    held: list[int] = []
    try:
        verdict, hold_id = await reserve(
            user_id=grant["user_id"],
            computer_id=prepared.claims.computer_id,
            workspace_id=prepared.claims.workspace_id,
            call_id=call_id,
            thread_id=thread_id,
            vendor=meter.vendor,
            tool=tool,
            estimate=estimate,
            owed_cap=meter.owed_cap,
            allowed=spendable / per_unit if spendable is not None else None,
            balance_read_at=balance_read_at if spendable is not None else None,
            on_hold=held.append,
        )
    except BaseException as e:
        if held:
            with anyio.CancelScope(shield=True):
                await _release_free(held[0], tool)
        if isinstance(e, Exception):
            raise unavailable from e
        raise
    if verdict == "charges_pending":
        raise RelayRejection(
            402, RelayError.CHARGES_PENDING, "earlier paid calls are not charged yet"
        )
    if verdict == "exhausted":
        raise RelayRejection(
            402,
            RelayError.CREDITS_EXHAUSTED,
            "the user's uncharged paid calls use up their remaining credits",
        )
    if verdict == "exceeds":
        raise RelayRejection(
            402,
            RelayError.COST_EXCEEDS_BALANCE,
            f"this call would cost about {user_credits(meter, estimate):g} credits,"
            + (
                " more than the user has left"
                if spendable is not None
                else " more than one call may run up before it is charged"
            ),
        )
    assert hold_id is not None
    return MeteredCall(
        meter=meter,
        prepared=prepared,
        hold_id=hold_id,
        call_id=call_id,
        thread_id=thread_id,
        catalog=catalog,
        estimate=estimate,
    )
