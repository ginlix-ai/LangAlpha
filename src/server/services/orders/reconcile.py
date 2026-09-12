"""Ask each brokerage what became of an order the host stopped watching.

Two things end an attempt that no turn will ever finish. A call that left the
relay and whose answer never came back leaves the row in ``submitting``, and
only the vendor knows whether an order exists behind it. An order that was
accepted and is still working leaves the row open, and it fills, or is
cancelled at the venue, with nobody in the loop to notice. Both are settled
here, by reading the vendor's own order list through the same egress relay a
direct tool call goes through, and writing the answer with a guarded UPDATE.

This job never places, changes or cancels anything. It calls read tools only,
and refuses to send one the capability map classifies as an order tool -- so
the relay's execution gate, which fires exactly on those, is never even
approached. Reconciliation therefore mints no execution token and needs none.

Multi-worker: two Postgres advisory locks, because the pass has two halves
with different costs. One runner per fleet holds the SQL-only head, where a
second worker would double-refuse a row or take the same candidates; the
vendor calls that follow are held per ``(user, server)`` instead, since that
is the only scope in which two workers ask one brokerage the same question.
Nothing is cached between passes; the ledger row is the only state, and every
write names the states it is allowed to overtake.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.config.env import EGRESS_RELAY_SECRET
from src.config.settings import get_order_reconcile_config
from src.server.database.mcp_oauth import SERVABLE, get_connection
from src.server.database.order_reconciliation import (
    Observation,
    active_grant_for_connection,
    cancelled_by_own_attempt,
    claimed_vendor_order_ids,
    fail_undispatched_attempts,
    lapse_unspent_approvals,
    list_stale_attempts,
    reconcile_group_lock,
    reconcile_pass_lock,
    record_observation,
    refuse_abandoned_proposals,
    unanswered_attempts,
)
from src.server.services.brokerage_capabilities import order_tool, vendor_for_url
from src.server.services.brokerage_orders import (
    OPEN_STATUSES,
    AttemptStatus,
    BrokerOrder,
    ListingIncomplete,
    MatchKey,
    OrderAdapter,
    StatusQuery,
    VendorOrder,
    adapter_for,
    instrument_to_json,
)
from src.server.services.egress.direct_tools import relay_mcp_client
from src.server.services.egress.execution_token import DEFAULT_TTL_SECONDS
from src.server.services.egress.relay_jwt import CALLER_HOST, mint_relay_jwt
from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

# The sandbox_id claim on this job's relay JWT. Audit-only at the relay, which
# authorizes on the (user, workspace) pair, but the claim must be non-empty --
# and in a relay log line this is what says the call came from the sweep.
RECONCILE_SANDBOX_ID = "reconcile"

# Pages one status read may turn before absence stops counting as evidence.
# At a hundred rows a page this is two thousand orders of history, which is
# more than the match window can hold; a list still going past it is read as
# incomplete, and an attempt missing from it is left open rather than failed.
MAX_LISTING_PAGES = 20

STOP_GRACE = 30.0

# How far a vendor's clock may run behind ours. ``executed_at`` is stamped
# before the call leaves, so this attempt's own order can predate it only by
# skew; an identical order listed earlier than that was placed by something
# else, most likely a person in the brokerage's own app.
CLOCK_SKEW_SECONDS = 60.0


@dataclass(frozen=True)
class Listing:
    """What one status read returned, and whether the vendor had more.

    ``complete`` is what turns absence into evidence: an attempt missing from
    a list the vendor cut short may be on the page not read.
    """

    orders: list[VendorOrder]
    complete: bool


@dataclass
class PassReport:
    """What one pass looked at and what it changed."""

    examined: int = 0
    moved: int = 0
    lapsed: int = 0
    undispatched: int = 0
    unresolved: int = 0
    skipped: int = 0
    errors: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def line(self) -> str:
        errors = ", ".join(f"{k}={v}" for k, v in sorted(self.errors.items()))
        return (
            f"examined={self.examined} moved={self.moved} lapsed={self.lapsed} "
            f"undispatched={self.undispatched} unresolved={self.unresolved} "
            f"skipped={self.skipped} errors={{{errors}}}"
        )


def _order_of(row: dict[str, Any]) -> BrokerOrder | None:
    payload = row.get("order_json")
    if not isinstance(payload, dict) or not payload:
        return None
    try:
        return BrokerOrder.from_json(payload)
    except Exception:
        logger.warning(
            "[OrderReconcile] attempt %s: stored order did not parse",
            row.get("attempt_id"),
            exc_info=True,
        )
        return None


def _result_body(result: Any) -> Any:
    """The vendor's payload out of an MCP call result, however the SDK wrapped it.

    Text first: every brokerage answers with its own JSON as text, and the
    structured field is what an SDK synthesizes when a server declares an
    output schema. Reading the synthesized one first would hand the adapter a
    re-wrapped shape instead of the envelope it parses.
    """
    blocks = getattr(result, "content", None) or []
    text = "".join(str(getattr(block, "text", "") or "") for block in blocks).strip()
    if text:
        return text
    structured = getattr(result, "structured_content", None)
    return structured if structured is not None else ""


# The instrument kind that names a vendor's own string and nothing a person
# reads: an id the request had to send because the vendor accepts nothing else.
_OPAQUE = "opaque"


def _better_instrument(order_json: Any, listed: Any) -> dict[str, Any] | None:
    """The listing's instrument when it beats the row's, else None."""
    patch = instrument_to_json(listed)
    if not patch or patch.get("kind") == _OPAQUE:
        return None
    current = order_json.get("instrument") if isinstance(order_json, dict) else None
    if isinstance(current, dict) and current and current.get("kind") != _OPAQUE:
        return None
    return patch


def _within(placed: datetime | None, executed: datetime | None, window: float) -> bool:
    """Whether a listed order was placed soon enough after the attempt to be its."""
    if placed is None or executed is None:
        return True
    offset = (placed - executed).total_seconds()
    return -CLOCK_SKEW_SECONDS <= offset <= window


def _candidates(
    listed: list[VendorOrder],
    *,
    key: MatchKey | None,
    claimed: set[str],
    executed_at: datetime | None,
    window: float,
) -> list[VendorOrder]:
    """Every listed order this attempt could be, which is rarely more than one.

    The caller settles only on exactly one. Two identical orders placed minutes
    apart are the case this exists for, and picking either would attach a real
    order to the wrong attempt -- worse than leaving the row open, because the
    row is what a person reads to decide whether to place it again.
    """
    if key is None:
        return []
    return [
        order
        for order in listed
        if key.matches(order.match_key)
        and order.vendor_order_id not in claimed
        and _within(order.placed_at, executed_at, window)
    ]


def _rivals(
    found: VendorOrder,
    unanswered: list[dict[str, Any]],
    *,
    attempt_id: str,
    key: MatchKey,
    adapter: OrderAdapter,
    window: float,
) -> list[dict[str, Any]]:
    """Every other unanswered attempt this listed order could equally be.

    The mirror of ``_candidates``: one order and two identical calls are as
    indistinguishable as two orders and one call, and which row a pass reached
    first says nothing about which call the vendor took.
    """
    return [
        other
        for other in unanswered
        if other.get("attempt_id") != attempt_id
        and key.matches(adapter.match_key(_order_of(other)))
        and _within(found.placed_at, other.get("executed_at"), window)
    ]


class OrderReconciler:
    """The per-worker pacemaker for the reconciliation pass."""

    _instance: OrderReconciler | None = None

    def __init__(self, *, config: Any = None) -> None:
        self._config = config
        self._loop_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> OrderReconciler:
        if cls._instance is None:
            cls._instance = OrderReconciler()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        config = self._settings()
        if not config.enabled:
            logger.info("[OrderReconcile] disabled by config")
            return
        if not EGRESS_RELAY_SECRET:
            logger.info("[OrderReconcile] no relay secret; nothing to read through")
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name="order-reconciler")
        logger.info(
            "[OrderReconcile] started (interval=%ds)", config.interval_seconds
        )

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning(
                "[OrderReconcile] loop ended with an error at shutdown",
                exc_info=True,
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    def _settings(self) -> Any:
        return self._config or get_order_reconcile_config()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            interval = float(self._settings().interval_seconds)
            # Jitter desynchronizes sibling workers so their advisory-lock
            # probes don't land in lockstep every cycle.
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval * (0.8 + 0.4 * random.random()),
                )
                return
            except TimeoutError:
                pass
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Never out of the loop: a brokerage outage, a bad row or a
                # dropped connection all end the pass, none of them the job.
                logger.error("[OrderReconcile] pass failed", exc_info=True)

    # ----------------------------------------------------------------- pass

    async def run_once(self) -> PassReport | None:
        """One bounded pass, or None when another worker holds the lock."""
        config = self._settings()
        async with reconcile_pass_lock() as held:
            if not held:
                logger.debug("[OrderReconcile] pass skipped, lock held elsewhere")
                return None
            # Proposals first: an approval waits while a sibling is still
            # proposed, so the approvals beside an abandoned one lapse this pass.
            abandoned = await refuse_abandoned_proposals(
                grace_seconds=config.approved_grace_seconds
            )
            for row in abandoned:
                logger.info(
                    "[OrderReconcile] attempt %s: proposed, run ended unasked; refused",
                    row["attempt_id"],
                )
            lapsed = await lapse_unspent_approvals(
                grace_seconds=config.approved_grace_seconds
            )
            for row in lapsed:
                logger.info(
                    "[OrderReconcile] attempt %s: approved, never executed; refused",
                    row["attempt_id"],
                )
            # Before the listing, not after: a row settled here is one the sweep
            # would otherwise fingerprint against the vendor's book, and one less
            # rival for the rows that do get read. The window is the longer of
            # the grace and the token's life, so no setting of the first can
            # judge a call the relay could still legitimately carry.
            undispatched = await fail_undispatched_attempts(
                grace_seconds=max(
                    float(config.submitting_grace_seconds), float(DEFAULT_TTL_SECONDS)
                )
            )
            for row in undispatched:
                logger.info(
                    "[OrderReconcile] attempt %s: never left the host; failed",
                    row["attempt_id"],
                )
            rows = await list_stale_attempts(
                submitting_grace_seconds=config.submitting_grace_seconds,
                open_after_seconds=config.open_after_seconds,
                limit=config.batch_limit,
            )
            report = PassReport(
                examined=len(rows),
                lapsed=len(abandoned) + len(lapsed),
                undispatched=len(undispatched),
            )
            groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                groups[(row["user_id"], row.get("server") or "")].append(row)
        # Out of the pass lock before the first vendor call. Everything above is
        # SQL and returns in milliseconds; everything below waits on a brokerage,
        # and holding one key across that is what let a single stalled account
        # stop reconciliation for the whole fleet until it answered.
        for (user_id, server), members in groups.items():
            if self._stop_event.is_set():
                report.skipped += len(members)
                continue
            try:
                async with reconcile_group_lock(user_id, server) as mine:
                    if not mine:
                        # A sibling worker is reading this account right now.
                        report.skipped += len(members)
                        continue
                    await self._reconcile_group(
                        user_id, server, members, config, report
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                report.errors[members[0].get("vendor") or "unknown"] += 1
                report.skipped += len(members)
                logger.warning(
                    "[OrderReconcile] group %s/%s failed", user_id, server,
                    exc_info=True,
                )
        logger.info("[OrderReconcile] pass %s", report.line())
        return report

    async def _reconcile_group(
        self,
        user_id: str,
        server: str,
        rows: list[dict[str, Any]],
        config: Any,
        report: PassReport,
    ) -> None:
        """Settle one user's attempts at one connected server."""
        connection = await get_connection(user_id, server)
        if connection is None or connection.status not in SERVABLE:
            logger.debug(
                "[OrderReconcile] %s/%s not reachable; %d attempt(s) left open",
                user_id, server, len(rows),
            )
            report.skipped += len(rows)
            return
        vendor = vendor_for_url(connection.server_url)
        adapter = adapter_for(vendor or "")
        if adapter is None:
            report.skipped += len(rows)
            return
        grant = await active_grant_for_connection(
            user_id, str(connection.connection_id)
        )
        if grant is None:
            logger.debug(
                "[OrderReconcile] %s/%s has no active grant; %d attempt(s) left open",
                user_id, server, len(rows),
            )
            report.skipped += len(rows)
            return

        orders = {row["attempt_id"]: _order_of(row) for row in rows}
        queries = self._plan(rows, orders, adapter, vendor or "", report)
        if not queries:
            return
        listings = await self._read(
            grant, user_id, vendor or "", adapter, queries, report
        )
        claims: dict[str | None, set[str]] = {}
        unanswered: dict[str | None, list[dict[str, Any]]] = {}
        for row in rows:
            query = queries.get(row["attempt_id"])
            if query is None:
                # Already accounted for in _plan: either nothing can read it,
                # or the read it named was refused.
                continue
            listing = listings.get(query.key())
            if listing is None:
                report.skipped += 1
                continue
            account = row.get("account_ref")
            if account not in claims:
                claims[account] = await claimed_vendor_order_ids(
                    user_id, vendor or "", account
                )
                unanswered[account] = await unanswered_attempts(
                    user_id, vendor or "", account
                )
            await self._record(
                row, orders[row["attempt_id"]], listing, claims[account],
                unanswered[account], adapter, config, report,
            )

    def _plan(
        self,
        rows: list[dict[str, Any]],
        orders: dict[str, BrokerOrder | None],
        adapter: OrderAdapter,
        vendor: str,
        report: PassReport,
    ) -> dict[str, StatusQuery]:
        """The read each attempt needs, dropping any that is not a read."""
        planned: dict[str, StatusQuery] = {}
        for row in rows:
            attempt_id = row["attempt_id"]
            query = adapter.status_query(
                orders[attempt_id],
                vendor_order_id=row.get("vendor_order_id"),
                route=row.get("route") or {},
            )
            if query is None:
                report.unresolved += 1
                continue
            if order_tool(vendor, query.tool) is not None:
                # Unreachable unless an adapter names a mutating tool as its
                # status read. Refused here rather than at the relay, because
                # this job must not be the thing that discovers it.
                logger.error(
                    "[OrderReconcile] %s: refusing to call order tool %r as a "
                    "status read",
                    vendor, query.tool,
                )
                report.errors[vendor] += 1
                continue
            planned[attempt_id] = query
        return planned

    async def _read(
        self,
        grant: dict[str, str],
        user_id: str,
        vendor: str,
        adapter: OrderAdapter,
        queries: dict[str, StatusQuery],
        report: PassReport,
    ) -> dict[tuple, Listing]:
        """Run each distinct status read once, through the user's own grant."""
        minted = mint_relay_jwt(
            EGRESS_RELAY_SECRET,
            user_id=user_id,
            workspace_id=grant["workspace_id"],
            sandbox_id=RECONCILE_SANDBOX_ID,
            caller=CALLER_HOST,
        )
        distinct = {query.key(): query for query in queries.values()}
        listings: dict[tuple, Listing] = {}
        client = relay_mcp_client(grant["grant_id"], token=minted.token)
        try:
            async with client:
                for key, query in distinct.items():
                    try:
                        listings[key] = await self._list(client, adapter, query)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        report.errors[vendor] += 1
                        logger.warning(
                            "[OrderReconcile] %s: status read %r failed: %s",
                            vendor, query.tool, e,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            report.errors[vendor] += 1
            logger.warning(
                "[OrderReconcile] %s: relay session did not open: %s", vendor, e
            )
        return listings

    @staticmethod
    async def _list(client: Any, adapter: OrderAdapter, query: StatusQuery) -> Listing:
        """Every page the vendor offers, up to the bound, and whether that was all."""
        orders: list[VendorOrder] = []
        page: StatusQuery | None = query
        for _ in range(MAX_LISTING_PAGES):
            result = await client.call_tool(page.tool, dict(page.args))
            body = _result_body(result)
            orders.extend(adapter.parse_status(page, body))
            try:
                page = adapter.next_page(page, body)
            except ListingIncomplete as e:
                # The vendor stopped answering mid-list. What was read still
                # settles an order found in it; absence from it settles nothing.
                logger.warning(
                    "[OrderReconcile] %s: %s; absence from this list is not "
                    "evidence",
                    query.tool, e,
                )
                return Listing(orders, complete=False)
            if page is None:
                return Listing(orders, complete=True)
        logger.warning(
            "[OrderReconcile] %s: %d pages read and the vendor still has more; "
            "absence from this list is not evidence",
            query.tool, MAX_LISTING_PAGES,
        )
        return Listing(orders, complete=False)

    async def _record(
        self,
        row: dict[str, Any],
        order: BrokerOrder | None,
        listing: Listing,
        claimed: set[str],
        unanswered: list[dict[str, Any]],
        adapter: OrderAdapter,
        config: Any,
        report: PassReport,
    ) -> None:
        """Resolve what the vendor said about one attempt, record it, report it."""
        attempt_id = row["attempt_id"]
        current = AttemptStatus(str(row["status"]))
        observation = await self._observe(
            row, current, order, listing, claimed, unanswered, adapter, config
        )
        if observation.status is None:
            report.unresolved += 1
            logger.log(
                logging.WARNING if observation.ambiguous else logging.INFO,
                "[OrderReconcile] attempt %s: %s; left %s",
                attempt_id, observation.reason, current,
            )
            return
        if await record_observation(attempt_id, current, observation) is None:
            report.unresolved += 1
            return
        if observation.vendor_order_id:
            # Claimed for the rest of this pass: a second identical attempt
            # must not settle on the order this one just took.
            claimed.add(observation.vendor_order_id)
        if observation.status == current:
            # The read named the state the row was already in: the fill grew or
            # the instrument got a name, and ``updated_at`` moved so the next
            # pass does not immediately re-read a row that has not changed.
            return
        report.moved += 1
        logger.info(
            "[OrderReconcile] attempt %s %s -> %s (%s)",
            attempt_id, current, observation.status, observation.reason,
        )

    async def _observe(
        self,
        row: dict[str, Any],
        current: AttemptStatus,
        order: BrokerOrder | None,
        listing: Listing,
        claimed: set[str],
        unanswered: list[dict[str, Any]],
        adapter: OrderAdapter,
        config: Any,
    ) -> Observation:
        """What this listing says about one attempt, or why it says nothing.

        An attempt that owns a vendor order id is looked up by it. One that
        does not has to be recognized by what was ordered, and two identical
        orders minutes apart are indistinguishable that way, as are two
        identical calls with one order between them, so ambiguity ends the
        resolution rather than picking either.
        """
        wanted = row.get("vendor_order_id")
        key = None if wanted else adapter.match_key(order)
        listed = listing.orders
        if wanted:
            found = next((o for o in listed if o.vendor_order_id == wanted), None)
        else:
            candidates = _candidates(
                listed,
                key=key,
                claimed=claimed,
                executed_at=row.get("executed_at"),
                window=float(config.match_window_seconds),
            )
            if len(candidates) > 1:
                # The vendor has orders we cannot tell apart, which is not the
                # same as having none: settling on either would name the wrong
                # order id in the ledger and in every surface reading it.
                ids = ", ".join(o.vendor_order_id for o in candidates)
                return Observation(
                    reason=(
                        f"{len(candidates)} listed orders match it ({ids}), so "
                        "it is left for a person to resolve"
                    ),
                    ambiguous=True,
                )
            found = candidates[0] if candidates else None
            if found is not None and (
                found.placed_at is None or row.get("executed_at") is None
            ):
                # Kept as a candidate so the row is not failed as absent, but not
                # adopted: with no time on one side the window proved nothing, and
                # an identical order placed days earlier would look just like it.
                return Observation(
                    reason=(
                        f"listed order {found.vendor_order_id} matches it but "
                        "cannot be dated against this call, so it is left for a "
                        "person to resolve"
                    ),
                    ambiguous=True,
                )
            if found is not None:
                rivals = _rivals(
                    found,
                    unanswered,
                    attempt_id=row["attempt_id"],
                    key=key,
                    adapter=adapter,
                    window=float(config.match_window_seconds),
                )
                if rivals:
                    # Adopting it would fail the rival as absent when it may be
                    # the call the vendor took, and a failed row is what a
                    # person reads as leave to place the order again.
                    return Observation(
                        reason=(
                            f"{len(rivals) + 1} unanswered attempts match listed "
                            f"order {found.vendor_order_id}, so it is left for a "
                            "person to resolve"
                        ),
                        ambiguous=True,
                    )
        if found is None:
            return await self._absent(
                row, current,
                had_identity=bool(wanted) or key is not None,
                complete=listing.complete,
            )
        outcome = found.outcome
        if outcome.status is AttemptStatus.UNKNOWN:
            # The vendor answered with a state this build cannot name. Writing
            # ``unknown`` over what the row already says would trade a fact for
            # the absence of one.
            return Observation(reason=f"{outcome.raw_status!r} is not a state we map")
        return Observation(
            status=outcome.status,
            reason=(
                f"vendor order {found.vendor_order_id}, raw {outcome.raw_status!r}"
            ),
            vendor_order_id=found.vendor_order_id,
            route=outcome.route or None,
            failure=(
                outcome.failure.to_json() if outcome.failure is not None else None
            ),
            # The fill is the answer a person came back for, and it changes
            # while the status word does not.
            filled_qty=outcome.filled_qty,
            avg_fill_price=outcome.avg_fill_price,
            fees=outcome.fees.to_json() if outcome.fees else None,
            instrument=_better_instrument(row.get("order_json"), found.instrument),
        )

    @staticmethod
    async def _absent(
        row: dict[str, Any],
        current: AttemptStatus,
        *,
        had_identity: bool,
        complete: bool,
    ) -> Observation:
        """A call the vendor has no order for, once the grace window has passed.

        An order this user cancelled is settled from that cancel, whatever
        state the row is in: a brokerage that answers a cancel by dropping the
        row, IBKR's deleted instruction being the case that forced this, would
        otherwise leave the placement re-read forever with the cancel that
        ended it one column away. Past that, only a call whose answer never
        came back ends here, ``submitting`` or ``unknown`` with no order id,
        and only when we had something to look it up by and the vendor listed
        everything it has: an attempt we could not identify, or one missing
        from a list the vendor cut short, is unresolved, not lost, and saying
        otherwise would report an order as failed that may be sitting in the
        book under a name we could not recognize or on a page we did not read.
        """
        vendor_order_id = row.get("vendor_order_id")
        if vendor_order_id and current in OPEN_STATUSES:
            cancel = await cancelled_by_own_attempt(
                row["user_id"],
                row.get("vendor") or "",
                str(vendor_order_id),
                account_ref=row.get("account_ref"),
                exclude_attempt_id=row["attempt_id"],
            )
            if cancel is not None:
                return Observation(
                    status=AttemptStatus.CANCELLED,
                    reason=(
                        f"vendor order {vendor_order_id}, cancelled by attempt "
                        f"{cancel['attempt_id']}"
                    ),
                )
        unanswered = current is AttemptStatus.SUBMITTING or (
            current is AttemptStatus.UNKNOWN and not vendor_order_id
        )
        if not unanswered or not had_identity:
            return Observation(reason="no matching order at the vendor")
        if not complete:
            return Observation(
                reason="not in the pages read, and the vendor has more"
            )
        return Observation(
            status=AttemptStatus.FAILED,
            reason="not_found",
            # Absence, and nothing the vendor said: the write holds it to the
            # row this was read from, so the attempt's own late answer wins.
            from_absence=True,
            failure={
                "kind": "reconciliation",
                "code": "not_found",
                "message": (
                    "the brokerage has no order for this call; it never reached "
                    "the book, and a retry is a new attempt"
                ),
            },
        )


async def _run_once_standalone() -> int:
    """One pass from a shell, with the pool this process has to open itself."""
    from src.config.logging_config import configure_logging
    from src.server.database.pool import get_or_create_pool

    configure_logging()
    logging.getLogger(__name__).setLevel(logging.INFO)
    pool = get_or_create_pool()
    await pool.open()
    try:
        report = await OrderReconciler().run_once()
    finally:
        await pool.close()
    if report is None:
        print("pass skipped: lock held elsewhere")  # noqa: T201
        return 1
    print(f"pass {report.line()}")  # noqa: T201
    return 0


def main() -> None:
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog="order-reconcile")
    parser.add_argument(
        "--once", action="store_true", help="run a single pass and exit"
    )
    args = parser.parse_args()
    if not args.once:
        parser.error("only --once is supported; the loop runs in the server")
    sys.exit(asyncio.run(_run_once_standalone()))


if __name__ == "__main__":
    main()
