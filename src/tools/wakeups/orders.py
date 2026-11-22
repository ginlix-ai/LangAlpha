"""Which orders an order wakeup can watch, and what it records about them.

An order wakeup fires from the reconciler's read of the order, so an order
that read never reaches, or a server that runs no reconciler, would leave the
wakeup waiting out its seven days on nothing: those are refused at set time
with a timer suggested instead.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Literal, Mapping, Optional, Protocol, Tuple

from src.config.env import EGRESS_RELAY_SECRET
from src.config.settings import get_order_reconcile_config
from src.server.database import order_attempts as attempts_db
from src.server.database.wakeups.arm import WakeupSpec
from src.server.services.brokerage_capabilities import OrderAction
from src.server.services.brokerage_orders import BrokerOrder
from src.server.services.brokerage_orders.base import adapter_for
from src.server.services.brokerage_orders.view import order_view
from src.server.services.orders.watch import WatchRefused, watch_order
from src.tools.wakeups.schedule import HORIZON, OrderAsk, Refused, WatchAsk
from src.tools.wakeups.wording import BUILD_TOOL_RESULTS, TEMPLATES

logger = logging.getLogger(__name__)


def readback_on() -> bool:
    """Whether this server reads order status back at all."""
    try:
        return bool(get_order_reconcile_config().enabled) and bool(EGRESS_RELAY_SECRET)
    except Exception:
        logger.warning("[Wakeups] reconcile config unreadable", exc_info=True)
        return False


def _order_of(attempt: Mapping[str, Any]) -> Optional[BrokerOrder]:
    payload = attempt.get("order_json")
    if not isinstance(payload, dict) or not payload:
        return None
    try:
        return BrokerOrder.from_json(payload)
    except Exception:
        return None


class Unfollowable(Exception):
    """No order wakeup can follow this order: nothing will ever read it back.

    A reason, not a sentence, so the tool's refusal and a placement's note
    each word it for where they are read.
    """

    def __init__(self, reason: Literal["readback_off", "staged", "vendor"], vendor: str):
        super().__init__(reason)
        self.reason = reason
        self.vendor = vendor


def follow_spec(
    attempt: Mapping[str, Any],
    *,
    label: Optional[str],
    timezone: Optional[str],
    now: datetime,
) -> WakeupSpec:
    """The wakeup that follows ``attempt``'s order from ``now``. Raises
    `Unfollowable` when the reconciler can never report it.

    The same lookup the reconciler plans its reads with, so the refusal and
    the reads cannot disagree about a vendor.
    """
    vendor = str(attempt.get("vendor") or "").strip()
    if not readback_on():
        raise Unfollowable("readback_off", vendor)
    if vendor.casefold() == "ibkr" and attempt.get("action") == OrderAction.STAGE:
        raise Unfollowable("staged", vendor)
    adapter = adapter_for(vendor)
    query = None
    if adapter is not None and attempt.get("vendor_order_id"):
        try:
            query = adapter.status_query(
                _order_of(attempt),
                vendor_order_id=attempt.get("vendor_order_id"),
                route=attempt.get("route") or {},
            )
        except Exception:
            logger.warning("[Wakeups] status_query raised vendor=%s", vendor, exc_info=True)
            query = None
    if query is None:
        raise Unfollowable("vendor", vendor)
    view = order_view(attempt)
    return WakeupSpec(
        kind="order",
        label=label,
        # Its lapse reads on the clock the order trades on.
        timezone=(view.timezone if view else None) or timezone,
        attempt_id=str(attempt["attempt_id"]),
        vendor_order_id=str(attempt.get("vendor_order_id")),
        baseline_filled_qty=baseline_filled(attempt),
        expires_at=now + HORIZON,
    )


async def order_spec(
    thread_id: str, ask: OrderAsk, now: datetime
) -> Tuple[Dict[str, Any], WakeupSpec]:
    """``(attempt, spec)``: the open order this thread placed or watches under
    the id, and the wakeup that follows it. Raises `Refused`."""
    _refuse_without_readback(ask.order_id)
    attempt = await attempts_db.find_open_thread_order(thread_id, ask.order_id)
    return _followed(attempt, ask, now)


class WatchRoom(Protocol):
    """The slots a call's watches may still take before their reads."""

    async def follows(self, attempt_id: str) -> bool:
        """Whether a pending wakeup already follows this order's row, which
        the reconciler then keeps read."""
        ...

    async def take(self) -> bool: ...

    def give(self) -> None: ...

    def hold(self, attempt_id: str) -> None:
        """This call follows that order, as a pending wakeup would."""
        ...


async def watch_spec(
    thread_id: str,
    user_id: Optional[str],
    ask: WatchAsk,
    now: datetime,
    *,
    room: Optional["WatchRoom"] = None,
) -> Tuple[Dict[str, Any], WakeupSpec]:
    """``(attempt, spec)``: as `order_spec`, or for an order this thread holds
    no row for, the watch recorded for it. Raises `Refused`."""
    _refuse_without_readback(ask.order_id)
    # Order ids are unique within an account, not across them: only a thread
    # row at the named brokerage and account is this order.
    attempt = await attempts_db.find_open_thread_order(
        thread_id,
        ask.order_id,
        brokerage=ask.brokerage.strip(),
        account_ref=ask.account.strip(),
    )
    dormant = (
        attempt is not None
        and attempt.get("action") == OrderAction.WATCH
        and not (room is not None and await room.follows(str(attempt["attempt_id"])))
    )
    if (attempt is None or dormant) and ask.brokerage and ask.account and user_id:
        # A new watch, or one nothing waited on, which stopped being read and
        # may be stale: either is a brokerage read, so room first, since one
        # the cap would refuse anyway is a read for nothing.
        if room is not None and not await room.take():
            raise Refused(TEMPLATES["refused_cap"])
        try:
            attempt = await _watch(thread_id, str(user_id), ask)
        except Refused:
            # Nothing will be armed for it, so the slot is free again.
            if room is not None:
                room.give()
            raise
        if room is not None:
            # The arm reuses one wakeup for the order, so the same order again
            # in this call takes no second read or slot.
            room.hold(str(attempt["attempt_id"]))
    return _followed(attempt, ask, now)


def _refuse_without_readback(order_id: str) -> None:
    """Checked before any lookup: with nothing reading orders back, a watch
    would read the brokerage for a row nothing ever reads again."""
    if not readback_on():
        raise Refused(BUILD_TOOL_RESULTS["order_readback_off"].format(id=order_id))


def _followed(
    attempt: Optional[Dict[str, Any]], ask: OrderAsk | WatchAsk, now: datetime
) -> Tuple[Dict[str, Any], WakeupSpec]:
    if attempt is None:
        raise Refused(TEMPLATES["refused_order_unknown"].format(id=ask.order_id))
    try:
        spec = follow_spec(attempt, label=ask.label, timezone=ask.timezone, now=now)
    except Unfollowable as e:
        raise Refused(_unfollowable_line(e, ask.order_id)) from None
    return attempt, spec


def _unfollowable_line(e: Unfollowable, order_id: str) -> str:
    if e.reason == "readback_off":
        return BUILD_TOOL_RESULTS["order_readback_off"].format(id=order_id)
    if e.reason == "staged":
        return TEMPLATES["refused_order_unreadable"].format(id=order_id)
    return BUILD_TOOL_RESULTS["order_vendor_unreadable"].format(
        id=order_id, vendor=e.vendor or "this brokerage's"
    )


async def _watch(thread_id: str, user_id: str, ask: WatchAsk) -> Dict[str, Any]:
    """The watch recorded for an order placed elsewhere. Raises `Refused`."""
    try:
        return await watch_order(
            user_id=user_id,
            thread_id=thread_id,
            brokerage=ask.brokerage,
            account=ask.account,
            order_id=ask.order_id,
        )
    except WatchRefused as e:
        raise Refused(
            BUILD_TOOL_RESULTS[f"watch_{e.reason}"].format(id=ask.order_id, **e.fields)
        ) from None
    except Exception:
        logger.warning("[Wakeups] watching order %s failed", ask.order_id, exc_info=True)
        raise Refused(BUILD_TOOL_RESULTS["watch_failed"].format(id=ask.order_id)) from None


def baseline_filled(attempt: Mapping[str, Any]) -> float:
    """What had filled at set time, so a fill before the wakeup is not its fire."""
    try:
        return float(attempt.get("filled_qty") or 0)
    except (TypeError, ValueError):
        return 0.0
