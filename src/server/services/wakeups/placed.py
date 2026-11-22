"""What a placed order's tool result ends with: the state the ledger recorded,
and the order wakeup that placing it set.

An order the market has not filled yet is unfinished work, and the agent that
sent it hears nothing more unless it thinks to ask. So placing one sets the
wakeup the agent could have set itself, and the reply says so, which leaves
the agent nothing to remember: it hears how the order ends, or reads why no
wakeup can tell it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

from src.server.database import order_attempts as attempts_db
from src.server.database.wakeups import arm as arm_db
from src.server.database.wakeups.contract import as_datetime
from src.server.services.brokerage_capabilities import OrderAction
from src.server.services.brokerage_orders import AttemptStatus
from src.server.services.brokerage_orders.models import outcome_key, partly_filled
from src.server.services.brokerage_orders.view import order_price, order_view, plain
from src.server.services.features import user_feature_enabled_or
from src.server.services.wakeups.message import user_timezone
from src.tools.wakeups.clock import when, zone_of
from src.tools.wakeups.orders import Unfollowable, follow_spec
from src.tools.wakeups.schedule import MAX_PENDING
from src.tools.wakeups.wording import PLACED_AT_AVG, PLACED_LINES, PLACED_WATCH

logger = logging.getLogger(__name__)

# The states an order wakeup is set for: the order may still fill, and its id
# is the one the reconciler reads it back by. A confirmation the brokerage
# still waits on is not working yet, so nothing can fill it.
_WATCHED = (
    AttemptStatus.SUBMITTED,
    AttemptStatus.WORKING,
    AttemptStatus.PARTIALLY_FILLED,
    AttemptStatus.UNKNOWN,
)

# The ends a placement's answer is worded for, by the outcome a wakeup's fire
# would read; an answer in any other state ends with no line.
_ENDS = (
    AttemptStatus.FILLED,
    AttemptStatus.CANCELLED,
    AttemptStatus.REJECTED_BY_VENDOR,
    AttemptStatus.FAILED,
)
# The other open states. A fire reads every open order as partly filled, since
# it fires on a fill; a placement's answer says how far the order got instead.
_OPEN_KEYS = {
    AttemptStatus.PARTIALLY_FILLED: "partial",
    AttemptStatus.PENDING_CONFIRM: "confirm",
    AttemptStatus.UNKNOWN: "unknown",
}


async def placed_note(
    attempt: Mapping[str, Any],
    *,
    user_id: str,
    thread_id: Optional[str],
    now: Optional[datetime] = None,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """``(note, wakeup)`` for ``attempt``, an ``order_attempts`` row the
    vendor's answer just settled: the lines its tool result ends with, and the
    order wakeup set for it. ``(None, None)`` for anything but a placement,
    or a confirmation that released one."""
    if attempt.get("action") == OrderAction.CONFIRM:
        attempt = await _released(attempt)
        if attempt is None:
            return None, None
    elif attempt.get("action") != OrderAction.PLACE:
        return None, None
    status = attempt.get("status")
    state = _state_line(attempt, status)
    if state is None:
        return None, None
    order_id = attempt.get("vendor_order_id")
    if status not in _WATCHED or not order_id or not thread_id:
        return state, None
    # Fails closed: a wakeup the user turned off is a turn they did not ask for.
    if not await user_feature_enabled_or(str(user_id), "wakeups", on_error=False):
        # No schedule_wakeup either, so there is nothing to point it at.
        return state, None
    if not await user_feature_enabled_or(str(user_id), "order_wakeups", on_error=False):
        return f"{state} {PLACED_WATCH['auto_off']}", None
    watch, row = await _watch(attempt, user_id, thread_id, now or datetime.now(timezone.utc))
    return f"{state} {watch}", row


async def _released(confirm: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """The placement a confirmation released, as this answer leaves it.

    The order is the placement's: the ledger hands the released order's id up
    to it and the reconciler follows that row, so its wakeup is armed there,
    with the state this confirmation's answer reported.
    """
    parent_id = confirm.get("parent_attempt_id")
    if not parent_id or not confirm.get("vendor_order_id"):
        return None
    root = await attempts_db.get_attempt(str(parent_id))
    if (
        root is None
        or root.get("action") != OrderAction.PLACE
        or str(root.get("user_id")) != str(confirm.get("user_id"))
    ):
        return None
    answer = {
        key: confirm.get(key)
        for key in ("status", "vendor_order_id", "filled_qty", "avg_fill_price", "failure")
    }
    return {**root, **answer}


def _state_line(attempt: Mapping[str, Any], status: Any) -> Optional[str]:
    view = order_view(attempt)
    order_qty = view.order.qty if view else None
    filled_qty = attempt.get("filled_qty")
    avg = attempt.get("avg_fill_price")
    fields = {
        "order_id": attempt.get("vendor_order_id") or "",
        "qty": plain(order_qty) if order_qty is not None else "?",
        "filled": plain(filled_qty) if filled_qty is not None else "?",
        "at_avg": (
            PLACED_AT_AVG.format(avg=order_price(view.order if view else None, avg))
            if avg is not None
            else ""
        ),
        "reason": ((attempt.get("failure") or {}).get("message") or "no reason given"),
    }
    if status in _ENDS:
        key = outcome_key(status, filled_qty)
    elif status in (AttemptStatus.SUBMITTED, AttemptStatus.WORKING):
        key = "partial" if partly_filled(filled_qty) else "working"
    elif status in _OPEN_KEYS:
        key = _OPEN_KEYS[status]
    else:
        return None
    if key == "filled" and filled_qty is None and order_qty is not None:
        # An answer that names no fill size filled the whole order.
        fields["filled"] = plain(order_qty)
    return PLACED_LINES[key].format(**fields)


async def _watch(
    attempt: Mapping[str, Any], user_id: str, thread_id: str, now: datetime
) -> Tuple[str, Optional[Dict[str, Any]]]:
    user_tz = await user_timezone(user_id)
    try:
        spec = follow_spec(attempt, label=None, timezone=user_tz, now=now)
        [row] = await arm_db.arm_wakeups(thread_id, user_id, [spec], cap=MAX_PENDING)
    except Unfollowable as e:
        if e.reason == "readback_off":
            return PLACED_WATCH["readback_off"], None
        return PLACED_WATCH["unreadable"].format(vendor=e.vendor or "this brokerage's"), None
    except Exception:
        logger.warning(
            "[Wakeups] arming the order wakeup for attempt %s failed",
            attempt.get("attempt_id"),
            exc_info=True,
        )
        return PLACED_WATCH["failed"], None
    if row is None:
        return PLACED_WATCH["full"], None
    lapses = when(as_datetime(row["expires_at"]), zone_of(row.get("timezone"), user_tz))
    return PLACED_WATCH["set"].format(short_id=row["short_id"], lapses=lapses), row
