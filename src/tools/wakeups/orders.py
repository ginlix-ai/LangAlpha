"""Which orders an order wakeup can watch, and what it records about them.

An order wakeup fires from the reconciler's read of the order, so an order
that read never reaches, or a server that runs no reconciler, would leave the
wakeup waiting out its seven days on nothing: those are refused at set time
with a timer suggested instead.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Tuple

from src.config.env import EGRESS_RELAY_SECRET
from src.config.settings import get_order_reconcile_config
from src.server.database import order_attempts as attempts_db
from src.server.database.wakeups.arm import WakeupSpec
from src.server.services.brokerage_orders import BrokerOrder
from src.server.services.brokerage_orders.base import adapter_for
from src.server.services.brokerage_orders.view import order_view
from src.tools.wakeups.schedule import HORIZON, OrderAsk, Refused
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


def _unreadable(attempt: Mapping[str, Any], order_id: str) -> Optional[str]:
    """Why the reconciler can never report this order, or None when it can.

    The same lookup the reconciler plans its reads with, so the refusal and
    the reads cannot disagree about a vendor.
    """
    vendor = str(attempt.get("vendor") or "").strip()
    if vendor.casefold() == "ibkr" and attempt.get("action") == "stage":
        return TEMPLATES["refused_order_unreadable"].format(id=order_id)
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
        return BUILD_TOOL_RESULTS["order_vendor_unreadable"].format(
            id=order_id, vendor=vendor or "this brokerage's"
        )
    return None


async def order_spec(
    thread_id: str, ask: OrderAsk, now: datetime
) -> Tuple[Dict[str, Any], WakeupSpec]:
    """``(attempt, spec)``: the open order this thread placed under the id,
    and the wakeup that watches it. Raises `Refused`."""
    order_id = str(ask.order_id).strip()
    if not readback_on():
        raise Refused(BUILD_TOOL_RESULTS["order_readback_off"].format(id=order_id))
    attempt = await attempts_db.find_open_thread_order(thread_id, order_id)
    if attempt is None:
        raise Refused(TEMPLATES["refused_order_unknown"].format(id=order_id))
    refusal = _unreadable(attempt, order_id)
    if refusal is not None:
        raise Refused(refusal)
    view = order_view(attempt)
    spec = WakeupSpec(
        kind="order",
        label=ask.label,
        # Its lapse reads on the clock the order trades on.
        timezone=(view.timezone if view else None) or ask.timezone,
        attempt_id=str(attempt["attempt_id"]),
        vendor_order_id=str(attempt.get("vendor_order_id") or ask.order_id),
        baseline_filled_qty=baseline_filled(attempt),
        expires_at=now + HORIZON,
    )
    return attempt, spec


def baseline_filled(attempt: Mapping[str, Any]) -> float:
    """What had filled at set time, so a fill before the wakeup is not its fire."""
    try:
        return float(attempt.get("filled_qty") or 0)
    except (TypeError, ValueError):
        return 0.0
