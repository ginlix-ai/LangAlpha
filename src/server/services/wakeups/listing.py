"""A thread's wakeups in the web's shape, the Status panel's and the chip's.

The state shown is what the user can still do about a wakeup, so it reads
past storage in two places. A price or an order past its window shows closed
before the sweep gets to it, as the monitor and the cancel already treat it.
And a fired wakeup whose notice the agent has not read shows ``held`` (or
``held_usage``, waiting on usage) rather than fired, since the agent has not
acted on it yet and it can still be called back.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from src.server.database import market_targets as targets_db
from src.server.database.automation import get_user_timezone
from src.server.database.wakeups import read as wakeups_read
from src.server.database.wakeups.cancel import cancel_decision, past_window
from src.server.database.wakeups.contract import as_float
from src.server.models.wakeup import (
    WakeupOrderOut,
    WakeupOut,
    WakeupPriceOut,
    WakeupsResponse,
)
from src.server.services.brokerage_orders.view import order_view
from src.server.services.market_targets.listing import PCT_FROM_TOOL, shown_symbol

logger = logging.getLogger(__name__)

# How long an ended wakeup stays in the listing.
ENDED_WINDOW = timedelta(hours=24)


def _state(row: Dict[str, Any], now: datetime) -> str:
    status = row["status"]
    if status == "cancelled":
        return "cancelled"
    if row["last_fired_seq"] > row["delivered_seq"]:
        # Held after its wake was refused, and not since taken by a turn.
        waiting = row.get("notice_waiting") and not row.get("notice_too_late")
        return "held_usage" if waiting else "held"
    if status == "armed" and past_window(row, now):
        status = "expired"
    if status == "expired":
        return "lapsed" if row["kind"] == "price" else "expired"
    return status


def _cancellable(row: Dict[str, Any], now: datetime) -> bool:
    """Whether the user's cancel would still end it or call its notice
    back, as the cancel itself decides."""
    late = bool(row.get("notice_too_late"))
    return cancel_decision(row, late=late, forced=False, now=now) in (
        "cancelled",
        "cancelled_in_flight",
    )


def _order_out(row: Dict[str, Any]) -> WakeupOrderOut:
    attempt = row.get("attempt") or None
    view = order_view(attempt)
    # Once fired, the order as it stood at the fire, which is what the agent
    # was told; before, as it stands.
    if row.get("order_outcome"):
        source = row["order_outcome"]
        qty = source.get("qty")
    else:
        source = attempt or {}
        qty = (source.get("order_json") or {}).get("qty")
    return WakeupOrderOut(
        vendor_order_id=row["vendor_order_id"],
        summary=view.summary if view else row["vendor_order_id"],
        outcome=(row.get("order_outcome") or {}).get("outcome"),
        filled_qty=as_float(source.get("filled_qty")),
        qty=as_float(qty),
        avg_fill_price=as_float(source.get("avg_fill_price")),
        currency=view.currency if view else None,
    )


def wakeup_out(row: Dict[str, Any], now: datetime) -> WakeupOut:
    kind = row["kind"]
    schedule = row.get("schedule") or []
    timed = kind in ("at", "cron")
    return WakeupOut(
        wakeup_id=row["wakeup_id"],
        short_id=row["short_id"],
        label=row.get("label"),
        kind=kind,
        state=_state(row, now),
        timezone=row.get("timezone"),
        cron=row.get("cron"),
        at=schedule[0] if kind == "at" and schedule else None,
        until=row.get("until_at"),
        next_fire_at=row.get("next_fire_at") if row["status"] == "armed" else None,
        fires_done=row["fires_done"],
        fires_total=len(schedule) if timed else 1,
        ends_at=(schedule[-1] if schedule else None) if timed else row.get("expires_at"),
        expires_at=row.get("expires_at"),
        fired_at=row.get("last_fired_at"),
        price=(
            WakeupPriceOut(
                symbol=shown_symbol(row),
                kind=row["price_kind"],
                value=row["price_value"],
                pct_from=PCT_FROM_TOOL.get(row.get("pct_from")),
                armed_price=row.get("armed_price"),
                fired_price=row.get("fired_price"),
            )
            if kind == "price"
            else None
        ),
        order=_order_out(row) if kind == "order" else None,
        cancellable=_cancellable(row, now),
        created_at=row["created_at"],
    )


def legacy_target_out(row: Dict[str, Any], now: datetime) -> WakeupOut:
    """A price target from before wakeups, as a price wakeup: it still fires
    or lapses on its own, and the panel is the only place left to cancel it."""
    status = row["status"]
    if status == "armed" and row["expires_at"] <= now:
        status = "expired"
    return WakeupOut(
        wakeup_id=str(row["target_id"]),
        kind="price",
        state="lapsed" if status == "expired" else status,
        fires_done=1 if status == "fired" else 0,
        fires_total=1,
        ends_at=row["expires_at"],
        expires_at=row["expires_at"],
        fired_at=row.get("fired_at"),
        price=WakeupPriceOut(
            symbol=shown_symbol(row),
            kind=row["kind"],
            value=row["value"],
            pct_from=PCT_FROM_TOOL.get(row.get("pct_from")),
            armed_price=row.get("armed_price"),
            fired_price=row.get("fired_price"),
        ),
        cancellable=status == "armed",
        legacy=True,
        created_at=row["created_at"],
    )


async def thread_wakeups(thread_id: str) -> List[WakeupOut]:
    """Open wakeups in the order they were set, then what ended in the last
    day, newest first, each list with the legacy targets after the new."""
    now = datetime.now(timezone.utc)
    rows = await wakeups_read.list_thread_wakeups(
        thread_id, ended_since=now - ENDED_WINDOW
    )
    try:
        legacy = await targets_db.list_thread_targets(
            thread_id, recent_seconds=int(ENDED_WINDOW.total_seconds())
        )
    except Exception:
        # Old targets drain within a day and a half; losing them for one
        # read must not take the wakeups down with them.
        logger.warning(
            "[Wakeups] listing legacy targets failed thread_id=%s", thread_id, exc_info=True
        )
        legacy = []
    new_out = [wakeup_out(r, now) for r in rows]
    old_out = sorted(
        (legacy_target_out(r, now) for r in legacy), key=lambda w: w.created_at
    )
    old_open = [w for w in old_out if w.cancellable]
    old_ended = [w for w in reversed(old_out) if not w.cancellable]
    open_new = [w for w, r in zip(new_out, rows) if r.get("is_open")]
    ended_new = [w for w, r in zip(new_out, rows) if not r.get("is_open")]
    return open_new + old_open + ended_new + old_ended


async def thread_wakeups_response(thread_id: str, user_id: str) -> WakeupsResponse:
    try:
        user_tz = await get_user_timezone(user_id)
    except Exception:
        logger.debug("[Wakeups] user timezone read failed", exc_info=True)
        user_tz = None
    return WakeupsResponse(
        thread_id=thread_id,
        user_timezone=user_tz,
        wakeups=await thread_wakeups(thread_id),
    )
