"""What a wakeup's notice says: the line it adds to a ``<wakeup>`` block, and
the WakeNotice the reader is shown.

A wake turn's own message and a running turn's steering message carry the
same lines. Each is worded by `wake_line` from the notice alone, built off
the row as it stands when the notice is shown, so a series check reports the
skips counted until then and the line cannot say what the reader is not
shown. The WakeNotice rides the wake turn's query metadata or the steering
entry, enough for the UI to draw the row without reading the wakeup back.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Union

from market_protocol import InstrumentRef

from src.server.database.wakeups.contract import as_datetime, as_float
from src.server.services.brokerage_orders.view import order_view, plain
from src.server.services.market_targets.listing import (
    PCT_FROM_TOOL,
    listing_ref,
    shown_symbol,
)
from src.tools.market_data.currency import fmt_price
from src.tools.market_data.display import price_text
from src.tools.market_data.quote_format import current_price
from src.tools.wakeups.clock import clock, when, zone_name, zone_of
from src.tools.wakeups.describe import price_phrase, quoted_line, schedule_of, venue_zone
from src.tools.wakeups.wording import BUILD_WAKE_LINES, WAKE_LABEL_PREFIX, WAKE_LINES

logger = logging.getLogger(__name__)

_LOG = "[Wakeups]"

# A lapse reminder reads better with the price, but it is worth more on time.
_QUOTE_TIMEOUT = 5.0

_OUTCOME_LINE = {
    "filled": "order_filled",
    "partial": "order_partial",
    "partial_cancelled": "order_partial_cancelled",
    "cancelled": "order_cancelled",
    "rejected": "order_rejected",
    "failed": "order_failed",
    "partial_expired": "order_partial_expired",
    "expired": "order_expired",
    "confirmed": "order_confirmed",
    "handed_off": "order_handed_off",
}

Quote = Union[float, Mapping[str, Any], None]

_REASON_CHARS = 200


def _iso(at: Optional[datetime]) -> Optional[str]:
    return at.isoformat() if at else None


@dataclass(frozen=True)
class PriceNotice:
    symbol: Optional[str]
    kind: Optional[str]
    value: Optional[float]
    pct_from: Optional[str]  # the tool's vocabulary, which the thread API speaks
    armed_price: Optional[float]
    fired_price: Optional[float]
    expires_at: Optional[datetime]
    #: The listing, which prices and venue times read on. Not sent.
    ref: Optional[InstrumentRef] = field(default=None, compare=False)

    def to_json(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "kind": self.kind,
            "value": self.value,
            "pct_from": self.pct_from,
            "armed_price": self.armed_price,
            "fired_price": self.fired_price,
            "expires_at": _iso(self.expires_at),
        }


@dataclass(frozen=True)
class OrderNotice:
    vendor_order_id: Optional[str]
    #: With the mode, since a fill is where money is at stake in the sentence.
    summary: Optional[str]
    outcome: Optional[str]
    filled_qty: Optional[float]
    qty: Optional[float]
    avg_fill_price: Optional[float]
    currency: Optional[str]
    reason: Optional[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "vendor_order_id": self.vendor_order_id,
            "summary": self.summary,
            "outcome": self.outcome,
            "filled_qty": self.filled_qty,
            "qty": self.qty,
            "avg_fill_price": self.avg_fill_price,
            "currency": self.currency,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class WakeNotice:
    wakeup_id: str
    short_id: Optional[str]
    label: Optional[str]
    kind: Optional[str]
    notice: str
    seq: int
    total: int
    last: bool
    skipped: int
    fired_at: Optional[datetime]
    timezone: Optional[str]
    cron: Optional[str]
    price: Optional[PriceNotice]
    order: Optional[OrderNotice]
    #: When it was set, and a series' next slot; the line reads both. Not sent.
    set_at: Optional[datetime] = None
    next_at: Optional[datetime] = None

    def to_json(self) -> dict[str, Any]:
        return {
            "wakeup_id": self.wakeup_id,
            "short_id": self.short_id,
            "label": self.label,
            "kind": self.kind,
            "notice": self.notice,
            "seq": self.seq,
            "total": self.total,
            "last": self.last,
            "skipped": self.skipped,
            "fired_at": _iso(self.fired_at),
            "timezone": self.timezone,
            "cron": self.cron,
            "price": self.price.to_json() if self.price else None,
            "order": self.order.to_json() if self.order else None,
        }


def _fired_at(row: Mapping[str, Any], notice: str, seq: int) -> Optional[datetime]:
    """When the notice's event happened: the slot it was scheduled for, since
    a late sweep fires a slot after its time and the turn's own clock row
    says when it arrived; the trade for a price; the observation for an
    order; the claim for a lapse reminder."""
    kind = row.get("kind")
    if kind in ("at", "cron"):
        schedule = row.get("schedule") or []
        if 0 < seq <= len(schedule):
            return as_datetime(schedule[seq - 1])
        return as_datetime(row.get("last_fired_at"))
    if notice == "lapse":
        return as_datetime(row.get("warned_at"))
    return as_datetime(row.get("last_fired_at"))


def notice_payload(row: Mapping[str, Any], notice: str, seq: int) -> WakeNotice:
    """One notice as the reader is shown it: a wake turn's
    ``metadata.wakeup.notices[]`` and a running turn's ``steering_delivered``
    entry, sent as its `WakeNotice.to_json`. An order's row carries the
    order it watches as ``attempt``, as every read of the table returns it."""
    kind = row.get("kind")
    timed = kind in ("at", "cron")
    total = len(row.get("schedule") or []) if timed else 1
    # The slots consumed after this fire while it waited to reach the agent,
    # all skipped but a series' last, which fires behind it and carries none.
    skipped = 0
    if kind == "cron" and notice == "fire":
        later_fire = int(row.get("last_fired_seq") or 0) > seq
        skipped = max(0, int(row.get("fires_done") or 0) - seq - (1 if later_fire else 0))
    zone = row.get("timezone")
    price = order = None
    if kind == "price":
        price = PriceNotice(
            symbol=shown_symbol(row),
            kind=row.get("price_kind"),
            value=as_float(row.get("price_value")),
            pct_from=PCT_FROM_TOOL.get(row.get("pct_from") or ""),
            armed_price=as_float(row.get("armed_price")),
            fired_price=as_float(row.get("fired_price")),
            expires_at=as_datetime(row.get("expires_at")),
            ref=listing_ref(row),
        )
    elif kind == "order":
        view = order_view(row.get("attempt"))
        # A lapse reads on the clock the order trades on.
        zone = zone or (view.timezone if view else None)
        outcome = row.get("order_outcome") or {}
        # The brokerage's own words, of any length and content.
        reason = outcome.get("reason")
        order = OrderNotice(
            vendor_order_id=row.get("vendor_order_id"),
            summary=(view.summary_live or view.summary) if view else None,
            outcome=outcome.get("outcome"),
            filled_qty=as_float(outcome.get("filled_qty")),
            qty=as_float(outcome.get("qty")),
            avg_fill_price=as_float(outcome.get("avg_fill_price")),
            currency=view.currency if view else None,
            reason=quoted_line(reason, _REASON_CHARS) or None if isinstance(reason, str) else None,
        )
    schedule = schedule_of(row)
    # The next check is past the slots skipped while this one waited, or it
    # would name a time already gone.
    upcoming = seq + skipped
    return WakeNotice(
        wakeup_id=str(row.get("wakeup_id")),
        short_id=row.get("short_id"),
        label=row.get("label"),
        kind=kind,
        notice=notice,
        seq=seq,
        total=total,
        last=(seq >= total) if timed else notice == "fire",
        skipped=skipped,
        fired_at=_fired_at(row, notice, seq),
        timezone=zone,
        cron=row.get("cron"),
        price=price,
        order=order,
        set_at=as_datetime(row.get("created_at")),
        next_at=schedule[upcoming] if upcoming < len(schedule) else None,
    )


def _money(value: Optional[float], currency: Optional[str]) -> str:
    return fmt_price(value, currency or "USD", group=True) if value is not None else "N/A"


def wake_line(
    notice: WakeNotice, *, last_price: Optional[float] = None, user_tz: str = "UTC"
) -> str:
    """The one line a notice adds to a ``<wakeup>`` block.

    Times print on the wakeup's own clock (the venue's, for a price), so a
    user elsewhere reads the same hour the agent wrote the schedule in.
    """
    templates = {**WAKE_LINES, **BUILD_WAKE_LINES}
    label = notice.label
    prefix = WAKE_LABEL_PREFIX.format(label=label) if label else ""
    created = notice.set_at
    now = datetime.now(timezone.utc)

    if notice.kind in ("at", "cron"):
        zone = zone_of(notice.timezone, user_tz)
        fired_at = notice.fired_at or now
        fire_time = clock(fired_at, zone, fired_at)
        set_time = clock(created, zone, fired_at) if created else fire_time
        if notice.kind == "at":
            return prefix + templates["time"].format(fire_time=fire_time, set_time=set_time)
        fields = {
            "n": notice.seq,
            "total": notice.total,
            "label_paren": f" ({label})" if label else "",
            "set_time": set_time,
            "cron": notice.cron,
            "timezone": zone_name(zone),
        }
        if notice.last:
            line = templates["series_last"].format(**fields)
        else:
            nxt = notice.next_at
            next_time = clock(nxt, zone, fired_at) if nxt else "an unknown time"
            line = templates["series"].format(next_time=next_time, **fields)
        if notice.skipped > 0:
            key = "series_skipped_one" if notice.skipped == 1 else "series_skipped"
            line += templates[key].format(skipped=notice.skipped)
        return line

    if notice.price is not None:
        price = notice.price
        ref = price.ref
        armed = price.armed_price if price.pct_from == "now" else None
        fields = {
            "symbol": price.symbol,
            "phrase": price_phrase(price.kind, price.value, armed, ref),
        }
        if notice.notice == "lapse":
            expires = price.expires_at
            anchor = notice.fired_at  # the claim of the reminder
            zone = venue_zone(ref, expires or anchor or now)
            if expires is None:
                expires_at = "soon"
            else:
                expires_at = clock(expires, zone, anchor) if anchor else when(expires, zone)
            if last_price is None:
                line = templates["price_expiring_unpriced"].format(
                    expires_at=expires_at, **fields
                )
            else:
                line = templates["price_expiring"].format(
                    expires_at=expires_at, last=price_text(last_price, ref), **fields
                )
            return prefix + line
        fired_at = notice.fired_at or now
        zone = venue_zone(ref, fired_at)
        fire_time = clock(fired_at, zone, fired_at)
        return prefix + templates["price_hit"].format(
            price=price_text(price.fired_price, ref),
            fire_time=fire_time,
            set_time=clock(created, zone, fired_at) if created else fire_time,
            **fields,
        )

    order = notice.order
    assert order is not None  # every order row builds one
    zone = zone_of(notice.timezone, user_tz)
    fired_at = notice.fired_at or now
    # A fill says how much filled: an order for an amount of money has no
    # quantity of its own to show.
    qty = order.filled_qty if order.outcome == "filled" and order.filled_qty else order.qty
    return prefix + templates[_OUTCOME_LINE.get(order.outcome or "", "order_filled")].format(
        order=order.summary or f"order {order.vendor_order_id}",
        order_id=order.vendor_order_id,
        qty=plain(qty) if qty is not None else "?",
        filled=plain(order.filled_qty) if order.filled_qty is not None else "?",
        avg=_money(order.avg_fill_price, order.currency),
        reason=order.reason or "no reason given",
        fire_time=clock(fired_at, zone, fired_at),
    )


def quote_price(quote: Quote) -> Optional[float]:
    """The latest price in ``quote``, a number or a quote row."""
    if isinstance(quote, (int, float)):
        return float(quote)
    if isinstance(quote, Mapping):
        try:
            return current_price(dict(quote))
        except Exception:
            return None
    return None


async def last_quote(
    listing: Mapping[str, Any], user_id: str, *, timeout: float
) -> Optional[dict]:
    """The latest quote for a stored row's listing (a target, a price
    wakeup), if it comes within ``timeout``; a lapse notice is worth more on
    time than with a price."""
    from src.server.services.cache.quote_cache_service import QuoteCacheService

    ref = listing_ref(listing)
    if ref is None:
        return None
    try:
        rows = await asyncio.wait_for(
            QuoteCacheService.get_instance().get_quotes([ref], user_id),
            timeout=timeout,
        )
    except Exception:
        logger.debug(f"{_LOG} quote for {listing.get('symbol')} unavailable", exc_info=True)
        return None
    return rows[0][1] if rows else None


async def user_timezone(user_id: str) -> str:
    from src.server.database.automation import get_user_timezone

    try:
        return await get_user_timezone(user_id) or "UTC"
    except Exception:
        logger.debug(f"{_LOG} user timezone read failed", exc_info=True)
        return "UTC"


async def render_notice(
    row: Mapping[str, Any],
    notice: str,
    seq: int,
    *,
    user_tz: Optional[str] = None,
    quote_timeout: float = _QUOTE_TIMEOUT,
) -> tuple[str, dict[str, Any]]:
    """The notice's line and its WakeNotice. ``user_tz`` saves a read when
    the caller renders several of one user's notices."""
    user_id = str(row.get("user_id") or "")
    if user_tz is None:
        user_tz = await user_timezone(user_id)
    wake = notice_payload(row, notice, seq)
    last_price = None
    if wake.price is not None and notice == "lapse":
        try:
            quote = await asyncio.wait_for(
                last_quote(row, user_id, timeout=quote_timeout),
                timeout=quote_timeout + 1.0,
            )
            last_price = quote_price(quote)
        except Exception:
            logger.debug(f"{_LOG} last price unavailable", exc_info=True)
    return wake_line(wake, last_price=last_price, user_tz=user_tz), wake.to_json()
