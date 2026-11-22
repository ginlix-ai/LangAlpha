"""Planning a price wakeup: the listing it watches, the price it fires at, and
whether it would fire the moment it is set.

Ported from the market-watch tool's price targets, whose checks and refusals
the price wakeup keeps: the monitor that fires both reads the same listing
and the same reference price, so what the set confirmation shows is what the
monitor fires at.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Tuple

from market_protocol import MarketPhase

from src.data_client.instrument_clock import clock_for_ref
from src.server.database.wakeups.arm import WakeupSpec
from src.server.services.market_targets.listing import (
    PCT_FROM_STORED,
    TargetListing,
    target_listing,
)
from src.server.services.market_targets.window import target_window
from src.server.services.price_monitor import target_hit, traded_since_close, watched_listing
from src.server.services.wakeups.price_reading import last_and_base, trigger_level
from src.tools.wakeups.schedule import PriceAsk, Refused
from src.tools.wakeups.wording import (
    BUILD_TOOL_RESULTS,
    PRICE_ALREADY_MET,
    PRICE_DESC,
    PRICE_DESC_BASES,
    PRICE_REFUSALS,
)
from src.utils.market_watch import clean_symbols

@dataclass(frozen=True)
class PricePlan:
    """A price entry that passed every check that needs no price."""

    symbol: str  # as the model spelled it, cleaned
    listing: TargetListing
    kind: str
    value: float
    pct_from: Optional[str]  # tool vocabulary
    desc: str


def _num(value: float) -> str:
    return f"{value:,.2f}" if abs(value) >= 1 else f"{value:.4g}"


def _describe(symbol: str, kind: str, value: float, pct_from: Optional[str]) -> str:
    if kind in ("above", "below"):
        return PRICE_DESC[kind].format(symbol=symbol, value=value)
    return PRICE_DESC["pct"].format(
        symbol=symbol,
        direction="up" if kind == "pct_up" else "down",
        value=value,
        base=PRICE_DESC_BASES[pct_from or "previous_close"],
    )


def plan_price(ask: PriceAsk) -> PricePlan:
    """The listing a price entry watches. Raises `Refused`."""
    cleaned = clean_symbols([ask.symbol])
    if not cleaned:
        label = str(ask.symbol or "?").strip()[:20] or "?"
        raise Refused(PRICE_REFUSALS["not_ticker"].format(label=label))
    symbol = cleaned[0]
    desc = _describe(symbol, ask.kind, ask.value, ask.pct_from)
    if not math.isfinite(ask.value) or ask.value <= 0:
        raise Refused(PRICE_REFUSALS["not_positive"].format(desc=desc, kind=ask.kind))
    if ask.kind == "pct_down" and ask.value >= 100:
        raise Refused(PRICE_REFUSALS["fall_too_far"].format(desc=desc))
    listing, reason = target_listing(symbol)
    if listing is None:
        raise Refused(PRICE_REFUSALS["reason"].format(desc=desc, reason=reason))
    return PricePlan(symbol, listing, ask.kind, ask.value, ask.pct_from, desc)


def _base_is_live(plan: PricePlan, now: datetime) -> bool:
    """Whether the snapshot's previous close is the base the monitor is
    measuring from right now. Between sessions it rolls before the next
    print, so a move already made says nothing about the next session."""
    ref = watched_listing(plan.listing.key, plan.listing.market)
    if ref is None:
        return False
    try:
        return clock_for_ref(ref).phase(now) is not MarketPhase.CLOSED
    except Exception:
        return False


def already_met(
    plan: PricePlan,
    last: Optional[float],
    prev_close: Optional[float],
    now: datetime,
    *,
    closed_base: bool = False,
) -> Optional[str]:
    """Why a wakeup would fire on the first price the monitor sees, or None.
    *closed_base* says *prev_close* is the close a closed venue's next
    session measures from, so a move already made counts while it is closed."""
    if last is None:
        return None
    if plan.kind in ("above", "below"):
        if not target_hit(plan.kind, plan.value, 0.0, last):
            return None
        return PRICE_ALREADY_MET["level"].format(
            symbol=plan.symbol,
            where="at" if last == plan.value else plan.kind,
            value=plan.value,
            last=_num(last),
        )
    if plan.pct_from != "previous_close" or not prev_close:
        return None
    if not closed_base and not _base_is_live(plan, now):
        return None
    if not target_hit(plan.kind, plan.value, prev_close, last):
        return None
    move = (last - prev_close) / prev_close * 100
    return PRICE_ALREADY_MET[plan.kind].format(
        symbol=plan.symbol, move=abs(move), last=_num(last)
    )


def price_spec(
    plan: PricePlan, label: Optional[str], snap: Mapping[str, Any], now: datetime
) -> Tuple[WakeupSpec, Optional[float], Optional[float]]:
    """``(spec, last, level)`` for a planned price entry. Raises `Refused`."""
    listing = (plan.listing.key, plan.listing.market)
    last, prev_close = last_and_base(listing, snap, now)
    closed_base = traded_since_close(listing, snap, now)
    reason = already_met(plan, last, prev_close, now, closed_base=closed_base)
    if reason is not None:
        raise Refused(PRICE_REFUSALS["reason"].format(desc=plan.desc, reason=reason))
    if plan.pct_from == "now" and last is None:
        raise Refused(BUILD_TOOL_RESULTS["price_no_live"].format(desc=plan.desc))
    expires_at, warn_at = target_window(plan.listing.key, plan.listing.market, now)
    pct_from = PCT_FROM_STORED.get(plan.pct_from) if plan.pct_from else None
    spec = WakeupSpec(
        kind="price",
        label=label,
        symbol=plan.listing.key,
        market=plan.listing.market.value,
        price_kind=plan.kind,
        price_value=plan.value,
        pct_from=pct_from,
        armed_price=last,
        warn_at=warn_at,
        expires_at=expires_at,
    )
    level = trigger_level(plan.kind, plan.value, pct_from, last, prev_close)
    return spec, last, level
