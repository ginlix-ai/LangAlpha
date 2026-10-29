"""Planning a price wakeup: the listing it watches, the price it fires at, and
whether it would fire the moment it is set.

Ported from the market-watch tool's price targets, whose checks and refusals
the price wakeup keeps: the monitor that fires both reads the same listing
and the same reference price, so what the set confirmation shows is what the
monitor fires at.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from market_protocol import MarketPhase

from src.data_client.instrument_clock import clock_for_ref
from src.data_client.registry import get_market_data_provider
from src.server.database.wakeups.arm import WakeupSpec
from src.server.models.automation import MarketType
from src.server.services.market_targets.listing import (
    PCT_FROM_STORED,
    TargetListing,
    target_listing,
)
from src.server.services.market_targets.window import target_window
from src.server.services.price_monitor import (
    Listing,
    _positive,
    listing_snapshots,
    target_hit,
    watched_listing,
)
from src.tools.market_data.quote_format import current_price
from src.tools.wakeups.schedule import PCT_KINDS, PriceAsk, Refused
from src.tools.wakeups.wording import (
    BUILD_TOOL_RESULTS,
    PRICE_ALREADY_MET,
    PRICE_DESC,
    PRICE_DESC_BASES,
    PRICE_REFUSALS,
)
from src.utils.market_watch import clean_symbols

logger = logging.getLogger(__name__)


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


async def snapshots(
    listings: Sequence[Listing], user_id: Any
) -> Dict[Listing, Dict[str, Any]]:
    """Rows by the monitor's listing, fetched as it fetches its reference
    prices, so the trigger shown is the one it fires at. {} on any failure,
    since a level can still be set without a price."""
    wanted = list(dict.fromkeys(listings))
    if not wanted:
        return {}
    try:
        provider = await get_market_data_provider()
        return await listing_snapshots(provider, wanted, user_id=user_id)
    except Exception:
        logger.warning("[Wakeups] price snapshot fetch failed", exc_info=True)
        return {}


def last_and_close(snap: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if not snap:
        return None, None
    return _positive(current_price(snap)), _positive(snap.get("previous_close"))


def trigger_level(
    kind: str,
    value: float,
    pct_from: Optional[str],
    last: Optional[float],
    prev_close: Optional[float],
    armed_price: Optional[float] = None,
) -> Optional[float]:
    """The price a wakeup fires at; ``pct_from`` as stored."""
    if kind in ("above", "below"):
        return value
    if pct_from == "armed":
        base = armed_price if armed_price is not None else last
    else:
        base = prev_close
    if base is None:
        return None
    sign = 1 if kind == "pct_up" else -1
    return base * (1 + sign * value / 100)


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
    plan: PricePlan, last: Optional[float], prev_close: Optional[float], now: datetime
) -> Optional[str]:
    """Why a wakeup would fire on the first price the monitor sees, or None."""
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
    if plan.pct_from != "previous_close" or not prev_close or not _base_is_live(plan, now):
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
    last, prev_close = last_and_close(snap)
    reason = already_met(plan, last, prev_close, now)
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


def row_listing(row: Mapping[str, Any]) -> Optional[Listing]:
    try:
        return (str(row["symbol"]), MarketType(row.get("market") or "stock"))
    except (KeyError, ValueError):
        return None


async def readback(
    rows: List[Mapping[str, Any]], user_id: Any
) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    """``{wakeup_id: (last, level)}`` for the % rows among ``rows``, which
    read back as a distance from the latest price. A level row needs no
    price, so a listing of levels fetches nothing."""
    pct_rows = [r for r in rows if r.get("kind") == "price" and r.get("price_kind") in PCT_KINDS]
    listings = [listing for r in pct_rows if (listing := row_listing(r)) is not None]
    snaps = await snapshots(listings, user_id)
    out: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    for row in pct_rows:
        listing = row_listing(row)
        last, prev_close = last_and_close(snaps.get(listing) or {}) if listing else (None, None)
        level = trigger_level(
            str(row.get("price_kind")),
            float(row.get("price_value") or 0),
            row.get("pct_from"),
            last,
            prev_close,
            armed_price=row.get("armed_price"),
        )
        out[str(row.get("wakeup_id"))] = (last, level)
    return out
