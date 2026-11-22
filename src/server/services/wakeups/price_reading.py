"""What a price wakeup reads off the market: the price, the base a % move is
measured from, and the level it fires at.

Read the way the price monitor reads them (``price_monitor``), so a level the
agent or the user is shown is the level the monitor fires at. The tool that
sets wakeups and the thread's listing both read through here.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, NamedTuple, Optional, Sequence, Tuple

from src.data_client.registry import get_market_data_provider
from src.server.models.automation import MarketType
from src.server.services.price_monitor import (
    Listing,
    _set_after,
    fire_price,
    listing_snapshots,
    measured_base,
    printed_at,
    reference_close,
)

logger = logging.getLogger(__name__)


class PriceReading(NamedTuple):
    """The price last read, and the level the wakeup fires at; either None
    when it cannot be known."""

    last: Optional[float] = None
    level: Optional[float] = None


async def snapshots(
    listings: Sequence[Listing], user_id: Any, *, within: Optional[float] = None
) -> Dict[Listing, Dict[str, Any]]:
    """Rows by the monitor's listing, fetched as it fetches its reference
    prices, so the trigger shown is the one it fires at. {} on any failure,
    or once ``within`` seconds pass, since a level can still be set and
    shown without a price."""
    wanted = list(dict.fromkeys(listings))
    if not wanted:
        return {}
    timer = asyncio.timeout(within)
    try:
        async with timer:
            provider = await get_market_data_provider()
            return await listing_snapshots(provider, wanted, user_id=user_id)
    except Exception:
        if timer.expired():
            logger.warning("[Wakeups] price snapshot fetch took over %ss; read without prices", within)
        else:
            logger.warning("[Wakeups] price snapshot fetch failed", exc_info=True)
        return {}


def last_and_base(
    listing: Optional[Listing],
    snap: Mapping[str, Any],
    now: datetime,
    *,
    armed: bool = False,
) -> Tuple[Optional[float], Optional[float]]:
    """``(last, base)``: the price and the close the monitor measures a % move
    from. A wakeup being set reads ``reference_close``, since a watcher set
    after a delayed closing value printed is never measured on it; an armed
    one reads ``measured_base``, as the monitor does."""
    if not snap or listing is None:
        return None, None
    measure = measured_base if armed else reference_close
    return fire_price(snap), measure(listing, snap, now)


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


def row_listing(row: Mapping[str, Any]) -> Optional[Listing]:
    try:
        return (str(row["symbol"]), MarketType(row.get("market") or "stock"))
    except (KeyError, ValueError):
        return None


def _set_since_print(row: Mapping[str, Any], snap: Mapping[str, Any]) -> bool:
    """Whether the wakeup was set after the print its listing's row holds,
    which the monitor then never measures it on (``_set_after``), so a
    delayed closing value's base is never its own. Read by when it was set,
    as the monitor holds a wakeup: an expiry warning rewrites ``updated_at``."""
    printed = printed_at(dict(snap))
    return printed is not None and _set_after({"created_at": row.get("created_at")}, printed)


async def price_readings(
    rows: Iterable[Mapping[str, Any]], user_id: Any, now: datetime, *, within: Optional[float] = None
) -> Dict[str, PriceReading]:
    """``{wakeup_id: PriceReading}`` for every price row in ``rows``, at the
    price the monitor reads and from the base it measures a set wakeup's move
    from (``measured_base``; ``reference_close`` for one set since the row
    printed), so a distance shown is the one it fires on. A row whose listing
    has no price, or none within ``within`` seconds, reads ``last`` None,
    with the level when it is known without one (a level, or a % from its
    armed price)."""
    priced = [r for r in rows if r.get("kind") == "price"]
    listings = [lst for r in priced if (lst := row_listing(r)) is not None]
    snaps = await snapshots(listings, user_id, within=within)
    out: Dict[str, PriceReading] = {}
    for row in priced:
        listing = row_listing(row)
        snap = (snaps.get(listing) or {}) if listing is not None else {}
        last, base = last_and_base(listing, snap, now, armed=not _set_since_print(row, snap))
        level = trigger_level(
            str(row.get("price_kind")),
            float(row.get("price_value") or 0),
            row.get("pct_from"),
            last,
            base,
            armed_price=row.get("armed_price"),
        )
        out[str(row.get("wakeup_id"))] = PriceReading(last, level)
    return out
