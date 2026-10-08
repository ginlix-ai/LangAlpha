"""Closing quotes synthesized from daily bars, for listings no snapshot covers.

Some listings have a daily publisher but no snapshot publisher (Beijing Stock
Exchange: tushare serves ``daily``, no realtime provider covers the venue). A
stale-but-real last close beats an empty header, so the row is built from the
bars cache and stamped ``source="daily"`` / ``tier="eod"`` so a reader can tell
it is not a live print.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from market_protocol import InstrumentRef, to_legacy_api
from market_protocol.calendars import get_calendar
from market_protocol.enums import AssetClass
from src.data_client.instrument_clock import clock_for_ref
from src.data_client.normalize import change_from
from src.server.services.cache.daily_cache_service import DailyCacheService

logger = logging.getLogger(__name__)

# Fallbacks one quote request may run. Each is a daily read that walks the
# whole provider chain on a miss, so a batch of unknown symbols on a weekend
# would otherwise fan out to hundreds of upstream calls.
_MAX_FALLBACKS = 20

# Concurrent daily reads within one request. Per request, so one caller's batch
# never queues behind another's.
_CONCURRENCY = 8


def _print_time_ms(ref: InstrumentRef, bar_ms: int, now: Optional[datetime] = None) -> int:
    """When the daily bar stamped *bar_ms* last printed: its session's close.

    A daily bar is stamped at its session's start (venue midnight), while a
    quote's ``as_of`` is measured against the venue's last close, so the raw
    stamp reads a settled close as hours stale. Capped at now, since a
    forming session's close is still ahead; *bar_ms* stands when the calendar
    names no session for that date.
    """
    try:
        cal = get_calendar(ref.calendar_id)
        day = datetime.fromtimestamp(bar_ms / 1000, tz=timezone.utc).astimezone(cal.tz).date()
        close_ms = cal.session_close_ms(day)
    except Exception:
        return bar_ms
    if close_ms is None:
        return bar_ms
    now_ms = int((now or datetime.now(timezone.utc)).timestamp() * 1000)
    return min(close_ms, now_ms)


async def daily_fallback_snapshot(ref: InstrumentRef, user_id: Optional[str]) -> Optional[dict]:
    """A closing quote from the last two daily bars; None when there are none.

    Reads the window-less live series, the one the chart reads too, and takes
    its tail. A dated window ending today would land on that same live key
    and leave only the window's bars there for every later chart read.
    """
    symbol = to_legacy_api(ref)
    try:
        result = await DailyCacheService.get_instance().get_stock_daily(
            symbol=symbol, is_index=False, user_id=user_id,
        )
    except Exception as exc:  # a fallback never turns a miss into a 500
        logger.info("snapshot.daily_fallback.failed | symbol=%s error=%s", symbol, exc)
        return None
    bars = [b for b in (result.data or []) if b.get("close") is not None] if not result.error else []
    if not bars:
        return None
    last = bars[-1]
    prev_close = bars[-2].get("close") if len(bars) >= 2 else None
    price = float(last["close"])
    change, change_percent = change_from(price, prev_close, ndigits=4)
    as_of = last.get("time")
    if isinstance(as_of, (int, float)) and as_of > 0:
        as_of = _print_time_ms(ref, int(as_of))
    return {
        "symbol": symbol,
        "price": price,
        "change": change,
        "change_percent": change_percent,
        "previous_close": prev_close,
        "open": last.get("open"),
        "high": last.get("high"),
        "low": last.get("low"),
        # Bar volume is a float on the wire (CN lots scale to shares with float
        # error); the snapshot field is an int, and a fractional value would
        # fail validation for the whole batch.
        "volume": round(v) if (v := last.get("volume")) is not None else None,
        "market_status": "closed",
        "source": "daily",
        "tier": "eod",
        "as_of": as_of,
        "regular_only": True,
    }


async def fill_from_daily(
    quotes: List[Tuple[InstrumentRef, Dict[str, Any]]],
    refs: List[InstrumentRef],
    user_id: Optional[str],
) -> List[Tuple[InstrumentRef, Dict[str, Any]]]:
    """Append a daily-bar quote for every requested stock the snapshot chain
    dropped, but only while that instrument's venue is closed.

    A last close is a truthful quote for a closed market; during the session
    it would dress a snapshot outage up as a live price and flag an open
    market as closed, so a US name with no row stays without one until the
    chain recovers. Served rows are never dropped or replaced. Indexes are
    left out: the fallback reads equity daily bars.
    """
    served = {ref.instrument_key for ref, _ in quotes}
    eligible = []
    for ref in refs:
        if ref.instrument_key in served or ref.asset_class is AssetClass.INDEX:
            continue
        try:
            if clock_for_ref(ref).is_closed():
                eligible.append(ref)
        except Exception:
            continue
    if not eligible:
        return quotes
    if len(eligible) > _MAX_FALLBACKS:
        logger.info(
            "snapshot.daily_fallback.capped | eligible=%d cap=%d", len(eligible), _MAX_FALLBACKS,
        )
        eligible = eligible[:_MAX_FALLBACKS]

    gate = asyncio.Semaphore(_CONCURRENCY)

    async def _bounded(ref: InstrumentRef):
        async with gate:
            return ref, await daily_fallback_snapshot(ref, user_id)

    filled = await asyncio.gather(*(_bounded(ref) for ref in eligible))
    return [*quotes, *((ref, row) for ref, row in filled if row)]
