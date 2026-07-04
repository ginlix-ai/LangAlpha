"""Shared envelope helpers for OHLCV cache services (daily + intraday).

Provides the envelope structure, parsing, delta-merge, and SWR staleness
check used by both DailyCacheService and IntradayCacheService.
"""

import time
from bisect import bisect_left
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.config.core import get_infrastructure_config
from src.data_client.market_data_provider import is_us_symbol
from src.utils.market_hours import (
    current_trading_date,
    expected_latest_bar_ms,
    expected_latest_daily_date,
    interval_seconds,
    is_market_closed,
)

_ET = ZoneInfo("America/New_York")

ENVELOPE_VERSION = 3  # v3: adds data_date and truncated fields
_SOFT_TTL_RATIO: float = get_infrastructure_config().redis.swr.soft_ttl_ratio
_TRUNCATED_TTL_RATIO = 0.25  # aggressive refresh for truncated data
_EMPTY_RESULT_TTL = 30  # short TTL for empty upstream results
# Floor between consecutive staleness-driven daily re-fetches. When the
# provider itself is behind (e.g. today's daily bar not yet published right
# after the open), re-asking immediately can't yield newer data — without
# this floor every request would bypass the cache with a blocking fetch.
_DAILY_STALE_REFETCH_COOLDOWN = 120

# Bare index symbols whose daily bars follow the US (ET) trading calendar.
# Index symbols aren't suffix-classifiable like stocks, so the daily
# watermark backstop only trusts this allowlist.
_US_INDEX_SYMBOLS = {
    "GSPC", "SPX", "DJI", "IXIC", "COMP", "NDX", "RUT", "VIX",
    "NYA", "XAX", "OEX", "MID", "SML", "SOX", "RUI", "RUA",
    "DJT", "DJU", "W5000", "WLSH",
}

# Dotted suffixes that mark a US class share / security type (BRK.B, BF.B,
# HEI.A). ``symbol_market`` maps these to "other" — not a foreign region — so
# ``is_us_symbol`` returns False and they'd wrongly skip the US backstop.
# Genuine foreign tickers resolve to a specific region (hk/uk/jp/...) via the
# suffix map and never land in this set, so trusting these is safe.
_US_DOTTED_SUFFIXES = {"A", "B", "C"}


def _follows_us_daily_calendar(symbol: str, is_index: bool) -> bool:
    """True if *symbol*'s daily bars anchor to the US (ET) trading calendar."""
    if is_index:
        bare = symbol.lstrip("^").upper()
        if bare.startswith("I:"):
            bare = bare[2:]
        return bare in _US_INDEX_SYMBOLS
    if is_us_symbol(symbol):
        return True
    # US dotted class-shares classify as "other" (unrecognized suffix), not a
    # foreign region — treat the known US class suffixes as US-calendar.
    if "." in symbol:
        return symbol.rsplit(".", 1)[-1].upper() in _US_DOTTED_SUFFIXES
    return False


def _build_envelope(
    bars: List[Dict[str, Any]],
    market_phase: str,
    complete: bool,
    stored_ttl: int = 0,
    truncated: bool = False,
    data_date: Optional[str] = None,
) -> Dict[str, Any]:
    watermark = bars[-1].get("time", 0) if bars else 0
    return {
        "v": ENVELOPE_VERSION,
        "bars": bars,
        "watermark": watermark,
        "fetched_at": time.time(),
        "market_phase": market_phase,
        "complete": complete,
        "stored_ttl": stored_ttl,
        "data_date": data_date or current_trading_date(),
        "truncated": truncated,
    }


def _parse_envelope(raw: Any) -> Optional[Dict[str, Any]]:
    """Return the envelope dict if valid, else None (treat as cache miss)."""
    if not isinstance(raw, dict):
        return None
    if raw.get("v") != ENVELOPE_VERSION:
        return None
    if "bars" not in raw:
        return None
    return raw


def _merge_bars(
    existing: List[Dict[str, Any]],
    delta: List[Dict[str, Any]],
    watermark,
) -> List[Dict[str, Any]]:
    """Merge delta bars into existing, keeping the immutable prefix intact.

    Everything before the watermark is immutable history.
    Delta replaces everything from the watermark onward.
    Delta may start earlier than the watermark (when from_date is a date
    string rather than a precise timestamp), so we filter it first.

    Gap fill: when the delta contains bars that predate the existing prefix
    (e.g. the initial load returned only recent bars), those earlier bars
    are prepended so the gap is filled on the next refresh.
    """
    if not existing:
        return delta
    if not delta:
        return existing

    # Find split point via bisect on the "time" field (Unix ms)
    times = [b.get("time", 0) for b in existing]
    split_idx = bisect_left(times, watermark)

    # Filter delta to only bars at or after the watermark so we don't
    # re-introduce bars that are already in the immutable prefix.
    fresh = [b for b in delta if b.get("time", 0) >= watermark]

    # Gap fill: delta bars that predate existing (partial initial load).
    first_existing_time = times[0] if times else 0
    gap_fill = [b for b in delta if 0 < b.get("time", 0) < first_existing_time]

    if not fresh and not gap_fill:
        return existing

    return gap_fill + existing[:split_idx] + fresh


def watermark_to_date_str(watermark) -> Optional[str]:
    """Convert a watermark (Unix ms) to an ET date string (YYYY-MM-DD)."""
    if not watermark or not isinstance(watermark, (int, float)) or watermark <= 0:
        return None
    dt_et = datetime.fromtimestamp(watermark / 1000, tz=timezone.utc).astimezone(_ET)
    return dt_et.strftime("%Y-%m-%d")


def _is_stale_date(envelope: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    """Return True if the envelope's ``data_date`` is behind the current trading date.

    ``current_trading_date()`` is correct in every market phase, including
    weekends and holidays, so no phase gate is needed.
    """
    data_date = envelope.get("data_date")
    if not data_date:
        return True  # missing data_date — treat as stale
    return data_date != current_trading_date(now)


def is_watermark_stale(
    envelope: Dict[str, Any],
    interval: str,
    now: Optional[datetime] = None,
    symbol: Optional[str] = None,
    is_index: bool = False,
) -> bool:
    """Return True if the envelope's watermark is meaningfully behind the
    most recent bar that *should* exist right now for this interval.

    Catches mid-session stagnation — when ``data_date`` still matches the
    current trading date but the watermark hasn't advanced because a prior
    delta-refresh failed or returned an empty / truncated response. Also
    catches the overnight case where the cache's last bar is from several
    trading days ago even though the date-level check alone might miss it
    (e.g. a ``data_date`` that got refreshed without the bars advancing).

    Daily (``1day``) is checked at the DATE level: the newest bar's ET trading
    date vs ``expected_latest_daily_date()`` (the newest bar that should exist
    now — the previous trading day during pre-market, since today's daily bar
    doesn't appear until the session opens). This is the only backstop daily
    has against a ``data_date`` that was silently re-stamped with today's date
    by a prior refresh that fetched nothing new (``_is_stale_date`` alone can't
    catch that). US symbols only: non-US daily bars anchor at exchange-local
    midnight, which converts to the *previous* ET date and would read as
    permanently stale against the US calendar — daily callers must pass
    ``symbol`` (and ``is_index``); without a symbol the daily check is
    skipped and non-US symbols keep the date-level check via
    ``_is_stale_date`` alone.

    Tolerance: ``2 * interval_period``. Absorbs provider delay plus small
    clock skew without hiding real stagnation.
    """
    if interval == "1day":
        # Date-level: stale when the newest bar's trading date is behind the
        # newest daily bar that should exist now. Uses the same watermark→date
        # conversion the daily delta-refresh trusts for its from_date.
        if symbol is None or not _follows_us_daily_calendar(symbol, is_index):
            # Unclassifiable (no symbol) or non-US: fail closed — a foreign
            # anchor read against the ET calendar would look permanently stale.
            return False
        if not envelope.get("bars"):
            return False  # empty window — soft-TTL governs re-fetch timing
        if time.time() - envelope.get("fetched_at", 0) < _DAILY_STALE_REFETCH_COOLDOWN:
            # Just fetched — the provider simply hasn't published a newer bar
            # yet; flagging stale again would re-fetch on every request.
            # Deliberately ahead of the corrupt-watermark check: a persistently
            # corrupt feed re-fetches once per cooldown, not per request.
            return False
        last_bar_date = watermark_to_date_str(envelope.get("watermark"))
        if last_bar_date is None:
            return True  # bars present but unusable watermark — corrupt
        return last_bar_date < expected_latest_daily_date(now)
    # Empty envelopes (no bars in requested window) are not meaningfully stale
    # on a watermark basis — there's nothing to be behind. They're deliberately
    # cached with a short _EMPTY_RESULT_TTL to dampen fetch storms for symbols
    # with no data; the soft TTL path handles re-fetch timing.
    if not envelope.get("bars"):
        return False
    watermark_ms = envelope.get("watermark") or 0
    if watermark_ms <= 0:
        # Bars exist but watermark is 0 — envelope is corrupt, treat as stale.
        return True
    expected_ms = expected_latest_bar_ms(interval, now)
    if expected_ms <= 0:
        return False
    tolerance_ms = interval_seconds(interval) * 2 * 1000
    return watermark_ms < expected_ms - tolerance_ms


def _needs_refresh(
    envelope: Dict[str, Any],
    ttl: int,
    interval: Optional[str] = None,
    now: Optional[datetime] = None,
    is_live: bool = True,
    symbol: Optional[str] = None,
    is_index: bool = False,
) -> bool:
    """Determine whether an SWR background refresh should fire.

    Priority order (live-only checks gated by ``is_live``):
    1. (live) Stale date (``data_date`` < current trading date) → always refresh.
    2. (live) Stale watermark (interval-aware) → always refresh — catches the
       case where the date is current but bars haven't advanced for N periods.
    3. (live) Complete + market reopened → refresh (day-boundary transition).
    4. Truncated data → aggressive 25% soft TTL (fires for both live and
       historical so incomplete ranges get retried).
    5. Normal → 50% soft TTL.

    ``is_live=False`` skips the three live-only branches (date/watermark/market-
    phase), since historical envelopes are immutable snapshots. The truncated-
    and soft-TTL branches still fire so truncated historical ranges keep
    getting retried on subsequent hits.

    ``interval`` is optional for backward compatibility, but callers should
    pass it whenever available so the watermark check fires. Daily callers
    must also pass ``symbol`` — without it the daily watermark check is
    skipped (see :func:`is_watermark_stale`).
    """
    if is_live:
        # 1. Stale date — strongest signal
        if _is_stale_date(envelope, now):
            return True

        # 2. Stale watermark — mid-session stagnation
        if interval and is_watermark_stale(envelope, interval, now, symbol=symbol, is_index=is_index):
            return True

        # 3. Complete + market reopened
        if envelope.get("complete"):
            if not is_market_closed(now):
                return True
            return False

    elapsed = time.time() - envelope.get("fetched_at", 0)

    # 4. Truncated data — aggressive refresh (both live and historical)
    if envelope.get("truncated"):
        return elapsed > ttl * _TRUNCATED_TTL_RATIO

    # 5. Normal soft TTL
    return elapsed > ttl * _SOFT_TTL_RATIO
