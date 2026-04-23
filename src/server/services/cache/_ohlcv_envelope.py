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
from src.utils.market_hours import (
    current_trading_date,
    expected_latest_bar_ms,
    interval_seconds,
    is_market_closed,
)

_ET = ZoneInfo("America/New_York")

ENVELOPE_VERSION = 3  # v3: adds data_date and truncated fields
_SOFT_TTL_RATIO: float = get_infrastructure_config().redis.swr.soft_ttl_ratio
_TRUNCATED_TTL_RATIO = 0.25  # aggressive refresh for truncated data
_EMPTY_RESULT_TTL = 30  # short TTL for empty upstream results


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
) -> bool:
    """Return True if the envelope's watermark is meaningfully behind the
    most recent bar that *should* exist right now for this interval.

    Catches mid-session stagnation — when ``data_date`` still matches the
    current trading date but the watermark hasn't advanced because a prior
    delta-refresh failed or returned an empty / truncated response. Also
    catches the overnight case where the cache's last bar is from several
    trading days ago even though the date-level check alone might miss it
    (e.g. a ``data_date`` that got refreshed without the bars advancing).

    Daily (``1day``) is a no-op — daily staleness is already handled at the
    date level by :func:`_is_stale_date`, and daily bars have provider-specific
    timestamp anchors (00:00 vs 09:30 vs 16:00) that would make a timestamp-
    level check brittle.

    Tolerance: ``2 * interval_period``. Absorbs provider delay plus small
    clock skew without hiding real stagnation.
    """
    if interval == "1day":
        return False
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
    pass it whenever available so the watermark check fires.
    """
    if is_live:
        # 1. Stale date — strongest signal
        if _is_stale_date(envelope, now):
            return True

        # 2. Stale watermark — mid-session stagnation
        if interval and is_watermark_stale(envelope, interval, now):
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
