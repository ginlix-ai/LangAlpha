"""US equity market hours and phase detection.

Provides phase classification (pre/open/post/closed) and timing helpers
used by the OHLCV cache to gate background refreshes and set TTL policies.
"""

from __future__ import annotations

import logging
from datetime import datetime, time, date, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# Session boundaries (Eastern Time)
_PRE_OPEN = time(4, 0)       # Pre-market opens
_MARKET_OPEN = time(9, 30)   # Regular session opens
_MARKET_CLOSE = time(16, 0)  # Regular session closes
_POST_CLOSE = time(20, 0)    # Post-market closes

# US market holidays for 2025-2027 (NYSE/NASDAQ observed closures).
# Update annually or replace with an API call.
_HOLIDAYS: set[date] = {
    # 2025
    date(2025, 1, 1),    # New Year's Day
    date(2025, 1, 20),   # MLK Day
    date(2025, 2, 17),   # Presidents' Day
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 26),   # Memorial Day
    date(2025, 6, 19),   # Juneteenth
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas
    # 2026
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 19),   # MLK Day
    date(2026, 2, 16),   # Presidents' Day
    date(2026, 4, 3),    # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 6, 19),   # Juneteenth
    date(2026, 7, 3),    # Independence Day (observed)
    date(2026, 9, 7),    # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
    # 2027
    date(2027, 1, 1),    # New Year's Day
    date(2027, 1, 18),   # MLK Day
    date(2027, 2, 15),   # Presidents' Day
    date(2027, 3, 26),   # Good Friday
    date(2027, 5, 31),   # Memorial Day
    date(2027, 6, 18),   # Juneteenth (observed)
    date(2027, 7, 5),    # Independence Day (observed)
    date(2027, 9, 6),    # Labor Day
    date(2027, 11, 25),  # Thanksgiving
    date(2027, 12, 24),  # Christmas (observed)
}

MarketPhase = str  # "pre" | "open" | "post" | "closed"

_holiday_staleness_warned = False


def _is_trading_day(d: date) -> bool:
    """Return True if *d* is a weekday and not a US market holiday."""
    global _holiday_staleness_warned
    if not _holiday_staleness_warned:
        _holiday_staleness_warned = True
        max_year = max(h.year for h in _HOLIDAYS)
        if date.today().year > max_year:
            logger.warning(
                "market_hours._HOLIDAYS only covers through %d. "
                "Update the holiday set or integrate exchange_calendars.",
                max_year,
            )
    return d.weekday() < 5 and d not in _HOLIDAYS


def current_market_phase(now: datetime | None = None) -> MarketPhase:
    """Classify the current moment into a market phase.

    Args:
        now: Optional override for testability. Must be tz-aware or None.

    Returns:
        One of ``"pre"``, ``"open"``, ``"post"``, or ``"closed"``.
    """
    if now is None:
        now = datetime.now(ET)
    else:
        now = now.astimezone(ET)

    if not _is_trading_day(now.date()):
        return "closed"

    t = now.time()
    if t < _PRE_OPEN:
        return "closed"
    if t < _MARKET_OPEN:
        return "pre"
    if t < _MARKET_CLOSE:
        return "open"
    if t < _POST_CLOSE:
        return "post"
    return "closed"


def is_market_active(now: datetime | None = None) -> bool:
    """Return True during pre-market, regular, or post-market sessions."""
    return current_market_phase(now) != "closed"


def is_market_closed(now: datetime | None = None) -> bool:
    """Return True when the market is fully closed (no session active)."""
    return current_market_phase(now) == "closed"


def current_trading_date(now: datetime | None = None) -> str:
    """Return the current trading date as ``YYYY-MM-DD``.

    Before 04:00 ET on a trading day the trading date is the *previous*
    trading day (pre-market hasn't started).  After 04:00 ET on a trading
    day it's today.  On weekends/holidays it's the most recent past
    trading day.
    """
    if now is None:
        now = datetime.now(ET)
    else:
        now = now.astimezone(ET)

    candidate = now.date()

    # If it's before pre-market open, the current session hasn't started yet
    if now.time() < _PRE_OPEN:
        candidate -= timedelta(days=1)

    # Walk backward to find the most recent trading day
    for _ in range(10):
        if _is_trading_day(candidate):
            return candidate.strftime("%Y-%m-%d")
        candidate -= timedelta(days=1)

    # Fallback — shouldn't happen
    return now.date().strftime("%Y-%m-%d")


def seconds_until_next_open(now: datetime | None = None) -> int:
    """Seconds until the next pre-market open (04:00 ET on a trading day).

    Returns 0 if a session is currently active.
    """
    if now is None:
        now = datetime.now(ET)
    else:
        now = now.astimezone(ET)

    if is_market_active(now):
        return 0

    # Walk forward day by day to find the next trading day
    candidate = now.date()
    t = now.time()

    # If we're before 04:00 on a trading day, the next open is today at 04:00
    if _is_trading_day(candidate) and t < _PRE_OPEN:
        next_open = datetime.combine(candidate, _PRE_OPEN, tzinfo=ET)
        return max(0, int((next_open - now).total_seconds()))

    # Otherwise advance to the next trading day
    candidate += timedelta(days=1)
    # Safety limit: max 10 days (handles long holiday runs)
    for _ in range(10):
        if _is_trading_day(candidate):
            next_open = datetime.combine(candidate, _PRE_OPEN, tzinfo=ET)
            return max(0, int((next_open - now).total_seconds()))
        candidate += timedelta(days=1)

    # Fallback: shouldn't happen but return 12 hours as safe default
    return 43200


def today_market_open_ms() -> int | None:
    """Return today's regular-session market open (9:30 ET) as Unix ms.

    Returns None if today is not a trading day or market hasn't opened yet.
    """
    now = datetime.now(ET)
    if not _is_trading_day(now.date()):
        return None
    if now.time() < _MARKET_OPEN:
        return None
    open_dt = datetime.combine(now.date(), _MARKET_OPEN, tzinfo=ET)
    return int(open_dt.timestamp() * 1000)


# Interval period in seconds. Used for "expected-latest-bar" staleness.
# Weekly / monthly intervals are not listed — callers fall back to 60s, which
# is intentionally permissive for staleness but would be wrong for scheduling.
_INTERVAL_SECONDS: dict[str, int] = {
    "1s": 1,
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1hour": 3600,
    "4hour": 14400,
    "1day": 86400,
}


def interval_seconds(interval: str) -> int:
    """Return the bar period in seconds for a given interval string.

    Unknown intervals fall back to 60s (1-minute) to stay permissive;
    staleness checks against unknown intervals still produce a sane answer.
    """
    return _INTERVAL_SECONDS.get(interval, 60)


def expected_latest_bar_ms(interval: str, now: datetime | None = None) -> int:
    """Return the Unix-ms timestamp of the most recent bar that should exist for this interval.

    Active session: ``floor(now, interval_period)``. Market closed: regular-session
    close (16:00 ET) of the most recent trading day, floored to the interval period.
    Returns 0 if no trading day found in the 10-day lookback.
    """
    if now is None:
        now = datetime.now(ET)
    else:
        now = now.astimezone(ET)

    period = max(1, interval_seconds(interval))

    if is_market_active(now):
        epoch_s = int(now.timestamp())
        floored = epoch_s - (epoch_s % period)
        return floored * 1000

    # Market closed — anchor to the most recent trading-day regular close.
    candidate = now.date()
    if now.time() < _PRE_OPEN:
        candidate -= timedelta(days=1)
    for _ in range(10):
        if _is_trading_day(candidate):
            close_dt = datetime.combine(candidate, _MARKET_CLOSE, tzinfo=ET)
            if close_dt > now:
                # We're on a trading day but before close (e.g. pre-market
                # but is_market_active was False — shouldn't happen). Step
                # back a day to stay safe.
                candidate -= timedelta(days=1)
                continue
            close_ms = int(close_dt.timestamp() * 1000)
            if period >= 86400:
                # Daily: skip floor() — UTC-midnight rounding crosses ET date boundary.
                return close_ms
            epoch_s = close_ms // 1000
            floored = epoch_s - (epoch_s % period)
            return floored * 1000
        candidate -= timedelta(days=1)
    return 0
