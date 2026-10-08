"""US equity market hours in the legacy phase vocabulary.

Phases are the strings cache envelopes and the WS feed store: ``"pre"``,
``"open"``, ``"post"``, ``"closed"``. Every answer is read off the protocol's
XNYS calendar, the one the per-instrument clocks also use, so holidays and
early closes are declared in exactly one place.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from market_protocol.calendars import get_calendar
from market_protocol.enums import MarketPhase

ET = ZoneInfo("America/New_York")

# Protocol phase → legacy envelope string (pinned in the regression suite).
LEGACY_PHASE: dict[MarketPhase, str] = {
    MarketPhase.PRE: "pre",
    MarketPhase.REGULAR: "open",
    MarketPhase.LUNCH: "open",
    MarketPhase.POST: "post",
    MarketPhase.CLOSED: "closed",
    MarketPhase.HALTED: "closed",
}


def _at(now: datetime | None) -> datetime:
    return datetime.now(ET) if now is None else now


def current_market_phase(now: datetime | None = None) -> str:
    """The US tape's phase at *now* (tz-aware, default the current moment)."""
    return LEGACY_PHASE[get_calendar("XNYS").phase_at(_at(now))]


def current_trading_date(now: datetime | None = None) -> str:
    """The US trading date as ``YYYY-MM-DD``.

    Today from the 04:00 ET pre-market open on a trading day; before that, and
    on weekends and holidays, the most recent past session.
    """
    return get_calendar("XNYS").latest_trading_date(_at(now)).isoformat()


# Interval period in seconds. Used for "expected-latest-bar" staleness.
# Weekly / monthly intervals are not listed — callers fall back to 60s, which
# is intentionally permissive for staleness but would be wrong for scheduling.
# Covers every protocol OHLCV schema (parity-tested), including 1s, which is
# WS-only and never REST-fetched.
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
