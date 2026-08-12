"""Per-instrument market clock for cache staleness (Phase 3).

`clock_for(symbol, is_index)` resolves an instrument to the time authority
its envelopes should be judged by:

- US-calendar instruments (bare tickers, US indexes, dotted class shares)
  delegate to ``src.utils.market_hours`` — the XNYS facade, byte-identical
  to pre-CMDP behavior.
- Everything else gets its real market calendar from the protocol layer,
  fixing the US-only staleness assumptions (HK envelopes never cache-hit,
  #304's disabled non-US daily backstop).

Fail-closed defaults are preserved: instruments whose daily-bar calendar we
cannot classify (unknown index families, unrecognized suffixes other than
US class shares) keep ``daily_backstop=False`` so they are never flagged
permanently stale against the wrong calendar.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from src.market_protocol import InstrumentRef, to_canonical
from src.market_protocol.calendars import MarketCalendar, get_calendar, session_bounds
from src.market_protocol.enums import AssetClass, MarketPhase
from src.market_protocol.symbology import UNKNOWN_MIC, index_legacy_to_polygon
from src.utils import market_hours

logger = logging.getLogger(__name__)

_ACTIVE_PHASES = (MarketPhase.PRE, MarketPhase.REGULAR, MarketPhase.POST)

# Legacy envelope market_phase strings (pinned in the regression suite).
_LEGACY_PHASE = {
    MarketPhase.PRE: "pre",
    MarketPhase.REGULAR: "open",
    MarketPhase.LUNCH: "open",
    MarketPhase.POST: "post",
    MarketPhase.CLOSED: "closed",
    MarketPhase.HALTED: "closed",
}

# US dotted class-share suffixes (BRK.B, BF.B) — kept from the pre-CMDP
# classifier so these retain the US daily backstop.
_US_CLASS_SUFFIXES = {"A", "B", "C"}


class UsClock:
    """XNYS clock delegating to market_hours — exact pre-CMDP parity."""

    tz = market_hours.ET

    def __init__(self, daily_backstop: bool = True) -> None:
        self.daily_backstop = daily_backstop

    def market_phase(self, now: datetime | None = None) -> str:
        return market_hours.current_market_phase(now)

    def is_closed(self, now: datetime | None = None) -> bool:
        return market_hours.is_market_closed(now)

    def current_trading_date(self, now: datetime | None = None) -> str:
        return market_hours.current_trading_date(now)

    def expected_latest_daily_date(self, now: datetime | None = None) -> str:
        return market_hours.expected_latest_daily_date(now)

    def expected_latest_bar_ms(self, interval: str, now: datetime | None = None) -> int:
        return market_hours.expected_latest_bar_ms(interval, now)

    def seconds_until_next_open(self, now: datetime | None = None) -> int:
        return market_hours.seconds_until_next_open(now)

    def next_phase_change_ms(self, now: datetime | None = None) -> int | None:
        return market_hours.next_phase_change_ms(now)

    def today_market_open_ms(self) -> int | None:
        return market_hours.today_market_open_ms()


class CalendarClock:
    """Clock over a protocol MarketCalendar (non-US venues, crypto, FX)."""

    daily_backstop = True

    def __init__(self, cal: MarketCalendar) -> None:
        self._cal = cal
        self.tz = cal.tz

    @staticmethod
    def _now(now: datetime | None) -> datetime:
        return now or datetime.now(timezone.utc)

    def market_phase(self, now: datetime | None = None) -> str:
        return _LEGACY_PHASE[self._cal.phase_at(self._now(now))]

    def is_closed(self, now: datetime | None = None) -> bool:
        return self.market_phase(now) == "closed"

    def current_trading_date(self, now: datetime | None = None) -> str:
        return self._cal.latest_trading_date(self._now(now)).isoformat()

    def expected_latest_daily_date(self, now: datetime | None = None) -> str:
        return self._cal.expected_latest_daily_date(self._now(now)).isoformat()

    def expected_latest_bar_ms(self, interval: str, now: datetime | None = None) -> int:
        """Most recent intraday bar anchor that should exist right now.

        Active session: floor(now). Lunch break: the break start (no bars
        form during lunch). Closed: the last session close. Daily periods
        skip flooring (UTC-midnight rounding would cross local dates).
        """
        now = self._now(now)
        period = max(1, market_hours.interval_seconds(interval))
        phase = self._cal.phase_at(now)

        if phase in _ACTIVE_PHASES:
            anchor_ms = int(now.timestamp()) * 1000
        elif phase is MarketPhase.LUNCH:
            anchor_ms = self._lunch_anchor_ms(now)
        else:
            d = self._cal.latest_trading_date(now)
            close_ms = self._cal.session_close_ms(d)
            if close_ms is None:
                return 0
            anchor_ms = min(close_ms, int(now.timestamp()) * 1000)

        if period >= 86400:
            return anchor_ms
        epoch_s = anchor_ms // 1000
        return (epoch_s - (epoch_s % period)) * 1000

    def _lunch_anchor_ms(self, now: datetime) -> int:
        local_date = now.astimezone(self.tz).date()
        bounds = session_bounds(self._cal.calendar_id, local_date.isoformat())
        if bounds and bounds[2] is not None:
            return bounds[2]
        return int(now.timestamp()) * 1000

    def seconds_until_next_open(self, now: datetime | None = None) -> int:
        return self._cal.seconds_until_next_open(self._now(now))

    def next_phase_change_ms(self, now: datetime | None = None) -> int | None:
        return self._cal.next_phase_change_ms(self._now(now))

    def today_market_open_ms(self) -> int | None:
        """Today's session open (exchange-local date), None before it opens."""
        now = datetime.now(timezone.utc)
        local_date = now.astimezone(self.tz).date()
        open_ms = self._cal.session_open_ms(local_date)
        if open_ms is None or int(now.timestamp() * 1000) < open_ms:
            return None
        return open_ms


# Bare index spellings whose daily bars follow the US (XNYS) calendar.
# Index symbols aren't suffix-classifiable, so the daily backstop only
# trusts this allowlist (kept from the pre-CMDP _US_INDEX_SYMBOLS set,
# plus the protocol's canonical family spellings).
_US_CALENDAR_INDEXES = frozenset(index_legacy_to_polygon().keys()) | {
    "SPX", "COMP",  # canonical family spellings of GSPC / IXIC
    "NYA", "XAX", "OEX", "MID", "SML", "SOX", "RUI", "RUA",
    "DJT", "DJU", "W5000", "WLSH",
}


def _classify(ref: InstrumentRef, symbol: str, is_index: bool):
    if ref.calendar_id == "XNYS":
        if is_index or ref.asset_class is AssetClass.INDEX:
            bare = symbol.lstrip("^").upper().removeprefix("I:")
            # Unknown index family: fail closed on the daily backstop —
            # its daily anchor may follow a non-US calendar.
            return UsClock(daily_backstop=bare in _US_CALENDAR_INDEXES)
        if ref.mic == UNKNOWN_MIC:
            suffix = symbol.rsplit(".", 1)[-1].upper() if "." in symbol else ""
            return UsClock(daily_backstop=suffix in _US_CLASS_SUFFIXES)
        return UsClock()
    return CalendarClock(get_calendar(ref.calendar_id))


@lru_cache(maxsize=4096)
def _clock_cached(symbol: str, is_index: bool):
    ref = to_canonical(symbol, asset_class=AssetClass.INDEX if is_index else None)
    return _classify(ref, symbol, is_index)


def clock_for(symbol: str | None, is_index: bool = False):
    """Resolve the market clock for *symbol* (None → US parity clock)."""
    if not symbol:
        return UsClock()
    try:
        return _clock_cached(str(symbol), bool(is_index))
    except Exception:
        logger.warning("instrument_clock.fallback_us | symbol=%r", symbol, exc_info=True)
        return UsClock()


def symbol_tz(symbol: str | None, is_index: bool = False) -> ZoneInfo:
    return clock_for(symbol, is_index).tz
