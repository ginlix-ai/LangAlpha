"""Per-instrument market clock for cache staleness (Phase 3).

`clock_for(symbol, is_index)` resolves an instrument to the protocol calendar
its envelopes should be judged by: XNYS for US-calendar instruments (bare
tickers, US indexes, dotted class shares), the real venue calendar for
everything else, which is what fixed the US-only staleness assumptions (HK
envelopes never cache-hit, #304's disabled non-US daily backstop).

Fail-closed defaults are preserved: instruments whose daily-bar calendar we
cannot classify (unknown index families, unrecognized suffixes other than
US class shares) keep ``daily_backstop=False`` so they are never flagged
permanently stale against the wrong calendar.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Protocol
from zoneinfo import ZoneInfo

from market_protocol import InstrumentRef, index_families, to_canonical
from market_protocol.calendars import (
    NEXT_OPEN_FALLBACK_S,
    MarketCalendar,
    get_calendar,
    session_bounds,
)
from market_protocol.enums import AssetClass, MarketPhase
from market_protocol.symbology import UNKNOWN_MIC
from src.data_client.normalize import _US_CLASS_SUFFIXES, is_us_class_share  # noqa: F401 (re-export)
from src.utils.market_hours import LEGACY_PHASE, interval_seconds

logger = logging.getLogger(__name__)

_ACTIVE_PHASES = (MarketPhase.PRE, MarketPhase.REGULAR, MarketPhase.POST)
_EXTENDED_PHASES = (MarketPhase.PRE, MarketPhase.POST)

# Calendar edges a regular-only clock may step over before its own phase
# changes. A day contributes at most two (pre-market open, after-hours close),
# and the calendar walks holidays itself, so this is slack, not a horizon.
_MAX_EDGE_SKIPS = 8


class InstrumentClock(Protocol):
    """The time authority ``clock_for`` resolves; phases use the legacy strings."""

    tz: ZoneInfo
    daily_backstop: bool

    def phase(self, now: datetime) -> MarketPhase: ...

    def market_phase(self, now: datetime | None = None) -> str: ...

    def is_closed(self, now: datetime | None = None) -> bool: ...

    def is_trading_day(self, day: date) -> bool: ...

    def current_trading_date(self, now: datetime | None = None) -> str: ...

    def expected_latest_daily_date(self, now: datetime | None = None) -> str: ...

    def expected_latest_bar_ms(self, interval: str, now: datetime | None = None) -> int: ...

    def seconds_until_next_open(self, now: datetime | None = None) -> int: ...

    def seconds_until_next_session_open(self, now: datetime | None = None) -> int:
        """Seconds until the regular open that follows the CURRENT session (never 0).

        ``seconds_until_next_open`` answers 0 while a session is active; a
        once-per-trading-day lock needs the next day's open instead, or it
        collapses to its floor for the whole session.
        """
        ...

    def next_phase_change_ms(self, now: datetime | None = None) -> int | None: ...

    def today_market_open_ms(self) -> int | None: ...


class CalendarClock:
    """Clock over a protocol MarketCalendar, answering in the legacy phase strings.

    ``regular_only`` reads the venue's extended hours as closed. An index is
    calculated only in the regular session; on the equity phases its
    after-hours reads as an open session that stopped printing at the close,
    which is stale. Day-level answers keep the calendar's.
    """

    def __init__(
        self, cal: MarketCalendar, daily_backstop: bool = True, regular_only: bool = False,
    ) -> None:
        self._cal = cal
        self.tz = cal.tz
        self.daily_backstop = daily_backstop
        self.regular_only = regular_only

    @staticmethod
    def _now(now: datetime | None) -> datetime:
        return now or datetime.now(timezone.utc)

    def phase(self, now: datetime) -> MarketPhase:
        phase = self._cal.phase_at(now)
        if self.regular_only and phase in _EXTENDED_PHASES:
            return MarketPhase.CLOSED
        return phase

    def market_phase(self, now: datetime | None = None) -> str:
        return LEGACY_PHASE[self.phase(self._now(now))]

    def is_closed(self, now: datetime | None = None) -> bool:
        return self.market_phase(now) == "closed"

    def is_trading_day(self, day: date) -> bool:
        return self._cal.is_trading_day(day)

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
        now_ms = int(now.timestamp()) * 1000
        period = max(1, interval_seconds(interval))
        phase = self.phase(now)

        if phase in _ACTIVE_PHASES:
            anchor_ms = now_ms
        elif phase is MarketPhase.LUNCH:
            anchor_ms = self._lunch_anchor_ms(now)
        else:
            d = self._cal.latest_trading_date(now)
            close_ms = self._cal.session_close_ms(d)
            if close_ms is not None and close_ms > now_ms:
                # The trading date rolls at the pre-market open, so a
                # regular-only clock in pre-market is dated today, whose
                # close is still ahead; the last close is the session before.
                close_ms = self._cal.session_close_ms(self._cal.previous_session(d))
            if close_ms is None:
                return 0
            anchor_ms = min(close_ms, now_ms)

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
        """0 while the session runs, else seconds to the next open.

        The calendar counts a closed night down to the pre-market open; a
        regular-only clock counts it, and its own pre-market, to the regular
        open.
        """
        now = self._now(now)
        if not self.regular_only:
            return self._cal.seconds_until_next_open(now)
        if self.phase(now) is not MarketPhase.CLOSED:
            return 0
        if self._cal.phase_at(now) is MarketPhase.PRE:
            open_ms = self._cal.session_open_ms(now.astimezone(self.tz).date())
        else:
            open_ms = self._cal.next_session_open_ms(now)
        if open_ms is None:
            return NEXT_OPEN_FALLBACK_S
        return max(0, (open_ms - int(now.timestamp() * 1000)) // 1000)

    def seconds_until_next_session_open(self, now: datetime | None = None) -> int:
        now = self._now(now)
        open_ms = self._cal.next_session_open_ms(now)
        if open_ms is None:
            return NEXT_OPEN_FALLBACK_S
        return max(0, (open_ms - int(now.timestamp() * 1000)) // 1000)

    def next_phase_change_ms(self, now: datetime | None = None) -> int | None:
        now = self._now(now)
        edge = self._cal.next_phase_change_ms(now)
        if not self.regular_only:
            return edge
        # Extended-hours edges leave a regular-only phase where it was; step
        # past them. Running out of skips returns an edge before the real
        # change, which only costs a client one early re-poll.
        current = self.phase(now)
        for _ in range(_MAX_EDGE_SKIPS):
            if edge is None:
                return None
            at = datetime.fromtimestamp(edge / 1000, tz=timezone.utc)
            if self.phase(at) is not current:
                return edge
            edge = self._cal.next_phase_change_ms(at)
        return edge

    def today_market_open_ms(self) -> int | None:
        """Today's session open (exchange-local date), None before it opens."""
        now = datetime.now(timezone.utc)
        local_date = now.astimezone(self.tz).date()
        open_ms = self._cal.session_open_ms(local_date)
        if open_ms is None or int(now.timestamp() * 1000) < open_ms:
            return None
        return open_ms


# Index families whose daily bars follow the US (XNYS) calendar. An unknown
# family also lands on XNYS (the protocol's default home), so the daily
# backstop trusts only the protocol's known US families plus the US indexes
# the pre-CMDP _US_INDEX_SYMBOLS set named.
_US_CALENDAR_INDEXES = frozenset(
    ref.index_family for ref in index_families() if ref.calendar_id == "XNYS"
) | {
    "NYA", "XAX", "OEX", "MID", "SML", "SOX", "RUI", "RUA",
    "DJT", "DJU", "W5000", "WLSH",
}


def _classify(ref: InstrumentRef, is_index: bool, regular_only: bool = False) -> InstrumentClock:
    cal = get_calendar(ref.calendar_id)
    if is_index or ref.asset_class is AssetClass.INDEX:
        # Unknown family on XNYS: fail closed on the daily backstop, since
        # its daily anchor may follow a non-US calendar.
        backstop = ref.calendar_id != "XNYS" or ref.index_family in _US_CALENDAR_INDEXES
        return CalendarClock(cal, daily_backstop=backstop, regular_only=True)
    if ref.calendar_id == "XNYS" and ref.mic == UNKNOWN_MIC:
        return CalendarClock(
            cal, daily_backstop=is_us_class_share(ref), regular_only=regular_only,
        )
    return CalendarClock(cal, regular_only=regular_only)


# The endpoint's asset-class decision as a resolution hint. False is a
# decision too: the series cache keys a non-index endpoint's bars under the
# EQUITY reading (``series_identity``), so its clock must read the same listing.
_HINTS = {True: AssetClass.INDEX, False: AssetClass.EQUITY, None: None}


@lru_cache(maxsize=4096)
def _clock_cached(symbol: str, is_index: bool | None, regular_only: bool) -> InstrumentClock:
    ref = to_canonical(symbol, asset_class=_HINTS[is_index])
    return _classify(ref, bool(is_index), regular_only)


def clock_for_ref(ref: InstrumentRef) -> InstrumentClock:
    """The clock for an already-resolved listing; its asset class decides an index."""
    return _classify(ref, False)


def clock_for(
    symbol: str | None, is_index: bool | None = None, *, regular_only: bool = False,
) -> InstrumentClock:
    """Resolve the market clock for *symbol* (None → the US clock).

    ``is_index=False`` reads a bare spelling as the listing, so the stock COMP
    keeps its extended hours instead of taking the Nasdaq Composite's; a ``^``
    or ``I:`` spelling is an index either way. Leave it None when only the
    spelling is known, and a bare index family (GSPC, HSI) autodetects.
    ``regular_only`` reads extended hours as closed, for a print that only the
    regular session makes.
    """
    if not symbol:
        return CalendarClock(get_calendar("XNYS"), regular_only=regular_only)
    try:
        return _clock_cached(
            str(symbol), None if is_index is None else bool(is_index), regular_only,
        )
    except Exception:
        logger.warning("instrument_clock.fallback_us | symbol=%r", symbol, exc_info=True)
        return CalendarClock(get_calendar("XNYS"), regular_only=regular_only)
