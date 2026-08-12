"""Market calendars: session/phase/freshness answers per exchange.

``MarketCalendar`` is the protocol staleness and phase logic will program
against (Phase 3 parameterizes the cache services by it). Implementations:

- ``XcalsCalendar`` — wraps ``exchange_calendars`` (XNYS, XHKG incl. lunch
  break, …). Instances are module-cached and session bounds memoized: the
  cache hit path calls into staleness on every request.
- ``Always24x7`` — crypto. ``Weekdays24x5`` — FX.

Call ``prebuild_calendars()`` at startup so the ~15 exchange calendars build
once, not on a request thread.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Protocol
from zoneinfo import ZoneInfo

from .enums import MarketPhase
from .symbology import ALWAYS_24_7, WEEKDAYS_24_5, _MICS

logger = logging.getLogger(__name__)

_UTC = timezone.utc


class MarketCalendar(Protocol):
    """Session-aware time answers for one exchange/regime."""

    calendar_id: str
    tz: ZoneInfo

    def phase_at(self, at: datetime) -> MarketPhase: ...

    def next_phase_change_ms(self, at: datetime) -> int | None:
        """Unix ms of the next ``phase_at`` transition, or None if the phase never changes."""
        ...

    def is_trading_day(self, d: date) -> bool: ...

    def latest_trading_date(self, at: datetime) -> date:
        """Trading date the market is currently 'in' (previous session before open)."""
        ...

    def expected_latest_daily_date(self, at: datetime) -> date:
        """Most recent session whose regular open is at or before *at*.

        The daily-staleness backstop anchor (generalizes #304 beyond XNYS).
        """
        ...

    def seconds_until_next_open(self, at: datetime) -> int: ...

    def session_open_ms(self, d: date) -> int | None: ...

    def session_close_ms(self, d: date) -> int | None: ...


# ---------------------------------------------------------------------------
# exchange_calendars-backed implementation
# ---------------------------------------------------------------------------

# Building deep history is wasted work; bounded start keeps startup fast.
_XCALS_START = "2016-01-01"

# Extended-hours windows (exchange-local). Only US equities expose pre/post
# data upstream today; other venues are CLOSED outside the regular session.
_EXTENDED_HOURS: dict[str, tuple[time, time]] = {
    "XNYS": (time(4, 0), time(20, 0)),
}


@lru_cache(maxsize=32)
def _xcals(calendar_id: str):
    import exchange_calendars as xcals

    return xcals.get_calendar(calendar_id, start=_XCALS_START)


@lru_cache(maxsize=16384)
def _session_bounds(
    calendar_id: str, iso_date: str
) -> tuple[int, int, int | None, int | None] | None:
    """(open_ms, close_ms, break_start_ms, break_end_ms) or None if no session."""
    import pandas as pd

    cal = _xcals(calendar_id)
    d = date.fromisoformat(iso_date)
    if d < cal.first_session.date() or d > cal.last_session.date():
        return None
    if not cal.is_session(iso_date):
        return None

    def _ms(ts) -> int | None:
        if ts is None or pd.isna(ts):
            return None
        return int(ts.timestamp() * 1000)

    open_ms = _ms(cal.session_open(iso_date))
    close_ms = _ms(cal.session_close(iso_date))
    if open_ms is None or close_ms is None:
        return None
    try:
        break_start = _ms(cal.session_break_start(iso_date))
        break_end = _ms(cal.session_break_end(iso_date))
    except Exception:
        break_start = break_end = None
    return (open_ms, close_ms, break_start, break_end)


def session_bounds(
    calendar_id: str, iso_date: str
) -> tuple[int, int, int | None, int | None] | None:
    """(open_ms, close_ms, break_start_ms, break_end_ms) for a session, or None."""
    return _session_bounds(calendar_id, iso_date)


class XcalsCalendar:
    """MarketCalendar backed by an ``exchange_calendars`` calendar."""

    def __init__(self, calendar_id: str) -> None:
        self.calendar_id = calendar_id
        cal = _xcals(calendar_id)
        tz = cal.tz
        self.tz = tz if isinstance(tz, ZoneInfo) else ZoneInfo(str(tz))
        self._extended = _EXTENDED_HOURS.get(calendar_id)

    def _bounds(self, d: date) -> tuple[int, int, int | None, int | None] | None:
        return _session_bounds(self.calendar_id, d.isoformat())

    def _day_start_ms(self, d: date) -> int | None:
        """Start of the session's data day: pre-market open if extended, else open."""
        bounds = self._bounds(d)
        if bounds is None:
            return None
        if self._extended:
            local = datetime.combine(d, self._extended[0], tzinfo=self.tz)
            return int(local.timestamp() * 1000)
        return bounds[0]

    def is_trading_day(self, d: date) -> bool:
        return self._bounds(d) is not None

    def phase_at(self, at: datetime) -> MarketPhase:
        ms = int(at.timestamp() * 1000)
        local_date = at.astimezone(self.tz).date()
        bounds = self._bounds(local_date)
        if bounds is None:
            return MarketPhase.CLOSED
        open_ms, close_ms, break_start, break_end = bounds
        if break_start is not None and break_end is not None and break_start <= ms < break_end:
            return MarketPhase.LUNCH
        if open_ms <= ms < close_ms:
            return MarketPhase.REGULAR
        if self._extended:
            pre_local = datetime.combine(local_date, self._extended[0], tzinfo=self.tz)
            post_local = datetime.combine(local_date, self._extended[1], tzinfo=self.tz)
            pre_ms = int(pre_local.timestamp() * 1000)
            post_ms = int(post_local.timestamp() * 1000)
            if pre_ms <= ms < open_ms:
                return MarketPhase.PRE
            if close_ms <= ms < post_ms:
                return MarketPhase.POST
        return MarketPhase.CLOSED

    def _phase_edges_ms(self, d: date) -> list[int]:
        """Every phase-transition instant of session *d*, ascending ([] if no session)."""
        bounds = self._bounds(d)
        if bounds is None:
            return []
        open_ms, close_ms, break_start, break_end = bounds
        edges = [open_ms, close_ms]
        if break_start is not None and break_end is not None:
            edges += [break_start, break_end]
        if self._extended:
            for t in self._extended:
                edges.append(int(datetime.combine(d, t, tzinfo=self.tz).timestamp() * 1000))
        return sorted(edges)

    def next_phase_change_ms(self, at: datetime) -> int | None:
        ms = int(at.timestamp() * 1000)
        local_date = at.astimezone(self.tz).date()
        for i in range(15):
            for edge in self._phase_edges_ms(local_date + timedelta(days=i)):
                if edge > ms:
                    return edge
        return None  # defensive; every xcals venue has a session within 15 days

    def latest_trading_date(self, at: datetime) -> date:
        ms = int(at.timestamp() * 1000)
        candidate = at.astimezone(self.tz).date()
        day_start = self._day_start_ms(candidate)
        if day_start is not None and ms >= day_start:
            return candidate
        return self._walk_back(candidate - timedelta(days=1))

    def expected_latest_daily_date(self, at: datetime) -> date:
        ms = int(at.timestamp() * 1000)
        candidate = at.astimezone(self.tz).date()
        for _ in range(15):
            bounds = self._bounds(candidate)
            if bounds is not None and bounds[0] <= ms:
                return candidate
            candidate -= timedelta(days=1)
        return candidate

    def _walk_back(self, candidate: date) -> date:
        for _ in range(15):
            if self.is_trading_day(candidate):
                return candidate
            candidate -= timedelta(days=1)
        return candidate

    def seconds_until_next_open(self, at: datetime) -> int:
        if self.phase_at(at) != MarketPhase.CLOSED:
            return 0
        ms = int(at.timestamp() * 1000)
        candidate = at.astimezone(self.tz).date()
        for _ in range(15):
            day_start = self._day_start_ms(candidate)
            if day_start is not None and day_start > ms:
                return max(0, (day_start - ms) // 1000)
            candidate += timedelta(days=1)
        return 43200  # defensive fallback, matches legacy behavior

    def session_open_ms(self, d: date) -> int | None:
        bounds = self._bounds(d)
        return bounds[0] if bounds else None

    def session_close_ms(self, d: date) -> int | None:
        bounds = self._bounds(d)
        return bounds[1] if bounds else None


# ---------------------------------------------------------------------------
# Hand-rolled regimes
# ---------------------------------------------------------------------------

class Always24x7:
    """Crypto: always regular session; the trading date is the UTC date."""

    calendar_id = ALWAYS_24_7
    tz = ZoneInfo("UTC")

    def phase_at(self, at: datetime) -> MarketPhase:
        return MarketPhase.REGULAR

    def next_phase_change_ms(self, at: datetime) -> int | None:
        return None  # always open — the phase never changes

    def is_trading_day(self, d: date) -> bool:
        return True

    def latest_trading_date(self, at: datetime) -> date:
        return at.astimezone(_UTC).date()

    def expected_latest_daily_date(self, at: datetime) -> date:
        return at.astimezone(_UTC).date()

    def seconds_until_next_open(self, at: datetime) -> int:
        return 0

    def session_open_ms(self, d: date) -> int | None:
        return int(datetime.combine(d, time(0, 0), tzinfo=_UTC).timestamp() * 1000)

    def session_close_ms(self, d: date) -> int | None:
        return int(datetime.combine(d + timedelta(days=1), time(0, 0), tzinfo=_UTC).timestamp() * 1000)


class Weekdays24x5:
    """FX: continuous Monday 00:00 – Saturday 00:00 UTC."""

    calendar_id = WEEKDAYS_24_5
    tz = ZoneInfo("UTC")

    def phase_at(self, at: datetime) -> MarketPhase:
        d = at.astimezone(_UTC)
        return MarketPhase.REGULAR if d.weekday() < 5 else MarketPhase.CLOSED

    def next_phase_change_ms(self, at: datetime) -> int | None:
        d = at.astimezone(_UTC)
        # Weekday → the Saturday 00:00 close; weekend → the Monday 00:00 open.
        days_ahead = (5 if d.weekday() < 5 else 7) - d.weekday()
        edge = datetime.combine(d.date() + timedelta(days=days_ahead), time(0, 0), tzinfo=_UTC)
        return int(edge.timestamp() * 1000)

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5

    def latest_trading_date(self, at: datetime) -> date:
        d = at.astimezone(_UTC).date()
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    def expected_latest_daily_date(self, at: datetime) -> date:
        return self.latest_trading_date(at)

    def seconds_until_next_open(self, at: datetime) -> int:
        d = at.astimezone(_UTC)
        if d.weekday() < 5:
            return 0
        days_ahead = 7 - d.weekday()
        next_open = datetime.combine(
            d.date() + timedelta(days=days_ahead), time(0, 0), tzinfo=_UTC
        )
        return max(0, int((next_open - d).total_seconds()))

    def session_open_ms(self, d: date) -> int | None:
        if d.weekday() >= 5:
            return None
        return int(datetime.combine(d, time(0, 0), tzinfo=_UTC).timestamp() * 1000)

    def session_close_ms(self, d: date) -> int | None:
        if d.weekday() >= 5:
            return None
        return int(datetime.combine(d + timedelta(days=1), time(0, 0), tzinfo=_UTC).timestamp() * 1000)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_HAND_ROLLED: dict[str, MarketCalendar] = {
    ALWAYS_24_7: Always24x7(),
    WEEKDAYS_24_5: Weekdays24x5(),
}


@lru_cache(maxsize=64)
def _xcals_calendar(calendar_id: str) -> XcalsCalendar:
    return XcalsCalendar(calendar_id)


def get_calendar(calendar_id: str) -> MarketCalendar:
    """Resolve a calendar id (from InstrumentRef.calendar_id) to an instance."""
    handrolled = _HAND_ROLLED.get(calendar_id)
    if handrolled is not None:
        return handrolled
    return _xcals_calendar(calendar_id)


def default_calendar_ids() -> tuple[str, ...]:
    """Every calendar id reachable from the built-in MIC table."""
    ids = {info.calendar_id for info in _MICS.values()}
    return tuple(sorted(ids)) + (ALWAYS_24_7, WEEKDAYS_24_5)


def prebuild_calendars(calendar_ids: tuple[str, ...] | None = None) -> int:
    """Eagerly build calendar instances (call once at startup). Returns count."""
    built = 0
    for cal_id in calendar_ids or default_calendar_ids():
        try:
            get_calendar(cal_id)
            built += 1
        except Exception:
            logger.warning("calendar.prebuild.failed | calendar_id=%s", cal_id, exc_info=True)
    return built
