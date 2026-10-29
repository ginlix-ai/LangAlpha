"""Market calendars: session/phase/freshness answers per exchange.

``MarketCalendar`` is the interface staleness and phase logic program against.
Implementations:

- ``XcalsCalendar`` wraps ``exchange_calendars`` (XNYS, XHKG incl. lunch
  break, ...). Instances are module-cached and session bounds memoized, since
  a staleness check can run on every request. Past the published horizon it
  projects weekday sessions (see ``calendar_range``).
- ``Always24x7`` for crypto, ``Weekdays24x5`` for FX.

Call ``prebuild_calendars()`` at startup so the ~15 exchange calendars build
once, not on a request thread.
"""

from __future__ import annotations

import logging
import threading
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Protocol
from zoneinfo import ZoneInfo

from .enums import MarketPhase
from .symbology import ALWAYS_24_7, WEEKDAYS_24_5, _MICS

logger = logging.getLogger(__name__)

_UTC = timezone.utc


def _aware(at: datetime) -> datetime:
    """*at*, pinned to UTC when it carries no offset.

    ``timestamp()`` and ``astimezone()`` read a naive datetime as host-local
    time, so every entry point that takes one goes through here first.
    """
    if at.tzinfo is None or at.tzinfo.utcoffset(at) is None:
        return at.replace(tzinfo=_UTC)
    return at


class MarketCalendar(Protocol):
    """Session-aware time answers for one exchange/regime.

    A naive datetime is read as UTC, never as the host's local time, so an
    answer does not depend on the machine it runs on.
    """

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

        The newest daily bar a complete series can hold at *at*, on this
        venue's own sessions: a US holiday does not make a Hong Kong series stale.
        """
        ...

    def seconds_until_next_open(self, at: datetime) -> int: ...

    def next_session_open_ms(self, at: datetime) -> int | None:
        """Regular open of the session after the current one.

        Unlike ``seconds_until_next_open`` this never answers "now": while any
        phase of a session is active (extended hours and lunch included) it
        names the next trading day's open, which is what a once-per-day lock
        needs. Outside a session it is the next regular open.
        """
        ...

    def previous_session(self, d: date) -> date:
        """Latest trading day strictly before *d*. Raises ValueError when the calendar holds none."""
        ...

    def session_open_ms(self, d: date) -> int | None: ...

    def session_close_ms(self, d: date) -> int | None: ...


# ---------------------------------------------------------------------------
# exchange_calendars-backed implementation
# ---------------------------------------------------------------------------

# Before this floor a venue has no sessions at all (``calendar_range``), so a
# long daily history must fit after it. 2000 costs well under a second of
# startup over 2016 across every calendar; XSHG, XBOM and XTKS refuse 1990.
_XCALS_START = "2000-01-01"

# Longer than any gap between published sessions (Shanghai's longest since
# the history floor is 17 days, at the 2000 Spring Festival); day walks stop
# here, so a walk from inside a gap always reaches a session on either side.
_WALK_DAYS = 21

# What "seconds until the next open" answers when the walk finds no open:
# half a day keeps a cache TTL or a once-a-day lock bounded without claiming
# the venue opens now.
NEXT_OPEN_FALLBACK_S = 43200

# Extended-hours windows, exchange-local: (pre-market open, after-hours close,
# regular close on a full day). Only the US venues have extended hours here;
# other venues are CLOSED outside the regular session. After-hours runs
# as long past the close as it does on a full day, so an early close pulls it
# in too (NYSE: 13:00 close, 17:00 end).
_EXTENDED_HOURS: dict[str, tuple[time, time, time]] = {
    "XNYS": (time(4, 0), time(20, 0), time(16, 0)),
}


# A session can close on the day after its own (a 24x7 day ends at the next
# midnight), so the last day any calendar can place one on is the eve of date.max.
_LAST_PLACEABLE_DAY = date.max - timedelta(days=1)


def _placeable(d: date) -> date:
    if d > _LAST_PLACEABLE_DAY:
        raise ValueError(f"{d.isoformat()} is past the last day a calendar can place a session on")
    return d


def _days(first: date, last: date):
    """Every day in ``[first, last]``; by ordinal, so centuries are one cheap pass."""
    return map(date.fromordinal, range(first.toordinal(), last.toordinal() + 1))


# Only the venues the MIC table names. exchange_calendars also answers to
# aliases and sibling MICs (NYSE, XNAS), which would skip the hours keyed here.
_XCALS_IDS = frozenset(info.calendar_id for info in _MICS.values())

_xcals_built: dict[str, object] = {}
# From Python 3.12 a cached_property no longer locks, and exchange_calendars
# builds through them: two threads building one calendar at once can corrupt it.
_xcals_lock = threading.Lock()


def _xcals(calendar_id: str):
    cal = _xcals_built.get(calendar_id)
    if cal is not None:
        return cal
    if calendar_id not in _XCALS_IDS:
        raise ValueError(f"Unknown calendar id: {calendar_id!r}")
    with _xcals_lock:
        cal = _xcals_built.get(calendar_id)
        if cal is None:
            import exchange_calendars as xcals

            cal = _xcals_built[calendar_id] = xcals.get_calendar(calendar_id, start=_XCALS_START)
    return cal


def _zone(cal) -> ZoneInfo:
    tz = cal.tz
    return tz if isinstance(tz, ZoneInfo) else ZoneInfo(str(tz))


def _latest(table) -> time | None:
    """The entry in effect last in an ``exchange_calendars`` ((since, time), ...) table."""
    return table[-1][1] if table else None


@lru_cache(maxsize=32)
def _regular_hours(calendar_id: str):
    """The venue's current open, close and break times, as its calendar declares them."""
    cal = _xcals(calendar_id)
    return (
        _latest(cal.open_times),
        _latest(cal.close_times),
        _latest(cal.break_start_times),
        _latest(cal.break_end_times),
        cal.open_offset,
        cal.close_offset,
        cal.weekmask,
    )


def _projected_weekmask(calendar_id: str) -> str:
    """The weekdays a projected session falls on, Monday first ("1111100"); none without hours."""
    open_t, close_t, *_, weekmask = _regular_hours(calendar_id)
    return weekmask if open_t is not None and close_t is not None else "0000000"


def _projected_bounds(
    calendar_id: str, d: date
) -> tuple[int, int, int | None, int | None] | None:
    """A session past the published horizon: every weekday, the latest regular hours.

    A calendar package lists holidays only as far as the exchange has announced
    them, and a venue does not stop trading where the list stops. Reading those
    days as closed would hold a live market shut until the package is upgraded;
    a projected weekday is wrong only on the holidays nobody has published yet.
    """
    if _projected_weekmask(calendar_id)[d.weekday()] != "1":
        return None
    open_t, close_t, break_start, break_end, open_offset, close_offset, _ = (
        _regular_hours(calendar_id)
    )
    tz = _zone(_xcals(calendar_id))

    def ms(day: date, t: time) -> int:
        return int(datetime.combine(day, t, tzinfo=tz).timestamp() * 1000)

    has_break = break_start is not None and break_end is not None
    return (
        ms(d + timedelta(days=open_offset), open_t),
        ms(d + timedelta(days=close_offset), close_t),
        ms(d, break_start) if has_break else None,
        ms(d, break_end) if has_break else None,
    )


# Calendars already reported as running past their published sessions: one
# line per calendar per process, not one per projected day.
_PROJECTION_WARNED: set[str] = set()


def _warn_projection(calendar_id: str, last_session: date, d: date) -> None:
    if calendar_id in _PROJECTION_WARNED:
        return
    _PROJECTION_WARNED.add(calendar_id)
    logger.warning(
        "calendar.projected | calendar_id=%s last_session=%s date=%s "
        "| weekdays past the last published session are projected with no holidays",
        calendar_id,
        last_session.isoformat(),
        d.isoformat(),
    )


@lru_cache(maxsize=16384)
def _session_bounds(
    calendar_id: str, iso_date: str
) -> tuple[int, int, int | None, int | None] | None:
    """(open_ms, close_ms, break_start_ms, break_end_ms) or None if no session."""
    import pandas as pd
    from exchange_calendars.errors import NotSessionError

    cal = _xcals(calendar_id)
    d = date.fromisoformat(iso_date)
    if d < cal.first_session.date():
        return None
    if d > cal.last_session.date():
        _warn_projection(calendar_id, cal.last_session.date(), d)
        return _projected_bounds(calendar_id, d)
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
    # A venue without a break answers NaT, not an error; NotSessionError is the
    # one error these lookups declare, and is_session above already ruled it out.
    try:
        break_start = _ms(cal.session_break_start(iso_date))
        break_end = _ms(cal.session_break_end(iso_date))
    except NotSessionError:
        break_start = break_end = None
    return (open_ms, close_ms, break_start, break_end)


def session_bounds(
    calendar_id: str, iso_date: str
) -> tuple[int, int, int | None, int | None] | None:
    """(open_ms, close_ms, break_start_ms, break_end_ms) for a session, or None.

    Past ``calendar_range`` the session is projected, not published.
    """
    d = _placeable(date.fromisoformat(iso_date))
    handrolled = _HAND_ROLLED.get(calendar_id)
    if handrolled is None:
        return _session_bounds(calendar_id, iso_date)
    open_ms, close_ms = handrolled.session_open_ms(d), handrolled.session_close_ms(d)
    return None if open_ms is None or close_ms is None else (open_ms, close_ms, None, None)


@lru_cache(maxsize=32)
def calendar_range(calendar_id: str) -> tuple[date, date]:
    """First and last session the published calendar covers.

    Past the last one every answer here is projected: each weekday is a
    session at the venue's latest regular hours, with no holidays. A day in
    this range is a verified session or a verified holiday; a day past it is
    a best guess, which is what a caller that reports "holidays unverified"
    checks against. Before the first one there is no session at all. A
    hand-rolled regime is a rule with no list to run out, so it covers every
    day a calendar can place.
    """
    if calendar_id in _HAND_ROLLED:
        return date.min, _LAST_PLACEABLE_DAY
    cal = _xcals(calendar_id)
    return cal.first_session.date(), cal.last_session.date()


def sessions_between(calendar_id: str, first: date, last: date) -> tuple[date, ...]:
    """Every session in ``[first, last]``, projected past ``calendar_range`` like every other answer.

    A window anchored on a projected ``latest_trading_date`` must still end on
    that day; clamping here would hand such a caller an empty or short window
    while ``is_trading_day`` calls the same days sessions. The projected tail
    is weekday arithmetic rather than a walk through the session cache, so a
    far end costs one pass and evicts no hot session.
    """
    _placeable(first)
    _placeable(last)
    handrolled = _HAND_ROLLED.get(calendar_id)
    if handrolled is not None:
        return tuple(d for d in _days(first, last) if handrolled.is_trading_day(d))
    lo, hi = calendar_range(calendar_id)
    first = max(first, lo)
    if last < first:
        return ()
    published: tuple[date, ...] = ()
    if first <= hi:
        cal = _xcals(calendar_id)
        published = tuple(
            ts.date()
            for ts in cal.sessions_in_range(first.isoformat(), min(last, hi).isoformat())
        )
    tail = max(first, hi + timedelta(days=1))
    if tail > last:
        return published
    _warn_projection(calendar_id, hi, tail)
    weekmask = _projected_weekmask(calendar_id)
    return published + tuple(d for d in _days(tail, last) if weekmask[d.weekday()] == "1")


class XcalsCalendar:
    """MarketCalendar backed by an ``exchange_calendars`` calendar."""

    def __init__(self, calendar_id: str) -> None:
        self.calendar_id = calendar_id
        self.tz = _zone(_xcals(calendar_id))
        self._extended = _EXTENDED_HOURS.get(calendar_id)

    def _bounds(self, d: date) -> tuple[int, int, int | None, int | None] | None:
        return _session_bounds(self.calendar_id, d.isoformat())

    def extended_bounds_ms(self, d: date) -> tuple[int, int] | None:
        """(pre-market open, after-hours close) of session *d*, or None.

        None when *d* is not a session or the venue has no extended hours.
        """
        bounds = self._bounds(d)
        if bounds is None or not self._extended:
            return None
        pre_open, post_close, full_close = self._extended

        def ms(t: time) -> int:
            return int(datetime.combine(d, t, tzinfo=self.tz).timestamp() * 1000)

        post_ms = min(ms(post_close), bounds[1] + ms(post_close) - ms(full_close))
        return ms(pre_open), post_ms

    def _day_start_ms(self, d: date) -> int | None:
        """Start of the session's data day: pre-market open if extended, else open."""
        extended = self.extended_bounds_ms(d)
        if extended is not None:
            return extended[0]
        bounds = self._bounds(d)
        return bounds[0] if bounds else None

    def is_trading_day(self, d: date) -> bool:
        return self._bounds(d) is not None

    def _days_from(self, d: date, step: int = 1):
        for i in range(_WALK_DAYS):
            yield d + timedelta(days=i * step)

    def phase_at(self, at: datetime) -> MarketPhase:
        at = _aware(at)
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
        extended = self.extended_bounds_ms(local_date)
        if extended is not None:
            pre_ms, post_ms = extended
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
        extended = self.extended_bounds_ms(d)
        if extended is not None:
            edges += list(extended)
        return sorted(edges)

    def next_phase_change_ms(self, at: datetime) -> int | None:
        at = _aware(at)
        ms = int(at.timestamp() * 1000)
        for d in self._days_from(at.astimezone(self.tz).date()):
            for edge in self._phase_edges_ms(d):
                if edge > ms:
                    return edge
        return None

    def previous_session(self, d: date) -> date:
        for candidate in self._days_from(d - timedelta(days=1), -1):
            if self.is_trading_day(candidate):
                return candidate
        # A published calendar never goes this long without a session; only a
        # day at or before the history floor gets here, and has none to name.
        raise ValueError(f"No {self.calendar_id} session before {d} in calendar range")

    def latest_trading_date(self, at: datetime) -> date:
        at = _aware(at)
        ms = int(at.timestamp() * 1000)
        today = at.astimezone(self.tz).date()
        day_start = self._day_start_ms(today)
        if day_start is not None and ms >= day_start:
            return today
        return self.previous_session(today)

    def expected_latest_daily_date(self, at: datetime) -> date:
        at = _aware(at)
        ms = int(at.timestamp() * 1000)
        today = at.astimezone(self.tz).date()
        open_ms = self.session_open_ms(today)
        if open_ms is not None and open_ms <= ms:
            return today
        return self.previous_session(today)

    def seconds_until_next_open(self, at: datetime) -> int:
        at = _aware(at)
        if self.phase_at(at) != MarketPhase.CLOSED:
            return 0
        ms = int(at.timestamp() * 1000)
        for d in self._days_from(at.astimezone(self.tz).date()):
            day_start = self._day_start_ms(d)
            if day_start is not None and day_start > ms:
                return (day_start - ms) // 1000
        return NEXT_OPEN_FALLBACK_S

    def next_session_open_ms(self, at: datetime) -> int | None:
        at = _aware(at)
        ms = int(at.timestamp() * 1000)
        today = at.astimezone(self.tz).date()
        if self.phase_at(at) != MarketPhase.CLOSED:
            today += timedelta(days=1)
        for d in self._days_from(today):
            open_ms = self.session_open_ms(d)
            if open_ms is not None and open_ms > ms:
                return open_ms
        return None

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
        return None  # always open, so the phase never changes

    def is_trading_day(self, d: date) -> bool:
        return True

    def latest_trading_date(self, at: datetime) -> date:
        return _aware(at).astimezone(_UTC).date()

    def expected_latest_daily_date(self, at: datetime) -> date:
        return _aware(at).astimezone(_UTC).date()

    def seconds_until_next_open(self, at: datetime) -> int:
        return 0

    def next_session_open_ms(self, at: datetime) -> int | None:
        return self.session_open_ms(_aware(at).astimezone(_UTC).date() + timedelta(days=1))

    def previous_session(self, d: date) -> date:
        return d - timedelta(days=1)

    def session_open_ms(self, d: date) -> int | None:
        return int(datetime.combine(d, time(0, 0), tzinfo=_UTC).timestamp() * 1000)

    def session_close_ms(self, d: date) -> int | None:
        return int(datetime.combine(d + timedelta(days=1), time(0, 0), tzinfo=_UTC).timestamp() * 1000)


class Weekdays24x5:
    """FX: continuous from Monday 00:00 to Saturday 00:00 UTC.

    UTC days approximate the market, which trades from about 17:00 New York on
    Sunday to 17:00 New York on Friday: ``phase_at`` reads CLOSED on Sunday
    evening UTC and REGULAR late on Friday UTC. They are kept because a session
    here is one trade date of the vendors' daily FX bars, stamped 00:00 UTC.
    """

    calendar_id = WEEKDAYS_24_5
    tz = ZoneInfo("UTC")

    def phase_at(self, at: datetime) -> MarketPhase:
        d = _aware(at).astimezone(_UTC)
        return MarketPhase.REGULAR if d.weekday() < 5 else MarketPhase.CLOSED

    def next_phase_change_ms(self, at: datetime) -> int | None:
        d = _aware(at).astimezone(_UTC)
        # Weekday → the Saturday 00:00 close; weekend → the Monday 00:00 open.
        days_ahead = (5 if d.weekday() < 5 else 7) - d.weekday()
        edge = datetime.combine(d.date() + timedelta(days=days_ahead), time(0, 0), tzinfo=_UTC)
        return int(edge.timestamp() * 1000)

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5

    def latest_trading_date(self, at: datetime) -> date:
        d = _aware(at).astimezone(_UTC).date()
        return d if self.is_trading_day(d) else self.previous_session(d)

    def expected_latest_daily_date(self, at: datetime) -> date:
        return self.latest_trading_date(at)

    def seconds_until_next_open(self, at: datetime) -> int:
        d = _aware(at).astimezone(_UTC)
        if d.weekday() < 5:
            return 0
        days_ahead = 7 - d.weekday()
        next_open = datetime.combine(
            d.date() + timedelta(days=days_ahead), time(0, 0), tzinfo=_UTC
        )
        return max(0, int((next_open - d).total_seconds()))

    def next_session_open_ms(self, at: datetime) -> int | None:
        d = _aware(at).astimezone(_UTC).date() + timedelta(days=1)
        while not self.is_trading_day(d):
            d += timedelta(days=1)
        return self.session_open_ms(d)

    def previous_session(self, d: date) -> date:
        d -= timedelta(days=1)
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

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
    """Resolve a calendar id (from InstrumentRef.calendar_id) to an instance.

    Raises ValueError for any id outside ``default_calendar_ids()``.
    """
    handrolled = _HAND_ROLLED.get(calendar_id)
    if handrolled is not None:
        return handrolled
    return _xcals_calendar(calendar_id)


def default_calendar_ids() -> tuple[str, ...]:
    """Every calendar id reachable from the built-in MIC table."""
    ids = {info.calendar_id for info in _MICS.values()}
    return tuple(sorted(ids)) + (ALWAYS_24_7, WEEKDAYS_24_5)


def prebuild_calendars(calendar_ids: tuple[str, ...] | None = None) -> int:
    """Eagerly build calendar instances (call once at startup). Returns count.

    Only None means every default calendar; an empty tuple builds none.
    """
    built = 0
    for cal_id in default_calendar_ids() if calendar_ids is None else calendar_ids:
        try:
            get_calendar(cal_id)
            built += 1
        except Exception:
            logger.warning("calendar.prebuild.failed | calendar_id=%s", cal_id, exc_info=True)
    return built
