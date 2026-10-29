"""Market clock for the runtime-context tail envelope.

The envelope re-renders on every model call, so no answer here touches the
network or a data provider. The exchange calendars it reads are prebuilt at
server startup; a process that skipped the prebuild pays one build per
calendar on first use. Callers pass the instant already captured for the
turn, which keeps the module deterministic under test.

Sessions come from the exchange calendars in ``market_protocol.calendars``
(holidays, early closes, lunch breaks) for every day the published calendar
covers; a day past its horizon, or a calendar that fails to build, falls back
to a weekday-and-hours table. A session says which it got through
:attr:`MarketSession.calendar_known`, so a reader never mistakes a best-effort
answer for a verified one.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from market_protocol import calendars as exchange_calendars
from market_protocol import market_of, to_canonical
from market_protocol.enums import MarketPhase
from src.tools.market_data.utils import (
    US_AFTER_HOURS_CLOSE,
    US_MARKET_CLOSE,
    US_MARKET_OPEN,
    US_PRE_MARKET_OPEN,
)
from src.utils.timezone_utils import zone_or_none

logger = logging.getLogger(__name__)

# Session phase names. The four values are the vocabulary the whole envelope
# speaks, and they match ``get_market_session`` so the tool output and the
# agent's runtime context cannot disagree.
PRE_MARKET = "PRE_MARKET"
REGULAR_HOURS = "REGULAR_HOURS"
AFTER_HOURS = "AFTER_HOURS"
CLOSED = "CLOSED"

# How far ahead a transition search walks before giving up. The longest gap a
# covered calendar has is a week-long national break plus its weekends (CN
# Spring Festival, Golden Week).
_LOOKAHEAD_DAYS = 14

# Day-by-day scan ceiling for :meth:`TableCalendar.trading_days_between`, so a
# bogus or epoch-zero ``start`` costs bounded work instead of spinning.
_MAX_SESSION_SCAN_DAYS = 3700

def _instant(moment: datetime) -> datetime:
    """Absolute-time view of an aware datetime.

    Python skips the offset adjustment when two datetimes share a tzinfo
    *object*, so comparison and subtraction fall back to naive wall clock, and a
    same-zone pair straddling a DST change comes out an hour wrong. Converting
    first is what makes the arithmetic here true elapsed time.
    """
    return moment.astimezone(timezone.utc)


_SESSION_LABELS = {
    PRE_MARKET: "pre-market",
    REGULAR_HOURS: "regular hours",
    AFTER_HOURS: "after-hours",
    CLOSED: "closed",
}

# The envelope's four phases over the protocol's. A lunch break reads as
# closed: nothing trades, and the next transition is the afternoon open.
_PHASE_NAMES = {
    MarketPhase.PRE: PRE_MARKET,
    MarketPhase.REGULAR: REGULAR_HOURS,
    MarketPhase.POST: AFTER_HOURS,
}


@dataclass(frozen=True, slots=True)
class MarketSession:
    """One market's session state at one instant.

    ``next_transition_name`` is the phase that *begins* at
    ``next_transition_at``, not the one being left.
    """

    market: str
    name: str
    is_trading_day: bool
    next_transition_at: datetime | None
    next_transition_name: str | None
    calendar_known: bool
    market_tz: str


class TableCalendar:
    """A market whose sessions are a fixed daily window table on weekdays.

    No holidays: every weekday trades, and ``calendar_known`` is False for every
    day. :class:`ExchangeCalendar` falls back to it past the published
    calendar's horizon and when that calendar cannot be built.

    Windows are exchange-local wall clock. None of the covered markets moves a
    session across a DST discontinuity (US transitions land at 02:00, outside
    the 04:00-20:00 table; CN and HK have no DST), so a window boundary always
    resolves to exactly one instant.
    """

    def __init__(
        self,
        *,
        market: str,
        tz_name: str,
        tz_label: str,
        windows: Iterable[tuple[time, time, str]],
    ) -> None:
        self.market = market
        self.tz_name = tz_name
        self.tz = ZoneInfo(tz_name)
        self.tz_label = tz_label
        self.windows = tuple(windows)

    # -- day-level facts ---------------------------------------------------

    def is_trading_day(self, day: date) -> bool:
        return day.weekday() < 5

    def calendar_known(self, day: date) -> bool:
        """True when a holiday list covers ``day``; False when only weekday logic does."""
        return False

    def trading_days_between(self, first: date, last: date) -> Iterable[date]:
        """Trading days in ``[first, last]``, ascending."""
        day = first
        for _ in range(_MAX_SESSION_SCAN_DAYS):
            if day > last:
                break
            if self.is_trading_day(day):
                yield day
            day += timedelta(days=1)

    def regular_close(self, day: date) -> datetime | None:
        """End of ``day``'s last regular window, or None when it is not a trading day."""
        if not self.is_trading_day(day):
            return None
        ends = [end for _start, end, phase in self.windows if phase == REGULAR_HOURS]
        if not ends:
            return None
        return datetime.combine(day, max(ends), tzinfo=self.tz)

    # -- instant-level facts -----------------------------------------------

    def _phase_for_local_time(self, at: time) -> str:
        for start, end, phase in self.windows:
            if start <= at < end:
                return phase
        return CLOSED

    def phase_at(self, now: datetime) -> str:
        local = now.astimezone(self.tz)
        if not self.is_trading_day(local.date()):
            return CLOSED
        return self._phase_for_local_time(local.time())

    def _day_edges(self, day: date, after: datetime) -> Iterator[tuple[datetime, str]]:
        """(instant, phase beginning there) for ``day`` after ``after``, ascending."""
        if not self.is_trading_day(day):
            return
        for mark in sorted({t for w in self.windows for t in (w[0], w[1])}):
            instant = datetime.combine(day, mark, tzinfo=self.tz)
            if _instant(instant) > after:
                yield instant, self._phase_for_local_time(mark)

    def _edges_after(self, now: datetime) -> Iterator[tuple[datetime, str]]:
        after = _instant(now)
        start_day = now.astimezone(self.tz).date()
        for offset in range(_LOOKAHEAD_DAYS):
            yield from self._day_edges(start_day + timedelta(days=offset), after)

    def next_transition(self, now: datetime) -> tuple[datetime, str] | None:
        current = self.phase_at(now)
        return next(((t, p) for t, p in self._edges_after(now) if p != current), None)

    def next_open(self, now: datetime) -> datetime | None:
        """Start of the next regular session strictly after ``now``."""
        return next((t for t, p in self._edges_after(now) if p == REGULAR_HOURS), None)

    def session_at(self, now: datetime) -> MarketSession:
        local_day = now.astimezone(self.tz).date()
        transition = self.next_transition(now)
        return MarketSession(
            market=self.market,
            name=self.phase_at(now),
            is_trading_day=self.is_trading_day(local_day),
            next_transition_at=transition[0] if transition else None,
            next_transition_name=transition[1] if transition else None,
            calendar_known=self.calendar_known(local_day),
            market_tz=self.tz_name,
        )


class ExchangeCalendar(TableCalendar):
    """A market whose sessions come from its published exchange calendar.

    Every answer for a day inside ``calendar_range`` is the protocol
    calendar's (holidays, early closes, lunch breaks, extended hours), so this
    line and the cache phase cannot disagree; a day past the horizon is the
    weekday table's, reported as ``calendar_known`` False.

    The calendar is touched lazily: the envelope renders on the model-call
    path, and the server prebuilds every calendar at startup, so a process
    that skipped that pays the one-off build here instead. A build that fails
    demotes the market to the table for the life of the process, with one
    warning, because a session line must never fail the model call.
    """

    def __init__(self, *, calendar_id: str, **table: Any) -> None:
        super().__init__(**table)
        self.calendar_id = calendar_id
        self._range: tuple[date, date] | None = None
        self._unavailable = False

    def calendar_known(self, day: date) -> bool:
        if self._range is None and not self._unavailable:
            try:
                self._range = exchange_calendars.calendar_range(self.calendar_id)
            except Exception:
                self._unavailable = True
                logger.warning(
                    "runtime_context.clock.calendar_unavailable | market=%s calendar_id=%s",
                    self.market, self.calendar_id, exc_info=True,
                )
        return self._range is not None and self._range[0] <= day <= self._range[1]

    @property
    def _calendar(self) -> exchange_calendars.MarketCalendar:
        return exchange_calendars.get_calendar(self.calendar_id)

    def _local(self, ms: int) -> datetime:
        return datetime.fromtimestamp(ms / 1000, tz=self.tz)

    def is_trading_day(self, day: date) -> bool:
        if not self.calendar_known(day):
            return super().is_trading_day(day)
        return self._calendar.is_trading_day(day)

    def trading_days_between(self, first: date, last: date) -> Iterable[date]:
        if self.calendar_known(first) and self.calendar_known(last):
            return exchange_calendars.sessions_between(self.calendar_id, first, last)
        return super().trading_days_between(first, last)

    def regular_close(self, day: date) -> datetime | None:
        if not self.calendar_known(day):
            return super().regular_close(day)
        close_ms = self._calendar.session_close_ms(day)
        return None if close_ms is None else self._local(close_ms)

    def phase_at(self, now: datetime) -> str:
        if not self.calendar_known(now.astimezone(self.tz).date()):
            return super().phase_at(now)
        return _PHASE_NAMES.get(self._calendar.phase_at(now), CLOSED)

    def _day_edges(self, day: date, after: datetime) -> Iterator[tuple[datetime, str]]:
        if not self.calendar_known(day):
            yield from super()._day_edges(day, after)
            return
        calendar = self._calendar
        # Just before the day starts, so an edge on its first instant counts.
        cursor = max(after, _instant(datetime.combine(day, time(0), tzinfo=self.tz)) - timedelta(milliseconds=1))
        while (edge_ms := calendar.next_phase_change_ms(cursor)) is not None:
            edge = self._local(edge_ms)
            if edge.date() != day:
                return
            yield edge, _PHASE_NAMES.get(calendar.phase_at(edge), CLOSED)
            cursor = edge


_US_CALENDAR = ExchangeCalendar(
    calendar_id="XNYS",
    market="US",
    # The IANA zone, not the ``US/Eastern`` link ``get_market_session`` passes
    # to pytz; same offsets, and this spelling matches the instrument registry.
    tz_name="America/New_York",
    tz_label="ET",
    windows=(
        (US_PRE_MARKET_OPEN, US_MARKET_OPEN, PRE_MARKET),
        (US_MARKET_OPEN, US_MARKET_CLOSE, REGULAR_HOURS),
        (US_MARKET_CLOSE, US_AFTER_HOURS_CLOSE, AFTER_HOURS),
    ),
)

# SSE/SZSE. Mainland breaks (Spring Festival, Golden Week) are long, which is
# exactly why the calendar, and the flag that says it ran out, travel with
# the session.
_CN_CALENDAR = ExchangeCalendar(
    calendar_id="XSHG",
    market="CN",
    tz_name="Asia/Shanghai",
    tz_label="CST",
    windows=(
        (time(9, 30), time(11, 30), REGULAR_HOURS),
        (time(13, 0), time(15, 0), REGULAR_HOURS),
    ),
)

_HK_CALENDAR = ExchangeCalendar(
    calendar_id="XHKG",
    market="HK",
    tz_name="Asia/Hong_Kong",
    tz_label="HKT",
    windows=(
        (time(9, 30), time(12, 0), REGULAR_HOURS),
        (time(13, 0), time(16, 0), REGULAR_HOURS),
    ),
)

MARKET_CALENDARS: dict[str, TableCalendar] = {
    "US": _US_CALENDAR,
    "CN": _CN_CALENDAR,
    "HK": _HK_CALENDAR,
}


def get_market_calendar(market: str | None) -> TableCalendar | None:
    """Calendar for a market code, or None for an unknown or missing one."""
    if not isinstance(market, str):
        return None
    return MARKET_CALENDARS.get(market.strip().upper())


class MarketClock:
    """Session and transition answers across the registered markets."""

    def __init__(self, calendars: Mapping[str, TableCalendar] | None = None) -> None:
        self._calendars = dict(MARKET_CALENDARS if calendars is None else calendars)

    def calendar(self, market: str | None) -> TableCalendar | None:
        if not isinstance(market, str):
            return None
        return self._calendars.get(market.strip().upper())

    def session_at(self, market: str | None, now: datetime) -> MarketSession | None:
        """Session state for ``market`` at ``now``; None for an unknown market."""
        calendar = self.calendar(market)
        return None if calendar is None else calendar.session_at(now)

    def next_open(self, market: str | None, now: datetime) -> datetime | None:
        calendar = self.calendar(market)
        return None if calendar is None else calendar.next_open(now)

    def describe(
        self,
        market: str | None,
        now: datetime,
        viewer_tz: str | None = None,
        *,
        relative: bool = True,
    ) -> str | None:
        """One compact line, e.g. ``US: after-hours, next open in 13h 32m (Tue 09:30 ET)``.

        ``viewer_tz`` adds the transition in the user's own clock, which is the
        whole point when they trade a market they do not live in. It is dropped
        when it would restate the market's own time, or names no zone: a stale
        preference degrades the line rather than fail the model call.
        ``relative=False`` drops the countdown and names only the instant,
        which is what a line written once into history needs: a countdown is
        true for one minute, and the row is read for the rest of the thread.
        """
        calendar = self.calendar(market)
        if calendar is None:
            return None
        session = calendar.session_at(now)
        local_day = now.astimezone(calendar.tz).date()

        label = _SESSION_LABELS.get(session.name, session.name.lower())
        if session.name == CLOSED and not session.is_trading_day:
            label += " (weekend)" if local_day.weekday() >= 5 else " (holiday)"
        parts = [f"{session.market}: {label}"]

        if session.name == REGULAR_HOURS:
            target, verb = calendar.regular_close(local_day), "closes"
        else:
            target, verb = calendar.next_open(now), "next open"
        if target is not None:
            when = f"{target.strftime('%a %H:%M')} {calendar.tz_label}"
            viewer = zone_or_none(viewer_tz)
            local = (
                target.astimezone(viewer).strftime("%a %H:%M")
                if viewer is not None and viewer.utcoffset(target) != target.utcoffset()
                else None
            )
            if relative:
                if local:
                    when += f", {local} local"
                parts.append(f"{verb} in {format_elapsed(now, target)} ({when})")
            else:
                parts.append(f"{verb} {when}" + (f" ({local} local)" if local else ""))

        # The target can sit past the horizon while today does not: a New
        # Year's Eve close names a next open on a day no holiday list covers.
        if not session.calendar_known or (
            target is not None and not calendar.calendar_known(target.astimezone(calendar.tz).date())
        ):
            parts.append("holidays unverified")
        return ", ".join(parts)


MARKET_CLOCK = MarketClock()


def market_status_line(
    market: str | None,
    now: datetime,
    viewer_tz: str | None = None,
    *,
    relative: bool = True,
) -> str | None:
    """Module-level :meth:`MarketClock.describe` on the default registry."""
    return MARKET_CLOCK.describe(market, now, viewer_tz, relative=relative)


def _symbol_market(symbol: str) -> str:
    """The market code a symbol votes for, read off its protocol listing.

    A market with no clock here (and a symbol the registry cannot read) votes
    for the default, so the vote always lands on a market the line can render.
    """
    try:
        code = market_of(to_canonical(symbol)).upper()
    except Exception:
        return DEFAULT_PREFERRED_MARKET
    return code if code in MARKET_CALENDARS else DEFAULT_PREFERRED_MARKET


def derive_preferred_market(
    symbols: Iterable[str], explicit: str | None = None
) -> str | None:
    """The market a user's symbols are mostly in, or ``explicit`` when they said.

    An explicit choice wins even when no calendar backs it: a stated preference
    is data about the user, not a lookup key. Otherwise it is a majority vote by
    listing, ties broken by first appearance so the user's own ordering decides.
    Returns None when there is nothing to go on.
    """
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().upper()

    counts: Counter[str] = Counter()
    for raw in symbols or ():
        if not isinstance(raw, str):
            continue
        symbol = raw.strip()
        if not symbol:
            continue
        counts[_symbol_market(symbol)] += 1
    if not counts:
        return None
    # Counter keeps insertion order, and max() keeps the first maximum.
    return max(counts, key=lambda code: counts[code])


# A user with nothing to go on (new account, empty watchlist) still gets a
# clock: the product's primary market is the US, and the identity block and the
# clock line must agree on one value rather than one asserting "US" while the
# other stays silent.
DEFAULT_PREFERRED_MARKET = "US"


def resolve_preferred_market(
    user_profile: Mapping[str, Any] | None,
    user_data_counts: Mapping[str, Any] | None = None,
) -> str:
    """The market the clock line reports on, derived from the user's profile.

    The seam the agent builders call: an explicit profile field wins, then the
    watchlist vote, then the product default, so identity and clock never
    disagree about which market this user is in. The symbols ride the per-turn
    data counts, which is where the server reads the watchlist fresh.
    """
    profile = user_profile if isinstance(user_profile, Mapping) else {}
    counts = user_data_counts if isinstance(user_data_counts, Mapping) else {}
    explicit = profile.get("preferred_market")
    symbols = (
        profile.get("watchlist_symbols")
        or profile.get("symbols")
        or counts.get("watchlist_symbols")
        or []
    )
    if not isinstance(symbols, (list, tuple)):
        symbols = []
    derived = derive_preferred_market(
        symbols, explicit if isinstance(explicit, str) else None
    )
    return derived or DEFAULT_PREFERRED_MARKET


def sessions_closed_between(market: str | None, start: datetime, end: datetime) -> int:
    """Regular-session closes strictly after ``start`` and at or before ``end``.

    The unit the answer is wanted in: "two sessions have closed since you last
    looked" tells the model its prices are stale in a way an hour count cannot.
    Zero for an unknown market or a non-positive interval.
    """
    calendar = get_market_calendar(market)
    if calendar is None or not isinstance(start, datetime) or not isinstance(end, datetime):
        return 0
    if _instant(end) <= _instant(start):
        return 0

    first, last = _instant(start), _instant(end)
    first_day = start.astimezone(calendar.tz).date()
    last_day = end.astimezone(calendar.tz).date()
    closed = 0
    for day in calendar.trading_days_between(first_day, last_day):
        # A day strictly inside the window closes inside it; only the two edge
        # days need the instant checked.
        if first_day < day < last_day:
            closed += 1
            continue
        close = calendar.regular_close(day)
        if close is not None and first < _instant(close) <= last:
            closed += 1
    return closed


def format_elapsed(start: datetime, end: datetime) -> str:
    """Coarse gap between two instants, e.g. ``3d 4h``, ``5h 58m``, ``45m``.

    Two units at most: below a day the minutes matter, above one they are noise.
    """
    seconds = int((_instant(end) - _instant(start)).total_seconds())
    if seconds <= 0:
        return "0m"
    days, remainder = divmod(seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    return f"{minutes}m"


def elapsed_summary(
    market: str | None, start: datetime, end: datetime
) -> tuple[str, int]:
    """``(human gap, regular-session closes)`` for the interval, for the envelope."""
    return format_elapsed(start, end), sessions_closed_between(market, start, end)
