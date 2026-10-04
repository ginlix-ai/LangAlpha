"""Market clock for the runtime-context tail envelope.

The envelope re-renders on every model call, so every answer here is a table
lookup plus arithmetic: no network, no data provider, no exchange-calendar
build on the request path. Callers pass the instant already captured for the
turn, which keeps the module deterministic under test.

Coverage is narrow on purpose. ``US`` carries a pinned NYSE holiday list;
``CN`` and ``HK`` are weekday-and-hours only. A session says which it is
through :attr:`MarketSession.calendar_known`, so a reader never mistakes a
best-effort answer for a verified one.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from src.tools.market_data.utils import (
    US_AFTER_HOURS_CLOSE,
    US_MARKET_CLOSE,
    US_MARKET_OPEN,
    US_PRE_MARKET_OPEN,
)
from src.utils.timezone_utils import zone_or_none

# Session phase names. The four values are the vocabulary the whole envelope
# speaks, and they match ``get_market_session`` so the tool output and the
# agent's runtime context cannot disagree.
PRE_MARKET = "PRE_MARKET"
REGULAR_HOURS = "REGULAR_HOURS"
AFTER_HOURS = "AFTER_HOURS"
CLOSED = "CLOSED"

# How far ahead a transition search walks before giving up. The longest gap in
# the calendars below is a holiday-extended weekend; the headroom is for a
# registered calendar with a week-long national break (CN Golden Week).
_LOOKAHEAD_DAYS = 14

# Day-by-day scan ceiling for :func:`sessions_closed_between`, so a bogus or
# epoch-zero ``start`` costs bounded work instead of spinning.
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


class MarketCalendar(Protocol):
    """Session answers for one market, offline.

    Implemented here by :class:`TableCalendar`. The protocol is the seam an
    exchange-calendar-backed implementation slots into (see the TODO on
    :data:`_US_HOLIDAYS`) without any caller changing.
    """

    market: str
    tz: ZoneInfo
    tz_name: str
    tz_label: str

    def is_trading_day(self, day: date) -> bool: ...

    def calendar_known(self, day: date) -> bool:
        """True when a holiday list covers ``day``; False when only weekday logic does."""
        ...

    def session_at(self, now: datetime) -> MarketSession: ...

    def next_open(self, now: datetime) -> datetime | None:
        """Start of the next regular session strictly after ``now``."""
        ...

    def regular_close(self, day: date) -> datetime | None:
        """End of ``day``'s last regular window, or None when it is not a trading day."""
        ...


class TableCalendar:
    """A market whose sessions are a fixed daily window table plus a holiday set.

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
        holidays: Mapping[int, Iterable[date]] | None = None,
        early_closes: Mapping[date, tuple[time, time]] | None = None,
    ) -> None:
        self.market = market
        self.tz_name = tz_name
        self.tz = ZoneInfo(tz_name)
        self.tz_label = tz_label
        self.windows = tuple(windows)
        self._holidays = {
            year: frozenset(days) for year, days in (holidays or {}).items()
        }
        # Day to (regular close, after-hours close) on a shortened session.
        self._early_closes = dict(early_closes or {})

    def _windows_for(self, day: date) -> tuple[tuple[time, time, str], ...]:
        early = self._early_closes.get(day)
        if early is None:
            return self.windows
        close, after_close = early
        shortened = []
        for start, end, phase in self.windows:
            if phase == REGULAR_HOURS:
                shortened.append((start, close, phase))
            elif phase == AFTER_HOURS:
                shortened.append((close, after_close, phase))
            else:
                shortened.append((start, end, phase))
        return tuple(shortened)

    # -- day-level facts ---------------------------------------------------

    def is_trading_day(self, day: date) -> bool:
        if day.weekday() >= 5:
            return False
        return day not in self._holidays.get(day.year, frozenset())

    def calendar_known(self, day: date) -> bool:
        return day.year in self._holidays

    def regular_close(self, day: date) -> datetime | None:
        if not self.is_trading_day(day):
            return None
        ends = [end for _start, end, phase in self._windows_for(day) if phase == REGULAR_HOURS]
        if not ends:
            return None
        return datetime.combine(day, max(ends), tzinfo=self.tz)

    # -- instant-level facts -----------------------------------------------

    def _phase_for_local_time(self, at: time, day: date) -> str:
        for start, end, phase in self._windows_for(day):
            if start <= at < end:
                return phase
        return CLOSED

    def phase_at(self, now: datetime) -> str:
        local = now.astimezone(self.tz)
        if not self.is_trading_day(local.date()):
            return CLOSED
        return self._phase_for_local_time(local.time(), local.date())

    def _edges(self, day: date) -> list[tuple[datetime, str]]:
        """Ascending (instant, phase beginning there) for ``day``; [] off a trading day."""
        if not self.is_trading_day(day):
            return []
        marks = sorted({t for w in self._windows_for(day) for t in (w[0], w[1])})
        return [
            (datetime.combine(day, mark, tzinfo=self.tz), self._phase_for_local_time(mark, day))
            for mark in marks
        ]

    def next_transition(self, now: datetime) -> tuple[datetime, str] | None:
        current = self.phase_at(now)
        after = _instant(now)
        start_day = now.astimezone(self.tz).date()
        for offset in range(_LOOKAHEAD_DAYS):
            for instant, phase in self._edges(start_day + timedelta(days=offset)):
                if _instant(instant) > after and phase != current:
                    return instant, phase
        return None

    def next_open(self, now: datetime) -> datetime | None:
        after = _instant(now)
        start_day = now.astimezone(self.tz).date()
        for offset in range(_LOOKAHEAD_DAYS):
            for instant, phase in self._edges(start_day + timedelta(days=offset)):
                if _instant(instant) > after and phase == REGULAR_HOURS:
                    return instant
        return None

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


# NYSE full-day closures. Pinned rather than computed: the rules mix fixed
# dates, nth-weekday rules, Good Friday (lunar) and weekend-observance shifts,
# and a wrong generated date is worse than an absent one.
#
# TODO: swap for ``market_protocol.calendars.get_calendar("XNYS")``, which
# already answers this exactly (and covers early closes, which this does not).
# It is not used here because building an ``exchange_calendars`` calendar costs
# real time on first touch, and the envelope renders on the model-call path in
# processes that never ran ``prebuild_calendars()``. Registering an adapter
# under :class:`MarketCalendar` is the shape of that change.
_US_HOLIDAYS: dict[int, tuple[date, ...]] = {
    2026: (
        date(2026, 1, 1),   # New Year's Day
        date(2026, 1, 19),  # Martin Luther King Jr. Day
        date(2026, 2, 16),  # Washington's Birthday
        date(2026, 4, 3),   # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 6, 19),  # Juneteenth
        date(2026, 7, 3),   # Independence Day (observed)
        date(2026, 9, 7),   # Labor Day
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),  # Christmas
    ),
    2027: (
        date(2027, 1, 1),   # New Year's Day
        date(2027, 1, 18),  # Martin Luther King Jr. Day
        date(2027, 2, 15),  # Washington's Birthday
        date(2027, 3, 26),  # Good Friday
        date(2027, 5, 31),  # Memorial Day
        date(2027, 6, 18),  # Juneteenth (observed)
        date(2027, 7, 5),   # Independence Day (observed)
        date(2027, 9, 6),   # Labor Day
        date(2027, 11, 25),  # Thanksgiving
        date(2027, 12, 24),  # Christmas (observed)
    ),
}


# NYSE shortened sessions: regular close 13:00, after-hours to 17:00. The day
# after Thanksgiving every year; Christmas Eve and July 3 only when they fall
# on a weekday that is not itself the observed holiday.
_US_EARLY_CLOSES: dict[date, tuple[time, time]] = {
    day: (time(13, 0), time(17, 0))
    for day in (
        date(2026, 11, 27),
        date(2026, 12, 24),
        date(2027, 11, 26),
    )
}


_US_CALENDAR = TableCalendar(
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
    early_closes=_US_EARLY_CLOSES,
    holidays=_US_HOLIDAYS,
)

# SSE/SZSE. Best effort: no holiday list, so every answer reports
# calendar_known=False. Mainland breaks (Spring Festival, Golden Week) are
# long, which is exactly why the flag has to travel with the session.
_CN_CALENDAR = TableCalendar(
    market="CN",
    tz_name="Asia/Shanghai",
    tz_label="CST",
    windows=(
        (time(9, 30), time(11, 30), REGULAR_HOURS),
        (time(13, 0), time(15, 0), REGULAR_HOURS),
    ),
)

# HKEX. Best effort, same caveat as CN.
_HK_CALENDAR = TableCalendar(
    market="HK",
    tz_name="Asia/Hong_Kong",
    tz_label="HKT",
    windows=(
        (time(9, 30), time(12, 0), REGULAR_HOURS),
        (time(13, 0), time(16, 0), REGULAR_HOURS),
    ),
)

MARKET_CALENDARS: dict[str, MarketCalendar] = {
    "US": _US_CALENDAR,
    "CN": _CN_CALENDAR,
    "HK": _HK_CALENDAR,
}


def get_market_calendar(market: str | None) -> MarketCalendar | None:
    """Calendar for a market code, or None for an unknown or missing one."""
    if not isinstance(market, str):
        return None
    return MARKET_CALENDARS.get(market.strip().upper())


class MarketClock:
    """Session and transition answers across the registered markets."""

    def __init__(self, calendars: Mapping[str, MarketCalendar] | None = None) -> None:
        self._calendars = dict(MARKET_CALENDARS if calendars is None else calendars)

    def calendar(self, market: str | None) -> MarketCalendar | None:
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

        if not session.calendar_known:
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


# Symbol suffix → market code. Mirrors the routing table in
# ``src.data_client.market_data_provider``; ``.SH`` is accepted alongside
# ``.SS`` because users write Shanghai tickers both ways.
_SUFFIX_MARKETS: dict[str, str] = {
    "SZ": "CN",
    "SH": "CN",
    "SS": "CN",
    "HK": "HK",
}


def derive_preferred_market(
    symbols: Iterable[str], explicit: str | None = None
) -> str | None:
    """The market a user's symbols are mostly in, or ``explicit`` when they said.

    An explicit choice wins even when no calendar backs it: a stated preference
    is data about the user, not a lookup key. Otherwise it is a majority vote by
    suffix, ties broken by first appearance so the user's own ordering decides.
    Returns None when there is nothing to go on.
    """
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().upper()

    counts: Counter[str] = Counter()
    for raw in symbols or ():
        if not isinstance(raw, str):
            continue
        symbol = raw.strip().upper()
        if not symbol:
            continue
        suffix = symbol.rsplit(".", 1)[-1] if "." in symbol else ""
        counts[_SUFFIX_MARKETS.get(suffix, "US")] += 1
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
    day = start.astimezone(calendar.tz).date()
    last_day = end.astimezone(calendar.tz).date()
    closed = 0
    for _ in range(_MAX_SESSION_SCAN_DAYS):
        if day > last_day:
            break
        close = calendar.regular_close(day)
        if close is not None and first < _instant(close) <= last:
            closed += 1
        day += timedelta(days=1)
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
