"""Measured freshness for served bars and quotes.

The declared tier says what a provider sells; this says what actually arrived.
Both ship on the REST boundary because they disagree often enough to matter: a
"realtime" chain can run twelve minutes behind, and a closed Shanghai session
whose last bar is 14:56 is missing its closing auction.

Pure and clock-driven — no cache or provider access — so the measurement is
taken at response time. Computing it inside the quote cache would freeze a
row's label at write time and report it as fresh for the life of the entry.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from typing import Any, Callable, Iterable, Optional

from market_protocol import Tier
from market_protocol.freshness import DELAYED_LAG_CEILING_S, MIN_CREDIBLE_LAG_S, QUOTE_LIVE_S
from pydantic import BaseModel, Field

from src.data_client.instrument_clock import InstrumentClock, clock_for
from src.data_client.normalize import bars_regular_only
from src.utils.market_hours import interval_seconds


class FreshnessLabel(StrEnum):
    LIVE = "live"
    DELAYED = "delayed"
    STALE = "stale"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class Freshness(BaseModel):
    """How current a served series or quote actually is, measured at response time.

    A provider's declared tier says what it sells; this says what arrived. Both
    ship, because they disagree often enough to matter (a "realtime" chain that
    is 12 minutes behind, a closed CN session missing its 15:00 auction bar).
    """

    expected_latest: Optional[int] = Field(None, description="Newest bar/print that should exist now (Unix ms)")
    actual_latest: Optional[int] = Field(None, description="Newest bar/print actually served (Unix ms)")
    lag_s: Optional[int] = Field(None, description="Seconds behind the expectation; on a closed venue, the shortfall against the session's final bar, except while a delayed feed is still inside its delay after the close")
    label: FreshnessLabel = Field(FreshnessLabel.UNKNOWN, description="live / delayed / stale / incomplete / unknown")
    measured: bool = Field(False, description="False when nothing carried a timestamp and the label came from the declared tier")
    source: Optional[str] = Field(None, description="Provider that filled the measured series or quote")
    interval: Optional[str] = Field(None, description="Bar interval the measurement is against; null for quotes")
    closed: Optional[bool] = Field(None, description="Venue fully closed at measurement time, so a live row holds the session's final print rather than a moving price; null when no venue resolved")


def is_live(freshness: Freshness | dict[str, Any] | None) -> bool:
    """A price moving now: measured (or declared) live on a venue that is open.

    A closed venue's last print also measures ``live``, since nothing has traded
    after it, but it is the session's close. Takes the model or its JSON dump,
    which is what a stamped quote row carries.
    """
    if isinstance(freshness, dict):
        return freshness.get("label") == FreshnessLabel.LIVE and not freshness.get("closed")
    return (
        freshness is not None
        and freshness.label == FreshnessLabel.LIVE
        and not freshness.closed
    )


def _delayed_ceiling(period_s: int) -> int:
    """Lag up to which a feed still reads ``delayed`` rather than ``stale``.

    A feed sold as 15 minutes behind delivers a bar 15 minutes plus that bar's
    own period plus publication after the fact. Judged against a flat 15 minutes
    it would read stale every minute of the session while doing exactly what it
    says; two periods of headroom keep the label honest in both directions.
    """
    return DELAYED_LAG_CEILING_S + 2 * max(1, period_s)


def _delayed_lag(
    expected_at: Callable[[datetime], int], moment: datetime, actual: int, live_s: int,
) -> int:
    """Lag held to what a feed sold 15 minutes behind should show, on the session clock.

    Subtracting the delay from the realtime expectation counts a lunch break or
    a night as lag, which read a delayed feed stale for its first quarter hour
    after every open. Asking the clock what it expected the delay plus one
    period ago skips those gaps the way the realtime expectation does; in a
    continuous session the two lags are equal. Only a feed declared delayed is
    held to it: a realtime one with nothing since the gap is stuck, not behind.
    """
    slack = _delayed_ceiling(live_s) - live_s
    behind = expected_at(moment - timedelta(seconds=slack))
    return slack + max(0, (behind - actual) // 1000)


_TIER_LABELS = {
    Tier.REALTIME: FreshnessLabel.LIVE,
    Tier.DELAYED_15M: FreshnessLabel.DELAYED,
    Tier.EOD: FreshnessLabel.STALE,
}

# Bound on the daily trading-day walk; anything further behind is stale either way.
_MAX_DAYS_BEHIND = 12


def _now(now: datetime | None) -> datetime:
    return now or datetime.now(timezone.utc)


def _venue_closed(
    symbol: str | None, is_index: bool, moment: datetime, regular_only: bool = False,
) -> Optional[bool]:
    """Whether *symbol*'s venue is fully closed at *moment*; None with no symbol."""
    if not symbol:
        return None
    try:
        return clock_for(symbol, is_index, regular_only=regular_only).is_closed(moment)
    except Exception:
        return None


# How far a print's stamp may run ahead of our clock (skew between hosts)
# before it is not believed: the protocol's own credibility floor.
_FUTURE_SKEW_MS = -MIN_CREDIBLE_LAG_S * 1000


def _unmeasured(
    actual: int, source: str | None, closed: Optional[bool], interval: str | None = None,
) -> Freshness:
    """A timestamp with nothing honest to hold it against: nothing was measured.

    Two cases land here: a clock whose calendar names no session to expect a
    print from anchors at 0, and a print or bar stamped in the future. Either
    way the lag comes out at or below zero, which would read it as ``live``.
    """
    return Freshness(
        actual_latest=actual,
        label=FreshnessLabel.UNKNOWN,
        measured=False,
        source=source,
        interval=interval,
        closed=closed,
    )


def _last_bar_ms(bars: Iterable[Any]) -> Optional[int]:
    """Newest bar timestamp in *bars* (dicts or models), or None when empty."""
    latest: Optional[int] = None
    for bar in bars or ():
        t = bar.get("time") if isinstance(bar, dict) else getattr(bar, "time", None)
        if isinstance(t, (int, float)) and t > 0 and (latest is None or t > latest):
            latest = int(t)
    return latest


def measure_bars(
    symbol: str,
    interval: str,
    bars: Iterable[Any],
    *,
    is_index: bool = False,
    now: datetime | None = None,
    source: str | None = None,
    tier: Tier | None = None,
) -> Freshness:
    """Freshness of an intraday series against its venue clock.

    Open venue: lag is the expectation minus the last bar — within one interval
    is ``live``, within 15 minutes ``delayed``, beyond that ``stale``. Closed
    venue: the question is completeness, not lag, so the last bar is compared to
    the session's final anchor with one interval of tolerance. That tolerance is
    what absorbs the Shanghai closing auction, which providers label 14:59 or
    15:00 by their own convention; ``lag_s`` then carries the shortfall past it,
    unless a delayed feed is still inside its delay, when it stays the lag to
    the close.
    A feed declared delayed has its delay counted on the session clock
    (``_delayed_lag``), so a lunch break or a night never reads it stale, nor
    the quarter hour after the close incomplete.
    A *source* whose bars stop at the regular session is read on the
    regular-only clock: a complete series ending at the close is not stale
    through the after-hours and pre-market it never covers.
    """
    clock = clock_for(symbol, is_index, regular_only=bars_regular_only(source))
    period = max(1, interval_seconds(interval))
    actual = _last_bar_ms(bars)
    if actual is None:
        return Freshness(label=FreshnessLabel.UNKNOWN, measured=False, source=source, interval=interval)

    moment = _now(now)
    expected = clock.expected_latest_bar_ms(interval, moment)
    closed = clock.is_closed(moment)
    # A bar is stamped at its open, so none can start after now.
    if expected <= 0 or actual > int(moment.timestamp() * 1000) + _FUTURE_SKEW_MS:
        return _unmeasured(actual, source, closed, interval)
    lag = max(0, (expected - actual) // 1000)
    delayed_lag = (
        _delayed_lag(lambda at: clock.expected_latest_bar_ms(interval, at), moment, actual, period)
        if tier == Tier.DELAYED_15M else None
    )
    catching_up = delayed_lag is not None and delayed_lag <= _delayed_ceiling(period)

    measured = True
    if closed:
        shortfall = max(0, lag - period)
        if shortfall and catching_up:
            # Inside the delay after the close a delayed feed has not shown
            # the final bars yet. Only the delayed lag can say so: the lag to
            # the close stops growing once the session ends.
            label = FreshnessLabel.DELAYED
        else:
            label = FreshnessLabel.LIVE if shortfall == 0 else FreshnessLabel.INCOMPLETE
            lag = shortfall
    elif lag <= period:
        label = FreshnessLabel.LIVE
        if tier == Tier.DELAYED_15M and period >= DELAYED_LAG_CEILING_S:
            # A bar as wide as the declared delay cannot show that delay: the
            # current 4-hour bar exists whether the feed is 15 minutes behind
            # or not. The measurement is real but silent, so the declaration
            # stands and ``measured`` says the label was not proven.
            label = FreshnessLabel.DELAYED
            measured = False
    elif lag <= _delayed_ceiling(period) or catching_up:
        # Either lag may be the honest one: the wall-clock lag while the moment
        # sits in a lunch break (its anchor holds at the break), the delayed
        # lag when a gap lies between the last bar and the moment.
        label = FreshnessLabel.DELAYED
        if catching_up:
            lag = min(lag, delayed_lag)
    else:
        label = FreshnessLabel.STALE

    return Freshness(
        expected_latest=expected,
        actual_latest=actual,
        lag_s=int(lag),
        label=label,
        measured=measured,
        source=source,
        interval=interval,
        closed=closed,
    )


def _quote_expected_ms(clock: InstrumentClock, at: datetime) -> int:
    """When the newest print should be stamped at *at*; 0 with no session to anchor to.

    The clock's bar anchor is floor(now) while prints form, the break start
    through a lunch break, and the last close when closed. Only the first means
    "expect a print right now"; a Shanghai quote stamped 11:30 is current at
    11:45 because nothing has traded since, not ten minutes late.
    """
    anchor = clock.expected_latest_bar_ms("1min", at)
    at_ms = int(at.timestamp() * 1000)
    if anchor > 0 and not clock.is_closed(at) and anchor >= at_ms - 60_000:
        return at_ms
    return anchor


def measure_quote(
    symbol: str,
    as_of: int | None,
    tier: Tier | None,
    *,
    is_index: bool = False,
    regular_only: bool = False,
    now: datetime | None = None,
    source: str | None = None,
) -> Freshness:
    """Freshness of a snapshot row.

    With no ``as_of`` there is nothing to measure, so the label degrades to the
    provider's declared tier and ``measured`` stays false — the header must never
    render "Realtime" off a measurement that never happened. A row that only
    quotes the regular session (``regular_only``) is measured as an index is:
    extended hours read as closed, so its last regular print is not stale then.
    """
    moment = _now(now)
    if not as_of or as_of <= 0:
        return Freshness(
            label=_TIER_LABELS.get(tier, FreshnessLabel.UNKNOWN),
            measured=False,
            source=source,
            closed=_venue_closed(symbol, is_index, moment, regular_only),
        )

    clock = clock_for(symbol, is_index, regular_only=regular_only)
    closed = clock.is_closed(moment)
    now_ms = int(moment.timestamp() * 1000)
    if int(as_of) > now_ms + _FUTURE_SKEW_MS:
        return _unmeasured(int(as_of), source, closed)
    expected = _quote_expected_ms(clock, moment)
    if expected <= 0:
        return _unmeasured(int(as_of), source, closed)

    lag = (expected - int(as_of)) // 1000
    delayed_lag = (
        _delayed_lag(lambda at: _quote_expected_ms(clock, at), moment, int(as_of), QUOTE_LIVE_S)
        if tier == Tier.DELAYED_15M else None
    )
    catching_up = delayed_lag is not None and delayed_lag <= _delayed_ceiling(QUOTE_LIVE_S)
    if closed:
        # A print past the anchor is still the session's last one: Hong Kong's
        # closing auction settles after 16:00, a US after-hours trade after the
        # regular close. Short of it, a delayed feed inside its delay is still
        # catching up; anything else is stale.
        if lag <= QUOTE_LIVE_S:
            label = FreshnessLabel.LIVE
        elif catching_up:
            label = FreshnessLabel.DELAYED
        else:
            label = FreshnessLabel.STALE
    elif lag <= QUOTE_LIVE_S:
        label = FreshnessLabel.LIVE
    elif lag <= _delayed_ceiling(QUOTE_LIVE_S) or catching_up:
        # As for bars: the wall-clock lag through a lunch break, the delayed
        # lag across a gap behind the moment.
        label = FreshnessLabel.DELAYED
        if catching_up:
            lag = min(lag, delayed_lag)
    else:
        label = FreshnessLabel.STALE

    return Freshness(
        expected_latest=expected,
        actual_latest=int(as_of),
        lag_s=int(max(0, lag)),
        label=label,
        measured=True,
        source=source,
        closed=closed,
    )


def measure_daily(
    symbol: str,
    bars: Iterable[Any],
    *,
    is_index: bool = False,
    now: datetime | None = None,
    source: str | None = None,
) -> Freshness:
    """Freshness of a daily series, counted in trading days rather than seconds.

    A daily bar is either the one that should exist (``live``), the previous
    session's (``delayed`` — the publisher has not posted today's yet), or older
    (``stale``). Calendar days would misread every weekend as a four-day lag.
    """
    clock = clock_for(symbol, is_index)
    actual = _last_bar_ms(bars)
    if actual is None:
        return Freshness(label=FreshnessLabel.UNKNOWN, measured=False, source=source, interval="1day")

    moment = _now(now)
    expected_date = date.fromisoformat(clock.expected_latest_daily_date(moment))
    actual_date = datetime.fromtimestamp(actual / 1000, tz=timezone.utc).astimezone(clock.tz).date()
    if actual_date > moment.astimezone(clock.tz).date():
        # A session the venue has not reached yet: a mis-stamped bar, not a current one.
        return _unmeasured(actual, source, clock.is_closed(moment), "1day")

    behind = 0
    cursor = expected_date
    while cursor > actual_date and behind <= _MAX_DAYS_BEHIND:
        if clock.is_trading_day(cursor):
            behind += 1
        cursor -= timedelta(days=1)

    label = (
        FreshnessLabel.LIVE if behind == 0
        else FreshnessLabel.DELAYED if behind == 1
        else FreshnessLabel.STALE
    )
    expected_ms = int(datetime.combine(expected_date, time(0, 0), tzinfo=clock.tz).timestamp() * 1000)
    return Freshness(
        expected_latest=expected_ms,
        actual_latest=actual,
        lag_s=int(max(0, (expected_ms - actual) // 1000)),
        label=label,
        measured=True,
        source=source,
        interval="1day",
        closed=clock.is_closed(moment),
    )
