"""Locks the market clock behind the runtime-context tail envelope.

Every case pins a literal instant. The module takes ``now`` from its caller
precisely so the session math can be exercised without a clock, and a boundary
that drifts (an open time, a holiday, a DST offset) has to fail here rather
than in a rendered prompt. Holidays come from the published exchange calendars,
so the dates below are the venues' own, not a list kept here.
"""

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from ptc_agent.agent.middleware.runtime_context.clock import (
    AFTER_HOURS,
    CLOSED,
    PRE_MARKET,
    REGULAR_HOURS,
    MarketClock,
    derive_preferred_market,
    elapsed_summary,
    format_elapsed,
    get_market_calendar,
    market_status_line,
    sessions_closed_between,
)

ET = ZoneInfo("America/New_York")
SHANGHAI = ZoneInfo("Asia/Shanghai")
HONG_KONG = ZoneInfo("Asia/Hong_Kong")

CLOCK = MarketClock()


def monday_past_horizon(calendar_id: str) -> date:
    """A Monday years past *calendar_id*'s published sessions, wherever they
    end now: a calendar's horizon moves with each release, and a hard-coded
    year would eventually fall inside it."""
    from market_protocol.calendars import calendar_range

    day = calendar_range(calendar_id)[1] + timedelta(days=3 * 366)
    return day + timedelta(days=-day.weekday() % 7)


def et(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=ET)


# ---------------------------------------------------------------------------
# US session boundaries
# ---------------------------------------------------------------------------

# 2026-09-09 is a Wednesday and not an NYSE holiday.
@pytest.mark.parametrize(
    "moment,expected",
    [
        (et(2026, 9, 9, 3, 59), CLOSED),
        (et(2026, 9, 9, 4, 0), PRE_MARKET),
        (et(2026, 9, 9, 9, 29), PRE_MARKET),
        (et(2026, 9, 9, 9, 30), REGULAR_HOURS),
        (et(2026, 9, 9, 15, 59), REGULAR_HOURS),
        (et(2026, 9, 9, 16, 0), AFTER_HOURS),
        (et(2026, 9, 9, 19, 59), AFTER_HOURS),
        (et(2026, 9, 9, 20, 0), CLOSED),
        (et(2026, 9, 9, 23, 59), CLOSED),
    ],
)
def test_us_session_boundaries(moment, expected):
    assert CLOCK.session_at("US", moment).name == expected


def test_us_boundaries_agree_with_the_market_data_tool():
    """The clock and ``get_market_session`` read one table; prove they still agree."""
    from src.tools.market_data.utils import get_market_session

    for hour, minute in [(3, 59), (4, 0), (9, 30), (15, 59), (16, 0), (20, 0)]:
        moment = et(2026, 9, 9, hour, minute)
        assert CLOCK.session_at("US", moment).name == get_market_session(moment)[0]


def test_us_session_transitions_name_the_phase_that_begins():
    session = CLOCK.session_at("US", et(2026, 9, 9, 10, 0))
    assert session.next_transition_at == et(2026, 9, 9, 16, 0)
    assert session.next_transition_name == AFTER_HOURS
    assert session.is_trading_day is True
    assert session.calendar_known is True
    assert session.market_tz == "America/New_York"


def test_us_evening_rolls_to_the_next_morning():
    session = CLOCK.session_at("US", et(2026, 9, 9, 21, 0))
    assert session.name == CLOSED
    assert session.next_transition_at == et(2026, 9, 10, 4, 0)
    assert session.next_transition_name == PRE_MARKET


def test_us_weekend_is_closed_and_opens_monday():
    # Saturday 2026-09-12.
    session = CLOCK.session_at("US", et(2026, 9, 12, 12, 0))
    assert session.name == CLOSED
    assert session.is_trading_day is False
    assert session.next_transition_at == et(2026, 9, 14, 4, 0)
    assert CLOCK.next_open("US", et(2026, 9, 12, 12, 0)) == et(2026, 9, 14, 9, 30)


def test_us_listed_holiday_is_not_a_trading_day():
    # Thanksgiving 2026 is Thursday 2026-11-26; Friday the 27th trades.
    session = CLOCK.session_at("US", et(2026, 11, 26, 11, 0))
    assert session.name == CLOSED
    assert session.is_trading_day is False
    assert session.calendar_known is True
    assert CLOCK.next_open("US", et(2026, 11, 26, 11, 0)) == et(2026, 11, 27, 9, 30)


def test_us_early_close_shortens_the_session():
    """The day after Thanksgiving closes at 13:00 with after-hours to 17:00."""
    assert CLOCK.session_at("US", et(2026, 11, 27, 12, 30)).name == REGULAR_HOURS
    after = CLOCK.session_at("US", et(2026, 11, 27, 13, 30))
    assert after.name == AFTER_HOURS
    assert after.calendar_known is True
    noon = CLOCK.session_at("US", et(2026, 11, 27, 12, 0))
    assert (noon.next_transition_at, noon.next_transition_name) == (et(2026, 11, 27, 13, 0), AFTER_HOURS)
    late = CLOCK.session_at("US", et(2026, 11, 27, 16, 30))
    assert (late.next_transition_at, late.next_transition_name) == (et(2026, 11, 27, 17, 0), CLOSED)
    assert sessions_closed_between("US", et(2026, 11, 27, 12, 0), et(2026, 11, 27, 14, 0)) == 1
    # An ordinary day is untouched.
    assert CLOCK.session_at("US", et(2026, 11, 30, 13, 30)).name == REGULAR_HOURS


def test_us_day_past_the_calendar_horizon_falls_back_to_weekdays():
    day = monday_past_horizon("XNYS")
    session = CLOCK.session_at("US", et(day.year, day.month, day.day, 10, 0))
    assert session.name == REGULAR_HOURS
    assert session.calendar_known is False


# ---------------------------------------------------------------------------
# CN and HK
# ---------------------------------------------------------------------------


def test_cn_lunch_break_is_closed_and_reopens_at_one():
    # Wednesday 2026-09-09, 12:00 in Shanghai: between the two sessions.
    lunch = datetime(2026, 9, 9, 12, 0, tzinfo=SHANGHAI)
    session = CLOCK.session_at("CN", lunch)
    assert session.name == CLOSED
    assert session.is_trading_day is True
    assert session.next_transition_at == datetime(2026, 9, 9, 13, 0, tzinfo=SHANGHAI)
    assert session.next_transition_name == REGULAR_HOURS
    assert session.calendar_known is True
    assert session.market_tz == "Asia/Shanghai"


@pytest.mark.parametrize(
    "moment,expected",
    [
        (datetime(2026, 9, 9, 9, 29, tzinfo=SHANGHAI), CLOSED),
        (datetime(2026, 9, 9, 9, 30, tzinfo=SHANGHAI), REGULAR_HOURS),
        (datetime(2026, 9, 9, 11, 30, tzinfo=SHANGHAI), CLOSED),
        (datetime(2026, 9, 9, 13, 0, tzinfo=SHANGHAI), REGULAR_HOURS),
        (datetime(2026, 9, 9, 15, 0, tzinfo=SHANGHAI), CLOSED),
    ],
)
def test_cn_session_boundaries(moment, expected):
    assert CLOCK.session_at("CN", moment).name == expected


def test_hk_afternoon_session_and_close():
    trading = datetime(2026, 9, 9, 14, 0, tzinfo=HONG_KONG)
    session = CLOCK.session_at("HK", trading)
    assert session.name == REGULAR_HOURS
    assert session.next_transition_at == datetime(2026, 9, 9, 16, 0, tzinfo=HONG_KONG)
    assert session.calendar_known is True
    assert session.market_tz == "Asia/Hong_Kong"

    noon = datetime(2026, 9, 9, 12, 30, tzinfo=HONG_KONG)
    assert CLOCK.session_at("HK", noon).name == CLOSED


def test_cn_national_day_break_is_a_holiday_closure():
    # Golden Week 2026: the mainland is shut from Thursday 10-01 through 10-07.
    session = CLOCK.session_at("CN", datetime(2026, 10, 1, 10, 0, tzinfo=SHANGHAI))
    assert session.name == CLOSED
    assert session.is_trading_day is False
    assert session.calendar_known is True
    assert CLOCK.next_open("CN", datetime(2026, 10, 1, 10, 0, tzinfo=SHANGHAI)) == datetime(
        2026, 10, 8, 9, 30, tzinfo=SHANGHAI
    )
    assert CLOCK.describe("CN", datetime(2026, 10, 1, 10, 0, tzinfo=SHANGHAI)).startswith(
        "CN: closed (holiday),"
    )
    # Wednesday 09-30 and Thursday 10-08 close; nothing in between does.
    assert sessions_closed_between(
        "CN",
        datetime(2026, 9, 30, 9, 0, tzinfo=SHANGHAI),
        datetime(2026, 10, 9, 9, 0, tzinfo=SHANGHAI),
    ) == 2


def test_hk_national_day_is_a_holiday_closure():
    session = CLOCK.session_at("HK", datetime(2026, 10, 1, 10, 0, tzinfo=HONG_KONG))
    assert (session.name, session.is_trading_day, session.calendar_known) == (CLOSED, False, True)
    assert CLOCK.next_open("HK", datetime(2026, 10, 1, 10, 0, tzinfo=HONG_KONG)) == datetime(
        2026, 10, 2, 9, 30, tzinfo=HONG_KONG
    )


def test_cn_day_past_the_calendar_horizon_is_unverified():
    day = monday_past_horizon("XSHG")
    line = CLOCK.describe("CN", datetime(day.year, day.month, day.day, 10, 0, tzinfo=SHANGHAI))
    assert line == "CN: regular hours, closes in 5h (Mon 15:00 CST), holidays unverified"


def test_a_next_open_past_the_horizon_is_unverified(monkeypatch):
    # The last published session is verified, but the open it points at lands
    # on a day no holiday list covers (New Year's Day, at the real horizon).
    from ptc_agent.agent.middleware.runtime_context import clock as clock_module

    monkeypatch.setattr(
        clock_module.exchange_calendars, "calendar_range",
        lambda calendar_id: (date(2016, 1, 4), date(2026, 9, 9)),
    )
    calendar = clock_module.ExchangeCalendar(
        calendar_id="XSHG", market="CN", tz_name="Asia/Shanghai", tz_label="CST",
        windows=((time(9, 30), time(11, 30), REGULAR_HOURS), (time(13, 0), time(15, 0), REGULAR_HOURS)),
    )
    clock = MarketClock({"CN": calendar})
    last_close = datetime(2026, 9, 9, 16, 0, tzinfo=SHANGHAI)
    assert clock.session_at("CN", last_close).calendar_known is True
    assert clock.describe("CN", last_close) == (
        "CN: closed, next open in 17h 30m (Thu 09:30 CST), holidays unverified"
    )
    # A close on the last verified day is itself verified.
    assert clock.describe("CN", datetime(2026, 9, 9, 14, 0, tzinfo=SHANGHAI)) == (
        "CN: regular hours, closes in 1h (Wed 15:00 CST)"
    )


def test_a_calendar_that_cannot_build_demotes_to_the_weekday_table(monkeypatch, caplog):
    from ptc_agent.agent.middleware.runtime_context import clock as clock_module

    def broken(calendar_id: str):
        raise RuntimeError("no calendar data")

    monkeypatch.setattr(clock_module.exchange_calendars, "calendar_range", broken)
    calendar = clock_module.ExchangeCalendar(
        calendar_id="XSHG", market="CN", tz_name="Asia/Shanghai", tz_label="CST",
        windows=((time(9, 30), time(11, 30), REGULAR_HOURS), (time(13, 0), time(15, 0), REGULAR_HOURS)),
    )
    with caplog.at_level("WARNING"):
        holiday = calendar.session_at(datetime(2026, 10, 1, 10, 0, tzinfo=SHANGHAI))
    # A weekday trades, and the session says the holidays were not checked.
    assert (holiday.name, holiday.calendar_known) == (REGULAR_HOURS, False)
    assert sum("calendar_unavailable" in r.getMessage() for r in caplog.records) == 1
    # The failure is remembered: the second answer does not retry the build.
    calendar.session_at(datetime(2026, 10, 2, 10, 0, tzinfo=SHANGHAI))
    assert sum("calendar_unavailable" in r.getMessage() for r in caplog.records) == 1


def test_unknown_market_yields_none_and_never_raises():
    assert get_market_calendar("JP") is None
    assert get_market_calendar(None) is None
    assert CLOCK.session_at("JP", et(2026, 9, 9, 10, 0)) is None
    assert CLOCK.describe("JP", et(2026, 9, 9, 10, 0)) is None
    assert sessions_closed_between("JP", et(2026, 9, 8, 10, 0), et(2026, 9, 9, 10, 0)) == 0


def test_market_code_is_case_and_space_insensitive():
    assert CLOCK.session_at(" us ", et(2026, 9, 9, 10, 0)).name == REGULAR_HOURS


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


def test_describe_after_hours_names_the_next_open():
    line = CLOCK.describe("US", et(2026, 9, 9, 17, 0))
    assert line == "US: after-hours, next open in 16h 30m (Thu 09:30 ET)"


def test_describe_adds_the_viewer_clock_when_it_differs():
    line = CLOCK.describe("US", et(2026, 9, 9, 17, 0), viewer_tz="Asia/Shanghai")
    assert line == (
        "US: after-hours, next open in 16h 30m (Thu 09:30 ET, Thu 21:30 local)"
    )


def test_describe_drops_a_viewer_clock_that_restates_the_market():
    line = CLOCK.describe("US", et(2026, 9, 9, 17, 0), viewer_tz="US/Eastern")
    assert "local" not in line


def test_describe_survives_a_broken_viewer_timezone():
    line = CLOCK.describe("US", et(2026, 9, 9, 17, 0), viewer_tz="Mars/Olympus")
    assert line == "US: after-hours, next open in 16h 30m (Thu 09:30 ET)"


def test_describe_during_regular_hours_counts_down_to_the_close():
    line = CLOCK.describe("US", et(2026, 9, 9, 13, 46))
    assert line == "US: regular hours, closes in 2h 14m (Wed 16:00 ET)"


def test_describe_labels_weekend_and_holiday_closures():
    assert CLOCK.describe("US", et(2026, 9, 12, 12, 0)).startswith(
        "US: closed (weekend),"
    )
    assert CLOCK.describe("US", et(2026, 11, 26, 12, 0)).startswith(
        "US: closed (holiday),"
    )


def test_describe_omits_the_holiday_caveat_inside_the_calendar():
    line = CLOCK.describe("CN", datetime(2026, 9, 9, 10, 0, tzinfo=SHANGHAI))
    assert line == "CN: regular hours, closes in 5h (Wed 15:00 CST)"


def test_market_status_line_matches_the_default_clock():
    moment = et(2026, 9, 9, 17, 0)
    assert market_status_line("US", moment) == CLOCK.describe("US", moment)


# ---------------------------------------------------------------------------
# Preferred market
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "symbols,expected",
    [
        (["AAPL", "MSFT", "NVDA"], "US"),
        (["600519.SS", "000001.SZ", "AAPL"], "CN"),
        (["600519.SH"], "CN"),
        (["0700.HK", "9988.HK", "TSLA"], "HK"),
        (["AAPL", "0700.HK"], "US"),
        (["0700.HK", "AAPL"], "HK"),
        (["BRK.B"], "US"),
        (["920300.BJ", "830799.BJ", "AAPL"], "CN"),
        (["^HSI"], "HK"),
        # No clock for Tokyo here: the vote lands on the default.
        (["7203.T"], "US"),
        ([], None),
        ([""], None),
        (None, None),
    ],
)
def test_derive_preferred_market_by_listing(symbols, expected):
    assert derive_preferred_market(symbols) == expected


def test_resolve_preferred_market_reads_the_per_turn_watchlist():
    """The server puts the symbols on the data counts, not the cached profile."""
    from ptc_agent.agent.middleware.runtime_context.clock import resolve_preferred_market

    profile = {"name": "Li", "timezone": "Asia/Shanghai"}
    assert resolve_preferred_market(profile, None) == "US"
    assert resolve_preferred_market(profile, {"watchlist_symbols": ["600519.SH", "AAPL", "000001.SZ"]}) == "CN"
    assert resolve_preferred_market({**profile, "preferred_market": "hk"}, {"watchlist_symbols": ["AAPL"]}) == "HK"


def test_explicit_preference_wins_over_the_symbols():
    assert derive_preferred_market(["AAPL", "MSFT"], explicit="cn") == "CN"
    assert derive_preferred_market([], explicit=" hk ") == "HK"
    # A stated preference is data about the user even with no calendar behind it.
    assert derive_preferred_market(["AAPL"], explicit="JP") == "JP"
    assert derive_preferred_market(["AAPL"], explicit="  ") == "US"


# ---------------------------------------------------------------------------
# Elapsed time and session counting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "delta,expected",
    [
        (timedelta(seconds=30), "0m"),
        (timedelta(minutes=45), "45m"),
        (timedelta(hours=5, minutes=58), "5h 58m"),
        (timedelta(hours=3), "3h"),
        (timedelta(days=3, hours=4), "3d 4h"),
        (timedelta(days=2), "2d"),
        (timedelta(days=1, hours=0, minutes=59), "1d"),
        (timedelta(seconds=-90), "0m"),
    ],
)
def test_format_elapsed(delta, expected):
    start = et(2026, 9, 9, 10, 0)
    assert format_elapsed(start, start + delta) == expected


def test_sessions_closed_counts_only_closes_inside_the_window():
    # Wednesday 15:00 ET to Thursday 15:00 ET spans exactly one 16:00 close.
    assert sessions_closed_between("US", et(2026, 9, 9, 15, 0), et(2026, 9, 10, 15, 0)) == 1
    # A window that ends exactly on the close counts it; one that starts there does not.
    assert sessions_closed_between("US", et(2026, 9, 9, 15, 0), et(2026, 9, 9, 16, 0)) == 1
    assert sessions_closed_between("US", et(2026, 9, 9, 16, 0), et(2026, 9, 9, 23, 0)) == 0
    assert sessions_closed_between("US", et(2026, 9, 9, 10, 0), et(2026, 9, 9, 11, 0)) == 0


def test_sessions_closed_skips_the_weekend():
    # Friday 2026-09-11 09:00 ET through Monday 2026-09-14 09:00 ET: only Friday closes.
    assert sessions_closed_between("US", et(2026, 9, 11, 9, 0), et(2026, 9, 14, 9, 0)) == 1
    # Friday 09:00 through Tuesday 09:00 adds Monday's close.
    assert sessions_closed_between("US", et(2026, 9, 11, 9, 0), et(2026, 9, 15, 9, 0)) == 2


def test_sessions_closed_skips_a_listed_holiday():
    # Wed 2026-11-25 09:00 ET to Fri 2026-11-27 09:00 ET: Thanksgiving has no close.
    assert (
        sessions_closed_between("US", et(2026, 11, 25, 9, 0), et(2026, 11, 27, 9, 0)) == 1
    )


def test_sessions_closed_uses_the_market_local_close():
    # A CN session closes at 15:00 Shanghai, which is 03:00 ET the same day.
    start = datetime(2026, 9, 9, 2, 0, tzinfo=ET)
    end = datetime(2026, 9, 9, 4, 0, tzinfo=ET)
    assert sessions_closed_between("CN", start, end) == 1
    assert sessions_closed_between("US", start, end) == 0


def test_sessions_closed_is_zero_for_a_reversed_window():
    assert sessions_closed_between("US", et(2026, 9, 10, 9, 0), et(2026, 9, 9, 9, 0)) == 0


def test_elapsed_summary_pairs_the_gap_with_the_session_count():
    assert elapsed_summary(
        "US", et(2026, 9, 11, 9, 0), et(2026, 9, 14, 9, 0)
    ) == ("3d", 1)
    assert elapsed_summary(None, et(2026, 9, 11, 9, 0), et(2026, 9, 14, 9, 0)) == ("3d", 0)


# ---------------------------------------------------------------------------
# DST
# ---------------------------------------------------------------------------


def test_spring_forward_weekend_is_an_hour_short():
    """US DST starts Sunday 2026-03-08: the Friday-to-Monday gap loses an hour."""
    friday_close = datetime(2026, 3, 6, 16, 30, tzinfo=ET)
    monday_open = CLOCK.next_open("US", friday_close)
    assert monday_open == datetime(2026, 3, 9, 9, 30, tzinfo=ET)
    assert monday_open.utcoffset() == timedelta(hours=-4)
    assert friday_close.utcoffset() == timedelta(hours=-5)
    # 65 nominal wall-clock hours, 64 real ones.
    assert format_elapsed(friday_close, monday_open) == "2d 16h"


def test_fall_back_weekend_is_an_hour_long():
    """US DST ends Sunday 2026-11-01: the same weekend gains an hour."""
    friday_close = datetime(2026, 10, 30, 16, 30, tzinfo=ET)
    monday_open = CLOCK.next_open("US", friday_close)
    assert monday_open == datetime(2026, 11, 2, 9, 30, tzinfo=ET)
    assert monday_open.utcoffset() == timedelta(hours=-5)
    assert format_elapsed(friday_close, monday_open) == "2d 18h"


def test_sessions_are_counted_once_across_a_dst_change():
    # Friday 2026-10-30 09:00 ET to Tuesday 2026-11-03 09:00 ET: Fri and Mon close.
    assert (
        sessions_closed_between("US", datetime(2026, 10, 30, 9, 0, tzinfo=ET),
                                datetime(2026, 11, 3, 9, 0, tzinfo=ET)) == 2
    )


def test_session_boundaries_hold_on_both_sides_of_a_dst_change():
    calendar = get_market_calendar("US")
    assert calendar.regular_close(date(2026, 3, 6)).utcoffset() == timedelta(hours=-5)
    assert calendar.regular_close(date(2026, 3, 9)).utcoffset() == timedelta(hours=-4)
    for day in (date(2026, 3, 6), date(2026, 3, 9)):
        assert calendar.regular_close(day).timetz().replace(tzinfo=None) == time(16, 0)
