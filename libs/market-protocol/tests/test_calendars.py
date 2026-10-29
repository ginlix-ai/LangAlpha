"""Calendar spot checks.

XNYS is the only US calendar: the holidays and early closes below are pinned,
so a calendar-data change that drops one fails here. XHKG/24x7/24x5 cover the
non-US regimes.
"""

import logging
import os
import re
import threading
import time as host_time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from exchange_calendars.errors import NotSessionError

from market_protocol import calendars, to_canonical
from market_protocol.calendars import (
    Always24x7,
    Weekdays24x5,
    calendar_range,
    default_calendar_ids,
    get_calendar,
    prebuild_calendars,
    session_bounds,
    sessions_between,
)
from market_protocol.enums import MarketPhase

ET = ZoneInfo("America/New_York")
HKT = ZoneInfo("Asia/Hong_Kong")
CST = ZoneInfo("Asia/Shanghai")

HAND_ROLLED_IDS = ["ALWAYS_24_7", "WEEKDAYS_24_5"]
# The exchange_calendars-backed ids; the hand-rolled regimes project nothing.
XCALS_IDS = [c for c in default_calendar_ids() if c not in HAND_ROLLED_IDS]


class TestXNYS:
    # NYSE full-day closures, 2025-2026.
    HOLIDAYS = [
        # 2025-01-09: one-off closure, the national day of mourning for President Carter.
        date(2025, 1, 1), date(2025, 1, 9), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
        date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
        date(2025, 11, 27), date(2025, 12, 25),
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    ]
    # NYSE early closes: regular session ends 13:00, after-hours 17:00.
    EARLY_CLOSES = [
        date(2025, 7, 3), date(2025, 11, 28), date(2025, 12, 24),
        date(2026, 11, 27), date(2026, 12, 24),
    ]

    # Every phase boundary of a trading day (2026-07-02), a holiday
    # (2026-07-03, Independence Day observed), a weekend, and an early close.
    PHASES = [
        (datetime(2026, 7, 2, 3, 59, tzinfo=ET), MarketPhase.CLOSED),
        (datetime(2026, 7, 2, 4, 0, tzinfo=ET), MarketPhase.PRE),
        (datetime(2026, 7, 2, 9, 29, tzinfo=ET), MarketPhase.PRE),
        (datetime(2026, 7, 2, 9, 30, tzinfo=ET), MarketPhase.REGULAR),
        (datetime(2026, 7, 2, 15, 59, tzinfo=ET), MarketPhase.REGULAR),
        (datetime(2026, 7, 2, 16, 0, tzinfo=ET), MarketPhase.POST),
        (datetime(2026, 7, 2, 19, 59, tzinfo=ET), MarketPhase.POST),
        (datetime(2026, 7, 2, 20, 0, tzinfo=ET), MarketPhase.CLOSED),
        (datetime(2026, 7, 3, 10, 0, tzinfo=ET), MarketPhase.CLOSED),
        (datetime(2026, 7, 5, 12, 0, tzinfo=ET), MarketPhase.CLOSED),
        (datetime(2026, 11, 27, 12, 59, tzinfo=ET), MarketPhase.REGULAR),
        (datetime(2026, 11, 27, 13, 0, tzinfo=ET), MarketPhase.POST),
        (datetime(2026, 11, 27, 16, 59, tzinfo=ET), MarketPhase.POST),
        (datetime(2026, 11, 27, 17, 0, tzinfo=ET), MarketPhase.CLOSED),
    ]

    @staticmethod
    def _ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    def test_holidays_are_not_sessions(self):
        cal = get_calendar("XNYS")
        for d in self.HOLIDAYS:
            assert not cal.is_trading_day(d), d.isoformat()

    def test_early_closes(self):
        cal = get_calendar("XNYS")
        for d in self.EARLY_CLOSES:
            close = datetime.combine(d, datetime.min.time(), tzinfo=ET).replace(hour=13)
            assert cal.session_close_ms(d) == self._ms(close), d.isoformat()
            assert cal.phase_at(close.replace(hour=16, minute=59)) == MarketPhase.POST
            assert cal.phase_at(close.replace(hour=17)) == MarketPhase.CLOSED

    def test_phases(self):
        cal = get_calendar("XNYS")
        for at, phase in self.PHASES:
            assert cal.phase_at(at) == phase, at.isoformat()

    def test_trading_dates(self):
        cal = get_calendar("XNYS")
        # Before the 04:00 pre-open the trading date is still the previous session.
        assert cal.latest_trading_date(datetime(2026, 7, 2, 3, 59, tzinfo=ET)) == date(2026, 7, 1)
        assert cal.latest_trading_date(datetime(2026, 7, 2, 4, 0, tzinfo=ET)) == date(2026, 7, 2)
        # The daily bar only exists from the 09:30 open.
        assert cal.expected_latest_daily_date(datetime(2026, 7, 2, 9, 29, tzinfo=ET)) == date(2026, 7, 1)
        assert cal.expected_latest_daily_date(datetime(2026, 7, 2, 9, 30, tzinfo=ET)) == date(2026, 7, 2)
        # Holiday Friday plus the weekend walk back to Thursday.
        assert cal.latest_trading_date(datetime(2026, 7, 5, 12, 0, tzinfo=ET)) == date(2026, 7, 2)

    def test_next_open_and_phase_change(self):
        cal = get_calendar("XNYS")
        after_hours = datetime(2026, 7, 2, 21, 0, tzinfo=ET)
        monday_pre = datetime(2026, 7, 6, 4, 0, tzinfo=ET)
        assert cal.next_phase_change_ms(after_hours) == self._ms(monday_pre)
        assert cal.seconds_until_next_open(after_hours) == int((monday_pre - after_hours).total_seconds())
        early = datetime(2026, 11, 27, 13, 30, tzinfo=ET)
        assert cal.next_phase_change_ms(early) == self._ms(early.replace(hour=17, minute=0))


class TestXHKG:
    def test_lunch_break(self):
        cal = get_calendar("XHKG")
        assert cal.phase_at(datetime(2026, 7, 3, 12, 15, tzinfo=HKT)) == MarketPhase.LUNCH
        assert cal.phase_at(datetime(2026, 7, 3, 10, 0, tzinfo=HKT)) == MarketPhase.REGULAR
        assert cal.phase_at(datetime(2026, 7, 3, 14, 0, tzinfo=HKT)) == MarketPhase.REGULAR

    def test_no_extended_hours(self):
        cal = get_calendar("XHKG")
        assert cal.phase_at(datetime(2026, 7, 3, 8, 0, tzinfo=HKT)) == MarketPhase.CLOSED
        assert cal.phase_at(datetime(2026, 7, 3, 17, 0, tzinfo=HKT)) == MarketPhase.CLOSED

    def test_us_holiday_is_hk_trading_day(self):
        """2026-07-03: XNYS closed, XHKG open, so a US holiday cannot mark HK stale."""
        cal = get_calendar("XHKG")
        assert cal.is_trading_day(date(2026, 7, 3))
        assert not get_calendar("XNYS").is_trading_day(date(2026, 7, 3))

    def test_expected_daily_uses_hk_sessions(self):
        cal = get_calendar("XHKG")
        # Friday 2026-07-03 20:00 HKT: session done, expected bar = today.
        at = datetime(2026, 7, 3, 20, 0, tzinfo=HKT)
        assert cal.expected_latest_daily_date(at) == date(2026, 7, 3)
        # Saturday: still Friday's bar.
        at = datetime(2026, 7, 4, 12, 0, tzinfo=HKT)
        assert cal.expected_latest_daily_date(at) == date(2026, 7, 3)

    def test_next_phase_change_walks_lunch_close_and_weekend(self):
        cal = get_calendar("XHKG")

        def ms(dt):
            return int(dt.timestamp() * 1000)

        # Morning session → lunch start; lunch → lunch end; after Friday's
        # close → Monday's 09:30 open (no extended hours to cross first).
        cases = [
            (datetime(2026, 7, 3, 10, 0, tzinfo=HKT), datetime(2026, 7, 3, 12, 0, tzinfo=HKT)),
            (datetime(2026, 7, 3, 12, 15, tzinfo=HKT), datetime(2026, 7, 3, 13, 0, tzinfo=HKT)),
            (datetime(2026, 7, 3, 17, 0, tzinfo=HKT), datetime(2026, 7, 6, 9, 30, tzinfo=HKT)),
        ]
        for at, expected in cases:
            assert cal.next_phase_change_ms(at) == ms(expected), at.isoformat()


class TestPastThePublishedHorizon:
    """A venue keeps trading where its published holiday list stops.

    The days are taken past whatever horizon the installed calendar has, so the
    cases keep testing the projection after a calendar upgrade moves it.
    """

    def _cal(self):
        return get_calendar(to_canonical("600519.SH").calendar_id)

    def _past_horizon(self, weekday: int, *calendar_ids: str) -> date:
        """The first *weekday* (Mon=0) at least a month past each calendar's published range.

        XSHG's alone by default; a case that projects several calendars names
        them all, since a calendar upgrade can move one horizon and not the rest.
        """
        day = max(calendar_range(c)[1] for c in calendar_ids or ("XSHG",)) + timedelta(days=30)
        return day + timedelta(days=(weekday - day.weekday()) % 7)

    @staticmethod
    def _at(day: date, hour: int, minute: int = 0) -> datetime:
        return datetime.combine(day, time(hour, minute), tzinfo=CST)

    def test_a_weekday_trades_the_latest_regular_hours(self):
        cal, wed = self._cal(), self._past_horizon(2)
        assert cal.is_trading_day(wed)
        assert cal.phase_at(self._at(wed, 9, 29)) == MarketPhase.CLOSED
        assert cal.phase_at(self._at(wed, 10, 0)) == MarketPhase.REGULAR
        assert cal.phase_at(self._at(wed, 12, 0)) == MarketPhase.LUNCH
        assert cal.phase_at(self._at(wed, 14, 0)) == MarketPhase.REGULAR
        assert cal.phase_at(self._at(wed, 15, 0)) == MarketPhase.CLOSED
        assert cal.session_open_ms(wed) == int(self._at(wed, 9, 30).timestamp() * 1000)
        assert cal.session_close_ms(wed) == int(self._at(wed, 15, 0).timestamp() * 1000)
        assert cal.latest_trading_date(self._at(wed, 20, 0)) == wed

    def test_the_next_open_is_the_next_weekday_not_the_fallback(self):
        cal, fri = self._cal(), self._past_horizon(4)
        monday_open = self._at(fri + timedelta(days=3), 9, 30)
        evening = self._at(fri, 16, 0)
        assert cal.seconds_until_next_open(evening) == int((monday_open - evening).total_seconds())
        assert cal.next_session_open_ms(evening) == int(monday_open.timestamp() * 1000)
        assert cal.next_phase_change_ms(evening) == int(monday_open.timestamp() * 1000)

    def test_a_weekend_is_closed(self):
        cal, sat = self._cal(), self._past_horizon(5)
        assert not cal.is_trading_day(sat)
        assert cal.phase_at(self._at(sat, 10, 0)) == MarketPhase.CLOSED
        assert cal.previous_session(sat) == sat - timedelta(days=1)

    def test_the_published_range_still_ends_where_the_list_does(self):
        # The runtime clock marks a day past it "holidays unverified".
        last = calendar_range("XSHG")[1]
        wed = self._past_horizon(2)
        assert last < wed
        assert calendar_range("XSHG")[1] == last
        window = sessions_between("XSHG", last, wed)
        assert (window[0], window[-1]) == (last, wed)

    def test_a_window_anchored_on_a_projected_day_ends_on_it(self):
        """A caller takes ``latest`` from the calendar, then lists the days up to it."""
        cal, fri = self._cal(), self._past_horizon(4)
        latest = cal.expected_latest_daily_date(self._at(fri, 20, 0))
        assert latest == fri
        assert sessions_between("XSHG", latest - timedelta(days=10), latest)[-1] == latest

    @pytest.mark.parametrize("calendar_id", XCALS_IDS)
    def test_sessions_between_agrees_with_is_trading_day(self, calendar_id):
        """Day by day past the range end, and over a span straddling it."""
        cal, hi = get_calendar(calendar_id), calendar_range(calendar_id)[1]
        days = [hi + timedelta(days=i) for i in range(-10, 22)]
        for d in days[11:]:
            assert sessions_between(calendar_id, d, d) == ((d,) if cal.is_trading_day(d) else ()), d
        straddle = sessions_between(calendar_id, days[0], days[-1])
        assert straddle == tuple(d for d in days if cal.is_trading_day(d))
        assert straddle[0] <= hi < straddle[-1]

    @pytest.fixture
    def fresh_projection_state(self, monkeypatch):
        monkeypatch.setattr(calendars, "_PROJECTION_WARNED", set())
        calendars._session_bounds.cache_clear()
        yield
        calendars._session_bounds.cache_clear()

    def test_projection_warns_once_per_calendar(self, fresh_projection_state, caplog):
        caplog.set_level(logging.WARNING, logger=calendars.__name__)
        cal, wed = self._cal(), self._past_horizon(2, "XSHG", "XSES", "XBOM")
        cal.is_trading_day(calendar_range("XSHG")[1])  # published: no warning
        cal.is_trading_day(wed)
        cal.is_trading_day(wed + timedelta(days=1))
        get_calendar("XSES").is_trading_day(wed)
        # sessions_between projects through the same path, so it warns the same way.
        sessions_between("XSHG", wed, wed + timedelta(days=7))
        sessions_between("XBOM", wed, wed)
        warnings = [r.getMessage() for r in caplog.records if "calendar.projected" in r.getMessage()]
        assert len(warnings) == 3
        assert "calendar_id=XSHG" in warnings[0]
        assert f"last_session={calendar_range('XSHG')[1].isoformat()}" in warnings[0]
        assert f"date={wed.isoformat()}" in warnings[0]
        assert "calendar_id=XSES" in warnings[1]
        assert "calendar_id=XBOM" in warnings[2]

    def test_a_far_end_is_quick_and_leaves_the_session_cache_alone(self, fresh_projection_state):
        hi, end = calendar_range("XSHG")[1], date(2600, 1, 1)
        session_bounds("XNYS", "2026-07-02")  # a hot entry a day walk would evict
        before = calendars._session_bounds.cache_info()
        started = host_time.perf_counter()
        window = sessions_between("XSHG", hi - timedelta(days=7), end)
        assert host_time.perf_counter() - started < 1.0
        assert calendars._session_bounds.cache_info() == before
        assert window[-1] == end - timedelta(days=max(0, end.weekday() - 4))
        assert all(d.weekday() < 5 for d in window if d > hi)


class TestHandRolled:
    def test_crypto_sunday_regular(self):
        cal = Always24x7()
        sunday = datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)
        assert cal.phase_at(sunday) == MarketPhase.REGULAR
        assert cal.latest_trading_date(sunday) == date(2026, 7, 5)
        assert cal.seconds_until_next_open(sunday) == 0

    def test_fx_weekend_closed(self):
        cal = Weekdays24x5()
        saturday = datetime(2026, 7, 4, 12, 0, tzinfo=timezone.utc)
        assert cal.phase_at(saturday) == MarketPhase.CLOSED
        assert cal.latest_trading_date(saturday) == date(2026, 7, 3)
        monday = datetime(2026, 7, 6, 1, 0, tzinfo=timezone.utc)
        assert cal.phase_at(monday) == MarketPhase.REGULAR
        # Saturday noon → Monday 00:00 UTC is 36h.
        assert cal.seconds_until_next_open(saturday) == 36 * 3600

    def test_crypto_phase_never_changes(self):
        cal = Always24x7()
        assert cal.next_phase_change_ms(datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)) is None

    def test_fx_next_change_is_the_weekend_boundary(self):
        cal = Weekdays24x5()
        wednesday = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
        saturday_open = datetime(2026, 7, 4, 0, 0, tzinfo=timezone.utc)
        assert cal.next_phase_change_ms(wednesday) == int(saturday_open.timestamp() * 1000)
        sunday = datetime(2026, 7, 5, 10, 0, tzinfo=timezone.utc)
        monday_open = datetime(2026, 7, 6, 0, 0, tzinfo=timezone.utc)
        assert cal.next_phase_change_ms(sunday) == int(monday_open.timestamp() * 1000)


class TestCalendarIds:
    """The module functions answer every id ``get_calendar`` resolves and refuse the rest alike."""

    FRI, SAT, SUN, MON = (date(2026, 7, 3) + timedelta(days=i) for i in range(4))

    @staticmethod
    def _midnight_ms(d: date) -> int:
        return int(datetime.combine(d, time(0, 0), tzinfo=timezone.utc).timestamp() * 1000)

    def test_crypto_trades_every_day_midnight_to_midnight(self):
        assert session_bounds("ALWAYS_24_7", self.SUN.isoformat()) == (
            self._midnight_ms(self.SUN), self._midnight_ms(self.MON), None, None,
        )
        assert sessions_between("ALWAYS_24_7", self.FRI, self.MON) == (
            self.FRI, self.SAT, self.SUN, self.MON,
        )

    def test_fx_trades_weekdays_and_closes_the_weekend(self):
        assert session_bounds("WEEKDAYS_24_5", self.FRI.isoformat()) == (
            self._midnight_ms(self.FRI), self._midnight_ms(self.SAT), None, None,
        )
        assert session_bounds("WEEKDAYS_24_5", self.SAT.isoformat()) is None
        assert session_bounds("WEEKDAYS_24_5", self.SUN.isoformat()) is None
        assert sessions_between("WEEKDAYS_24_5", self.FRI, self.MON) == (self.FRI, self.MON)

    @pytest.mark.parametrize("calendar_id", HAND_ROLLED_IDS)
    def test_a_rule_agrees_with_its_calendar_and_projects_nothing(self, calendar_id):
        cal = get_calendar(calendar_id)
        days = [date(2026, 6, 29) + timedelta(days=i) for i in range(14)]
        assert sessions_between(calendar_id, days[0], days[-1]) == tuple(
            d for d in days if cal.is_trading_day(d)
        )
        for d in days:
            expected = (
                (cal.session_open_ms(d), cal.session_close_ms(d), None, None)
                if cal.is_trading_day(d) else None
            )
            assert session_bounds(calendar_id, d.isoformat()) == expected, d
        lo, hi = calendar_range(calendar_id)
        assert cal.is_trading_day(lo) and cal.is_trading_day(hi)
        assert lo < date(1900, 1, 1) and hi > date(2600, 1, 1)

    @pytest.mark.parametrize("calendar_id", ["XSHG", *HAND_ROLLED_IDS])
    def test_a_day_no_calendar_can_place_is_a_value_error(self, calendar_id):
        # A 24x7 session on date.max would close on a day that does not exist.
        eve = date.max - timedelta(days=1)
        assert session_bounds(calendar_id, eve.isoformat()) is not None
        assert sessions_between(calendar_id, eve - timedelta(days=6), eve)[-1] == eve
        with pytest.raises(ValueError):
            session_bounds(calendar_id, date.max.isoformat())
        with pytest.raises(ValueError):
            sessions_between(calendar_id, eve, date.max)

    @pytest.mark.parametrize(
        "call",
        [
            lambda c: get_calendar(c),
            lambda c: calendar_range(c),
            lambda c: session_bounds(c, "2026-07-02"),
            lambda c: sessions_between(c, date(2026, 7, 1), date(2026, 7, 2)),
        ],
        ids=["get_calendar", "calendar_range", "session_bounds", "sessions_between"],
    )
    # exchange_calendars knows XNAS and NYSE, but the venue table names neither,
    # and neither carries XNYS's extended hours.
    @pytest.mark.parametrize("calendar_id", ["XNOPE", "XNAS", "NYSE"])
    def test_an_unknown_id_is_a_value_error(self, call, calendar_id):
        with pytest.raises(ValueError, match=re.escape(calendar_id)):
            call(calendar_id)


class TestSessionWalks:
    @pytest.mark.parametrize("calendar_id", sorted(calendars._XCALS_IDS))
    def test_walks_span_every_published_gap(self, calendar_id):
        # A walk counts days from the session itself, so the longest gap must
        # fit with one day to spare, or the session past it is never reached.
        sessions = calendars._xcals(calendar_id).sessions
        longest = max((sessions[1:] - sessions[:-1]).days, default=0)
        assert longest < calendars._WALK_DAYS

    def test_walks_cross_a_17_day_closure(self):
        cal = get_calendar("XSHG")
        assert cal.previous_session(date(2000, 2, 14)) == date(2000, 1, 28)
        after_close = datetime(2000, 1, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        reopen = datetime(2000, 2, 14, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        assert cal.next_session_open_ms(after_close) == int(reopen.timestamp() * 1000)

    def test_previous_session_skips_holiday_and_weekend(self):
        # 2026-07-03 is the observed Independence Day holiday.
        assert get_calendar("XNYS").previous_session(date(2026, 7, 6)) == date(2026, 7, 2)
        assert Weekdays24x5().previous_session(date(2026, 7, 6)) == date(2026, 7, 3)
        assert Always24x7().previous_session(date(2026, 7, 6)) == date(2026, 7, 5)

    def test_previous_session_at_the_history_floor_is_refused(self):
        # A walk past the first session built would name a weekend.
        first, _ = calendar_range("XNYS")
        with pytest.raises(ValueError, match="XNYS"):
            get_calendar("XNYS").previous_session(first)

    def test_history_reaches_back_to_2000(self):
        # A floor raised for startup speed would turn real sessions into closed days.
        assert calendar_range("XNYS")[0] == date(2000, 1, 3)
        assert get_calendar("XSHG").is_trading_day(date(2015, 6, 1))

    def test_next_session_open_crosses_the_holiday(self):
        holiday_noon = datetime(2026, 7, 3, 12, 0, tzinfo=ET)
        monday_open = datetime(2026, 7, 6, 9, 30, tzinfo=ET)
        assert get_calendar("XNYS").next_session_open_ms(holiday_noon) == int(monday_open.timestamp() * 1000)

    def test_next_session_open_inside_a_session_is_the_next_day(self):
        cal = get_calendar("XHKG")
        lunch = datetime(2026, 7, 2, 12, 30, tzinfo=HKT)
        before_open = datetime(2026, 7, 2, 8, 0, tzinfo=HKT)
        today_open = cal.session_open_ms(date(2026, 7, 2))
        assert cal.next_session_open_ms(lunch) == cal.session_open_ms(date(2026, 7, 3))
        assert cal.next_session_open_ms(before_open) == today_open

    def test_next_session_open_for_the_hand_rolled_calendars(self):
        saturday = datetime(2026, 7, 4, 12, 0, tzinfo=timezone.utc)
        monday = datetime(2026, 7, 6, 0, 0, tzinfo=timezone.utc)
        sunday = datetime(2026, 7, 5, 0, 0, tzinfo=timezone.utc)
        assert Weekdays24x5().next_session_open_ms(saturday) == int(monday.timestamp() * 1000)
        assert Always24x7().next_session_open_ms(saturday) == int(sunday.timestamp() * 1000)


class TestNaiveDatetimes:
    """A naive datetime is UTC at every entry point, whatever the host's zone."""

    @pytest.fixture(autouse=True)
    def host_far_from_utc(self):
        if not hasattr(host_time, "tzset"):
            pytest.skip("needs time.tzset to move the host zone")
        saved = os.environ.get("TZ")
        os.environ["TZ"] = "Pacific/Kiritimati"  # UTC+14: a local reading moves the date
        host_time.tzset()
        yield
        if saved is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = saved
        host_time.tzset()

    INSTANTS = [
        datetime(2026, 7, 2, 13, 45),  # XNYS regular, XHKG closed
        datetime(2026, 7, 3, 4, 30),  # XHKG lunch, XNYS holiday
        datetime(2026, 7, 4, 23, 30),  # Saturday night UTC, Sunday local
    ]

    @pytest.mark.parametrize(
        "calendar", [get_calendar("XNYS"), get_calendar("XHKG"), Always24x7(), Weekdays24x5()],
        ids=lambda c: c.calendar_id,
    )
    def test_naive_agrees_with_utc(self, calendar):
        for naive in self.INSTANTS:
            aware = naive.replace(tzinfo=timezone.utc)
            for method in (
                "phase_at", "next_phase_change_ms", "latest_trading_date",
                "expected_latest_daily_date", "seconds_until_next_open", "next_session_open_ms",
            ):
                answer = getattr(calendar, method)
                assert answer(naive) == answer(aware), (method, naive.isoformat())


class TestBreakLookups:
    @pytest.fixture
    def xhkg_breaks_raising(self, monkeypatch):
        """XHKG with break lookups that raise *error*; the bounds cache is cleared around it."""
        real = calendars._xcals("XHKG")

        def install(error: Exception):
            class Calendar:
                def __getattr__(self, name):
                    return getattr(real, name)

                def session_break_start(self, *_args, **_kwargs):
                    raise error

            monkeypatch.setattr(calendars, "_xcals", lambda _calendar_id: Calendar())
            calendars._session_bounds.cache_clear()

        yield install
        calendars._session_bounds.cache_clear()

    def test_an_unexpected_error_is_not_swallowed(self, xhkg_breaks_raising):
        xhkg_breaks_raising(RuntimeError("bug"))
        with pytest.raises(RuntimeError):
            session_bounds("XHKG", "2026-07-02")

    def test_a_declared_lookup_error_means_no_break(self, xhkg_breaks_raising):
        xhkg_breaks_raising(NotSessionError(calendars._xcals("XHKG"), "2026-07-02", "session"))
        bounds = session_bounds("XHKG", "2026-07-02")
        assert bounds is not None and bounds[2:] == (None, None)


class TestRegistry:
    def test_prebuild_covers_all_mic_calendars(self):
        ids = default_calendar_ids()
        assert prebuild_calendars() == len(ids)
        for calendar_id in ids:
            assert get_calendar(calendar_id).calendar_id == calendar_id

    def test_prebuild_of_nothing_builds_nothing(self):
        assert prebuild_calendars(()) == 0
        assert prebuild_calendars(("XNYS",)) == 1

    def test_concurrent_first_use_builds_once(self, monkeypatch):
        # XKRX's construction corrupts itself when two threads run it at once.
        monkeypatch.setattr(calendars, "_xcals_built", {})
        threads = 16
        start = threading.Barrier(threads)

        def build(_):
            start.wait()
            return calendars._xcals("XKRX")

        with ThreadPoolExecutor(threads) as pool:
            built = list(pool.map(build, range(threads)))
        assert all(cal is built[0] for cal in built)

    def test_instances_are_cached(self):
        assert get_calendar("XNYS") is get_calendar("XNYS")
        assert get_calendar("ALWAYS_24_7") is get_calendar("ALWAYS_24_7")

    def test_session_bounds(self):
        cal = get_calendar("XNYS")
        open_ms = cal.session_open_ms(date(2026, 7, 2))
        close_ms = cal.session_close_ms(date(2026, 7, 2))
        assert open_ms is not None and close_ms is not None
        opened = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).astimezone(ET)
        closed = datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc).astimezone(ET)
        assert (opened.hour, opened.minute) == (9, 30)
        assert (closed.hour, closed.minute) == (16, 0)
        assert cal.session_open_ms(date(2026, 7, 4)) is None  # Saturday
