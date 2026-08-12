"""Calendar spot checks + parity with the legacy US-only market_hours module.

XNYS must reproduce legacy phase/date answers exactly (it becomes the facade
behind market_hours in Phase 3); XHKG/24x7/24x5 cover what the legacy module
never could.
"""

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo


from src.market_protocol.calendars import (
    Always24x7,
    Weekdays24x5,
    get_calendar,
    prebuild_calendars,
)
from src.market_protocol.enums import MarketPhase
from src.utils import market_hours

ET = ZoneInfo("America/New_York")
HKT = ZoneInfo("Asia/Hong_Kong")

_LEGACY_TO_PHASE = {
    "pre": MarketPhase.PRE,
    "open": MarketPhase.REGULAR,
    "post": MarketPhase.POST,
    "closed": MarketPhase.CLOSED,
}


class TestXNYSLegacyParity:
    """The generalized calendar must agree with the hand-rolled ET module."""

    # Trading day (2026-07-02), half-ish coverage of all phase boundaries,
    # holiday (2026-07-03, Independence Day observed), weekend (2026-07-05).
    MATRIX = [
        datetime(2026, 7, 2, 3, 59, tzinfo=ET),
        datetime(2026, 7, 2, 4, 0, tzinfo=ET),
        datetime(2026, 7, 2, 9, 29, tzinfo=ET),
        datetime(2026, 7, 2, 9, 30, tzinfo=ET),
        datetime(2026, 7, 2, 12, 0, tzinfo=ET),
        datetime(2026, 7, 2, 15, 59, tzinfo=ET),
        datetime(2026, 7, 2, 16, 0, tzinfo=ET),
        datetime(2026, 7, 2, 19, 59, tzinfo=ET),
        datetime(2026, 7, 2, 20, 0, tzinfo=ET),
        datetime(2026, 7, 3, 10, 0, tzinfo=ET),
        datetime(2026, 7, 5, 12, 0, tzinfo=ET),
        datetime(2026, 1, 19, 12, 0, tzinfo=ET),   # MLK Day
        datetime(2026, 11, 26, 12, 0, tzinfo=ET),  # Thanksgiving
    ]

    def test_phase_parity(self):
        cal = get_calendar("XNYS")
        for at in self.MATRIX:
            legacy = market_hours.current_market_phase(at)
            assert cal.phase_at(at) == _LEGACY_TO_PHASE[legacy], at.isoformat()

    def test_current_trading_date_parity(self):
        cal = get_calendar("XNYS")
        for at in self.MATRIX:
            legacy = market_hours.current_trading_date(at)
            assert cal.latest_trading_date(at).isoformat() == legacy, at.isoformat()

    def test_expected_latest_daily_date_parity(self):
        cal = get_calendar("XNYS")
        for at in self.MATRIX:
            legacy = market_hours.expected_latest_daily_date(at)
            assert cal.expected_latest_daily_date(at).isoformat() == legacy, at.isoformat()

    def test_seconds_until_next_open_parity(self):
        cal = get_calendar("XNYS")
        for at in self.MATRIX:
            legacy = market_hours.seconds_until_next_open(at)
            assert abs(cal.seconds_until_next_open(at) - legacy) <= 1, at.isoformat()

    def test_next_phase_change_parity(self):
        # Full-day boundaries only — early-close divergence (13:00 vs the
        # hand-rolled 16:00) is a known deferred limitation of market_hours.
        cal = get_calendar("XNYS")
        for at in self.MATRIX:
            legacy = market_hours.next_phase_change_ms(at)
            assert cal.next_phase_change_ms(at) == legacy, at.isoformat()


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
        """2026-07-03: XNYS closed, XHKG open — the bug class behind #304."""
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


class TestRegistry:
    def test_prebuild_covers_all_mic_calendars(self):
        assert prebuild_calendars() >= 15

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
