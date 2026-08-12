"""Per-instrument market clock — calendar-correct staleness primitives.

The CalendarClock cases pin the exact behaviors that fix the HK staleness
bug: bar expectations anchor to the XHKG session (incl. the lunch break),
not the US grid.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.server.services.cache._instrument_clock import UsClock, clock_for
from src.utils import market_hours

ET = ZoneInfo("America/New_York")
HKT = ZoneInfo("Asia/Hong_Kong")


def _bar_dt(ms: int, tz: ZoneInfo) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=tz)


class TestUsClockParity:
    """UsClock must be a pure delegate — byte-identical to market_hours."""

    def test_delegates_expected_latest_bar(self):
        now = datetime(2026, 4, 15, 10, 7, 30, tzinfo=ET)
        clock = clock_for("AAPL")
        assert clock.expected_latest_bar_ms("5min", now) == market_hours.expected_latest_bar_ms("5min", now)

    def test_delegates_dates(self):
        now = datetime(2026, 4, 18, 10, 0, tzinfo=ET)  # Saturday
        clock = clock_for("AAPL")
        assert clock.current_trading_date(now) == market_hours.current_trading_date(now)
        assert clock.expected_latest_daily_date(now) == market_hours.expected_latest_daily_date(now)

    def test_clock_cache_shares_instances(self):
        assert clock_for("AAPL") is clock_for("AAPL")
        assert isinstance(clock_for("MSFT"), UsClock)


class TestXhkgClock:
    """0700.HK judged on the XHKG session — the core of the HK cache fix."""

    def setup_method(self):
        self.clock = clock_for("0700.HK")

    def test_regular_session_floors_to_interval(self):
        # Wed 2026-04-15 10:32 HKT — mid morning session.
        now = datetime(2026, 4, 15, 10, 32, tzinfo=HKT)
        assert self.clock.market_phase(now) == "open"
        expected = _bar_dt(self.clock.expected_latest_bar_ms("5min", now), HKT)
        assert (expected.hour, expected.minute) == (10, 30)

    def test_lunch_break_expects_no_new_bar(self):
        # 12:30 HKT is inside the XHKG lunch break — the newest bar that can
        # exist anchors at the morning-half close (12:00), NOT floor(now).
        # Without this, HK envelopes read stale every lunch and refetch-storm.
        now = datetime(2026, 4, 15, 12, 30, tzinfo=HKT)
        assert self.clock.market_phase(now) == "open"  # legacy string for LUNCH
        expected = _bar_dt(self.clock.expected_latest_bar_ms("5min", now), HKT)
        assert (expected.hour, expected.minute) == (12, 0)

    def test_evening_anchors_to_hk_close(self):
        # 19:00 HKT Wed — XHKG closed; expected bar anchors at 16:00 HKT close.
        # (Under the old US grid this moment was mid-US-session and HK bars
        # read hours stale — the never-cache-hit bug.)
        now = datetime(2026, 4, 15, 19, 0, tzinfo=HKT)
        assert self.clock.is_closed(now) is True
        expected = _bar_dt(self.clock.expected_latest_bar_ms("1hour", now), HKT)
        assert expected.date().isoformat() == "2026-04-15"
        assert (expected.hour, expected.minute) == (16, 0)

    def test_weekend_anchors_to_friday_close(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=HKT)  # Saturday
        expected = _bar_dt(self.clock.expected_latest_bar_ms("5min", now), HKT)
        assert expected.date().isoformat() == "2026-04-17"  # Friday
        assert (expected.hour, expected.minute) == (16, 0)

    def test_trading_dates_roll_on_hk_calendar(self):
        pre_open = datetime(2026, 4, 15, 8, 0, tzinfo=HKT)
        assert self.clock.current_trading_date(pre_open) == "2026-04-14"
        saturday = datetime(2026, 4, 18, 12, 0, tzinfo=HKT)
        assert self.clock.current_trading_date(saturday) == "2026-04-17"
        assert self.clock.expected_latest_daily_date(saturday) == "2026-04-17"

    def test_phase_diverges_from_us(self):
        # 11:00 ET == 23:00 HKT: US open, HK closed. One instant, two answers —
        # exactly what the single global phase could not express.
        us_midday = datetime(2026, 4, 15, 11, 0, tzinfo=ET)
        assert self.clock.is_closed(us_midday) is True
        assert clock_for("AAPL").is_closed(us_midday) is False

    def test_next_phase_change_diverges_from_us(self):
        # Mid HK morning session: HK's next boundary is the 12:00 lunch start;
        # the same instant on the US clock (22:32 ET Tue) points at the next
        # day's 04:00 pre-open.
        now = datetime(2026, 4, 15, 10, 32, tzinfo=HKT)
        hk_next = self.clock.next_phase_change_ms(now)
        assert _bar_dt(hk_next, HKT).strftime("%H:%M") == "12:00"
        us_next = clock_for("AAPL").next_phase_change_ms(now)
        assert _bar_dt(us_next, ET).strftime("%Y-%m-%d %H:%M") == "2026-04-15 04:00"

    def test_seconds_until_next_open_positive_when_closed(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=HKT)  # Saturday
        secs = self.clock.seconds_until_next_open(now)
        assert 0 < secs <= 3 * 24 * 3600


class TestHandRolledClocks:
    def test_crypto_never_closes(self):
        clock = clock_for("BTC-USD.CRYPTO")
        sunday = datetime(2026, 4, 19, 3, 0, tzinfo=ZoneInfo("UTC"))
        assert clock.market_phase(sunday) == "open"
        assert clock.is_closed(sunday) is False
        assert clock.seconds_until_next_open(sunday) == 0
        expected = clock.expected_latest_bar_ms("5min", sunday)
        assert expected == int(sunday.timestamp()) * 1000  # floor of exact 5min edge

    def test_fx_closed_on_weekend(self):
        clock = clock_for("EUR-USD.FX")
        saturday = datetime(2026, 4, 18, 12, 0, tzinfo=ZoneInfo("UTC"))
        assert clock.is_closed(saturday) is True
        monday = datetime(2026, 4, 20, 12, 0, tzinfo=ZoneInfo("UTC"))
        assert clock.is_closed(monday) is False
