"""Per-instrument market clock — calendar-correct staleness primitives.

The CalendarClock cases pin the exact behaviors that fix the HK staleness
bug: bar expectations anchor to the XHKG session (incl. the lunch break),
not the US grid.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.data_client.freshness import measure_bars, measure_quote
from src.data_client.instrument_clock import CalendarClock, clock_for
from src.tools.market_data.quote_format import format_quote_line

ET = ZoneInfo("America/New_York")
HKT = ZoneInfo("Asia/Hong_Kong")


def _bar_dt(ms: int, tz: ZoneInfo) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=tz)


class TestUsCalendar:
    """US instruments run on the XNYS calendar."""

    def test_regular_session_floors_to_interval(self):
        now = datetime(2026, 4, 15, 10, 7, 30, tzinfo=ET)
        bar = _bar_dt(clock_for("AAPL").expected_latest_bar_ms("5min", now), ET)
        assert bar == datetime(2026, 4, 15, 10, 5, tzinfo=ET)

    def test_closed_anchors_to_the_last_close(self):
        # Saturday → Friday's 16:00 close; the dates walk back to Friday too.
        now = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        clock = clock_for("AAPL")
        bar = _bar_dt(clock.expected_latest_bar_ms("5min", now), ET)
        assert bar == datetime(2026, 4, 17, 16, 0, tzinfo=ET)
        assert clock.current_trading_date(now) == "2026-04-17"
        assert clock.expected_latest_daily_date(now) == "2026-04-17"

    def test_next_session_open_skips_the_current_session_and_holidays(self):
        """BRK.B resolves to the US clock; a once-per-day lock runs to the next 09:30 open."""
        clock = clock_for("BRK.B")
        # Wed 2026-11-25 19:00 (post-market) → Fri 09:30; Thanksgiving is skipped.
        now = datetime(2026, 11, 25, 19, 0, tzinfo=ET)
        assert clock.seconds_until_next_session_open(now) == int(38.5 * 3600)
        # 02:00 before a trading day's pre-market → that same morning's open.
        now = datetime(2026, 9, 24, 2, 0, tzinfo=ET)
        assert clock.seconds_until_next_session_open(now) == int(7.5 * 3600)

    def test_clock_cache_shares_instances(self):
        assert clock_for("AAPL") is clock_for("AAPL")
        clock = clock_for("MSFT")
        assert isinstance(clock, CalendarClock) and clock.tz == ET


class TestIndexReadsRegularSessionOnly:
    """An index is calculated only in the regular session, so XNYS extended
    hours read as closed for it, not as an open session with no prints.

    2026-10-01 is a Thursday session; 2026-11-26 is Thanksgiving and the
    Friday after closes at 13:00.
    """

    def setup_method(self):
        self.clock = clock_for("^GSPC", True)

    def _ms(self, *args) -> int:
        return int(datetime(*args, tzinfo=ET).timestamp() * 1000)

    def test_pre_market_reads_closed_on_the_previous_close(self):
        now = datetime(2026, 10, 1, 8, 0, tzinfo=ET)
        assert self.clock.market_phase(now) == "closed"
        assert self.clock.is_closed(now) is True
        assert self.clock.expected_latest_bar_ms("1min", now) == self._ms(2026, 9, 30, 16, 0)
        assert self.clock.seconds_until_next_open(now) == int(1.5 * 3600)
        assert self.clock.next_phase_change_ms(now) == self._ms(2026, 10, 1, 9, 30)
        # Day-level answers keep the calendar's: the trading date rolled at 04:00.
        assert self.clock.current_trading_date(now) == "2026-10-01"
        assert self.clock.expected_latest_daily_date(now) == "2026-09-30"

    def test_regular_session_is_unchanged(self):
        now = datetime(2026, 10, 1, 10, 0, tzinfo=ET)
        equity = clock_for("AAPL")
        for interval in ("1min", "5min", "1day"):
            assert self.clock.expected_latest_bar_ms(interval, now) == (
                equity.expected_latest_bar_ms(interval, now)
            )
        assert self.clock.market_phase(now) == "open"
        assert self.clock.seconds_until_next_open(now) == 0
        assert self.clock.next_phase_change_ms(now) == self._ms(2026, 10, 1, 16, 0)

    def test_after_hours_reads_closed_on_the_close(self):
        now = datetime(2026, 10, 1, 19, 0, tzinfo=ET)
        assert self.clock.market_phase(now) == "closed"
        assert self.clock.expected_latest_bar_ms("5min", now) == self._ms(2026, 10, 1, 16, 0)
        assert self.clock.seconds_until_next_open(now) == int(14.5 * 3600)
        assert self.clock.next_phase_change_ms(now) == self._ms(2026, 10, 2, 9, 30)

    def test_holiday_counts_down_to_the_regular_open(self):
        now = datetime(2026, 11, 26, 10, 0, tzinfo=ET)
        assert self.clock.market_phase(now) == "closed"
        assert self.clock.expected_latest_bar_ms("1min", now) == self._ms(2026, 11, 25, 16, 0)
        assert self.clock.seconds_until_next_open(now) == int(23.5 * 3600)
        assert self.clock.next_phase_change_ms(now) == self._ms(2026, 11, 27, 9, 30)

    def test_early_close_day(self):
        # The 13:00 close starts after-hours for an equity; the index is done.
        now = datetime(2026, 11, 27, 14, 0, tzinfo=ET)
        assert self.clock.market_phase(now) == "closed"
        assert self.clock.expected_latest_bar_ms("1min", now) == self._ms(2026, 11, 27, 13, 0)
        assert self.clock.seconds_until_next_open(now) == int(67.5 * 3600)
        assert self.clock.next_phase_change_ms(now) == self._ms(2026, 11, 30, 9, 30)
        early = datetime(2026, 11, 27, 12, 0, tzinfo=ET)
        assert self.clock.next_phase_change_ms(early) == self._ms(2026, 11, 27, 13, 0)

    def test_equity_and_default_clock_keep_extended_hours(self):
        now = datetime(2026, 10, 1, 19, 0, tzinfo=ET)
        for clock in (clock_for("AAPL"), clock_for(None)):
            assert clock.market_phase(now) == "post"
            assert clock.seconds_until_next_open(now) == 0
            assert clock.expected_latest_bar_ms("1min", now) == self._ms(2026, 10, 1, 19, 0)
            assert clock.next_phase_change_ms(now) == self._ms(2026, 10, 1, 20, 0)
        # Pre-market for an equity: open, with the night counted to 04:00.
        assert clock_for("AAPL").market_phase(datetime(2026, 10, 1, 8, 0, tzinfo=ET)) == "pre"
        night = datetime(2026, 10, 1, 2, 0, tzinfo=ET)
        assert clock_for("AAPL").seconds_until_next_open(night) == 2 * 3600
        assert self.clock.seconds_until_next_open(night) == int(7.5 * 3600)

    def test_bare_family_spelling_resolves_the_index_clock(self):
        # Snapshot rows echo the legacy "GSPC"; it autodetects as the index.
        now = datetime(2026, 10, 1, 19, 0, tzinfo=ET)
        assert clock_for("GSPC").market_phase(now) == "closed"

    def test_a_stock_endpoint_keeps_the_listing_clock_for_a_family_collision(self):
        # The stock COMP is keyed COMP.XNYS by the series cache; its clock must
        # read the same listing, so after-hours is a moving price, not the
        # Nasdaq Composite's settled close. Caret spellings stay the index.
        now = datetime(2026, 10, 1, 17, 30, tzinfo=ET)
        assert clock_for("COMP", False).market_phase(now) == "post"
        for is_index in (False, True):
            assert clock_for("^IXIC", is_index).market_phase(now) == "closed"
        printed = self._ms(2026, 10, 1, 17, 29)
        stock = measure_quote("COMP", printed, "realtime", now=now)
        assert (stock.label, stock.closed) == ("live", False)
        bars = [{"time": self._ms(2026, 10, 1, 17, 29), "close": 1.0}]
        assert measure_bars("COMP", "1min", bars, now=now).closed is False
        index = measure_quote("^IXIC", self._ms(2026, 10, 1, 16, 0), "realtime", now=now)
        assert (index.label, index.closed) == ("live", True)

    def test_after_hours_quote_and_bars_hold_the_final_print(self):
        # Through after-hours the 16:00 print is the session's last, not a
        # stale one, and the quote line says closed rather than post.
        now = datetime(2026, 10, 1, 19, 0, tzinfo=ET)
        as_of = self._ms(2026, 10, 1, 16, 0)
        f = measure_quote("GSPC", as_of, "realtime", is_index=True, now=now)
        assert (f.label, f.closed, f.lag_s) == ("live", True, 0)
        bars = [{"time": self._ms(2026, 10, 1, 15, 59), "close": 1.0}]
        assert measure_bars("^GSPC", "1min", bars, is_index=True, now=now).label == "live"


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
