"""Measured freshness rules for served bars and quotes.

Pure: real venue calendars (XSHG for 600519.SS, XNYS for AAPL) against fixed
``now`` values, no cache, provider or network. 2026-09-09 is a Wednesday and a
session on both calendars.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from src.data_client.freshness import measure_bars, measure_daily, measure_quote

SH = ZoneInfo("Asia/Shanghai")
ET = ZoneInfo("America/New_York")

CN_AFTER_CLOSE = datetime(2026, 9, 9, 15, 30, tzinfo=SH)
US_MIDSESSION = datetime(2026, 9, 9, 15, 30, tzinfo=ET)
CN_MIDSESSION = datetime(2026, 9, 9, 10, 30, tzinfo=SH)


def _bar(dt: datetime) -> dict:
    return {"time": int(dt.timestamp() * 1000), "close": 1.0}


def _cn_bars(hour: int, minute: int) -> list[dict]:
    return [_bar(datetime(2026, 9, 9, hour, minute, tzinfo=SH))]


def _us_bars(hour: int, minute: int) -> list[dict]:
    return [_bar(datetime(2026, 9, 9, hour, minute, tzinfo=ET))]


class TestClosedSessionCompleteness:
    def test_cn_series_stopping_before_the_auction_is_incomplete(self):
        # Yahoo's CN 1m feed stops at 14:56 and drops the closing auction; the
        # shortfall is measured past the one-interval auction tolerance.
        f = measure_bars(
            "600519.SS", "1min", _cn_bars(14, 56), now=CN_AFTER_CLOSE, source="yfinance"
        )
        assert f.label == "incomplete"
        assert f.lag_s == 180
        assert f.measured is True
        assert f.source == "yfinance"
        assert f.interval == "1min"

    def test_the_auction_bar_reads_complete_under_either_convention(self):
        # Providers label the Shanghai closing auction 14:59 or 15:00; both are
        # a complete session.
        for hour, minute in ((14, 59), (15, 0)):
            f = measure_bars("600519.SS", "1min", _cn_bars(hour, minute), now=CN_AFTER_CLOSE)
            assert f.label == "live", (hour, minute)
            assert f.lag_s == 0

    def test_a_regular_session_series_stands_through_extended_hours(self):
        # FMP and Yahoo intraday stop at 16:00: a series ending on the 15:59 bar
        # is the whole session until the next open. A feed with extended-hours
        # bars is behind by then.
        last = _us_bars(15, 59)
        for now in (
            datetime(2026, 9, 9, 16, 30, tzinfo=ET),
            datetime(2026, 9, 9, 19, 59, tzinfo=ET),
            datetime(2026, 9, 10, 8, 0, tzinfo=ET),
        ):
            for source, tier in (("fmp", "realtime"), ("yfinance", "delayed_15m")):
                f = measure_bars("AAPL", "1min", last, now=now, source=source, tier=tier)
                assert (f.label, f.closed, f.lag_s) == ("live", True, 0), (source, now)
            extended = measure_bars("AAPL", "1min", last, now=now, source="ginlix-data")
            assert extended.label == "stale", now
        # The regular session still expects today's bars.
        later = measure_bars(
            "AAPL", "1min", last, now=datetime(2026, 9, 10, 11, 0, tzinfo=ET), source="fmp",
        )
        assert later.label == "stale"


class TestOpenSessionLag:
    def test_one_interval_behind_is_live(self):
        f = measure_bars("AAPL", "1min", _us_bars(15, 29), now=US_MIDSESSION, source="fmp")
        assert f.label == "live"
        assert f.lag_s == 60

    def test_twenty_minutes_behind_is_stale(self):
        f = measure_bars("AAPL", "1min", _us_bars(15, 10), now=US_MIDSESSION)
        assert f.label == "stale"
        assert f.lag_s == 1200

    def test_five_minutes_behind_is_delayed(self):
        f = measure_bars("AAPL", "1min", _us_bars(15, 25), now=US_MIDSESSION)
        assert f.label == "delayed"
        assert f.lag_s == 300

    def test_an_empty_series_measures_nothing(self):
        f = measure_bars("AAPL", "1min", [], now=US_MIDSESSION, source="fmp")
        assert f.label == "unknown"
        assert f.measured is False
        assert f.actual_latest is None


class TestQuotes:
    def test_a_print_at_the_close_is_live_after_the_close(self):
        as_of = int(datetime(2026, 9, 9, 15, 0, tzinfo=SH).timestamp() * 1000)
        f = measure_quote("600519.SS", as_of, "realtime", now=CN_AFTER_CLOSE, source="tushare")
        assert f.label == "live"
        assert f.measured is True
        assert f.lag_s == 0
        assert f.source == "tushare"

    def test_a_print_past_the_close_is_the_final_one(self):
        # A US after-hours trade, and Hong Kong's closing auction settling after
        # 16:00, both land past the regular-close anchor; neither is stale.
        cases = (
            ("AAPL", datetime(2026, 9, 11, 19, 59, 58, tzinfo=ET), datetime(2026, 9, 12, 12, 0, tzinfo=ET)),
            ("0700.HK", datetime(2026, 9, 9, 16, 8, tzinfo=ZoneInfo("Asia/Hong_Kong")),
             datetime(2026, 9, 9, 18, 0, tzinfo=ZoneInfo("Asia/Hong_Kong"))),
        )
        for symbol, printed, now in cases:
            f = measure_quote(symbol, int(printed.timestamp() * 1000), "realtime", now=now)
            assert (f.label, f.closed, f.lag_s) == ("live", True, 0), symbol

    def test_a_regular_session_print_stands_through_extended_hours(self):
        # FMP and Yahoo quote the regular session only: their 16:00 print is the
        # latest they will have until the next open, not an hour-old trade.
        close = int(datetime(2026, 9, 9, 16, 0, tzinfo=ET).timestamp() * 1000)
        for now in (
            datetime(2026, 9, 9, 17, 0, tzinfo=ET),
            datetime(2026, 9, 9, 19, 59, tzinfo=ET),
            datetime(2026, 9, 10, 8, 0, tzinfo=ET),
        ):
            f = measure_quote("AAPL", close, None, regular_only=True, now=now)
            assert (f.label, f.closed) == ("live", True), now
            assert measure_quote("AAPL", close, None, now=now).label == "stale", now
        # The regular session still expects today's print.
        f = measure_quote(
            "AAPL", close, None, regular_only=True, now=datetime(2026, 9, 10, 11, 0, tzinfo=ET),
        )
        assert f.label == "stale"

    def test_the_venue_clock_rides_every_quote(self):
        as_of = int(datetime(2026, 9, 9, 15, 29, 30, tzinfo=ET).timestamp() * 1000)
        assert measure_quote("AAPL", as_of, "realtime", now=US_MIDSESSION).closed is False
        # Unmeasured rows too: the web must not badge a closed venue realtime
        # just because no print time came back.
        assert measure_quote("AAPL", None, "realtime", now=CN_AFTER_CLOSE).closed is True
        assert measure_quote("", None, "realtime", now=US_MIDSESSION).closed is None

    def test_a_stale_print_on_a_closed_venue_is_stale(self):
        as_of = int(datetime(2026, 9, 9, 14, 30, tzinfo=SH).timestamp() * 1000)
        f = measure_quote("600519.SS", as_of, "realtime", now=CN_AFTER_CLOSE)
        assert f.label == "stale"
        assert f.lag_s == 1800

    def test_no_timestamp_degrades_to_the_declared_tier(self):
        f = measure_quote("0700.HK", None, "delayed_15m", now=US_MIDSESSION)
        assert f.label == "delayed"
        assert f.measured is False
        assert f.expected_latest is None

    def test_no_timestamp_and_no_tier_claims_nothing(self):
        f = measure_quote("AMD", None, None, now=US_MIDSESSION)
        assert f.label == "unknown"
        assert f.measured is False

    def test_an_eod_row_with_no_timestamp_reads_stale(self):
        f = measure_quote("920300.BJ", None, "eod", now=US_MIDSESSION, source="daily")
        assert f.label == "stale"
        assert f.measured is False

    def test_a_fresh_print_during_the_session_is_live(self):
        as_of = int(datetime(2026, 9, 9, 15, 29, 30, tzinfo=ET).timestamp() * 1000)
        f = measure_quote("AAPL", as_of, "realtime", now=US_MIDSESSION)
        assert f.label == "live"

    def test_the_morning_close_print_is_live_through_the_lunch_break(self):
        # Shanghai halts 11:30-13:00; the 11:30 print is the newest that can exist.
        as_of = int(datetime(2026, 9, 9, 11, 30, tzinfo=SH).timestamp() * 1000)
        f = measure_quote("600519.SS", as_of, "realtime", now=datetime(2026, 9, 9, 11, 45, tzinfo=SH))
        assert f.label == "live"
        assert f.lag_s == 0

    def test_a_print_from_before_the_morning_close_is_late_through_lunch(self):
        as_of = int(datetime(2026, 9, 9, 11, 20, tzinfo=SH).timestamp() * 1000)
        f = measure_quote("600519.SS", as_of, "realtime", now=datetime(2026, 9, 9, 11, 45, tzinfo=SH))
        assert f.label == "delayed"
        assert f.lag_s == 600

    def test_a_fifteen_minute_feed_measures_delayed_however_it_declares(self):
        as_of = int(datetime(2026, 9, 9, 15, 20, tzinfo=ET).timestamp() * 1000)
        f = measure_quote("AAPL", as_of, "realtime", now=US_MIDSESSION)
        assert f.label == "delayed"
        assert f.lag_s == 600


class TestDaily:
    def test_todays_bar_is_live(self):
        bars = [_bar(datetime(2026, 9, 9, 16, 0, tzinfo=ET))]
        f = measure_daily("AAPL", bars, now=US_MIDSESSION, source="fmp")
        assert f.label == "live"
        assert f.interval == "1day"

    def test_one_trading_day_behind_is_delayed(self):
        bars = [_bar(datetime(2026, 9, 8, 16, 0, tzinfo=ET))]
        assert measure_daily("AAPL", bars, now=US_MIDSESSION).label == "delayed"

    def test_several_trading_days_behind_is_stale(self):
        bars = [_bar(datetime(2026, 9, 3, 16, 0, tzinfo=ET))]
        assert measure_daily("AAPL", bars, now=US_MIDSESSION).label == "stale"

    def test_a_complete_series_says_whether_the_venue_is_closed(self):
        bars = _cn_bars(15, 0)
        assert measure_daily("600519.SS", bars, now=CN_AFTER_CLOSE).closed is True
        assert measure_daily("600519.SS", bars, now=CN_MIDSESSION).closed is False
        assert measure_bars("600519.SS", "1min", bars, now=CN_AFTER_CLOSE).closed is True

    def test_a_weekend_is_not_a_lag(self):
        # Friday's bar read in Monday's session: one trading day behind, not
        # three. Calendar-day arithmetic would call this stale.
        friday = [_bar(datetime(2026, 9, 11, 16, 0, tzinfo=ET))]
        monday = datetime(2026, 9, 14, 10, 0, tzinfo=ET)
        assert measure_daily("AAPL", friday, now=monday).label == "delayed"

    def test_an_empty_series_measures_nothing(self):
        f = measure_daily("AAPL", [], now=US_MIDSESSION)
        assert f.label == "unknown"
        assert f.measured is False


def test_a_feed_exactly_as_delayed_as_declared_reads_delayed_not_stale():
    """FMP outside the US: 15 min plus the bar's own period. Judged at
    10:30 Shanghai with the last 1-minute bar stamped 10:14, this is the
    feed doing what it sells and must not read stale."""
    from datetime import datetime, timezone, timedelta
    from src.data_client.freshness import measure_bars

    cst = timezone(timedelta(hours=8))
    now = datetime(2026, 9, 10, 10, 30, tzinfo=cst)
    last = int(datetime(2026, 9, 10, 10, 14, tzinfo=cst).timestamp() * 1000)
    f = measure_bars("600519.SS", "1min", [{"time": last}], now=now, source="fmp")
    assert f.label == "delayed" and f.lag_s == 960
    # Five-minute bars carry up to two extra periods of headroom as well.
    last5 = int(datetime(2026, 9, 10, 10, 10, tzinfo=cst).timestamp() * 1000)
    assert measure_bars("600519.SS", "5min", [{"time": last5}], now=now).label == "delayed"
    # Well past the declared delay is stale.
    old = int(datetime(2026, 9, 10, 9, 45, tzinfo=cst).timestamp() * 1000)
    assert measure_bars("600519.SS", "1min", [{"time": old}], now=now).label == "stale"


def test_a_minute_stamped_print_stays_live_through_the_whole_minute():
    """Tushare stamps the current minute bar: a quote seconds old can carry an
    as_of up to a minute behind the clock and must not flap to delayed."""
    from datetime import datetime, timezone, timedelta
    from src.data_client.freshness import measure_quote

    cst = timezone(timedelta(hours=8))
    as_of = int(datetime(2026, 9, 10, 10, 33, tzinfo=cst).timestamp() * 1000)
    for seconds in (5, 29, 59, 72, 110):
        now = datetime(2026, 9, 10, 10, 33, tzinfo=cst) + timedelta(seconds=seconds)
        assert measure_quote("600519.SS", as_of, "realtime", now=now).label == "live", seconds
    late = datetime(2026, 9, 10, 10, 36, tzinfo=cst)
    assert measure_quote("600519.SS", as_of, "realtime", now=late).label == "delayed"


class TestDeclaredTierBeatsABlindMeasurement:
    def test_a_four_hour_bar_cannot_prove_a_delayed_feed_live(self):
        f = measure_bars(
            "600519.SS", "4hour", _cn_bars(9, 30), now=CN_MIDSESSION,
            source="yfinance", tier="delayed_15m",
        )
        assert f.label == "delayed"
        assert f.measured is False
        assert f.actual_latest is not None

    def test_a_one_minute_bar_still_measures_a_delayed_feed(self):
        f = measure_bars(
            "600519.SS", "1min", _cn_bars(10, 29), now=CN_MIDSESSION,
            source="yfinance", tier="delayed_15m",
        )
        assert f.label == "live"
        assert f.measured is True

    def test_a_realtime_tier_is_never_demoted(self):
        f = measure_bars(
            "600519.SS", "4hour", _cn_bars(9, 30), now=CN_MIDSESSION,
            source="fmp", tier="realtime",
        )
        assert f.label == "live"
        assert f.measured is True


class TestNoExpectationIsNoMeasurement:
    def test_a_cn_quote_past_the_published_calendar_is_measured_against_a_session(self):
        # 2027-01-20 16:00 in Beijing, past the installed holiday list: the
        # venue still has a 15:00 close to hold the print against, so a print
        # three weeks old reads stale rather than realtime.
        now = datetime(2027, 1, 20, 8, 0, tzinfo=timezone.utc)
        as_of = int(datetime(2026, 12, 31, 15, 0, tzinfo=SH).timestamp() * 1000)
        for printed in (as_of, 2):
            f = measure_quote("920300.BJ", printed, None, now=now)
            assert (f.label, f.measured, f.closed) == ("stale", True, True), printed
            assert f.expected_latest == int(datetime(2027, 1, 20, 15, 0, tzinfo=SH).timestamp() * 1000)

    def test_a_clock_with_no_anchor_never_reads_live(self, monkeypatch):
        # A calendar with no session to expect a print from anchors at 0; a lag
        # against that is negative, which once read every print as live.
        clock = MagicMock()
        clock.expected_latest_bar_ms.return_value = 0
        clock.is_closed.return_value = True
        monkeypatch.setattr("src.data_client.freshness.clock_for", lambda *_a, **_k: clock)
        as_of = int(datetime(2026, 9, 9, 15, 0, tzinfo=SH).timestamp() * 1000)

        quote = measure_quote("600519.SS", as_of, "realtime", now=CN_AFTER_CLOSE)
        bars = measure_bars("600519.SS", "1min", _cn_bars(15, 0), now=CN_AFTER_CLOSE)
        for f in (quote, bars):
            assert (f.label, f.measured, f.expected_latest) == ("unknown", False, None)
            assert f.actual_latest == as_of
            assert f.closed is True

    def test_a_print_stamped_in_the_future_is_not_believed(self):
        # A stamp ahead of our clock gives a negative lag, which once read live.
        # A few seconds of host skew is still measured.
        ahead = int(US_MIDSESSION.timestamp() * 1000)
        f = measure_quote("AAPL", ahead + 5 * 60_000, "realtime", now=US_MIDSESSION)
        assert (f.label, f.measured, f.closed) == ("unknown", False, False)
        assert f.actual_latest == ahead + 5 * 60_000
        skewed = measure_quote("AAPL", ahead + 30_000, "realtime", now=US_MIDSESSION)
        assert (skewed.label, skewed.measured, skewed.lag_s) == ("live", True, 0)


    def test_a_bar_stamped_in_the_future_is_not_believed(self):
        # A bar is stamped at its open; one past now is mis-stamped (a time
        # zone read wrong), and its clamped lag of zero once read live.
        ahead = _us_bars(16, 30)  # an hour past US_MIDSESSION
        f = measure_bars("AAPL", "1min", ahead, now=US_MIDSESSION)
        assert (f.label, f.measured) == ("unknown", False)
        assert f.actual_latest == ahead[0]["time"]

    def test_a_daily_bar_for_a_date_not_yet_reached_is_not_believed(self):
        tomorrow = [_bar(datetime(2026, 9, 10, 16, 0, tzinfo=ET))]
        f = measure_daily("AAPL", tomorrow, now=US_MIDSESSION)
        assert (f.label, f.measured, f.interval) == ("unknown", False, "1day")


def test_a_delayed_feed_counts_its_delay_on_the_session_clock():
    """A feed declared 15 minutes delayed in Hong Kong: the lunch break and the
    night are not lag, and the quarter hour after the close is delay, not a
    short session. A realtime feed with nothing since the gap is stuck."""
    hk = ZoneInfo("Asia/Hong_Kong")

    def at(day: int, hour: int, minute: int) -> datetime:
        return datetime(2026, 9, day, hour, minute, tzinfo=hk)

    def bars(day: int, hour: int, minute: int, now: datetime, tier: str = "delayed_15m"):
        return measure_bars("0700.HK", "5min", [_bar(at(day, hour, minute))], now=now, tier=tier)

    # 13:10 shows 12:55, inside lunch: the morning's last bar is the newest.
    lunch = bars(9, 11, 55, at(9, 13, 10))
    assert (lunch.label, lunch.lag_s) == ("delayed", 1500)
    assert bars(9, 11, 55, at(9, 13, 10), tier="realtime").label == "stale"
    # 09:46 shows 09:31: no bar of today has completed yet.
    assert bars(8, 15, 55, at(9, 9, 46)).label == "delayed"
    # 16:05 shows 15:50; by 16:40 the same series is short of the close.
    after_close = bars(9, 15, 45, at(9, 16, 5))
    assert (after_close.label, after_close.lag_s) == ("delayed", 900)
    assert bars(9, 15, 45, at(9, 16, 40)).label == "incomplete"
    printed = int(at(9, 15, 50).timestamp() * 1000)
    quote = measure_quote("0700.HK", printed, "delayed_15m", now=at(9, 16, 5))
    assert (quote.label, quote.lag_s) == ("delayed", 600)
    assert measure_quote("0700.HK", printed, "delayed_15m", now=at(9, 18, 0)).label == "stale"
