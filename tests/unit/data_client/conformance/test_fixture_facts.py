"""Locks what the live captures show — the evidence base for Phase 1.

These must stay green: they assert facts about the *fixtures*, not about the
code. If a fixture is re-captured and one of these breaks, upstream behavior
changed and the paired normalizer logic must be revisited.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.market_protocol.calendars import get_calendar
from src.market_protocol.enums import MarketPhase

HKT = ZoneInfo("Asia/Hong_Kong")
ET = ZoneInfo("America/New_York")


class TestFmpHkIntradayRaw:
    def test_dates_are_hkt_wall_clock(self, fmp_hk_raw):
        """FMP stamps bars with HKT wall-clock strings and no timezone —
        read as HKT they all land inside the XHKG session grid."""
        cal = get_calendar("XHKG")
        for row in fmp_hk_raw["data"]:
            local = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=HKT)
            assert cal.phase_at(local) != MarketPhase.CLOSED, row["date"]

    def test_read_as_et_lands_outside_session(self, fmp_hk_raw):
        """The legacy normalizer's tzinfo=ET read shifts every bar out of the
        HK session — the bug the Phase 1 normalizer kills."""
        cal = get_calendar("XHKG")
        misplaced = 0
        for row in fmp_hk_raw["data"]:
            as_et = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=ET)
            if cal.phase_at(as_et) == MarketPhase.CLOSED:
                misplaced += 1
        assert misplaced == len(fmp_hk_raw["data"])

    def test_rows_are_descending(self, fmp_hk_raw):
        dates = [row["date"] for row in fmp_hk_raw["data"]]
        assert dates == sorted(dates, reverse=True)


class TestYfinanceHkIntraday:
    def test_epochs_land_inside_xhkg_sessions(self, yf_hk_1h):
        """yfinance is the tz-correct reference: every bar anchor falls inside
        an XHKG session (regular or lunch) per our calendar — this also
        validates XcalsCalendar against real exchange data."""
        cal = get_calendar("XHKG")
        for row in yf_hk_1h["data"]:
            at = datetime.fromtimestamp(row["ts_utc_ms"] / 1000, tz=timezone.utc)
            assert cal.phase_at(at) != MarketPhase.CLOSED, row["iso"]

    def test_index_is_hkt(self, yf_hk_1h):
        assert all(row["iso"].endswith("+08:00") for row in yf_hk_1h["data"])

    def test_rows_are_ascending(self, yf_hk_1h):
        times = [row["ts_utc_ms"] for row in yf_hk_1h["data"]]
        assert times == sorted(times)


class TestVodlPenceScale:
    """VOD.L arrives in GBp from both providers (~100, i.e. ~£1)."""

    def test_fmp_quote_is_pence(self, fmp_quotes_raw):
        vod = next(r for r in fmp_quotes_raw["data"] if r["symbol"] == "VOD.L")
        assert vod["price"] > 10

    def test_fmp_daily_is_pence(self, fmp_vodl_daily_raw):
        assert fmp_vodl_daily_raw["data"][0]["close"] > 10

    def test_yfinance_daily_is_pence(self, yf_vodl_adjusted):
        assert yf_vodl_adjusted["data"][-1]["close"] > 10


class TestYfinanceAdjustmentPair:
    def test_raw_capture_carries_corporate_actions(self, yf_vodl_raw):
        """auto_adjust=False exposes dividends/splits columns — the evidence
        that yfinance default (True) is dividend-adjusted, unlike FMP/Polygon."""
        assert any("dividends" in row for row in yf_vodl_raw["data"])

    def test_paired_windows_align(self, yf_vodl_adjusted, yf_vodl_raw):
        adjusted = {row["ts_utc_ms"] for row in yf_vodl_adjusted["data"]}
        raw = {row["ts_utc_ms"] for row in yf_vodl_raw["data"]}
        assert adjusted & raw, "adjusted/raw captures share no timestamps"


class TestFmpQuotes:
    def test_batch_covers_all_regimes(self, fmp_quotes_raw):
        symbols = {r["symbol"] for r in fmp_quotes_raw["data"]}
        assert {"AAPL", "VOD.L", "0700.HK"} <= symbols

    def test_daily_rows_are_date_only(self, fmp_vodl_daily_raw):
        assert all(" " not in row["date"] for row in fmp_vodl_daily_raw["data"])
