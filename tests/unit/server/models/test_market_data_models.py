"""Legacy market-data wire models: nullable-volume coercion and currency carry."""

from src.server.models.market_data import (
    CompanyOverviewResponse,
    DailyResponse,
    IntradayDataPoint,
    IntradayResponse,
    SnapshotData,
)


class TestIntradayDataPointVolume:
    def test_null_volume_becomes_zero(self):
        # The protocol allows a null volume (index bars, Tushare rows with no
        # `vol`); the legacy wire is a plain int, so it must not 500.
        point = IntradayDataPoint(
            time=1, open=1.0, high=2.0, low=0.5, close=1.5, volume=None
        )
        assert point.volume == 0

    def test_float_volume_truncates(self):
        point = IntradayDataPoint(
            time=1, open=1.0, high=2.0, low=0.5, close=1.5, volume=1500.9
        )
        assert point.volume == 1500


class TestCurrencyFields:
    def test_snapshot_currency_optional_and_carried(self):
        assert SnapshotData(symbol="AAPL").currency is None
        assert SnapshotData(symbol="600519.SS", currency="CNY").currency == "CNY"

    def test_overview_currency_optional_and_carried(self):
        assert CompanyOverviewResponse(symbol="AAPL").currency is None
        assert (
            CompanyOverviewResponse(symbol="600519.SS", currency="CNY").currency == "CNY"
        )

    def test_bar_responses_carry_currency_timezone_treatment(self):
        cache = {"cached": False}
        intraday = IntradayResponse(
            symbol="600519.SS",
            interval="5min",
            currency="CNY",
            timezone="Asia/Shanghai",
            price_treatment="raw",
            cache=cache,
        )
        assert (intraday.currency, intraday.timezone, intraday.price_treatment) == (
            "CNY",
            "Asia/Shanghai",
            "raw",
        )
        daily = DailyResponse(symbol="AAPL", cache=cache)
        assert (daily.currency, daily.timezone, daily.price_treatment) == (
            None,
            None,
            None,
        )
