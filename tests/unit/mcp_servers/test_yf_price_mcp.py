"""Tests for yf_price_mcp_server tools.

Covers the standard envelope (symbol/interval/currency/timezone/count/data),
canonical interval mapping, partial-success semantics, and machine error codes.
yfinance is mocked — no live network.
"""

from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from mcp_servers.yf_price_mcp_server import (
    get_dividends_and_splits,
    get_multiple_stocks_dividends,
    get_multiple_stocks_history,
    get_stock_history,
)

from .conftest import assert_error, assert_ok_envelope


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_history_df():
    """OHLCV DataFrame with dividends and splits columns (tz-naive daily index)."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    return pd.DataFrame(
        {
            "Open": [150.0, 151.0, 152.0, 151.5, 153.0],
            "High": [152.0, 153.0, 154.0, 153.5, 155.0],
            "Low": [149.0, 150.0, 151.0, 150.5, 152.0],
            "Close": [151.0, 152.0, 153.0, 152.5, 154.0],
            "Volume": [1000000, 1100000, 1200000, 1050000, 1300000],
            "Dividends": [0.0, 0.0, 0.24, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0, 0.0, 0.0],
        },
        index=dates,
    )


@pytest.fixture
def mock_dividends_series():
    dates = pd.date_range("2023-01-15", periods=4, freq="QE")
    return pd.Series([0.24, 0.24, 0.25, 0.25], index=dates)


@pytest.fixture
def mock_splits_series():
    dates = pd.DatetimeIndex(["2020-08-31", "2014-06-09"])
    return pd.Series([4.0, 7.0], index=dates)


@pytest.fixture
def empty_series():
    return pd.Series([], dtype=float)


@pytest.fixture
def empty_df():
    return pd.DataFrame()


# ============================================================================
# get_stock_history
# ============================================================================


class TestGetStockHistory:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_history_df):
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("AAPL")

        assert_ok_envelope(
            result, source="yfinance", symbol="AAPL", currency="USD",
            interval="1day", count=5,  # canonical echo for yfinance "1d"
        )
        assert result["period"] == "1y"
        assert "timezone" not in result  # tz-naive index → omitted
        assert result["data"][0]["date"] == "2024-01-01"
        assert result["data"][0]["close"] == 151.0
        assert result["data"][2]["dividends"] == 0.24
        mock_ticker_cls.return_value.history.assert_called_once_with(
            period="1y", interval="1d"
        )

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_canonical_interval_input(self, mock_ticker_cls, mock_history_df):
        """Canonical vocab in → canonical echo out, mapped to yfinance spelling."""
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("AAPL", interval="1month")

        assert_ok_envelope(result, interval="1month")
        mock_ticker_cls.return_value.history.assert_called_once_with(
            period="1y", interval="1mo"
        )

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_custom_params(self, mock_ticker_cls, mock_history_df):
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("MSFT", period="6mo", interval="1wk")

        assert_ok_envelope(result, interval="1week")  # yfinance "1wk" → canonical
        assert result["period"] == "6mo"
        mock_ticker_cls.return_value.history.assert_called_once_with(
            period="6mo", interval="1wk"
        )

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_native_only_interval_passthrough(self, mock_ticker_cls, mock_history_df):
        """yfinance-native granularity (3mo) passes through and echoes natively."""
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("AAPL", interval="3mo")

        assert_ok_envelope(result, interval="3mo")
        mock_ticker_cls.return_value.history.assert_called_once_with(
            period="1y", interval="3mo"
        )

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_unsupported_interval(self, mock_ticker_cls):
        result = get_stock_history("AAPL", interval="4hour")

        assert_error(result, "unsupported_interval", symbol="AAPL")
        assert "supported" in result
        mock_ticker_cls.assert_not_called()

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_symbol_canonicalized(self, mock_ticker_cls, mock_history_df):
        """HK symbol echoes canonical display spelling and its currency."""
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("0700.hk")

        assert_ok_envelope(result, symbol="0700.HK", currency="HKD")
        mock_ticker_cls.assert_called_once_with("0700.HK")

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_intraday_includes_time_and_timezone(self, mock_ticker_cls):
        import pytz

        tz = pytz.timezone("America/New_York")
        dates = pd.date_range("2024-01-15 09:30", periods=3, freq="5min", tz=tz)
        df = pd.DataFrame(
            {
                "Open": [150.0, 150.5, 151.0],
                "High": [151.0, 151.5, 152.0],
                "Low": [149.0, 149.5, 150.0],
                "Close": [150.5, 151.0, 151.5],
                "Volume": [1000, 2000, 3000],
            },
            index=dates,
        )
        mock_ticker_cls.return_value.history.return_value = df
        result = get_stock_history("AAPL", period="1d", interval="5m")

        assert_ok_envelope(
            result, interval="5min", timezone="America/New_York", count=3,
        )
        bar_dates = [r["date"] for r in result["data"]]
        assert bar_dates == [
            "2024-01-15 09:30:00",
            "2024-01-15 09:35:00",
            "2024-01-15 09:40:00",
        ]

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_empty_data_is_not_found(self, mock_ticker_cls, empty_df):
        mock_ticker_cls.return_value.history.return_value = empty_df
        result = get_stock_history("INVALID")

        assert_error(result, "not_found", symbol="INVALID")

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_exception_is_sanitized_upstream_error(self, mock_ticker_cls):
        mock_ticker_cls.return_value.history.side_effect = Exception("Network error")
        result = get_stock_history("AAPL")

        # raw text not leaked
        assert_error(
            result, "upstream_error", symbol="AAPL",
            detail_excludes=("Network error",),
        )


# ============================================================================
# NaN bars (yfinance placeholder / in-progress session rows)
# ============================================================================


class TestNaNBarHandling:
    """yfinance appends a placeholder bar for an in-progress or dataless session
    whose OHLC is NaN (timing-dependent — this is what makes VOD.L history flaky
    around LSE hours). NaN is not valid JSON and a priceless bar is not a real
    observation, so such rows must be dropped before serialization."""

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_trailing_nan_bar_dropped(self, mock_ticker_cls):
        dates = pd.date_range("2024-01-01", periods=4, freq="D")
        df = pd.DataFrame(
            {
                "Open": [150.0, 151.0, 152.0, np.nan],
                "High": [152.0, 153.0, 154.0, np.nan],
                "Low": [149.0, 150.0, 151.0, np.nan],
                "Close": [151.0, 152.0, 153.0, np.nan],
                "Volume": [1000000, 1100000, 1200000, 0],
            },
            index=dates,
        )
        mock_ticker_cls.return_value.history.return_value = df
        result = get_stock_history("AAPL")

        assert_ok_envelope(result, count=3)  # placeholder bar dropped
        closes = [row["close"] for row in result["data"]]
        assert all(isinstance(c, float) and c == c for c in closes)  # no NaN
        assert result["data"][-1]["close"] == 153.0  # last surviving bar is real

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_all_nan_bars_is_not_found(self, mock_ticker_cls):
        dates = pd.date_range("2024-01-01", periods=2, freq="D")
        df = pd.DataFrame(
            {
                "Open": [np.nan, np.nan],
                "High": [np.nan, np.nan],
                "Low": [np.nan, np.nan],
                "Close": [np.nan, np.nan],
                "Volume": [0, 0],
            },
            index=dates,
        )
        mock_ticker_cls.return_value.history.return_value = df
        result = get_stock_history("AAPL")

        # Every bar dropped → same as an empty frame → not_found, never a crash.
        assert_error(result, "not_found", symbol="AAPL")

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_nan_volume_on_valid_price_bar_coerced(self, mock_ticker_cls):
        """A priced bar with a missing volume is kept (volume → 0), not dropped
        and not crashed on int(NaN)."""
        dates = pd.date_range("2024-01-01", periods=2, freq="D")
        df = pd.DataFrame(
            {
                "Open": [150.0, 151.0],
                "High": [152.0, 153.0],
                "Low": [149.0, 150.0],
                "Close": [151.0, 152.0],
                "Volume": [1000000, np.nan],
            },
            index=dates,
        )
        mock_ticker_cls.return_value.history.return_value = df
        result = get_stock_history("AAPL")

        assert_ok_envelope(result, count=2)
        assert result["data"][1]["close"] == 152.0
        assert result["data"][1]["volume"] == 0

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_multi_history_drops_nan_bars(self, mock_ticker_cls):
        dates = pd.date_range("2024-01-01", periods=3, freq="D")
        df = pd.DataFrame(
            {
                "Open": [150.0, 151.0, np.nan],
                "High": [152.0, 153.0, np.nan],
                "Low": [149.0, 150.0, np.nan],
                "Close": [151.0, 152.0, np.nan],
                "Volume": [1000000, 1100000, 0],
            },
            index=dates,
        )
        mock_ticker_cls.return_value.history.return_value = df
        result = get_multiple_stocks_history(["AAPL"])

        assert result["data"]["AAPL"]["count"] == 2
        assert result["count"] == 2
        closes = [row["close"] for row in result["data"]["AAPL"]["data"]]
        assert all(c == c for c in closes)  # no NaN


# ============================================================================
# get_multiple_stocks_history
# ============================================================================


class TestGetMultipleStocksHistory:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_history_df):
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_multiple_stocks_history(["AAPL", "MSFT"])

        assert_ok_envelope(result, source="yfinance", interval="1day", count=10)
        assert result["period"] == "1y"
        assert "AAPL" in result["data"]
        assert "MSFT" in result["data"]
        assert result["data"]["AAPL"]["count"] == 5
        assert result["data"]["AAPL"]["currency"] == "USD"
        assert len(result["data"]["AAPL"]["data"]) == 5
        assert "errors" not in result

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_partial_failure(self, mock_ticker_cls, mock_history_df):
        def side_effect(ticker):
            m = Mock()
            if ticker == "BAD":
                m.history.side_effect = Exception("Not found")
            else:
                m.history.return_value = mock_history_df
            return m

        mock_ticker_cls.side_effect = side_effect
        result = get_multiple_stocks_history(["AAPL", "BAD"])

        assert_ok_envelope(result, count=5)
        assert "AAPL" in result["data"]
        assert "BAD" not in result["data"]
        assert len(result["errors"]) == 1
        assert result["errors"][0]["error"] == "upstream_error"
        assert result["errors"][0]["symbol"] == "BAD"

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_unsupported_interval(self, mock_ticker_cls):
        result = get_multiple_stocks_history(["AAPL", "MSFT"], interval="4hour")

        assert_error(result, "unsupported_interval")
        assert "supported" in result
        mock_ticker_cls.assert_not_called()

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_all_empty(self, mock_ticker_cls, empty_df):
        mock_ticker_cls.return_value.history.return_value = empty_df
        result = get_multiple_stocks_history(["X", "Y"])

        assert_ok_envelope(result, count=0)
        assert result["data"]["X"]["count"] == 0
        assert result["data"]["Y"]["count"] == 0


# ============================================================================
# get_dividends_and_splits
# ============================================================================


class TestGetDividendsAndSplits:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_dividends_series, mock_splits_series):
        mock_obj = mock_ticker_cls.return_value
        mock_obj.dividends = mock_dividends_series
        mock_obj.splits = mock_splits_series
        result = get_dividends_and_splits("AAPL")

        assert_ok_envelope(
            result, source="yfinance", symbol="AAPL", currency="USD",
            count=6,  # total records across both lists
        )
        assert result["dividend_count"] == 4
        assert result["split_count"] == 2
        divs = result["data"]["dividends"]
        assert len(divs) == 4
        assert divs[0]["amount"] == 0.24
        assert "date" in divs[0]
        splits = result["data"]["splits"]
        assert len(splits) == 2
        assert splits[0]["ratio"] == 4.0

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls, empty_series):
        mock_obj = mock_ticker_cls.return_value
        mock_obj.dividends = empty_series
        mock_obj.splits = empty_series
        result = get_dividends_and_splits("NOCORP")

        assert_ok_envelope(result, count=0)
        assert result["dividend_count"] == 0
        assert result["split_count"] == 0
        assert result["data"]["dividends"] == []
        assert result["data"]["splits"] == []

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("API down")
        result = get_dividends_and_splits("AAPL")

        assert_error(
            result, "upstream_error", symbol="AAPL", detail_excludes=("API down",),
        )


# ============================================================================
# get_multiple_stocks_dividends
# ============================================================================


class TestGetMultipleStocksDividends:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_dividends_series):
        mock_ticker_cls.return_value.dividends = mock_dividends_series
        result = get_multiple_stocks_dividends(["AAPL", "MSFT"])

        assert_ok_envelope(result, source="yfinance", count=8)
        assert "AAPL" in result["data"]
        assert "MSFT" in result["data"]
        assert result["data"]["AAPL"]["count"] == 4
        assert result["data"]["AAPL"]["currency"] == "USD"
        assert "errors" not in result

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_partial_failure(self, mock_ticker_cls, mock_dividends_series):
        def side_effect(ticker):
            m = Mock()
            if ticker == "BAD":
                type(m).dividends = property(
                    lambda self: (_ for _ in ()).throw(Exception("No data"))
                )
            else:
                m.dividends = mock_dividends_series
            return m

        mock_ticker_cls.side_effect = side_effect
        result = get_multiple_stocks_dividends(["AAPL", "BAD"])

        assert_ok_envelope(result, count=4)
        assert "AAPL" in result["data"]
        assert "BAD" not in result["data"]
        assert len(result["errors"]) == 1
        assert result["errors"][0]["error"] == "upstream_error"
        assert result["errors"][0]["symbol"] == "BAD"

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_all_empty(self, mock_ticker_cls, empty_series):
        mock_ticker_cls.return_value.dividends = empty_series
        result = get_multiple_stocks_dividends(["X", "Y"])

        assert_ok_envelope(result, count=0)
        assert result["data"]["X"]["count"] == 0
        assert result["data"]["Y"]["count"] == 0


# ============================================================================
# Minor-unit conversion (keyed on yfinance's OWN declared currency)
# ============================================================================


@pytest.fixture
def pence_history_df():
    """OHLCV in pence (GBp scale), with a pence dividend on one bar."""
    dates = pd.date_range("2024-01-01", periods=2, freq="D")
    return pd.DataFrame(
        {
            "Open": [9850.0, 9900.0],
            "High": [9920.5, 9950.0],
            "Low": [9805.0, 9870.0],
            "Close": [9890.25, 9910.0],
            "Volume": [1000000, 1100000],
            "Dividends": [0.0, 50.0],
            "Stock Splits": [0.0, 0.0],
        },
        index=dates,
    )


class TestMinorUnitConversion:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_history_pence_converted_to_pounds(self, mock_ticker_cls, pence_history_df):
        stock = mock_ticker_cls.return_value
        stock.history.return_value = pence_history_df
        stock.history_metadata = {"currency": "GBp"}  # declared by yfinance

        result = get_stock_history("TEST.L")

        assert_ok_envelope(result, currency="GBP")  # major code, not GBp
        # pence → pounds (÷100), 4-decimal precision preserved
        assert result["data"][0]["close"] == 98.9025
        assert result["data"][0]["high"] == 99.205
        assert result["data"][1]["dividends"] == 0.5  # 50 pence → £0.50
        assert result["data"][0]["volume"] == 1000000  # volume NOT scaled

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_conversion_keyed_on_declared_not_symbol(
        self, mock_ticker_cls, mock_history_df
    ):
        """Declared GBp on a US-ref symbol still converts (not keyed on suffix)."""
        stock = mock_ticker_cls.return_value
        stock.history.return_value = mock_history_df
        stock.history_metadata = {"currency": "GBp"}

        result = get_stock_history("AAPL")  # ref currency is USD

        assert_ok_envelope(result, currency="GBP")
        assert result["data"][0]["close"] == 1.51  # 151.0 / 100

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_no_conversion_when_declared_major(self, mock_ticker_cls, mock_history_df):
        """A non-minor-unit declared currency leaves values untouched."""
        stock = mock_ticker_cls.return_value
        stock.history.return_value = mock_history_df
        stock.history_metadata = {"currency": "USD"}

        result = get_stock_history("AAPL")

        assert_ok_envelope(result, currency="USD")
        assert result["data"][0]["close"] == 151.0  # untouched, 2 decimals

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_fast_info_currency_fallback(self, mock_ticker_cls, mock_history_df):
        """When history_metadata is absent, fast_info.currency drives conversion."""
        stock = mock_ticker_cls.return_value
        stock.history.return_value = mock_history_df
        stock.history_metadata = None  # not a dict → fall back
        stock.fast_info = {"currency": "GBX"}  # case/alias variant

        result = get_stock_history("AAPL")

        assert_ok_envelope(result, currency="GBP")
        assert result["data"][0]["close"] == 1.51

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_multi_history_per_entry_conversion(self, mock_ticker_cls, pence_history_df):
        stock = mock_ticker_cls.return_value
        stock.history.return_value = pence_history_df
        stock.history_metadata = {"currency": "GBp"}

        result = get_multiple_stocks_history(["TEST.L"])

        entry = result["data"]["TEST.L"]
        assert entry["currency"] == "GBP"
        assert entry["data"][0]["close"] == 98.9025

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_dividends_pence_converted_splits_untouched(self, mock_ticker_cls):
        div_dates = pd.date_range("2023-01-15", periods=2, freq="QE")
        split_dates = pd.DatetimeIndex(["2020-08-31"])
        stock = mock_ticker_cls.return_value
        stock.dividends = pd.Series([50.0, 60.0], index=div_dates)  # pence
        stock.splits = pd.Series([4.0], index=split_dates)  # ratio
        stock.history_metadata = {"currency": "GBp"}

        result = get_dividends_and_splits("TEST.L")

        assert_ok_envelope(result, currency="GBP")
        assert result["data"]["dividends"][0]["amount"] == 0.5  # 50 pence → £0.50
        assert result["data"]["splits"][0]["ratio"] == 4.0  # ratio NOT scaled
