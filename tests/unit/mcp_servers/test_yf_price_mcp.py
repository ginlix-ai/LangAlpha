"""Tests for yf_price_mcp_server tools.

Covers success, empty data, and exception paths for all four tools.
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from mcp_servers.yf_price_mcp_server import (
    get_dividends_and_splits,
    get_multiple_stocks_dividends,
    get_multiple_stocks_history,
    get_stock_history,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_history_df():
    """Mock OHLCV DataFrame with dividends and splits columns."""
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
    """Mock dividends Series."""
    dates = pd.date_range("2023-01-15", periods=4, freq="QE")
    return pd.Series([0.24, 0.24, 0.25, 0.25], index=dates)


@pytest.fixture
def mock_splits_series():
    """Mock stock splits Series."""
    dates = pd.DatetimeIndex(["2020-08-31", "2014-06-09"])
    return pd.Series([4.0, 7.0], index=dates)


@pytest.fixture
def empty_series():
    """Empty pandas Series."""
    return pd.Series([], dtype=float)


@pytest.fixture
def empty_df():
    """Empty DataFrame."""
    return pd.DataFrame()


# ============================================================================
# get_stock_history
# ============================================================================


class TestGetStockHistory:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_history_df):
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("AAPL")

        assert result["data_type"] == "stock_history"
        assert result["source"] == "yfinance"
        assert result["ticker"] == "AAPL"
        assert result["period"] == "1y"
        assert result["interval"] == "1d"
        assert result["count"] == 5
        assert len(result["data"]) == 5
        assert result["data"][0]["date"] == "2024-01-01"
        assert result["data"][0]["close"] == 151.0
        assert result["data"][2]["dividends"] == 0.24
        mock_ticker_cls.return_value.history.assert_called_once_with(
            period="1y", interval="1d"
        )

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_custom_params(self, mock_ticker_cls, mock_history_df):
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_stock_history("MSFT", period="6mo", interval="1wk")

        assert result["period"] == "6mo"
        assert result["interval"] == "1wk"
        mock_ticker_cls.return_value.history.assert_called_once_with(
            period="6mo", interval="1wk"
        )

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_intraday_includes_time(self, mock_ticker_cls):
        """Intraday bars use YYYY-MM-DD HH:MM:SS format (matches FMP convention)."""
        import pytz

        tz = pytz.timezone("America/New_York")
        dates = pd.date_range(
            "2024-01-15 09:30", periods=3, freq="5min", tz=tz
        )
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

        assert result["count"] == 3
        # Each bar should have unique timestamp, not just "2024-01-15"
        bar_dates = [r["date"] for r in result["data"]]
        assert len(set(bar_dates)) == 3  # All unique
        assert bar_dates[0] == "2024-01-15 09:30:00"
        assert bar_dates[1] == "2024-01-15 09:35:00"
        assert bar_dates[2] == "2024-01-15 09:40:00"

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_empty_data(self, mock_ticker_cls, empty_df):
        mock_ticker_cls.return_value.history.return_value = empty_df
        result = get_stock_history("INVALID")

        assert "error" in result
        assert "No data found" in result["error"]

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.return_value.history.side_effect = Exception("Network error")
        result = get_stock_history("AAPL")

        assert "error" in result
        assert "Network error" in result["error"]


# ============================================================================
# get_multiple_stocks_history
# ============================================================================


class TestGetMultipleStocksHistory:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_history_df):
        mock_ticker_cls.return_value.history.return_value = mock_history_df
        result = get_multiple_stocks_history(["AAPL", "MSFT"])

        assert result["data_type"] == "multiple_stocks_history"
        assert result["source"] == "yfinance"
        assert result["total_data_points"] == 10
        assert result["period"] == "1y"
        assert result["interval"] == "1d"
        assert "AAPL" in result["data"]
        assert "MSFT" in result["data"]
        assert result["data"]["AAPL"]["count"] == 5
        assert "errors" not in result

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_partial_failure(self, mock_ticker_cls, mock_history_df):
        def side_effect(ticker):
            mock = Mock()
            if ticker == "BAD":
                mock.history.side_effect = Exception("Not found")
            else:
                mock.history.return_value = mock_history_df
            return mock

        mock_ticker_cls.side_effect = side_effect
        result = get_multiple_stocks_history(["AAPL", "BAD"])

        assert "AAPL" in result["data"]
        assert "BAD" not in result["data"]
        assert result["total_data_points"] == 5
        assert len(result["errors"]) == 1
        assert result["errors"][0]["ticker"] == "BAD"

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_all_empty(self, mock_ticker_cls, empty_df):
        mock_ticker_cls.return_value.history.return_value = empty_df
        result = get_multiple_stocks_history(["X", "Y"])

        assert result["total_data_points"] == 0
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

        assert result["data_type"] == "dividends_and_splits"
        assert result["source"] == "yfinance"
        assert result["ticker"] == "AAPL"
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

        assert result["dividend_count"] == 0
        assert result["split_count"] == 0
        assert result["data"]["dividends"] == []
        assert result["data"]["splits"] == []

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.return_value.dividends = property(
            lambda self: (_ for _ in ()).throw(Exception("API down"))
        )
        mock_ticker_cls.side_effect = Exception("API down")
        result = get_dividends_and_splits("AAPL")

        assert "error" in result
        assert "API down" in result["error"]


# ============================================================================
# get_multiple_stocks_dividends
# ============================================================================


class TestGetMultipleStocksDividends:
    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_dividends_series):
        mock_ticker_cls.return_value.dividends = mock_dividends_series
        result = get_multiple_stocks_dividends(["AAPL", "MSFT"])

        assert result["data_type"] == "multiple_stocks_dividends"
        assert result["source"] == "yfinance"
        assert result["total_dividends"] == 8
        assert "AAPL" in result["data"]
        assert "MSFT" in result["data"]
        assert result["data"]["AAPL"]["count"] == 4
        assert "errors" not in result

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_partial_failure(self, mock_ticker_cls, mock_dividends_series):
        def side_effect(ticker):
            mock = Mock()
            if ticker == "BAD":
                type(mock).dividends = property(
                    lambda self: (_ for _ in ()).throw(Exception("No data"))
                )
            else:
                mock.dividends = mock_dividends_series
            return mock

        mock_ticker_cls.side_effect = side_effect
        result = get_multiple_stocks_dividends(["AAPL", "BAD"])

        assert "AAPL" in result["data"]
        assert "BAD" not in result["data"]
        assert result["total_dividends"] == 4
        assert len(result["errors"]) == 1
        assert result["errors"][0]["ticker"] == "BAD"

    @patch("mcp_servers.yf_price_mcp_server.yf.Ticker")
    def test_all_empty(self, mock_ticker_cls, empty_series):
        mock_ticker_cls.return_value.dividends = empty_series
        result = get_multiple_stocks_dividends(["X", "Y"])

        assert result["total_dividends"] == 0
        assert result["data"]["X"]["count"] == 0
        assert result["data"]["Y"]["count"] == 0
