"""Tests for yf_fundamentals_mcp_server tools.

Tests all tools using mocked yfinance responses: success, empty data, and exceptions.
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from mcp_servers.yf_fundamentals_mcp_server import (
    compare_financials,
    compare_valuations,
    get_balance_sheet,
    get_cash_flow,
    get_company_info,
    get_earnings_data,
    get_earnings_dates,
    get_income_statement,
    get_multiple_stocks_earnings,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_financial_df():
    """Financial statement DataFrame: metrics as rows, dates as columns."""
    dates = pd.DatetimeIndex(["2024-03-31", "2023-12-31"])
    return pd.DataFrame(
        {
            dates[0]: [1000000, 500000, 300000],
            dates[1]: [900000, 450000, 270000],
        },
        index=["Total Revenue", "Gross Profit", "Net Income"],
    )


@pytest.fixture
def mock_info():
    """Company info dict."""
    return {
        "shortName": "Apple Inc.",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "marketCap": 3000000000000,
        "trailingPE": 30.5,
        "forwardPE": 28.0,
        "priceToBook": 45.0,
        "currentPrice": 195.0,
        "beta": 1.2,
        "fiftyTwoWeekHigh": 200.0,
        "fiftyTwoWeekLow": 150.0,
        "dividendYield": 0.005,
        "emptyField": None,
    }


@pytest.fixture
def mock_earnings_dates_df():
    """Earnings dates DataFrame with EPS columns."""
    dates = pd.DatetimeIndex(["2024-04-25", "2024-01-25"])
    return pd.DataFrame(
        {
            "EPS Estimate": [1.50, 1.45],
            "Reported EPS": [1.55, 1.48],
            "Surprise(%)": [3.33, 2.07],
        },
        index=dates,
    )


@pytest.fixture
def mock_earnings_history_df():
    """earnings_history DataFrame: quarter dates with EPS estimate/actual."""
    dates = pd.DatetimeIndex(["2024-03-31", "2023-12-31", "2023-09-30"])
    return pd.DataFrame(
        {
            "epsEstimate": [1.50, 1.45, 1.40],
            "epsActual": [1.55, 1.48, 1.42],
            "epsDifference": [0.05, 0.03, 0.02],
            "surprisePercent": [3.33, 2.07, 1.43],
        },
        index=dates,
    )


# ============================================================================
# Income Statement
# ============================================================================


class TestGetIncomeStatement:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success_quarterly(self, mock_ticker_cls, mock_financial_df):
        mock_stock = Mock()
        mock_stock.quarterly_income_stmt = mock_financial_df
        mock_ticker_cls.return_value = mock_stock

        result = get_income_statement("AAPL", quarterly=True)
        assert result["data_type"] == "income_statement"
        assert result["source"] == "yfinance"
        assert result["ticker"] == "AAPL"
        assert result["quarterly"] is True
        assert "Total Revenue" in result["data"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success_annual(self, mock_ticker_cls, mock_financial_df):
        mock_stock = Mock()
        mock_stock.income_stmt = mock_financial_df
        mock_ticker_cls.return_value = mock_stock

        result = get_income_statement("AAPL", quarterly=False)
        assert result["quarterly"] is False
        assert "Total Revenue" in result["data"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.quarterly_income_stmt = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_income_statement("AAPL")
        assert "error" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("API error")
        result = get_income_statement("AAPL")
        assert "error" in result
        assert "API error" in result["error"]


# ============================================================================
# Balance Sheet
# ============================================================================


class TestGetBalanceSheet:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_financial_df):
        mock_stock = Mock()
        mock_stock.quarterly_balance_sheet = mock_financial_df
        mock_ticker_cls.return_value = mock_stock

        result = get_balance_sheet("AAPL")
        assert result["data_type"] == "balance_sheet"
        assert result["ticker"] == "AAPL"
        assert "Total Revenue" in result["data"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.quarterly_balance_sheet = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_balance_sheet("AAPL")
        assert "error" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Network error")
        result = get_balance_sheet("AAPL")
        assert "error" in result


# ============================================================================
# Cash Flow
# ============================================================================


class TestGetCashFlow:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_financial_df):
        mock_stock = Mock()
        mock_stock.quarterly_cashflow = mock_financial_df
        mock_ticker_cls.return_value = mock_stock

        result = get_cash_flow("MSFT")
        assert result["data_type"] == "cash_flow"
        assert result["ticker"] == "MSFT"

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.quarterly_cashflow = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_cash_flow("MSFT")
        assert "error" in result


# ============================================================================
# Company Info
# ============================================================================


class TestGetCompanyInfo:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_info):
        mock_stock = Mock()
        mock_stock.info = mock_info
        mock_ticker_cls.return_value = mock_stock

        result = get_company_info("AAPL")
        assert result["data_type"] == "company_info"
        assert result["data"]["shortName"] == "Apple Inc."
        # None values should be stripped
        assert "emptyField" not in result["data"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty_info(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.info = {}
        mock_ticker_cls.return_value = mock_stock

        result = get_company_info("AAPL")
        assert "error" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Timeout")
        result = get_company_info("AAPL")
        assert "error" in result


# ============================================================================
# Earnings Dates
# ============================================================================


class TestGetEarningsDates:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_earnings_dates_df):
        mock_stock = Mock()
        mock_stock.earnings_dates = mock_earnings_dates_df
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_dates("AAPL")
        assert result["data_type"] == "earnings_dates"
        assert result["count"] == 2
        record = result["data"][0]
        assert "eps_estimate" in record
        assert "reported_eps" in record
        assert "surprise_pct" in record

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.earnings_dates = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_dates("AAPL")
        assert "error" in result


# ============================================================================
# Earnings Data (FIXED: uses earnings_history, not quarterly_earnings)
# ============================================================================


class TestGetEarningsData:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_earnings_history_df):
        mock_stock = Mock()
        mock_stock.earnings_history = mock_earnings_history_df
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_data("AAPL")
        assert result["data_type"] == "earnings_data"
        assert result["count"] == 3
        record = result["data"][0]
        assert "epsestimate" in record
        assert "epsactual" in record
        assert "epsdifference" in record
        assert "surprisepercent" in record

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.earnings_history = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_data("AAPL")
        assert "error" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_none(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.earnings_history = None
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_data("AAPL")
        assert "error" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("API down")
        result = get_earnings_data("AAPL")
        assert "error" in result
        assert "API down" in result["error"]


# ============================================================================
# Compare Financials
# ============================================================================


class TestCompareFinancials:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_financial_df):
        mock_stock = Mock()
        mock_stock.quarterly_income_stmt = mock_financial_df
        mock_ticker_cls.return_value = mock_stock

        result = compare_financials(["AAPL", "MSFT"])
        assert result["data_type"] == "compare_financials"
        assert "AAPL" in result["data"]
        assert "MSFT" in result["data"]
        assert result["successful_tickers"] == ["AAPL", "MSFT"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_partial_failure(self, mock_ticker_cls, mock_financial_df):
        def side_effect(ticker):
            mock_stock = Mock()
            if ticker == "AAPL":
                mock_stock.quarterly_income_stmt = mock_financial_df
            else:
                mock_stock.quarterly_income_stmt = pd.DataFrame()
            return mock_stock

        mock_ticker_cls.side_effect = side_effect

        result = compare_financials(["AAPL", "BAD"])
        assert "AAPL" in result["data"]
        assert "BAD" not in result["data"]
        assert "errors" in result

    def test_invalid_statement_type(self):
        result = compare_financials(["AAPL"], statement_type="invalid")
        assert "error" in result
        assert "Invalid statement_type" in result["error"]


# ============================================================================
# Compare Valuations
# ============================================================================


class TestCompareValuations:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_info):
        mock_stock = Mock()
        mock_stock.info = mock_info
        mock_ticker_cls.return_value = mock_stock

        result = compare_valuations(["AAPL", "MSFT"])
        assert result["data_type"] == "compare_valuations"
        assert "AAPL" in result["data"]
        assert "trailing_p_e" in result["data"]["AAPL"]
        assert result["data"]["AAPL"]["current_price"] == 195.0

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty_info(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.info = {}
        mock_ticker_cls.return_value = mock_stock

        result = compare_valuations(["AAPL"])
        assert result["data"] == {}
        assert "errors" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Timeout")
        result = compare_valuations(["AAPL"])
        assert "errors" in result


# ============================================================================
# Multiple Stocks Earnings (FIXED: uses earnings_history)
# ============================================================================


class TestGetMultipleStocksEarnings:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_earnings_history_df):
        mock_stock = Mock()
        mock_stock.earnings_history = mock_earnings_history_df
        mock_ticker_cls.return_value = mock_stock

        result = get_multiple_stocks_earnings(["AAPL", "MSFT"])
        assert result["data_type"] == "multiple_stocks_earnings"
        assert "AAPL" in result["data"]
        assert "MSFT" in result["data"]
        assert result["data"]["AAPL"]["count"] == 3
        record = result["data"]["AAPL"]["earnings"][0]
        assert "epsestimate" in record
        assert "epsactual" in record

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_partial_failure(self, mock_ticker_cls, mock_earnings_history_df):
        def side_effect(ticker):
            mock_stock = Mock()
            if ticker == "AAPL":
                mock_stock.earnings_history = mock_earnings_history_df
            else:
                mock_stock.earnings_history = pd.DataFrame()
            return mock_stock

        mock_ticker_cls.side_effect = side_effect

        result = get_multiple_stocks_earnings(["AAPL", "BAD"])
        assert "AAPL" in result["data"]
        assert "BAD" not in result["data"]
        assert "errors" in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Network error")
        result = get_multiple_stocks_earnings(["AAPL"])
        assert "errors" in result
        assert result["data"] == {}
