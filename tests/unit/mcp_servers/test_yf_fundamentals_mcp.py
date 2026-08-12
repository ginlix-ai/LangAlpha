"""Tests for yf_fundamentals_mcp_server — standard envelope + machine codes.

yfinance is mocked — no live network. Neutral placeholder symbols only.
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

from .conftest import assert_error, assert_ok_envelope


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
    return {
        "shortName": "Placeholder Inc.",
        "currency": "USD",
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
        assert_ok_envelope(result, source="yfinance", symbol="AAPL", count=3)  # metrics
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
    def test_empty_is_not_found(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.quarterly_income_stmt = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_income_statement("AAPL")
        assert_error(result, "not_found", symbol="AAPL")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception_is_sanitized(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("API error")
        result = get_income_statement("AAPL")
        assert_error(result, "upstream_error", detail_excludes=("API error",))


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
        assert_ok_envelope(result, symbol="AAPL")
        assert "Total Revenue" in result["data"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.quarterly_balance_sheet = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_balance_sheet("AAPL")
        assert_error(result, "not_found")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Network error")
        result = get_balance_sheet("AAPL")
        assert_error(result, "upstream_error")


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
        assert_ok_envelope(result, symbol="MSFT")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.quarterly_cashflow = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = get_cash_flow("MSFT")
        assert_error(result, "not_found")


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
        assert_ok_envelope(result, symbol="AAPL", currency="USD", count=1)
        assert result["data"]["shortName"] == "Placeholder Inc."
        # None values should be stripped
        assert "emptyField" not in result["data"]

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_empty_info(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.info = {}
        mock_ticker_cls.return_value = mock_stock

        result = get_company_info("AAPL")
        assert_error(result, "not_found")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_currency_omitted_when_yahoo_declares_none(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.info = {"shortName": "Placeholder Inc."}  # no currency field
        mock_ticker_cls.return_value = mock_stock

        result = get_company_info("AAPL")
        # No declared currency and no ref-currency guess → key omitted.
        assert "currency" not in result

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_declared_minor_unit_currency_label_preserved(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.info = {"shortName": "Placeholder Inc.", "currency": "GBp"}
        mock_ticker_cls.return_value = mock_stock

        result = get_company_info("TEST.L")
        # Values are Yahoo-native (unconverted); label honestly reflects the
        # declared minor unit rather than guessing a major code.
        assert_ok_envelope(result, currency="GBp")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Timeout")
        result = get_company_info("AAPL")
        assert_error(result, "upstream_error")


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
        assert_ok_envelope(result, symbol="AAPL", count=2)
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
        assert_error(result, "not_found")


# ============================================================================
# Earnings Data (uses earnings_history)
# ============================================================================


class TestGetEarningsData:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_earnings_history_df):
        mock_stock = Mock()
        mock_stock.earnings_history = mock_earnings_history_df
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_data("AAPL")
        assert_ok_envelope(result, symbol="AAPL", count=3)
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
        assert_error(result, "not_found")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_none(self, mock_ticker_cls):
        mock_stock = Mock()
        mock_stock.earnings_history = None
        mock_ticker_cls.return_value = mock_stock

        result = get_earnings_data("AAPL")
        assert_error(result, "not_found")

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("API down")
        result = get_earnings_data("AAPL")
        assert_error(result, "upstream_error", detail_excludes=("API down",))


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
        assert_ok_envelope(result, source="yfinance", count=6)  # 3 metrics x 2 tickers
        assert result["statement_type"] == "income"
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
        assert result["errors"][0]["error"] == "not_found"
        assert result["errors"][0]["symbol"] == "BAD"

    def test_invalid_statement_type(self):
        result = compare_financials(["AAPL"], statement_type="invalid")
        assert_error(result, "invalid_argument")
        assert "supported" in result


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
        assert_ok_envelope(result, source="yfinance", count=2)
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
        assert result["errors"][0]["error"] == "not_found"

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Timeout")
        result = compare_valuations(["AAPL"])
        assert result["errors"][0]["error"] == "upstream_error"


# ============================================================================
# Multiple Stocks Earnings (uses earnings_history)
# ============================================================================


class TestGetMultipleStocksEarnings:
    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_success(self, mock_ticker_cls, mock_earnings_history_df):
        mock_stock = Mock()
        mock_stock.earnings_history = mock_earnings_history_df
        mock_ticker_cls.return_value = mock_stock

        result = get_multiple_stocks_earnings(["AAPL", "MSFT"])
        assert_ok_envelope(result, source="yfinance", count=6)  # 3 records x 2 tickers
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
        assert result["errors"][0]["symbol"] == "BAD"

    @patch("mcp_servers.yf_fundamentals_mcp_server.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("Network error")
        result = get_multiple_stocks_earnings(["AAPL"])
        assert result["data"] == {}
        assert result["errors"][0]["error"] == "upstream_error"
