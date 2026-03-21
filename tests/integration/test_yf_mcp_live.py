"""Integration tests for yfinance MCP servers — hits real Yahoo Finance API.

Run with:  uv run pytest tests/integration/test_yf_mcp_live.py -m integration -v
Requires:  yfinance library (production dependency, always available)

Tests all 4 yfinance MCP servers against live data to verify:
- API response structure matches what our tools expect
- Fixed tools (get_news, get_earnings_data) work with yfinance 1.2.0
- New tools return valid data
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

try:
    import yfinance as yf  # noqa: F401

    _has_yfinance = True
except ImportError:
    _has_yfinance = False

_SYMBOL = "AAPL"
_skip = pytest.mark.skipif(not _has_yfinance, reason="yfinance not installed")


# ===========================================================================
# yf_price_mcp_server
# ===========================================================================


@_skip
class TestYfPriceLive:
    """Live tests for yf_price_mcp_server."""

    def test_get_stock_history(self):
        from mcp_servers.yf_price_mcp_server import get_stock_history

        result = get_stock_history(_SYMBOL, period="5d", interval="1d")
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "stock_history"
        assert result["source"] == "yfinance"
        assert result["count"] > 0
        row = result["data"][0]
        assert all(k in row for k in ("date", "open", "high", "low", "close", "volume"))
        assert row["close"] > 0

    def test_get_stock_history_intraday(self):
        from mcp_servers.yf_price_mcp_server import get_stock_history

        result = get_stock_history(_SYMBOL, period="1d", interval="5m")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    def test_get_multiple_stocks_history(self):
        from mcp_servers.yf_price_mcp_server import get_multiple_stocks_history

        result = get_multiple_stocks_history([_SYMBOL, "MSFT"], period="5d")
        assert "error" not in result
        assert result["data_type"] == "multiple_stocks_history"
        assert _SYMBOL in result["data"]
        assert "MSFT" in result["data"]
        assert result["total_data_points"] > 0

    def test_get_dividends_and_splits(self):
        from mcp_servers.yf_price_mcp_server import get_dividends_and_splits

        result = get_dividends_and_splits(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "dividends_and_splits"
        assert result["dividend_count"] > 0
        div = result["data"]["dividends"][0]
        assert "date" in div
        assert "amount" in div
        assert div["amount"] > 0

    def test_get_multiple_stocks_dividends(self):
        from mcp_servers.yf_price_mcp_server import get_multiple_stocks_dividends

        result = get_multiple_stocks_dividends([_SYMBOL, "MSFT"])
        assert "error" not in result
        assert result["total_dividends"] > 0


# ===========================================================================
# yf_fundamentals_mcp_server
# ===========================================================================


@_skip
class TestYfFundamentalsLive:
    """Live tests for yf_fundamentals_mcp_server."""

    def test_get_income_statement_quarterly(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_income_statement

        result = get_income_statement(_SYMBOL, quarterly=True)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "income_statement"
        assert result["source"] == "yfinance"
        data = result["data"]
        assert len(data) > 0
        # Should have common financial metrics
        keys = set(data.keys())
        assert keys & {"Total Revenue", "Net Income", "Gross Profit"}

    def test_get_income_statement_annual(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_income_statement

        result = get_income_statement(_SYMBOL, quarterly=False)
        assert "error" not in result, result.get("error")
        assert len(result["data"]) > 0

    def test_get_balance_sheet(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_balance_sheet

        result = get_balance_sheet(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "balance_sheet"
        assert len(result["data"]) > 0

    def test_get_cash_flow(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_cash_flow

        result = get_cash_flow(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "cash_flow"
        assert len(result["data"]) > 0

    def test_get_company_info(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_company_info

        result = get_company_info(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "company_info"
        info = result["data"]
        assert info.get("shortName") or info.get("longName")
        assert info.get("sector")
        assert info.get("marketCap", 0) > 0

    def test_get_earnings_dates(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_earnings_dates

        result = get_earnings_dates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "earnings_dates"
        assert result["count"] > 0
        record = result["data"][0]
        # Check expected columns exist (lowercased/cleaned)
        keys = set(record.keys())
        assert "eps_estimate" in keys or "reported_eps" in keys

    def test_get_earnings_data_fixed(self):
        """Verify the FIXED earnings tool works with earnings_history API."""
        from mcp_servers.yf_fundamentals_mcp_server import get_earnings_data

        result = get_earnings_data(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "earnings_data"
        assert result["count"] > 0
        record = result["data"][0]
        # Should have EPS estimate/actual from earnings_history
        keys = set(record.keys())
        assert "epsestimate" in keys or "epsactual" in keys

    def test_compare_financials(self):
        from mcp_servers.yf_fundamentals_mcp_server import compare_financials

        result = compare_financials([_SYMBOL, "MSFT"], statement_type="income")
        assert result["data_type"] == "compare_financials"
        assert _SYMBOL in result["data"]
        assert "MSFT" in result["data"]

    def test_compare_valuations(self):
        from mcp_servers.yf_fundamentals_mcp_server import compare_valuations

        result = compare_valuations([_SYMBOL, "MSFT"])
        assert result["data_type"] == "compare_valuations"
        assert _SYMBOL in result["data"]
        vals = result["data"][_SYMBOL]
        assert vals.get("current_price", 0) > 0

    def test_get_multiple_stocks_earnings_fixed(self):
        """Verify the FIXED multi-earnings tool works."""
        from mcp_servers.yf_fundamentals_mcp_server import get_multiple_stocks_earnings

        result = get_multiple_stocks_earnings([_SYMBOL])
        assert result["data_type"] == "multiple_stocks_earnings"
        assert _SYMBOL in result["data"]
        assert result["data"][_SYMBOL]["count"] > 0


# ===========================================================================
# yf_analysis_mcp_server
# ===========================================================================


@_skip
class TestYfAnalysisLive:
    """Live tests for yf_analysis_mcp_server — including FIXED get_news."""

    def test_get_analyst_recommendations(self):
        from mcp_servers.yf_analysis_mcp_server import get_analyst_recommendations

        result = get_analyst_recommendations(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "analyst_recommendations"
        assert result["count"] > 0

    def test_get_news_fixed(self):
        """Verify the FIXED news tool works with yfinance 1.2.0 xhr/ncp API."""
        from mcp_servers.yf_analysis_mcp_server import get_news

        result = get_news(_SYMBOL, count=5)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "news"
        assert result["count"] > 0
        # Verify we get actual article data (not all-None fields)
        article = result["data"][0]
        assert isinstance(article, dict)
        assert len(article) > 0

    def test_get_news_tab_press_releases(self):
        from mcp_servers.yf_analysis_mcp_server import get_news

        result = get_news(_SYMBOL, count=5, tab="press releases")
        # May be empty for some tickers, but shouldn't error
        assert "error" not in result or "No news" in result.get("error", "")

    def test_get_institutional_holders(self):
        from mcp_servers.yf_analysis_mcp_server import get_institutional_holders

        result = get_institutional_holders(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "institutional_holders"
        assert result["count"] > 0
        holder = result["data"][0]
        assert "holder" in holder
        assert "shares" in holder

    def test_get_mutualfund_holders(self):
        from mcp_servers.yf_analysis_mcp_server import get_mutualfund_holders

        result = get_mutualfund_holders(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    def test_get_insider_transactions(self):
        from mcp_servers.yf_analysis_mcp_server import get_insider_transactions

        result = get_insider_transactions(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "insider_transactions"
        assert result["count"] > 0
        txn = result["data"][0]
        assert "insider" in txn or "text" in txn

    def test_get_insider_roster(self):
        from mcp_servers.yf_analysis_mcp_server import get_insider_roster

        result = get_insider_roster(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        insider = result["data"][0]
        assert "name" in insider
        assert "position" in insider

    def test_get_analyst_price_targets(self):
        from mcp_servers.yf_analysis_mcp_server import get_analyst_price_targets

        result = get_analyst_price_targets(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "analyst_price_targets"
        data = result["data"]
        assert "current" in data or "mean" in data or "high" in data

    def test_get_upgrades_downgrades(self):
        from mcp_servers.yf_analysis_mcp_server import get_upgrades_downgrades

        result = get_upgrades_downgrades(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "upgrades_downgrades"
        assert result["count"] > 0
        rec = result["data"][0]
        assert "firm" in rec
        assert "tograde" in rec

    def test_get_earnings_history(self):
        from mcp_servers.yf_analysis_mcp_server import get_earnings_history

        result = get_earnings_history(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "earnings_history"
        assert result["count"] > 0

    def test_get_earnings_estimates(self):
        from mcp_servers.yf_analysis_mcp_server import get_earnings_estimates

        result = get_earnings_estimates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "earnings_estimates"
        assert result["count"] > 0

    def test_get_revenue_estimates(self):
        from mcp_servers.yf_analysis_mcp_server import get_revenue_estimates

        result = get_revenue_estimates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "revenue_estimates"
        assert result["count"] > 0

    def test_get_growth_estimates(self):
        from mcp_servers.yf_analysis_mcp_server import get_growth_estimates

        result = get_growth_estimates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "growth_estimates"
        assert result["count"] > 0

    def test_get_major_holders(self):
        from mcp_servers.yf_analysis_mcp_server import get_major_holders

        result = get_major_holders(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "major_holders"
        assert result["count"] > 0


# ===========================================================================
# yf_market_mcp_server
# ===========================================================================


@_skip
class TestYfMarketLive:
    """Live tests for yf_market_mcp_server — all new tools."""

    def test_search_tickers(self):
        from mcp_servers.yf_market_mcp_server import search_tickers

        result = search_tickers("Apple")
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "search_results"
        quotes = result["data"]["quotes"]
        assert len(quotes) > 0
        symbols = [q.get("symbol") for q in quotes]
        assert "AAPL" in symbols

    def test_get_market_status(self):
        from mcp_servers.yf_market_mcp_server import get_market_status

        result = get_market_status("US")
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "market_status"
        assert "status" in result["data"]

    def test_get_predefined_screen_most_actives(self):
        from mcp_servers.yf_market_mcp_server import get_predefined_screen

        result = get_predefined_screen("most_actives")
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "predefined_screen"
        assert result["screen_name"] == "most_actives"

    def test_get_predefined_screen_invalid(self):
        from mcp_servers.yf_market_mcp_server import get_predefined_screen

        result = get_predefined_screen("nonexistent_screen")
        assert "error" in result

    def test_screen_stocks(self):
        from mcp_servers.yf_market_mcp_server import screen_stocks

        filters = [
            {"operator": "gt", "operands": ["percentchange", 1]},
        ]
        result = screen_stocks(filters=filters, count=10)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "screen_results"

    def test_get_sector_info(self):
        from mcp_servers.yf_market_mcp_server import get_sector_info

        result = get_sector_info("technology")
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "sector_info"
        data = result["data"]
        assert "overview" in data
        assert "top_companies" in data

    def test_get_industry_info(self):
        from mcp_servers.yf_market_mcp_server import get_industry_info

        result = get_industry_info("software-infrastructure")
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "industry_info"
        data = result["data"]
        assert "sector_key" in data or "overview" in data
