"""Integration tests for yfinance MCP servers — hits real Yahoo Finance API.

Run with:  uv run pytest tests/integration/test_yf_mcp_live.py -m integration -v
Requires:  yfinance library (production dependency, always available)

The yfinance servers use the standard agent-facing envelope
(see mcp_servers/AGENT_CONTRACT.md): payload key `data`, canonical `symbol`
echoed, plain-int `count`, time-ordered series ascending (oldest first), and
tool-specific echo keys (interval, market, screen_name, sector, industry, ...).
They do NOT carry a `data_type` key — the FMP-backed servers do, the yf ones
identify themselves via `source == "yfinance"` plus their echo keys.
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
        assert result["symbol"] == _SYMBOL
        assert result["interval"] == "1day"  # "1d" alias normalized to canonical
        assert result["source"] == "yfinance"
        assert result["count"] > 0
        data = result["data"]
        assert result["count"] == len(data)
        row = data[0]
        assert all(k in row for k in ("date", "open", "high", "low", "close", "volume"))
        assert row["close"] > 0
        # Ascending order (oldest first).
        if len(data) > 1:
            assert data[0]["date"] <= data[1]["date"]

    def test_get_stock_history_intraday(self):
        from mcp_servers.yf_price_mcp_server import get_stock_history

        result = get_stock_history(_SYMBOL, period="1d", interval="5m")
        assert "error" not in result, result.get("error")
        assert result["interval"] == "5min"  # "5m" alias normalized to canonical
        assert result["count"] > 0

    def test_get_stock_history_lse_minor_units(self):
        """LSE quotes arrive from Yahoo in pence (GBp); the server converts to
        major units and reports currency GBP."""
        from mcp_servers.yf_price_mcp_server import get_stock_history

        result = get_stock_history("VOD.L", period="5d", interval="1d")
        assert "error" not in result, result.get("error")
        assert result["currency"] == "GBP"
        assert result["count"] > 0
        close = result["data"][-1]["close"]
        # Pounds, not pence — a VOD.L close quoted in pence would be ~70-100.
        assert 0 < close < 20

    def test_get_multiple_stocks_history(self):
        from mcp_servers.yf_price_mcp_server import get_multiple_stocks_history

        result = get_multiple_stocks_history([_SYMBOL, "MSFT"], period="5d")
        assert "error" not in result
        assert result["source"] == "yfinance"
        assert _SYMBOL in result["data"]
        assert "MSFT" in result["data"]
        # `count` is the total bar count across every symbol.
        assert result["count"] > 0
        assert result["data"][_SYMBOL]["count"] > 0

    def test_get_dividends_and_splits(self):
        from mcp_servers.yf_price_mcp_server import get_dividends_and_splits

        result = get_dividends_and_splits(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["source"] == "yfinance"
        assert result["dividend_count"] > 0
        div = result["data"]["dividends"][0]
        assert "date" in div
        assert "amount" in div
        assert div["amount"] > 0

    def test_get_multiple_stocks_dividends(self):
        from mcp_servers.yf_price_mcp_server import get_multiple_stocks_dividends

        result = get_multiple_stocks_dividends([_SYMBOL, "MSFT"])
        assert "error" not in result
        # `count` is the total dividend records across symbols.
        assert result["count"] > 0


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
        assert result["symbol"] == _SYMBOL
        assert result["source"] == "yfinance"
        data = result["data"]
        assert len(data) > 0
        # Should have common financial metrics (Yahoo-native metric names).
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
        assert result["source"] == "yfinance"
        assert len(result["data"]) > 0

    def test_get_cash_flow(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_cash_flow

        result = get_cash_flow(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["source"] == "yfinance"
        assert len(result["data"]) > 0

    def test_get_company_info(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_company_info

        result = get_company_info(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] == 1
        info = result["data"]
        assert info.get("shortName") or info.get("longName")
        assert info.get("sector")
        assert info.get("marketCap", 0) > 0

    def test_get_earnings_dates(self):
        from mcp_servers.yf_fundamentals_mcp_server import get_earnings_dates

        result = get_earnings_dates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        record = result["data"][0]
        # Check expected columns exist (lowercased/cleaned).
        keys = set(record.keys())
        assert "eps_estimate" in keys or "reported_eps" in keys

    def test_get_earnings_data_fixed(self):
        """Verify the earnings tool works with the earnings_history API."""
        from mcp_servers.yf_fundamentals_mcp_server import get_earnings_data

        result = get_earnings_data(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        record = result["data"][0]
        # Should have EPS estimate/actual from earnings_history.
        keys = set(record.keys())
        assert "epsestimate" in keys or "epsactual" in keys

    def test_compare_financials(self):
        from mcp_servers.yf_fundamentals_mcp_server import compare_financials

        result = compare_financials([_SYMBOL, "MSFT"], statement_type="income")
        assert result["statement_type"] == "income"
        assert _SYMBOL in result["data"]
        assert "MSFT" in result["data"]

    def test_compare_valuations(self):
        from mcp_servers.yf_fundamentals_mcp_server import compare_valuations

        result = compare_valuations([_SYMBOL, "MSFT"])
        assert _SYMBOL in result["data"]
        vals = result["data"][_SYMBOL]
        assert vals.get("current_price", 0) > 0

    def test_get_multiple_stocks_earnings_fixed(self):
        """Verify the multi-earnings tool works."""
        from mcp_servers.yf_fundamentals_mcp_server import get_multiple_stocks_earnings

        result = get_multiple_stocks_earnings([_SYMBOL])
        assert _SYMBOL in result["data"]
        assert result["data"][_SYMBOL]["count"] > 0


# ===========================================================================
# yf_analysis_mcp_server
# ===========================================================================


@_skip
class TestYfAnalysisLive:
    """Live tests for yf_analysis_mcp_server."""

    def test_get_analyst_recommendations(self):
        from mcp_servers.yf_analysis_mcp_server import get_analyst_recommendations

        result = get_analyst_recommendations(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["source"] == "yfinance"
        assert result["count"] > 0

    def test_get_news_fixed(self):
        """Verify the news tool works with yfinance's xhr/ncp API."""
        from mcp_servers.yf_analysis_mcp_server import get_news

        result = get_news(_SYMBOL, count=5)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        # Verify we get actual article data (not all-None fields).
        article = result["data"][0]
        assert isinstance(article, dict)
        assert len(article) > 0

    def test_get_news_tab_press_releases(self):
        from mcp_servers.yf_analysis_mcp_server import get_news

        result = get_news(_SYMBOL, count=5, tab="press releases")
        # May be empty for some tickers (a success envelope with empty data);
        # if it does error, it must be a well-formed error envelope.
        assert "error" not in result or "detail" in result

    def test_get_institutional_holders(self):
        from mcp_servers.yf_analysis_mcp_server import get_institutional_holders

        result = get_institutional_holders(_SYMBOL)
        assert "error" not in result, result.get("error")
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
        data = result["data"]
        assert "current" in data or "mean" in data or "high" in data

    def test_get_upgrades_downgrades(self):
        from mcp_servers.yf_analysis_mcp_server import get_upgrades_downgrades

        result = get_upgrades_downgrades(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        rec = result["data"][0]
        assert "firm" in rec
        assert "tograde" in rec

    def test_get_earnings_history(self):
        from mcp_servers.yf_analysis_mcp_server import get_earnings_history

        result = get_earnings_history(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    def test_get_earnings_estimates(self):
        from mcp_servers.yf_analysis_mcp_server import get_earnings_estimates

        result = get_earnings_estimates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    def test_get_revenue_estimates(self):
        from mcp_servers.yf_analysis_mcp_server import get_revenue_estimates

        result = get_revenue_estimates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    def test_get_growth_estimates(self):
        from mcp_servers.yf_analysis_mcp_server import get_growth_estimates

        result = get_growth_estimates(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    def test_get_major_holders(self):
        from mcp_servers.yf_analysis_mcp_server import get_major_holders

        result = get_major_holders(_SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0


# ===========================================================================
# yf_market_mcp_server
# ===========================================================================


@_skip
class TestYfMarketLive:
    """Live tests for yf_market_mcp_server."""

    def test_search_tickers(self):
        from mcp_servers.yf_market_mcp_server import search_tickers

        result = search_tickers("Apple")
        assert "error" not in result, result.get("error")
        quotes = result["data"]["quotes"]
        assert len(quotes) > 0
        symbols = [q.get("symbol") for q in quotes]
        assert "AAPL" in symbols

    def test_get_market_status(self):
        from mcp_servers.yf_market_mcp_server import get_market_status

        result = get_market_status("US")
        assert "error" not in result, result.get("error")
        assert result["market"] == "US"
        assert "status" in result["data"]

    def test_get_predefined_screen_most_actives(self):
        from mcp_servers.yf_market_mcp_server import get_predefined_screen

        result = get_predefined_screen("most_actives")
        assert "error" not in result, result.get("error")
        assert result["screen_name"] == "most_actives"

    def test_get_predefined_screen_invalid(self):
        from mcp_servers.yf_market_mcp_server import get_predefined_screen

        result = get_predefined_screen("nonexistent_screen")
        assert "error" in result
        assert result["error"] == "invalid_argument"

    def test_screen_stocks(self):
        from mcp_servers.yf_market_mcp_server import screen_stocks

        filters = [
            {"operator": "gt", "operands": ["percentchange", 1]},
        ]
        result = screen_stocks(filters=filters, count=10)
        assert "error" not in result, result.get("error")
        assert "data" in result

    def test_get_sector_info(self):
        from mcp_servers.yf_market_mcp_server import get_sector_info

        result = get_sector_info("technology")
        assert "error" not in result, result.get("error")
        assert result["sector"] == "technology"
        data = result["data"]
        assert "overview" in data
        assert "top_companies" in data

    def test_get_industry_info(self):
        from mcp_servers.yf_market_mcp_server import get_industry_info

        result = get_industry_info("software-infrastructure")
        assert "error" not in result, result.get("error")
        assert result["industry"] == "software-infrastructure"
        data = result["data"]
        assert "sector_key" in data or "overview" in data
