"""Integration tests for yfinance data sources — hits real Yahoo Finance API.

Run with:  uv run pytest tests/integration/test_yfinance_live.py -m integration -v
Requires:  yfinance library (production dependency, always available)
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_SYMBOL = "AAPL"

try:
    import yfinance as yf  # noqa: F401

    _has_yfinance = True
except ImportError:
    _has_yfinance = False


# ---------------------------------------------------------------------------
# FinancialDataSource (financial_source.py)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_yfinance, reason="yfinance not installed")
class TestYFinanceFinancialSourceLive:
    """Hit real yfinance API for every FinancialDataSource method."""

    async def test_get_company_profile(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_company_profile(_SYMBOL)
        assert len(result) == 1
        p = result[0]
        assert p["symbol"] == _SYMBOL
        assert p["companyName"]
        assert p["sector"]
        assert p["industry"]
        assert p["mktCap"] and p["mktCap"] > 0
        assert p["currency"]

    async def test_get_realtime_quote(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_realtime_quote(_SYMBOL)
        assert len(result) == 1
        q = result[0]
        assert q["symbol"] == _SYMBOL
        assert q["price"] > 0
        assert q["volume"] > 0
        assert "changesPercentage" in q

    async def test_get_income_statements(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_income_statements(_SYMBOL, period="quarter", limit=4)
        assert len(result) >= 1
        stmt = result[0]
        # FMP-compatible keys must be present
        assert "revenue" in stmt, f"Missing 'revenue', keys: {list(stmt.keys())}"
        assert "netIncome" in stmt
        assert "grossProfit" in stmt
        assert "date" in stmt
        # Margin ratios should be computed
        assert "grossProfitRatio" in stmt
        assert "netIncomeRatio" in stmt
        assert 0 < stmt["grossProfitRatio"] < 1

    async def test_get_income_statements_annual(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_income_statements(_SYMBOL, period="annual", limit=4)
        assert len(result) >= 1
        assert "revenue" in result[0]

    async def test_get_cash_flows(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_cash_flows(_SYMBOL, period="quarter", limit=4)
        assert len(result) >= 1
        cf = result[0]
        assert "operatingCashFlow" in cf, f"Missing 'operatingCashFlow', keys: {list(cf.keys())}"
        assert "capitalExpenditure" in cf
        assert "date" in cf

    async def test_get_key_metrics(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_key_metrics(_SYMBOL)
        assert len(result) == 1
        m = result[0]
        assert m["symbol"] == _SYMBOL
        assert m["marketCap"] and m["marketCap"] > 0
        assert m["peRatio"] is not None

    async def test_get_financial_ratios(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_financial_ratios(_SYMBOL)
        assert len(result) == 1
        r = result[0]
        assert r["symbol"] == _SYMBOL
        assert r["returnOnEquity"] is not None

    async def test_get_price_performance(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_price_performance(_SYMBOL)
        assert len(result) == 1
        perf = result[0]
        assert perf["symbol"] == _SYMBOL
        # At least short-term periods should be populated
        assert perf["1D"] is not None
        assert perf["1M"] is not None
        assert perf["1Y"] is not None

    async def test_get_analyst_price_targets(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_analyst_price_targets(_SYMBOL)
        # AAPL should always have analyst coverage
        assert len(result) == 1
        t = result[0]
        assert t["symbol"] == _SYMBOL
        assert t["targetHigh"] is not None
        assert t["targetLow"] is not None
        assert t["targetHigh"] >= t["targetLow"]

    async def test_get_analyst_ratings(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_analyst_ratings(_SYMBOL)
        assert len(result) >= 1
        r = result[0]
        # Consensus must be derived
        assert "consensus" in r, f"Missing 'consensus', keys: {list(r.keys())}"
        assert r["consensus"] in ("Strong Buy", "Buy", "Hold", "Sell", "Strong Sell")

    async def test_get_earnings_history(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_earnings_history(_SYMBOL, limit=4)
        assert len(result) >= 1
        e = result[0]
        # FMP-compatible keys
        assert "date" in e, f"Missing 'date', keys: {list(e.keys())}"
        assert "eps" in e

    async def test_search_stocks(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.search_stocks("Apple", limit=5)
        assert len(result) >= 1
        # AAPL should be in results
        symbols = [r["symbol"] for r in result]
        assert "AAPL" in symbols
        hit = next(r for r in result if r["symbol"] == "AAPL")
        assert hit["name"]

    async def test_get_sector_performance(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_sector_performance()
        assert len(result) >= 8, f"Expected most sectors, got {len(result)}"
        sector_names = {r["sector"] for r in result}
        # At least these major sectors should be present
        assert "Technology" in sector_names
        assert "Healthcare" in sector_names
        assert "Energy" in sector_names
        # Check format
        for r in result:
            assert "changesPercentage" in r
            pct_str = r["changesPercentage"]
            assert pct_str.endswith("%")
            # Should be parseable as float after stripping % and +
            float(pct_str.replace("%", "").replace("+", ""))

    async def test_screen_stocks_no_filters(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.screen_stocks()
        assert len(result) >= 1, "Screener should return results with no filters"
        r = result[0]
        # FMP-compatible keys
        assert "symbol" in r
        assert "companyName" in r
        assert "price" in r
        assert "volume" in r

    async def test_screen_stocks_sector_filter(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.screen_stocks(sector="Technology", limit=10)
        assert len(result) >= 1
        for r in result:
            assert r["symbol"]
            assert r["companyName"]
            assert r["price"] is not None

    async def test_screen_stocks_market_cap_filter(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        # Large caps only (> $100B)
        result = await src.screen_stocks(marketCapMoreThan=100_000_000_000, limit=10)
        assert len(result) >= 1
        # Verify results have the expected keys (Yahoo filtering isn't exact)
        for r in result:
            assert r["symbol"]
            assert r["companyName"]

    async def test_get_revenue_by_segment_returns_empty(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        result = await src.get_revenue_by_segment(_SYMBOL)
        assert result == []

    async def test_close_is_noop(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        src = YFinanceFinancialSource()
        await src.close()  # Should not raise


# ---------------------------------------------------------------------------
# MarketDataSource (data_source.py)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_yfinance, reason="yfinance not installed")
class TestYFinanceDataSourceLive:
    """Hit real yfinance API for intraday and daily data."""

    async def test_intraday_1min(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        result = await src.get_intraday(_SYMBOL, interval="1min")
        assert len(result) >= 1
        bar = result[0]
        assert all(k in bar for k in ("time", "open", "high", "low", "close", "volume"))
        assert bar["close"] > 0

    async def test_intraday_5min(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        result = await src.get_intraday(_SYMBOL, interval="5min")
        assert len(result) >= 1

    async def test_intraday_1hour(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        result = await src.get_intraday(_SYMBOL, interval="1hour")
        assert len(result) >= 1

    async def test_intraday_unsupported_interval_raises(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        with pytest.raises(ValueError, match="not supported"):
            await src.get_intraday(_SYMBOL, interval="1s")

    async def test_intraday_4hour_unsupported(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        with pytest.raises(ValueError, match="not supported"):
            await src.get_intraday(_SYMBOL, interval="4hour")

    async def test_intraday_index_symbol(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        result = await src.get_intraday("GSPC", interval="5min", is_index=True)
        assert len(result) >= 1, "Index ^GSPC should return intraday data"
        assert result[0]["close"] > 0

    async def test_daily(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        result = await src.get_daily(
            _SYMBOL,
            from_date="2025-01-01",
            to_date="2025-01-31",
        )
        assert len(result) >= 15, "Should have ~20 trading days in Jan 2025"
        bar = result[0]
        assert all(k in bar for k in ("time", "open", "high", "low", "close", "volume"))

    async def test_daily_index(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        result = await src.get_daily(
            "GSPC",
            from_date="2025-01-01",
            to_date="2025-01-31",
            is_index=True,
        )
        assert len(result) >= 15

    async def test_close_is_noop(self):
        from src.data_client.yfinance.data_source import YFinanceDataSource

        src = YFinanceDataSource()
        await src.close()
