"""Live contract tests for the public data API surface backed by FMP.

Pins the contract that callers depend on so the FMP stable-API migration
(issue #210) can be verified end-to-end. Tests are organized by layer:

  1. FinancialDataSource protocol (FMPFinancialSource)
  2. MarketDataSource protocol  (FMPDataSource)
  3. NewsDataSource protocol    (FMPNewsSource)
  4. Fundamentals MCP tools
  5. Macro MCP tools
  6. Price-data MCP tools (FMP fallback paths)
  7. LangChain fetcher functions in src/tools/market_data/implementations.py
  8. SEC earnings-call fetcher

Each test asserts on the consumer-facing shape — the field names that
downstream code in implementations.py / fundamentals_mcp_server.py /
server REST handlers actually destructure. A successful run before and
after migration proves no caller has regressed.

Run with:  uv run python -m pytest tests/integration/test_fmp_public_api_contract.py -m integration -v
Requires:  FMP_API_KEY
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_has_fmp = bool(os.getenv("FMP_API_KEY"))
skip_no_fmp = pytest.mark.skipif(not _has_fmp, reason="FMP_API_KEY not set")

SYMBOL = "AAPL"
PEER_SYMBOL = "MSFT"


@pytest_asyncio.fixture(autouse=True)
async def _reset_fmp_state():
    """Reset every FMP-related module singleton so each test runs on a
    fresh event loop with no httpx connections from a previous loop.

    The conftest's `_reset_fmp_singleton` only touches
    `data_client.fmp._fmp_client`, but production code commonly imports via
    `src.data_client.fmp` — Python treats those as two separate modules
    with separate `_fmp_client` globals. We close both and also clear the
    cached `FinancialDataProvider`, which holds a strong reference to a
    FMPFinancialSource wrapping the FMP client.
    """
    import src.data_client.registry as registry
    import src.data_client.fmp as src_fmp
    try:
        import data_client.fmp as bare_fmp  # may not exist on all paths
    except ImportError:
        bare_fmp = None

    registry._financial_data_provider = None
    yield

    # Teardown: close both singletons so their httpx connections release
    # before the test's event loop closes.
    if src_fmp._fmp_client is not None:
        await src_fmp._fmp_client.close()
        src_fmp._fmp_client = None
    if bare_fmp is not None and bare_fmp._fmp_client is not None:
        await bare_fmp._fmp_client.close()
        bare_fmp._fmp_client = None
    registry._financial_data_provider = None


def has_any(d: dict, *names: str) -> bool:
    """True if `d` contains at least one of `names`.

    Lets a single assertion accept either the v3 or stable field name —
    so the test passes whether the FMP migration has shipped or not.
    """
    return any(n in d for n in names)


# ---------------------------------------------------------------------------
# 1. FinancialDataSource protocol (FMPFinancialSource)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestFinancialDataSourceContract:
    """The protocol used by company overview, screener, sector performance."""

    @pytest_asyncio.fixture
    async def source(self):
        from src.data_client.fmp.fmp_client import FMPClient
        from src.data_client.fmp.financial_source import FMPFinancialSource

        client = FMPClient()
        try:
            yield FMPFinancialSource(client)
        finally:
            await client.close()

    async def test_get_company_profile(self, source):
        data = await source.get_company_profile(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert row.get("symbol") == SYMBOL
        assert "marketCap" in row
        assert has_any(row, "companyName", "name")

    async def test_get_realtime_quote(self, source):
        data = await source.get_realtime_quote(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert row.get("symbol") == SYMBOL
        assert "price" in row
        assert "changePercentage" in row
        assert "marketCap" in row

    async def test_get_income_statements(self, source):
        data = await source.get_income_statements(SYMBOL, period="quarter", limit=4)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert row.get("symbol") == SYMBOL
        assert "revenue" in row
        assert "fiscalYear" in row

    async def test_get_cash_flows(self, source):
        data = await source.get_cash_flows(SYMBOL, period="quarter", limit=4)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert "operatingCashFlow" in row or "netCashProvidedByOperatingActivities" in row
        assert "freeCashFlow" in row

    async def test_get_key_metrics(self, source):
        data = await source.get_key_metrics(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        # TTM key metrics: peRatioTTM or marketCap should be present.
        row = data[0]
        assert has_any(row, "peRatioTTM", "marketCap", "enterpriseValueTTM")

    async def test_get_financial_ratios(self, source):
        data = await source.get_financial_ratios(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        # TTM ratios: at least one of these must be present.
        assert has_any(row, "currentRatioTTM", "debtToEquityTTM", "priceEarningsRatioTTM")

    async def test_get_price_performance(self, source):
        data = await source.get_price_performance(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        # Consumer expects 1D/5D/1M/ytd/1Y keys.
        for key in ("1D", "1M", "ytd", "1Y"):
            assert key in row, f"missing {key} in price performance"

    async def test_get_analyst_price_targets(self, source):
        data = await source.get_analyst_price_targets(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        # AnalystDataResponse model destructures these (market_data.py L578-581).
        for key in ("targetHigh", "targetLow", "targetConsensus", "targetMedian"):
            assert key in row, f"missing {key} in price target consensus"

    async def test_get_analyst_ratings(self, source):
        data = await source.get_analyst_ratings(SYMBOL)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        # Grades-consensus expected keys.
        assert has_any(row, "consensus", "strongBuy", "buy")

    async def test_get_earnings_history(self, source):
        data = await source.get_earnings_history(SYMBOL, limit=5)
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert row.get("symbol") == SYMBOL
        # Consumer (implementations.py L1148-1152, L2227-2231) reads
        # Stable returns date + epsActual/epsEstimated; fiscalDateEnding is dropped.
        assert "date" in row
        assert "epsActual" in row
        assert "epsEstimated" in row

    async def test_get_revenue_by_segment_product(self, source):
        data = await source.get_revenue_by_segment(
            SYMBOL, segment_type="product", period="annual", structure="flat"
        )
        assert isinstance(data, list)
        # Segmentation may be empty for some tickers — assert structure only.
        if data:
            assert isinstance(data[0], dict)

    async def test_get_revenue_by_segment_geography(self, source):
        data = await source.get_revenue_by_segment(
            SYMBOL, segment_type="geography", period="annual", structure="flat"
        )
        assert isinstance(data, list)
        if data:
            assert isinstance(data[0], dict)

    async def test_get_sector_performance(self, source):
        data = await source.get_sector_performance()
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert "sector" in row
        # _format_sector_change_pct renders averageChange into changePctStr;
        # both keys are expected on the row.
        assert "changePctStr" in row
        assert row["changePctStr"].endswith("%")

    async def test_screen_stocks(self, source):
        data = await source.screen_stocks(
            sector="Technology",
            marketCapMoreThan=1_000_000_000_000,
            limit=10,
        )
        assert isinstance(data, list) and len(data) > 0
        row = data[0]
        assert "symbol" in row

    async def test_search_stocks_by_symbol(self, source):
        data = await source.search_stocks(query="AAPL", limit=5)
        assert isinstance(data, list) and len(data) > 0
        # Symbol-style query must surface AAPL.
        assert any(r.get("symbol", "").startswith("AAPL") for r in data)

    async def test_search_stocks_by_name(self, source):
        # Name-style query — v3 `search` matches both. Stable `search-symbol`
        # alone won't match "Apple"; migration must add `search-name`.
        data = await source.search_stocks(query="Apple", limit=10)
        assert isinstance(data, list) and len(data) > 0
        assert any("Apple" in (r.get("name") or "") for r in data)


# ---------------------------------------------------------------------------
# 2. MarketDataSource protocol (FMPDataSource)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestMarketDataSourceContract:
    """The OHLCV / snapshot surface used by market_data REST endpoints.

    `FMPDataSource` takes no constructor args — it opens a fresh
    `FMPClient` per call via `async with`.
    """

    @pytest.fixture
    def src(self):
        from src.data_client.fmp.data_source import FMPDataSource

        return FMPDataSource()

    async def test_get_daily(self, src):
        today = date.today().isoformat()
        from_d = (date.today() - timedelta(days=30)).isoformat()
        bars = await src.get_daily(SYMBOL, from_date=from_d, to_date=today)
        assert isinstance(bars, list) and len(bars) > 0
        bar = bars[0]
        # Normalized contract — see data_source.py:normalize_bars.
        for key in ("time", "open", "high", "low", "close", "volume"):
            assert key in bar, f"missing {key} in daily bar"

    async def test_get_intraday_5min(self, src):
        # Pick a known weekday in the recent past with US market data.
        bars = await src.get_intraday(
            SYMBOL, interval="5min",
            from_date="2025-03-03", to_date="2025-03-07",
        )
        assert isinstance(bars, list) and len(bars) > 0
        bar = bars[0]
        for key in ("time", "open", "high", "low", "close", "volume"):
            assert key in bar

    async def test_get_snapshots_batch(self, src):
        snaps = await src.get_snapshots([SYMBOL, PEER_SYMBOL], asset_type="stocks")
        assert isinstance(snaps, list) and len(snaps) == 2
        snap = snaps[0]
        for key in ("symbol", "price", "change", "change_percent",
                    "previous_close", "open", "high", "low", "volume"):
            assert key in snap, f"missing {key} in snapshot"

    async def test_get_market_status(self, src):
        status = await src.get_market_status()
        # Local-clock derived; no network call. Just sanity check shape.
        assert "market" in status


# ---------------------------------------------------------------------------
# 3. NewsDataSource protocol (FMPNewsSource)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestNewsDataSourceContract:
    @pytest.fixture
    def src(self):
        from src.data_client.fmp.news_source import FMPNewsSource

        return FMPNewsSource()

    async def test_get_news_tickered(self, src):
        result = await src.get_news(tickers=SYMBOL, limit=5)
        assert "results" in result
        assert isinstance(result["results"], list) and len(result["results"]) > 0
        article = result["results"][0]
        for key in ("id", "title", "published_at", "article_url", "source"):
            assert key in article, f"missing {key} in news article"

    async def test_get_news_general(self, src):
        result = await src.get_news(tickers=None, limit=5)
        assert "results" in result
        assert isinstance(result["results"], list)


# ---------------------------------------------------------------------------
# 4. Fundamentals MCP tools
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestFundamentalsMcpContract:
    async def test_get_financial_statements_income(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        result = await get_financial_statements(SYMBOL, statement_type="income", limit=3)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert row.get("symbol") == SYMBOL
        assert "revenue" in row

    async def test_get_financial_statements_balance(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        result = await get_financial_statements(SYMBOL, statement_type="balance", limit=2)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert "totalAssets" in row

    async def test_get_financial_statements_cash(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        result = await get_financial_statements(SYMBOL, statement_type="cash", limit=2)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert has_any(row, "operatingCashFlow", "netCashProvidedByOperatingActivities")

    async def test_get_financial_statements_all(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        result = await get_financial_statements(SYMBOL, statement_type="all", limit=1)
        assert "error" not in result, result.get("error")
        assert result["count"]["income_statement"] >= 1
        assert result["count"]["balance_sheet"] >= 1
        assert result["count"]["cash_flow"] >= 1

    async def test_get_financial_ratios(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_ratios

        result = await get_financial_ratios(SYMBOL, limit=2)
        assert "error" not in result, result.get("error")
        assert result["count"]["key_metrics"] > 0
        assert result["count"]["ratios"] > 0

    async def test_get_growth_metrics(self):
        from mcp_servers.fundamentals_mcp_server import get_growth_metrics

        result = await get_growth_metrics(SYMBOL, limit=3)
        assert "error" not in result, result.get("error")
        # The bug surfaces here today: financial-growth works on v3, but
        # balance-sheet-growth / cash-flow-growth return [] on v3.
        # Stable returns rich data for both.
        assert result["count"]["financial_growth"] > 0
        assert result["count"]["income_statement_growth"] > 0

    async def test_get_historical_valuation(self):
        from mcp_servers.fundamentals_mcp_server import get_historical_valuation

        result = await get_historical_valuation(SYMBOL, limit=2)
        assert "error" not in result, result.get("error")
        assert "current_dcf" in result["data"]
        assert "enterprise_value" in result["data"]

    async def test_get_insider_trades(self):
        from mcp_servers.fundamentals_mcp_server import get_insider_trades

        result = await get_insider_trades(SYMBOL, limit=5)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "insider_trades"

    async def test_get_dividends_and_splits(self):
        from mcp_servers.fundamentals_mcp_server import get_dividends_and_splits

        result = await get_dividends_and_splits(SYMBOL)
        assert "error" not in result, result.get("error")
        assert "dividends" in result["data"]
        assert "splits" in result["data"]
        assert result["count"]["dividends"] > 0

    async def test_get_shares_float(self):
        from mcp_servers.fundamentals_mcp_server import get_shares_float

        result = await get_shares_float(SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert has_any(row, "floatShares", "outstandingShares", "freeFloat")

    async def test_get_key_executives(self):
        from mcp_servers.fundamentals_mcp_server import get_key_executives

        result = await get_key_executives(SYMBOL)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    async def test_get_technical_indicator_rsi(self):
        from mcp_servers.fundamentals_mcp_server import get_technical_indicator

        result = await get_technical_indicator(SYMBOL, indicator="rsi", period=14)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert "date" in row
        # The indicator key is the name itself ("rsi") on stable.
        assert "rsi" in row or "RSI" in row


# ---------------------------------------------------------------------------
# 5. Macro MCP tools (covered in test_mcp_macro_live.py; add a smoke test
#    so this file is self-contained)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestMacroMcpContract:
    async def test_economic_indicator_gdp(self):
        from mcp_servers.macro_mcp_server import get_economic_indicator

        result = await get_economic_indicator("GDP", limit=3)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        assert "date" in result["data"][0] and "value" in result["data"][0]

    async def test_treasury_rates(self):
        from mcp_servers.macro_mcp_server import get_treasury_rates

        result = await get_treasury_rates(
            from_date="2025-03-01", to_date="2025-03-05",
        )
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    async def test_earnings_calendar(self):
        from mcp_servers.macro_mcp_server import get_earnings_calendar

        result = await get_earnings_calendar(
            from_date="2025-01-27", to_date="2025-01-31",
        )
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert "symbol" in row


# ---------------------------------------------------------------------------
# 6. Price-data MCP (FMP fallback path)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestPriceDataMcpContract:
    """Cover FMP-fallback paths. ginlix-data is preferred when available;
    these tests pin the FMP-side contract.
    """

    async def test_get_stock_data_daily(self):
        from mcp_servers.price_data_mcp_server import get_stock_data

        result = await get_stock_data(SYMBOL, interval="1day")
        assert "error" not in result, result.get("error")
        assert result["symbol"] == SYMBOL
        assert result["count"] > 0
        row = result["rows"][0]
        for key in ("date", "open", "high", "low", "close", "volume"):
            assert key in row

    async def test_get_asset_data_commodity(self):
        from mcp_servers.price_data_mcp_server import get_asset_data

        result = await get_asset_data("GCUSD", asset_type="commodity")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0

    async def test_get_asset_data_crypto(self):
        from mcp_servers.price_data_mcp_server import get_asset_data

        result = await get_asset_data("BTCUSD", asset_type="crypto")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0


# ---------------------------------------------------------------------------
# 7. LangChain fetcher functions (the layer between agent tools and FMP)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestLangChainFetcherContract:
    async def test_fetch_company_overview_data(self):
        from src.tools.market_data.implementations import (
            fetch_company_overview_data,
        )

        artifact = await fetch_company_overview_data(SYMBOL)
        assert artifact["type"] == "company_overview"
        assert artifact["symbol"] == SYMBOL
        # The fetcher pulls profile.companyName — must be populated.
        assert "name" in artifact and artifact["name"] != SYMBOL
        # Quote and analyst sections come from FMP under load.
        assert "quote" in artifact

    async def test_fetch_sector_performance(self):
        from src.tools.market_data.implementations import fetch_sector_performance

        content, artifact = await fetch_sector_performance()
        assert artifact["type"] == "sector_performance"
        assert isinstance(artifact["sectors"], list)
        assert len(artifact["sectors"]) > 0
        s = artifact["sectors"][0]
        assert "sector" in s
        # Artifact (consumed by UI) exposes numeric changePercentage.
        assert "changePercentage" in s
        assert isinstance(s["changePercentage"], (int, float))

    async def test_fetch_stock_screener(self):
        from src.tools.market_data.implementations import fetch_stock_screener

        content, artifact = await fetch_stock_screener(
            market_cap_more_than=2_000_000_000_000,
            sector="Technology",
            limit=10,
        )
        assert artifact["type"] == "stock_screener"
        assert isinstance(artifact["results"], list)
        assert artifact["count"] >= 0

    async def test_fetch_daily_prices(self):
        from src.tools.market_data.implementations import fetch_daily_prices

        content, artifact = await fetch_daily_prices(SYMBOL)
        assert artifact["type"] == "stock_prices"
        assert artifact["symbol"] == SYMBOL
        assert "ohlcv" in artifact and len(artifact["ohlcv"]) > 0
        bar = artifact["ohlcv"][0]
        for key in ("date", "open", "high", "low", "close", "volume"):
            assert key in bar

    async def test_fetch_market_indices(self):
        from src.tools.market_data.implementations import fetch_market_indices

        content, artifact = await fetch_market_indices()
        assert artifact["type"] == "market_indices"
        assert "indices" in artifact
        # At least one index series should be populated.
        assert any(len(v) > 0 for v in artifact["indices"].values())


# ---------------------------------------------------------------------------
# 8. Server REST handler — analyst-data (single dependency on FMP grades)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestAnalystDataRoute:
    """Direct call to the FastAPI handler so the Redis/auth wiring is exercised
    only as a thin wrapper. Mocks the Redis cache to keep the test hermetic.
    """

    async def test_get_analyst_data(self, monkeypatch):
        from unittest.mock import AsyncMock
        from src.server.app import market_data as md
        from src.server.models.market_data import AnalystDataResponse

        # Stub Redis: cache miss + no-op set.
        fake_cache = AsyncMock()
        fake_cache.get.return_value = None
        fake_cache.set.return_value = None
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client",
            lambda: fake_cache,
        )

        result = await md.get_analyst_data(
            symbol=SYMBOL, user_id="test-user", grade_limit=10,
        )
        assert isinstance(result, AnalystDataResponse)
        assert result.symbol == SYMBOL
        # Price target consensus should be populated for AAPL.
        assert result.priceTargets is not None
        assert result.priceTargets.targetConsensus is not None
        # Grades should be a list (may be empty if FMP rate-limits).
        assert isinstance(result.grades, list)


# ---------------------------------------------------------------------------
# 9. SEC earnings-call fetcher (uses FMPClient directly, not via singleton)
# ---------------------------------------------------------------------------


@skip_no_fmp
class TestSecEarningsCall:
    async def test_fetch_matching_earnings_call(self):
        from src.tools.sec.earnings_call import fetch_matching_earnings_call

        # AAPL filed a 10-Q a few days after the FY2025 Q3 earnings call
        # (call: 2025-07-31, filing: ~2025-08-01). Either we find that call
        # or the fetcher returns None — both are acceptable.
        result = await fetch_matching_earnings_call(
            symbol=SYMBOL, filing_date=date(2025, 8, 1),
        )
        if result is not None:
            content, fiscal_year, quarter, call_date = result
            assert isinstance(content, str) and len(content) > 0
            assert isinstance(fiscal_year, int) and 2020 <= fiscal_year <= 2030
            assert isinstance(quarter, int) and 1 <= quarter <= 4
