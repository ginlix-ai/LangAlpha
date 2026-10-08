"""Tests for the fundamentals server's CN branch (ginlix-data, vendor Tushare).

Locks the routing contract: `_tushare_for` gates on the cn market, a populated
result is served with source="tushare", and any empty/error result falls
through to the unchanged FMP path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from .conftest import assert_error, assert_ok_envelope

_MOD = "plugins.langalpha_market_data.fundamentals_mcp_server"

_INCOME = [{"date": "2025-03-31", "revenue": 500.0, "netIncome": 50.0}]
_BALANCE = [{"date": "2025-03-31", "totalAssets": 3000.0}]
_CASH = [{"date": "2025-03-31", "operatingCashFlow": 80.0}]


def _ts_source(**overrides) -> AsyncMock:
    ts = AsyncMock()
    ts.get_income_statements = AsyncMock(return_value=overrides.get("income", _INCOME))
    ts.get_balance_sheets = AsyncMock(return_value=overrides.get("balance", _BALANCE))
    ts.get_cash_flows = AsyncMock(return_value=overrides.get("cash", _CASH))
    ts.get_key_metrics_periods = AsyncMock(return_value=overrides.get(
        "key_metrics", [{"date": "2025-03-31", "returnOnEquity": 0.26}]))
    ts.get_ratios_periods = AsyncMock(return_value=overrides.get(
        "ratios", [{"date": "2025-03-31", "netProfitMargin": 0.5}]))
    ts.get_growth_periods = AsyncMock(return_value=overrides.get("growth", {
        "financial_growth": [{"date": "2024-12-31", "revenueGrowth": 0.15}],
        "income_statement_growth": [{"date": "2024-12-31", "growthRevenue": 0.15}],
    }))
    ts.get_dividends = AsyncMock(return_value=overrides.get(
        "dividends", {"dividends": [{"date": "2025-06-27", "dividend": 27.0}], "splits": []}))
    ts.get_shares_float = AsyncMock(return_value=overrides.get(
        "float", [{"date": "2026-07-10", "floatShares": 1_250_000_000.0}]))
    ts.get_key_executives = AsyncMock(return_value=overrides.get(
        "executives", [{"name": "张三", "title": "Chairman"}]))
    return ts


def _fmp_client() -> AsyncMock:
    client = AsyncMock()
    client.get_income_statement = AsyncMock(return_value=[{"date": "2024-12-31", "revenue": 1.0}])
    client.get_balance_sheet = AsyncMock(return_value=[{"date": "2024-12-31", "totalAssets": 2.0}])
    client.get_cash_flow = AsyncMock(return_value=[{"date": "2024-12-31", "operatingCashFlow": 3.0}])
    client.get_key_metrics = AsyncMock(return_value=[{"date": "2024-12-31", "marketCap": 4.0}])
    client.get_financial_ratios = AsyncMock(return_value=[{"date": "2024-12-31", "returnOnEquity": 0.3}])
    client.get_financial_growth = AsyncMock(return_value=[{"date": "2024-12-31", "revenueGrowth": 0.1}])
    client.get_income_statement_growth = AsyncMock(return_value=[{"date": "2024-12-31", "growthRevenue": 0.1}])
    client.get_dividends = AsyncMock(return_value=[{"date": "2024-12-15", "dividend": 0.25}])
    client.get_splits = AsyncMock(return_value=[])
    client.get_shares_float = AsyncMock(return_value=[{"floatShares": 15_000_000_000}])
    client.get_key_executives = AsyncMock(return_value=[{"name": "Jane Doe", "title": "CEO"}])
    return client


class TestTushareForGating:
    def test_non_cn_symbol_returns_none(self, monkeypatch):
        import plugins.langalpha_market_data.fundamentals_mcp_server as mod

        monkeypatch.setattr(mod, "_tushare_financial", None)
        assert mod._tushare_for("AAPL") is None
        assert mod._tushare_for("0700.HK") is None

    def test_cn_symbol_returns_singleton(self, monkeypatch):
        import plugins.langalpha_market_data.fundamentals_mcp_server as mod
        from data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

        monkeypatch.setattr(mod, "_tushare_financial", None)
        ts = mod._tushare_for("600519.SS")
        assert isinstance(ts, GinlixDataCnFinancialSource)
        assert mod._tushare_for("000001.SZ") is ts


class TestStatementsCnBranch:
    @pytest.mark.asyncio
    async def test_income_served_from_tushare(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        ts = _ts_source()
        with patch(f"{_MOD}._tushare_for", return_value=ts):
            result = await get_financial_statements("600519.SS", statement_type="income", period="quarter")

        # Asked as .SS, echoed and forwarded in the display spelling.
        assert_ok_envelope(result, symbol="600519.SH", source="tushare", count=1)
        assert result["statement_type"] == "income"
        assert result["period"] == "quarter"
        ts.get_income_statements.assert_awaited_once_with("600519.SH", period="quarter", limit=10)

    @pytest.mark.asyncio
    async def test_all_statements_served_from_tushare(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        ts = _ts_source()
        with patch(f"{_MOD}._tushare_for", return_value=ts):
            result = await get_financial_statements("600519.SS", statement_type="all")

        assert_ok_envelope(result, source="tushare", count=3)
        assert result["data"]["income_statement"] == _INCOME
        assert result["data"]["balance_sheet"] == _BALANCE
        assert result["data"]["cash_flow"] == _CASH

    @pytest.mark.asyncio
    async def test_empty_tushare_falls_through_to_fmp(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        ts = _ts_source(income=[], balance=[], cash=[])
        client = _fmp_client()
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("600519.SS", statement_type="all")

        assert_ok_envelope(result, source="fmp")
        client.get_income_statement.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_tushare_error_falls_through_to_fmp(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        ts = _ts_source()
        ts.get_income_statements = AsyncMock(side_effect=RuntimeError("boom"))
        client = _fmp_client()
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("600519.SS", statement_type="income")

        assert_ok_envelope(result, source="fmp")

    @pytest.mark.asyncio
    async def test_tushare_error_then_fmp_error_is_clean(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        ts = _ts_source()
        ts.get_income_statements = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_financial_statements("600519.SS", statement_type="income")

        assert_error(result, "client_unavailable", detail_excludes=("boom", "no key"))

    @pytest.mark.asyncio
    async def test_canonical_cn_key_is_served_from_tushare(self, monkeypatch):
        # ``600519.XSHG`` is a CN listing in its canonical spelling; the gate
        # reads the display spelling, so it must not skip to FMP.
        import plugins.langalpha_market_data.fundamentals_mcp_server as mod

        ts = _ts_source()
        monkeypatch.setattr(mod, "_tushare_financial", ts)
        result = await mod.get_financial_statements("600519.XSHG", statement_type="income")

        assert_ok_envelope(result, symbol="600519.SH", source="tushare")
        ts.get_income_statements.assert_awaited_once_with("600519.SH", period="annual", limit=10)

    @pytest.mark.asyncio
    async def test_canonical_cn_key_falls_through_to_fmp_as_a_ticker(self, monkeypatch):
        import plugins.langalpha_market_data.fundamentals_mcp_server as mod

        monkeypatch.setattr(mod, "_tushare_financial", _ts_source(income=[]))
        client = _fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_financial_statements("600519.XSHG", statement_type="income")

        assert_ok_envelope(result, symbol="600519.SH", source="fmp")
        # The FMP client respells .SH to the .SS it resolves.
        assert client.get_income_statement.await_args.args[0] == "600519.SH"

    @pytest.mark.asyncio
    async def test_upstream_miss_logs_one_line_and_falls_through(self, caplog):
        import httpx

        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        request = httpx.Request("GET", "http://ginlix-data.test/fundamentals")
        ts = _ts_source()
        ts.get_income_statements = AsyncMock(side_effect=httpx.HTTPStatusError(
            "busy", request=request, response=httpx.Response(429, request=request)))
        client = _fmp_client()
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", return_value=client), \
                caplog.at_level("WARNING", logger=_MOD):
            result = await get_financial_statements("600519.SS", statement_type="income")

        assert_ok_envelope(result, source="fmp")
        [record] = [r for r in caplog.records if r.name == _MOD]
        assert record.getMessage() == (
            "ginlix-data financial_statements unavailable for 600519.SH: HTTPStatusError 429"
        )
        assert record.exc_info is None

    @pytest.mark.asyncio
    async def test_one_failed_statement_kind_sends_the_whole_set_to_fmp(self, caplog):
        # Served with the failed kind as [], "all" would read as a CN answer
        # with no balance sheet; the failure has to reach the fallback.
        import httpx

        from data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        request = httpx.Request("GET", "http://ginlix-data.test/fundamentals")

        async def fundamentals(key, kind, **params):
            if kind == "balance":
                raise httpx.HTTPStatusError(
                    "busy", request=request, response=httpx.Response(503, request=request))
            return [{"date": "2025-03-31", "revenue": 500.0}]

        routes = AsyncMock()
        routes.get_fundamentals_v2 = fundamentals
        client = _fmp_client()
        with patch(f"{_MOD}._tushare_for", return_value=GinlixDataCnFinancialSource(routes)), \
                patch(f"{_MOD}.get_fmp_client", return_value=client), \
                caplog.at_level("WARNING", logger=_MOD):
            result = await get_financial_statements("600519.SS", statement_type="all")

        assert_ok_envelope(result, source="fmp")
        [record] = [r for r in caplog.records if r.name == _MOD]
        assert record.getMessage() == (
            "ginlix-data financial_statements unavailable for 600519.SH: HTTPStatusError 503"
        )

    @pytest.mark.asyncio
    async def test_us_symbol_never_touches_tushare(self, monkeypatch):
        import plugins.langalpha_market_data.fundamentals_mcp_server as mod

        monkeypatch.setattr(mod, "_tushare_financial", None)
        client = _fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_financial_statements("AAPL", statement_type="income")

        assert_ok_envelope(result, symbol="AAPL", source="fmp")
        assert mod._tushare_financial is None  # lazy singleton never built


class TestOtherToolsCnBranch:
    @pytest.mark.asyncio
    async def test_cn_ratios_come_from_fmp_which_prices_the_valuation_fields(self):
        # The docstring promises marketCap, enterpriseValue and P/E; ginlix-data's
        # CN period rows carry none of them, so FMP answers first.
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_ratios

        ts = _ts_source()
        client = _fmp_client()
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_ratios("600519.SS")

        assert_ok_envelope(result, symbol="600519.SH", source="fmp")
        assert result["data"]["key_metrics"][0]["marketCap"] == 4.0
        ts.get_key_metrics_periods.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ratios_fall_back_to_tushare_when_fmp_cannot_answer(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_ratios

        ts = _ts_source()
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_financial_ratios("600519.SS", period="annual", limit=5)

        assert_ok_envelope(result, source="tushare")
        assert result["data"]["key_metrics"][0]["returnOnEquity"] == 0.26
        assert result["data"]["ratios"][0]["netProfitMargin"] == 0.5
        ts.get_key_metrics_periods.assert_awaited_once_with("600519.SH", period="annual", limit=5)

    @pytest.mark.asyncio
    async def test_growth_derives_income_statement_growth(self):
        from data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_growth_metrics

        growth = [{"date": "2024-12-31", "revenueGrowth": 0.15,
                   "netIncomeGrowth": 0.12, "epsgrowth": 0.10}]
        client = AsyncMock()
        client.get_fundamentals_v2 = AsyncMock(return_value=growth)
        ts = GinlixDataCnFinancialSource(client)
        with patch(f"{_MOD}._tushare_for", return_value=ts):
            result = await get_growth_metrics("600519.SS")

        assert_ok_envelope(result, source="tushare")
        assert result["data"]["financial_growth"] == growth
        isg = result["data"]["income_statement_growth"][0]
        assert isg == {
            "date": "2024-12-31", "growthRevenue": 0.15,
            "growthNetIncome": 0.12, "growthEPS": 0.10,
        }

    @pytest.mark.asyncio
    async def test_dividends_served_from_tushare(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_dividends_and_splits

        ts = _ts_source()
        with patch(f"{_MOD}._tushare_for", return_value=ts):
            result = await get_dividends_and_splits("600519.SS")

        assert_ok_envelope(result, source="tushare")
        assert result["data"]["dividends"][0]["dividend"] == 27.0

    @pytest.mark.asyncio
    async def test_empty_dividends_falls_through(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_dividends_and_splits

        ts = _ts_source(dividends={"dividends": [], "splits": []})
        client = _fmp_client()
        with patch(f"{_MOD}._tushare_for", return_value=ts), \
                patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_dividends_and_splits("600519.SS")

        assert_ok_envelope(result, source="fmp")

    @pytest.mark.asyncio
    async def test_shares_float_and_executives_served_from_tushare(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_key_executives, get_shares_float

        ts = _ts_source()
        with patch(f"{_MOD}._tushare_for", return_value=ts):
            float_result = await get_shares_float("600519.SS")
            exec_result = await get_key_executives("600519.SS")

        assert_ok_envelope(float_result, source="tushare", count=1)
        assert_ok_envelope(exec_result, source="tushare", count=1)
        assert exec_result["data"][0]["title"] == "Chairman"
