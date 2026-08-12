"""Tests for fundamentals_mcp_server (standard market-data envelope)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from .conftest import assert_error, assert_ok_envelope

_MOD = "mcp_servers.fundamentals_mcp_server"


def _make_fmp_client() -> AsyncMock:
    client = AsyncMock()
    client.get_income_statement = AsyncMock(return_value=[{"date": "2024-12-31", "revenue": 100}])
    client.get_balance_sheet = AsyncMock(return_value=[{"date": "2024-12-31", "totalAssets": 500}])
    client.get_cash_flow = AsyncMock(return_value=[{"date": "2024-12-31", "operatingCashFlow": 50}])
    client.get_key_metrics = AsyncMock(return_value=[{"date": "2024-12-31", "marketCap": 25}])
    client.get_financial_ratios = AsyncMock(return_value=[{"date": "2024-12-31", "returnOnEquity": 0.3}])
    client.get_financial_growth = AsyncMock(return_value=[{"date": "2024-12-31", "revenueGrowth": 0.1}])
    client.get_income_statement_growth = AsyncMock(return_value=[{"date": "2024-12-31", "growthRevenue": 0.1}])
    client.get_dcf = AsyncMock(return_value=[{"dcf": 180}])
    client.get_historical_dcf = AsyncMock(return_value=[])
    client.get_enterprise_value = AsyncMock(return_value=[{"date": "2024-12-31", "enterpriseValue": 3_000_000}])
    client.get_insider_trades = AsyncMock(return_value=[{"transactionDate": "2025-01-01", "transactionType": "S-Sale"}])
    client.get_insider_trade_stats = AsyncMock(return_value=[{"totalBought": 10, "totalSold": 5}])
    client.get_dividends = AsyncMock(return_value=[{"date": "2024-12-15", "dividend": 0.25}])
    client.get_splits = AsyncMock(return_value=[{"date": "2020-08-31", "numerator": 4, "denominator": 1}])
    client.get_shares_float = AsyncMock(return_value=[{"floatShares": 15_000_000_000}])
    client.get_key_executives = AsyncMock(return_value=[{"name": "Jane Doe", "title": "CEO"}])
    client.get_technical_indicator = AsyncMock(return_value=[{"date": "2025-01-01", "rsi": 55}])
    return client


# ---------------------------------------------------------------------------
# get_financial_statements
# ---------------------------------------------------------------------------

class TestGetFinancialStatements:
    @pytest.mark.asyncio
    async def test_income_only(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("AAPL", statement_type="income")

        assert_ok_envelope(result, symbol="AAPL", count=1, source="fmp")
        assert result["statement_type"] == "income"
        assert result["period"] == "annual"
        assert result["data_type"] == "financial_statements"
        assert isinstance(result["count"], int)
        assert isinstance(result["data"], list)
        client.get_income_statement.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_balance_only(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("AAPL", statement_type="balance")

        assert_ok_envelope(result, count=1)
        client.get_balance_sheet.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cash_only(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_financial_statements("AAPL", statement_type="cash")

        client.get_cash_flow.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_all_statements(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("AAPL", statement_type="all")

        # count is a PLAIN INT summed across the three sections, not a nested dict.
        assert_ok_envelope(result, count=3)
        assert "income_statement" in result["data"]
        assert "balance_sheet" in result["data"]
        assert "cash_flow" in result["data"]
        assert isinstance(result["count"], int)

    @pytest.mark.asyncio
    async def test_canonical_symbol_echo(self):
        """Echoed symbol is the canonical display spelling; upstream gets input."""
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("aapl", statement_type="income")

        assert_ok_envelope(result, symbol="AAPL")
        # Upstream receives the raw input spelling; pin only the symbol arg,
        # not the default period/limit.
        client.get_income_statement.assert_awaited_once()
        assert client.get_income_statement.await_args.args[0] == "aapl"

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_financial_statements("AAPL")

        # raw exception never leaks
        assert_error(result, "client_unavailable", symbol="AAPL", detail_excludes=("no key",))
        assert "data" not in result

    @pytest.mark.asyncio
    async def test_api_error(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        client.get_income_statement = AsyncMock(side_effect=Exception("timeout at https://fmp?apikey=SECRET"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("AAPL", statement_type="income")

        assert_error(
            result, "upstream_error", symbol="AAPL",
            detail_excludes=("SECRET", "apikey"),
        )


# ---------------------------------------------------------------------------
# get_financial_ratios
# ---------------------------------------------------------------------------

class TestGetFinancialRatios:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_ratios

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_ratios("AAPL")

        # count is a plain int (key_metrics + ratios), not a nested dict.
        assert_ok_envelope(result, symbol="AAPL", count=2)
        assert result["data_type"] == "financial_ratios"
        assert "key_metrics" in result["data"]
        assert "ratios" in result["data"]
        assert isinstance(result["count"], int)

    @pytest.mark.asyncio
    async def test_quarterly(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_ratios

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_ratios("AAPL", period="quarter", limit=4)

        assert result["period"] == "quarter"
        client.get_key_metrics.assert_awaited_once_with("AAPL", period="quarter", limit=4)


# ---------------------------------------------------------------------------
# get_growth_metrics
# ---------------------------------------------------------------------------

class TestGetGrowthMetrics:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_growth_metrics

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_growth_metrics("AAPL")

        assert_ok_envelope(result, count=2)
        assert result["data_type"] == "growth_metrics"
        assert "financial_growth" in result["data"]
        assert "income_statement_growth" in result["data"]
        assert isinstance(result["count"], int)


# ---------------------------------------------------------------------------
# get_historical_valuation
# ---------------------------------------------------------------------------

class TestGetHistoricalValuation:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_historical_valuation

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_historical_valuation("AAPL")

        assert_ok_envelope(result, count=2)
        assert result["data_type"] == "historical_valuation"
        assert "current_dcf" in result["data"]
        assert "historical_dcf" in result["data"]
        assert "enterprise_value" in result["data"]
        # historical_dcf is [] (stable API dropped it): count = 1 + 0 + 1.
        assert result["data"]["historical_dcf"] == []
        assert isinstance(result["count"], int)


# ---------------------------------------------------------------------------
# get_insider_trades
# ---------------------------------------------------------------------------

class TestGetInsiderTrades:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_insider_trades

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_insider_trades("AAPL")

        assert_ok_envelope(result, count=2)
        assert result["data_type"] == "insider_trades"
        assert "trades" in result["data"]
        assert "stats" in result["data"]
        assert isinstance(result["count"], int)

    @pytest.mark.asyncio
    async def test_custom_limit(self):
        from mcp_servers.fundamentals_mcp_server import get_insider_trades

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_insider_trades("AAPL", limit=10)

        client.get_insider_trades.assert_awaited_once_with("AAPL", limit=10)


# ---------------------------------------------------------------------------
# get_dividends_and_splits
# ---------------------------------------------------------------------------

class TestGetDividendsAndSplits:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_dividends_and_splits

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_dividends_and_splits("AAPL")

        assert_ok_envelope(result, count=2)
        assert result["data_type"] == "dividends_and_splits"
        assert "dividends" in result["data"]
        assert "splits" in result["data"]
        assert isinstance(result["count"], int)


# ---------------------------------------------------------------------------
# get_shares_float
# ---------------------------------------------------------------------------

class TestGetSharesFloat:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_shares_float

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_shares_float("AAPL")

        assert_ok_envelope(result, count=1)
        assert result["data_type"] == "shares_float"
        assert isinstance(result["data"], list)


# ---------------------------------------------------------------------------
# get_key_executives
# ---------------------------------------------------------------------------

class TestGetKeyExecutives:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_key_executives

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_key_executives("AAPL")

        assert_ok_envelope(result, count=1)
        assert result["data_type"] == "key_executives"
        assert result["data"][0]["name"] == "Jane Doe"


# ---------------------------------------------------------------------------
# get_technical_indicator
# ---------------------------------------------------------------------------

class TestGetTechnicalIndicator:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_technical_indicator("AAPL", indicator="rsi")

        assert_ok_envelope(result, symbol="AAPL", count=1)
        assert result["data_type"] == "technical_indicator"
        assert result["indicator"] == "rsi"
        assert result["period"] == 14
        assert result["timeframe"] == "1day"
        client.get_technical_indicator.assert_awaited_once_with(
            "AAPL", indicator="rsi", period=14, timeframe="1day",
        )

    @pytest.mark.asyncio
    async def test_custom_params(self):
        from mcp_servers.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_technical_indicator("AAPL", indicator="ema", period=50, timeframe="1hour")

        assert result["indicator"] == "ema"
        assert result["period"] == 50
        assert result["timeframe"] == "1hour"
