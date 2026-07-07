"""Integration tests for fundamentals_mcp_server — hits real FMP API.

Run with:  uv run python -m pytest tests/integration/ -m integration -v
Requires:  FMP_API_KEY
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_has_fmp = bool(os.getenv("FMP_API_KEY"))
skip_no_fmp = pytest.mark.skipif(not _has_fmp, reason="FMP_API_KEY not set")


@skip_no_fmp
class TestFinancialStatementsLive:
    async def test_income(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        result = await get_financial_statements("AAPL", statement_type="income", limit=2)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert "revenue" in row or "Revenue" in str(row)

    async def test_all(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_statements

        result = await get_financial_statements("MSFT", statement_type="all", limit=1)
        assert "error" not in result, result.get("error")
        # "all" → data is a dict of the three statement lists; count is the int total.
        data = result["data"]
        assert len(data["income_statement"]) >= 1
        assert len(data["balance_sheet"]) >= 1
        assert len(data["cash_flow"]) >= 1
        assert result["count"] == (
            len(data["income_statement"])
            + len(data["balance_sheet"])
            + len(data["cash_flow"])
        )


@skip_no_fmp
class TestFinancialRatiosLive:
    async def test_annual(self):
        from mcp_servers.fundamentals_mcp_server import get_financial_ratios

        result = await get_financial_ratios("AAPL", limit=3)
        assert "error" not in result, result.get("error")
        assert len(result["data"]["key_metrics"]) > 0
        assert len(result["data"]["ratios"]) > 0


@skip_no_fmp
class TestGrowthMetricsLive:
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_growth_metrics

        result = await get_growth_metrics("AAPL", limit=3)
        assert "error" not in result, result.get("error")
        assert len(result["data"]["financial_growth"]) > 0


@skip_no_fmp
class TestHistoricalValuationLive:
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_historical_valuation

        result = await get_historical_valuation("AAPL", limit=2)
        assert "error" not in result, result.get("error")
        assert "current_dcf" in result["data"]
        assert "enterprise_value" in result["data"]


@skip_no_fmp
class TestInsiderTradesLive:
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_insider_trades

        result = await get_insider_trades("AAPL", limit=5)
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "insider_trades"


@skip_no_fmp
class TestDividendsAndSplitsLive:
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_dividends_and_splits

        result = await get_dividends_and_splits("AAPL")
        assert "error" not in result, result.get("error")
        assert len(result["data"]["dividends"]) > 0
        assert len(result["data"]["splits"]) > 0


@skip_no_fmp
class TestSharesFloatLive:
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_shares_float

        result = await get_shares_float("AAPL")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0


@skip_no_fmp
class TestKeyExecutivesLive:
    async def test_success(self):
        from mcp_servers.fundamentals_mcp_server import get_key_executives

        result = await get_key_executives("AAPL")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        assert "name" in result["data"][0]


@skip_no_fmp
class TestTechnicalIndicatorLive:
    async def test_rsi(self):
        from mcp_servers.fundamentals_mcp_server import get_technical_indicator

        result = await get_technical_indicator("AAPL", indicator="rsi")
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        assert result["indicator"] == "rsi"
