"""Integration tests for macro_mcp_server — hits real FMP API.

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
class TestEconomicIndicatorLive:
    async def test_gdp(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        result = await get_economic_indicator("GDP", limit=5)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert "date" in row
        assert "value" in row

    async def test_cpi(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        result = await get_economic_indicator("CPI", limit=3)
        assert "error" not in result, result.get("error")
        assert result["count"] > 0


@skip_no_fmp
class TestEconomicCalendarLive:
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        result = await get_economic_calendar(
            from_date="2025-03-01", to_date="2025-03-07",
        )
        assert "error" not in result, result.get("error")
        assert result["data_type"] == "economic_calendar"
        # Calendar may be empty on some date ranges, just check structure
        assert isinstance(result["data"], list)


@skip_no_fmp
class TestTreasuryRatesLive:
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_treasury_rates

        result = await get_treasury_rates(
            from_date="2025-03-01", to_date="2025-03-05",
        )
        assert "error" not in result, result.get("error")
        assert result["count"] > 0


@skip_no_fmp
class TestMarketRiskPremiumLive:
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_market_risk_premium

        result = await get_market_risk_premium()
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        # Find US entry
        us = [r for r in result["data"] if r.get("country") == "United States"]
        assert len(us) > 0


@skip_no_fmp
class TestEarningsCalendarLive:
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_earnings_calendar

        result = await get_earnings_calendar(
            from_date="2025-01-27", to_date="2025-01-31",
        )
        assert "error" not in result, result.get("error")
        assert result["count"] > 0
        row = result["data"][0]
        assert "symbol" in row
