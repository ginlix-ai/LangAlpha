"""Tests for macro_mcp_server."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

_MOD = "mcp_servers.macro_mcp_server"


def _make_fmp_client() -> AsyncMock:
    client = AsyncMock()
    client.get_economic_indicators = AsyncMock(return_value=[
        {"date": "2024-12-01", "value": 2.5},
        {"date": "2024-09-01", "value": 2.3},
    ])
    client.get_economic_calendar = AsyncMock(return_value=[
        {"event": "GDP", "date": "2025-01-30", "country": "US",
         "estimate": 2.5, "actual": 2.6, "previous": 2.4},
    ])
    client.get_treasury_rates = AsyncMock(return_value=[
        {"date": "2025-01-30", "month1": 4.3, "year10": 4.5, "year30": 4.7},
    ])
    client.get_market_risk_premium = AsyncMock(return_value=[
        {"country": "United States", "totalEquityRiskPremium": 5.5},
    ])
    client.get_earnings_calendar_by_date = AsyncMock(return_value=[
        {"symbol": "AAPL", "date": "2025-01-30", "epsEstimated": 2.35},
        {"symbol": "MSFT", "date": "2025-01-30", "epsEstimated": 3.10},
    ])
    return client


# ---------------------------------------------------------------------------
# get_economic_indicator
# ---------------------------------------------------------------------------

class TestGetEconomicIndicator:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("GDP")

        assert result["data_type"] == "economic_indicator"
        assert result["indicator"] == "GDP"
        assert result["count"] == 2
        assert result["source"] == "fmp"
        client.get_economic_indicators.assert_awaited_once_with("GDP", limit=50)

    @pytest.mark.asyncio
    async def test_custom_limit(self):
        from mcp_servers.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_economic_indicator("CPI", limit=10)

        client.get_economic_indicators.assert_awaited_once_with("CPI", limit=10)

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        from mcp_servers.macro_mcp_server import get_economic_indicator

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_economic_indicator("GDP")

        assert "error" in result

    @pytest.mark.asyncio
    async def test_api_error(self):
        from mcp_servers.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        client.get_economic_indicators = AsyncMock(side_effect=Exception("timeout"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("GDP")

        assert "error" in result


# ---------------------------------------------------------------------------
# get_economic_calendar
# ---------------------------------------------------------------------------

class TestGetEconomicCalendar:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert result["data_type"] == "economic_calendar"
        assert result["count"] == 1
        assert result["data"][0]["event"] == "GDP"
        client.get_economic_calendar.assert_awaited_once_with(
            from_date="2025-01-01", to_date="2025-01-31",
        )

    @pytest.mark.asyncio
    async def test_default_dates(self):
        from mcp_servers.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar()

        assert result["from_date"] is None
        assert result["to_date"] is None


# ---------------------------------------------------------------------------
# get_treasury_rates
# ---------------------------------------------------------------------------

class TestGetTreasuryRates:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.macro_mcp_server import get_treasury_rates

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_treasury_rates()

        assert result["data_type"] == "treasury_rates"
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_with_date_range(self):
        from mcp_servers.macro_mcp_server import get_treasury_rates

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_treasury_rates(from_date="2025-01-01", to_date="2025-01-31")

        client.get_treasury_rates.assert_awaited_once_with(
            from_date="2025-01-01", to_date="2025-01-31",
        )
        assert result["from_date"] == "2025-01-01"


# ---------------------------------------------------------------------------
# get_market_risk_premium
# ---------------------------------------------------------------------------

class TestGetMarketRiskPremium:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.macro_mcp_server import get_market_risk_premium

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_market_risk_premium()

        assert result["data_type"] == "market_risk_premium"
        assert result["count"] == 1
        assert result["data"][0]["country"] == "United States"

    @pytest.mark.asyncio
    async def test_api_error(self):
        from mcp_servers.macro_mcp_server import get_market_risk_premium

        client = _make_fmp_client()
        client.get_market_risk_premium = AsyncMock(side_effect=Exception("fail"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_market_risk_premium()

        assert "error" in result


# ---------------------------------------------------------------------------
# get_earnings_calendar
# ---------------------------------------------------------------------------

class TestGetEarningsCalendar:
    @pytest.mark.asyncio
    async def test_success(self):
        from mcp_servers.macro_mcp_server import get_earnings_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_calendar(from_date="2025-01-27", to_date="2025-01-31")

        assert result["data_type"] == "earnings_calendar"
        assert result["count"] == 2
        assert result["from_date"] == "2025-01-27"
        assert result["to_date"] == "2025-01-31"
        client.get_earnings_calendar_by_date.assert_awaited_once_with(
            from_date="2025-01-27", to_date="2025-01-31",
        )

    @pytest.mark.asyncio
    async def test_truncated_when_provider_cap_reached(self):
        from mcp_servers.macro_mcp_server import get_earnings_calendar

        client = _make_fmp_client()
        client.get_earnings_calendar_by_date = AsyncMock(return_value=[{}] * 4000)
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_calendar("2025-01-01", "2025-01-31")

        assert result["count"] == 4000
        assert result["truncated"] is True
        assert "Narrow the date range" in result["note"]

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        from mcp_servers.macro_mcp_server import get_earnings_calendar

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_earnings_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert "error" in result
