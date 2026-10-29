"""Tests for macro_mcp_server (standard market-data envelope)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from .conftest import assert_error, assert_ok_envelope

_MOD = "plugins.langalpha_market_data.macro_mcp_server"


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
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("GDP")

        assert_ok_envelope(result, count=2, source="fmp")
        assert result["data_type"] == "economic_indicator"
        assert result["indicator"] == "GDP"
        assert isinstance(result["count"], int)
        # macro tools without a symbol omit the symbol key entirely.
        assert "symbol" not in result
        client.get_economic_indicators.assert_awaited_once_with("GDP", limit=50)

    @pytest.mark.asyncio
    async def test_custom_limit(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_economic_indicator("CPI", limit=10)

        client.get_economic_indicators.assert_awaited_once_with("CPI", limit=10)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "documented, fmp",
        [
            ("federalFundsRate", "federalFunds"),
            ("nonFarmPayrolls", "totalNonfarmPayroll"),
        ],
    )
    async def test_documented_name_reaches_fmp_under_its_fmp_name(self, documented, fmp):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator(documented)

        client.get_economic_indicators.assert_awaited_once_with(fmp, limit=50)
        assert_ok_envelope(result, count=2, source="fmp")
        assert result["indicator"] == documented

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name",
        ["GDP", "CPI", "unemploymentRate", "inflationRate", "retailSales", "consumerSentiment", "federalFunds"],
    )
    async def test_every_other_name_reaches_fmp_as_given(self, name):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator(name, limit=7)

        client.get_economic_indicators.assert_awaited_once_with(name, limit=7)
        assert result["indicator"] == name

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_economic_indicator("GDP")

        assert_error(result, "client_unavailable", detail_excludes=("no key",))
        assert result["indicator"] == "GDP"

    @pytest.mark.asyncio
    async def test_api_error(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        client.get_economic_indicators = AsyncMock(side_effect=Exception("timeout at ?apikey=SECRET"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("GDP")

        assert_error(result, "upstream_error", detail_excludes=("SECRET",))
        assert result["indicator"] == "GDP"


# ---------------------------------------------------------------------------
# get_economic_calendar
# ---------------------------------------------------------------------------

class TestGetEconomicCalendar:
    @pytest.mark.asyncio
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert_ok_envelope(result, count=1, source="fmp")
        assert result["data_type"] == "economic_calendar"
        assert result["data"][0]["event"] == "GDP"
        client.get_economic_calendar.assert_awaited_once_with(
            from_date="2025-01-01", to_date="2025-01-31",
        )

    @pytest.mark.asyncio
    async def test_default_dates(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar()

        assert result["from_date"] is None
        assert result["to_date"] is None
        assert "truncated" not in result

    @pytest.mark.asyncio
    async def test_truncated_when_provider_cap_reached(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        client.get_economic_calendar = AsyncMock(return_value=[{}] * 4000)
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(from_date="2025-01-01", to_date="2025-12-31")

        assert result["count"] == 4000
        assert result["truncated"] is True
        assert "Narrow the date range" in result["note"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool, fetch",
    [
        ("get_economic_calendar", "get_economic_calendar"),
        ("get_treasury_rates", "get_treasury_rates"),
        ("get_earnings_calendar", "get_earnings_calendar_by_date"),
    ],
)
@pytest.mark.parametrize(
    "lo, hi, detail",
    [
        ("2026-10-16", "2026-10-09", "from is after to"),
        ("not-a-date", "2026-10-16", "from must be an ISO date, not 'not-a-date'"),
    ],
)
async def test_calendar_bounds_refused_before_fmp(tool, fetch, lo, hi, detail):
    import plugins.langalpha_market_data.macro_mcp_server as mod

    client = _make_fmp_client()
    with patch(f"{_MOD}.get_fmp_client", return_value=client):
        result = await getattr(mod, tool)(from_date=lo, to_date=hi)

    assert_error(result, "invalid_argument")
    assert result["detail"] == detail
    getattr(client, fetch).assert_not_awaited()


# ---------------------------------------------------------------------------
# get_treasury_rates
# ---------------------------------------------------------------------------

class TestGetTreasuryRates:
    @pytest.mark.asyncio
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_treasury_rates

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_treasury_rates()

        assert_ok_envelope(result, count=1)
        assert result["data_type"] == "treasury_rates"
        assert isinstance(result["count"], int)

    @pytest.mark.asyncio
    async def test_with_date_range(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_treasury_rates

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
        from plugins.langalpha_market_data.macro_mcp_server import get_market_risk_premium

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_market_risk_premium()

        assert_ok_envelope(result, count=1)
        assert result["data_type"] == "market_risk_premium"
        assert result["data"][0]["country"] == "United States"

    @pytest.mark.asyncio
    async def test_api_error(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_market_risk_premium

        client = _make_fmp_client()
        client.get_market_risk_premium = AsyncMock(side_effect=Exception("fail"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_market_risk_premium()

        assert_error(result, "upstream_error")


# ---------------------------------------------------------------------------
# get_earnings_calendar
# ---------------------------------------------------------------------------

class TestGetEarningsCalendar:
    @pytest.mark.asyncio
    async def test_success(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_earnings_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_calendar(from_date="2025-01-27", to_date="2025-01-31")

        assert_ok_envelope(result, count=2)
        assert result["data_type"] == "earnings_calendar"
        assert result["from_date"] == "2025-01-27"
        assert result["to_date"] == "2025-01-31"
        client.get_earnings_calendar_by_date.assert_awaited_once_with(
            from_date="2025-01-27", to_date="2025-01-31",
        )

    @pytest.mark.asyncio
    async def test_truncated_when_provider_cap_reached(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_earnings_calendar

        client = _make_fmp_client()
        client.get_earnings_calendar_by_date = AsyncMock(return_value=[{}] * 4000)
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_calendar("2025-01-01", "2025-01-31")

        assert result["count"] == 4000
        assert result["truncated"] is True
        assert "Narrow the date range" in result["note"]

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_earnings_calendar

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_earnings_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert_error(result, "client_unavailable")
