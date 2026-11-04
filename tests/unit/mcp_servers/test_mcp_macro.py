"""Tests for macro_mcp_server (standard market-data envelope)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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


class TestIndicatorSeries:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name, frequency",
        [
            ("GDP", "quarterly"),
            ("CPI", "monthly"),
            ("unemploymentRate", "monthly"),
            ("federalFundsRate", "monthly"),
            ("inflationRate", "daily"),
            ("retailSales", "monthly"),
            ("consumerSentiment", "monthly"),
            ("nonFarmPayrolls", "monthly"),
        ],
    )
    async def test_every_documented_name_says_what_it_is(self, name, frequency):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator(name)

        series = result["series"]
        assert series["country"] == "US"
        assert series["frequency"] == frequency
        assert series["unit"] and series["description"] and series["adjustment"]

    @pytest.mark.asyncio
    async def test_the_breakeven_is_not_described_as_cpi(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("inflationRate")

        assert "breakeven" in result["series"]["description"]
        assert "not CPI" in result["series"]["description"]

    @pytest.mark.asyncio
    async def test_a_name_without_a_description_carries_none(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("durableGoods")

        assert "series" not in result
        assert "missing_periods" not in result

    @pytest.mark.asyncio
    async def test_a_skipped_month_is_marked(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        client.get_economic_indicators = AsyncMock(return_value=[
            {"date": "2025-12-01", "value": 326.0},
            {"date": "2025-11-01", "value": 325.1},
            {"date": "2025-09-01", "value": 324.2},
            {"date": "2025-07-01", "value": 323.0},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("CPI")

        assert result["missing_periods"] == ["2025-10-01", "2025-08-01"]

    @pytest.mark.asyncio
    async def test_a_skipped_quarter_is_marked_and_a_whole_series_is_not(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        client.get_economic_indicators = AsyncMock(return_value=[
            {"date": "2026-07-01", "value": 3},
            {"date": "2026-01-01", "value": 1},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            gap = await get_economic_indicator("GDP")
        client.get_economic_indicators = AsyncMock(return_value=[
            {"date": "2026-07-01", "value": 3},
            {"date": "2026-04-01", "value": 2},
            {"date": "2026-01-01", "value": 1},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            whole = await get_economic_indicator("GDP")

        assert gap["missing_periods"] == ["2026-04-01"]
        assert "missing_periods" not in whole

    @pytest.mark.asyncio
    async def test_daily_series_have_no_gaps_to_mark(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        client.get_economic_indicators = AsyncMock(return_value=[
            {"date": "2026-11-02", "value": 2.29},
            {"date": "2026-10-30", "value": 2.34},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("inflationRate")

        assert "missing_periods" not in result

    @pytest.mark.asyncio
    async def test_an_unknown_name_reads_as_invalid_input(self):
        from data_client.fmp import FMPRequestError
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_indicator

        client = _make_fmp_client()
        client.get_economic_indicators = AsyncMock(
            side_effect=FMPRequestError("FMP has no economic indicator named 'PPI'", status_code=422)
        )
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_indicator("PPI")

        assert_error(result, "invalid_argument")
        assert result["detail"] == "FMP has no economic indicator named 'PPI'"


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
    async def test_default_dates_are_yesterday_through_a_week_after_it(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        yesterday_day = datetime.now(timezone.utc).date() - timedelta(days=1)
        yesterday = yesterday_day.isoformat()
        week = (yesterday_day + timedelta(days=7)).isoformat()
        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar()

        client.get_economic_calendar.assert_awaited_once_with(from_date=yesterday, to_date=week)
        assert result["from_date"] == yesterday
        assert result["to_date"] == week

    @pytest.mark.asyncio
    async def test_a_lone_future_start_reads_the_week_after_it(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(from_date="2099-01-01")

        client.get_economic_calendar.assert_awaited_once_with(
            from_date="2099-01-01", to_date="2099-01-08"
        )
        assert result["to_date"] == "2099-01-08"

    @pytest.mark.asyncio
    async def test_a_lone_past_end_reads_the_week_before_it(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(to_date="2020-01-31")

        client.get_economic_calendar.assert_awaited_once_with(
            from_date="2020-01-24", to_date="2020-01-31"
        )
        assert result["from_date"] == "2020-01-24"

    @pytest.mark.asyncio
    async def test_times_are_labeled_utc(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert result["timezone"] == "UTC"

    @pytest.mark.asyncio
    async def test_an_unreleased_event_has_no_change_percentage(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_economic_calendar

        client = _make_fmp_client()
        client.get_economic_calendar = AsyncMock(return_value=[
            {"event": "ISM Services PMI", "actual": None, "change": None, "changePercentage": 0},
            {"event": "Durable Goods", "actual": 2.3, "change": 2, "changePercentage": 666.667},
            {"event": "Holiday", "actual": None},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_economic_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert [e.get("changePercentage") for e in result["data"]] == [None, 666.667, None]
        assert "changePercentage" not in result["data"][2]


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

    @pytest.mark.asyncio
    async def test_no_end_reads_to_today(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_treasury_rates

        today = datetime.now(timezone.utc).date().isoformat()
        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_treasury_rates()

        client.get_treasury_rates.assert_awaited_once_with(from_date=None, to_date=today)
        assert result["from_date"] is None
        assert result["to_date"] == today


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
    async def test_names_its_source_and_says_it_is_undated(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_market_risk_premium

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_market_risk_premium()

        assert "Damodaran" in result["method"]
        assert result["as_of"] is None

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
    async def test_fmp_init_error(self):
        from plugins.langalpha_market_data.macro_mcp_server import get_earnings_calendar

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_earnings_calendar(from_date="2025-01-01", to_date="2025-01-31")

        assert_error(result, "client_unavailable")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool, fetch, args",
    [
        ("get_economic_indicator", "get_economic_indicators", {"name": "CPI"}),
        ("get_economic_calendar", "get_economic_calendar", {}),
        ("get_treasury_rates", "get_treasury_rates", {}),
        ("get_earnings_calendar", "get_earnings_calendar_by_date",
         {"from_date": "2026-10-01", "to_date": "2026-10-16"}),
    ],
)
async def test_a_read_the_call_cap_cut_says_so(tool, fetch, args):
    import plugins.langalpha_market_data.macro_mcp_server as mod
    from src.data_client.fmp.fmp_client import Walk

    client = _make_fmp_client()
    cut = Walk(await getattr(client, fetch)())
    cut.truncated = True
    setattr(client, fetch, AsyncMock(return_value=cut))
    with patch(f"{_MOD}.get_fmp_client", return_value=client):
        result = await getattr(mod, tool)(**args)

    assert_ok_envelope(result, source="fmp")
    assert result["truncated"] is True
    with patch(f"{_MOD}.get_fmp_client", return_value=_make_fmp_client()):
        assert "truncated" not in await getattr(mod, tool)(**args)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "method", "args"),
    [
        ("get_economic_indicator", "get_economic_indicators", ("CPI",)),
        ("get_economic_calendar", "get_economic_calendar", ("2025-01-01", "2025-01-31")),
        ("get_treasury_rates", "get_treasury_rates", ("2025-01-01", "2025-01-31")),
        ("get_earnings_calendar", "get_earnings_calendar_by_date", ("2025-01-01", "2025-01-31")),
    ],
)
async def test_a_refusal_fmp_answers_with_a_200_is_an_upstream_error(tool, method, args):
    import plugins.langalpha_market_data.macro_mcp_server as mod

    client = _make_fmp_client()
    setattr(client, method, AsyncMock(return_value={"Error Message": "Limit Reach"}))
    with patch(f"{_MOD}.get_fmp_client", return_value=client):
        result = await getattr(mod, tool)(*args)
    assert_error(result, "upstream_error")
