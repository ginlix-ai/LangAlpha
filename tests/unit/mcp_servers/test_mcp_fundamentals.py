"""Tests for fundamentals_mcp_server (standard market-data envelope)."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from .conftest import assert_error, assert_ok_envelope, frozen_clock

_MOD = "plugins.langalpha_market_data.fundamentals_mcp_server"


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
    client.get_insider_trade_stats = AsyncMock(return_value=[{"totalPurchases": 10, "totalSales": 5}])
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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("AAPL", statement_type="balance")

        assert_ok_envelope(result, count=1)
        client.get_balance_sheet.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cash_only(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_financial_statements("AAPL", statement_type="cash")

        client.get_cash_flow.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_all_statements(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

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
        """Echoed and forwarded in the display spelling, whatever was typed."""
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_financial_statements("aapl", statement_type="income")

        assert_ok_envelope(result, symbol="AAPL")
        # The FMP client respells the suffix itself (.SH -> .SS); pin only the
        # symbol arg, not the default period/limit.
        client.get_income_statement.assert_awaited_once()
        assert client.get_income_statement.await_args.args[0] == "AAPL"

    @pytest.mark.asyncio
    async def test_canonical_instrument_key_reaches_fmp_as_a_ticker(self):
        """FMP cannot read ``AAPL.XNAS``; it gets the ticker the key names."""
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_technical_indicator("AAPL.XNAS", indicator="rsi")

        assert_ok_envelope(result, symbol="AAPL")
        assert client.get_technical_indicator.await_args.args[0] == "AAPL"

    @pytest.mark.asyncio
    async def test_fmp_init_error(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

        with patch(f"{_MOD}.get_fmp_client", side_effect=RuntimeError("no key")):
            result = await get_financial_statements("AAPL")

        # raw exception never leaks
        assert_error(result, "client_unavailable", symbol="AAPL", detail_excludes=("no key",))
        assert "data" not in result

    @pytest.mark.asyncio
    async def test_api_error(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_statements

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_ratios

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_financial_ratios

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_growth_metrics

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_historical_valuation

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

    @pytest.mark.asyncio
    async def test_each_price_names_its_currency(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_historical_valuation

        client = _make_fmp_client()
        client.get_dcf = AsyncMock(return_value=[
            {"symbol": "TSM", "date": "2026-11-03", "dcf": 124.03, "Stock Price": 406.41},
        ])
        client.get_enterprise_value = AsyncMock(return_value=[
            {"date": "2025-12-31", "stockPrice": 9521.48},
            {"date": "2024-12-31", "stockPrice": 6486.4},
        ])
        client.get_key_metrics = AsyncMock(return_value=[
            {"date": "2025-12-31", "reportedCurrency": "TWD"},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_historical_valuation("TSM")

        assert result["currency"] == "USD"
        assert result["data"]["current_dcf"] == [
            {"symbol": "TSM", "date": "2026-11-03", "dcf": 124.03, "stockPrice": 406.41},
        ]
        assert [r["reportedCurrency"] for r in result["data"]["enterprise_value"]] == ["TWD", None]

    @pytest.mark.asyncio
    async def test_an_unknown_venue_names_no_currency(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_historical_valuation

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            foreign = await get_historical_valuation("PETR4.SA")
            class_share = await get_historical_valuation("BRK.B")

        assert "currency" not in foreign
        assert class_share["currency"] == "USD"

    @pytest.mark.asyncio
    async def test_a_key_metrics_miss_leaves_the_rows_unlabelled(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_historical_valuation

        client = _make_fmp_client()
        client.get_key_metrics = AsyncMock(side_effect=RuntimeError("429"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_historical_valuation("AAPL")

        assert result["data"]["enterprise_value"][0]["reportedCurrency"] is None
        assert result["data"]["enterprise_value"][0]["enterpriseValue"] == 3_000_000

    @pytest.mark.asyncio
    async def test_a_pence_listing_prices_its_dcf_in_pounds(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_historical_valuation

        client = _make_fmp_client()
        client.get_dcf = AsyncMock(return_value=[
            {"symbol": "VOD.L", "date": "2026-11-03", "dcf": 486.29, "Stock Price": 117.2},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_historical_valuation("VOD.L")

        assert result["currency"] == "GBP"
        row = result["data"]["current_dcf"][0]
        assert (row["dcf"], row["stockPrice"]) == (pytest.approx(4.8629), pytest.approx(1.172))


# ---------------------------------------------------------------------------
# get_insider_trades
# ---------------------------------------------------------------------------

class TestGetInsiderTrades:
    @pytest.mark.asyncio
    async def test_success(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_insider_trades

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_insider_trades

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_dividends_and_splits

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_shares_float

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_shares_float("AAPL")

        assert_ok_envelope(result, count=1)
        assert result["data_type"] == "shares_float"
        assert isinstance(result["data"], list)

    @pytest.mark.asyncio
    async def test_the_refresh_stamp_is_cut_to_its_date(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_shares_float

        client = _make_fmp_client()
        client.get_shares_float = AsyncMock(return_value=[
            {"symbol": "AAPL", "date": "2026-11-04 04:17:50", "floatShares": 14_569_078_010},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_shares_float("AAPL")

        assert result["data"][0]["date"] == "2026-11-04"


# ---------------------------------------------------------------------------
# get_key_executives
# ---------------------------------------------------------------------------

class TestGetKeyExecutives:
    @pytest.mark.asyncio
    async def test_success(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_key_executives

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
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

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
    async def test_newest_bar_of_a_running_session_is_in_progress(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        client.get_technical_indicator = AsyncMock(return_value=[
            {"date": "2025-01-03 00:00:00", "close": 2, "rsi": 55},
            {"date": "2025-01-02 00:00:00", "close": 1, "rsi": 50},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client), \
                frozen_clock(datetime(2025, 1, 3, 11, 0, tzinfo=ZoneInfo("America/New_York"))):
            result = await get_technical_indicator("AAPL", indicator="rsi")

        assert result["price_treatment"] == "split_adjusted"
        assert result["in_progress"] == "2025-01-03"
        assert [r["date"] for r in result["data"]] == ["2025-01-03", "2025-01-02"]

    @pytest.mark.asyncio
    async def test_caret_index_reaches_fmp_with_its_caret(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_technical_indicator("^GSPC", indicator="rsi")

        assert client.get_technical_indicator.await_args.args[0] == "^GSPC"

    @pytest.mark.asyncio
    async def test_canonical_index_key_reaches_fmp_with_its_caret(self):
        """The display spelling ``SPX`` is not one FMP reads."""
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_technical_indicator("SPX.INDEX", indicator="rsi")

        assert client.get_technical_indicator.await_args.args[0] == "^GSPC"

    @pytest.mark.asyncio
    async def test_ticker_colliding_with_an_index_alias_stays_the_company(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            await get_technical_indicator("COMP", indicator="rsi")

        assert client.get_technical_indicator.await_args.args[0] == "COMP"

    @pytest.mark.asyncio
    async def test_custom_params(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_technical_indicator("AAPL", indicator="ema", period=50, timeframe="1hour")

        assert result["indicator"] == "ema"
        assert result["period"] == 50
        assert result["timeframe"] == "1hour"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("timeframe", ["2day", "1d", "1h", "1HOUR", "1week", ""])
    async def test_unknown_timeframe_is_refused_before_fmp(self, timeframe):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_technical_indicator("AAPL", indicator="rsi", timeframe=timeframe)

        # The hosted tool's envelope, word for word.
        assert result == {
            "error": "invalid_argument",
            "detail": (
                "timeframe must be one of 1min, 5min, 15min, 30min, 1hour, 4hour, 1day, "
                f"not {timeframe!r}"
            ),
            "symbol": "AAPL",
        }
        client.get_technical_indicator.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("timeframe", ["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"])
    async def test_every_bar_size_reaches_fmp(self, timeframe):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_technical_indicator

        client = _make_fmp_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_technical_indicator("AAPL", indicator="rsi", timeframe=timeframe)

        assert result["timeframe"] == timeframe
        assert client.get_technical_indicator.await_args.kwargs["timeframe"] == timeframe


# ---------------------------------------------------------------------------
# analyst tools
# ---------------------------------------------------------------------------

def _analyst_client() -> AsyncMock:
    client = _make_fmp_client()
    client.get_price_target_consensus = AsyncMock(return_value=[
        {"symbol": "AAPL", "targetHigh": 300, "targetLow": 200, "targetConsensus": 250.5, "targetMedian": 255},
    ])
    client.get_grades_historical = AsyncMock(return_value=[
        {"symbol": "AAPL", "date": "2026-02-01", "analystRatingsStrongBuy": 2, "analystRatingsBuy": 10,
         "analystRatingsHold": 5, "analystRatingsSell": 1, "analystRatingsStrongSell": 0},
    ])
    client.get_stock_grades = AsyncMock(return_value=[
        {"symbol": "AAPL", "date": "2026-01-20", "gradingCompany": "Firm A", "previousGrade": "Hold",
         "newGrade": "Buy", "action": "upgrade"},
    ])
    client.get_price_target_news = AsyncMock(return_value=[
        {"symbol": "AAPL", "publishedDate": "2026-01-15T10:30:00.000Z", "newsURL": "https://example.com/a",
         "newsTitle": "Firm B raises target", "analystName": "", "priceTarget": 260, "adjPriceTarget": 260,
         "priceWhenPosted": 240.5, "newsPublisher": "Example", "newsBaseURL": "example.com",
         "analystCompany": "Firm B"},
    ])
    client.get_analyst_estimates = AsyncMock(return_value=[
        {"symbol": "AAPL", "date": "2028-12-31", "revenueLow": 0, "revenueHigh": 0, "revenueAvg": 0,
         "ebitdaLow": 5, "ebitdaHigh": 7, "ebitdaAvg": 6, "ebitLow": 0, "ebitHigh": 0, "ebitAvg": 0,
         "netIncomeLow": 3, "netIncomeHigh": 4, "netIncomeAvg": 3.5, "sgaExpenseLow": 1, "sgaExpenseHigh": 1,
         "sgaExpenseAvg": 1, "epsAvg": 1.2, "epsHigh": 1.4, "epsLow": 1.0, "numAnalystsRevenue": 0,
         "numAnalystsEps": 0},
    ])
    client.get_earnings_report = AsyncMock(return_value=[
        {"symbol": "AAPL", "date": "2099-01-28", "epsActual": None, "epsEstimated": 1.5,
         "revenueActual": None, "revenueEstimated": 100, "lastUpdated": "2026-11-01"},
        {"symbol": "AAPL", "date": "2026-10-29", "epsActual": 1.4, "epsEstimated": 1.3,
         "revenueActual": 95, "revenueEstimated": 90, "lastUpdated": "2026-11-01"},
        {"symbol": "AAPL", "date": "2026-07-30", "epsActual": 1.2, "epsEstimated": 1.25,
         "revenueActual": 88, "revenueEstimated": 89, "lastUpdated": "2026-11-01"},
    ])
    client.get_income_statement = AsyncMock(return_value=[
        {"date": "2026-09-27", "period": "Q4", "reportedCurrency": "USD"},
        {"date": "2026-06-28", "period": "Q3", "reportedCurrency": "USD"},
    ])
    return client


class TestGetAnalystRatings:
    @pytest.mark.asyncio
    async def test_each_list_keeps_its_documented_fields(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL", limit=5)

        assert_ok_envelope(result, symbol="AAPL", source="fmp", currency="USD", count=4)
        data = result["data"]
        assert data["target_consensus"] == [{"high": 300, "low": 200, "median": 255, "mean": 250.5}]
        assert data["rating_counts"] == [
            {"date": "2026-02-01", "strongBuy": 2, "buy": 10, "hold": 5, "sell": 1, "strongSell": 0},
        ]
        assert data["rating_actions"] == [{
            "date": "2026-01-20", "gradingCompany": "Firm A", "previousGrade": "Hold",
            "newGrade": "Buy", "action": "upgrade",
        }]
        assert data["price_targets"] == [{
            "date": "2026-01-15", "analystCompany": "Firm B", "priceTarget": 260, "priceWhenPosted": 240.5,
            "newsTitle": "Firm B raises target", "newsURL": "https://example.com/a",
        }]
        for read in (client.get_grades_historical, client.get_stock_grades, client.get_price_target_news):
            assert read.await_args.kwargs["limit"] == 5

    @pytest.mark.asyncio
    async def test_every_list_is_cut_to_limit(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        days = range(20, 0, -1)
        # FMP answers every grade whatever limit asks for.
        client.get_stock_grades = AsyncMock(return_value=[{"date": f"2026-01-{d:02d}"} for d in days])
        client.get_grades_historical = AsyncMock(return_value=[{"date": f"2026-01-{d:02d}"} for d in days])
        client.get_price_target_news = AsyncMock(return_value=[
            {"publishedDate": f"2026-01-{d:02d}T15:00:00.000Z"} for d in days
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL", limit=5)

        expected = ["2026-01-20", "2026-01-19", "2026-01-18", "2026-01-17", "2026-01-16"]
        for name in ("rating_counts", "rating_actions", "price_targets"):
            assert [row["date"] for row in result["data"][name]] == expected, name

    @pytest.mark.asyncio
    async def test_a_target_takes_the_day_it_was_published_where_the_listing_trades(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        client.get_price_target_news = AsyncMock(return_value=[
            {"publishedDate": "2026-01-15T00:30:00.000Z", "adjPriceTarget": 260, "analystCompany": "Firm B"},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL")

        assert result["data"]["price_targets"][0]["date"] == "2026-01-14"

    @pytest.mark.asyncio
    async def test_a_target_set_before_a_split_keeps_fmps_adjusted_prices(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        # FMP adjusts the price when posted itself; only the target has a raw twin.
        client.get_price_target_news = AsyncMock(return_value=[
            {"publishedDate": "2024-06-04T00:00:00.000Z", "priceTarget": 1400, "adjPriceTarget": 140,
             "priceWhenPosted": 115, "analystCompany": "Firm C"},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL")

        target = result["data"]["price_targets"][0]
        assert (target["priceTarget"], target["priceWhenPosted"]) == (140, 115)

    @pytest.mark.asyncio
    async def test_pence_listing_prices_in_pounds(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("VOD.L")

        assert result["currency"] == "GBP"
        assert result["data"]["target_consensus"][0]["high"] == 3
        assert result["data"]["price_targets"][0]["priceWhenPosted"] == 2.405

    @pytest.mark.asyncio
    async def test_no_consensus_is_an_empty_list(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        client.get_price_target_consensus = AsyncMock(return_value=[])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL")

        assert result["data"]["target_consensus"] == []
        assert result["count"] == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, 101])
    async def test_limit_out_of_range_is_refused_before_fmp(self, limit):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL", limit=limit)

        assert result == {
            "error": "invalid_argument",
            "detail": f"limit must be between 1 and 100, not {limit}",
            "symbol": "AAPL",
        }
        client.get_stock_grades.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_upstream_failure_is_an_error(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_ratings

        client = _analyst_client()
        client.get_stock_grades = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_ratings("AAPL")

        assert_error(result, "upstream_error", symbol="AAPL", detail_excludes=("boom",))


class TestGetAnalystEstimates:
    @pytest.mark.asyncio
    async def test_missing_metrics_are_null_and_currency_is_the_filings(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_estimates

        client = _analyst_client()
        client.get_income_statement = AsyncMock(return_value=[{"date": "2025-12-31", "reportedCurrency": "TWD"}])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_estimates("TSM", period="quarter", limit=4)

        assert_ok_envelope(result, symbol="TSM", currency="TWD", count=1)
        assert result["period"] == "quarter"
        row = result["data"][0]
        assert "symbol" not in row
        # No analysts: no consensus. All three zero where FMP counts nobody: none either.
        assert (row["revenueAvg"], row["revenueLow"], row["revenueHigh"]) == (None, None, None)
        assert (row["ebitAvg"], row["epsAvg"], row["epsLow"], row["epsHigh"]) == (None, None, None, None)
        assert (row["ebitdaAvg"], row["netIncomeAvg"], row["sgaExpenseAvg"]) == (6, 3.5, 1)
        client.get_analyst_estimates.assert_awaited_once_with("TSM", period="quarter", limit=4)

    @pytest.mark.asyncio
    async def test_a_zero_consensus_analysts_agree_on_stays(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_estimates

        client = _analyst_client()
        client.get_analyst_estimates = AsyncMock(return_value=[
            {"date": "2028-12-31", "epsAvg": 0, "epsLow": 0, "epsHigh": 0, "numAnalystsEps": 3},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_estimates("AAPL")

        row = result["data"][0]
        assert (row["epsAvg"], row["epsLow"], row["epsHigh"]) == (0, 0, 0)

    @pytest.mark.asyncio
    async def test_statement_failure_only_drops_the_currency(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_analyst_estimates

        client = _analyst_client()
        client.get_income_statement = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_analyst_estimates("AAPL")

        assert_ok_envelope(result, count=1)
        assert "currency" not in result

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool", ["get_analyst_estimates", "get_earnings_surprises"])
    async def test_a_limit_below_one_is_refused_before_fmp(self, tool):
        from plugins.langalpha_market_data import fundamentals_mcp_server

        client = _analyst_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await getattr(fundamentals_mcp_server, tool)("AAPL", limit=0)

        assert result == {"error": "invalid_argument", "detail": "limit must be at least 1, not 0", "symbol": "AAPL"}
        client.get_income_statement.assert_not_awaited()


class TestGetEarningsSurprises:
    @pytest.mark.asyncio
    async def test_reports_take_their_filed_quarter(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("AAPL", limit=3)

        assert_ok_envelope(result, symbol="AAPL", currency="USD", count=3)
        assert [(r["date"], r["fiscalDateEnding"]) for r in result["data"]] == [
            ("2099-01-28", None),
            ("2026-10-29", "2026-09-27"),
            ("2026-07-30", "2026-06-28"),
        ]
        assert set(result["data"][1]) == {
            "date", "fiscalDateEnding", "epsActual", "epsEstimated", "revenueActual", "revenueEstimated",
        }
        client.get_income_statement.assert_awaited_once_with("AAPL", period="quarter", limit=5)

    @pytest.mark.asyncio
    async def test_a_report_on_a_quarter_not_filed_yet_is_unmatched(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        client.get_income_statement = AsyncMock(return_value=[{"date": "2026-06-28", "reportedCurrency": "USD"}])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("AAPL")

        assert [r["fiscalDateEnding"] for r in result["data"]] == [None, None, "2026-06-28"]

    @pytest.mark.asyncio
    async def test_an_annual_report_after_the_next_quarter_ends_keeps_its_own_quarter(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        client.get_earnings_report = AsyncMock(return_value=[
            {"date": "2026-04-24", "epsActual": 2.0, "epsEstimated": 2.1},
            {"date": "2026-04-17", "epsActual": 1.4, "epsEstimated": 1.8},
            {"date": "2025-10-29", "epsActual": 1.5, "epsEstimated": 1.6},
        ])
        client.get_income_statement = AsyncMock(return_value=[
            {"date": "2026-03-31", "period": "Q1", "reportedCurrency": "CNY"},
            {"date": "2025-12-31", "period": "Q4", "reportedCurrency": "CNY"},
            {"date": "2025-09-30", "period": "Q3", "reportedCurrency": "CNY"},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("600519.SH")

        assert [r["fiscalDateEnding"] for r in result["data"]] == ["2026-03-31", "2025-12-31", "2025-09-30"]
        assert "currency" not in result

    @pytest.mark.asyncio
    async def test_a_past_report_never_filled_in_is_dropped(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        client.get_earnings_report = AsyncMock(return_value=[
            {"date": "2026-10-29", "epsActual": 1.4, "epsEstimated": 1.3},
            {"date": "2026-08-01", "epsActual": None, "epsEstimated": 1.2, "revenueActual": None},
            {"date": "2026-07-30", "epsActual": 1.2, "epsEstimated": 1.25},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("AAPL")

        assert [r["date"] for r in result["data"]] == ["2026-10-29", "2026-07-30"]

    @pytest.mark.asyncio
    async def test_a_report_with_revenue_but_no_eps_takes_its_quarter(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        client.get_earnings_report = AsyncMock(return_value=[
            {"date": "2026-10-29", "epsActual": None, "epsEstimated": 1.3, "revenueActual": 95},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("AAPL")

        assert result["data"][0]["fiscalDateEnding"] == "2026-09-27"

    @pytest.mark.asyncio
    async def test_two_reports_on_one_day_are_both_unmatched(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        client.get_earnings_report = AsyncMock(return_value=[
            {"date": "2026-10-29", "epsActual": 1.4, "epsEstimated": 1.3},
            {"date": "2026-10-29", "epsActual": 1.1, "epsEstimated": 1.0},
        ])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("AAPL")

        assert [r["fiscalDateEnding"] for r in result["data"]] == [None, None]

    @pytest.mark.asyncio
    async def test_a_us_listing_filing_in_another_currency_names_none(self):
        from plugins.langalpha_market_data.fundamentals_mcp_server import get_earnings_surprises

        client = _analyst_client()
        client.get_income_statement = AsyncMock(return_value=[{"date": "2026-07-31", "reportedCurrency": "CAD"}])
        with patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await get_earnings_surprises("RY")

        assert_ok_envelope(result, count=3)
        assert "currency" not in result
