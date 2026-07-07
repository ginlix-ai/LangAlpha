"""Regression tests for market_data tool implementation functions."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.market_data.implementations import (
    _calculate_price_statistics,
    _format_price_data_as_table,
    _format_price_summary,
    fetch_company_overview,
    fetch_market_indices,
    fetch_market_movers,
    fetch_options_chain,
    fetch_sector_performance,
    fetch_stock_daily_prices,
    fetch_stock_screener,
)

_MOD = "src.tools.market_data.implementations"

# ---------------------------------------------------------------------------
# Helpers — canned data
# ---------------------------------------------------------------------------

def _make_daily_records(n: int, symbol: str = "AAPL", base_price: float = 150.0):
    """Generate n canned daily OHLCV records (newest-first) in formatter format."""
    records = []
    for i in range(n):
        day_offset = n - 1 - i
        price = base_price + day_offset * 0.5
        records.append({
            "date": f"2025-01-{(day_offset + 1):02d}",
            "symbol": symbol,
            "open": price,
            "high": price + 2.0,
            "low": price - 1.0,
            "close": price + 1.0,
            "volume": 1_000_000 + i * 100_000,
            "change": 1.0,
            "changePercent": 0.67,
            "vwap": price + 0.5,
        })
    return records


def _make_provider_bars(n: int, base_price: float = 150.0):
    """Generate n canned OHLCV bars in MarketDataSource format {time, open, high, low, close, volume}."""
    from datetime import timedelta

    base_dt = datetime(2025, 1, 1, 14, 30, tzinfo=timezone.utc)
    bars = []
    for i in range(n):
        price = base_price + i * 0.5
        ts = int((base_dt + timedelta(days=i)).timestamp() * 1000)
        bars.append({
            "time": ts,
            "open": price,
            "high": price + 2.0,
            "low": price - 1.0,
            "close": price + 1.0,
            "volume": 1_000_000 + i * 100_000,
        })
    return bars


def _make_fake_market_provider(*, daily_bars=None, intraday_bars=None):
    """Build a mock MarketDataProvider."""
    provider = AsyncMock()
    provider.get_daily = AsyncMock(return_value=daily_bars or [])
    provider.get_intraday = AsyncMock(return_value=intraday_bars or [])
    return provider


def _make_fake_financial_source(
    *,
    profile_data=None,
    income_stmt=None,
    earnings_calendar=None,
    price_change=None,
    key_metrics=None,
    ratios=None,
    price_target_consensus=None,
    grades_summary=None,
    product_data=None,
    geo_data=None,
    quote_data=None,
    cash_flow=None,
    screener_results=None,
    sector_data=None,
):
    """Build a mock FinancialDataSource."""
    src = AsyncMock()
    src.get_company_profile = AsyncMock(return_value=profile_data)
    src.get_income_statements = AsyncMock(return_value=income_stmt or [])
    src.get_earnings_history = AsyncMock(return_value=earnings_calendar or [])
    src.get_price_performance = AsyncMock(return_value=price_change or [])
    src.get_key_metrics = AsyncMock(return_value=key_metrics or [])
    src.get_financial_ratios = AsyncMock(return_value=ratios or [])
    src.get_analyst_price_targets = AsyncMock(return_value=price_target_consensus or [])
    src.get_analyst_ratings = AsyncMock(return_value=grades_summary or [])
    src.get_revenue_by_segment = AsyncMock(return_value=product_data or [])
    src.get_realtime_quote = AsyncMock(return_value=quote_data or [])
    src.get_cash_flows = AsyncMock(return_value=cash_flow or [])
    src.screen_stocks = AsyncMock(return_value=screener_results or [])
    src.get_sector_performance = AsyncMock(return_value=sector_data or [])
    return src


def _make_fake_financial_provider(financial=None, intel=None):
    """Build a mock FinancialDataProvider composite."""
    provider = MagicMock()
    provider.financial = financial
    provider.intel = intel
    return provider


# ---------------------------------------------------------------------------
# Pure helper tests (no mocking needed)
# ---------------------------------------------------------------------------

class TestCalculatePriceStatistics:
    def test_empty_data_returns_empty(self):
        assert _calculate_price_statistics([]) == {}
        assert _calculate_price_statistics(None) == {}

    def test_single_record(self):
        data = _make_daily_records(1)
        stats = _calculate_price_statistics(data)
        assert stats["period_days"] == 1
        assert stats["symbol"] == "AAPL"
        assert stats["period_open"] is not None
        assert stats["period_close"] is not None
        assert stats["volatility"] is None  # need >=2 points

    def test_basic_stats_with_20_records(self):
        data = _make_daily_records(20)
        stats = _calculate_price_statistics(data)

        assert stats["period_days"] == 20
        assert stats["ma_20"] is not None
        assert stats["ma_50"] is None  # only 20 records
        assert stats["ma_200"] is None
        assert stats["volatility"] is not None
        assert stats["avg_volume"] is not None
        assert stats["total_volume"] is not None
        # Performance
        assert stats["period_change"] is not None
        assert stats["period_change_pct"] is not None

    def test_moving_averages_thresholds(self):
        data = _make_daily_records(50)
        stats = _calculate_price_statistics(data)
        assert stats["ma_20"] is not None
        assert stats["ma_50"] is not None
        assert stats["ma_200"] is None

    def test_period_high_low(self):
        data = [
            {"date": "2025-01-01", "open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000},
            {"date": "2025-01-02", "open": 105, "high": 120, "low": 95, "close": 115, "volume": 2000},
        ]
        stats = _calculate_price_statistics(data)
        assert stats["period_high"] == 120
        assert stats["period_low"] == 90
        assert stats["min_close"] == 105
        assert stats["max_close"] == 115


class TestFormatPriceDataAsTable:
    def test_empty_data(self):
        assert _format_price_data_as_table([]) == "No price data available."
        assert _format_price_data_as_table(None) == "No price data available."

    def test_single_record_table(self):
        data = _make_daily_records(1)
        result = _format_price_data_as_table(data)
        assert "AAPL" in result
        assert "Daily Prices" in result
        assert "| Date" in result
        assert "Total Volume" in result

    def test_table_contains_all_records(self):
        data = _make_daily_records(5)
        result = _format_price_data_as_table(data)
        # Should have header + separator + 5 data rows
        table_lines = [l for l in result.split("\n") if l.startswith("|")]
        assert len(table_lines) == 7  # header + separator + 5 rows


class TestFormatPriceSummary:
    def test_empty_stats(self):
        assert _format_price_summary({}) == "No data available for summary"
        assert _format_price_summary(None) == "No data available for summary"

    def test_with_full_stats(self):
        data = _make_daily_records(20)
        stats = _calculate_price_statistics(data)
        result = _format_price_summary(stats)
        assert "Period:" in result
        assert "trading days" in result
        assert "| Metric | Value |" in result
        assert "Period Open" in result
        assert "20-Day MA" in result


# ---------------------------------------------------------------------------
# fetch_stock_daily_prices
# ---------------------------------------------------------------------------

class TestFetchStockDailyPrices:
    @pytest.mark.asyncio
    async def test_short_period_returns_table(self):
        """< 14 days should return markdown table format."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices("AAPL", limit=5)

        assert "Daily Prices" in content
        assert "| Date" in content
        assert artifact["type"] == "stock_prices"
        assert artifact["symbol"] == "AAPL"
        assert len(artifact["ohlcv"]) == 5

    @pytest.mark.asyncio
    async def test_long_period_returns_summary(self):
        """>= 14 days should return formatted summary."""
        bars = _make_provider_bars(20)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices("AAPL", limit=20)

        assert "| Metric | Value |" in content
        assert "Period Open" in content
        assert artifact["type"] == "stock_prices"
        assert artifact["stats"]["ma_20"] is not None

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """No data should return 'No data available' message."""
        provider = _make_fake_market_provider(daily_bars=[])

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices("FAKE")

        assert "No data available" in content
        assert artifact["type"] == "stock_prices"
        assert artifact["symbol"] == "FAKE"

    @pytest.mark.asyncio
    async def test_date_range_query(self):
        """Using start_date/end_date should call get_daily with dates."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices(
                "AAPL", start_date="2025-01-01", end_date="2025-01-05"
            )

        provider.get_daily.assert_called_once_with(
            "AAPL", from_date="2025-01-01", to_date="2025-01-05", user_id=None
        )
        assert artifact["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_intraday_fetched_for_short_period(self):
        """Periods <= 60 days should attempt intraday data."""
        daily_bars = _make_provider_bars(5)
        intraday_bars = _make_provider_bars(50, base_price=150.0)
        provider = _make_fake_market_provider(
            daily_bars=daily_bars, intraday_bars=intraday_bars
        )

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices("AAPL", limit=5)

        provider.get_intraday.assert_called_once()
        assert artifact["chart_interval"] == "5min"

    @pytest.mark.asyncio
    async def test_intraday_failure_falls_back_to_daily(self):
        """If intraday fetch fails, chart_ohlcv should use daily data."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)
        provider.get_intraday = AsyncMock(side_effect=Exception("API error"))

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices("AAPL", limit=5)

        assert artifact["chart_interval"] == "daily"

    @pytest.mark.asyncio
    async def test_default_limit_applied(self):
        """No args should default to limit=60."""
        bars = _make_provider_bars(60)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_stock_daily_prices("AAPL")

        # Should call with date range (limit logic converts to date range)
        provider.get_daily.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_company_overview
# ---------------------------------------------------------------------------

class TestFetchCompanyOverview:
    @pytest.fixture
    def full_profile(self):
        return [{
            "companyName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "marketCap": 3_500_000_000_000,
            "price": 235.50,
            "exchangeShortName": "NASDAQ",
            "pe": 32.5,
        }]

    @pytest.fixture
    def full_financial(self, full_profile):
        return _make_fake_financial_source(
            profile_data=full_profile,
            income_stmt=[{
                "date": "2025-06-30",
                "period": "Q3",
                "fiscalYear": "2025",
                "revenue": 94_000_000_000,
                "netIncome": 23_000_000_000,
                "grossProfit": 44_000_000_000,
                "operatingIncome": 29_000_000_000,
                "ebitda": 32_000_000_000,
                "epsdiluted": 1.52,
                "grossProfitRatio": 0.468,
                "operatingIncomeRatio": 0.308,
                "netIncomeRatio": 0.245,
            }],
            earnings_calendar=[{
                "date": "2025-07-24",
                "epsActual": 1.52,
                "epsEstimated": 1.45,
                "revenueActual": 94_000_000_000,
                "revenueEstimated": 92_000_000_000,
                "fiscalDateEnding": "2025-06-30",
            }],
            price_change=[{"1D": 0.5, "5D": 1.2, "1M": -2.3, "ytd": 15.0, "1Y": 30.0}],
            key_metrics=[{"peRatioTTM": 32.5, "pbRatioTTM": 50.0, "roeTTM": 1.60}],
            ratios=[{
                "returnOnEquityTTM": 1.60,
                "netProfitMarginTTM": 0.245,
                "debtEquityRatioTTM": 1.87,
                "currentRatioTTM": 0.99,
            }],
            price_target_consensus=[{"targetMedian": 260.0, "targetLow": 200.0, "targetHigh": 300.0, "targetConsensus": "Buy"}],
            grades_summary=[{"strongBuy": 10, "buy": 15, "hold": 5, "sell": 1, "strongSell": 0, "consensus": "Buy"}],
            quote_data=[{
                "price": 235.50, "change": 2.30, "changePercentage": 0.99,
                "dayHigh": 237.0, "dayLow": 233.0, "yearHigh": 260.0, "yearLow": 165.0,
                "open": 234.0, "previousClose": 233.20, "volume": 55_000_000, "avgVolume": 60_000_000,
                "marketCap": 3_500_000_000_000,
            }],
            cash_flow=[{
                "date": "2025-06-30",
                "operatingCashFlow": 28_000_000_000,
                "capitalExpenditure": -3_000_000_000,
                "freeCashFlow": 25_000_000_000,
            }],
            product_data=[{"2025-06-30": {"iPhone": 46_000_000_000, "Services": 24_000_000_000}}],
            geo_data=[{"2025-06-30": {"Americas": 40_000_000_000, "Europe": 25_000_000_000}}],
        )

    @pytest.mark.asyncio
    async def test_full_overview(self, full_financial, full_profile):
        """Full data should produce comprehensive formatted output."""
        provider = _make_fake_financial_provider(financial=full_financial)
        with (
            patch(f"{_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("AAPL")

        assert "Apple Inc." in content
        assert "Technology" in content
        assert "Real-Time Quote" in content
        assert "Stock Price Performance" in content
        assert "Key Financial Metrics" in content
        assert "Earnings Performance" in content
        assert "Analyst Consensus" in content
        assert "Revenue Breakdown" in content
        assert artifact["type"] == "company_overview"
        assert artifact["symbol"] == "AAPL"
        assert artifact["name"] == "Apple Inc."
        assert "quote" in artifact
        assert "performance" in artifact
        assert "analystRatings" in artifact

    @pytest.mark.asyncio
    async def test_missing_profile_returns_error(self):
        """Missing profile should return error content."""
        financial = _make_fake_financial_source(profile_data=[])
        provider = _make_fake_financial_provider(financial=financial)

        with (
            patch(f"{_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("FAKE")

        assert "No data found for symbol FAKE" in content
        assert artifact["type"] == "company_overview"
        assert "error" not in artifact  # it's not an exception error

    @pytest.mark.asyncio
    async def test_partial_data_handled_gracefully(self, full_profile):
        """Some gather calls raising exceptions should not crash."""
        financial = _make_fake_financial_source(
            profile_data=full_profile,
            quote_data=[{
                "price": 235.50, "change": 2.30, "changePercentage": 0.99,
                "dayHigh": 237.0, "dayLow": 233.0, "yearHigh": 260.0, "yearLow": 165.0,
                "open": 234.0, "previousClose": 233.20, "volume": 55_000_000,
                "avgVolume": 60_000_000, "marketCap": 3_500_000_000_000,
            }],
        )
        # Make some calls raise exceptions (simulating partial failures)
        financial.get_income_statements = AsyncMock(side_effect=Exception("API error"))
        financial.get_price_performance = AsyncMock(side_effect=Exception("timeout"))
        provider = _make_fake_financial_provider(financial=financial)

        with (
            patch(f"{_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_MOD}._fmp_request", return_value=[]),
        ):
            content, artifact = await fetch_company_overview("AAPL")

        # Should still have basic profile info
        assert "Apple Inc." in content
        assert artifact["type"] == "company_overview"
        # Should NOT have sections dependent on failed calls
        assert "Stock Price Performance" not in content

    @pytest.mark.asyncio
    async def test_provider_exception_returns_error(self):
        """get_financial_data_provider raising should produce error content."""
        with patch(f"{_MOD}.get_financial_data_provider", side_effect=Exception("Connection failed")):
            content, artifact = await fetch_company_overview("AAPL")

        assert "Error" in content
        assert "error" in artifact

    @pytest.mark.asyncio
    async def test_boundary_normalizes_symbol_before_provider(self):
        """A non-canonical spelling reaches the provider canonicalized (FIX 3).

        ``zzzz.us`` is a neutral placeholder: lowercase + ``.US`` suffix, which
        the protocol boundary folds to bare ``ZZZZ`` before any provider call.
        """
        financial = _make_fake_financial_source(profile_data=None)  # early-return path
        provider = _make_fake_financial_provider(financial=financial)
        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_company_overview("zzzz.us")

        financial.get_company_profile.assert_called_once_with("ZZZZ")


# ---------------------------------------------------------------------------
# fetch_market_indices
# ---------------------------------------------------------------------------

class TestFetchMarketIndices:
    @pytest.mark.asyncio
    async def test_default_indices(self):
        """Should fetch ^GSPC, ^IXIC, ^DJI, ^RUT by default."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_market_indices(limit=5)

        # Should call get_daily for each default index (4 calls)
        assert provider.get_daily.call_count == 4
        assert artifact["type"] == "market_indices"

    @pytest.mark.asyncio
    async def test_short_period_table_format(self):
        """< 14 days should return markdown table format."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_market_indices(
                indices=["^GSPC"], limit=5
            )

        # Short period uses table format
        assert "| Date" in content
        assert artifact["type"] == "market_indices"

    @pytest.mark.asyncio
    async def test_long_period_summary_format(self):
        """>= 14 days should return summary format."""
        bars = _make_provider_bars(20)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_market_indices(
                indices=["^GSPC"], limit=20
            )

        assert "| Metric | Value |" in content
        assert artifact["type"] == "market_indices"

    @pytest.mark.asyncio
    async def test_no_data_available(self):
        """No data should return 'No data available' message."""
        provider = _make_fake_market_provider(daily_bars=[])

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_market_indices(indices=["^GSPC"])

        assert "No data available" in content or "No index data available" in content

    @pytest.mark.asyncio
    async def test_date_range_query(self):
        """Using start_date/end_date should pass dates through."""
        bars = _make_provider_bars(5)
        provider = _make_fake_market_provider(daily_bars=bars)

        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, artifact = await fetch_market_indices(
                indices=["^GSPC"],
                start_date="2025-01-01",
                end_date="2025-01-05",
            )

        provider.get_daily.assert_called_once_with(
            "^GSPC", from_date="2025-01-01", to_date="2025-01-05",
            is_index=True, user_id=None,
        )


# ---------------------------------------------------------------------------
# fetch_sector_performance
# ---------------------------------------------------------------------------

class TestFetchSectorPerformance:
    @pytest.mark.asyncio
    async def test_normal_sector_data(self):
        """Normal data should produce formatted sector table."""
        sector_data = [
            {"sector": "Technology", "changePctStr": "+1.50%"},
            {"sector": "Healthcare", "changePctStr": "-0.42%"},
            {"sector": "Energy", "changePctStr": "+0.85%"},
        ]
        financial = _make_fake_financial_source(sector_data=sector_data)
        provider = _make_fake_financial_provider(financial=financial)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_sector_performance()

        assert "Sector Performance" in content
        assert "Technology" in content
        assert "Healthcare" in content
        assert artifact["type"] == "sector_performance"
        assert len(artifact["sectors"]) == 3
        # Sorted descending by performance
        assert artifact["sectors"][0]["sector"] == "Technology"
        assert artifact["sectors"][-1]["sector"] == "Healthcare"

    @pytest.mark.asyncio
    async def test_empty_sector_data(self):
        """Empty data should return 'No data available'."""
        financial = _make_fake_financial_source(sector_data=[])
        provider = _make_fake_financial_provider(financial=financial)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_sector_performance()

        assert "No data available" in content or "No sector performance" in content
        assert artifact["sectors"] == []

    @pytest.mark.asyncio
    async def test_with_date_parameter(self):
        """Passing a date should be forwarded to provider.get_sector_performance."""
        sector_data = [
            {"sector": "Technology", "changePctStr": "+1.50%"},
        ]
        financial = _make_fake_financial_source(sector_data=sector_data)
        provider = _make_fake_financial_provider(financial=financial)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_sector_performance(date="2025-01-15")

        assert "Technology" in content
        assert artifact["type"] == "sector_performance"
        financial.get_sector_performance.assert_awaited_once_with(target_date="2025-01-15")


# ---------------------------------------------------------------------------
# fetch_stock_screener
# ---------------------------------------------------------------------------

class TestFetchStockScreener:
    def _make_screener_provider(self, results):
        financial = _make_fake_financial_source(screener_results=results)
        return _make_fake_financial_provider(financial=financial), financial

    @pytest.mark.asyncio
    async def test_with_filters(self):
        """Filters should be passed through and results formatted."""
        results = [
            {
                "symbol": "AAPL",
                "companyName": "Apple Inc.",
                "price": 235.50,
                "marketCap": 3_500_000_000_000,
                "sector": "Technology",
                "beta": 1.24,
                "volume": 55_000_000,
                "change": 2.30,
            },
            {
                "symbol": "MSFT",
                "companyName": "Microsoft Corporation",
                "price": 420.0,
                "marketCap": 3_100_000_000_000,
                "sector": "Technology",
                "beta": 0.93,
                "volume": 25_000_000,
                "change": -1.50,
            },
        ]
        provider, financial = self._make_screener_provider(results)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_stock_screener(
                sector="Technology",
                market_cap_more_than=1_000_000_000_000,
            )

        assert "AAPL" in content
        assert "MSFT" in content
        assert "Stock Screener Results" in content
        assert "2 stocks" in content
        assert artifact["type"] == "stock_screener"
        assert artifact["count"] == 2
        # Verify params were passed to financial source
        financial.screen_stocks.assert_called_once()
        call_kwargs = financial.screen_stocks.call_args[1]
        assert call_kwargs["sector"] == "Technology"
        assert call_kwargs["marketCapMoreThan"] == 1_000_000_000_000

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """No matches should return appropriate message."""
        provider, _ = self._make_screener_provider([])

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_stock_screener(sector="Nonexistent")

        assert "No stocks matched" in content
        assert artifact["count"] == 0

    @pytest.mark.asyncio
    async def test_all_filter_params_passed(self):
        """All filter params should be converted to camelCase and passed."""
        provider, financial = self._make_screener_provider([])

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_stock_screener(
                market_cap_more_than=1e9,
                price_more_than=10.0,
                volume_more_than=1e6,
                beta_more_than=0.5,
                dividend_more_than=2.0,
                is_etf=False,
                is_actively_trading=True,
                exchange="NASDAQ",
                country="US",
                limit=25,
            )

        call_kwargs = financial.screen_stocks.call_args[1]
        assert call_kwargs["marketCapMoreThan"] == 1e9
        assert call_kwargs["priceMoreThan"] == 10.0
        assert call_kwargs["volumeMoreThan"] == 1e6
        assert call_kwargs["betaMoreThan"] == 0.5
        assert call_kwargs["dividendMoreThan"] == 2.0
        assert call_kwargs["isEtf"] is False
        assert call_kwargs["isActivelyTrading"] is True
        assert call_kwargs["exchange"] == "NASDAQ"
        assert call_kwargs["country"] == "US"
        assert call_kwargs["limit"] == 25

    @pytest.mark.asyncio
    async def test_provider_exception_returns_error(self):
        """Provider exception should return error content."""
        with patch(f"{_MOD}.get_financial_data_provider", side_effect=Exception("Connection failed")):
            content, artifact = await fetch_stock_screener()

        assert "Error" in content
        assert artifact["count"] == 0


# ---------------------------------------------------------------------------
# Helpers for intel-based tools
# ---------------------------------------------------------------------------

def _make_fake_intel_source(
    *,
    options_chain=None,
    options_ohlcv=None,
    short_interest=None,
    short_volume=None,
    float_shares=None,
    movers=None,
):
    """Build a mock MarketIntelSource."""
    src = AsyncMock()
    src.get_options_chain = AsyncMock(return_value=options_chain or {"results": []})
    src.get_options_ohlcv = AsyncMock(return_value=options_ohlcv or [])
    src.get_short_interest = AsyncMock(return_value=short_interest or [])
    src.get_short_volume = AsyncMock(return_value=short_volume or [])
    src.get_float_shares = AsyncMock(return_value=float_shares or {})
    src.get_movers = AsyncMock(return_value=movers or [])
    return src


# ---------------------------------------------------------------------------
# fetch_options_chain
# ---------------------------------------------------------------------------

class TestFetchOptionsChain:
    @pytest.mark.asyncio
    async def test_with_filters(self):
        """Filters should be passed and results formatted as table."""
        chain_data = {
            "results": [
                {
                    "ticker": "O:AAPL250117C00200000",
                    "contract_type": "call",
                    "strike_price": 200.0,
                    "expiration_date": "2025-01-17",
                    "exercise_style": "american",
                },
                {
                    "ticker": "O:AAPL250117P00180000",
                    "contract_type": "put",
                    "strike_price": 180.0,
                    "expiration_date": "2025-01-17",
                    "exercise_style": "american",
                },
            ],
            "next_cursor": "abc123",
        }
        intel = _make_fake_intel_source(options_chain=chain_data)
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain(
                "AAPL", contract_type="call", strike_min=150.0, limit=10
            )

        assert "Options Chain: AAPL" in content
        assert "contracts" in content
        assert "O:AAPL250117C00200000" in content
        assert "$200.00" in content
        assert artifact["type"] == "options_chain"
        assert len(artifact["results"]) >= 2

        # Verify filters passed (implementation may paginate, so check first call)
        assert intel.get_options_chain.await_count >= 1
        first_call = intel.get_options_chain.call_args_list[0]
        assert first_call[0][0] == "AAPL"
        assert first_call[1]["contract_type"] == "call"
        assert first_call[1]["strike_price_gte"] == 150.0

    @pytest.mark.asyncio
    async def test_empty_results(self):
        intel = _make_fake_intel_source(options_chain={"results": []})
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain("FAKE")

        assert "No contracts found" in content
        assert artifact["results"] == []

    @pytest.mark.asyncio
    async def test_no_intel_source(self):
        """Missing intel source should return unavailable message."""
        provider = _make_fake_financial_provider(intel=None)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_options_chain("AAPL")

        assert "not available" in content
        assert artifact["results"] == []

    @pytest.mark.asyncio
    async def test_boundary_normalizes_underlying_before_provider(self):
        """A non-canonical underlying reaches the provider canonicalized (FIX 3).

        ``zzzz.us`` is a neutral placeholder: lowercase + ``.US`` suffix, which
        the protocol boundary folds to bare ``ZZZZ`` before the provider call.
        """
        intel = _make_fake_intel_source(options_chain={"results": []})
        provider = _make_fake_financial_provider(intel=intel)
        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            await fetch_options_chain("zzzz.us")

        assert intel.get_options_chain.await_count >= 1
        assert intel.get_options_chain.call_args_list[0][0][0] == "ZZZZ"


# ---------------------------------------------------------------------------
# fetch_market_movers
# ---------------------------------------------------------------------------

class TestFetchMarketMovers:
    @pytest.mark.asyncio
    async def test_gainers(self):
        movers = [
            {"ticker": "NVDA", "name": "NVIDIA Corp", "price": 950.0, "change_percent": 8.5},
            {"ticker": "AMD", "name": "AMD Inc", "price": 180.0, "change_percent": 5.2},
        ]
        intel = _make_fake_intel_source(movers=movers)
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers(direction="gainers")

        assert "Market Gainers" in content
        assert "2 stocks" in content
        assert "NVDA" in content
        assert "+8.50%" in content
        assert artifact["type"] == "market_movers"
        assert artifact["direction"] == "gainers"
        assert len(artifact["results"]) == 2

    @pytest.mark.asyncio
    async def test_losers(self):
        movers = [
            {"ticker": "INTC", "name": "Intel Corp", "price": 20.0, "change_percent": -6.3},
        ]
        intel = _make_fake_intel_source(movers=movers)
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers(direction="losers")

        assert "Market Losers" in content
        assert "INTC" in content
        assert "-6.30%" in content

    @pytest.mark.asyncio
    async def test_empty_results(self):
        intel = _make_fake_intel_source(movers=[])
        provider = _make_fake_financial_provider(intel=intel)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers()

        assert "No gainers data available" in content
        assert artifact["results"] == []

    @pytest.mark.asyncio
    async def test_no_intel_source(self):
        provider = _make_fake_financial_provider(intel=None)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, artifact = await fetch_market_movers()

        assert "not available" in content


# ---------------------------------------------------------------------------
# CMDP display fixes: per-instrument Market header, unitless index levels,
# US-Eastern session line gated to US listings. Neutral placeholder symbols.
# ---------------------------------------------------------------------------

def _quote_only_financial(profile, quote):
    """Financial source with just a profile + realtime quote (FMP-quote path)."""
    return _make_fake_financial_source(profile_data=profile, quote_data=quote)


class TestMarketHeaderLabel:
    """`**Market:**` header is derived from the resolved instrument, not hardcoded."""

    @pytest.mark.asyncio
    async def test_us_header_byte_compatible(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_stock_daily_prices("AAPL", limit=5)
        assert "**Market:** US Stock" in content

    @pytest.mark.asyncio
    async def test_hk_header(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_stock_daily_prices("0700.HK", limit=5)
        assert "**Market:** HK Stock" in content
        assert "**Market:** US Stock" not in content

    @pytest.mark.asyncio
    async def test_ashare_header(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(5))
        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_stock_daily_prices("600519.SS", limit=5)
        assert "**Market:** A-Share" in content


class TestIndexSummaryNoCurrencyPrefix:
    """Index levels are unitless points — the summary path carries no `$`/prefix."""

    @pytest.mark.asyncio
    async def test_index_summary_has_no_currency_symbol(self):
        provider = _make_fake_market_provider(daily_bars=_make_provider_bars(20))
        with patch(f"{_MOD}.get_market_data_provider", return_value=provider):
            content, _ = await fetch_market_indices(indices=["^GSPC"], limit=20)
        # Summary path (>= 14 days) — metric table present, no currency prefix.
        assert "| Metric | Value |" in content
        assert "$" not in content


class TestOverviewSessionClockGating:
    """US listings show the US-Eastern session phase + 'As of … ET' clock; non-US
    listings show their exchange-calendar phase + the exchange-local clock (never ET)."""

    _US_PROFILE = [{
        "companyName": "Placeholder US Co.",
        "sector": "Technology",
        "industry": "Software",
        "marketCap": 1_000_000_000_000,
        "price": 100.0,
        "exchangeShortName": "NASDAQ",
    }]
    _HK_PROFILE = [{
        "companyName": "Placeholder HK Co.",
        "sector": "Technology",
        "industry": "Software",
        "marketCap": 1_000_000_000_000,
        "price": 318.20,
        "exchangeShortName": "HKSE",
    }]
    _QUOTE = [{
        "price": 100.0, "change": 1.0, "changePercentage": 1.0,
        "dayHigh": 101.0, "dayLow": 99.0, "yearHigh": 120.0, "yearLow": 80.0,
        "open": 99.5, "previousClose": 99.0, "volume": 1_000_000, "avgVolume": 1_200_000,
    }]

    def _patches(self, financial):
        mdp = AsyncMock()
        mdp.get_snapshots = AsyncMock(return_value=[])  # force FMP-quote path
        provider = _make_fake_financial_provider(financial=financial)
        return (
            patch(f"{_MOD}.get_financial_data_provider", return_value=provider),
            patch(f"{_MOD}.get_market_data_provider", return_value=mdp),
            patch(f"{_MOD}._fmp_request", return_value=[]),
        )

    @pytest.mark.asyncio
    async def test_us_shows_session_and_et_clock(self):
        financial = _quote_only_financial(self._US_PROFILE, self._QUOTE)
        p1, p2, p3 = self._patches(financial)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("AAPL")
        assert "**Market Status:**" in content
        assert "ET" in content

    @pytest.mark.asyncio
    async def test_non_us_shows_calendar_phase_and_local_clock(self):
        financial = _quote_only_financial(self._HK_PROFILE, self._QUOTE)
        p1, p2, p3 = self._patches(financial)
        with p1, p2, p3:
            content, _ = await fetch_company_overview("0700.HK")
        status_line = next(
            (ln for ln in content.splitlines() if ln.startswith("**Market Status:**")), None
        )
        # Non-US listings DO get a status line, but from the exchange calendar:
        # the HK exchange-local clock (HKT), never the US-Eastern "ET" clock.
        assert status_line is not None
        assert "HKT" in status_line
        assert " ET" not in status_line
        # …and a phase label the calendar can actually produce (time-of-run dependent).
        assert any(
            label in status_line
            for label in (
                "Regular Hours", "Lunch Break", "Market Closed",
                "Pre-Market", "After-Hours", "Halted",
            )
        )
        # Prices still localize to the HK listing currency.
        assert "HK$" in content
