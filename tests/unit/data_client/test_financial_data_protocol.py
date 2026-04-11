"""Tests for the financial data protocol layer: composite, sources, and factory."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data_client.financial_data_provider import FinancialDataProvider


# ---------------------------------------------------------------------------
# FinancialDataProvider composite
# ---------------------------------------------------------------------------

class TestFinancialDataProvider:
    def test_exposes_financial_and_intel(self):
        financial = MagicMock()
        intel = MagicMock()
        provider = FinancialDataProvider(financial=financial, intel=intel)
        assert provider.financial is financial
        assert provider.intel is intel

    def test_none_sources_allowed(self):
        provider = FinancialDataProvider()
        assert provider.financial is None
        assert provider.intel is None

    @pytest.mark.asyncio
    async def test_close_calls_both_sources(self):
        financial = AsyncMock()
        intel = AsyncMock()
        provider = FinancialDataProvider(financial=financial, intel=intel)
        await provider.close()
        financial.close.assert_awaited_once()
        intel.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_skips_none_sources(self):
        provider = FinancialDataProvider(financial=None, intel=None)
        await provider.close()  # should not raise


# ---------------------------------------------------------------------------
# FMPFinancialSource delegation
# ---------------------------------------------------------------------------

class TestFMPFinancialSource:
    """Each method should delegate to the corresponding FMPClient method."""

    @pytest.fixture
    def mock_fmp_client(self):
        client = AsyncMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        return client

    @pytest.fixture
    def source(self, mock_fmp_client):
        from src.data_client.fmp.financial_source import FMPFinancialSource

        src = FMPFinancialSource(mock_fmp_client)
        return src, mock_fmp_client

    @pytest.mark.asyncio
    async def test_get_company_profile(self, source):
        src, client = source
        client.get_profile.return_value = [{"companyName": "Apple"}]
        result = await src.get_company_profile("AAPL")
        client.get_profile.assert_awaited_once_with("AAPL")
        assert result == [{"companyName": "Apple"}]

    @pytest.mark.asyncio
    async def test_get_realtime_quote(self, source):
        src, client = source
        client.get_quote.return_value = [{"price": 150}]
        result = await src.get_realtime_quote("AAPL")
        client.get_quote.assert_awaited_once_with("AAPL")
        assert result == [{"price": 150}]

    @pytest.mark.asyncio
    async def test_get_income_statements(self, source):
        src, client = source
        client.get_income_statement.return_value = [{"revenue": 1e9}]
        result = await src.get_income_statements("AAPL", period="annual", limit=4)
        client.get_income_statement.assert_awaited_once_with(
            "AAPL", period="annual", limit=4
        )
        assert result == [{"revenue": 1e9}]

    @pytest.mark.asyncio
    async def test_get_cash_flows(self, source):
        src, client = source
        client.get_cash_flow.return_value = [{"freeCashFlow": 5e8}]
        result = await src.get_cash_flows("AAPL")
        client.get_cash_flow.assert_awaited_once_with("AAPL", period="quarter", limit=8)
        assert result == [{"freeCashFlow": 5e8}]

    @pytest.mark.asyncio
    async def test_get_key_metrics(self, source):
        src, client = source
        client.get_key_metrics_ttm.return_value = [{"peRatioTTM": 30}]
        result = await src.get_key_metrics("AAPL")
        client.get_key_metrics_ttm.assert_awaited_once_with("AAPL")
        assert result == [{"peRatioTTM": 30}]

    @pytest.mark.asyncio
    async def test_get_financial_ratios(self, source):
        src, client = source
        client.get_ratios_ttm.return_value = [{"currentRatioTTM": 1.2}]
        result = await src.get_financial_ratios("AAPL")
        client.get_ratios_ttm.assert_awaited_once_with("AAPL")
        assert result == [{"currentRatioTTM": 1.2}]

    @pytest.mark.asyncio
    async def test_get_price_performance(self, source):
        src, client = source
        client.get_stock_price_change.return_value = [{"1D": 0.5}]
        result = await src.get_price_performance("AAPL")
        client.get_stock_price_change.assert_awaited_once_with("AAPL")
        assert result == [{"1D": 0.5}]

    @pytest.mark.asyncio
    async def test_get_analyst_price_targets(self, source):
        src, client = source
        client.get_price_target_consensus.return_value = [{"targetMedian": 200}]
        result = await src.get_analyst_price_targets("AAPL")
        client.get_price_target_consensus.assert_awaited_once_with("AAPL")
        assert result == [{"targetMedian": 200}]

    @pytest.mark.asyncio
    async def test_get_analyst_ratings(self, source):
        src, client = source
        client.get_grades_summary.return_value = [{"consensus": "Buy"}]
        result = await src.get_analyst_ratings("AAPL")
        client.get_grades_summary.assert_awaited_once_with("AAPL")
        assert result == [{"consensus": "Buy"}]

    @pytest.mark.asyncio
    async def test_get_earnings_history(self, source):
        src, client = source
        client.get_historical_earnings_calendar.return_value = [{"eps": 1.5}]
        result = await src.get_earnings_history("AAPL", limit=5)
        client.get_historical_earnings_calendar.assert_awaited_once_with("AAPL", limit=5)
        assert result == [{"eps": 1.5}]

    @pytest.mark.asyncio
    async def test_get_revenue_by_segment_product(self, source):
        src, client = source
        client.get_revenue_product_segmentation.return_value = [{"iPhone": 1e9}]
        result = await src.get_revenue_by_segment("AAPL", segment_type="product")
        client.get_revenue_product_segmentation.assert_awaited_once_with("AAPL")
        assert result == [{"iPhone": 1e9}]

    @pytest.mark.asyncio
    async def test_get_revenue_by_segment_geography(self, source):
        src, client = source
        client.get_revenue_geographic_segmentation.return_value = [{"Americas": 2e9}]
        result = await src.get_revenue_by_segment("AAPL", segment_type="geography")
        client.get_revenue_geographic_segmentation.assert_awaited_once_with("AAPL")
        assert result == [{"Americas": 2e9}]

    @pytest.mark.asyncio
    async def test_get_sector_performance(self, source):
        src, client = source
        client._make_request.return_value = [{"sector": "Tech"}]
        result = await src.get_sector_performance()
        client._make_request.assert_awaited_once()
        assert result == [{"sector": "Tech"}]

    @pytest.mark.asyncio
    async def test_screen_stocks(self, source):
        src, client = source
        client.get_company_screener.return_value = [{"symbol": "AAPL"}]
        result = await src.screen_stocks(sector="Technology", limit=10)
        client.get_company_screener.assert_awaited_once_with(sector="Technology", limit=10)
        assert result == [{"symbol": "AAPL"}]

    @pytest.mark.asyncio
    async def test_search_stocks(self, source):
        src, client = source
        client.search_stocks = AsyncMock(return_value=[{"symbol": "AAPL"}])
        result = await src.search_stocks(query="apple", limit=10)
        client.search_stocks.assert_awaited_once_with(query="apple", limit=10)
        assert result == [{"symbol": "AAPL"}]

    @pytest.mark.asyncio
    async def test_close_is_noop(self, source):
        src, _ = source
        await src.close()  # should not raise


# ---------------------------------------------------------------------------
# GinlixMarketIntelSource delegation
# ---------------------------------------------------------------------------

class TestGinlixMarketIntelSource:
    @pytest.fixture
    def mock_client(self):
        return AsyncMock()

    @pytest.fixture
    def source(self, mock_client):
        from src.data_client.ginlix_data.market_intel_source import (
            GinlixMarketIntelSource,
        )

        return GinlixMarketIntelSource(mock_client)

    @pytest.mark.asyncio
    async def test_get_options_chain(self, source, mock_client):
        mock_client.get_options_contracts.return_value = {"results": [{"ticker": "O:AAPL"}]}
        result = await source.get_options_chain("AAPL", limit=5)
        mock_client.get_options_contracts.assert_awaited_once_with(
            underlying_ticker="AAPL", user_id=None, limit=5
        )
        assert result == {"results": [{"ticker": "O:AAPL"}]}

    @pytest.mark.asyncio
    async def test_get_options_ohlcv(self, source, mock_client):
        mock_client.get_aggregates.return_value = ([{"open": 1}], False)
        result = await source.get_options_ohlcv(
            "O:AAPL250117C00200000", from_date="2025-01-01", interval="1hour"
        )
        mock_client.get_aggregates.assert_awaited_once_with(
            market="option",
            symbol="O:AAPL250117C00200000",
            timespan="hour",
            multiplier=1,
            from_date="2025-01-01",
            to_date=None,
            user_id=None,
        )
        assert result == [{"open": 1}]

    @pytest.mark.asyncio
    async def test_get_options_ohlcv_bad_interval(self, source):
        with pytest.raises(ValueError, match="Unsupported interval"):
            await source.get_options_ohlcv("O:AAPL", interval="2hour")

    @pytest.mark.asyncio
    async def test_get_short_interest(self, source, mock_client):
        mock_client.get_short_interest.return_value = [{"short_interest": 1000}]
        result = await source.get_short_interest("AAPL")
        mock_client.get_short_interest.assert_awaited_once_with(
            "AAPL", limit=500, sort="settlement_date.asc", user_id=None,
        )
        assert result == [{"short_interest": 1000}]

    @pytest.mark.asyncio
    async def test_get_short_volume(self, source, mock_client):
        mock_client.get_short_volume.return_value = [{"short_volume": 500}]
        result = await source.get_short_volume("AAPL")
        mock_client.get_short_volume.assert_awaited_once_with(
            "AAPL", limit=500, sort="date.asc", user_id=None,
        )
        assert result == [{"short_volume": 500}]

    @pytest.mark.asyncio
    async def test_get_float_shares(self, source, mock_client):
        mock_client.get_float.return_value = {"float": 15_000_000_000}
        result = await source.get_float_shares("AAPL")
        mock_client.get_float.assert_awaited_once_with("AAPL", user_id=None)
        assert result == {"float": 15_000_000_000}

    @pytest.mark.asyncio
    async def test_get_movers_gainers(self, source, mock_client):
        mock_client.get_movers.return_value = [{"ticker": "NVDA"}]
        result = await source.get_movers("gainers")
        mock_client.get_movers.assert_awaited_once_with("gainers", user_id=None)
        assert result == [{"ticker": "NVDA"}]

    @pytest.mark.asyncio
    async def test_get_movers_losers(self, source, mock_client):
        mock_client.get_movers.return_value = [{"ticker": "INTC"}]
        result = await source.get_movers("losers")
        mock_client.get_movers.assert_awaited_once_with("losers", user_id=None)
        assert result == [{"ticker": "INTC"}]

    @pytest.mark.asyncio
    async def test_close_is_noop(self, source):
        await source.close()  # should not raise


# ---------------------------------------------------------------------------
# get_financial_data_provider factory
# ---------------------------------------------------------------------------

class TestGetFinancialDataProviderFactory:
    """Test the factory function with mocked availability checks."""

    def _reset_singleton(self):
        """Clear the module-level singleton so each test starts fresh."""
        import src.data_client.registry as mod
        mod._financial_data_provider = None

    @pytest.mark.asyncio
    async def test_fmp_only(self):
        self._reset_singleton()
        mock_fmp_client = AsyncMock()

        with (
            patch("src.data_client.registry._eodhd_available", return_value=False),
            patch("src.data_client.registry._fmp_available", return_value=True),
            patch("src.data_client.registry._ginlix_data_available", return_value=False),
            patch("src.data_client.fmp.get_fmp_client", return_value=mock_fmp_client),
            patch("src.data_client.fmp.financial_source.FMPFinancialSource") as MockFMP,
        ):
            from src.data_client import get_financial_data_provider

            provider = await get_financial_data_provider()

        assert provider.financial is not None
        assert provider.intel is None
        MockFMP.assert_called_once_with(mock_fmp_client)
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_ginlix_only(self):
        self._reset_singleton()
        mock_client = AsyncMock()

        with (
            patch("src.data_client.registry._eodhd_available", return_value=False),
            patch("src.data_client.registry._fmp_available", return_value=False),
            patch("src.data_client.registry._yfinance_available", return_value=False),
            patch("src.data_client.registry._ginlix_data_available", return_value=True),
            patch(
                "src.data_client.ginlix_data.get_ginlix_data_client",
                return_value=mock_client,
            ),
            patch(
                "src.data_client.ginlix_data.market_intel_source.GinlixMarketIntelSource"
            ) as MockIntel,
        ):
            from src.data_client import get_financial_data_provider

            provider = await get_financial_data_provider()

        assert provider.financial is None
        assert provider.intel is not None
        MockIntel.assert_called_once_with(mock_client)
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_both_available(self):
        self._reset_singleton()
        mock_client = AsyncMock()
        mock_fmp_client = AsyncMock()

        with (
            patch("src.data_client.registry._eodhd_available", return_value=False),
            patch("src.data_client.registry._fmp_available", return_value=True),
            patch("src.data_client.registry._ginlix_data_available", return_value=True),
            patch("src.data_client.fmp.get_fmp_client", return_value=mock_fmp_client),
            patch("src.data_client.fmp.financial_source.FMPFinancialSource") as MockFMP,
            patch(
                "src.data_client.ginlix_data.get_ginlix_data_client",
                return_value=mock_client,
            ),
            patch(
                "src.data_client.ginlix_data.market_intel_source.GinlixMarketIntelSource"
            ) as MockIntel,
        ):
            from src.data_client import get_financial_data_provider

            provider = await get_financial_data_provider()

        assert provider.financial is not None
        assert provider.intel is not None
        MockFMP.assert_called_once_with(mock_fmp_client)
        MockIntel.assert_called_once()
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_yfinance_fallback(self):
        """When FMP unavailable but yfinance importable, uses YFinanceFinancialSource."""
        self._reset_singleton()

        with (
            patch("src.data_client.registry._eodhd_available", return_value=False),
            patch("src.data_client.registry._fmp_available", return_value=False),
            patch("src.data_client.registry._yfinance_available", return_value=True),
            patch("src.data_client.registry._ginlix_data_available", return_value=False),
            patch(
                "src.data_client.yfinance.financial_source.YFinanceFinancialSource"
            ) as MockYF,
        ):
            from src.data_client import get_financial_data_provider

            provider = await get_financial_data_provider()

        assert provider.financial is not None
        MockYF.assert_called_once()
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_neither_available(self):
        self._reset_singleton()

        with (
            patch("src.data_client.registry._eodhd_available", return_value=False),
            patch("src.data_client.registry._fmp_available", return_value=False),
            patch("src.data_client.registry._yfinance_available", return_value=False),
            patch("src.data_client.registry._ginlix_data_available", return_value=False),
        ):
            from src.data_client import get_financial_data_provider

            provider = await get_financial_data_provider()

        assert provider.financial is None
        assert provider.intel is None
        self._reset_singleton()


# ---------------------------------------------------------------------------
# YFinanceFinancialSource
# ---------------------------------------------------------------------------

class TestYFinanceFinancialSource:
    """Tests for the yfinance-backed FinancialDataSource."""

    @pytest.mark.asyncio
    async def test_search_stocks_passes_max_results(self):
        mock_search = MagicMock()
        mock_search.quotes = [{"symbol": "AAPL", "shortname": "Apple Inc."}]

        with patch(
            "src.data_client.yfinance.financial_source.yf.Search",
            return_value=mock_search,
        ) as MockSearch:
            from src.data_client.yfinance.financial_source import YFinanceFinancialSource

            source = YFinanceFinancialSource()
            result = await source.search_stocks(query="apple", limit=20)

        MockSearch.assert_called_once_with("apple", max_results=20)
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_analyst_price_targets_empty_dict_returns_empty(self):
        mock_ticker = MagicMock()
        mock_ticker.analyst_price_targets = {}

        with patch(
            "src.data_client.yfinance.financial_source.yf.Ticker",
            return_value=mock_ticker,
        ):
            from src.data_client.yfinance.financial_source import YFinanceFinancialSource

            source = YFinanceFinancialSource()
            result = await source.get_analyst_price_targets("AAPL")

        assert result == []

    @pytest.mark.asyncio
    async def test_close_is_noop(self):
        from src.data_client.yfinance.financial_source import YFinanceFinancialSource

        source = YFinanceFinancialSource()
        await source.close()  # should not raise
