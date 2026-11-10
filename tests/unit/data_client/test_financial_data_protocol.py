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
        client.get_historical_earnings_calendar.return_value = [{"epsActual": 1.5}]
        result = await src.get_earnings_history("AAPL", limit=5)
        client.get_historical_earnings_calendar.assert_awaited_once_with("AAPL", limit=5)
        assert result == [{"epsActual": 1.5}]

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
    async def test_get_revenue_by_segment_flattens_stable_rows(self, source):
        # Stable answers {date, data} rows; the overview reads the first key as the date.
        src, client = source
        client.get_revenue_product_segmentation.return_value = [
            {"symbol": "ACME", "period": "Q3", "date": "2026-06-27", "data": {"Widgets": 5.0}},
        ]
        result = await src.get_revenue_by_segment("ACME", period="quarter", structure="flat")
        client.get_revenue_product_segmentation.assert_awaited_once_with(
            "ACME", period="quarter", structure="flat"
        )
        assert result == [{"2026-06-27": {"Widgets": 5.0}}]

    @pytest.mark.asyncio
    async def test_get_revenue_by_segment_empty_answer_is_a_list(self, source):
        src, client = source
        client.get_revenue_geographic_segmentation.return_value = None
        assert await src.get_revenue_by_segment("ACME", segment_type="geography") == []

    @pytest.mark.asyncio
    async def test_screen_stocks(self, source):
        src, client = source
        client.get_company_screener.return_value = [{"symbol": "AAPL"}]
        result = await src.screen_stocks(sector="Technology", limit=10)
        # FMP's screen includes funds and ETFs unless told; a stock screen does not.
        client.get_company_screener.assert_awaited_once_with(
            isEtf=False, isFund=False, sector="Technology", limit=10
        )
        assert result == [{"symbol": "AAPL"}]

    @pytest.mark.asyncio
    async def test_screen_stocks_asks_for_funds_when_told(self, source):
        src, client = source
        client.get_company_screener.return_value = []
        await src.screen_stocks(isEtf=True)
        client.get_company_screener.assert_awaited_once_with(isEtf=True, isFund=False)

    @pytest.mark.asyncio
    async def test_screen_stocks_keeps_a_country_to_its_own_exchanges(self, source):
        # FMP's country is the issuer's domicile, so a US screen otherwise
        # ranks Buenos Aires lines in pesos.
        src, client = source
        client.get_company_screener.return_value = []
        await src.screen_stocks(country="us")
        assert client.get_company_screener.await_args.kwargs["exchange"] == "NYSE,NASDAQ,AMEX,CBOE"

        client.get_company_screener.reset_mock()
        await src.screen_stocks(country="CN", exchange="NYSE")
        assert client.get_company_screener.await_args.kwargs["exchange"] == "NYSE"

        client.get_company_screener.reset_mock()
        await src.screen_stocks(country="IL")
        assert "exchange" not in client.get_company_screener.await_args.kwargs

    @pytest.mark.asyncio
    async def test_a_foreign_issuers_london_line_names_no_unit(self, source):
        # London trades many lines in their own currency (a Japanese issuer's in
        # yen, an order-book 0-prefixed code in dollars even for a UK issuer),
        # which the row does not name.
        src, client = source
        row = {"symbol": "ACME.L", "price": 3533, "lastAnnualDividend": 95, "exchangeShortName": "LSE", "country": "JP"}
        book = {"symbol": "0ABC.L", "price": 335.62, "exchangeShortName": "LSE", "country": "GB"}
        home = {"symbol": "BETA.L", "price": 3784, "exchangeShortName": "LSE", "country": "GB"}
        client.get_company_screener.return_value = [row, book, home]
        result = await src.screen_stocks(exchange="LSE")
        assert result == [{**row, "currency": None}, {**book, "currency": None}, home]

    @pytest.mark.asyncio
    async def test_search_stocks(self, source):
        src, client = source
        client.search_stocks = AsyncMock(return_value=[{"symbol": "AAPL"}])
        result = await src.search_stocks(query="apple", limit=10)
        client.search_stocks.assert_awaited_once_with(query="apple", limit=10)
        assert result == [{"symbol": "AAPL"}]

    @pytest.mark.asyncio
    async def test_search_rows_name_their_exchange_as_the_route_reads_it(self, source):
        # Stable search says exchange / exchangeFullName; the route reads the
        # v3 names, so the exchange on every row read as empty.
        src, client = source
        row = {"symbol": "NVDA", "name": "NVIDIA", "exchange": "NASDAQ", "exchangeFullName": "NASDAQ Global Select"}
        client.search_stocks = AsyncMock(return_value=[row])
        result = await src.search_stocks(query="nv", limit=10)
        assert result == [{**row, "exchangeShortName": "NASDAQ", "stockExchange": "NASDAQ Global Select"}]

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
        # The vendor row has no price field: the last trade is the price its
        # change was measured at, and it traded at the trade's own time (ns).
        mock_client.get_movers.return_value = [{
            "ticker": "NVDA", "change": 1.5, "change_percent": 50.0,
            "day": {"close": 4.4, "volume": 1200.0, "vwap": 4.2},
            "last_trade": {"price": 4.5, "time": 1793753998341978055},
        }]
        result = await source.get_movers("gainers")
        mock_client.get_movers.assert_awaited_once_with("gainers", user_id=None)
        assert result == [{"symbol": "NVDA", "price": 4.5, "change": 1.5,
                           "change_percent": 50.0, "volume": 1200.0,
                           "traded_value": 1200.0 * 4.2, "traded_at": 1793753998341}]

    @pytest.mark.asyncio
    async def test_get_movers_losers(self, source, mock_client):
        # With no last trade, the day bar's close stands in.
        mock_client.get_movers.return_value = [{"ticker": "INTC", "day": {"close": 2.5}}]
        result = await source.get_movers("losers")
        mock_client.get_movers.assert_awaited_once_with("losers", user_id=None)
        assert result == [{"symbol": "INTC", "price": 2.5, "change": None,
                           "change_percent": None, "volume": None,
                           "traded_value": None, "traded_at": None}]

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

        # ginlix-data serves CN fundamentals: routed wrappers with no default fundamentals.
        assert provider.financial._default is None
        assert "cn" in provider.financial._by_market
        assert provider.intel._default is MockIntel.return_value
        MockIntel.assert_called_once_with(mock_client)
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_both_available(self):
        self._reset_singleton()
        mock_client = AsyncMock()
        mock_fmp_client = AsyncMock()

        with (
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

        assert provider.financial._default is MockFMP.return_value
        assert provider.intel._default is MockIntel.return_value
        MockFMP.assert_called_once_with(mock_fmp_client)
        MockIntel.assert_called_once()
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_yfinance_fallback(self):
        """When FMP unavailable but yfinance importable, uses YFinanceFinancialSource."""
        self._reset_singleton()

        with (
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
            patch("src.data_client.registry._fmp_available", return_value=False),
            patch("src.data_client.registry._yfinance_available", return_value=False),
            patch("src.data_client.registry._ginlix_data_available", return_value=False),
        ):
            from src.data_client import get_financial_data_provider

            provider = await get_financial_data_provider()

        assert provider.financial is None
        assert provider.intel is None
        self._reset_singleton()

    @pytest.mark.asyncio
    async def test_ginlix_data_wraps_sources_in_routing_proxies(self):
        """With ginlix-data configured, both sources get the per-market routing wrapper."""
        self._reset_singleton()
        mock_fmp_client = AsyncMock()

        with (
            patch("src.data_client.registry._fmp_available", return_value=True),
            patch("src.data_client.registry._ginlix_data_available", return_value=True),
            patch("src.data_client.fmp.get_fmp_client", return_value=mock_fmp_client),
            patch("src.data_client.fmp.financial_source.FMPFinancialSource") as MockFMP,
            patch("src.data_client.ginlix_data.get_ginlix_data_client", return_value=AsyncMock()),
        ):
            from src.data_client import get_financial_data_provider
            from src.data_client.financial_data_provider import (
                RoutedFinancialSource,
                RoutedMarketIntelSource,
            )

            provider = await get_financial_data_provider()

        assert isinstance(provider.financial, RoutedFinancialSource)
        assert provider.financial._default is MockFMP.return_value
        assert "cn" in provider.financial._by_market
        assert isinstance(provider.intel, RoutedMarketIntelSource)
        assert "cn" in provider.intel._by_market
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


# ---------------------------------------------------------------------------
# yfinance screen
# ---------------------------------------------------------------------------

class TestYFinanceScreen:
    def test_dividend_filters_compare_per_share_and_london_rows_arrive_in_pounds(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        # Yahoo quotes a pence line's price in pence and its dividend rate in pounds.
        quotes = [
            {"symbol": "SHEL.L", "currency": "GBp", "regularMarketPrice": 3784.0, "dividendRate": 1.16,
             "marketCap": 2.15e11, "regularMarketChangePercent": 1.25},
            {"symbol": "ACME.L", "currency": "JPY", "regularMarketPrice": 3533.0, "dividendRate": 100.0},
            {"symbol": "AAPL", "currency": "USD", "regularMarketPrice": 344.17, "dividendRate": 1.06},
        ]
        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(country="us", dividendMoreThan=3, limit=5)
        # A yield bar would read "3" as 3%; FMP and the CN screen read $3 a share.
        operands = [n["operands"] for n in screen.call_args.args[0].to_dict()["operands"]]
        assert ["forward_dividend_per_share", 3.0] in operands

        with patch("yfinance.screener.screener.screen", return_value={"quotes": quotes}):
            rows = _screen_stocks_sync(dividendMoreThan=1, limit=5)
        # Each row stays in its quote unit, which it names; screen_row_units scales it.
        got = [(r["symbol"], r["currency"], r.get("quoteUnit"), r["price"], r["lastAnnualDividend"]) for r in rows]
        assert got == [
            ("SHEL.L", "GBP", "GBX", 3784.0, pytest.approx(116.0)),
            ("ACME.L", "JPY", None, 3533.0, 100.0),
            ("AAPL", "USD", None, 344.17, 1.06),
        ]
        assert rows[0]["change"] == 1.25

    def test_a_london_bar_holds_each_row_to_its_own_unit(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        # Yahoo compares a pence line's cap in pence and its dividend in pounds,
        # a yen line's both in yen; the bars arrive in pence and the major unit.
        quotes = [
            {"symbol": "BIG.L", "currency": "GBp", "regularMarketPrice": 5000.0, "marketCap": 1e11, "dividendRate": 1.6},
            {"symbol": "SMALL.L", "currency": "GBp", "regularMarketPrice": 2500.0, "marketCap": 1e9, "dividendRate": 2.0},
            {"symbol": "NOCAP.L", "currency": "GBp", "regularMarketPrice": 2500.0, "dividendRate": 2.0},
            {"symbol": "LOWDIV.L", "currency": "JPY", "regularMarketPrice": 3533.0, "marketCap": 4e13, "dividendRate": 100.0},
            {"symbol": "HIDIV.L", "currency": "JPY", "regularMarketPrice": 28660.0, "marketCap": 4e12, "dividendRate": 250.0},
            {"symbol": "MORE.L", "currency": "GBp", "regularMarketPrice": 3000.0, "marketCap": 6e10, "dividendRate": 1.7},
        ]
        with patch("yfinance.screener.screener.screen", return_value={"quotes": quotes}) as screen:
            rows = _screen_stocks_sync(
                country="gb", dividendMoreThan=150, priceMoreThan=2000, marketCapMoreThan=5e10, limit=2,
            )

        # Yahoo gets the bound loose enough for either unit, over a full page.
        operands = [n["operands"] for n in screen.call_args.args[0].to_dict()["operands"]]
        assert ["forward_dividend_per_share", 1.5] in operands
        assert ["intradaymarketcap", 5e10] in operands
        assert ["intradayprice", 2000.0] in operands
        assert screen.call_args.kwargs["size"] == 250
        assert [r["symbol"] for r in rows] == ["BIG.L", "HIDIV.L"]

    def test_a_screen_of_every_venue_holds_each_unit_to_its_own_bar_and_ranks_by_cap(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        # Yahoo compares a pence line's cap in pence, so across every venue the
        # other lines and the minor-unit lines are read as two screens.
        everywhere = [
            {"symbol": "BIG", "currency": "USD", "marketCap": 9.99e8},
            {"symbol": "TINY.L", "currency": "GBp", "marketCap": 5e6},
            {"symbol": "MID.T", "currency": "JPY", "marketCap": 9.9e8},
        ]
        minor_venues = [
            {"symbol": "PENCE.L", "currency": "GBp", "marketCap": 9.95e8},
            {"symbol": "0ABC.L", "currency": "USD", "marketCap": 5e10},
        ]
        with patch(
            "yfinance.screener.screener.screen", side_effect=[{"quotes": everywhere}, {"quotes": minor_venues}],
        ) as screen:
            rows = _screen_stocks_sync(sector="Technology", marketCapLowerThan=1e9, limit=5)

        assert [r["symbol"] for r in rows] == ["BIG", "PENCE.L", "MID.T"]
        first, second = (str(c.args[0].to_dict()) for c in screen.call_args_list)
        assert "'intradaymarketcap', 1000000000.0" in first and "LSE" not in first
        assert "'intradaymarketcap', 100000000000.0" in second and "'exchange', 'LSE'" in second

    def test_a_held_bar_pages_on_past_the_rows_it_drops(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        # The loose bound lets a full page of yen lines under the real bar in.
        miss = {"currency": "JPY", "regularMarketPrice": 3533.0, "marketCap": 4e13, "dividendRate": 100.0}
        page = [{**miss, "symbol": f"J{i}.L"} for i in range(250)]
        hit = {"symbol": "BIG.L", "currency": "GBp", "regularMarketPrice": 5000.0, "dividendRate": 1.6}
        with patch("yfinance.screener.screener.screen", side_effect=[{"quotes": page}, {"quotes": [hit]}]) as screen:
            rows = _screen_stocks_sync(country="gb", dividendMoreThan=150, limit=5)

        assert [r["symbol"] for r in rows] == ["BIG.L"]
        assert [c.kwargs["offset"] for c in screen.call_args_list] == [0, 250]

    def test_a_screen_without_minor_unit_lines_sends_its_bars_as_given(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(country="jp", dividendLowerThan=150, marketCapLowerThan=5e10, limit=5)

        operands = [n["operands"] for n in screen.call_args.args[0].to_dict()["operands"]]
        assert ["forward_dividend_per_share", 150.0] in operands
        assert ["intradaymarketcap", 5e10] in operands
        assert screen.call_args.kwargs["size"] == 5

    def test_a_country_screen_keeps_to_the_regions_home_exchanges(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        # Yahoo's GB region also holds Cboe Europe copies of UK lines.
        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(country="gb", limit=5)
        query = str(screen.call_args.args[0].to_dict())
        assert "'exchange', 'LSE'" in query and "CXE" not in query

        with pytest.raises(ValueError, match="another country's exchange"):
            _screen_stocks_sync(country="gb", exchange="NYSE", limit=5)

        with pytest.raises(ValueError, match="another country's exchange"):
            _screen_stocks_sync(country="gb", exchange="BSE", limit=5)

        # A country's own exchange outside its default list is still its own.
        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(country="in", exchange="BSE", limit=5)
        assert "'exchange', 'BSE'" in str(screen.call_args.args[0].to_dict())

    def test_a_failed_page_fails_the_screen_rather_than_cutting_it_short(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        page = [{"symbol": f"J{i}.L", "currency": "JPY", "marketCap": 4e13, "dividendRate": 100.0} for i in range(250)]
        with patch("yfinance.screener.screener.screen", side_effect=[{"quotes": page}, RuntimeError("429")]):
            with pytest.raises(RuntimeError):
                _screen_stocks_sync(country="gb", dividendMoreThan=150, limit=5)

    def test_an_exchange_named_as_fmp_names_it_reaches_yahoo_by_its_code(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(country="de", exchange="XETRA", limit=5)
        query = str(screen.call_args.args[0].to_dict())
        assert "'exchange', 'GER'" in query
        # The largest caps first, as FMP ranks them.
        assert screen.call_args.kwargs["sortField"] == "intradaymarketcap"
        assert screen.call_args.kwargs["sortAsc"] is False

    def test_a_minor_unit_the_protocol_does_not_scale_names_no_currency(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        quotes = [
            {"symbol": "ACME.JO", "currency": "ZAc", "regularMarketPrice": 5120.0, "dividendRate": 2.0},
            {"symbol": "ACME.KW", "currency": "KWF", "regularMarketPrice": 735.0},
        ]
        with patch("yfinance.screener.screener.screen", return_value={"quotes": quotes}):
            rows = _screen_stocks_sync(sector="Technology", limit=5)
        assert [r["currency"] for r in rows] == [None, None]
        # A cents line's rand dividend moves to cents beside its price.
        assert rows[0]["lastAnnualDividend"] == 200.0

    def test_a_cboe_europe_screen_holds_its_pence_lines_to_the_bar(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(exchange="CXE", dividendMoreThan=150, limit=5)
        assert "'forward_dividend_per_share', 1.5" in str(screen.call_args.args[0].to_dict())

    def test_a_screen_with_no_filter_ranks_every_listing_by_cap(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        with patch("yfinance.screener.screener.screen", return_value={"quotes": []}) as screen:
            _screen_stocks_sync(limit=5)
        assert screen.call_args.args[0].to_dict() == {"operator": "GT", "operands": ["intradaymarketcap", 0]}
        assert screen.call_args.kwargs["sortField"] == "intradaymarketcap"

    def test_a_fund_screen_is_refused_rather_than_answered_with_stocks(self):
        from src.data_client.yfinance.financial_source import _screen_stocks_sync

        with pytest.raises(ValueError, match="stocks only"):
            _screen_stocks_sync(isEtf=True)


# ---------------------------------------------------------------------------
# screen_row_units
# ---------------------------------------------------------------------------

class TestScreenRowUnits:
    def test_rows_name_their_unit_and_a_pence_row_moves_to_pounds(self):
        from src.data_client.normalize import screen_row_units

        rows = screen_row_units([
            {"symbol": "SHEL.L", "price": 3784, "lastAnnualDividend": 112.82, "marketCap": 2.15e11},
            {"symbol": "AAPL", "price": 344.17, "marketCap": 5.0e12},
            {"symbol": "ACME.BA", "price": 39240, "lastAnnualDividend": 193.05},
            {"symbol": "BETA.L", "price": 3533, "currency": None},
            {"symbol": "GAMA.L", "price": 3784, "lastAnnualDividend": 116.0, "currency": "GBP", "quoteUnit": "GBX"},
        ])
        assert rows == [
            # The cap is already in pounds; a field a row lacks stays absent.
            {"symbol": "SHEL.L", "price": 37.84, "lastAnnualDividend": 1.1282, "marketCap": 2.15e11,
             "currency": "GBP", "quoteUnit": "GBX"},
            {"symbol": "AAPL", "price": 344.17, "marketCap": 5.0e12, "currency": "USD", "quoteUnit": "USD"},
            # An unknown venue resolves to a USD default, not its currency.
            {"symbol": "ACME.BA", "price": 39240, "lastAnnualDividend": 193.05, "currency": None, "quoteUnit": None},
            {"symbol": "BETA.L", "price": 3533, "currency": None, "quoteUnit": None},
            {"symbol": "GAMA.L", "price": 37.84, "lastAnnualDividend": 1.16, "currency": "GBP", "quoteUnit": "GBX"},
        ]
