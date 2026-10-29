"""Protocol boundary on the market-data router: spelling collapse + reverse map.

Every inbound spelling of one instrument must map to a single legacy-form
symbol before touching caches or providers (Phase 1; cache keys cut over to
instrument_key in Phase 3).
"""

import pytest

from src.server.app.market_data import _boundary_symbol, _boundary_symbols, _listing_symbol


class TestBoundarySymbol:
    def test_us_spellings_collapse(self):
        assert _boundary_symbol("AAPL") == "AAPL"
        assert _boundary_symbol("aapl") == "AAPL"
        assert _boundary_symbol("AAPL.US") == "AAPL"

    def test_index_spellings_collapse_to_legacy_bare(self):
        for spelling in ("GSPC", "^GSPC", "I:SPX", "SPX.INDEX"):
            assert _boundary_symbol(spelling, is_index=True) == "GSPC", spelling

    def test_foreign_suffixes_round_trip(self):
        assert _boundary_symbol("0700.HK") == "0700.HK"
        assert _boundary_symbol("0700.XHKG") == "0700.HK"
        assert _boundary_symbol("VOD.L") == "VOD.L"

    def test_us_class_shares_survive(self):
        # Dotted class shares must not lose their suffix (BRK.B ≠ BRK).
        assert _boundary_symbol("BRK.B") == "BRK.B"
        assert _boundary_symbol("BF.B") == "BF.B"

    def test_exchange_listed_index_keeps_no_caret(self):
        # CN exchange indexes resolve INDEX on a venue MIC; a caret would
        # reach the upstream verbatim and miss.
        assert _boundary_symbol("000001.SS", equity=True) == "000001.SH"
        assert _boundary_symbol("000300.SS", equity=True) == "000300.SH"
        assert _boundary_symbol("399001.SZ", equity=True) == "399001.SZ"

    def test_family_index_keeps_caret_on_equity_endpoint(self):
        assert _boundary_symbol("^GSPC", equity=True) == "^GSPC"

    def test_unknown_symbol_passes_through(self):
        assert _boundary_symbol("ZZZZFAKE1") == "ZZZZFAKE1"


class TestSeriesRouteRefusal:
    @pytest.mark.asyncio
    async def test_refused_spelling_is_422_before_the_cache(self):
        """The series cache keys on the canonical instrument, so a spelling the
        protocol refuses used to reach it and fail there as a 500."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from fastapi import HTTPException

        from src.server.app import market_data

        svc = MagicMock()
        svc.get_stock_intraday = AsyncMock()
        with (
            patch("src.server.app.market_data.IntradayCacheService.get_instance", return_value=svc),
            pytest.raises(HTTPException) as err,
        ):
            await market_data.get_stock_intraday(
                "AAPL..", user_id="u1", interval="1min", from_date=None, to_date=None,
            )

        assert err.value.status_code == 422
        svc.get_stock_intraday.assert_not_awaited()


class TestBoundarySymbols:
    def test_collapsing_spellings_dedupe(self):
        assert _boundary_symbols(["AAPL", "aapl", "AAPL.US", "MSFT"]) == ["AAPL", "MSFT"]

    def test_index_batch(self):
        assert _boundary_symbols(["^GSPC", "I:SPX", "IXIC"], is_index=True) == ["GSPC", "IXIC"]


class TestOverviewBoundary:
    @pytest.mark.asyncio
    async def test_alias_spelling_reaches_the_fetch_canonical_and_echoes_back(self, monkeypatch):
        """600519.SS fetches, caches and answers as 600519.SH. Respelling to
        what an upstream knows is the vendor adapters' job, below this route."""
        from src.server.app import market_data
        from src.tools.market_data import company

        seen: list[str] = []

        async def _fetch(symbol):
            seen.append(symbol)
            return {"symbol": symbol, "name": "贵州茅台", "currency": "CNY", "quote": {"yearHigh": 1568.0}}

        class _Cache:
            async def get(self, key):
                seen.append(f"cache:{key}")
                return None

            async def set(self, *a, **k):
                return None

        monkeypatch.setattr(company, "fetch_company_overview_data", _fetch)
        from src.utils.cache import redis_cache
        monkeypatch.setattr(redis_cache, "get_cache_client", lambda: _Cache())

        resp = await market_data.get_company_overview("600519.SS", user_id="u1")

        assert seen == ["cache:overview:600519.SH", "600519.SH"]
        assert resp.symbol == "600519.SH"
        assert resp.quote == {"yearHigh": 1568.0}


class TestListingSymbol:
    def test_listing_aliases_collapse(self):
        assert _listing_symbol("600519.SS") == "600519.SH"
        assert _listing_symbol("00700.HK") == "0700.HK"
        assert _listing_symbol("^GSPC") == "^GSPC"

    def test_an_fx_pair_keeps_the_spelling_its_providers_know(self):
        # The legacy form EUR-USD would resolve as an equity in the company data.
        assert _listing_symbol("eurusd=x") == "EURUSD=X"

    @pytest.mark.parametrize("refused", ["600519.SH,000858.SZ", "AAPL/X"])
    def test_a_refused_spelling_is_422_before_the_upstream(self, refused):
        # Passed through, the overview failed at the upstream as a 500.
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as err:
            _listing_symbol(refused)
        assert err.value.status_code == 422


class TestOverviewCacheHit:
    @pytest.mark.asyncio
    async def test_a_cached_quote_is_measured_when_served(self, monkeypatch):
        """A label measured when the overview was cached would keep calling an
        aging print live for the entry's whole lifetime."""
        from datetime import datetime, timezone

        from src.data_client import freshness
        from src.server.app import market_data
        from src.utils.cache import redis_cache

        printed = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)  # a Wednesday, 10:00 ET
        cached = {
            "symbol": "AAPL", "currency": "USD",
            "quote": {"price": 210.0, "tier": "realtime", "freshness": {
                "actual_latest": int(printed.timestamp() * 1000), "lag_s": 0,
                "label": "live", "measured": True, "source": "fmp",
            }},
        }

        class _Cache:
            async def get(self, key):
                return cached

        monkeypatch.setattr(redis_cache, "get_cache_client", lambda: _Cache())
        monkeypatch.setattr(freshness, "_now", lambda now=None: printed.replace(minute=10))

        resp = await market_data.get_company_overview("AAPL", user_id="u1")

        fresh = resp.quote["freshness"]
        assert (fresh["label"], fresh["lag_s"]) == ("delayed", 600)
        assert fresh["source"] == "fmp"
        assert cached["quote"]["freshness"]["label"] == "live"  # the entry itself is untouched


class TestSearchFailure:
    @pytest.mark.asyncio
    async def test_failure_keeps_the_query_and_upstream_url_out_of_log_and_answer(self, monkeypatch, caplog):
        import logging

        import httpx
        from fastapi import HTTPException

        from src.server.app import market_data
        from src.utils.cache import redis_cache
        import src.data_client as data_client

        class _Cache:
            async def get(self, key):
                return None

        class _Financial:
            async def search_stocks(self, query, limit):
                raise httpx.ConnectError("connect failed: http://data.example.com/v2/search?q=secretquery")

        class _Provider:
            financial = _Financial()

        async def _provider():
            return _Provider()

        monkeypatch.setattr(redis_cache, "get_cache_client", lambda: _Cache())
        monkeypatch.setattr(data_client, "get_financial_data_provider", _provider)

        with caplog.at_level(logging.ERROR), pytest.raises(HTTPException) as err:
            await market_data.search_stocks(user_id="u1", query="secretquery", limit=5, exchange=[])

        assert err.value.status_code == 500
        assert "secretquery" not in str(err.value.detail)
        assert "example.com" not in str(err.value.detail)
        assert "secretquery" not in caplog.text
        assert "ConnectError" in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_fallback_directory_answers_uncached(self, monkeypatch):
        """The default found no match and the directory asked after it was
        down: the search still answers, but not as a cached "no match"."""
        import httpx

        from src.data_client.financial_data_provider import MarketRoute, RoutedFinancialSource
        from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource, has_cjk
        from src.server.app import market_data
        from src.utils.cache import redis_cache
        import src.data_client as data_client

        class _Cache:
            def __init__(self):
                self.writes = []

            async def get(self, key):
                return None

            async def set(self, key, value, ttl=None):
                self.writes.append(key)

        class _NoMatch:
            async def search_stocks(self, query, limit=50):
                return []

        class _Down:
            async def search_v2(self, query, *, market, limit):
                raise httpx.ConnectError("down")

        class _Provider:
            financial = RoutedFinancialSource(
                default=_NoMatch(),
                by_market={"cn": MarketRoute(
                    GinlixDataCnFinancialSource(_Down()), claims_query=has_cjk,
                )},
            )

        async def _provider():
            return _Provider()

        cache = _Cache()
        monkeypatch.setattr(redis_cache, "get_cache_client", lambda: cache)
        monkeypatch.setattr(data_client, "get_financial_data_provider", _provider)

        resp = await market_data.search_stocks(user_id="u1", query="XYZQW", limit=5, exchange=[])

        assert (resp.results, resp.count) == ([], 0)
        assert cache.writes == []
