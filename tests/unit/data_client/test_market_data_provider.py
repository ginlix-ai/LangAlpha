"""Tests for the MarketDataProvider chain-of-responsibility pattern."""

from __future__ import annotations

import pytest

from src.data_client.market_data_provider import (
    MarketDataProvider,
    ProviderEntry,
    symbol_market,
)


# ---------------------------------------------------------------------------
# Helpers — lightweight fake data sources
# ---------------------------------------------------------------------------

class FakeSource:
    """Configurable fake MarketDataSource for testing."""

    def __init__(self, name: str = "fake", *, fail: bool = False, empty: bool = False):
        self.name = name
        self.fail = fail
        self.empty = empty
        self.calls: list[tuple[str, dict]] = []
        self.closed = False

    async def get_intraday(self, **kwargs):
        self.calls.append(("get_intraday", kwargs))
        if self.fail:
            raise RuntimeError(f"{self.name} intraday error")
        if self.empty:
            return []
        return [{"date": "2025-01-01", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}]

    async def get_daily(self, **kwargs):
        self.calls.append(("get_daily", kwargs))
        if self.fail:
            raise RuntimeError(f"{self.name} daily error")
        return [{"date": "2025-01-01", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}]

    async def close(self):
        self.closed = True


class SnapshotSource(FakeSource):
    """Fake source that returns configured snapshots for requested symbols."""

    def __init__(
        self,
        name: str,
        snapshots: dict[str, dict] | None = None,
        *,
        extra_rows: list[dict] | None = None,
        fail: bool = False,
    ):
        super().__init__(name, fail=fail)
        self.snapshots = {
            str(k).strip().upper(): v for k, v in (snapshots or {}).items()
        }
        self.extra_rows = extra_rows or []

    async def get_snapshots(self, **kwargs):
        self.calls.append(("get_snapshots", kwargs))
        if self.fail:
            raise RuntimeError(f"{self.name} snapshots error")
        return [
            self.snapshots[str(s).strip().upper()]
            for s in kwargs.get("symbols", [])
            if str(s).strip().upper() in self.snapshots
        ] + list(self.extra_rows)


# ---------------------------------------------------------------------------
# symbol_market tests
# ---------------------------------------------------------------------------

class TestSymbolMarket:
    def test_bare_symbol_is_us(self):
        assert symbol_market("AAPL") == "us"

    def test_us_suffix(self):
        assert symbol_market("AAPL.US") == "us"

    def test_hk_suffix(self):
        assert symbol_market("0700.HK") == "hk"

    def test_shanghai_suffix(self):
        assert symbol_market("600519.SS") == "cn"

    def test_shenzhen_suffix(self):
        assert symbol_market("000001.SZ") == "cn"

    def test_london_suffix(self):
        assert symbol_market("SHEL.L") == "uk"

    def test_tokyo_suffix(self):
        assert symbol_market("7203.T") == "jp"

    def test_unknown_suffix(self):
        assert symbol_market("XYZ.ZZ") == "other"

    def test_case_insensitive(self):
        assert symbol_market("0700.hk") == "hk"


# ---------------------------------------------------------------------------
# MarketDataProvider tests
# ---------------------------------------------------------------------------

class TestMarketDataProvider:
    @pytest.mark.asyncio
    async def test_single_provider_passthrough(self):
        src = FakeSource("primary")
        provider = MarketDataProvider([ProviderEntry("primary", src, {"all"})])
        result = await provider.get_intraday(symbol="AAPL", interval="1min")
        assert len(result) == 1
        assert src.calls == [("get_intraday", {"symbol": "AAPL", "interval": "1min", "from_date": None, "to_date": None, "is_index": False, "user_id": None})]

    @pytest.mark.asyncio
    async def test_us_symbol_primary_succeeds_no_fallback(self):
        primary = FakeSource("ginlix")
        fallback = FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("ginlix", primary, {"us"}),
            ProviderEntry("fmp", fallback, {"all"}),
        ])
        result = await provider.get_intraday(symbol="AAPL", interval="1min")
        assert len(result) == 1
        assert len(primary.calls) == 1
        assert len(fallback.calls) == 0

    @pytest.mark.asyncio
    async def test_us_symbol_primary_fails_fallback_called(self):
        primary = FakeSource("ginlix", fail=True)
        fallback = FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("ginlix", primary, {"us"}),
            ProviderEntry("fmp", fallback, {"all"}),
        ])
        result = await provider.get_intraday(symbol="AAPL", interval="1min")
        assert len(result) == 1
        assert len(primary.calls) == 1
        assert len(fallback.calls) == 1

    @pytest.mark.asyncio
    async def test_non_us_symbol_skips_us_only_provider(self):
        us_only = FakeSource("ginlix")
        global_src = FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("ginlix", us_only, {"us"}),
            ProviderEntry("fmp", global_src, {"all"}),
        ])
        result = await provider.get_daily(symbol="0700.HK")
        assert len(result) == 1
        assert len(us_only.calls) == 0  # skipped — no HK market coverage
        assert len(global_src.calls) == 1

    @pytest.mark.asyncio
    async def test_empty_result_falls_through_to_next_source(self):
        # A source may cover the market but return no bars for this
        # symbol/window (e.g. Yahoo lookback caps) — the chain must try the
        # rest instead of accepting the empty list. Caught live: yfinance
        # returned [] for HK 1h and the chain never reached the next provider.
        empty = FakeSource("empty-first", empty=True)
        full = FakeSource("full-second")
        provider = MarketDataProvider([
            ProviderEntry(source=empty, name="empty-first", markets={"all"}),
            ProviderEntry(source=full, name="full-second", markets={"all"}),
        ])
        bars, source, _ = await provider.get_intraday_with_source("0700.HK", "1hour")
        assert bars and source == "full-second"
        assert len(empty.calls) == 1 and len(full.calls) == 1

    @pytest.mark.asyncio
    async def test_all_sources_empty_returns_first_empty(self):
        e1, e2 = FakeSource("e1", empty=True), FakeSource("e2", empty=True)
        provider = MarketDataProvider([
            ProviderEntry(source=e1, name="e1", markets={"all"}),
            ProviderEntry(source=e2, name="e2", markets={"all"}),
        ])
        bars, source, truncated = await provider.get_intraday_with_source("AAPL", "1hour")
        assert bars == [] and source == "e1" and truncated is False

    @pytest.mark.asyncio
    async def test_all_providers_fail_raises_last_exception(self):
        src1 = FakeSource("a", fail=True)
        src2 = FakeSource("b", fail=True)
        provider = MarketDataProvider([
            ProviderEntry("a", src1, {"all"}),
            ProviderEntry("b", src2, {"all"}),
        ])
        with pytest.raises(RuntimeError, match="b daily error"):
            await provider.get_daily(symbol="AAPL")

    @pytest.mark.asyncio
    async def test_no_providers_for_market_raises(self):
        us_only = FakeSource("ginlix")
        provider = MarketDataProvider([
            ProviderEntry("ginlix", us_only, {"us"}),
        ])
        with pytest.raises(RuntimeError, match="No data source configured"):
            await provider.get_intraday(symbol="0700.HK", interval="1min")

    @pytest.mark.asyncio
    async def test_close_closes_all_sources(self):
        src1 = FakeSource("a")
        src2 = FakeSource("b")
        provider = MarketDataProvider([
            ProviderEntry("a", src1, {"all"}),
            ProviderEntry("b", src2, {"all"}),
        ])
        await provider.close()
        assert src1.closed
        assert src2.closed

    @pytest.mark.asyncio
    async def test_close_continues_on_error(self):
        """Even if one source's close() raises, other sources are still closed."""
        class FailCloseSource(FakeSource):
            async def close(self):
                raise RuntimeError("close failed")

        src1 = FailCloseSource("a")
        src2 = FakeSource("b")
        provider = MarketDataProvider([
            ProviderEntry("a", src1, {"all"}),
            ProviderEntry("b", src2, {"all"}),
        ])
        await provider.close()  # should not raise
        assert src2.closed

    def test_source_names(self):
        provider = MarketDataProvider([
            ProviderEntry("ginlix-data", FakeSource(), {"us"}),
            ProviderEntry("fmp", FakeSource(), {"all"}),
        ])
        assert provider.source_names == ["ginlix-data", "fmp"]

    @pytest.mark.asyncio
    async def test_get_daily_passthrough(self):
        src = FakeSource("fmp")
        provider = MarketDataProvider([ProviderEntry("fmp", src, {"all"})])
        result = await provider.get_daily(symbol="MSFT", from_date="2025-01-01", to_date="2025-06-01")
        assert len(result) == 1
        assert src.calls[0] == ("get_daily", {
            "symbol": "MSFT",
            "from_date": "2025-01-01",
            "to_date": "2025-06-01",
            "is_index": False,
            "user_id": None,
        })

    @pytest.mark.asyncio
    async def test_multi_market_provider_routing(self):
        """A provider covering {hk, cn} should be used for HK and CN symbols."""
        asia_src = FakeSource("asia")
        global_src = FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("asia", asia_src, {"hk", "cn"}),
            ProviderEntry("fmp", global_src, {"all"}),
        ])

        await provider.get_intraday(symbol="0700.HK", interval="1min")
        assert len(asia_src.calls) == 1
        assert len(global_src.calls) == 0

        await provider.get_intraday(symbol="600519.SS", interval="1min")
        assert len(asia_src.calls) == 2
        assert len(global_src.calls) == 0

        # US symbol should skip asia provider
        await provider.get_intraday(symbol="AAPL", interval="1min")
        assert len(asia_src.calls) == 2  # unchanged
        assert len(global_src.calls) == 1

    @pytest.mark.asyncio
    async def test_get_snapshots_routes_each_symbol_by_market(self):
        us_src = SnapshotSource(
            "ginlix",
            {"AAPL": {"symbol": "AAPL", "price": 190.0}},
        )
        global_src = SnapshotSource(
            "fmp",
            {"301189.SZ": {"symbol": "301189.SZ", "price": 42.0}},
        )
        provider = MarketDataProvider(
            [
                ProviderEntry("ginlix", us_src, {"us"}),
                ProviderEntry("fmp", global_src, {"all"}),
            ]
        )

        result = await provider.get_snapshots(["AAPL", "301189.SZ"])

        assert [r["symbol"] for r in result] == ["AAPL", "301189.SZ"]
        assert us_src.calls[0][1]["symbols"] == ["AAPL"]
        assert global_src.calls[0][1]["symbols"] == ["301189.SZ"]

    @pytest.mark.asyncio
    async def test_get_snapshots_partial_resolution_fallback(self):
        primary_src = SnapshotSource(
            "primary",
            {"AAPL": {"symbol": "AAPL", "price": 190.0}},
        )
        fallback_src = SnapshotSource(
            "fallback",
            {"MSFT": {"symbol": "MSFT", "price": 420.0}},
        )
        provider = MarketDataProvider(
            [
                ProviderEntry("primary", primary_src, {"all"}),
                ProviderEntry("fallback", fallback_src, {"all"}),
            ]
        )

        result = await provider.get_snapshots(["AAPL", "MSFT"])

        assert result == [
            {"symbol": "AAPL", "price": 190.0, "source": "primary"},
            {"symbol": "MSFT", "price": 420.0, "source": "fallback"},
        ]
        assert len(primary_src.calls) == 1
        assert len(fallback_src.calls) == 1
        assert primary_src.calls[0][1]["symbols"] == ["AAPL", "MSFT"]
        assert fallback_src.calls[0][1]["symbols"] == ["MSFT"]

    @pytest.mark.asyncio
    async def test_get_snapshots_normalizes_whitespace_padded_input_symbols(self):
        primary_src = SnapshotSource(
            "primary",
            {"AAPL": {"symbol": "AAPL", "price": 190.0}},
        )
        fallback_src = SnapshotSource(
            "fallback",
            {"MSFT": {"symbol": "MSFT", "price": 420.0}},
        )
        provider = MarketDataProvider(
            [
                ProviderEntry("primary", primary_src, {"all"}),
                ProviderEntry("fallback", fallback_src, {"all"}),
            ]
        )

        result = await provider.get_snapshots(["  AAPL  ", " MSFT "])

        assert result == [
            {"symbol": "AAPL", "price": 190.0, "source": "primary"},
            {"symbol": "MSFT", "price": 420.0, "source": "fallback"},
        ]
        assert primary_src.calls[0][1]["symbols"] == ["  AAPL  ", " MSFT "]
        assert fallback_src.calls[0][1]["symbols"] == [" MSFT "]

    @pytest.mark.asyncio
    async def test_get_snapshots_routes_whitespace_padded_suffix_symbols(self):
        cn_src = SnapshotSource(
            "cn",
            {"301189.SZ": {"symbol": "301189.SZ", "price": 42.0}},
        )
        provider = MarketDataProvider(
            [ProviderEntry("cn", cn_src, {"cn"})]
        )

        result = await provider.get_snapshots([" 301189.SZ "])

        assert result == [{"symbol": "301189.SZ", "price": 42.0, "source": "cn"}]
        assert cn_src.calls[0][1]["symbols"] == [" 301189.SZ "]

    @pytest.mark.asyncio
    async def test_get_snapshots_extra_from_wrong_market_does_not_resolve_pending(self, caplog):
        us_src = SnapshotSource(
            "ginlix",
            {"AAPL": {"symbol": "AAPL", "price": 190.0}},
            extra_rows=[{"symbol": "300059.SZ", "price": 0.0}],
        )
        cn_src = SnapshotSource(
            "cn",
            {"300059.SZ": {"symbol": "300059.SZ", "price": 42.0}},
        )
        provider = MarketDataProvider(
            [
                ProviderEntry("ginlix", us_src, {"us"}),
                ProviderEntry("cn", cn_src, {"cn"}),
            ]
        )

        result = await provider.get_snapshots(["AAPL", "300059.SZ"])

        assert result == [
            {"symbol": "AAPL", "price": 190.0, "source": "ginlix"},
            {"symbol": "300059.SZ", "price": 42.0, "source": "cn"},
        ]
        assert us_src.calls[0][1]["symbols"] == ["AAPL"]
        assert cn_src.calls[0][1]["symbols"] == ["300059.SZ"]
        assert "market_data.snapshot.drop_unrequested" in caplog.text

    @pytest.mark.asyncio
    async def test_get_snapshots_keeps_caret_prefixed_index_symbol(self, caplog):
        # A provider that echoes the Yahoo caret form ("^GSPC") for a bare
        # requested index symbol ("GSPC") must be matched, not dropped —
        # normalize_symbol strips the caret. Regression for the index-card
        # 0.00 bug (#287).
        src = SnapshotSource(
            "caret",
            extra_rows=[{"symbol": "^GSPC", "price": 5000.0}],
        )
        provider = MarketDataProvider([ProviderEntry("caret", src, {"all"})])

        result = await provider.get_snapshots(["GSPC"], asset_type="indices")

        assert result == [{"symbol": "^GSPC", "price": 5000.0, "source": "caret"}]
        assert "market_data.snapshot.drop_unrequested" not in caplog.text

    @pytest.mark.asyncio
    async def test_get_snapshots_double_caret_row_does_not_alias_bare_index(self, caplog):
        # normalize_symbol strips exactly ONE leading caret (removeprefix, not
        # lstrip), so a malformed "^^GSPC" row normalizes to "^GSPC" and must
        # NOT alias the bare requested "GSPC" — it's dropped as unrequested
        # instead of resolving the request against the wrong data.
        src = SnapshotSource(
            "caret",
            extra_rows=[{"symbol": "^^GSPC", "price": 5000.0}],
        )
        provider = MarketDataProvider([ProviderEntry("caret", src, {"all"})])

        result = await provider.get_snapshots(["GSPC"], asset_type="indices")

        assert result == []
        assert "market_data.snapshot.drop_unrequested" in caplog.text

    @pytest.mark.asyncio
    async def test_get_snapshots_falls_back_when_provider_returns_no_rows(self):
        empty_src = SnapshotSource("primary")
        fallback_src = SnapshotSource(
            "fallback",
            {"301189.SZ": {"symbol": "301189.SZ", "price": 42.0}},
        )
        provider = MarketDataProvider(
            [
                ProviderEntry("primary", empty_src, {"all"}),
                ProviderEntry("fallback", fallback_src, {"all"}),
            ]
        )

        result = await provider.get_snapshots(["301189.SZ"])

        assert result == [{"symbol": "301189.SZ", "price": 42.0, "source": "fallback"}]
        assert empty_src.calls[0][1]["symbols"] == ["301189.SZ"]
        assert fallback_src.calls[0][1]["symbols"] == ["301189.SZ"]

    @pytest.mark.asyncio
    async def test_get_snapshots_drops_symbol_less_rows_and_keeps_symbol_pending(self, caplog):
        bad_src = SnapshotSource(
            "bad",
            extra_rows=[{"price": 999.0}],
        )
        fallback_src = SnapshotSource(
            "fallback",
            {"AAPL": {"symbol": "AAPL", "price": 190.0}},
        )
        provider = MarketDataProvider(
            [
                ProviderEntry("bad", bad_src, {"all"}),
                ProviderEntry("fallback", fallback_src, {"all"}),
            ]
        )

        result = await provider.get_snapshots(["AAPL"])

        assert result == [{"symbol": "AAPL", "price": 190.0, "source": "fallback"}]
        assert bad_src.calls[0][1]["symbols"] == ["AAPL"]
        assert fallback_src.calls[0][1]["symbols"] == ["AAPL"]
        assert "market_data.snapshot.drop_unkeyed" in caplog.text

    @pytest.mark.asyncio
    async def test_get_snapshots_returns_partial_results_when_other_symbols_have_no_matching_market(self):
        us_src = SnapshotSource(
            "ginlix",
            {"AAPL": {"symbol": "AAPL", "price": 190.0}},
        )
        provider = MarketDataProvider(
            [ProviderEntry("ginlix", us_src, {"us"})]
        )

        result = await provider.get_snapshots(["AAPL", "XYZ.ZZ"])

        assert result == [{"symbol": "AAPL", "price": 190.0, "source": "ginlix"}]
        assert us_src.calls[0][1]["symbols"] == ["AAPL"]

    @pytest.mark.asyncio
    async def test_get_snapshots_all_provider_errors_raise_last_exception(self):
        src1 = SnapshotSource("a", fail=True)
        src2 = SnapshotSource("b", fail=True)
        provider = MarketDataProvider(
            [
                ProviderEntry("a", src1, {"all"}),
                ProviderEntry("b", src2, {"all"}),
            ]
        )

        with pytest.raises(RuntimeError, match="b snapshots error"):
            await provider.get_snapshots(["AAPL"])

    @pytest.mark.asyncio
    async def test_get_snapshots_empty_priority_slot_does_not_block_catch_all(self):
        # yfinance appears twice sharing one source: an intraday-only priority
        # slot (empty snapshot coverage) then a catch-all. The empty first slot
        # must not mark the name "tried", or snapshot fallback to the catch-all
        # never runs and the US symbol is dropped.
        primary = SnapshotSource("primary")  # covers US but resolves nothing
        yf = SnapshotSource("yfinance", {"AAPL": {"symbol": "AAPL", "price": 190.0}})
        provider = MarketDataProvider([
            ProviderEntry("primary", primary, {"all"}),
            ProviderEntry("yfinance", yf, set(), intraday_markets={"non-us"}),
            ProviderEntry("yfinance", yf, {"all"}),
        ])

        result = await provider.get_snapshots(["AAPL"])

        assert result == [{"symbol": "AAPL", "price": 190.0, "source": "yfinance"}]
        assert len(yf.calls) == 1
        assert yf.calls[0][1]["symbols"] == ["AAPL"]


# ---------------------------------------------------------------------------
# FMPDataSource interval guard tests
# ---------------------------------------------------------------------------

class TestFMPDataSourceIntervalGuard:
    @pytest.mark.asyncio
    async def test_fmp_rejects_1s_interval(self):
        from src.data_client.fmp.data_source import FMPDataSource
        source = FMPDataSource()
        with pytest.raises(ValueError, match="not supported"):
            await source.get_intraday(symbol="AAPL", interval="1s")

    @pytest.mark.asyncio
    async def test_chain_surfaces_unsupported_interval_error(self):
        """When the only provider rejects an interval, the error propagates."""
        class IntervalAwareSource:
            async def get_intraday(self, **kwargs):
                if kwargs.get("interval") == "1s":
                    raise ValueError("1s not supported")
                return [{"date": "2025-01-01", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}]
            async def get_daily(self, **kwargs):
                return []
            async def close(self):
                pass

        provider = MarketDataProvider([ProviderEntry("only", IntervalAwareSource(), {"all"})])
        with pytest.raises(ValueError, match="1s not supported"):
            await provider.get_intraday(symbol="AAPL", interval="1s")


# ---------------------------------------------------------------------------
# Config accessor tests
# ---------------------------------------------------------------------------

class TestConfigAccessor:
    def test_default_providers_when_no_config(self):
        """get_market_data_providers returns FMP-only when key is missing."""
        from src.config.settings import get_nested_config
        # The function uses get_nested_config with a default
        result = get_nested_config("market_data.providers_nonexistent", [{"name": "fmp", "markets": ["all"]}])
        assert result == [{"name": "fmp", "markets": ["all"]}]

    def test_actual_config_has_providers(self):
        """config.yaml should have market_data.providers configured."""
        from src.config.settings import get_market_data_providers
        providers = get_market_data_providers()
        assert isinstance(providers, list)
        assert len(providers) >= 1
        names = [p["name"] for p in providers]
        assert "fmp" in names


# ---------------------------------------------------------------------------
# Per-capability routing tests
# ---------------------------------------------------------------------------

class TestCapabilityRouting:
    """intraday/daily/snapshot market overrides + duplicate priority entries."""

    def _chain(self):
        ginlix = FakeSource("ginlix")
        yf = FakeSource("yf")
        fmp = FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("ginlix-data", ginlix, {"us"}),
            ProviderEntry("yfinance", yf, set(), intraday_markets={"non-us"}),
            ProviderEntry("fmp", fmp, {"all"}),
            ProviderEntry("yfinance", yf, {"all"}),
        ])
        return provider, ginlix, yf, fmp

    @pytest.mark.asyncio
    async def test_non_us_intraday_prefers_yfinance(self):
        provider, ginlix, yf, fmp = self._chain()
        _, source, _ = await provider.get_intraday_with_source("0700.HK", interval="1hour")
        assert source == "yfinance"
        assert not fmp.calls and not ginlix.calls

    @pytest.mark.asyncio
    async def test_us_intraday_routing_unchanged(self):
        provider, ginlix, yf, fmp = self._chain()
        _, source, _ = await provider.get_intraday_with_source("AAPL", interval="1hour")
        assert source == "ginlix-data"
        assert not yf.calls and not fmp.calls

    @pytest.mark.asyncio
    async def test_non_us_daily_still_fmp_first(self):
        """The empty base `markets` keeps the priority slot out of daily routing."""
        provider, _, yf, _ = self._chain()
        _, source, _ = await provider.get_daily_with_source("0700.HK")
        assert source == "fmp"
        assert not yf.calls

    @pytest.mark.asyncio
    async def test_non_us_intraday_falls_back_to_fmp(self):
        yf = FakeSource("yf", fail=True)
        fmp = FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("yfinance", yf, set(), intraday_markets={"non-us"}),
            ProviderEntry("fmp", fmp, {"all"}),
            ProviderEntry("yfinance", yf, {"all"}),
        ])
        _, source, _ = await provider.get_intraday_with_source("0700.HK", interval="1hour")
        assert source == "fmp"

    @pytest.mark.asyncio
    async def test_duplicate_provider_tried_once_per_request(self):
        yf = FakeSource("yf", fail=True)
        provider = MarketDataProvider([
            ProviderEntry("yfinance", yf, set(), intraday_markets={"non-us"}),
            ProviderEntry("yfinance", yf, {"all"}),
        ])
        with pytest.raises(RuntimeError):
            await provider.get_intraday("0700.HK", interval="1hour")
        assert len(yf.calls) == 1

    def test_source_names_deduplicated_in_order(self):
        provider, *_ = self._chain()
        assert provider.source_names == ["ginlix-data", "yfinance", "fmp"]

    def test_source_names_for_intraday_prefers_yfinance_over_catch_all(self):
        """Non-US intraday: the priority slot puts yfinance ahead of fmp."""
        provider, *_ = self._chain()
        assert provider.source_names_for("0700.HK", "intraday") == ["yfinance", "fmp"]

    def test_source_names_for_snapshot_excludes_empty_coverage_slot(self):
        """US snapshot: the empty-coverage priority slot is skipped, so yfinance
        appears once in its catch-all position (last), not ahead of fmp."""
        provider, *_ = self._chain()
        names = provider.source_names_for("AAPL", "snapshot")
        assert names == ["ginlix-data", "fmp", "yfinance"]
        assert names.count("yfinance") == 1

    def test_non_us_token_never_matches_us(self):
        from src.data_client.market_data_provider import _market_matches
        assert _market_matches({"non-us"}, "hk")
        assert _market_matches({"non-us"}, "other")
        assert not _market_matches({"non-us"}, "us")
        assert not _market_matches(set(), "hk")


# ---------------------------------------------------------------------------
# Null-field snapshot recovery (Phase 2)
# ---------------------------------------------------------------------------

class TestNullRowRecovery:
    @pytest.mark.asyncio
    async def test_null_row_falls_through_to_next_provider(self):
        null_row = {"symbol": "AAPL", "name": None, "price": None, "change": None,
                    "change_percent": None, "previous_close": None, "open": None,
                    "high": None, "low": None, "volume": None}
        first = SnapshotSource("first", {"AAPL": null_row})
        second = SnapshotSource("second", {"AAPL": {"symbol": "AAPL", "price": 190.0}})
        provider = MarketDataProvider([
            ProviderEntry("first", first, {"all"}),
            ProviderEntry("second", second, {"all"}),
        ])
        out = await provider.get_snapshots(["AAPL"])
        assert out == [{"symbol": "AAPL", "price": 190.0, "source": "second"}]

    @pytest.mark.asyncio
    async def test_unresolvable_symbol_absent_from_results(self):
        null_row = {"symbol": "ZZZFAKE", "price": None, "change": None,
                    "change_percent": None, "previous_close": None, "open": None,
                    "high": None, "low": None, "volume": None}
        first = SnapshotSource("first", {"AAPL": {"symbol": "AAPL", "price": 190.0},
                                         "ZZZFAKE": null_row})
        second = SnapshotSource("second", {})
        provider = MarketDataProvider([
            ProviderEntry("first", first, {"all"}),
            ProviderEntry("second", second, {"all"}),
        ])
        out = await provider.get_snapshots(["AAPL", "ZZZFAKE"])
        assert [r["symbol"] for r in out] == ["AAPL"]
