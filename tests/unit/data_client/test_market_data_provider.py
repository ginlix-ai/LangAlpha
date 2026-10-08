"""Tests for the MarketDataProvider chain-of-responsibility pattern."""

from __future__ import annotations

import pytest

from src.data_client.ginlix_data.directory import fill_quote_names
from src.data_client.market_data_provider import (
    MarketDataProvider,
    ProviderEntry,
    symbol_market,
)


def _without_tier(rows):
    """Routing tests compare rows the chain produced; the stamped freshness tier is covered separately."""
    return [{k: v for k, v in row.items() if k != "tier"} for row in rows]


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

    def test_full_width_spelling_routes_like_to_canonical_reads_it(self):
        # A Chinese IME types full-width forms and the ideographic full stop.
        assert symbol_market("６００５１９．ＳＨ") == "cn"
        assert symbol_market("600519。SH") == "cn"
        assert symbol_market("＾ＨＳＩ") == "hk"


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
    async def test_get_snapshots_miss_still_reaches_a_provider_already_asked_for_others(self):
        """Rounds run providers concurrently: fmp answers the CN name in round
        one, and must still get the US name after its own provider misses."""
        us_src = SnapshotSource("ginlix", {})
        global_src = SnapshotSource("fmp", {
            "AAPL": {"symbol": "AAPL", "price": 190.0},
            "301189.SZ": {"symbol": "301189.SZ", "price": 42.0},
        })
        provider = MarketDataProvider([
            ProviderEntry("ginlix", us_src, {"us"}),
            ProviderEntry("fmp", global_src, {"all"}),
        ])

        result = await provider.get_snapshots(["AAPL", "301189.SZ"])

        assert [r["symbol"] for r in result] == ["AAPL", "301189.SZ"]
        assert [c[1]["symbols"] for c in global_src.calls] == [["301189.SZ"], ["AAPL"]]

    @pytest.mark.asyncio
    async def test_get_snapshots_stamps_the_declared_quote_tier(self):
        """The header says realtime / delayed from the provider's declaration:
        FMP is realtime only for US, yfinance is a 15-minute feed, and a row that
        already carries its own tier (a daily-derived quote) keeps it."""
        fmp = SnapshotSource("fmp", {
            "AAPL": {"symbol": "AAPL", "price": 190.0},
            "0700.HK": {"symbol": "0700.HK", "price": 400.0},
            "601318.SS": {"symbol": "601318.SS", "price": 50.0, "tier": "eod"},
        })
        yf = SnapshotSource("yfinance", {"VOD.L": {"symbol": "VOD.L", "price": 1.0}})
        provider = MarketDataProvider([
            ProviderEntry("fmp", fmp, {"us", "cn", "hk"}),
            ProviderEntry("yfinance", yf, {"all"}),
        ])
        rows = {r["symbol"]: r for r in await provider.get_snapshots(["AAPL", "0700.HK", "601318.SS", "VOD.L"])}
        assert rows["AAPL"]["tier"] == "realtime"
        assert rows["0700.HK"]["tier"] == "delayed_15m"
        assert rows["601318.SS"]["tier"] == "eod"
        assert rows["VOD.L"]["tier"] == "delayed_15m"

    @pytest.mark.asyncio
    async def test_index_tier_follows_the_family_home_market(self):
        """Indices are requested caret-free, which the suffix parser reads as US;
        the freshness stamp must still know HSI is a Hong Kong print."""
        fmp = SnapshotSource("fmp", {
            "GSPC": {"symbol": "GSPC", "price": 5000.0},
            "HSI": {"symbol": "HSI", "price": 25000.0},
        })
        provider = MarketDataProvider([ProviderEntry("fmp", fmp, {"all"})])
        rows = {r["symbol"]: r for r in await provider.get_snapshots(["GSPC", "HSI"], asset_type="indices")}
        assert rows["GSPC"]["tier"] == "realtime"
        assert rows["HSI"]["tier"] == "delayed_15m"

    @pytest.mark.asyncio
    async def test_caret_index_on_the_stocks_endpoint_routes_to_its_home_market(self):
        """The queue is keyed caret-free, but ^FTSE is the London index, not a
        US ticker FTSE: the US-only provider must not be asked for it."""
        us_src = SnapshotSource("ginlix", {"FTSE": {"symbol": "FTSE", "price": 1.0}})
        global_src = SnapshotSource("fmp", {"^FTSE": {"symbol": "^FTSE", "price": 8000.0}})
        provider = MarketDataProvider([
            ProviderEntry("ginlix", us_src, {"us"}),
            ProviderEntry("fmp", global_src, {"all"}),
        ])

        rows = await provider.get_snapshots(["^FTSE"], asset_type="stocks")

        assert [(r["symbol"], r["source"]) for r in rows] == [("^FTSE", "fmp")]
        assert us_src.calls == []

    @pytest.mark.asyncio
    async def test_get_snapshots_attaches_dual_names(self, monkeypatch):
        """CN/HK rows gain additive name_local/name_en; the provider's own name
        stays untouched, and non-CN rows pass through clean."""
        async def _fake_names(syms):
            return {
                s: ("甲公司", "Alpha Co., Ltd.") if s == "600001.SS" else (None, None) for s in syms
            }

        monkeypatch.setattr("src.data_client.ginlix_data.directory.display_names_many", _fake_names)
        src = SnapshotSource("fmp", {
            "600001.SS": {"symbol": "600001.SS", "price": 42.0, "name": "Alpha Co"},
            "AAPL": {"symbol": "AAPL", "price": 190.0, "name": "Apple Inc."},
        })
        provider = MarketDataProvider([ProviderEntry("fmp", src, {"all"})], name_rows=fill_quote_names)

        result = await provider.get_snapshots(["600001.SS", "AAPL"])

        by_sym = {r["symbol"]: r for r in result}
        assert by_sym["600001.SS"]["name"] == "Alpha Co"
        assert by_sym["600001.SS"]["name_local"] == "甲公司"
        assert by_sym["600001.SS"]["name_en"] == "Alpha Co., Ltd."
        assert by_sym["AAPL"]["name"] == "Apple Inc."
        assert "name_local" not in by_sym["AAPL"]

    @pytest.mark.asyncio
    async def test_get_snapshots_fills_missing_name_from_directory(self, monkeypatch):
        """A nameless row (tushare-served) gets name filled English-first."""
        async def _fake_names(syms):
            return {s: ("甲公司", "Alpha Co., Ltd.") for s in syms}

        monkeypatch.setattr("src.data_client.ginlix_data.directory.display_names_many", _fake_names)
        src = SnapshotSource("tushare", {
            "600001.SS": {"symbol": "600001.SS", "price": 42.0},
        })
        provider = MarketDataProvider([ProviderEntry("tushare", src, {"all"})], name_rows=fill_quote_names)

        result = await provider.get_snapshots(["600001.SS"])

        assert result[0]["name"] == "Alpha Co., Ltd."
        assert result[0]["name_local"] == "甲公司"

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

        assert _without_tier(result) == [
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

        assert _without_tier(result) == [
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

        assert _without_tier(result) == [{"symbol": "301189.SZ", "price": 42.0, "source": "cn"}]
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

        assert _without_tier(result) == [
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

        assert _without_tier(result) == [{"symbol": "^GSPC", "price": 5000.0, "source": "caret"}]
        assert "market_data.snapshot.drop_unrequested" not in caplog.text

    @pytest.mark.asyncio
    async def test_get_snapshots_matches_across_shanghai_spellings(self, caplog):
        # A caller asking ``.SH`` gets the vendor's ``.SS`` row, not a drop.
        src = SnapshotSource(
            "cn", extra_rows=[{"symbol": "600519.SS", "price": 1.0}],
        )
        provider = MarketDataProvider([ProviderEntry("cn", src, {"all"})])

        result = await provider.get_snapshots(["600519.SH"], asset_type="stocks")

        assert [r["symbol"] for r in result] == ["600519.SS"]
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

        assert _without_tier(result) == [{"symbol": "301189.SZ", "price": 42.0, "source": "fallback"}]
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

        assert _without_tier(result) == [{"symbol": "AAPL", "price": 190.0, "source": "fallback"}]
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

        assert _without_tier(result) == [{"symbol": "AAPL", "price": 190.0, "source": "ginlix"}]
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

        assert _without_tier(result) == [{"symbol": "AAPL", "price": 190.0, "source": "yfinance"}]
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
        assert _without_tier(out) == [{"symbol": "AAPL", "price": 190.0, "source": "second"}]

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


class StatusSource(FakeSource):
    """Fake source exposing get_market_status."""

    def __init__(self, name: str, market_label: str, *, fail: bool = False):
        super().__init__(name, fail=fail)
        self.market_label = market_label

    async def get_market_status(self, user_id=None):
        self.calls.append(("get_market_status", {"user_id": user_id}))
        if self.fail:
            raise RuntimeError(f"{self.name} status error")
        return {"market": "open", "exchanges": {self.market_label: "open"}}


class TestMarketStatusRouting:
    @pytest.mark.asyncio
    async def test_us_default_skips_cn_only_entry(self):
        cn = StatusSource("tushare", "sse")
        us = StatusSource("fmp", "nasdaq")
        provider = MarketDataProvider([
            ProviderEntry("tushare", cn, {"cn"}),
            ProviderEntry("fmp", us, {"all"}),
        ])
        out = await provider.get_market_status()
        assert out["exchanges"] == {"nasdaq": "open"}
        assert cn.calls == []

    @pytest.mark.asyncio
    async def test_non_us_market_is_the_venue_calendar(self):
        """Even a provider listing cn explicitly is not asked: status is a clock."""
        cn = StatusSource("tushare", "sse")
        us = StatusSource("fmp", "nasdaq")
        provider = MarketDataProvider([
            ProviderEntry("tushare", cn, {"cn"}),
            ProviderEntry("fmp", us, {"us"}),
        ])
        out = await provider.get_market_status(market="cn")
        assert out["exchanges"] is None
        assert out["serverTime"].endswith("+08:00")
        assert cn.calls == [] and us.calls == []

    def test_providers_list_only_the_markets_own(self):
        """The shipped chain's shape: a CN-only entry is not credited on a US
        status, and a name listed twice keeps its first chain position."""
        src = FakeSource("x")
        provider = MarketDataProvider([
            ProviderEntry("ginlix-data", src, {"us"}),
            ProviderEntry("tushare", src, {"cn"}, intraday_markets=set(), snapshot_markets={"cn"}),
            ProviderEntry("yfinance", src, set(), intraday_markets={"non-us"}),
            ProviderEntry("fmp", src, {"all"}),
            ProviderEntry("yfinance", src, {"all"}),
        ])
        assert provider.source_names_for_market("us") == ["ginlix-data", "yfinance", "fmp"]
        assert provider.source_names_for_market("cn") == ["tushare", "yfinance", "fmp"]
        assert provider.source_names_for_market("hk") == ["yfinance", "fmp"]

    @pytest.mark.asyncio
    async def test_no_covering_source_raises(self):
        cn = StatusSource("tushare", "sse")
        provider = MarketDataProvider([ProviderEntry("tushare", cn, {"cn"})])
        with pytest.raises(RuntimeError, match="No data source supports"):
            await provider.get_market_status(market="us")

    @pytest.mark.asyncio
    async def test_catch_all_entry_never_answers_a_foreign_market(self):
        """A provider matched through ``all`` only knows the US session: the
        venue calendar answers for hk, in the venue's own clock."""
        us = StatusSource("fmp", "nasdaq")
        provider = MarketDataProvider([ProviderEntry("fmp", us, {"all"})])
        out = await provider.get_market_status(market="hk")
        assert us.calls == []
        assert out["exchanges"] is None
        assert out["market"] in {"open", "closed"}
        assert out["serverTime"].endswith("+08:00")

    @pytest.mark.asyncio
    async def test_calendar_status_unknown_market_raises(self):
        from src.data_client.market_data_provider import calendar_market_status

        with pytest.raises(ValueError, match="No market calendar"):
            calendar_market_status("mars")

    @pytest.mark.asyncio
    async def test_failure_falls_through_to_next_covering_entry(self):
        first = StatusSource("first", "nasdaq", fail=True)
        second = StatusSource("second", "nyse")
        provider = MarketDataProvider([
            ProviderEntry("first", first, {"all"}),
            ProviderEntry("second", second, {"all"}),
        ])
        out = await provider.get_market_status()
        assert out["exchanges"] == {"nyse": "open"}


# ---------------------------------------------------------------------------
# Generated routing ruleset (data_routing.yaml) overriding the config chain
# ---------------------------------------------------------------------------

def _table(cells: dict[str, list[str]]):
    """RoutingTable from ``{"cn/equity/intraday/1m": ["fmp", "!yfinance"]}``.

    A ``!`` marks a provider the cell measured and excluded.
    """
    from datetime import datetime, timezone

    from market_protocol.routing import (
        Cell, CellKey, ProbedProvider, Ruleset, RoutingTable, Surface,
    )

    built = []
    for cell, names in cells.items():
        market, asset_class, surface, *rest = cell.split("/")
        built.append(Cell(
            key=CellKey(
                market=market,
                asset_class=asset_class,
                surface=Surface(surface),
                interval=rest[0] if rest else None,
            ),
            probed_at=datetime(2026, 9, 9, tzinfo=timezone.utc),
            providers=[
                ProbedProvider(
                    name=n.lstrip("!"), coverage_hit=1, coverage_total=1,
                    excluded_reason="incomplete_session" if n.startswith("!") else None,
                )
                for n in names
            ],
        ))
    return RoutingTable(Ruleset(generated_at=datetime(2026, 9, 9, tzinfo=timezone.utc), cells=built))


class _RecordingTable:
    """A routing table with no cells that records every lookup it is asked."""

    def __init__(self):
        self.asked: list[tuple] = []

    def order_for(self, surface, market, asset_class, interval=None):
        self.asked.append((str(surface), market, asset_class.value, interval))
        return None


class TestRoutingCellLookup:
    """The cell a request reads, as handed to ``RoutingTable.order_for``."""

    def _asked(self, symbol, capability, interval=None, is_index=False):
        table = _RecordingTable()
        provider = MarketDataProvider([ProviderEntry("fmp", FakeSource("fmp"), {"all"})], routing=table)
        provider.source_names_for(symbol, capability, interval, is_index=is_index)
        return table.asked

    def test_cn_equity_intraday(self):
        assert self._asked("600519.SS", "intraday", "1min") == [("intraday", "cn", "equity", "1min")]

    def test_cn_etf_is_a_fund_even_through_the_equity_endpoint(self):
        assert self._asked("510300.SS", "intraday", "5min") == [("intraday", "cn", "fund", "5min")]

    def test_caret_free_index_keeps_its_home_market(self):
        assert self._asked("HSI", "snapshot", is_index=True) == [("snapshot", "hk", "index", None)]

    def test_market_matches_the_protocol_token(self):
        """ginlix-data keys the same ruleset by ``market_of``; the two must agree."""
        assert self._asked("I:HSI", "snapshot", is_index=True)[0][1] == "hk"
        assert self._asked("SAP.XETR", "daily")[0][1] == "eu"
        assert self._asked("0700.XHKG", "daily")[0][1] == "hk"
        assert self._asked("EURUSD=X", "snapshot")[0][1:3] == ("fx", "fx")

    def test_daily_and_snapshot_carry_no_interval(self):
        assert self._asked("AAPL", "daily", "1min") == [("daily", "us", "equity", None)]
        assert self._asked("AAPL", "snapshot") == [("snapshot", "us", "equity", None)]

    def test_unknown_interval_or_capability_reads_no_cell(self):
        assert self._asked("AAPL", "intraday", "3min") == []
        assert self._asked("AAPL", "intraday", None) == []
        assert self._asked("AAPL", "status") == []


class TestRuleSetRouting:
    def _chain(self, routing=None):
        """The shipped chain shape: tushare cn (no intraday), yfinance non-us
        intraday priority slot, fmp catch-all, yfinance catch-all."""
        ts, yf, fmp = FakeSource("ts"), FakeSource("yf"), FakeSource("fmp")
        provider = MarketDataProvider([
            ProviderEntry("tushare", ts, {"cn"}, intraday_markets=set(), snapshot_markets={"cn"}),
            ProviderEntry("yfinance", yf, set(), intraday_markets={"non-us"}),
            ProviderEntry("fmp", fmp, {"all"}),
            ProviderEntry("yfinance", yf, {"all"}),
        ], routing=routing)
        return provider, ts, yf, fmp

    def test_config_order_without_a_ruleset(self):
        provider, *_ = self._chain()
        assert provider.source_names_for("600519.SS", "intraday", "1min") == ["yfinance", "fmp"]

    @pytest.mark.asyncio
    async def test_cell_order_beats_config_order(self):
        provider, _, yf, _ = self._chain(_table({"cn/equity/intraday/1m": ["fmp", "yfinance"]}))
        _, source, _ = await provider.get_intraday_with_source("600519.SS", interval="1min")
        assert source == "fmp"
        assert yf.calls == []

    @pytest.mark.asyncio
    async def test_cell_order_still_falls_back_on_failure(self):
        table = _table({"cn/equity/intraday/1m": ["fmp", "yfinance"]})
        provider, _, _, fmp = self._chain(table)
        fmp.fail = True
        _, source, _ = await provider.get_intraday_with_source("600519.SS", interval="1min")
        assert source == "yfinance"

    @pytest.mark.asyncio
    async def test_cell_reaches_a_provider_the_config_excludes(self):
        """tushare has ``intraday_markets: []``; a cell that measured it routable
        puts it back in play without touching config.yaml."""
        provider, ts, _, _ = self._chain(_table({"cn/equity/intraday/1m": ["tushare"]}))
        _, source, _ = await provider.get_intraday_with_source("600519.SS", interval="1min")
        assert source == "tushare"
        assert ts.calls[0][1]["interval"] == "1min"

    @pytest.mark.asyncio
    async def test_no_matching_cell_falls_back_to_config(self):
        table = _table({"cn/equity/intraday/1m": ["fmp"]})
        provider, *_ = self._chain(table)
        # A different market and (510300.SS) a different asset class miss the
        # cell; a different interval takes the nearest probed one.
        assert provider.source_names_for("600519.SS", "intraday", "5min") == ["fmp", "yfinance"]
        assert provider.source_names_for("0700.HK", "intraday", "1min") == ["yfinance", "fmp"]
        assert provider.source_names_for("510300.SS", "intraday", "1min") == ["yfinance", "fmp"]
        _, source, _ = await provider.get_intraday_with_source("0700.HK", interval="1min")
        assert source == "yfinance"

    @pytest.mark.asyncio
    async def test_a_borrowed_width_orders_the_chain_without_excluding(self):
        """4hour has no cell, so the table answers from 1h's. yfinance leads
        there but has no 4-hour bars; fmp, excluded only at 1h, must follow it."""
        provider, _, yf, _ = self._chain(_table({"cn/equity/intraday/1h": ["yfinance", "!fmp"]}))
        assert provider.source_names_for("600519.SS", "intraday", "1hour") == ["yfinance"]
        assert provider.source_names_for("600519.SS", "intraday", "4hour") == ["yfinance", "fmp"]
        yf.fail = True
        _, source, _ = await provider.get_intraday_with_source("600519.SS", interval="4hour")
        assert source == "fmp"

    def test_interval_less_cell_governs_every_interval(self):
        provider, *_ = self._chain(_table({"cn/equity/intraday": ["fmp"]}))
        assert provider.source_names_for("600519.SS", "intraday", "30min") == ["fmp", "yfinance"]

    def test_interval_less_cell_excludes_at_every_width(self):
        """Unlike a borrowed width, a cell probed without one speaks for them all."""
        provider, *_ = self._chain(_table({"cn/equity/intraday": ["fmp", "!yfinance"]}))
        assert provider.source_names_for("600519.SS", "intraday", "30min") == ["fmp"]

    def test_daily_cell_applies_to_daily_only(self):
        provider, *_ = self._chain(_table({"cn/equity/daily": ["fmp", "tushare"]}))
        assert provider.source_names_for("600519.SS", "daily") == ["fmp", "tushare", "yfinance"]
        assert provider.source_names_for("600519.SS", "intraday", "1min") == ["yfinance", "fmp"]

    @pytest.mark.asyncio
    async def test_daily_routes_through_the_cell(self):
        provider, ts, _, _ = self._chain(_table({"cn/equity/daily": ["tushare"]}))
        _, source, _ = await provider.get_daily_with_source("600519.SS")
        assert source == "tushare"

    def test_a_cell_refines_the_chain_without_narrowing_it(self):
        """A provider the cell has no verdict on (a ``--provider`` run, a token
        missing when it ran) keeps its configured slot behind the cell's own;
        one the cell measured and excluded stays out."""
        provider, *_ = self._chain(_table({"cn/equity/intraday/1m": ["fmp"]}))
        assert provider.source_names_for("600519.SS", "intraday", "1min") == ["fmp", "yfinance"]
        provider, *_ = self._chain(_table({"cn/equity/intraday/1m": ["fmp", "!yfinance"]}))
        assert provider.source_names_for("600519.SS", "intraday", "1min") == ["fmp"]

    def test_a_cell_never_routes_a_provider_outside_its_configured_markets(self):
        """tushare is configured for ``cn``; a US cell that measured it routable
        (its source answered a US canary through another feed) cannot put it
        in a US chain."""
        provider, *_ = self._chain(_table({"us/equity/daily": ["tushare", "fmp"]}))
        assert provider.source_names_for("AAPL", "daily") == ["fmp", "yfinance"]

    def test_unknown_names_are_ignored(self):
        provider, *_ = self._chain(_table({"cn/equity/intraday/1m": ["polygon", "fmp"]}))
        assert provider.source_names_for("600519.SS", "intraday", "1min") == ["fmp", "yfinance"]

    def test_cell_of_only_unknown_names_falls_back_to_config(self):
        """A ruleset refines the configured chain; it never empties it."""
        provider, *_ = self._chain(_table({"cn/equity/intraday/1m": ["polygon"]}))
        assert provider.source_names_for("600519.SS", "intraday", "1min") == ["yfinance", "fmp"]

    def test_index_request_uses_the_index_cell(self):
        provider, *_ = self._chain(_table({"hk/index/intraday/1m": ["fmp", "!yfinance"]}))
        assert provider.source_names_for("HSI", "intraday", "1min", is_index=True) == ["fmp"]
        # Without the index hint the bare symbol reads as US equity — a
        # different cell (and, here, no cell at all), so config routing stands.
        assert provider.source_names_for("HSI", "intraday", "1min") == ["fmp", "yfinance"]

    @pytest.mark.asyncio
    async def test_snapshots_follow_the_cell_per_symbol(self):
        us_rows = {"AAPL": {"symbol": "AAPL", "price": 190.0}}
        cn_rows = {"600519.SS": {"symbol": "600519.SS", "price": 1500.0}}
        primary = SnapshotSource("primary", {**us_rows, **cn_rows})
        secondary = SnapshotSource("secondary", {**us_rows, **cn_rows})
        provider = MarketDataProvider(
            [
                ProviderEntry("primary", primary, {"all"}),
                ProviderEntry("secondary", secondary, {"all"}),
            ],
            routing=_table({"cn/equity/snapshot": ["secondary"]}),
        )
        out = await provider.get_snapshots(["AAPL", "600519.SS"])
        assert [(r["symbol"], r["source"]) for r in out] == [
            ("AAPL", "primary"),
            ("600519.SS", "secondary"),
        ]
        assert primary.calls[0][1]["symbols"] == ["AAPL"]
        assert secondary.calls[0][1]["symbols"] == ["600519.SS"]

    @pytest.mark.asyncio
    async def test_snapshot_cell_falls_back_when_its_provider_misses(self):
        first = SnapshotSource("first", {})
        second = SnapshotSource("second", {"AAPL": {"symbol": "AAPL", "price": 190.0}})
        provider = MarketDataProvider(
            [
                ProviderEntry("first", first, {"all"}),
                ProviderEntry("second", second, {"all"}),
            ],
            routing=_table({"us/equity/snapshot": ["first", "second"]}),
        )
        out = await provider.get_snapshots(["AAPL"])
        assert _without_tier(out) == [{"symbol": "AAPL", "price": 190.0, "source": "second"}]

    @pytest.mark.asyncio
    async def test_snapshot_without_a_cell_keeps_config_batching(self):
        cn = SnapshotSource("tushare", {"600519.SS": {"symbol": "600519.SS", "price": 1500.0}})
        catch_all = SnapshotSource("fmp", {"AAPL": {"symbol": "AAPL", "price": 190.0}})
        provider = MarketDataProvider(
            [
                ProviderEntry("tushare", cn, {"cn"}),
                ProviderEntry("fmp", catch_all, {"all"}),
            ],
            routing=_table({"hk/index/snapshot": ["fmp"]}),
        )
        out = await provider.get_snapshots(["AAPL", "600519.SS"])
        assert [(r["symbol"], r["source"]) for r in out] == [
            ("AAPL", "fmp"),
            ("600519.SS", "tushare"),
        ]
