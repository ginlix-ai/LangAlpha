"""Unit tests for RoutedFinancialSource / RoutedMarketIntelSource dispatch.

Locks the routing contract: cn symbols → the cn source, everything else → default,
routed errors/empties fall back, and get_realtime_quote stays pinned to the
default while CN realtime is deferred.
"""

from __future__ import annotations

import httpx
import pytest

from src.data_client.financial_data_provider import (
    MarketRoute,
    RoutedFinancialSource,
    RoutedMarketIntelSource,
)
from src.data_client.ginlix_data.cn_financial import has_cjk, is_cn_option, screens_cn


class _StubSource:
    """Records calls; every method returns a canned value (or raises)."""

    def __init__(self, name: str, result=None, error: Exception | None = None):
        self.name = name
        self.result = result
        self.error = error
        self.calls: list[tuple[str, tuple, dict]] = []
        self.closed = False

    def __getattr__(self, method):
        if method.startswith("_"):
            raise AttributeError(method)

        async def _call(*args, **kwargs):
            self.calls.append((method, args, kwargs))
            if self.error is not None:
                raise self.error
            return self.result

        return _call

    async def close(self):
        self.closed = True


def _tag(name):
    return [{"source": name}]


def _cn(source):
    """The cn route as the registry wires it."""
    return {"cn": MarketRoute(
        source, screens=screens_cn, claims_query=has_cjk, claims_option=is_cn_option,
    )}


@pytest.mark.asyncio
async def test_cn_symbol_routes_to_cn_source():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.get_company_profile("600519.SS") == _tag("cn")
    assert await routed.get_company_profile("000001.SZ") == _tag("cn")
    assert cn.calls[0] == ("get_company_profile", ("600519.SS",), {})
    assert default.calls == []


@pytest.mark.asyncio
async def test_us_symbol_stays_on_default_with_args():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    result = await routed.get_income_statements("AAPL", period="annual", limit=4)

    assert result == _tag("default")
    assert default.calls == [
        ("get_income_statements", ("AAPL",), {"period": "annual", "limit": 4})
    ]
    assert cn.calls == []


@pytest.mark.asyncio
async def test_routed_error_falls_back_to_default():
    cn = _StubSource("cn", error=RuntimeError("boom"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.get_key_metrics("600519.SS") == _tag("default")


@pytest.mark.asyncio
async def test_routed_empty_falls_back_to_default():
    cn = _StubSource("cn", result=[])
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.get_financial_ratios("600519.SS") == _tag("default")
    assert cn.calls and default.calls  # both consulted, in order


@pytest.mark.asyncio
async def test_routed_dict_of_empties_falls_back_to_default():
    cn = _StubSource("cn", result={"results": []})
    default = _StubSource("default", result={"results": [{"ticker": "us"}]})
    routed = RoutedMarketIntelSource(default=default, by_market=_cn(cn))

    assert await routed.get_options_chain("510050.SS") == {"results": [{"ticker": "us"}]}
    assert cn.calls and default.calls


@pytest.mark.asyncio
async def test_search_names_the_default_rows_only():
    named: list[list[dict]] = []

    async def name_rows(rows):
        named.append(rows)

    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn), name_rows=name_rows)

    await routed.search_stocks("apple")
    await routed.search_stocks("贵州茅台")
    assert named == [_tag("default")]


@pytest.mark.asyncio
async def test_no_default_degrades_to_empty():
    cn = _StubSource("cn", result=[])
    routed = RoutedFinancialSource(default=None, by_market=_cn(cn))

    assert await routed.get_analyst_ratings("600519.SS") == []
    assert await routed.get_analyst_ratings("AAPL") == []
    assert await routed.get_sector_performance() == []


@pytest.mark.asyncio
async def test_realtime_quote_pinned_to_default():
    # CN realtime is deferred: even a cn symbol must not hit the cn source.
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.get_realtime_quote("600519.SS") == _tag("default")
    assert cn.calls == []


@pytest.mark.asyncio
async def test_search_cjk_prefers_cn_directory():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.search_stocks("贵州茅台") == _tag("cn")
    assert default.calls == []


@pytest.mark.asyncio
async def test_search_non_cjk_default_first_then_cn_fallback():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=[])
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    # Default is empty (e.g. pinyin abbreviation) → directory fallback.
    assert await routed.search_stocks("GZMT") == _tag("cn")
    assert default.calls[0][0] == "search_stocks"


@pytest.mark.asyncio
async def test_search_non_cjk_default_hit_wins():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.search_stocks("apple") == _tag("default")
    assert cn.calls == []


@pytest.mark.asyncio
async def test_search_cjk_cn_error_falls_back_to_default():
    cn = _StubSource("cn", error=RuntimeError("boom"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.search_stocks("贵州茅台") == _tag("default")


@pytest.mark.asyncio
async def test_screen_routes_on_cn_signals_only():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.screen_stocks(market="cn", limit=5) == _tag("cn")
    assert await routed.screen_stocks(country="CN") == _tag("cn")
    assert await routed.screen_stocks(exchange="SSE") == _tag("cn")
    assert await routed.screen_stocks(exchange="NASDAQ") == _tag("default")
    assert await routed.screen_stocks(marketCapMoreThan=1e9) == _tag("default")


@pytest.mark.asyncio
async def test_intel_dispatch_and_pins():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedMarketIntelSource(default=default, by_market=_cn(cn))

    assert await routed.get_short_interest("600519.SS") == _tag("cn")
    assert await routed.get_float_shares("600519.SS") == _tag("cn")
    assert await routed.get_short_volume("AAPL") == _tag("default")
    # get_movers pinned to default even though a cn source exists.
    assert await routed.get_movers("gainers") == _tag("default")
    assert all(m != "get_movers" for m, _, _ in cn.calls)


@pytest.mark.asyncio
async def test_intel_options_ohlcv_routes_on_ticker_suffix():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedMarketIntelSource(default=default, by_market=_cn(cn))

    assert await routed.get_options_ohlcv("10001234.SH", interval="1day") == _tag("cn")
    assert await routed.get_options_ohlcv("O:SPY251219C00650000") == _tag("default")


@pytest.mark.asyncio
async def test_intel_options_chain_dispatches_on_underlying():
    cn = _StubSource("cn", result={"results": [{"ticker": "10001234.SH"}]})
    default = _StubSource("default", result={"results": [{"ticker": "us"}]})
    routed = RoutedMarketIntelSource(default=default, by_market=_cn(cn))

    chain = await routed.get_options_chain("510050.SS")
    assert chain["results"][0]["ticker"] == "10001234.SH"

    chain = await routed.get_options_chain("SPY")
    assert chain["results"][0]["ticker"] == "us"


@pytest.mark.asyncio
async def test_close_closes_default_and_routed():
    cn = _StubSource("cn")
    default = _StubSource("default")
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    await routed.close()

    assert cn.closed and default.closed


@pytest.mark.asyncio
async def test_close_reaches_every_source_when_one_fails(caplog):
    from src.data_client.financial_data_provider import FinancialDataProvider

    class _Failing(_StubSource):
        async def close(self):
            self.closed = True
            raise RuntimeError("close failed")

    default, cn, intel = _Failing("default"), _StubSource("cn"), _StubSource("intel")
    provider = FinancialDataProvider(
        financial=RoutedFinancialSource(default=default, by_market=_cn(cn)), intel=intel,
    )

    with caplog.at_level("WARNING"), pytest.raises(RuntimeError, match="close failed"):
        await provider.close()

    assert default.closed and cn.closed and intel.closed
    assert "source=default failed" in caplog.text


def test_routed_method_names_exist_on_the_cn_sources():
    """Drift guard: dispatch is by method name, so a rename on the cn sources
    would otherwise fail only at runtime. Every symbol-routed name, plus the
    explicitly routed ones, must be a real method on the concrete class.
    """
    from src.data_client.financial_data_provider import _BySymbol, _DefaultOnly
    from src.data_client.ginlix_data.cn_financial import (
        GinlixDataCnFinancialSource,
        GinlixDataCnIntelSource,
    )

    def routed(cls, *explicit):
        by_symbol = {
            name for name, attr in vars(cls).items()
            if isinstance(attr, _BySymbol) and not isinstance(attr, _DefaultOnly)
        }
        return by_symbol | set(explicit)

    fin = routed(RoutedFinancialSource, "screen_stocks", "search_stocks")
    intel = routed(RoutedMarketIntelSource, "get_options_ohlcv")
    assert "get_key_metrics" in fin and "get_float_shares" in intel
    for name in fin:
        assert callable(getattr(GinlixDataCnFinancialSource, name, None)), name
    for name in intel:
        assert callable(getattr(GinlixDataCnIntelSource, name, None)), name


@pytest.mark.asyncio
async def test_screen_targets_cn_covers_beijing_and_mixed_case_market():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    for filters in ({"exchange": "BEIJING"}, {"exchange": "BJ"}, {"exchange": "BJSE"},
                    {"exchange": "XSHG"}, {"exchange": "BSE", "country": "CN"},
                    {"market": "CN"}, {"market": " Cn "}):
        assert await routed.screen_stocks(**filters) == _tag("cn"), filters
    # FMP's screener spells Bombay "BSE"; alone it is not a CN screen.
    assert await routed.screen_stocks(exchange="BSE") == _tag("default")


def test_screen_targets_cn_for_every_country_spelling_the_cn_screen_accepts():
    # Routing and the CN screen's own validation must agree, or a filter the
    # screen would serve is sent to the default source instead.
    for value in ("CN", "CHN", "China", " china "):
        assert screens_cn({"country": value}), value
        assert screens_cn({"market": value}), value
    assert not screens_cn({"country": "US"})


@pytest.mark.asyncio
async def test_options_ohlcv_falls_back_when_the_cn_source_fails():
    cn = _StubSource("cn", error=RuntimeError("boom"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedMarketIntelSource(default=default, by_market=_cn(cn))

    assert await routed.get_options_ohlcv("10001234.SH", interval="1day") == _tag("default")


@pytest.mark.asyncio
async def test_options_ohlcv_falls_back_when_the_cn_source_is_empty():
    cn = _StubSource("cn", result=[])
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedMarketIntelSource(default=default, by_market=_cn(cn))

    # An unservable interval comes back empty from the CN source; the default
    # source still gets its shot rather than the caller getting nothing.
    assert await routed.get_options_ohlcv("10001234.SH", interval="1hour") == _tag("default")


@pytest.mark.asyncio
async def test_cn_segments_pass_the_period_and_drop_rollup_rows():
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    calls: list[dict] = []

    class _Client:
        async def get_fundamentals_v2(self, key, kind, **params):
            calls.append({"kind": kind, **params})
            return [{"2025-12-31": {
                "产品": 100.0, "Liquor": 60.0, "Series B": 30.0, "其他业务": 10.0,
                "合计特别调整": 1.5, "分部间抵消": -2.0, "小计": 90.0,
            }}]

    source = GinlixDataCnFinancialSource(_Client())
    rows = await source.get_revenue_by_segment("600000.SH", segment_type="product", period="annual")
    assert calls == [{"kind": "segments_product", "period": "annual"}]
    assert rows == [{"2025-12-31": {"Liquor": 60.0, "Series B": 30.0, "其他业务": 10.0}}]


@pytest.mark.asyncio
async def test_cn_segments_drop_a_period_left_with_no_segment():
    # ``{date: {}}`` would read as an answer and keep the default from being asked.
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def get_fundamentals_v2(self, key, kind, **params):
            return [{"2025-12-31": {"产品": 100.0, "合计": 100.0}}]

    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(
        default=default, by_market=_cn(GinlixDataCnFinancialSource(_Client()))
    )

    assert await routed.get_revenue_by_segment("600000.SH") == _tag("default")


@pytest.mark.asyncio
async def test_search_directory_failure_with_nothing_found_raises():
    # The caller caches an empty search as "no match"; a blip must not be one.
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def search_v2(self, query, *, market, limit):
            if market == "cn":
                raise httpx.ConnectError("down")
            return []

    default = _StubSource("default", result=[])
    routed = RoutedFinancialSource(
        default=default, by_market=_cn(GinlixDataCnFinancialSource(_Client()))
    )

    with pytest.raises(httpx.ConnectError):
        await routed.search_stocks("贵州茅台")


@pytest.mark.asyncio
async def test_search_fallback_directory_failure_after_a_default_no_match_is_partial():
    # The default answered "no match" for an ASCII query; only the directory
    # asked as a fallback failed, so the answer stands but must not be cached.
    from src.data_client.financial_data_provider import PartialSearch
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def search_v2(self, query, *, market, limit):
            raise httpx.ConnectError("down")

    default = _StubSource("default", result=[])
    routed = RoutedFinancialSource(
        default=default, by_market=_cn(GinlixDataCnFinancialSource(_Client()))
    )

    with pytest.raises(PartialSearch) as partial:
        await routed.search_stocks("XYZQW")

    assert partial.value.rows == []


@pytest.mark.asyncio
async def test_search_default_failure_still_asks_the_directories():
    cn = _StubSource("cn", result=_tag("cn"))
    default = _StubSource("default", error=RuntimeError("down"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    assert await routed.search_stocks("GZMT") == _tag("cn")


@pytest.mark.asyncio
async def test_routed_fallback_logs_neither_the_arguments_nor_a_traceback(caplog):
    cn = _StubSource("cn", error=RuntimeError("no match for 贵州茅台"))
    default = _StubSource("default", result=_tag("default"))
    routed = RoutedFinancialSource(default=default, by_market=_cn(cn))

    with caplog.at_level("WARNING"):
        await routed.search_stocks("贵州茅台")
        await routed.get_key_metrics("600519.SH")

    assert len(caplog.records) == 2
    for record in caplog.records:
        assert "贵州茅台" not in record.getMessage() and "600519" not in record.getMessage()
        assert record.exc_info is None


@pytest.mark.asyncio
async def test_cn_search_row_without_names_is_named_by_its_symbol():
    # Both names are optional on the wire; a nameless row would fail the
    # search response model and take the whole merged search down.
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def search_v2(self, query, *, market, limit):
            if market != "cn":
                return []
            return [{"instrument_key": "600519.XSHG", "mic": "XSHG",
                     "name": None, "name_local": None}]

    (row,) = await GinlixDataCnFinancialSource(_Client()).search_stocks("600519")

    assert row["name"] == "600519.SH"


@pytest.mark.asyncio
async def test_cn_search_with_one_directory_down_is_partial():
    # The endpoint caches a search for minutes; one directory's names cached as
    # the whole answer would hide the other's that long.
    from src.data_client.financial_data_provider import PartialSearch
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def search_v2(self, query, *, market, limit):
            if market == "hk":
                raise httpx.ConnectError("down")
            return [{"instrument_key": "600519.XSHG", "mic": "XSHG", "name_local": "贵州茅台"}]

    with pytest.raises(PartialSearch) as partial:
        await GinlixDataCnFinancialSource(_Client()).search_stocks("茅台")

    assert [r["symbol"] for r in partial.value.rows] == ["600519.SH"]


@pytest.mark.asyncio
async def test_a_fallback_directory_answering_partially_reaches_the_caller_as_partial():
    # A pinyin abbreviation the default cannot place: the CN names found while
    # the HK directory is down are the answer, still marked partial.
    from src.data_client.financial_data_provider import PartialSearch
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def search_v2(self, query, *, market, limit):
            if market == "hk":
                raise httpx.ConnectError("down")
            return [{"instrument_key": "600519.XSHG", "mic": "XSHG", "name_local": "贵州茅台"}]

    routed = RoutedFinancialSource(
        default=_StubSource("default", result=[]),
        by_market=_cn(GinlixDataCnFinancialSource(_Client())),
    )

    with pytest.raises(PartialSearch) as partial:
        await routed.search_stocks("GZMT")

    assert [r["symbol"] for r in partial.value.rows] == ["600519.SH"]


@pytest.mark.asyncio
async def test_cn_executives_carry_every_key_the_tool_promises():
    # CN filings name no gender, birth year or tenure; agent code indexes them anyway.
    from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource

    class _Client:
        async def get_fundamentals_v2(self, key, kind, **params):
            return [{"name": "张三", "title": "董事长", "pay": 1200000, "currencyPay": "CNY"}]

    rows = await GinlixDataCnFinancialSource(_Client()).get_key_executives("600519.SH")
    assert rows[0]["gender"] is None and rows[0]["yearBorn"] is None
    assert rows[0]["titleSince"] is None
    assert (rows[0]["name"], rows[0]["currencyPay"]) == ("张三", "CNY")
