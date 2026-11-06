"""A source that failed stays an error unless another answered, and the fallback is recorded."""

from __future__ import annotations

import httpx
import pytest

from src.data_client.base import IntervalNotSupported
from src.data_client.fallbacks import Fallback, SourceFailed, answered, record, recording
from src.data_client.financial_data_provider import (
    MarketRoute,
    RoutedFinancialSource,
    RoutedMarketIntelSource,
)
from src.data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource, GinlixDataCnIntelSource
from src.data_client.market_data_provider import MarketDataProvider, ProviderEntry

_BAR = {"time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}


class _Source:
    """Answers every read with *rows*, or raises *error*."""

    def __init__(self, rows=None, error: Exception | None = None):
        self.rows, self.error = rows, error

    async def _answer(self, **_):
        if self.error is not None:
            raise self.error
        return self.rows

    get_daily = get_intraday = get_snapshots = _answer

    async def get_company_profile(self, symbol):
        return await self._answer()


def _chain(*sources):
    return MarketDataProvider([
        ProviderEntry(name, source, {"all"}) for name, source in sources
    ])


@pytest.mark.asyncio
async def test_an_outage_at_the_covering_source_is_not_read_as_no_data():
    chain = _chain(("tushare", _Source(error=ConnectionError("down"))), ("fmp", _Source(rows=[])))
    with recording() as taken, pytest.raises(ConnectionError):
        await chain.get_daily_with_source("920799.BJ")
    assert taken == [Fallback("920799.BJ", "daily bars", "tushare", "ConnectionError", None)]


@pytest.mark.asyncio
async def test_a_fallback_that_answers_is_recorded_by_name():
    chain = _chain(("tushare", _Source(error=ConnectionError("down"))), ("yfinance", _Source(rows=[_BAR])))
    with recording() as taken:
        bars, source, _ = await chain.get_daily_with_source("000300.SH")
    assert (bars, source) == ([_BAR], "yfinance")
    assert taken == [Fallback("000300.SH", "daily bars", "tushare", "ConnectionError", "yfinance")]


@pytest.mark.asyncio
async def test_the_covering_sources_empty_answer_outranks_a_later_failure():
    chain = _chain(("ginlix-data", _Source(rows=[])), ("fmp", _Source(error=ConnectionError("down"))))
    with recording() as taken:
        assert await chain.get_daily_with_source("AAPL") == ([], "ginlix-data", False)
    # The failed source might have had the bars, so the empty answer names it.
    assert taken == [Fallback("AAPL", "daily bars", "fmp", "ConnectionError", None)]


def _status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://data.internal/api/v1/data/aggregates/stock/600519.SH?from=x")
    return httpx.HTTPStatusError(
        "boom", request=request, response=httpx.Response(status, request=request)
    )


@pytest.mark.asyncio
async def test_a_raised_chain_failure_names_the_source_not_the_request_url():
    chain = _chain(("ginlix-data", _Source(error=_status_error(503))), ("fmp", _Source(rows=[])))
    with pytest.raises(SourceFailed) as failed:
        await chain.get_daily_with_source("600519.SH")
    assert str(failed.value) == "ginlix-data request failed (HTTPStatusError 503)"
    assert failed.value.status_code == 503
    assert isinstance(failed.value.__cause__, httpx.HTTPStatusError)


@pytest.mark.asyncio
async def test_a_raised_snapshot_failure_names_the_source_not_the_request_url():
    chain = _chain(("ginlix-data", _Source(error=_status_error(502))))
    with pytest.raises(SourceFailed, match=r"^ginlix-data request failed \(HTTPStatusError 502\)$"):
        await chain.get_snapshots(["600519.SH"])


@pytest.mark.asyncio
async def test_a_source_refusing_the_bar_width_is_skipped_not_read_as_an_outage():
    chain = _chain(("yfinance", _Source(error=IntervalNotSupported("4hour"))), ("fmp", _Source(rows=[])))
    with recording() as taken:
        assert await chain.get_intraday_with_source("0700.HK", "4hour") == ([], "fmp", False)
    assert taken == []


@pytest.mark.asyncio
async def test_a_width_every_source_refuses_stays_refused():
    chain = _chain(("yfinance", _Source(error=IntervalNotSupported("4hour"))))
    with pytest.raises(IntervalNotSupported):
        await chain.get_intraday_with_source("0700.HK", "4hour")


def test_a_later_answer_credits_an_unanswered_read():
    with recording() as taken:
        record(Fallback("920799.BJ", "quote", "tushare", "ConnectError", None))
        answered("920799.BJ", "quote", "daily close")
    assert taken == [Fallback("920799.BJ", "quote", "tushare", "ConnectError", "daily close")]


@pytest.mark.asyncio
async def test_a_batch_quote_records_the_listing_whose_sources_failed():
    class _Tushare(_Source):
        async def get_snapshots(self, symbols, **_):
            raise ConnectionError("down")

    fmp = _Source(rows=[{"symbol": "600519.SH", "price": 1.0}])
    chain = _chain(("tushare", _Tushare()), ("fmp", fmp))
    with recording() as taken:
        rows = await chain.get_snapshots(["920799.BJ", "600519.SH"])
    assert [r["symbol"] for r in rows] == ["600519.SH"]
    assert set(taken) == {
        Fallback("920799.BJ", "quote", "tushare", "ConnectionError", None),
        Fallback("600519.SH", "quote", "tushare", "ConnectionError", "fmp"),
    }


@pytest.mark.asyncio
async def test_a_cn_option_chain_outage_is_not_read_as_no_contracts():
    class _Client:
        async def get_options_chain_v2(self, key, **_):
            raise httpx.ConnectError("down")

    class _UsIntel:
        async def get_options_chain(self, underlying, **_):
            return {"results": []}

    routed = RoutedMarketIntelSource(
        default=_UsIntel(),
        by_market={"cn": MarketRoute(GinlixDataCnIntelSource(_Client()), name="tushare")},
        default_name="ginlix-data",
    )
    with recording() as taken, pytest.raises(SourceFailed, match=r"^tushare request failed \(ConnectError\)$"):
        await routed.get_options_chain("510300.SH")
    assert taken == [Fallback("510300.SH", "options chain", "tushare", "ConnectError", None)]


@pytest.mark.asyncio
async def test_a_cn_option_bars_outage_is_not_read_as_no_bars():
    class _Client:
        async def get_options_bars_v2(self, ticker, **_):
            raise httpx.ConnectError("down")

    class _UsIntel:
        async def get_options_ohlcv(self, ticker, **_):
            return []

    routed = RoutedMarketIntelSource(
        default=_UsIntel(),
        by_market={"cn": MarketRoute(
            GinlixDataCnIntelSource(_Client()), claims_option=lambda t: True, name="tushare",
        )},
        default_name="ginlix-data",
    )
    with recording() as taken, pytest.raises(SourceFailed):
        await routed.get_options_ohlcv("10001234.SH", interval="1day")
    assert taken == [Fallback("10001234.SH", "options ohlcv", "tushare", "ConnectError", None)]


@pytest.mark.asyncio
async def test_a_cn_screen_the_default_answers_after_an_outage_is_recorded():
    class _Client:
        async def screen_v2(self, **_):
            raise httpx.ConnectError("down")

    class _Fmp:
        async def screen_stocks(self, **_):
            return [{"symbol": "600519.SH"}]

    routed = RoutedFinancialSource(
        default=_Fmp(),
        by_market={"cn": MarketRoute(
            GinlixDataCnFinancialSource(_Client()), screens=lambda f: True, name="tushare",
        )},
        default_name="fmp",
    )
    with recording() as taken:
        assert await routed.screen_stocks(country="CN") == [{"symbol": "600519.SH"}]
    assert taken == [Fallback("screen", "screen stocks", "tushare", "ConnectError", "fmp")]


def _routed(cn: _Source, default: _Source) -> RoutedFinancialSource:
    return RoutedFinancialSource(
        default=default, by_market={"cn": MarketRoute(cn, name="tushare")}, default_name="fmp",
    )


@pytest.mark.asyncio
async def test_a_failed_cn_source_stays_an_error_when_the_default_has_nothing():
    routed = _routed(_Source(error=ConnectionError("down")), _Source(rows=[]))
    with recording() as taken, pytest.raises(ConnectionError):
        await routed.get_company_profile("920799.BJ")
    assert taken == [Fallback("920799.BJ", "company profile", "tushare", "ConnectionError", None)]


@pytest.mark.asyncio
async def test_a_failed_cn_source_stays_an_error_when_the_default_fails_too():
    routed = _routed(_Source(error=ConnectionError("down")), _Source(error=ValueError("fmp")))
    with recording() as taken, pytest.raises(ConnectionError):
        await routed.get_company_profile("600519.SH")
    assert taken == [Fallback("600519.SH", "company profile", "tushare", "ConnectionError", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("cn", "error"), [
    (_Source(error=ConnectionError("down")), "ConnectionError"),
    (_Source(rows=[]), None),
])
async def test_the_default_answering_for_a_cn_listing_is_recorded(cn, error):
    routed = _routed(cn, _Source(rows=[{"symbol": "600519.SS"}]))
    with recording() as taken:
        assert await routed.get_company_profile("600519.SH") == [{"symbol": "600519.SS"}]
    assert taken == [Fallback("600519.SH", "company profile", "tushare", error, "fmp")]


@pytest.mark.asyncio
async def test_nothing_is_recorded_outside_a_recording_block():
    routed = _routed(_Source(error=ConnectionError("down")), _Source(rows=[{"symbol": "x"}]))
    assert await routed.get_company_profile("600519.SH") == [{"symbol": "x"}]
