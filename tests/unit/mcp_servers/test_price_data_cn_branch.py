"""Tests for the price-data server's CN daily branch.

Locks the branch contract: CN daily requests are served from ginlix-data's
protocol route with source="tushare" (the upstream vendor); CN intraday skips
it; an empty/erroring result falls through the existing chain unchanged.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone as _tz
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.data_client.base import FetchResult

from .conftest import assert_ok_envelope

_MOD = "plugins.langalpha_market_data.price_data_mcp_server"

_CN_TZ = ZoneInfo("Asia/Shanghai")


def _ms(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, 9, 30, tzinfo=_CN_TZ).timestamp() * 1000)


# Legacy chart bars as GinlixDataCnSource.get_daily carries them (ascending).
_CN_BARS = [
    {"time": _ms(2025, 1, 2), "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 1000},
    {"time": _ms(2025, 1, 3), "open": 103.0, "high": 108.0, "low": 102.0, "close": 107.0, "volume": 1200},
]

_RAW_FMP_ROWS = [
    {"date": "2025-01-02 10:00:00", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000},
    {"date": "2025-01-02 10:05:00", "open": 103, "high": 108, "low": 102, "close": 107, "volume": 1200},
]


def _force_fmp_path(mod):
    return patch.object(mod._ginlix, "fetch_stock_data", new=AsyncMock(return_value=None))


@pytest.fixture(autouse=True)
def _stub_cn_display_name():
    # Keep the display-name lookup off the network; the name-attachment test
    # overrides this with its own value.
    import plugins.langalpha_market_data.price_data_mcp_server as mod

    with patch.object(mod._names, "display_names", new=AsyncMock(return_value=(None, None))):
        yield


class TestCnDailyBranch:
    @pytest.mark.asyncio
    async def test_cn_daily_served_from_ginlix_data(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=_CN_BARS)) as daily:
            result = await mod.get_stock_data("600519.SS", interval="1day")

        assert_ok_envelope(
            result, symbol="600519.SH", interval="1day", currency="CNY",
            timezone="Asia/Shanghai", count=2, source="tushare",
        )
        # ms-UTC bar stamps localized to exchange-local dates, ascending.
        assert [r["date"] for r in result["data"]] == ["2025-01-02", "2025-01-03"]
        assert result["data"][0]["close"] == 103.0
        daily.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_undated_cn_daily_gets_a_bounded_default_window(self):
        # Undated, the route answers with the instrument's whole listed history
        # (thousands of bars), where every other source is bounded.
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        before = datetime.now(_tz.utc).date()
        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=_CN_BARS)) as daily:
            await mod.get_stock_data("600519.SS", interval="1day")
        after = datetime.now(_tz.utc).date()

        start, end = daily.await_args.args[1], daily.await_args.args[2]
        assert end is None
        back = timedelta(days=mod._CN_DEFAULT_DAILY_DAYS)
        assert date.fromisoformat(start) in {before - back, after - back}

    @pytest.mark.asyncio
    async def test_a_given_start_date_is_never_overridden(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=_CN_BARS)) as daily:
            await mod.get_stock_data(
                "600519.SS", interval="1day",
                start_date="2001-09-07", end_date="2025-01-03",
            )

        assert daily.await_args.args[1:] == ("2001-09-07", "2025-01-03")

    @pytest.mark.asyncio
    async def test_cn_daily_envelope_carries_chinese_name(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=_CN_BARS)), \
                patch.object(mod._names, "display_names", new=AsyncMock(return_value=("甲公司", None))):
            result = await mod.get_stock_data("600519.SS", interval="1day")

        assert result["name"] == "甲公司"
        assert_ok_envelope(result, symbol="600519.SH", source="tushare")

    @pytest.mark.asyncio
    async def test_us_envelope_has_no_name_key(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        ginlix_rows = [
            {"date": "2025-01-02", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000},
        ]
        with patch.object(mod._ginlix, "fetch_stock_data", new=AsyncMock(return_value=ginlix_rows)):
            result = await mod.get_stock_data("AAPL", interval="1day")

        assert "name" not in result

    @pytest.mark.asyncio
    async def test_cn_intraday_skips_the_cn_daily_route(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        client = AsyncMock()
        client.get_intraday_chart = AsyncMock(return_value=_RAW_FMP_ROWS)
        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=_CN_BARS)) as daily, \
                _force_fmp_path(mod), \
                patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data(
                "600519.SS", interval="5min",
                start_date="2025-01-02", end_date="2025-01-03",
            )

        assert_ok_envelope(result, source="fmp", interval="5min")
        daily.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_cn_daily_falls_through_chain(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        client = AsyncMock()
        client.get_stock_price = AsyncMock(return_value=list(reversed(_RAW_FMP_ROWS)))
        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=[])), \
                _force_fmp_path(mod), \
                patch(f"{_MOD}.get_fmp_client", return_value=client):
            result = await mod.get_stock_data("600519.SS", interval="1day")

        assert_ok_envelope(result, source="fmp")
        client.get_stock_price.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cn_daily_error_falls_through_chain(self, caplog):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        client = AsyncMock()
        client.get_stock_price = AsyncMock(return_value=list(reversed(_RAW_FMP_ROWS)))
        with patch.object(mod._cn, "get_daily", side_effect=RuntimeError("boom")), \
                _force_fmp_path(mod), \
                patch(f"{_MOD}.get_fmp_client", return_value=client), \
                caplog.at_level("WARNING", logger=_MOD):
            result = await mod.get_stock_data("600519.SS", interval="1day")

        assert_ok_envelope(result, source="fmp")
        # A soft miss still leaves a trace, as the fundamentals CN branch does.
        assert "ginlix-data CN daily failed for 600519.SH" in caplog.text

    @pytest.mark.asyncio
    async def test_cn_daily_upstream_miss_logs_one_line(self, caplog):
        # A 503 is an expected upstream miss: one WARNING naming the symbol and
        # the status, no traceback, and the chain still answers.
        import httpx

        import plugins.langalpha_market_data.price_data_mcp_server as mod

        request = httpx.Request("GET", "http://ginlix-data.test/bars")
        miss = httpx.HTTPStatusError(
            "unavailable", request=request, response=httpx.Response(503, request=request)
        )
        client = AsyncMock()
        client.get_stock_price = AsyncMock(return_value=list(reversed(_RAW_FMP_ROWS)))
        with patch.object(mod._cn, "get_daily", side_effect=miss), \
                _force_fmp_path(mod), \
                patch(f"{_MOD}.get_fmp_client", return_value=client), \
                caplog.at_level("WARNING", logger=_MOD):
            result = await mod.get_stock_data("600519.SS", interval="1day")

        assert_ok_envelope(result, source="fmp")
        [record] = [r for r in caplog.records if r.name == _MOD]
        assert record.getMessage() == (
            "ginlix-data CN daily unavailable for 600519.SH: HTTPStatusError 503"
        )
        assert record.exc_info is None

    @pytest.mark.asyncio
    async def test_us_symbol_skips_the_cn_daily_route(self):
        import plugins.langalpha_market_data.price_data_mcp_server as mod

        ginlix_rows = [
            {"date": "2025-01-03", "open": 103, "high": 108, "low": 102, "close": 107, "volume": 1200},
            {"date": "2025-01-02", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000},
        ]
        with patch.object(mod._cn, "get_daily", return_value=FetchResult(bars=_CN_BARS)) as daily, \
                patch.object(mod._ginlix, "fetch_stock_data", new=AsyncMock(return_value=ginlix_rows)):
            result = await mod.get_stock_data("AAPL", interval="1day")

        assert_ok_envelope(result, symbol="AAPL", source="ginlix-data", count=2)
        daily.assert_not_awaited()
