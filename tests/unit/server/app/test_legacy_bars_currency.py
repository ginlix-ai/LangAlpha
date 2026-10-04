"""Legacy /intraday and /daily responses carry currency, timezone and treatment.

The protocol resolves these per symbol; without them on the wire the client
assumes USD/ET for every listing.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_BAR = {"time": 1705322400000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": None}


def _result(symbol, *, interval="5min", header=None):
    return SimpleNamespace(
        symbol=symbol,
        interval=interval,
        data=[_BAR],
        error=None,
        cached=False,
        cache_key="k",
        ttl_remaining=None,
        background_refresh_triggered=False,
        watermark=None,
        complete=None,
        market_phase=None,
        truncated=False,
        header=header,
    )


@pytest_asyncio.fixture
async def client():
    from src.server.app.market_data import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_intraday_carries_currency_timezone_and_treatment(client):
    svc = MagicMock()
    svc.get_stock_intraday = AsyncMock(
        return_value=_result("600519.SS", header={"price_treatment": "raw", "revision": 2})
    )
    with patch(
        "src.server.app.market_data.IntradayCacheService.get_instance", return_value=svc
    ):
        resp = await client.get(
            "/api/v1/market-data/intraday/stocks/600519.SS?interval=5min"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["currency"] == "CNY"
    assert body["timezone"] == "Asia/Shanghai"
    assert body["price_treatment"] == "raw"
    # A live chart reloads rather than splices when a poll shows a new revision.
    assert body["cache"]["revision"] == 2
    # A null upstream volume must not 500 the legacy wire.
    assert body["data"][0]["volume"] == 0


async def test_daily_carries_currency_and_timezone(client):
    svc = MagicMock()
    svc.get_stock_daily = AsyncMock(return_value=_result("AAPL", header=None))
    with patch(
        "src.server.app.market_data.DailyCacheService.get_instance", return_value=svc
    ):
        resp = await client.get("/api/v1/market-data/daily/stocks/AAPL")

    assert resp.status_code == 200
    body = resp.json()
    assert body["currency"] == "USD"
    assert body["timezone"] == "America/New_York"
    # No envelope header names a publisher, so the conservative default stands.
    assert body["price_treatment"] == "split_adjusted"
    assert body["cache"]["revision"] is None


async def test_stock_bars_echo_the_requested_spelling(client):
    """The cache result carries the vendor's spelling; the response answers in ours."""
    svc = MagicMock()
    svc.get_stock_intraday = AsyncMock(return_value=_result("600519.SS"))
    svc.get_stock_daily = AsyncMock(return_value=_result("600519.SS", interval="1day"))
    with (
        patch("src.server.app.market_data.IntradayCacheService.get_instance", return_value=svc),
        patch("src.server.app.market_data.DailyCacheService.get_instance", return_value=svc),
    ):
        intraday = await client.get("/api/v1/market-data/intraday/stocks/600519.ss?interval=5min")
        daily = await client.get("/api/v1/market-data/daily/stocks/600519.ss")

    assert intraday.json()["symbol"] == "600519.SH"
    assert daily.json()["symbol"] == "600519.SH"
    # The cache and provider layer still see the collapsed spelling.
    assert svc.get_stock_intraday.await_args.kwargs["symbol"] == "600519.SH"
    assert svc.get_stock_daily.await_args.kwargs["symbol"] == "600519.SH"
