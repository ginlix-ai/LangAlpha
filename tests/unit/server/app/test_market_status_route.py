"""/market-status market scoping: validated param, per-market cache key, passthrough."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_RAW = {
    "market": "open",
    "afterHours": False,
    "earlyHours": False,
    "serverTime": "2026-09-08T10:00:00-04:00",
    "exchanges": {"nasdaq": "open"},
}


@pytest_asyncio.fixture
async def client():
    from src.server.app.market_data import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _stubs():
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    provider = AsyncMock()
    provider.get_market_status = AsyncMock(return_value=_RAW)
    provider.source_names_for_market = MagicMock(return_value=["fmp"])
    return cache, provider


def _patches(cache, provider):
    return (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
    )


async def test_defaults_to_us_and_scopes_the_cache_key(client):
    cache, provider = _stubs()
    p1, p2 = _patches(cache, provider)
    with p1, p2:
        resp = await client.get("/api/v1/market-data/market-status")

    assert resp.status_code == 200
    provider.get_market_status.assert_awaited_once()
    assert provider.get_market_status.await_args.kwargs["market"] == "us"
    assert cache.get.await_args.args[0] == "market:status:us"
    assert cache.set.await_args.args[0] == "market:status:us"


async def test_market_param_reaches_the_provider_on_its_own_key(client):
    cache, provider = _stubs()
    p1, p2 = _patches(cache, provider)
    with p1, p2:
        resp = await client.get("/api/v1/market-data/market-status?market=cn")

    assert resp.status_code == 200
    assert provider.get_market_status.await_args.kwargs["market"] == "cn"
    assert cache.get.await_args.args[0] == "market:status:cn"
    provider.source_names_for_market.assert_called_once_with("cn")


async def test_unsupported_market_is_400(client):
    cache, provider = _stubs()
    p1, p2 = _patches(cache, provider)
    with p1, p2:
        resp = await client.get("/api/v1/market-data/market-status?market=jp")

    assert resp.status_code == 400
    provider.get_market_status.assert_not_called()


async def test_alias_route_carries_the_market_through(client):
    cache, provider = _stubs()
    p1, p2 = _patches(cache, provider)
    with p1, p2:
        resp = await client.get("/api/v1/market-data/status?market=hk")

    assert resp.status_code == 200
    assert provider.get_market_status.await_args.kwargs["market"] == "hk"
