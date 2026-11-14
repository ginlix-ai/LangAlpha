"""/logos/{symbol}: a public relay of the data service's logo, caching headers intact."""

from functools import partial
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client():
    from src.server.app.logos import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _relay(upstream):
    """The data service on, its logo fetch answering ``upstream`` (a response or an error)."""
    kind = "side_effect" if isinstance(upstream, Exception) else "return_value"
    fetch = AsyncMock(**{kind: upstream})
    return fetch, (
        patch("src.server.app.logos.GINLIX_DATA_ENABLED", True),
        patch("src.server.app.logos.fetch_logo", fetch),
    )


async def test_without_a_data_service_answers_a_long_lived_404(client):
    with patch("src.server.app.logos.GINLIX_DATA_ENABLED", False):
        resp = await client.get("/api/v1/market-data/logos/AAPL")
    assert resp.status_code == 404
    assert resp.headers["cache-control"] == "public, max-age=86400"


async def test_relays_the_image_its_caching_headers_and_only_size_and_format(client):
    fetch, patches = _relay(
        httpx.Response(
            200,
            content=b"RIFF-webp",
            headers={"content-type": "image/webp", "cache-control": "public, max-age=86400", "etag": '"abc"', "x-other": "1"},
        )
    )
    with patches[0], patches[1]:
        resp = await client.get(
            "/api/v1/market-data/logos/0700.HK?size=64&format=png&junk=1",
            headers={"If-None-Match": '"abc"'},
        )
    assert resp.status_code == 200
    assert resp.content == b"RIFF-webp"
    assert resp.headers["content-type"] == "image/webp"
    assert resp.headers["etag"] == '"abc"'
    assert "x-other" not in resp.headers
    fetch.assert_awaited_once_with("0700.HK", size=64, format="png", if_none_match='"abc"')


async def test_an_upstream_failure_is_never_cached(client):
    fetch, patches = _relay(httpx.Response(502, headers={"cache-control": "public, max-age=600"}))
    with patches[0], patches[1]:
        resp = await client.get("/api/v1/market-data/logos/AAPL")
    assert resp.status_code == 502
    assert resp.headers["cache-control"] == "no-store"


async def test_an_unreachable_data_service_is_an_uncached_503(client):
    fetch, patches = _relay(httpx.ConnectError("down"))
    with patches[0], patches[1]:
        resp = await client.get("/api/v1/market-data/logos/AAPL")
    assert resp.status_code == 503
    assert resp.headers["cache-control"] == "no-store"


async def test_the_logo_fetch_sends_no_service_credential():
    """The shared data client carries the service token; the public relay must not."""
    seen: list[httpx.Request] = []
    transport = httpx.MockTransport(lambda r: seen.append(r) or httpx.Response(404))
    with (
        patch.dict("os.environ", {"INTERNAL_SERVICE_TOKEN": "secret"}),
        patch("src.config.settings.GINLIX_DATA_URL", "http://data.test/"),
        patch("src.data_client.ginlix_data.logos._http", None),
        patch("src.data_client.ginlix_data.logos.httpx.AsyncClient", partial(httpx.AsyncClient, transport=transport)),
    ):
        from src.data_client.ginlix_data.logos import fetch_logo

        await fetch_logo("BRK.B", size=128, format=None, if_none_match=None)
    (req,) = seen
    assert str(req.url) == "http://data.test/api/v1/data/logos/BRK.B?size=128"
    assert "x-service-token" not in req.headers and "x-user-id" not in req.headers
