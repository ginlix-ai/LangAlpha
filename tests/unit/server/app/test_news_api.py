"""Tests for the News API router (src/server/app/news.py).

Focus: the ``provider`` query param routes to a single named source
(``get_news_source``) and bypasses the fallback chain
(``get_news_data_provider``), with a provider-scoped cache key.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

ARTICLE = {
    "id": "abc123",
    "title": "Top story",
    "author": None,
    "description": "d",
    "published_at": "2025-12-01T00:00:00+00:00",
    "article_url": "https://example.com/a",
    "image_url": None,
    "source": {"name": "investing.com", "logo_url": None, "homepage_url": None, "favicon_url": None},
    "tickers": ["TSLA"],
    "keywords": [],
    "sentiments": None,
}


@pytest_asyncio.fixture
async def client():
    from src.server.app.news import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def test_provider_routes_to_named_source(client):
    """provider=tickertick → get_news_source, NOT the fallback chain."""
    source = AsyncMock()
    source.get_news.return_value = {"results": [ARTICLE], "count": 1, "next_cursor": None}
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock()) as get_chain,
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["results"][0]["id"] == "abc123"
    get_source.assert_awaited_once_with("tickertick")
    get_chain.assert_not_called()
    # Cache key is provider-scoped.
    assert cache.get.await_args.kwargs.get("provider") == "tickertick"
    assert cache.set.await_args.kwargs.get("provider") == "tickertick"


async def test_compact_inlines_article_body(client):
    """The compact list response carries description/keywords/sentiments/author so
    the detail modal renders without a by-id round-trip."""
    rich = {
        **ARTICLE,
        "author": "Jane Doe",
        "description": "A full summary paragraph.",
        "keywords": ["ev", "earnings"],
        "sentiments": [{"ticker": "TSLA", "sentiment": "positive", "reasoning": "beat"}],
    }
    source = AsyncMock()
    source.get_news.return_value = {"results": [rich], "count": 1, "next_cursor": None}
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    row = resp.json()["results"][0]
    assert row["author"] == "Jane Doe"
    assert row["description"] == "A full summary paragraph."
    assert row["keywords"] == ["ev", "earnings"]
    assert row["sentiments"][0]["ticker"] == "TSLA"
    assert row["has_sentiment"] is True


async def test_no_provider_uses_chain(client):
    """No provider → the fallback chain, provider passed as None to the cache."""
    chain = AsyncMock()
    chain.get_news.return_value = {"results": [ARTICLE], "count": 1, "next_cursor": None}
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock()) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)) as get_chain,
    ):
        resp = await client.get("/api/v1/news?limit=5")

    assert resp.status_code == 200
    get_chain.assert_awaited_once()
    get_source.assert_not_called()
    assert cache.get.await_args.kwargs.get("provider") is None


async def test_unknown_provider_returns_400(client):
    """An unknown ?provider value is a client error (400), not an unhandled 500.

    Exercises the real registry get_news_source, which raises ValueError for a
    name absent from _NEWS_SOURCE_REGISTRY.
    """
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()

    with patch("src.server.app.news._cache", cache):
        resp = await client.get("/api/v1/news?provider=not-a-real-provider&limit=5")

    assert resp.status_code == 400
    assert "not available" in resp.json()["detail"]


async def test_article_by_id_tickertick_fallback(client):
    """Cache miss → chain returns None → the TickerTick source resolves the row."""
    cache = AsyncMock()
    cache.get_article_by_id = AsyncMock(return_value=None)
    chain = AsyncMock()
    chain.get_news_article = AsyncMock(return_value=None)
    tickertick = AsyncMock()
    tickertick.get_news_article = AsyncMock(return_value=ARTICLE)

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
        patch("src.data_client.get_news_source", AsyncMock(return_value=tickertick)) as get_source,
    ):
        resp = await client.get("/api/v1/news/abc123")

    assert resp.status_code == 200
    assert resp.json()["id"] == "abc123"
    get_source.assert_awaited_once_with("tickertick")


async def test_concurrent_misses_collapse_to_one_fetch(client):
    """A burst of identical cache-miss requests hits the upstream provider once.

    Exercises the in-process single-flight: while the leader's fetch is in
    flight, the 4 followers attach to the same task instead of each stampeding
    the provider.
    """
    call_count = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_get_news(**kwargs):
        nonlocal call_count
        call_count += 1
        started.set()
        await release.wait()
        return {"results": [ARTICLE], "count": 1, "next_cursor": None}

    source = AsyncMock()
    source.get_news = slow_get_news
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)  # every request misses
    cache.set = AsyncMock()

    async def releaser():
        await started.wait()  # leader fetch is running
        await asyncio.sleep(0.02)  # let the followers attach to the inflight task
        release.set()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        reqs = [
            client.get("/api/v1/news?provider=tickertick&limit=5") for _ in range(5)
        ]
        results = await asyncio.gather(releaser(), *reqs)

    resps = results[1:]
    assert all(r.status_code == 200 for r in resps)
    assert all(r.json()["results"][0]["id"] == "abc123" for r in resps)
    assert call_count == 1  # 5 concurrent misses → 1 upstream fetch


async def test_distributed_leader_fetches_and_releases(client):
    """The worker that wins the Redis lock fetches upstream and releases it."""
    source = AsyncMock()
    source.get_news = AsyncMock(
        return_value={"results": [ARTICLE], "count": 1, "next_cursor": None}
    )
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    cache.acquire_lock = AsyncMock(return_value=True)  # we are the leader
    cache.release_lock = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    source.get_news.assert_awaited_once()
    cache.set.assert_awaited_once()
    cache.release_lock.assert_awaited_once()


async def test_distributed_follower_waits_for_leader(client):
    """A contended worker serves the leader's cached result without fetching."""
    source = AsyncMock()
    source.get_news = AsyncMock(
        return_value={"results": [ARTICLE], "count": 1, "next_cursor": None}
    )
    cache = AsyncMock()
    # Initial miss → None; first poll → None; second poll → leader filled it.
    cache.get = AsyncMock(
        side_effect=[None, None, {"results": [ARTICLE], "count": 1, "next_cursor": None}]
    )
    cache.set = AsyncMock()
    cache.acquire_lock = AsyncMock(return_value=False)  # someone else holds it
    cache.release_lock = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    assert resp.json()["results"][0]["id"] == "abc123"
    source.get_news.assert_not_awaited()  # follower never hit the upstream
    cache.release_lock.assert_not_awaited()  # didn't own the lock


async def test_distributed_redis_down_fetches_directly(client):
    """If the lock can't be evaluated (Redis down) the request fetches directly."""
    source = AsyncMock()
    source.get_news = AsyncMock(
        return_value={"results": [ARTICLE], "count": 1, "next_cursor": None}
    )
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    cache.acquire_lock = AsyncMock(return_value=None)  # Redis unusable
    cache.release_lock = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    source.get_news.assert_awaited_once()
    cache.release_lock.assert_not_awaited()  # no lock was held


async def test_distributed_follower_fallback_when_leader_stalls(client, monkeypatch):
    """A follower that never sees the cache fill falls back to a direct fetch."""
    monkeypatch.setattr("src.server.app.news._FOLLOWER_MAX_WAIT", 0.1)
    monkeypatch.setattr("src.server.app.news._FOLLOWER_POLL_INTERVAL", 0.02)
    source = AsyncMock()
    source.get_news = AsyncMock(
        return_value={"results": [ARTICLE], "count": 1, "next_cursor": None}
    )
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)  # leader never fills the cache
    cache.set = AsyncMock()
    cache.acquire_lock = AsyncMock(return_value=False)
    cache.release_lock = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    source.get_news.assert_awaited_once()  # fell back rather than blocking


async def test_filtered_request_bypasses_cache(client):
    """A date/sort-filtered request must NOT read or write the shared cache.

    The cache key is filter-blind, so caching a filtered result would serve
    wrong data to unfiltered callers and poison the global feed.
    """
    source = AsyncMock()
    source.get_news = AsyncMock(
        return_value={"results": [ARTICLE], "count": 1, "next_cursor": None}
    )
    cache = AsyncMock()
    cache.get = AsyncMock(return_value={"results": [ARTICLE]})  # would be wrong to read
    cache.set = AsyncMock()
    cache.acquire_lock = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get(
            "/api/v1/news?provider=tickertick&limit=5&published_after=2025-01-01"
        )

    assert resp.status_code == 200
    source.get_news.assert_awaited_once()
    assert source.get_news.await_args.kwargs["published_after"] == "2025-01-01"
    cache.get.assert_not_awaited()  # did not read the filter-blind key
    cache.set.assert_not_awaited()  # did not poison it
    cache.acquire_lock.assert_not_awaited()  # no single-flight/lock either


async def test_warm_buffer_sliced_to_requested_limit(client):
    """A cached buffer larger than `limit` (the poller keeps up to max_items) is
    sliced down on read so the response honors the requested limit."""
    buffer = [{**ARTICLE, "id": f"a{i}"} for i in range(100)]
    cache = AsyncMock()
    cache.get = AsyncMock(return_value={"results": buffer, "next_cursor": None})
    cache.set = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_source", AsyncMock()),
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=50")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["results"]) == 50  # not 100
    assert body["count"] == 50


async def test_single_flight_leader_cancel_does_not_fail_followers():
    """A cancelled leader (client disconnect) must not cancel the shared fetch or
    fail the followers waiting on it; the fetch still runs exactly once."""
    from src.server.app.news import _inflight, _single_flight

    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def slow():
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"results": [ARTICLE]}

    key = "cancel-test-key"
    leader = asyncio.ensure_future(_single_flight(key, slow))
    await started.wait()  # leader's fetch is in flight
    follower = asyncio.ensure_future(_single_flight(key, slow))
    await asyncio.sleep(0.01)  # let the follower attach to the shared task

    leader.cancel()  # client disconnects mid-fetch
    with pytest.raises(asyncio.CancelledError):
        await leader

    release.set()
    result = await follower
    assert result["results"][0]["id"] == "abc123"  # follower still got the result
    assert calls == 1  # shared fetch ran once, never re-run
    assert key not in _inflight  # cleaned up via the done-callback


async def test_article_by_id_fallback_swallows_and_404s(client):
    """If the TickerTick fallback raises, the except swallows it → 404, not 500."""
    cache = AsyncMock()
    cache.get_article_by_id = AsyncMock(return_value=None)
    chain = AsyncMock()
    chain.get_news_article = AsyncMock(return_value=None)

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
        patch("src.data_client.get_news_source", AsyncMock(side_effect=Exception("boom"))),
    ):
        resp = await client.get("/api/v1/news/missing")

    assert resp.status_code == 404
