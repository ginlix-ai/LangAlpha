"""Tests for the CN news eligibility gate (provider=tushare) in app/news.py.

Locks the serving contract: eligible users (A-share pack + zh locale) get the
tushare source under the tushare cache key; ineligible requests downgrade to
the default chain BEFORE any cache key is derived, so a fallback response can
never poison the tushare feed key. An article fetched by its ``ts-`` id
answers an ineligible user as an unknown id.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

CN_ARTICLE = {
    "id": "ts-abc",
    "title": "示例新闻标题",
    "author": None,
    "description": "内容",
    "published_at": "2026-07-11T11:00:00+08:00",
    "article_url": None,
    "image_url": None,
    "source": {"name": "sina", "logo_url": None, "homepage_url": None, "favicon_url": None},
    "tickers": [],
    "keywords": [],
    "sentiments": None,
}

EN_ARTICLE = {
    "id": "chain-1",
    "title": "Chain story",
    "author": None,
    "description": None,
    "published_at": "2026-07-11T02:00:00+00:00",
    "article_url": "https://example.com/a",
    "image_url": None,
    "source": {"name": "wire", "logo_url": None, "homepage_url": None, "favicon_url": None},
    "tickers": [],
    "keywords": [],
    "sentiments": None,
}


@pytest.fixture(autouse=True)
def _feed_buildable():
    """The feed is buildable unless a test says otherwise; that needs a data
    URL this suite does not configure."""
    with patch("src.server.app.news.news_source_available", return_value=True) as available:
        yield available


@pytest_asyncio.fixture
async def client():
    from src.server.app.news import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _mock_cache():
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    cache.acquire_lock = AsyncMock(return_value=True)
    cache.release_lock = AsyncMock()
    return cache


def _user_db(pack: bool, locale: str):
    # The pack toggle rides the feature-flag framework: an opt_in override
    # under other_preference.feature_overrides, read via services.features
    # (which imports get_user_preferences at module level — patch it there).
    prefs = AsyncMock(
        return_value={"other_preference": {"feature_overrides": {"a_share_pack": pack}}}
    )
    user = AsyncMock(return_value={"user_id": "u1", "locale": locale})
    return prefs, user


async def test_eligible_user_gets_tushare_feed(client):
    source = AsyncMock()
    source.get_news.return_value = {"results": [CN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()
    prefs, user = _user_db(pack=True, locale="zh-CN")

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.server.database.user.get_user", user),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock()) as get_chain,
    ):
        resp = await client.get("/api/v1/news?provider=tushare&limit=5")

    assert resp.status_code == 200
    assert resp.json()["results"][0]["id"] == "ts-abc"
    get_source.assert_awaited_once_with("tushare")
    get_chain.assert_not_called()
    assert cache.get.await_args.kwargs.get("provider") == "tushare"
    assert cache.set.await_args.kwargs.get("provider") == "tushare"


@pytest.mark.parametrize(
    "pack,locale",
    [(False, "zh-CN"), (True, "en-US"), (False, "en-US")],
)
async def test_ineligible_user_downgrades_to_chain(client, pack, locale):
    chain = AsyncMock()
    chain.get_news.return_value = {"results": [EN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()
    prefs, user = _user_db(pack=pack, locale=locale)

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.server.database.user.get_user", user),
        patch("src.data_client.get_news_source", AsyncMock()) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)) as get_chain,
    ):
        resp = await client.get("/api/v1/news?provider=tushare&limit=5")

    assert resp.status_code == 200
    assert resp.json()["results"][0]["id"] == "chain-1"
    get_source.assert_not_called()
    get_chain.assert_awaited_once()
    # Downgrade happened before cache-key derivation — never the tushare key.
    assert cache.get.await_args.kwargs.get("provider") is None
    assert cache.set.await_args.kwargs.get("provider") is None


async def test_an_unbuildable_feed_downgrades_for_an_eligible_user(client, _feed_buildable):
    """The client asks for the feed on eligibility alone; a server that cannot
    build it answers the default feed, not a 400."""
    _feed_buildable.return_value = False
    chain = AsyncMock()
    chain.get_news.return_value = {"results": [EN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()
    prefs, user = _user_db(pack=True, locale="zh-CN")

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.server.database.user.get_user", user),
        patch("src.data_client.get_news_source", AsyncMock()) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
    ):
        resp = await client.get("/api/v1/news?provider=tushare&limit=5")

    assert resp.status_code == 200
    assert resp.json()["results"][0]["id"] == "chain-1"
    get_source.assert_not_called()
    assert cache.get.await_args.kwargs.get("provider") is None


async def test_eligibility_lookup_failure_downgrades(client):
    chain = AsyncMock()
    chain.get_news.return_value = {"results": [EN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()

    with (
        patch("src.server.app.news._cache", cache),
        patch(
            "src.server.services.features.get_user_preferences",
            AsyncMock(side_effect=RuntimeError("db down")),
        ),
        patch("src.data_client.get_news_source", AsyncMock()) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
    ):
        resp = await client.get("/api/v1/news?provider=tushare&limit=5")

    assert resp.status_code == 200
    get_source.assert_not_called()


async def test_other_providers_not_gated(client):
    """The gate is tushare-specific — tickertick needs no eligibility lookup."""
    source = AsyncMock()
    source.get_news.return_value = {"results": [EN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()
    prefs = AsyncMock()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock()),
    ):
        resp = await client.get("/api/v1/news?provider=tickertick&limit=5")

    assert resp.status_code == 200
    get_source.assert_awaited_once_with("tickertick")
    prefs.assert_not_called()


@pytest.mark.parametrize(
    "accept_language,eligible",
    [
        ("zh-CN,zh;q=0.9,en;q=0.8", True),
        ("zh", True),
        ("en-US,en;q=0.9", False),
        (None, False),
    ],
)
async def test_null_db_locale_falls_back_to_accept_language(
    client, accept_language, eligible
):
    """users.locale is null for nearly everyone — the header decides then."""
    source = AsyncMock()
    source.get_news.return_value = {"results": [CN_ARTICLE], "count": 1, "next_cursor": None}
    chain = AsyncMock()
    chain.get_news.return_value = {"results": [EN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()
    prefs, user = _user_db(pack=True, locale=None)

    headers = {"Accept-Language": accept_language} if accept_language else {}
    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.server.database.user.get_user", user),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
    ):
        resp = await client.get("/api/v1/news?provider=tushare&limit=5", headers=headers)

    assert resp.status_code == 200
    if eligible:
        get_source.assert_awaited_once_with("tushare")
    else:
        get_source.assert_not_called()


async def test_db_locale_wins_over_accept_language(client):
    """An explicit Settings choice is not overridden by the browser header."""
    chain = AsyncMock()
    chain.get_news.return_value = {"results": [EN_ARTICLE], "count": 1, "next_cursor": None}
    cache = _mock_cache()
    prefs, user = _user_db(pack=True, locale="en-US")

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.server.database.user.get_user", user),
        patch("src.data_client.get_news_source", AsyncMock()) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
    ):
        resp = await client.get(
            "/api/v1/news?provider=tushare&limit=5",
            headers={"Accept-Language": "zh-CN,zh;q=0.9"},
        )

    assert resp.status_code == 200
    get_source.assert_not_called()


@pytest.mark.parametrize("pack", [True, False])
async def test_tushare_article_by_id_is_gated(client, pack):
    """A ts- id reaches only eligible users; everyone else gets the unknown-id
    404 before any cache or source read, so the gate doesn't confirm the id."""
    # A flash has no link; ginlix-data sends "" because the full shape needs a str.
    article = {**CN_ARTICLE, "article_url": ""}
    cache = AsyncMock()
    cache.get_article_by_id = AsyncMock(return_value=article)
    source = AsyncMock()
    source.get_news_article = AsyncMock(return_value=article)
    prefs, user = _user_db(pack=pack, locale="zh-CN")

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.server.services.features.get_user_preferences", prefs),
        patch("src.server.database.user.get_user", user),
        patch("src.data_client.get_news_source", AsyncMock(return_value=source)) as get_source,
        patch("src.data_client.get_news_data_provider", AsyncMock()) as get_chain,
    ):
        resp = await client.get("/api/v1/news/ts-abc")

    if pack:
        assert resp.status_code == 200
        assert resp.json()["id"] == "ts-abc"
    else:
        assert resp.status_code == 404
        assert resp.json() == {"detail": "Article not found"}
        cache.get_article_by_id.assert_not_called()
        get_chain.assert_not_called()
        get_source.assert_not_called()


async def test_upstream_failure_is_503_not_500(client):
    """A vendor refusal (rate limit, outage) is unavailability, not a fault in
    this server: the client polls again later."""
    chain = AsyncMock()
    chain.get_news.side_effect = Exception("TickerTick request failed: 429 Too Many Requests")
    cache = _mock_cache()

    with (
        patch("src.server.app.news._cache", cache),
        patch("src.data_client.get_news_data_provider", AsyncMock(return_value=chain)),
    ):
        resp = await client.get("/api/v1/news?limit=5")

    assert resp.status_code == 503
    assert "unavailable" in resp.json()["detail"].lower()
    cache.set.assert_not_called()
