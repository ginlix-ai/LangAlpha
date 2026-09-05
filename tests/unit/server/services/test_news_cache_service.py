"""Unit tests for NewsCacheService.get_article_by_id.

Regression guard: the by-id scan previously called ``cache.keys()`` — a method
RedisCacheClient does not define — so it always raised AttributeError, was
swallowed, and returned None (the fast path never fired). It now uses the
SCAN-based ``scan_keys`` helper.
"""

from __future__ import annotations

import pytest

from src.server.services.cache.news_cache_service import NewsCacheService

_GET_CACHE = "src.server.services.cache.news_cache_service.get_cache_client"


class _StubCache:
    """Minimal stand-in: scan_keys + get over an in-memory keyspace.

    Values are decoded dicts, matching what RedisCacheClient.get returns — it
    JSON-decodes before handing the value back to NewsCacheService.
    """

    def __init__(self, store: dict[str, dict]):
        self._store = store
        self.mget_calls = 0

    async def scan_keys(self, pattern: str) -> list[str]:
        return list(self._store)

    async def get(self, key: str):
        return self._store.get(key)

    async def mget(self, keys: list[str]) -> list:
        self.mget_calls += 1
        return [self._store.get(k) for k in keys]


@pytest.mark.asyncio
async def test_get_article_by_id_finds_match(monkeypatch):
    article = {"id": "abc", "title": "Top story", "article_url": "https://x/a"}
    store = {
        "news:general:20": {"results": [{"id": "zzz"}]},
        "news:tickertick:general:50": {"results": [article]},
    }
    cache = _StubCache(store)
    monkeypatch.setattr(_GET_CACHE, lambda: cache)

    found = await NewsCacheService().get_article_by_id("abc")
    assert found == article
    # One MGET for the whole scan, not a GET per key.
    assert cache.mget_calls == 1


@pytest.mark.asyncio
async def test_get_article_by_id_miss_returns_none(monkeypatch):
    store = {"news:general:20": {"results": [{"id": "zzz"}]}}
    monkeypatch.setattr(_GET_CACHE, lambda: _StubCache(store))

    assert await NewsCacheService().get_article_by_id("abc") is None


@pytest.mark.asyncio
async def test_get_article_by_id_empty_keyspace(monkeypatch):
    monkeypatch.setattr(_GET_CACHE, lambda: _StubCache({}))
    assert await NewsCacheService().get_article_by_id("abc") is None


@pytest.mark.asyncio
async def test_lock_helpers_degrade_when_client_unavailable(monkeypatch):
    # get_cache_client() blowing up (e.g. pool not ready on a cold start) must
    # not propagate: acquire_lock returns None so the single-flight leader falls
    # back to a direct fetch instead of 500-ing every concurrent waiter, and
    # release_lock stays a no-op.
    def _boom():
        raise RuntimeError("cache client unavailable")

    monkeypatch.setattr(_GET_CACHE, _boom)
    svc = NewsCacheService()
    assert await svc.acquire_lock("newslock:k", "tok", 30_000) is None
    await svc.release_lock("newslock:k", "tok")  # must not raise
