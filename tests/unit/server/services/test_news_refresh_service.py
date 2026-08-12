"""Unit tests for the news refresh poller (delta merge + leader election)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.config.models import NewsPollFeedConfig
from src.server.services.news_refresh_service import (
    _SERVICE_USER_ID,
    NewsRefreshService,
    _merge_delta,
)


def _article(aid: str, hour: int) -> dict:
    return {"id": aid, "published_at": f"2025-12-01T{hour:02d}:00:00+00:00", "title": aid}


class TestMergeDelta:
    def test_dedupe_sort_and_new_count(self):
        existing = [_article("b", 8), _article("c", 7)]
        fetched = [
            _article("a", 9),  # genuinely new, newest
            {**_article("b", 8), "title": "b-updated"},  # updated copy of existing
        ]
        merged, new_count = _merge_delta(existing, fetched, max_items=10)

        assert [m["id"] for m in merged] == ["a", "b", "c"]  # newest-first
        assert merged[1]["title"] == "b-updated"  # fetched copy wins on dup
        assert new_count == 1  # only 'a' was not already in the buffer

    def test_trims_to_max_items_keeping_newest(self):
        fetched = [_article(str(i), i) for i in range(5)]
        merged, _ = _merge_delta([], fetched, max_items=3)
        assert [m["id"] for m in merged] == ["4", "3", "2"]

    def test_empty_fetch_keeps_existing(self):
        existing = [_article("a", 9)]
        merged, new_count = _merge_delta(existing, [], max_items=10)
        assert [m["id"] for m in merged] == ["a"]
        assert new_count == 0


def _make_service() -> NewsRefreshService:
    svc = NewsRefreshService()
    svc._interval = 60
    svc._max_items = 100
    return svc


class TestRefreshFeed:
    @pytest.mark.asyncio
    async def test_leader_fetches_merges_writes(self, monkeypatch):
        svc = _make_service()
        cache = AsyncMock()
        cache.acquire_lock = AsyncMock(return_value=True)  # we win the lock
        cache.get = AsyncMock(return_value={"results": [_article("old", 7)]})
        cache.set = AsyncMock()
        svc._cache = cache

        source = AsyncMock()
        source.get_news = AsyncMock(
            return_value={"results": [_article("new", 9)], "count": 1, "next_cursor": "x"}
        )
        monkeypatch.setattr(svc, "_resolve_source", AsyncMock(return_value=source))

        await svc._refresh_feed(NewsPollFeedConfig(provider="tickertick", limit=50))

        source.get_news.assert_awaited_once_with(
            tickers=None, limit=50, user_id=_SERVICE_USER_ID
        )
        payload, kwargs = cache.set.await_args.args[0], cache.set.await_args.kwargs
        assert [r["id"] for r in payload["results"]] == ["new", "old"]
        assert payload["next_cursor"] is None  # only 2 items < limit 50
        assert kwargs == {"tickers": None, "limit": 50, "provider": "tickertick"}

        # Lock key mirrors the cache identity; TTL is the poll interval (ms).
        lock_args = cache.acquire_lock.await_args.args
        assert lock_args[0] == "newspolllock:news:tickertick:general:50"
        assert lock_args[2] == 60000

    @pytest.mark.asyncio
    async def test_full_buffer_sets_continuation_cursor(self, monkeypatch):
        """Cursor-capable provider + full buffer → cursor at the served-page boundary."""
        svc = _make_service()
        cache = AsyncMock()
        cache.acquire_lock = AsyncMock(return_value=True)
        cache.get = AsyncMock(return_value=None)  # cold start
        cache.set = AsyncMock()
        svc._cache = cache

        fetched = [_article(f"s{i}", i) for i in range(3)]
        source = AsyncMock()
        source.get_news = AsyncMock(
            return_value={"results": fetched, "count": 3, "next_cursor": None}
        )
        monkeypatch.setattr(svc, "_resolve_source", AsyncMock(return_value=source))

        await svc._refresh_feed(NewsPollFeedConfig(provider="tickertick", limit=3))

        payload = cache.set.await_args.args[0]
        assert [r["id"] for r in payload["results"]] == ["s2", "s1", "s0"]
        assert payload["next_cursor"] == "s0"  # boundary id (merged[limit-1])
        assert (
            cache.acquire_lock.await_args.args[0]
            == "newspolllock:news:tickertick:general:3"
        )

    @pytest.mark.asyncio
    async def test_chain_feed_never_sets_cursor(self, monkeypatch):
        """provider=None resolves to the chain (FMP/yfinance), which can't honor a
        cursor — so the buffer is served without one even when full."""
        svc = _make_service()
        cache = AsyncMock()
        cache.acquire_lock = AsyncMock(return_value=True)
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock()
        svc._cache = cache

        fetched = [_article(f"s{i}", i) for i in range(3)]
        source = AsyncMock()
        source.get_news = AsyncMock(return_value={"results": fetched})
        monkeypatch.setattr(svc, "_resolve_source", AsyncMock(return_value=source))

        await svc._refresh_feed(NewsPollFeedConfig(provider=None, limit=3))

        payload = cache.set.await_args.args[0]
        assert payload["next_cursor"] is None  # full buffer but chain feed
        assert cache.acquire_lock.await_args.args[0] == "newspolllock:news:general:3"

    @pytest.mark.asyncio
    async def test_cursor_is_page_boundary_not_buffer_tail(self, monkeypatch):
        """When the buffer holds more than one page (max_items > limit), the cursor
        is the served-page boundary id, not the oldest buffered id."""
        svc = _make_service()
        svc._max_items = 5
        cache = AsyncMock()
        cache.acquire_lock = AsyncMock(return_value=True)
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock()
        svc._cache = cache

        fetched = [_article(f"s{i}", i) for i in range(5)]  # hours 0..4
        source = AsyncMock()
        source.get_news = AsyncMock(return_value={"results": fetched})
        monkeypatch.setattr(svc, "_resolve_source", AsyncMock(return_value=source))

        await svc._refresh_feed(NewsPollFeedConfig(provider="tickertick", limit=3))

        payload = cache.set.await_args.args[0]
        # merged newest-first: s4,s3,s2,s1,s0 (5 items, max_items=5)
        assert [r["id"] for r in payload["results"]] == ["s4", "s3", "s2", "s1", "s0"]
        # served page is limit=3 → boundary is merged[2] == "s2", NOT the tail "s0"
        assert payload["next_cursor"] == "s2"

    @pytest.mark.asyncio
    async def test_poller_names_service_user_on_every_feed(self, monkeypatch):
        """Regression: the user-less poller must send a service-account identity
        so its background data call authenticates. Without it the request is
        rejected and the provider chain silently falls back (news.fallback).
        """
        svc = _make_service()
        cache = AsyncMock()
        cache.acquire_lock = AsyncMock(return_value=True)
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock()
        svc._cache = cache

        source = AsyncMock()
        source.get_news = AsyncMock(return_value={"results": []})
        monkeypatch.setattr(svc, "_resolve_source", AsyncMock(return_value=source))

        # The chain feed (provider=None) is the path that hits the data service.
        await svc._refresh_feed(NewsPollFeedConfig(provider=None, limit=50))

        assert source.get_news.await_args.kwargs["user_id"] == _SERVICE_USER_ID

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lock_result", [False, None])
    async def test_skips_when_not_leader(self, monkeypatch, lock_result):
        """False (another worker holds it) or None (Redis down) → skip, no fetch."""
        svc = _make_service()
        cache = AsyncMock()
        cache.acquire_lock = AsyncMock(return_value=lock_result)
        cache.get = AsyncMock()
        cache.set = AsyncMock()
        svc._cache = cache

        resolve = AsyncMock()
        monkeypatch.setattr(svc, "_resolve_source", resolve)

        await svc._refresh_feed(NewsPollFeedConfig(provider="tickertick", limit=50))

        resolve.assert_not_called()
        cache.get.assert_not_called()
        cache.set.assert_not_called()


class TestPollOnce:
    @pytest.mark.asyncio
    async def test_one_feed_error_does_not_skip_the_rest(self):
        svc = _make_service()
        svc._feeds = [
            NewsPollFeedConfig(provider="tickertick", limit=50),
            NewsPollFeedConfig(provider=None, limit=50),
        ]
        seen = []

        async def fake_refresh(feed):
            seen.append(feed.provider)
            if feed.provider == "tickertick":
                raise RuntimeError("boom")

        svc._refresh_feed = fake_refresh
        await svc._poll_once()

        assert seen == ["tickertick", None]  # second feed still ran
