"""Simple Redis TTL cache for news articles."""

from __future__ import annotations

import logging
from typing import Any

from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# TTLs in seconds
_GENERAL_TTL = 300  # 5 min for general news
_TICKER_TTL = 180  # 3 min for ticker-specific news


def _cache_key(tickers: list[str] | None, limit: int, provider: str | None = None) -> str:
    # Keep the ``news:`` root so get_article_by_id's ``news:*`` scan still
    # covers provider-scoped lists.
    prefix = f"news:{provider}" if provider else "news"
    if tickers:
        tag = ",".join(sorted(t.upper() for t in tickers))
        return f"{prefix}:tickers:{tag}:{limit}"
    return f"{prefix}:general:{limit}"


# Public alias — the news router and refresh poller derive lock / single-flight
# keys from this, so it's part of the module's intentional surface, not private.
news_cache_key = _cache_key


class NewsCacheService:
    _instance: NewsCacheService | None = None

    def __new__(cls) -> NewsCacheService:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def get(
        self,
        tickers: list[str] | None = None,
        limit: int = 20,
        provider: str | None = None,
    ) -> dict[str, Any] | None:
        try:
            cache = get_cache_client()
            key = _cache_key(tickers, limit, provider)
            # RedisCacheClient.get already JSON-decodes — the value is a dict.
            return await cache.get(key)
        except Exception:
            logger.debug("news_cache.get.miss", exc_info=True)
        return None

    async def get_article_by_id(self, article_id: str) -> dict[str, Any] | None:
        """Scan all cached news lists for an article matching the given ID.

        Uses a non-blocking SCAN over the ``news:*`` keyspace (short-lived,
        bounded by provider × ticker-combo × limit), reading each list and
        returning the first article whose id matches. A miss falls through to
        the provider in the caller, so this is a best-effort fast path.
        """
        try:
            cache = get_cache_client()
            keys = await cache.scan_keys("news:*")
            if not keys:
                return None
            # One MGET instead of a GET per key: the scan is bounded but not
            # small, and N sequential round trips made a best-effort fast path
            # the slowest thing on the request.
            for data in await cache.mget(list(keys)):
                if data:
                    for article in data.get("results", []):
                        if article.get("id") == article_id:
                            return article
        except Exception:
            logger.debug("news_cache.get_article_by_id.failed", exc_info=True)
        return None

    async def acquire_lock(self, key: str, token: str, ttl_ms: int) -> bool | None:
        """Distributed refresh lock (see RedisCacheClient.acquire_lock).

        Guards the client lookup like every other method here: a failure returns
        None ("uncoordinated — fetch directly") rather than propagating through
        the shared single-flight task and 500-ing every waiter.
        """
        try:
            return await get_cache_client().acquire_lock(key, token, ttl_ms)
        except Exception:
            logger.debug("news_cache.acquire_lock.failed", exc_info=True)
            return None

    async def release_lock(self, key: str, token: str) -> None:
        """Release a refresh lock held under ``key`` with ``token`` (best-effort)."""
        try:
            await get_cache_client().release_lock(key, token)
        except Exception:
            logger.debug("news_cache.release_lock.failed", exc_info=True)

    async def set(
        self,
        data: dict[str, Any],
        tickers: list[str] | None = None,
        limit: int = 20,
        provider: str | None = None,
    ) -> None:
        try:
            cache = get_cache_client()
            key = _cache_key(tickers, limit, provider)
            ttl = _TICKER_TTL if tickers else _GENERAL_TTL
            await cache.set(key, data, ttl=ttl)
        except Exception:
            logger.debug("news_cache.set.failed", exc_info=True)
