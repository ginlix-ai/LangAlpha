"""Background news refresh poller.

Keeps the GLOBAL news feeds (curated "Top" + the Market general feed) warm by
fetching the latest page every interval and delta-merging new articles (by id)
into a rolling buffer, stored under the same Redis key the ``/news`` endpoint
reads. Reads stay always-warm (no cold-miss stampede) and the buffer self-heals
via the cache TTL if the poller stops.

Multi-worker safe: each feed is polled by exactly one worker per tick via a
Redis lock held for the interval (leader election), so N workers don't each
hammer the upstream every minute. Per-ticker feeds are unbounded and are left
on the on-demand cache path.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from src.config.settings import get_news_poll_config
from src.server.services.cache.news_cache_service import NewsCacheService, news_cache_key

logger = logging.getLogger(__name__)

# Service-account identity for the poller's background service-to-service data
# calls. Matches the principal price_monitor and the live-data WS already send,
# so every background transport authenticates the same way.
_SERVICE_USER_ID = "langalpha-service"


def _merge_delta(
    existing: list[dict[str, Any]],
    fetched: list[dict[str, Any]],
    max_items: int,
) -> tuple[list[dict[str, Any]], int]:
    """Merge the freshly fetched page into the existing rolling buffer.

    Dedupes by article id (the fetched copy wins so updated metadata sticks),
    sorts newest-first by ``published_at`` (UTC ISO strings sort chronologically),
    and trims to ``max_items``. Returns the merged buffer and the count of
    genuinely new articles (ids not previously in the buffer).
    """
    existing_ids = {a.get("id") for a in existing if a.get("id")}

    combined: dict[str, dict[str, Any]] = {}
    for article in (*fetched, *existing):  # fetched first → freshest copy wins
        aid = article.get("id")
        if aid and aid not in combined:
            combined[aid] = article

    merged = sorted(
        combined.values(),
        key=lambda a: a.get("published_at") or "",
        reverse=True,
    )[:max_items]

    new_count = len({a.get("id") for a in fetched if a.get("id")} - existing_ids)
    return merged, new_count


class NewsRefreshService:
    """Singleton background poller that delta-refreshes global news feeds."""

    _instance: NewsRefreshService | None = None

    @classmethod
    def get_instance(cls) -> NewsRefreshService:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._cache = NewsCacheService()
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._interval = 60
        self._max_items = 100
        self._feeds: list[Any] = []

    async def start(self) -> None:
        """Load config and launch the poll loop (no-op if disabled / no feeds)."""
        cfg = get_news_poll_config()
        if not cfg.enabled:
            logger.info("[NewsRefresh] disabled by config")
            return
        if not cfg.feeds:
            logger.info("[NewsRefresh] no feeds configured — not starting")
            return

        self._interval = cfg.interval_seconds
        self._max_items = cfg.max_items
        self._feeds = list(cfg.feeds)
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._poll_loop(), name="news_refresh_poll")
        logger.info(
            "[NewsRefresh] started — %d feed(s), every %ds, buffer<=%d",
            len(self._feeds), self._interval, self._max_items,
        )

    async def stop(self) -> None:
        """Signal shutdown and cancel the poll loop."""
        self._shutdown_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[NewsRefresh] stopped")

    # ─── Loop ────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Poll on start, then every interval — one failed cycle never kills it."""
        while not self._shutdown_event.is_set():
            try:
                await self._poll_once()
            except Exception:
                logger.error("[NewsRefresh] poll cycle failed", exc_info=True)

            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._interval
                )
                return  # shutdown requested during the sleep
            except asyncio.TimeoutError:
                pass

    async def _poll_once(self) -> None:
        for feed in self._feeds:
            try:
                await self._refresh_feed(feed)
            except Exception:
                logger.error(
                    "[NewsRefresh] feed refresh failed (provider=%s)",
                    getattr(feed, "provider", None), exc_info=True,
                )

    async def _refresh_feed(self, feed: Any) -> None:
        provider = feed.provider
        limit = feed.limit

        # Leader election: hold the lock for the interval (don't release) so
        # only one worker polls this feed per tick. None/False → skip. The
        # ``newspolllock:`` prefix keeps it out of the ``news:*`` keyspace that
        # get_article_by_id scans (its value is a token, not JSON).
        lock_key = "newspolllock:" + news_cache_key(None, limit, provider)
        acquired = await self._cache.acquire_lock(
            lock_key, uuid.uuid4().hex, self._interval * 1000
        )
        if not acquired:
            return

        source = await self._resolve_source(provider)
        data = await source.get_news(
            tickers=None, limit=limit, user_id=_SERVICE_USER_ID
        )
        fetched = data.get("results", []) if isinstance(data, dict) else []

        existing_wrap = await self._cache.get(
            tickers=None, limit=limit, provider=provider
        )
        existing = existing_wrap.get("results", []) if existing_wrap else []

        merged, new_count = _merge_delta(existing, fetched, self._max_items)
        # Continuation cursor for infinite scroll: the id at the served-page
        # boundary (the endpoint slices the buffer to `limit`), so page 2 picks
        # up exactly where page 1 ends — no gap, no overlap. Only named
        # providers (e.g. tickertick) paginate by cursor; the chain feed
        # (provider=None → FMP/yfinance) can't honor one, so leave it None.
        cursor_capable = provider is not None
        next_cursor = (
            merged[limit - 1].get("id")
            if cursor_capable and len(merged) >= limit
            else None
        )
        await self._cache.set(
            {"results": merged, "count": len(merged), "next_cursor": next_cursor},
            tickers=None, limit=limit, provider=provider,
        )
        logger.info(
            "[NewsRefresh] %s: +%d new, buffer=%d",
            provider or "chain", new_count, len(merged),
        )

    async def _resolve_source(self, provider: str | None) -> Any:
        if provider:
            from src.data_client import get_news_source

            return await get_news_source(provider)
        from src.data_client import get_news_data_provider

        return await get_news_data_provider()
