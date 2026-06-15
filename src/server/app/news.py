"""News feed endpoint (general + ticker-filtered sections)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, HTTPException, Query

from src.server.models.news import (
    NewsArticle,
    NewsArticleCompact,
    NewsCompactResponse,
    NewsPublisher,
)
from src.server.services.cache.news_cache_service import NewsCacheService, news_cache_key
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/news", tags=["News"])

_cache = NewsCacheService()

# Cache-stampede protection has two layers so it holds up under many workers:
#
#  1. In-process single-flight (`_inflight`): collapses a burst of concurrent
#     misses *within one worker/event loop* to a single participant.
#  2. Distributed refresh lock (`_get_news_data`): collapses across workers and
#     replicas sharing Redis — exactly one worker fetches a given cold key; the
#     rest wait for it to fill the cache, then read it.
#
# News is public and globally cached (key = provider+tickers+limit, no user),
# so without this a hot key's TTL expiry would fan out to one upstream fetch per
# concurrent request. Cursor (paginated) requests are unique and skip both.
_inflight: dict[str, "asyncio.Future[dict]"] = {}

# Leader holds the refresh lock at most this long — set to cover the upstream
# request timeout so a slow-but-alive fetch can't have its lock expire and let a
# second worker double-fetch. It's only an auto-release safety net for a crashed
# leader; live followers never wait this long (see _FOLLOWER_MAX_WAIT).
_LOCK_TTL_MS = 30_000
# A follower (lost the lock race) polls the cache this often, up to this long,
# for the leader's result before giving up and fetching directly. Bounds tail
# latency if the leader is slow or died mid-fetch.
_FOLLOWER_POLL_INTERVAL = 0.05
_FOLLOWER_MAX_WAIT = 3.0


async def _single_flight(key: str, fetch: Callable[[], Awaitable[dict]]) -> dict:
    """Run *fetch* once per *key*, sharing its result with overlapping callers.

    The shared task is awaited through ``asyncio.shield`` so a caller that gets
    cancelled (e.g. client disconnect) can't cancel the in-flight fetch and fail
    every other waiter or leave the cache cold. Cleanup is tied to the task
    finishing (done-callback), not to any single awaiter's lifecycle.
    """
    task = _inflight.get(key)
    if task is None:
        task = asyncio.ensure_future(fetch())
        _inflight[key] = task
        task.add_done_callback(lambda t: _inflight.pop(key, None))
    return await asyncio.shield(task)


async def _get_news_data(
    ticker_list: list[str] | None,
    limit: int,
    provider: str | None,
    fetch: Callable[[], Awaitable[dict]],
) -> dict:
    """Cross-worker single-flight for one (provider, tickers, limit) cold key.

    The lock winner (leader) fetches upstream and populates the cache; everyone
    else waits for the cache to fill. Degrades safely: if Redis is unusable the
    caller fetches directly, and if the leader stalls/dies a follower falls back
    to its own fetch rather than blocking.
    """
    # Lock keys live OUTSIDE the ``news:*`` keyspace that get_article_by_id
    # scans — their value is a bare token, not JSON, so a ``news:*`` scan over
    # them would log spurious deserialize errors.
    lock_key = "newslock:" + news_cache_key(ticker_list, limit, provider)
    token = uuid.uuid4().hex
    acquired = await _cache.acquire_lock(lock_key, token, _LOCK_TTL_MS)

    if acquired is None:
        # Redis unavailable — no coordination possible, just fetch.
        return await fetch()

    if acquired:
        try:
            return await fetch()  # fetch() also populates the cache
        finally:
            await _cache.release_lock(lock_key, token)

    # Another worker is fetching — wait for it to fill the cache.
    waited = 0.0
    while waited < _FOLLOWER_MAX_WAIT:
        await asyncio.sleep(_FOLLOWER_POLL_INTERVAL)
        waited += _FOLLOWER_POLL_INTERVAL
        cached = await _cache.get(tickers=ticker_list, limit=limit, provider=provider)
        if cached is not None:
            return cached

    # Leader too slow or gone — fall back to a direct fetch.
    return await fetch()


def _compact(article: dict) -> NewsArticleCompact | None:
    """Convert a full article dict to a compact model. Returns None for invalid articles."""
    title = article.get("title")
    if not title:
        return None
    sentiments = article.get("sentiments")
    article_id = article.get("id")
    source = article.get("source")
    if not article_id or not source:
        return None
    return NewsArticleCompact(
        id=article_id,
        title=title,
        published_at=article.get("published_at", ""),
        image_url=article.get("image_url"),
        article_url=article.get("article_url"),
        source=NewsPublisher(**source),
        tickers=article.get("tickers", []),
        has_sentiment=bool(sentiments and len(sentiments) > 0),
        author=article.get("author"),
        description=article.get("description"),
        keywords=article.get("keywords", []),
        sentiments=sentiments,
    )


@router.get("", response_model=NewsCompactResponse)
async def get_news(
    user_id: CurrentUserId,
    tickers: str | None = Query(None, description="Comma-separated ticker symbols"),
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = Query(None, description="Pagination cursor"),
    published_after: str | None = Query(None, description="ISO 8601 date filter"),
    published_before: str | None = Query(None, description="ISO 8601 date filter"),
    order: str | None = Query(None, description="Sort order: asc or desc"),
    sort: str | None = Query(None, description="Sort field, e.g. published_utc"),
    provider: str | None = Query(
        None,
        max_length=32,
        pattern=r"^[a-z0-9_-]+$",
        description="Target a specific news provider (e.g. 'tickertick')",
    ),
) -> NewsCompactResponse:
    ticker_list = (
        [t.strip().upper() for t in tickers.split(",") if t.strip()]
        if tickers
        else None
    )

    # Cursors and date/sort filters make a request unique, but the cache and
    # single-flight/lock keys only carry (provider, tickers, limit). Sharing
    # them would serve wrong (filter-blind) results and let a filtered cold-miss
    # poison the global feed, so these requests bypass the shared path entirely.
    bypass_cache = bool(
        cursor or published_after or published_before or order or sort
    )

    async def _fetch_from_provider() -> dict:
        if provider:
            from src.data_client import get_news_source

            try:
                source = await get_news_source(provider)
            except ValueError as e:
                # Unknown/unavailable provider is a client error, not a 500.
                raise HTTPException(status_code=400, detail=str(e)) from e
        else:
            from src.data_client import get_news_data_provider

            source = await get_news_data_provider()

        data = await source.get_news(
            tickers=ticker_list,
            limit=limit,
            cursor=cursor,
            published_after=published_after,
            published_before=published_before,
            order=order,
            sort=sort,
            user_id=user_id,
        )
        # Populate cache (stores full articles internally).
        if not bypass_cache:
            await _cache.set(data, tickers=ticker_list, limit=limit, provider=provider)
        return data

    # Cursor / filtered requests go straight to the provider — no cache read,
    # no cache write, no single-flight or distributed lock.
    if bypass_cache:
        data = await _fetch_from_provider()
    else:
        cached = await _cache.get(tickers=ticker_list, limit=limit, provider=provider)
        if cached is not None:
            data = cached
        else:
            # Cache miss: collapse identical concurrent misses into one upstream
            # fetch — in-process first, then across workers via the Redis lock.
            sf_key = news_cache_key(ticker_list, limit, provider)
            data = await _single_flight(
                sf_key,
                lambda: _get_news_data(
                    ticker_list, limit, provider, _fetch_from_provider
                ),
            )

    # Slice to the requested limit: the warm buffer the poller maintains can hold
    # more than one page (max_items > limit), so honor the contract on read.
    articles = data["results"][:limit]
    results = [c for a in articles if (c := _compact(a)) is not None]
    return NewsCompactResponse(
        results=results,
        count=len(results),
        next_cursor=data.get("next_cursor"),
    )


@router.get("/{article_id}", response_model=NewsArticle)
async def get_news_article(article_id: str, user_id: CurrentUserId):
    # Fast path: check cache
    cached = await _cache.get_article_by_id(article_id)
    if cached:
        return NewsArticle(**cached)

    # Slow path: fetch from provider chain
    from src.data_client import get_news_data_provider

    provider = await get_news_data_provider()
    article = await provider.get_news_article(article_id, user_id=user_id)
    if article:
        return NewsArticle(**article)

    # TickerTick is targeted directly (not in the chain) — try it for its rows.
    try:
        from src.data_client import get_news_source

        tickertick = await get_news_source("tickertick")
        article = await tickertick.get_news_article(article_id, user_id=user_id)
        if article:
            return NewsArticle(**article)
    except Exception:
        logger.debug("news.tickertick.article_lookup_failed", exc_info=True)

    raise HTTPException(status_code=404, detail="Article not found")
