"""News feed endpoint — replaces the infoflow proxy for news sections."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from src.server.models.news import (
    NewsArticle,
    NewsArticleCompact,
    NewsCompactResponse,
    NewsPublisher,
)
from src.server.services.cache.news_cache_service import NewsCacheService
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/news", tags=["News"])

_cache = NewsCacheService()


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
) -> NewsCompactResponse:
    ticker_list = (
        [t.strip().upper() for t in tickers.split(",") if t.strip()]
        if tickers
        else None
    )

    # Check cache (skip when cursor is used — paginated requests bypass cache)
    if not cursor:
        cached = await _cache.get(tickers=ticker_list, limit=limit)
        if cached:
            results = [c for a in cached["results"] if (c := _compact(a)) is not None]
            return NewsCompactResponse(
                results=results,
                count=len(results),
                next_cursor=cached.get("next_cursor"),
            )

    from src.data_client import get_news_data_provider

    provider = await get_news_data_provider()
    data = await provider.get_news(
        tickers=ticker_list,
        limit=limit,
        cursor=cursor,
        published_after=published_after,
        published_before=published_before,
        order=order,
        sort=sort,
        user_id=user_id,
    )

    # Populate cache (stores full articles internally)
    if not cursor:
        await _cache.set(data, tickers=ticker_list, limit=limit)

    results = [c for a in data["results"] if (c := _compact(a)) is not None]
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

    raise HTTPException(status_code=404, detail="Article not found")
