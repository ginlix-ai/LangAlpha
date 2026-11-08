"""News data source backed by ginlix-data."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .client import GinlixDataClient

logger = logging.getLogger(__name__)


def _normalize_article(raw: dict[str, Any]) -> dict[str, Any]:
    """Map ginlix-data article schema → common NewsArticle dict."""
    publisher = raw.get("publisher") or {}
    return {
        "id": str(raw.get("id", "")),
        "title": raw.get("title", ""),
        "author": raw.get("author"),
        "description": raw.get("description"),
        "published_at": raw.get("published_utc", ""),
        "article_url": raw.get("article_url", ""),
        "image_url": raw.get("image_url"),
        "source": {
            "name": publisher.get("name", "")
            if isinstance(publisher, dict)
            else str(publisher),
            "logo_url": publisher.get("logo_url")
            if isinstance(publisher, dict)
            else None,
            "homepage_url": publisher.get("homepage_url")
            if isinstance(publisher, dict)
            else None,
            "favicon_url": publisher.get("favicon_url")
            if isinstance(publisher, dict)
            else None,
        },
        "tickers": raw.get("tickers") or [],
        "keywords": raw.get("keywords") or [],
        "sentiments": [
            {
                "ticker": ins.get("ticker", ""),
                "sentiment": ins.get("sentiment"),
                "reasoning": ins.get("sentiment_reasoning"),
            }
            for ins in (raw.get("insights") or [])
        ]
        or None,
    }


class GinlixDataNewsSource:
    """Fetches news from the ginlix-data service.

    For multi-ticker queries the service only supports a single ``ticker``
    param, so we fan-out requests in parallel and merge results.
    """

    def __init__(self, client: GinlixDataClient) -> None:
        self._client = client

    async def get_news(
        self,
        tickers: list[str] | None = None,
        limit: int = 20,
        published_after: str | None = None,
        published_before: str | None = None,
        cursor: str | None = None,
        order: str | None = None,
        sort: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        if not tickers or len(tickers) == 1:
            body = await self._client.get_news(
                ticker=tickers[0] if tickers else None,
                limit=limit,
                published_after=published_after,
                published_before=published_before,
                cursor=cursor,
                order=order,
                sort=sort,
                user_id=user_id,
            )
            results = [_normalize_article(r) for r in body.get("results", [])]
            return {
                "results": results,
                "count": len(results),
                "next_cursor": body.get("next_cursor"),
            }

        # Multi-ticker: parallel requests, merge & sort by published_at in the
        # requested direction (desc by default). Asking each ticker for `limit`
        # rows makes the merged top `limit` exact either way.
        async def _fetch(ticker: str) -> list[dict[str, Any]]:
            try:
                body = await self._client.get_news(
                    ticker=ticker,
                    limit=limit,
                    published_after=published_after,
                    published_before=published_before,
                    order=order,
                    sort=sort,
                    user_id=user_id,
                )
                return [_normalize_article(r) for r in body.get("results", [])]
            except Exception:
                logger.warning(
                    "news.ginlix_data.ticker_fetch_failed | ticker=%s",
                    ticker,
                    exc_info=True,
                )
                return []

        batches = await asyncio.gather(*[_fetch(t) for t in tickers])
        merged: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for batch in batches:
            for article in batch:
                if article["id"] not in seen_ids:
                    seen_ids.add(article["id"])
                    merged.append(article)

        merged.sort(
            key=lambda a: a.get("published_at", ""),
            reverse=(order or "").lower() != "asc",
        )
        merged = merged[:limit]

        return {
            "results": merged,
            "count": len(merged),
            "next_cursor": None,  # pagination not supported for merged multi-ticker
        }

    async def get_news_article(
        self, article_id: str, user_id: str | None = None
    ) -> dict[str, Any] | None:
        """Fetch recent articles and return the one matching *article_id*."""
        try:
            body = await self._client.get_news(limit=100, user_id=user_id)
            for raw in body.get("results", []):
                article = _normalize_article(raw)
                if article["id"] == article_id:
                    return article
        except Exception:
            logger.warning("news.ginlix_data.article_lookup_failed", exc_info=True)
        return None

    async def close(self) -> None:
        pass  # lifecycle managed by GinlixDataClient singleton
