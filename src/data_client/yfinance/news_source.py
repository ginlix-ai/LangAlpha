"""NewsDataSource implementation backed by yfinance.

Free fallback provider — used when both ginlix-data and FMP are unavailable.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)


_DEFAULT_NEWS_TICKERS = ["SPY", "QQQ", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA"]


def _fetch_news(
    tickers: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    """Synchronous helper — called via ``asyncio.to_thread``."""
    articles: list[dict[str, Any]] = []
    seen_uuids: set[str] = set()

    symbols = tickers if tickers else _DEFAULT_NEWS_TICKERS
    for sym in symbols:
        if len(articles) >= limit:
            break
        try:
            news = yf.Ticker(sym).news or []
        except Exception:
            logger.warning("yfinance.news.failed | symbol=%s", sym, exc_info=True)
            continue

        for item in news:
            if len(articles) >= limit:
                break

            # yfinance >= 0.2.31 nests data under 'content'
            content = item.get("content", item)
            title = content.get("title")
            if not title:
                continue

            uuid = item.get("id") or content.get("id") or ""
            if not uuid or uuid in seen_uuids:
                continue
            seen_uuids.add(uuid)

            # Publish time: ISO string in new format, epoch int in old
            publish_time = content.get("pubDate") or content.get("displayTime")
            if not publish_time:
                raw_ts = content.get("providerPublishTime")
                if raw_ts and isinstance(raw_ts, (int, float)):
                    publish_time = datetime.fromtimestamp(raw_ts, tz=timezone.utc).isoformat()

            # Publisher name
            provider = content.get("provider")
            publisher = (
                provider.get("displayName", "") if isinstance(provider, dict)
                else content.get("publisher", "")
            )

            # Article URL
            canonical = content.get("canonicalUrl")
            article_url = (
                canonical.get("url") if isinstance(canonical, dict)
                else content.get("link")
            )

            # Thumbnail
            thumbnail = content.get("thumbnail")
            image_url = None
            if isinstance(thumbnail, dict):
                resolutions = thumbnail.get("resolutions", [])
                if resolutions and isinstance(resolutions[0], dict):
                    image_url = resolutions[0].get("url")

            articles.append(
                {
                    "id": uuid,
                    "title": title,
                    "text": None,
                    "article_url": article_url,
                    "published_at": publish_time or "",
                    "source": {
                        "name": publisher,
                        "logo_url": None,
                        "homepage_url": None,
                        "favicon_url": None,
                    },
                    "tickers": [sym],
                    "image_url": image_url,
                    "author": None,
                    "description": content.get("summary") or content.get("description"),
                    "keywords": [],
                    "sentiments": [],
                }
            )

    return {
        "results": articles,
        "count": len(articles),
        "next_cursor": None,
    }


class YFinanceNewsSource:
    """News data source backed by Yahoo Finance (yfinance library)."""

    async def get_news(
        self,
        tickers: list[str] | None = None,
        limit: int = 10,
        published_after: str | None = None,
        published_before: str | None = None,
        cursor: str | None = None,
        order: str | None = None,
        sort: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        ignored = {k: v for k, v in {"published_after": published_after,
                   "published_before": published_before, "cursor": cursor,
                   "order": order, "sort": sort}.items() if v is not None}
        if ignored:
            logger.debug("yfinance.news: ignoring unsupported params: %s", ignored)
        return await asyncio.to_thread(_fetch_news, tickers, limit)

    async def get_news_article(
        self,
        article_id: str,
        user_id: str | None = None,
    ) -> dict[str, Any] | None:
        return None  # yfinance has no article detail endpoint

    async def close(self) -> None:
        pass
