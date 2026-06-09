"""News data source backed by TickerTick.

Unlike FMP/ginlix-data, TickerTick provides no article image and no sentiment,
so those fields are normalized to ``None``. With no tickers the source returns
TickerTick's curated top-news feed (``T:curated``); with tickers it uses broad
ticker queries (``tt:``) which include related-entity coverage.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .client import TickerTickClient

logger = logging.getLogger(__name__)

# Tickers flow into TickerTick's query DSL (``tt:<sym>``), so only let clean
# ticker tokens through — keeps a crafted ``?tickers=`` value from injecting DSL
# operators (e.g. ``") (or s:cnn"``) into the feed query.
_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-]{1,15}$")


def _build_query(tickers: list[str] | None) -> str:
    """Map a ticker list to a TickerTick feed query (ignoring non-ticker tokens)."""
    safe = [t for t in tickers or [] if _TICKER_RE.match(t)]
    if not safe:
        return "T:curated"
    if len(safe) == 1:
        return f"tt:{safe[0].lower()}"
    return "(or " + " ".join(f"tt:{t.lower()}" for t in safe) + ")"


def _normalize_story(raw: dict[str, Any]) -> dict[str, Any]:
    """Map a TickerTick story → common NewsArticle dict."""
    return {
        "id": str(raw.get("id", "")),
        "title": raw.get("title", ""),
        "author": None,
        "description": raw.get("description"),
        "published_at": raw.get("time", ""),  # already ISO from the client
        "article_url": raw.get("url", ""),
        "image_url": None,
        "source": {
            "name": raw.get("site", ""),
            "logo_url": None,
            "homepage_url": None,
            "favicon_url": raw.get("favicon_url"),
        },
        "tickers": [t.upper() for t in raw.get("tickers", [])],
        "keywords": raw.get("tags", []),
        "sentiments": None,
    }


class TickerTickNewsSource:
    """Fetches news from TickerTick (curated or ticker feeds)."""

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
        query = _build_query(tickers)
        async with TickerTickClient() as client:
            data = await client.get_feed(query, limit=limit, last_id=cursor)

        stories = data.get("stories", []) if isinstance(data, dict) else []
        results = [_normalize_story(s) for s in stories]
        # TickerTick paginates via ``last=<id>`` (older stories). Hand the last
        # story id back as the cursor when we filled the page, so the caller can
        # request the next page; a short page means we've reached the end.
        next_cursor = results[-1]["id"] if results and len(results) >= limit else None
        return {"results": results, "count": len(results), "next_cursor": next_cursor}

    async def get_news_article(
        self, article_id: str, user_id: str | None = None
    ) -> dict[str, Any] | None:
        """Best-effort lookup: scan the curated feed for a matching story id."""
        try:
            async with TickerTickClient() as client:
                data = await client.get_feed("T:curated", limit=50)
            for story in data.get("stories", []):
                normalized = _normalize_story(story)
                if normalized["id"] == article_id:
                    return normalized
        except Exception:
            logger.warning("news.tickertick.article_lookup_failed", exc_info=True)
        return None

    async def close(self) -> None:
        pass  # TickerTickClient is used as a context manager per-request
