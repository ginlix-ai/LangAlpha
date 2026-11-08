"""News data source backed by TickerTick.

Unlike FMP/ginlix-data, TickerTick provides no article image and no sentiment,
so those fields are normalized to ``None``. With no tickers the source returns
TickerTick's curated top-news feed (``T:curated``); with tickers it uses broad
ticker queries (``tt:``) which include related-entity coverage.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from .client import TickerTickClient

logger = logging.getLogger(__name__)

# Tickers flow into TickerTick's query DSL (``tt:<sym>``), so only let clean
# ticker tokens through — keeps a crafted ``?tickers=`` value from injecting DSL
# operators (e.g. ``") (or s:cnn"``) into the feed query.
_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-]{1,15}$")

# TickerTick's feed has no time filter, so a date window is served by paging
# older with `last=<id>` and cutting client-side. The feed is strictly newest
# first, so the first story older than the window ends the scan. The upstream
# allows 5 requests per 30 s per IP, shared with the curated poller, so the scan
# stops after two pages (the feed serves at most 150 stories per page); a deeper
# window is resumed through the cursor.
_WINDOW_PAGE_SIZE = 150
_WINDOW_MAX_PAGES = 2


def _parse_time(value: str | None) -> datetime | None:
    """Parse an ISO-8601 instant or date (naive reads as UTC); None when unparseable."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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


def _page(results: list[dict[str, Any]], next_cursor: str | None) -> dict[str, Any]:
    return {"results": results, "count": len(results), "next_cursor": next_cursor}


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
        after = _parse_time(published_after)
        before = _parse_time(published_before)
        windowed = after is not None or before is not None
        page_size = _WINDOW_PAGE_SIZE if windowed else limit
        results: list[dict[str, Any]] = []
        last_id = cursor
        async with TickerTickClient() as client:
            for page in range(_WINDOW_MAX_PAGES if windowed else 1):
                try:
                    data = await client.get_feed(
                        query, limit=page_size, last_id=last_id
                    )
                except Exception:
                    if page == 0:
                        raise
                    logger.warning(
                        "news.tickertick.window_scan_interrupted", exc_info=True
                    )
                    return _page(results, last_id)
                stories = data.get("stories", []) if isinstance(data, dict) else []
                for story in stories:
                    article = _normalize_story(story)
                    last_id = article["id"]
                    if windowed:
                        published = _parse_time(article["published_at"])
                        if published is None or (before and published > before):
                            continue
                        if after and published < after:
                            return _page(results, None)
                    results.append(article)
                    # TickerTick paginates via ``last=<id>`` (older stories); the
                    # last story id is the cursor for the next page.
                    if len(results) >= limit:
                        return _page(results, last_id)
                if len(stories) < page_size:
                    return _page(results, None)
        # Scan budget spent inside the window: resume from the last story read.
        return _page(results, last_id)

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
