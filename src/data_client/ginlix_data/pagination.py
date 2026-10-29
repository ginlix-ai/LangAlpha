"""Shared cursor-based pagination for ginlix-data endpoints."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

FetchPage = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


async def follow_cursor(
    fetch_page: FetchPage,
    params: dict[str, Any],
    *,
    limit: int | None = None,
    max_pages: int = 10,
    label: str = "ginlix_data",
) -> tuple[list[dict[str, Any]], bool]:
    """Every ``results`` row a cursor walk reads, and whether more was left unread.

    Later pages resend every first-page param beside ``cursor``: some routes
    reject a bare cursor. The walk ends at the last page, an empty page or
    *limit* rows, and is cut short (``truncated``) by a cursor that does not
    advance or by *max_pages*.
    """
    rows: list[dict[str, Any]] = []
    params = dict(params)  # avoid mutating caller's dict
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        body = await fetch_page(params)
        results = body.get("results") or []
        rows.extend(results)
        cursor = body.get("next_cursor")
        if not cursor or not results or (limit is not None and len(rows) >= limit):
            return rows[:limit], False
        if cursor in seen:
            # The cursor did not advance: following it only re-reads this page.
            logger.warning("%s: cursor repeated on page %d, stopping", label, page)
            return rows[:limit], True
        seen.add(cursor)
        logger.info("%s: page %d returned %d rows, following cursor", label, page, len(results))
        params["cursor"] = cursor
    logger.warning("%s: hit %d-page ceiling, data truncated", label, max_pages)
    return rows[:limit], True


async def paginate_cursor(
    fetch_page: FetchPage,
    params: dict[str, Any],
    limit: int,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    """Up to *limit* rows of a cursor walk, for a caller with no use for truncation."""
    rows, _ = await follow_cursor(fetch_page, params, limit=limit, max_pages=max_pages)
    return rows


def unique_by_time(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop repeated bars by ``time``, keeping the first; bars with no time stay.

    Pages that overlap (or a cursor that does not advance) would otherwise
    hand a chart two bars for one timestamp.
    """
    seen: set[Any] = set()
    out: list[dict[str, Any]] = []
    for bar in bars:
        t = bar.get("time")
        if t is not None:
            if t in seen:
                continue
            seen.add(t)
        out.append(bar)
    return out
