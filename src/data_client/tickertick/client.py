"""Async client for the TickerTick news API (free, no API key).

Base endpoint is ``{BASE}/feed?q=<query>&n=<limit>[&last=<id>]`` where ``q``
uses TickerTick's query language (``T:curated``, ``tt:<ticker>``,
``(or tt:A tt:B)``, ``s:<source>``, ``E:<entity>``). Stories carry ``time`` as
epoch **milliseconds**; we convert it to ISO-8601 UTC so the rest of the news
stack sees the same ``published_at`` shape as the other providers.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import httpx


def _convert_timestamps(data: dict[str, Any]) -> dict[str, Any]:
    """Convert each story's millisecond ``time`` to an ISO-8601 UTC string in place."""
    # TickerTick is an unowned upstream; a 200 with an unexpected body (list,
    # string, …) shouldn't crash the endpoint — treat it as an empty feed.
    if not isinstance(data, dict):
        return {"stories": []}
    for story in data.get("stories", []):
        if not isinstance(story, dict):
            continue
        ts = story.get("time")
        if isinstance(ts, (int, float)):
            story["time"] = datetime.fromtimestamp(ts / 1000, timezone.utc).isoformat()
    return data


class TickerTickClient:
    """Async client for TickerTick's ``/feed`` endpoint."""

    BASE_URL = os.getenv("TICKERTICK_BASE_URL", "https://api.tickertick.com")

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self) -> TickerTickClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def get_feed(
        self, query: str, limit: int = 30, last_id: str | None = None
    ) -> dict[str, Any]:
        """Fetch a feed for *query*. Returns ``{"stories": [...]}`` (timestamps ISO-normalized)."""
        params: dict[str, Any] = {"q": query, "n": limit}
        if last_id:
            params["last"] = last_id

        client = await self._get_client()
        try:
            response = await client.get(f"{self.BASE_URL}/feed", params=params)
            response.raise_for_status()
            return _convert_timestamps(response.json())
        except httpx.HTTPStatusError as e:
            raise Exception(f"TickerTick request failed: {e}") from e
        except httpx.TimeoutException as e:
            raise Exception(f"TickerTick request timed out: {e}") from e
        except httpx.RequestError as e:
            raise Exception(f"TickerTick request failed: {e}") from e
