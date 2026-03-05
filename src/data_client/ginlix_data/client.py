"""REST client for ginlix-data aggregates API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class GinlixDataClient:
    """Low-level httpx client for ``GET /api/v1/data/aggregates``."""

    def __init__(self, base_url: str, service_token: str = ""):
        self.base_url = base_url.rstrip("/")
        headers: dict[str, str] = {}
        if service_token:
            headers["X-Service-Token"] = service_token
        self.http = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=30.0,
        )

    def _user_headers(self, user_id: str | None) -> dict[str, str]:
        """Build per-request headers with the caller's user ID."""
        if user_id:
            return {"X-User-Id": user_id}
        return {}

    # Maximum pages to follow when auto-paginating (safety bound).
    _MAX_PAGES = 5

    async def get_aggregates(
        self,
        market: str,
        symbol: str,
        timespan: str = "day",
        multiplier: int = 1,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 5000,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch OHLCV bars for a single symbol, auto-paginating if needed.

        ``GET /api/v1/data/aggregates/{market}/{symbol}``

        When the upstream response contains a ``next_cursor``, follows it
        automatically (up to ``_MAX_PAGES`` total requests) so the caller
        always receives the complete result set.
        """
        params: dict[str, Any] = {
            "timespan": timespan,
            "multiplier": multiplier,
            "limit": limit,
        }
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        all_results: list[dict[str, Any]] = []
        headers = self._user_headers(user_id)
        url = f"/api/v1/data/aggregates/{market}/{symbol}"

        for page in range(self._MAX_PAGES):
            resp = await self.http.get(url, params=params, headers=headers)
            resp.raise_for_status()
            body = resp.json()
            results = body.get("results", [])
            all_results.extend(results)

            cursor = body.get("next_cursor")
            if not cursor or len(results) < limit:
                break

            logger.info(
                "get_aggregates %s %s: page %d returned %d bars, following cursor",
                symbol, timespan, page + 1, len(results),
            )
            # Next page: carry same params but add cursor
            params["cursor"] = cursor

        return all_results

    async def get_news(
        self,
        ticker: str | None = None,
        limit: int = 20,
        published_after: str | None = None,
        published_before: str | None = None,
        cursor: str | None = None,
        order: str | None = None,
        sort: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch news articles.

        ``GET /api/v1/data/news``
        """
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if published_after:
            params["published_utc.gte"] = published_after
        if published_before:
            params["published_utc.lte"] = published_before
        if cursor:
            params["cursor"] = cursor
        if order:
            params["order"] = order
        if sort:
            params["sort"] = sort

        resp = await self.http.get(
            "/api/v1/data/news",
            params=params,
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        return resp.json()

    async def get_snapshots(
        self,
        asset_type: str,
        symbols: list[str],
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch batch snapshots for multiple symbols.

        ``GET /api/v1/data/snapshots/{asset_type}?symbols=AAPL,TSLA``

        Returns the ``results`` array from the response envelope.
        """
        resp = await self.http.get(
            f"/api/v1/data/snapshots/{asset_type}",
            params={"symbols": ",".join(symbols)},
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        body = resp.json()
        # Response envelope: {"request_id": ..., "status": ..., "results": [...]}
        if isinstance(body, dict):
            return body.get("results", [])
        return body

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch current market status.

        ``GET /api/v1/data/marketstatus/now``
        """
        resp = await self.http.get(
            "/api/v1/data/marketstatus/now",
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self) -> None:
        await self.http.aclose()
