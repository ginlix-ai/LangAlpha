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
    _MAX_PAGES = 10

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
        sort: str = "desc",
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch OHLCV bars for a single symbol, auto-paginating if needed.

        ``GET /api/v1/data/aggregates/{market}/{symbol}``

        When the upstream response contains a ``next_cursor``, follows it
        automatically (up to ``_MAX_PAGES`` total requests) so the caller
        always receives the complete result set.

        ``sort`` defaults to ``"desc"`` so page-ceiling truncation drops the
        oldest bars rather than the recent tail. Results are sorted back to
        ascending before return.

        Returns ``(results, truncated)`` where *truncated* is ``True`` when
        the page ceiling was hit while more data was available.
        """
        params: dict[str, Any] = {
            "timespan": timespan,
            "multiplier": multiplier,
            "limit": limit,
            "sort": sort,
        }
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        all_results: list[dict[str, Any]] = []
        headers = self._user_headers(user_id)
        url = f"/api/v1/data/aggregates/{market}/{symbol}"
        truncated = False

        for page in range(self._MAX_PAGES):
            resp = await self.http.get(url, params=params, headers=headers)
            resp.raise_for_status()
            body = resp.json()
            results = body.get("results", [])
            all_results.extend(results)

            cursor = body.get("next_cursor")
            if not cursor or not results:
                break

            logger.info(
                "get_aggregates %s %s: page %d returned %d bars, following cursor",
                symbol, timespan, page + 1, len(results),
            )
            # Next page: carry same params but add cursor
            params["cursor"] = cursor
        else:
            # Loop exhausted without break — max pages hit with more data available
            if cursor:
                truncated = True
                logger.warning(
                    "get_aggregates %s %s: hit %d-page ceiling, data truncated",
                    symbol, timespan, self._MAX_PAGES,
                )

        # Sort ascending; callers (lightweight-charts, cache watermark, delta-merge) depend on it.
        if sort == "desc":
            all_results.sort(key=lambda b: b.get("time", 0))

        return all_results, truncated

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

    async def get_options_contracts(
        self,
        underlying_ticker: str,
        contract_type: str | None = None,
        expiration_date: str | None = None,
        expiration_date_gte: str | None = None,
        expiration_date_lte: str | None = None,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
        order: str | None = None,
        sort: str | None = None,
        limit: int = 10,
        cursor: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch options contracts for an underlying ticker.

        ``GET /api/v1/data/options/contracts``
        """
        params: dict[str, Any] = {
            "underlying_ticker": underlying_ticker,
            "limit": limit,
        }
        if contract_type:
            params["contract_type"] = contract_type
        if expiration_date:
            params["expiration_date"] = expiration_date
        if expiration_date_gte:
            params["expiration_date.gte"] = expiration_date_gte
        if expiration_date_lte:
            params["expiration_date.lte"] = expiration_date_lte
        if strike_price_gte is not None:
            params["strike_price.gte"] = strike_price_gte
        if strike_price_lte is not None:
            params["strike_price.lte"] = strike_price_lte
        if order:
            params["order"] = order
        if sort:
            params["sort"] = sort
        if cursor:
            params["cursor"] = cursor

        resp = await self.http.get(
            "/api/v1/data/options/contracts",
            params=params,
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        return resp.json()

    async def get_option_contract(
        self,
        options_ticker: str,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch a single options contract by ticker.

        ``GET /api/v1/data/options/contracts/{options_ticker}``
        """
        resp = await self.http.get(
            f"/api/v1/data/options/contracts/{options_ticker}",
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        return resp.json()

    async def get_short_interest(
        self,
        symbol: str,
        limit: int = 500,
        sort: str = "settlement_date.asc",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch short interest data for a symbol.

        ``GET /api/v1/data/stocks/short-interest``

        Results are returned ascending by settlement date (default sort).
        Use ``[-1]`` for the latest.
        """
        resp = await self.http.get(
            "/api/v1/data/stocks/short-interest",
            params={"ticker": symbol, "limit": limit, "sort": sort},
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        body = resp.json()
        return body.get("results", [])

    async def get_short_volume(
        self,
        symbol: str,
        limit: int = 500,
        sort: str = "date.asc",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch short volume data for a symbol.

        ``GET /api/v1/data/stocks/short-volume``

        Results are returned ascending by date (default sort).
        Use ``[-1]`` for the latest.
        """
        resp = await self.http.get(
            "/api/v1/data/stocks/short-volume",
            params={"ticker": symbol, "limit": limit, "sort": sort},
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        body = resp.json()
        return body.get("results", [])

    async def get_float(
        self,
        symbol: str,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch float data for a symbol.

        ``GET /api/v1/data/stocks/float``

        Returns the first result dict (or empty dict if none).
        """
        resp = await self.http.get(
            "/api/v1/data/stocks/float",
            params={"ticker": symbol},
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        body = resp.json()
        results = body.get("results", []) if isinstance(body, dict) else []
        return results[0] if results else {}

    async def get_movers(
        self,
        direction: str = "gainers",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch market movers (gainers or losers).

        ``GET /api/v1/data/snapshots/movers/{direction}``
        """
        resp = await self.http.get(
            f"/api/v1/data/snapshots/movers/{direction}",
            headers=self._user_headers(user_id),
        )
        resp.raise_for_status()
        body = resp.json()
        return body.get("results", [])

    async def close(self) -> None:
        await self.http.aclose()
