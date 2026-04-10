"""
EODHD (eodhistoricaldata.com) API Client
Central client for all EODHD API calls with caching and error handling.
"""

import json
import logging
import os
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import httpx

logger = logging.getLogger(__name__)

_CACHE_MAX_SIZE = 512


class EODHDClient:
    """Async client for EODHD API with connection pooling and LRU caching."""

    BASE_URL = "https://eodhd.com/api"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_ttl: int = 300,
        default_exchange: str = "US",
    ):
        self.api_key = api_key or os.getenv("EODHD_API_KEY")
        if not self.api_key:
            raise ValueError(
                "EODHD API key required. Set EODHD_API_KEY env var or pass api_key."
            )
        self.cache_ttl = cache_ttl
        self.default_exchange = default_exchange
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._cache_timestamps: Dict[str, datetime] = {}
        self._exchange_cache: Dict[str, str] = {}  # symbol → exchange

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                http2=True,
                timeout=30.0,
                limits=httpx.Limits(max_keepalive_connections=10),
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    # ------------------------------------------------------------------
    # Symbol normalisation
    # ------------------------------------------------------------------

    def _eodhd_symbol(self, symbol: str) -> str:
        """Convert bare ticker to EODHD format (TICKER.EXCHANGE).

        If the symbol already contains a dot (e.g. ``600519.SS``),
        it is returned as-is.  Otherwise the default exchange suffix
        is appended (e.g. ``AAPL`` → ``AAPL.US``).

        For resolved symbols (via :meth:`_resolve_exchange`), the cache
        is consulted first.
        """
        if "." in symbol:
            return symbol
        # Check if we've previously resolved a non-US exchange
        cached = self._exchange_cache.get(symbol)
        if cached:
            return f"{symbol}.{cached}"
        return f"{symbol}.{self.default_exchange}"

    async def _resolve_exchange(self, symbol: str) -> str:
        """Search EODHD for the primary exchange of *symbol*.

        Called when the default ``.US`` suffix returns a 404.
        Caches the result so subsequent calls are instant.
        """
        try:
            data = await self._make_request(
                f"search/{symbol}", use_cache=True
            )
            if isinstance(data, list):
                # Prefer primary listing
                for entry in data:
                    if entry.get("Code") == symbol and entry.get("isPrimary"):
                        exchange = entry["Exchange"]
                        self._exchange_cache[symbol] = exchange
                        return f"{symbol}.{exchange}"
                # Fall back to first exact code match
                for entry in data:
                    if entry.get("Code") == symbol:
                        exchange = entry["Exchange"]
                        self._exchange_cache[symbol] = exchange
                        return f"{symbol}.{exchange}"
        except Exception:
            logger.debug("EODHD search fallback failed for %s", symbol)
        return f"{symbol}.{self.default_exchange}"

    # ------------------------------------------------------------------
    # Caching helpers
    # ------------------------------------------------------------------

    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key not in self._cache_timestamps:
            return False
        cached_time = self._cache_timestamps[cache_key]
        return (datetime.now() - cached_time).total_seconds() < self.cache_ttl

    # ------------------------------------------------------------------
    # Core request
    # ------------------------------------------------------------------

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> Union[Dict, List]:
        params = params or {}
        params["api_token"] = self.api_key
        params["fmt"] = "json"

        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"

        if use_cache and self._is_cache_valid(cache_key):
            self._cache.move_to_end(cache_key)
            return self._cache[cache_key]

        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        client = await self._get_client()

        try:
            response = await client.get(url, params=params)
            # 404 = unknown symbol — return empty rather than raising
            if response.status_code == 404:
                logger.debug("EODHD 404 for %s — symbol not found", endpoint)
                return {}
            response.raise_for_status()
            data = response.json()

            if use_cache and data:
                self._cache[cache_key] = data
                self._cache_timestamps[cache_key] = datetime.now()
                while len(self._cache) > _CACHE_MAX_SIZE:
                    oldest_key, _ = self._cache.popitem(last=False)
                    self._cache_timestamps.pop(oldest_key, None)

            return data

        except httpx.HTTPStatusError as e:
            raise Exception(f"EODHD API request failed: {e}")
        except httpx.TimeoutException as e:
            raise Exception(f"EODHD API request timed out: {e}")
        except httpx.RequestError as e:
            raise Exception(f"EODHD API request failed: {e}")

    # ------------------------------------------------------------------
    # Fundamentals (single endpoint, filter-based)
    # ------------------------------------------------------------------

    async def get_fundamentals(
        self, symbol: str, filters: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch fundamentals for *symbol*.

        ``filters`` is a comma-separated string of section paths, e.g.
        ``"General"`` or ``"Financials::Income_Statement::quarterly"``.

        If the default exchange suffix returns an empty result (404),
        the client searches EODHD for the correct exchange and retries.
        """
        sym = self._eodhd_symbol(symbol)
        params: Dict[str, Any] = {}
        if filters:
            params["filter"] = filters
        data = await self._make_request(f"fundamentals/{sym}", params=params)

        # If empty (404) and symbol had no explicit exchange, try resolving
        if not data and "." not in symbol:
            resolved = await self._resolve_exchange(symbol)
            if resolved != sym:
                data = await self._make_request(
                    f"fundamentals/{resolved}", params=params
                )

        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------

    async def get_profile(self, symbol: str) -> Dict[str, Any]:
        return await self.get_fundamentals(symbol, "General")

    async def get_highlights(self, symbol: str) -> Dict[str, Any]:
        return await self.get_fundamentals(symbol, "Highlights")

    async def get_valuation(self, symbol: str) -> Dict[str, Any]:
        return await self.get_fundamentals(symbol, "Valuation")

    async def get_technicals(self, symbol: str) -> Dict[str, Any]:
        return await self.get_fundamentals(symbol, "Technicals")

    async def get_analyst_ratings(self, symbol: str) -> Dict[str, Any]:
        return await self.get_fundamentals(symbol, "AnalystRatings")

    async def get_income_statement(
        self, symbol: str, period: str = "quarterly", limit: int = 8
    ) -> List[Dict[str, Any]]:
        """Return income statements as a list of dicts (newest first)."""
        raw = await self.get_fundamentals(
            symbol, f"Financials::Income_Statement::{period}"
        )
        return self._dict_to_list(raw, limit)

    async def get_cash_flow(
        self, symbol: str, period: str = "quarterly", limit: int = 8
    ) -> List[Dict[str, Any]]:
        raw = await self.get_fundamentals(
            symbol, f"Financials::Cash_Flow::{period}"
        )
        return self._dict_to_list(raw, limit)

    async def get_balance_sheet(
        self, symbol: str, period: str = "quarterly", limit: int = 8
    ) -> List[Dict[str, Any]]:
        raw = await self.get_fundamentals(
            symbol, f"Financials::Balance_Sheet::{period}"
        )
        return self._dict_to_list(raw, limit)

    async def get_earnings_history(
        self, symbol: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        raw = await self.get_fundamentals(symbol, "Earnings::History")
        return self._dict_to_list(raw, limit)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dict_to_list(
        data: Any, limit: int | None = None
    ) -> List[Dict[str, Any]]:
        """Convert EODHD's dict-keyed-by-date to a list sorted newest-first."""
        if not data or not isinstance(data, dict):
            return []
        items = list(data.values())
        # Sort by 'date' descending if present
        try:
            items.sort(key=lambda x: x.get("date", ""), reverse=True)
        except (TypeError, AttributeError):
            pass
        if limit:
            items = items[:limit]
        return items
