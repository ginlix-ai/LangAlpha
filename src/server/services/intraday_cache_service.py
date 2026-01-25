"""
Intraday data caching service with SWR (Stale-While-Revalidate) pattern.

This service provides cached access to FMP intraday chart data with:
- Redis caching with 60-second TTL
- SWR pattern for background refresh
- Batch fetching with concurrency control
- Lock-based deduplication for background refreshes
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from src.utils.cache.redis_cache import get_cache_client
from src.config.settings import get_nested_config
from src.data_client.fmp.fmp_client import FMPClient

logger = logging.getLogger(__name__)


class IntradayCacheKeyBuilder:
    """Build cache keys for intraday data."""

    PREFIX = "fmp:intraday"

    @classmethod
    def stock_key(cls, symbol: str, interval: str = "1min", from_date: Optional[str] = None, to_date: Optional[str] = None) -> str:
        """Build cache key for stock intraday data."""
        key = f"{cls.PREFIX}:stock:symbol={symbol.upper()}:interval={interval}"
        if from_date:
            key += f":from={from_date}"
        if to_date:
            key += f":to={to_date}"
        return key

    @classmethod
    def index_key(cls, symbol: str, interval: str = "1min", from_date: Optional[str] = None, to_date: Optional[str] = None) -> str:
        """Build cache key for index intraday data."""
        # Normalize index symbols (e.g., ^GSPC -> GSPC)
        normalized_symbol = symbol.lstrip("^").upper()
        key = f"{cls.PREFIX}:index:symbol={normalized_symbol}:interval={interval}"
        if from_date:
            key += f":from={from_date}"
        if to_date:
            key += f":to={to_date}"
        return key


@dataclass
class IntradayFetchResult:
    """Result of an intraday data fetch operation."""
    symbol: str
    interval: str
    data: List[Dict[str, Any]]
    cached: bool
    ttl_remaining: Optional[int]
    background_refresh_triggered: bool
    error: Optional[str] = None


class IntradayCacheService:
    """
    Singleton service for cached intraday data access.

    Implements SWR pattern with:
    - 60-second TTL for 1-minute data
    - 0.5 soft TTL ratio (refresh triggers at 30s remaining)
    - Non-blocking background refresh with deduplication
    """

    _instance: Optional["IntradayCacheService"] = None
    _refresh_locks: Dict[str, asyncio.Lock]  # Per-key locks for refresh deduplication
    _max_concurrent_fetches: int = 10  # Semaphore limit for batch requests

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._refresh_locks = {}
            cls._instance._semaphore = asyncio.Semaphore(cls._max_concurrent_fetches)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "IntradayCacheService":
        """Get singleton instance."""
        return cls()

    def _get_ttl(self) -> int:
        """Get TTL from config, default 60 seconds."""
        return get_nested_config("redis.ttl.intraday_1min", 60)

    def _get_soft_ttl_ratio(self) -> float:
        """Get SWR soft TTL ratio from config, default 0.5."""
        return get_nested_config("redis.swr.soft_ttl_ratio", 0.5)

    def _get_refresh_lock(self, cache_key: str) -> asyncio.Lock:
        """Get or create a lock for the given cache key."""
        if cache_key not in self._refresh_locks:
            self._refresh_locks[cache_key] = asyncio.Lock()
        return self._refresh_locks[cache_key]

    async def _fetch_from_fmp(
        self,
        symbol: str,
        is_index: bool,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch intraday data from FMP API."""
        # For indexes, ensure symbol has ^ prefix for FMP API
        api_symbol = symbol
        if is_index and not symbol.startswith("^"):
            api_symbol = f"^{symbol}"

        async with FMPClient() as client:
            data = await client.get_intraday_chart(
                symbol=api_symbol,
                interval=interval,
                from_date=from_date,
                to_date=to_date
            )
            return data if data else []

    async def _background_refresh(
        self,
        cache_key: str,
        symbol: str,
        is_index: bool,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> None:
        """
        Perform background refresh with lock-based deduplication.

        Only one refresh per key can run at a time.
        """
        lock = self._get_refresh_lock(cache_key)

        # Try to acquire lock without blocking
        if lock.locked():
            logger.debug(f"Background refresh already in progress for {cache_key}")
            return

        async with lock:
            try:
                logger.debug(f"Starting background refresh for {cache_key}")
                data = await self._fetch_from_fmp(symbol, is_index, interval, from_date, to_date)

                # Update cache
                cache = get_cache_client()
                ttl = self._get_ttl()
                await cache.set(cache_key, data, ttl=ttl)
                logger.debug(f"Background refresh completed for {cache_key}, {len(data)} points")

            except Exception as e:
                logger.warning(f"Background refresh failed for {cache_key}: {e}")

    async def get_stock_intraday(
        self,
        symbol: str,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> IntradayFetchResult:
        """
        Get stock intraday data with SWR caching.

        Args:
            symbol: Stock ticker symbol
            interval: Data interval (1min, 5min, 15min, 30min, 1hour, 4hour)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            IntradayFetchResult with data and cache metadata
        """
        return await self._get_intraday(
            symbol=symbol,
            is_index=False,
            interval=interval,
            from_date=from_date,
            to_date=to_date
        )

    async def get_index_intraday(
        self,
        symbol: str,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> IntradayFetchResult:
        """
        Get index intraday data with SWR caching.

        Args:
            symbol: Index symbol (with or without ^ prefix)
            interval: Data interval (1min, 5min, 1hour)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            IntradayFetchResult with data and cache metadata
        """
        return await self._get_intraday(
            symbol=symbol,
            is_index=True,
            interval=interval,
            from_date=from_date,
            to_date=to_date
        )

    async def _get_intraday(
        self,
        symbol: str,
        is_index: bool,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> IntradayFetchResult:
        """
        Internal method to get intraday data with SWR pattern.
        """
        # Build cache key
        if is_index:
            cache_key = IntradayCacheKeyBuilder.index_key(symbol, interval, from_date, to_date)
        else:
            cache_key = IntradayCacheKeyBuilder.stock_key(symbol, interval, from_date, to_date)

        # Normalize symbol for storage
        normalized_symbol = symbol.lstrip("^").upper()

        cache = get_cache_client()
        ttl = self._get_ttl()
        soft_ratio = self._get_soft_ttl_ratio()

        # Try to get from cache with SWR
        cached_data, needs_refresh = await cache.get_with_swr(
            key=cache_key,
            original_ttl=ttl,
            soft_ttl_ratio=soft_ratio
        )

        background_refresh_triggered = False

        if cached_data is not None:
            # Cache hit
            ttl_remaining = await cache.ttl(cache_key)
            ttl_remaining = max(0, ttl_remaining) if ttl_remaining > 0 else None

            if needs_refresh:
                # Trigger background refresh (non-blocking)
                background_refresh_triggered = True
                asyncio.create_task(
                    self._background_refresh(cache_key, normalized_symbol, is_index, interval, from_date, to_date)
                )

            return IntradayFetchResult(
                symbol=normalized_symbol,
                interval=interval,
                data=cached_data,
                cached=True,
                ttl_remaining=ttl_remaining,
                background_refresh_triggered=background_refresh_triggered
            )

        # Cache miss - fetch from API
        try:
            data = await self._fetch_from_fmp(normalized_symbol, is_index, interval, from_date, to_date)

            # Store in cache
            await cache.set(cache_key, data, ttl=ttl)

            return IntradayFetchResult(
                symbol=normalized_symbol,
                interval=interval,
                data=data,
                cached=False,
                ttl_remaining=ttl,
                background_refresh_triggered=False
            )

        except Exception as e:
            logger.error(f"Failed to fetch intraday data for {symbol}: {e}")
            return IntradayFetchResult(
                symbol=normalized_symbol,
                interval=interval,
                data=[],
                cached=False,
                ttl_remaining=None,
                background_refresh_triggered=False,
                error=str(e)
            )

    async def get_batch_stocks(
        self,
        symbols: List[str],
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
        """
        Get batch stock intraday data with concurrency control.

        Args:
            symbols: List of stock symbols (max 50)
            interval: Data interval (1min, 5min, 15min, 30min, 1hour, 4hour)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            Tuple of (results dict, errors dict, cache stats dict)
        """
        return await self._get_batch(
            symbols=symbols,
            is_index=False,
            interval=interval,
            from_date=from_date,
            to_date=to_date
        )

    async def get_batch_indexes(
        self,
        symbols: List[str],
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
        """
        Get batch index intraday data with concurrency control.

        Args:
            symbols: List of index symbols (max 50)
            interval: Data interval (1min, 5min, 1hour)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            Tuple of (results dict, errors dict, cache stats dict)
        """
        return await self._get_batch(
            symbols=symbols,
            is_index=True,
            interval=interval,
            from_date=from_date,
            to_date=to_date
        )

    async def _get_batch(
        self,
        symbols: List[str],
        is_index: bool,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
        """
        Optimized batch fetch with two-phase approach:
        1. Phase 1: Parallel cache lookups (no rate limiting needed)
        2. Phase 2: Semaphore-controlled API calls for cache misses only
        """
        results: Dict[str, List[Dict[str, Any]]] = {}
        errors: Dict[str, str] = {}
        cache_hits = 0
        background_refreshes = 0

        cache = get_cache_client()
        ttl = self._get_ttl()
        soft_ratio = self._get_soft_ttl_ratio()

        # Build symbol -> (normalized_symbol, cache_key) mapping
        symbol_info: Dict[str, Tuple[str, str]] = {}
        for symbol in symbols:
            normalized = symbol.lstrip("^").upper()
            if is_index:
                cache_key = IntradayCacheKeyBuilder.index_key(symbol, interval, from_date, to_date)
            else:
                cache_key = IntradayCacheKeyBuilder.stock_key(symbol, interval, from_date, to_date)
            symbol_info[symbol] = (normalized, cache_key)

        # Phase 1: Parallel cache lookups (no semaphore needed)
        cache_misses_symbols: List[str] = []

        async def check_cache(symbol: str) -> None:
            nonlocal cache_hits, background_refreshes
            normalized, cache_key = symbol_info[symbol]

            cached_data, needs_refresh = await cache.get_with_swr(
                key=cache_key, original_ttl=ttl, soft_ttl_ratio=soft_ratio
            )

            if cached_data is not None:
                results[normalized] = cached_data
                cache_hits += 1
                if needs_refresh:
                    background_refreshes += 1
                    asyncio.create_task(
                        self._background_refresh(cache_key, normalized, is_index, interval, from_date, to_date)
                    )
            else:
                cache_misses_symbols.append(symbol)

        await asyncio.gather(*[check_cache(s) for s in symbols])

        # Phase 2: Fetch cache misses with shared FMPClient and semaphore
        if cache_misses_symbols:
            async with FMPClient() as client:
                async def fetch_from_api(symbol: str) -> None:
                    normalized, cache_key = symbol_info[symbol]
                    api_symbol = f"^{normalized}" if is_index and not symbol.startswith("^") else normalized

                    async with self._semaphore:
                        try:
                            data = await client.get_intraday_chart(
                                symbol=api_symbol,
                                interval=interval,
                                from_date=from_date,
                                to_date=to_date
                            )
                            data = data if data else []
                            results[normalized] = data
                            # Store in cache (fire and forget)
                            asyncio.create_task(cache.set(cache_key, data, ttl=ttl))
                        except Exception as e:
                            logger.error(f"Failed to fetch {symbol}: {e}")
                            errors[normalized] = str(e)

                await asyncio.gather(*[fetch_from_api(s) for s in cache_misses_symbols])

        cache_stats = {
            "total_requests": len(symbols),
            "cache_hits": cache_hits,
            "cache_misses": len(cache_misses_symbols),
            "background_refreshes": background_refreshes
        }

        return results, errors, cache_stats
