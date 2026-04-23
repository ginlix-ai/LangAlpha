"""Daily EOD stock data caching with envelope metadata and incremental delta refresh.

Same envelope/delta pattern as IntradayCacheService, simplified for daily granularity:
- Single interval (1day) with its own TTL from config.
- Watermark is a date string (YYYY-MM-DD).
- Market hours gating: no refresh when market is fully closed.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

from src.config.settings import get_ohlcv_ttl
from src.data_client import get_market_data_provider
from src.server.services.cache._ohlcv_envelope import (
    _EMPTY_RESULT_TTL,
    _build_envelope,
    _is_stale_date,
    _merge_bars,
    _needs_refresh,
    _parse_envelope,
    watermark_to_date_str,
)
from src.utils.cache.redis_cache import get_cache_client
from src.utils.market_hours import current_market_phase, seconds_until_next_open

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cache key builder
# ---------------------------------------------------------------------------

class DailyCacheKeyBuilder:
    """Build cache keys for daily OHLCV data."""

    PREFIX = "ohlcv"

    @classmethod
    def _is_live(cls, to_date: Optional[str]) -> bool:
        if to_date is None:
            return True
        try:
            return to_date >= date.today().strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return True

    @classmethod
    def daily_key(
        cls,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        source: Optional[str] = None,
        is_index: bool = False,
    ) -> str:
        symbol = symbol.upper()
        src = f"{source}:" if source else ""
        market = "index" if is_index else "stock"
        if cls._is_live(to_date):
            return f"{cls.PREFIX}:{src}{market}:{symbol}:1day"
        return f"{cls.PREFIX}:{src}{market}:{symbol}:1day:{from_date}:{to_date}"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class DailyFetchResult:
    """Result of a daily data fetch operation."""

    symbol: str
    data: List[Dict[str, Any]]
    cached: bool
    ttl_remaining: Optional[int]
    background_refresh_triggered: bool
    cache_key: Optional[str] = None
    watermark: Optional[int] = None
    complete: Optional[bool] = None
    market_phase: Optional[str] = None
    truncated: Optional[bool] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class DailyCacheService:
    """Singleton service for cached daily EOD stock data with delta refresh."""

    _instance: Optional["DailyCacheService"] = None
    _refresh_locks: Dict[str, asyncio.Lock]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._refresh_locks = {}
        return cls._instance

    @classmethod
    def get_instance(cls) -> "DailyCacheService":
        return cls()

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _base_ttl() -> int:
        return get_ohlcv_ttl("1day")

    @staticmethod
    def _effective_ttl(base_ttl: int, complete: bool) -> int:
        if complete:
            secs = seconds_until_next_open()
            return max(base_ttl, secs) if secs > 0 else base_ttl
        return base_ttl

    def _get_refresh_lock(self, cache_key: str) -> asyncio.Lock:
        if cache_key not in self._refresh_locks:
            self._refresh_locks[cache_key] = asyncio.Lock()
        return self._refresh_locks[cache_key]

    async def _fetch_data(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        is_index: bool = False,
        user_id: Optional[str] = None,
    ) -> tuple:
        """Fetch daily data and return ``(bars, source_name, truncated)``."""
        provider = await get_market_data_provider()
        data, source, truncated = await provider.get_daily_with_source(
            symbol=symbol, from_date=from_date, to_date=to_date,
            is_index=is_index, user_id=user_id,
        )
        return data, source, truncated

    async def _find_cached(
        self, symbol: str, from_date: Optional[str], to_date: Optional[str],
        is_index: bool = False,
    ) -> tuple:
        """Try cache lookup across all known data sources.

        Returns ``(cache_key, envelope)`` on hit, ``(None, None)`` on miss.
        """
        cache = get_cache_client()
        provider = await get_market_data_provider()
        for source in provider.source_names:
            key = DailyCacheKeyBuilder.daily_key(symbol, from_date, to_date, source=source, is_index=is_index)
            raw = await cache.get(key)
            envelope = _parse_envelope(raw) if raw else None
            if envelope is not None:
                return key, envelope
        return None, None

    # -- delta refresh ----------------------------------------------------

    async def _delta_refresh(
        self,
        cache_key: str,
        symbol: str,
        is_index: bool = False,
        user_id: Optional[str] = None,
    ) -> None:
        lock = self._get_refresh_lock(cache_key)
        if lock.locked():
            return

        async with lock:
            try:
                cache = get_cache_client()
                raw = await cache.get(cache_key)
                envelope = _parse_envelope(raw) if raw else None

                phase = current_market_phase()
                closed = phase == "closed"

                if envelope and envelope.get("complete") and closed:
                    return

                watermark = envelope["watermark"] if envelope else None
                existing_bars = envelope["bars"] if envelope else []

                if envelope and envelope.get("truncated"):
                    # Truncated base — full re-fetch instead of delta
                    delta, _source, truncated = await self._fetch_data(symbol, from_date=None, to_date=None, is_index=is_index, user_id=user_id)
                    merged = delta
                else:
                    # Normal delta refresh
                    # Convert watermark (Unix ms) to date string for API from_date param
                    delta_from = watermark_to_date_str(watermark)

                    delta, _source, truncated = await self._fetch_data(symbol, from_date=delta_from, to_date=None, is_index=is_index, user_id=user_id)

                    if watermark and existing_bars:
                        merged = _merge_bars(existing_bars, delta, watermark)
                    else:
                        merged = delta

                complete = closed and len(merged) > 0
                base = self._base_ttl()
                eff = self._effective_ttl(base, complete)
                env = _build_envelope(merged, phase, complete, stored_ttl=eff, truncated=truncated)
                await cache.set(cache_key, env, ttl=eff)

                logger.debug(
                    f"Daily delta refresh for {cache_key}: "
                    f"fetched {len(delta)}, total {len(merged)}, "
                    f"phase={phase}, complete={complete}"
                )

            except Exception as e:
                logger.warning(f"Daily delta refresh failed for {cache_key}: {e}")

    # -- public API -------------------------------------------------------

    async def get_stock_daily(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        is_index: bool = False,
        user_id: Optional[str] = None,
    ) -> DailyFetchResult:
        normalized = symbol.lstrip("^").upper()

        base_ttl = self._base_ttl()
        cache = get_cache_client()
        phase = current_market_phase()

        # --- Try cache (across all known sources) ---
        cache_key, envelope = await self._find_cached(normalized, from_date, to_date, is_index=is_index)
        is_live = DailyCacheKeyBuilder._is_live(to_date)

        if envelope is not None:
            bars = envelope["bars"]
            watermark = envelope.get("watermark")
            complete = envelope.get("complete", False)
            stored_ttl = envelope.get("stored_ttl", 0)
            elapsed = time.time() - envelope.get("fetched_at", 0)
            ttl_remaining = max(0, int(stored_ttl - elapsed)) if stored_ttl else None

            bg_triggered = False
            # Historical daily envelopes skip live-only staleness checks but
            # still participate in truncated/soft-TTL refresh via is_live.
            if _needs_refresh(envelope, base_ttl, interval="1day", is_live=is_live):
                if is_live and (_is_stale_date(envelope) or envelope.get("complete")):
                    # Stale date or day-boundary transition → sync re-fetch
                    logger.info("Daily cache %s: stale/complete → sync re-fetch", normalized)
                    envelope = None
                else:
                    # Normal SWR: return stale bars, refresh in background.
                    bg_triggered = True
                    asyncio.create_task(
                        self._delta_refresh(cache_key, normalized, is_index=is_index, user_id=user_id)
                    )

            if envelope is not None:
                return DailyFetchResult(
                    symbol=normalized,
                    data=bars,
                    cached=True,
                    ttl_remaining=ttl_remaining,
                    background_refresh_triggered=bg_triggered,
                    cache_key=cache_key,
                    watermark=watermark,
                    complete=complete,
                    market_phase=phase,
                    truncated=envelope.get("truncated"),
                )

        # --- Cache miss: full fetch ---
        try:
            data, source, truncated = await self._fetch_data(normalized, from_date, to_date, is_index=is_index, user_id=user_id)
            cache_key = DailyCacheKeyBuilder.daily_key(normalized, from_date, to_date, source=source, is_index=is_index)

            closed = phase == "closed"
            complete = closed and len(data) > 0
            eff_ttl = self._effective_ttl(base_ttl, complete)
            if not data:
                eff_ttl = _EMPTY_RESULT_TTL
            env = _build_envelope(data, phase, complete, stored_ttl=eff_ttl, truncated=truncated)

            await cache.set(cache_key, env, ttl=eff_ttl)

            return DailyFetchResult(
                symbol=normalized,
                data=data,
                cached=False,
                ttl_remaining=eff_ttl,
                background_refresh_triggered=False,
                cache_key=cache_key,
                watermark=env["watermark"],
                complete=complete,
                market_phase=phase,
                truncated=truncated,
            )

        except Exception as e:
            logger.error(f"Failed to fetch daily data for {symbol}: {e}")
            return DailyFetchResult(
                symbol=normalized,
                data=[],
                cached=False,
                ttl_remaining=None,
                background_refresh_triggered=False,
                market_phase=phase,
                error=str(e),
            )
