"""Intraday OHLCV caching with envelope metadata and incremental delta refresh.

Key improvements over the previous flat-TTL, full-refetch approach:
- **Envelope** wraps bars with watermark / complete / market_phase / fetched_at.
- **Interval-aware TTL** (e.g. 5 s for 1 s bars, 90 s for 1 min bars).
- **Delta refresh** fetches only bars from the watermark onward, then merges.
- **Market hours gating** skips refresh when market is closed.
- **Date-free cache keys** for live queries enable cross-request sharing.

The delta-refresh / pinning / dual-read core is shared with DailyCacheService
via :class:`_SeriesCacheCore`; this module owns the intraday specifics —
interval-aware TTL, the structural discard classifier, and the batch API.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import get_ohlcv_ttl
from src.data_client import get_market_data_provider
from src.server.services.cache._instrument_clock import clock_for
from src.server.services.cache._ohlcv_envelope import (
    _EMPTY_RESULT_TTL,
    _build_envelope,
    _is_stale_date,
    _needs_refresh,
    is_watermark_stale,
    series_identity,
)
from src.server.services.cache._series_cache_core import _SeriesCacheCore, is_live_window
from src.utils.cache.redis_cache import get_cache_client


# Max gap tolerance between market open and first cached bar (10 min in ms).
# If the first bar is more than this after market open, the cache has a
# coverage gap and should be discarded for a full re-fetch.
_GAP_TOLERANCE_MS = 10 * 60 * 1000

# Large gap threshold (30 min in ms).  Gaps this big bypass the grace period
# and trigger immediate discard — they're almost certainly a cache issue, not
# persistent upstream behaviour.
_LARGE_GAP_TOLERANCE_MS = 30 * 60 * 1000

# Grace period (seconds) before discarding for a *small* coverage gap
# (between _GAP_TOLERANCE_MS and _LARGE_GAP_TOLERANCE_MS).  Prevents fetch
# storms when the upstream consistently returns partial data — a fresh
# envelope is served as-is and retried after the grace window expires.
_COVERAGE_GAP_GRACE_S = 10


def _should_discard_envelope(
    envelope: dict,
    interval: Optional[str] = None,
    elapsed: float = 0.0,
    gap_grace_s: float = 0.0,
    is_live: bool = True,
    clock=None,
) -> bool:
    """Return True if the cached envelope should be discarded for a sync re-fetch.

    Covers four cases (live envelopes only; historical envelopes are immutable
    snapshots and only evict at TTL):
    - Stale date: cached data is from a previous trading date.
    - Stale watermark: interval-aware — latest bar is behind the expected
      latest bar for *now*, even if the date still matches (mid-session
      stagnation or overnight freeze).
    - Day-boundary: cached as ``complete`` but market is now active.
    - Coverage gap: first bar is significantly after market open.

    ``is_live`` gates the stale-date + stale-watermark + day-boundary checks.
    Pass ``False`` for historical envelopes (cache keys with explicit
    ``:{from_date}:{to_date}`` suffix) — their date and watermark are
    intentionally in the past and must not trigger re-fetches.

    ``interval`` is optional for backward compatibility, but callers should
    pass it so the watermark-staleness check fires.

    The *elapsed* and *gap_grace_s* parameters gate the coverage-gap check:
    if the envelope was written less than *gap_grace_s* seconds ago the gap
    check is skipped to avoid fetch storms when the upstream consistently
    returns partial data.
    """
    clock = clock or clock_for(None)
    if is_live:
        if _is_stale_date(envelope, clock=clock):
            return True
        if interval and is_watermark_stale(envelope, interval, clock=clock):
            return True
        if envelope.get("complete") and not clock.is_closed():
            return True
    # Coverage gap: bars start well after market open.
    # Large gaps (>30 min) always discard immediately.  Small gaps (10-30 min)
    # respect the grace period to avoid fetch storms when the upstream
    # consistently returns partial data.
    # Runs for both live and historical envelopes, but is a no-op for
    # historical: ``first_bar_time`` is on a past trading day and ``open_ms``
    # is today's market open, so ``gap_ms`` is always negative and neither
    # threshold fires.
    bars = envelope.get("bars")
    if bars and not envelope.get("complete"):
        open_ms = clock.today_market_open_ms()
        if open_ms is not None:
            first_bar_time = bars[0].get("time", 0)
            gap_ms = first_bar_time - open_ms
            if gap_ms > _LARGE_GAP_TOLERANCE_MS:
                return True
            if gap_ms > _GAP_TOLERANCE_MS:
                if gap_grace_s > 0 and elapsed < gap_grace_s:
                    return False
                return True
    return False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cache key builder
# ---------------------------------------------------------------------------

class IntradayCacheKeyBuilder:
    """Build cache keys for intraday OHLCV data.

    Live queries (to_date is None or today) omit dates so that requests for
    different date ranges can share the same cached envelope.  Historical
    queries (to_date strictly in the past) embed both dates and get a long TTL.
    """

    PREFIX = "ohlcv"

    @classmethod
    def _is_live(cls, to_date: Optional[str]) -> bool:
        return is_live_window(to_date)

    @classmethod
    def stock_key(
        cls,
        symbol: str,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        source: Optional[str] = None,
    ) -> str:
        symbol = symbol.upper()
        src = f"{source}:" if source else ""
        if cls._is_live(to_date):
            return f"{cls.PREFIX}:{src}stock:{symbol}:{interval}"
        return f"{cls.PREFIX}:{src}stock:{symbol}:{interval}:{from_date}:{to_date}"

    @classmethod
    def index_key(
        cls,
        symbol: str,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        source: Optional[str] = None,
    ) -> str:
        normalized = symbol.removeprefix("I:").lstrip("^").upper()
        src = f"{source}:" if source else ""
        if cls._is_live(to_date):
            return f"{cls.PREFIX}:{src}index:{normalized}:{interval}"
        return f"{cls.PREFIX}:{src}index:{normalized}:{interval}:{from_date}:{to_date}"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class IntradayFetchResult:
    """Result of an intraday data fetch operation."""

    symbol: str
    interval: str
    data: List[Dict[str, Any]]
    cached: bool
    ttl_remaining: Optional[int]
    background_refresh_triggered: bool
    cache_key: Optional[str] = None
    watermark: Optional[int] = None
    complete: Optional[bool] = None
    market_phase: Optional[str] = None
    truncated: Optional[bool] = None
    # v4 envelope header (lineage), carried from the fetch so the router builds
    # the wire Series header without a second cache read.
    header: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class IntradayCacheService(_SeriesCacheCore):
    """Singleton service for cached intraday OHLCV data with delta refresh."""

    _instance: Optional["IntradayCacheService"] = None
    _refresh_locks: Dict[str, asyncio.Lock]
    _max_concurrent_fetches: int = 10
    _logger = logger

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._refresh_locks = {}
            cls._instance._semaphore = asyncio.Semaphore(cls._max_concurrent_fetches)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "IntradayCacheService":
        return cls()

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _ttl_for(interval: str) -> int:
        return get_ohlcv_ttl(interval)

    # -- per-service hooks (genuine deltas) -------------------------------

    @staticmethod
    def _cache_client():
        return get_cache_client()

    @staticmethod
    async def _provider():
        return await get_market_data_provider()

    async def _fetch_chain(
        self, provider, symbol, interval, from_date, to_date, is_index, user_id,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
        return await provider.get_intraday_with_source(
            symbol=symbol, interval=interval,
            from_date=from_date, to_date=to_date,
            is_index=is_index, user_id=user_id,
        )

    async def _fetch_from(
        self, provider, publisher, symbol, interval, from_date, to_date, is_index, user_id,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
        return await provider.get_intraday_from(
            publisher, symbol, interval=interval,
            from_date=from_date, to_date=to_date,
            is_index=is_index, user_id=user_id,
        )

    def _legacy_key(
        self, symbol, interval, from_date, to_date, source, is_index,
    ) -> str:
        if is_index:
            return IntradayCacheKeyBuilder.index_key(symbol, interval, from_date, to_date, source=source)
        return IntradayCacheKeyBuilder.stock_key(symbol, interval, from_date, to_date, source=source)

    # -- public API -------------------------------------------------------

    async def get_stock_intraday(
        self,
        symbol: str,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> IntradayFetchResult:
        return await self._get_intraday(
            symbol=symbol, is_index=False,
            interval=interval, from_date=from_date, to_date=to_date,
            user_id=user_id,
        )

    async def get_index_intraday(
        self,
        symbol: str,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> IntradayFetchResult:
        return await self._get_intraday(
            symbol=symbol, is_index=True,
            interval=interval, from_date=from_date, to_date=to_date,
            user_id=user_id,
        )

    async def _get_intraday(
        self,
        symbol: str,
        is_index: bool,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> IntradayFetchResult:
        normalized = symbol.removeprefix("I:").lstrip("^").upper()

        base_ttl = self._ttl_for(interval)
        cache = get_cache_client()
        clock = clock_for(normalized, is_index)
        phase = clock.market_phase()

        # --- Try cache (across all known sources) ---
        cache_key, envelope = await self._find_cached(
            normalized, interval, from_date, to_date, is_index,
        )
        is_live = IntradayCacheKeyBuilder._is_live(to_date)

        if envelope is not None:
            bars = envelope["bars"]
            watermark = envelope.get("watermark")
            complete = envelope.get("complete", False)
            stored_ttl = envelope.get("stored_ttl", 0)
            elapsed = time.time() - envelope.get("fetched_at", 0)
            ttl_remaining = max(0, int(stored_ttl - elapsed)) if stored_ttl else None

            log_fn = logger.info if interval == "1s" else logger.debug
            log_fn(
                "Cache HIT %s %s: %d bars, wm=%s, complete=%s, phase=%s, elapsed=%.1fs, ttl_rem=%s",
                normalized, interval, len(bars), watermark, complete, phase, elapsed, ttl_remaining,
            )

            bg_triggered = False
            # Always check structural integrity (stale date, day-boundary,
            # coverage gap) before considering soft-TTL refresh.  This ensures
            # partial/stale envelopes are discarded promptly even if the soft
            # TTL hasn't elapsed yet. Historical envelopes skip the live-only
            # checks via is_live=False.
            if _should_discard_envelope(envelope, interval=interval, elapsed=elapsed, gap_grace_s=_COVERAGE_GAP_GRACE_S, is_live=is_live, clock=clock):
                # Use per-key lock to prevent concurrent sync re-fetches
                # (multiple requests seeing the same stale envelope).
                lock = self._get_refresh_lock(cache_key)
                async with lock:
                    # Re-check cache — another request may have refreshed it
                    refreshed = False
                    _, fresh = await self._find_cached(
                        normalized, interval, from_date, to_date, is_index,
                    )
                    if fresh is not None:
                        fresh_elapsed = time.time() - fresh.get("fetched_at", 0)
                        if not _should_discard_envelope(fresh, interval=interval, elapsed=fresh_elapsed, gap_grace_s=_COVERAGE_GAP_GRACE_S, is_live=is_live, clock=clock):
                            envelope = fresh
                            bars = fresh["bars"]
                            watermark = fresh.get("watermark")
                            complete = fresh.get("complete", False)
                            refreshed = True
                    if not refreshed:
                        logger.info(
                            "Cache %s %s: discarding envelope (bars=%d, first_t=%s) → sync re-fetch",
                            normalized, interval, len(bars),
                            bars[0].get("time") if bars else None,
                        )
                        envelope = None
            elif _needs_refresh(envelope, base_ttl, interval=interval, is_live=is_live, symbol=normalized, is_index=is_index, clock=clock):
                # Normal SWR: return stale bars, refresh in background.
                bg_triggered = True
                logger.info("Cache %s %s: SWR delta refresh triggered", normalized, interval)
                asyncio.create_task(
                    self._delta_refresh(cache_key, normalized, interval, is_index, user_id)
                )

            if envelope is not None:
                return IntradayFetchResult(
                    symbol=normalized,
                    interval=interval,
                    data=bars,
                    cached=True,
                    ttl_remaining=ttl_remaining,
                    background_refresh_triggered=bg_triggered,
                    cache_key=cache_key,
                    watermark=watermark,
                    complete=complete,
                    market_phase=phase,
                    truncated=envelope.get("truncated"),
                    header=envelope.get("header"),
                )

        # --- Cache miss: full fetch (pinned publisher first) ---
        logger.info("Cache MISS %s %s: fetching from=%s to=%s", normalized, interval, from_date, to_date)
        try:
            data, source, truncated = await self._pinned_fetch(
                normalized, interval, from_date, to_date, is_index, user_id,
            )
            cache_key = self._build_key(normalized, interval, from_date, to_date, is_index)
            first_t = data[0].get("time") if data else None
            last_t = data[-1].get("time") if data else None
            logger.info(
                "Cache MISS %s %s: got %d bars from %s, first=%s last=%s, key=%s",
                normalized, interval, len(data), source, first_t, last_t, cache_key,
            )

            closed = phase == "closed"
            complete = closed and len(data) > 0
            effective_ttl = self._effective_ttl(base_ttl, complete, clock)

            # Use short TTL for empty results so we retry quickly
            if not data:
                effective_ttl = _EMPTY_RESULT_TTL
            instrument_key, schema = series_identity(normalized, interval, is_index)
            new_envelope = _build_envelope(
                data, phase, complete, stored_ttl=effective_ttl, truncated=truncated,
                data_date=clock.current_trading_date(),
                instrument_key=instrument_key, schema=schema, publisher=source,
            )

            await cache.set(cache_key, new_envelope, ttl=effective_ttl)
            if source and data:
                await self._write_pin(normalized, interval, is_index, source)

            return IntradayFetchResult(
                symbol=normalized,
                interval=interval,
                data=data,
                cached=False,
                ttl_remaining=effective_ttl,
                background_refresh_triggered=False,
                cache_key=cache_key,
                watermark=new_envelope["header"]["watermark"],
                complete=complete,
                market_phase=phase,
                truncated=truncated,
                header=new_envelope["header"],
            )

        except Exception as e:
            logger.error(f"Failed to fetch intraday data for {symbol}: {e}")
            return IntradayFetchResult(
                symbol=normalized,
                interval=interval,
                data=[],
                cached=False,
                ttl_remaining=None,
                background_refresh_triggered=False,
                market_phase=phase,
                error=str(e),
            )

    # -- batch API --------------------------------------------------------

    async def get_batch_stocks(
        self,
        symbols: List[str],
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
        return await self._get_batch(
            symbols=symbols, is_index=False,
            interval=interval, from_date=from_date, to_date=to_date,
            user_id=user_id,
        )

    async def get_batch_indexes(
        self,
        symbols: List[str],
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
        return await self._get_batch(
            symbols=symbols, is_index=True,
            interval=interval, from_date=from_date, to_date=to_date,
            user_id=user_id,
        )

    async def _get_batch(
        self,
        symbols: List[str],
        is_index: bool,
        interval: str = "1min",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
        """Two-phase batch: parallel cache lookups, then semaphore-controlled API calls."""
        results: Dict[str, List[Dict[str, Any]]] = {}
        errors: Dict[str, str] = {}
        cache_hits = 0
        background_refreshes = 0

        base_ttl = self._ttl_for(interval)
        cache = get_cache_client()

        # Phase 1: parallel cache lookups (try all source-namespaced keys)
        cache_misses: List[str] = []
        # cache_key resolved per symbol (set during hit or fetch)
        resolved_keys: Dict[str, str] = {}

        async def check_cache(sym: str) -> None:
            nonlocal cache_hits, background_refreshes
            normalized = sym.lstrip("^").upper()
            clock = clock_for(normalized, is_index)

            key, envelope = await self._find_cached(
                normalized, interval, from_date, to_date, is_index,
            )
            is_live = IntradayCacheKeyBuilder._is_live(to_date)

            if envelope is not None:
                env_elapsed = time.time() - envelope.get("fetched_at", 0)
                if _should_discard_envelope(envelope, interval=interval, elapsed=env_elapsed, gap_grace_s=_COVERAGE_GAP_GRACE_S, is_live=is_live, clock=clock):
                    cache_misses.append(sym)
                    return
                results[normalized] = envelope["bars"]
                resolved_keys[sym] = key
                cache_hits += 1
                if _needs_refresh(envelope, base_ttl, interval=interval, is_live=is_live, symbol=normalized, is_index=is_index, clock=clock):
                    background_refreshes += 1
                    asyncio.create_task(
                        self._delta_refresh(key, normalized, interval, is_index, user_id)
                    )
            else:
                cache_misses.append(sym)

        await asyncio.gather(*[check_cache(s) for s in symbols])

        # Phase 2: fetch misses with semaphore
        if cache_misses:
            async def fetch_from_api(sym: str) -> None:
                normalized = sym.lstrip("^").upper()
                clock = clock_for(normalized, is_index)
                phase = clock.market_phase()
                async with self._semaphore:
                    try:
                        data, source, truncated = await self._pinned_fetch(
                            normalized, interval, from_date, to_date, is_index, user_id,
                        )
                        results[normalized] = data
                        key = self._build_key(
                            normalized, interval, from_date, to_date, is_index,
                        )

                        closed = phase == "closed"
                        complete = closed and len(data) > 0
                        eff_ttl = self._effective_ttl(base_ttl, complete, clock)
                        if not data:
                            eff_ttl = _EMPTY_RESULT_TTL
                        instrument_key, schema = series_identity(normalized, interval, is_index)
                        env = _build_envelope(
                            data, phase, complete, stored_ttl=eff_ttl, truncated=truncated,
                            data_date=clock.current_trading_date(),
                            instrument_key=instrument_key, schema=schema, publisher=source,
                        )
                        # Awaited (not fire-and-forget create_task): keeps the
                        # data-then-pin write order and errors surface into the
                        # per-symbol except below. Symbols still run concurrently
                        # via the fetch_from_api gather.
                        await cache.set(key, env, ttl=eff_ttl)
                        if source and data:
                            await self._write_pin(normalized, interval, is_index, source)

                    except Exception as e:
                        logger.error(f"Failed to fetch {sym}: {e}")
                        errors[sym.lstrip("^").upper()] = str(e)

            await asyncio.gather(*[fetch_from_api(s) for s in cache_misses])

        cache_stats = {
            "total_requests": len(symbols),
            "cache_hits": cache_hits,
            "cache_misses": len(cache_misses),
            "background_refreshes": background_refreshes,
        }
        return results, errors, cache_stats
