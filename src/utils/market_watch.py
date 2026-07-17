"""Redis-backed per-thread market watch list (JSON list under market_watch:{thread_id})."""

import asyncio
import logging
import re

from src.config.settings import get_market_watch_max_symbols, get_redis_ttl_market_watch
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# Bounded ticker grammar (NVDA, 0700.HK, ^STOXX50E, BRK-B, EURUSD=X). Anything
# outside it — control characters, markup, oversized strings — never reaches
# Redis or the <market-watch> stamp text.
_SYMBOL_RE = re.compile(r"[A-Z0-9.^=-]{1,15}")

# Serializes read-modify-write mutations so parallel watch/unwatch tool calls
# within a turn can't drop each other's update (concurrent turns on a thread
# are already excluded by admission).
_mutate_lock = asyncio.Lock()


def watch_key(thread_id: str) -> str:
    return f"market_watch:{thread_id}"


def _clean(symbols: list[str] | str | None) -> list[str]:
    """Clean and normalize symbols. Coerce int to str, skip other types."""
    # Treat bare string as single symbol (not character iteration).
    if isinstance(symbols, str):
        symbols = [symbols]

    out: list[str] = []
    for s in symbols or []:
        # Skip None, dicts, lists; coerce str/int.
        if s is None or isinstance(s, (dict, list)):
            continue
        if isinstance(s, (str, int)):
            sym = str(s).strip().upper()
            if sym and _SYMBOL_RE.fullmatch(sym) and sym not in out:
                out.append(sym)
    return out


async def get_watchlist(thread_id: str) -> list[str]:
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return []
    try:
        value = await cache.get(watch_key(thread_id))
        return value if isinstance(value, list) else []
    except Exception:
        logger.warning("[MarketWatch] get_watchlist failed", exc_info=True)
        return []


async def add_symbols(thread_id: str, symbols: list[str] | str) -> list[str] | None:
    """Merged watch list after adding, or None when the cache is unavailable.

    None means the mutation did not happen — callers must not report success.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None
    try:
        async with _mutate_lock:
            current = await get_watchlist(thread_id)
            merged = _clean(current + _clean(symbols))[: get_market_watch_max_symbols()]
            await cache.set(watch_key(thread_id), merged, ttl=get_redis_ttl_market_watch())
        return merged
    except Exception:
        logger.warning("[MarketWatch] add_symbols failed", exc_info=True)
        return None


async def remove_symbols(
    thread_id: str, symbols: list[str] | None = None
) -> list[str] | None:
    """Remaining watch list after removal, or None when the mutation didn't happen.

    A None must never be reported as "stopped": the Redis key may survive and
    injection would resume once the cache recovers.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None
    try:
        async with _mutate_lock:
            if symbols is None:
                await cache.delete(watch_key(thread_id))
                return []
            drop = set(_clean(symbols))
            remaining = [s for s in await get_watchlist(thread_id) if s not in drop]
            await cache.set(
                watch_key(thread_id), remaining, ttl=get_redis_ttl_market_watch()
            )
        return remaining
    except Exception:
        logger.warning("[MarketWatch] remove_symbols failed", exc_info=True)
        return None
