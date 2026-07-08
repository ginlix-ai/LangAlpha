"""Per-symbol quote cache with batched upstream fill and in-flight dedup.

Replaces the inline snapshot caches that keyed on the full sorted symbol
list (overlapping watchlists never shared a quote) and the separate
single-symbol path. Keys are per-instrument and schema-versioned
(``quote:v{N}:{instrument_key}``), reads are one MGET, misses fill via ONE
batched provider call, and TTL is phase-aware per the instrument's calendar.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.data_client import get_market_data_provider
from src.market_protocol import InstrumentRef, to_canonical, to_legacy_api
from src.market_protocol.calendars import get_calendar
from src.market_protocol.enums import AssetClass, MarketPhase
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# Phase-aware TTLs (seconds). Closed markets hold until the next open,
# clamped to sane bounds in case of calendar anomalies.
_TTL_REGULAR = 8
_TTL_EXTENDED = 20
_TTL_CLOSED_MIN = 60
_TTL_CLOSED_MAX = 72 * 3600

# Negative cache: a symbol no provider resolved. Without it every request
# for an unknown symbol re-fans out across the whole provider chain.
_TTL_NEGATIVE = 30
_NO_DATA = {"__no_data__": True}

# Cached-row contract version. The closed-phase TTL freezes a row for up to
# 72h, so an unversioned key keeps serving the old row shape until the next
# open after any deploy that changes the normalizer contract. Bump to
# cold-start the namespace (cheap: one MGET + one batched provider call,
# in-flight dedup absorbs the stampede); orphaned old-version keys expire on
# their own. v2: rows gained regular_close / last_minute_close / exact
# dollar early-late changes.
_QUOTE_SCHEMA_VERSION = 2


def _quote_ttl(ref: InstrumentRef, now: Optional[datetime] = None) -> int:
    """TTL for a quote of *ref* right now, from its market calendar."""
    now = now or datetime.now(timezone.utc)
    try:
        cal = get_calendar(ref.calendar_id)
        phase = cal.phase_at(now)
        if phase in (MarketPhase.REGULAR, MarketPhase.LUNCH):
            return _TTL_REGULAR
        if phase in (MarketPhase.PRE, MarketPhase.POST):
            return _TTL_EXTENDED
        secs = cal.seconds_until_next_open(now)
        if secs > 0:
            return max(_TTL_CLOSED_MIN, min(secs, _TTL_CLOSED_MAX))
        return _TTL_CLOSED_MIN
    except Exception:
        logger.warning("quote_cache.ttl_fallback | key=%s", ref.instrument_key, exc_info=True)
        return _TTL_EXTENDED


def _normalize(symbol: str) -> str:
    return str(symbol).strip().upper().removeprefix("^")


class QuoteCacheService:
    """Singleton service for per-symbol cached snapshots (quotes)."""

    _instance: Optional["QuoteCacheService"] = None
    _inflight: Dict[str, asyncio.Future]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._inflight = {}
        return cls._instance

    @classmethod
    def get_instance(cls) -> "QuoteCacheService":
        return cls()

    @staticmethod
    def _ref(symbol: str, is_index: bool) -> InstrumentRef:
        # EQUITY (not autodetect) when the caller said stocks: a bare index-
        # family collision (COMP the stock) must not serve the index's quote.
        # Caret/I: spellings still force INDEX, so explicit index rows in a
        # stock watchlist keep resolving via the marker.
        hint = AssetClass.INDEX if is_index else AssetClass.EQUITY
        return to_canonical(symbol, asset_class=hint)

    @staticmethod
    def quote_key(ref: InstrumentRef) -> str:
        return f"quote:v{_QUOTE_SCHEMA_VERSION}:{ref.instrument_key}"

    async def get_quotes(
        self,
        symbols: List[str],
        asset_type: str = "stocks",
        user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return snapshot rows for *symbols* in request order.

        Symbols that no provider can resolve are dropped (no null-field
        rows). One MGET serves cache hits; all misses fill via a single
        batched provider call; concurrent requests for the same instrument
        share one upstream fetch.
        """
        is_index = asset_type == "indices"
        request: list[tuple[str, InstrumentRef, str]] = []
        seen_keys: set[str] = set()
        for s in symbols:
            if not str(s).strip():
                continue
            ref = self._ref(s, is_index)
            key = self.quote_key(ref)
            if key not in seen_keys:
                seen_keys.add(key)
                request.append((_normalize(s), ref, key))
        if not request:
            return []

        cache = get_cache_client()
        cached = await cache.mget([key for _, _, key in request])
        rows: Dict[str, Dict[str, Any]] = {}
        to_fetch: list[tuple[str, InstrumentRef, str]] = []
        followers: list[tuple[str, asyncio.Future]] = []

        for (sym, ref, key), hit in zip(request, cached):
            if hit is not None:
                if not hit.get("__no_data__"):
                    rows[key] = hit
            elif key in self._inflight:
                followers.append((key, self._inflight[key]))
            else:
                fut: asyncio.Future = asyncio.get_running_loop().create_future()
                self._inflight[key] = fut
                to_fetch.append((sym, ref, key))

        if to_fetch:
            try:
                fetched = await self._fetch_batch(to_fetch, user_id)
            except BaseException as exc:  # incl. CancelledError — never strand a future
                for _, _, key in to_fetch:
                    fut = self._inflight.pop(key, None)
                    if fut and not fut.done():
                        if isinstance(exc, Exception):
                            fut.set_exception(exc)
                            # Followers may or may not await; don't warn on GC.
                            fut.exception()
                        else:
                            fut.cancel()
                raise
            for sym, ref, key in to_fetch:
                row = fetched.get(sym)
                if row is not None:
                    rows[key] = row
                fut = self._inflight.pop(key, None)
                if fut and not fut.done():
                    fut.set_result(row)

        for key, fut in followers:
            row = await asyncio.shield(fut)
            if row is not None:
                rows[key] = row

        return [rows[key] for _, _, key in request if key in rows]

    async def _fetch_batch(
        self,
        to_fetch: list[tuple[str, InstrumentRef, str]],
        user_id: Optional[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Batched provider call per asset class; write-through with phase TTL.

        Partitioned by the RESOLVED instrument, not the caller's asset_type: a
        caret-spelled index reaching the stocks endpoint still fetches via the
        index path, so a miss there can never negative-cache the canonical
        index key out from under legitimate index readers.
        """
        provider = await get_market_data_provider()
        index_refs = [ref for _, ref, _ in to_fetch if ref.asset_class is AssetClass.INDEX]
        stock_refs = [ref for _, ref, _ in to_fetch if ref.asset_class is not AssetClass.INDEX]

        rows_by_legacy: Dict[str, Dict[str, Any]] = {}
        for refs, atype in ((stock_refs, "stocks"), (index_refs, "indices")):
            if not refs:
                continue
            # Providers speak the legacy API form (bare family for indexes) —
            # a ^-marked spelling from the boundary is not a provider symbol.
            legacy_syms = list(dict.fromkeys(to_legacy_api(r) for r in refs))
            raw = await provider.get_snapshots(legacy_syms, asset_type=atype, user_id=user_id)
            for r in raw or []:
                rows_by_legacy[_normalize(r.get("symbol") or "")] = r

        cache = get_cache_client()
        out: Dict[str, Dict[str, Any]] = {}
        writes = []
        for sym, ref, key in to_fetch:
            # Read back via the ref-derived legacy spelling so every alias of
            # one instrument (e.g. ^IXIC / ^COMP) finds its row.
            row = rows_by_legacy.get(_normalize(to_legacy_api(ref)))
            if row is None:
                writes.append(cache.set(key, _NO_DATA, ttl=_TTL_NEGATIVE))
                continue
            out[sym] = row
            writes.append(cache.set(key, row, ttl=_quote_ttl(ref)))
        if writes:
            # Concurrent write-through — a 250-symbol fill must not pay N
            # serial Redis round-trips inside the leader's critical section.
            await asyncio.gather(*writes)
        return out
