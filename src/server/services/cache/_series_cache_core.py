"""Shared delta-refresh / pinning / dual-read core for the OHLCV series caches.

DailyCacheService and IntradayCacheService are the same cache with three
genuine deltas: the schema interval (a live ``interval`` vs the literal
``1day``), which provider method fills the series, and the log wording. This
mixin owns the five subsystems both services share — effective-TTL extension,
per-key refresh locks, publisher pinning, canonical/legacy dual-read, and the
watermark delta refresh — and defers the deltas to a few hooks
(:meth:`_fetch_chain`, :meth:`_fetch_from`, :meth:`_legacy_key`) plus the
``interval`` threaded through every call. Daily binds ``interval="1day"`` at
its call sites; its provider hooks ignore the interval argument.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import get_ohlcv_ttl
from src.server.services.cache._instrument_clock import clock_for
from src.server.services.cache._ohlcv_envelope import (
    _build_envelope,
    _merge_bars,
    _parse_envelope,
    adopt_v3_envelope,
    canonical_series_key,
    pin_key,
    series_identity,
    splice_is_discontinuous,
    watermark_to_date_str,
)

logger = logging.getLogger(__name__)

# Series pin lifetime. Long enough to keep provider stickiness across data-key
# TTL gaps (weekends included); refreshed on every successful fill.
_PIN_TTL = 7 * 24 * 3600

# TTL floor for envelopes adopted from legacy v3 keys during dual-read.
_ADOPTED_TTL_FLOOR = 30

_FetchResult = Tuple[List[Dict[str, Any]], Optional[str], bool]

# Cap on the per-key refresh-lock dict: every distinct historical window mints
# a key, so an unbounded dict is a slow leak. Eviction only touches unlocked
# entries — a lock with a holder or queued waiters always reads locked().
_MAX_REFRESH_LOCKS = 4096


# Strong refs for fire-and-forget refresh tasks: the event loop keeps only
# weak references to tasks, so an unreferenced background refresh can be
# garbage-collected mid-flight.
_BG_TASKS: set[asyncio.Task] = set()


def spawn_bg_task(coro) -> asyncio.Task:
    """``create_task`` with a strong reference held until the task completes."""
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


def is_live_window(to_date: Optional[str]) -> bool:
    """True when a series window ending at *to_date* may still grow.

    Compared against the western-most plausible venue-local date (UTC-12),
    not the server's ``date.today()`` — on a UTC host that flips an ET
    window to "historical" at 00:00 UTC, freezing the live evening session.
    A false "live" for a genuinely closed window only costs a shorter TTL.
    """
    if to_date is None:
        return True
    try:
        floor = (datetime.now(timezone.utc) - timedelta(hours=12)).date()
        return to_date >= floor.isoformat()
    except (ValueError, TypeError):
        return True


class _SeriesCacheCore:
    """Delta-refresh / pinning / dual-read core shared by the OHLCV caches.

    Concrete services mix this in, set ``_refresh_locks`` in ``__new__``, and
    provide the per-service hooks + log wording below.
    """

    _refresh_locks: Dict[str, asyncio.Lock]

    # Per-service log wording (defaults match the intraday service). Threaded
    # through the shared methods so log lines keep their existing prefixes.
    _logger: logging.Logger = logger
    _log_delta: str = "Delta refresh"
    _log_discontinuity: str = "Discontinuity"
    _log_adopt: str = "Cache ADOPT v3→v4"

    # Provider-routing capability for this series family (daily overrides).
    _capability: str = "intraday"

    # -- per-service hooks (genuine deltas) -------------------------------

    async def _fetch_chain(
        self, provider, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str],
        is_index: bool, user_id: Optional[str],
    ) -> _FetchResult:
        """Full-series fetch via the provider fallback chain → (bars, source, truncated)."""
        raise NotImplementedError

    async def _fetch_from(
        self, provider, publisher: str, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str],
        is_index: bool, user_id: Optional[str],
    ) -> _FetchResult:
        """Full-series fetch from a single pinned publisher → (bars, source, truncated)."""
        raise NotImplementedError

    def _legacy_key(
        self, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str],
        source: str, is_index: bool,
    ) -> str:
        """Pre-cutover, source-segmented v3 cache key (for dual-read adoption)."""
        raise NotImplementedError

    # Cache/provider access resolves through the concrete service module so each
    # service's collaborators stay independently swappable.
    def _cache_client(self):
        raise NotImplementedError

    async def _provider(self):
        raise NotImplementedError

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _effective_ttl(base_ttl: int, complete: bool, clock=None) -> int:
        """Extend TTL when market is closed so the key survives until next open."""
        if complete:
            secs = (clock or clock_for(None)).seconds_until_next_open()
            return max(base_ttl, secs) if secs > 0 else base_ttl
        return base_ttl

    def _get_refresh_lock(self, cache_key: str) -> asyncio.Lock:
        if cache_key not in self._refresh_locks:
            if len(self._refresh_locks) >= _MAX_REFRESH_LOCKS:
                for stale in [k for k, lk in self._refresh_locks.items() if not lk.locked()]:
                    del self._refresh_locks[stale]
            self._refresh_locks[cache_key] = asyncio.Lock()
        return self._refresh_locks[cache_key]

    @staticmethod
    def _is_live(to_date: Optional[str]) -> bool:
        return is_live_window(to_date)

    def _build_key(
        self, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str], is_index: bool,
        live: Optional[bool] = None,
    ) -> str:
        """``live=None`` derives liveness from the date heuristic; an explicit
        bool overrides it — a bounded paging window must never share the
        window-less live key just because its right edge is near today."""
        return canonical_series_key(
            symbol, interval, from_date, to_date,
            is_index=is_index,
            live=self._is_live(to_date) if live is None else live,
        )

    # -- dual-read --------------------------------------------------------

    async def _find_cached(
        self, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str], is_index: bool,
        live: Optional[bool] = None,
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Canonical-key lookup with legacy v3 dual-read (adopt-on-read).

        Misses on the canonical key fall back to the pre-cutover
        source-segmented keys (warm caches, and old servers sharing this
        Redis); a legacy hit is adopted — translated to v4 and written through
        under the canonical key with its remaining TTL — so the cutover never
        causes a synchronized total miss. Returns ``(cache_key, envelope)`` on
        hit, ``(None, None)`` on miss.
        """
        cache = self._cache_client()
        key = self._build_key(symbol, interval, from_date, to_date, is_index, live=live)
        envelope = _parse_envelope(await cache.get(key))
        if envelope is not None:
            return key, envelope

        if live is False and self._is_live(to_date):
            # Explicit-historical override disagrees with the date heuristic:
            # old writers used the heuristic, so any legacy hit for this window
            # sits under the legacy LIVE key — adopting it would graft a live
            # series onto a bounded window. Skip dual-read.
            return None, None

        provider = await self._provider()
        # Capability-aware order: adoption must prefer the same publisher the
        # live chain would pick for this symbol (config order alone would let
        # the non-US-intraday yfinance priority slot shadow FMP everywhere).
        sources = provider.source_names_for(symbol, self._capability)
        legacy_keys = [
            self._legacy_key(symbol, interval, from_date, to_date, source, is_index)
            for source in sources
        ]
        values = await cache.mget(legacy_keys) if legacy_keys else []
        for source, legacy, raw in zip(sources, legacy_keys, values):
            v3 = _parse_envelope(raw)
            if v3 is None:
                continue
            instrument_key, schema = series_identity(symbol, interval, is_index)
            adopted = adopt_v3_envelope(v3, instrument_key, schema, publisher=source)
            remaining = int(v3.get("stored_ttl", 0) - (time.time() - v3.get("fetched_at", 0)))
            await cache.set(key, adopted, ttl=max(remaining, _ADOPTED_TTL_FLOOR))
            self._logger.info("%s %s ← %s", self._log_adopt, key, legacy)
            return key, _parse_envelope(adopted)
        return None, None

    # -- pinning ----------------------------------------------------------

    async def _pinned_publisher(
        self, symbol: str, interval: str, is_index: bool,
    ) -> Optional[str]:
        try:
            pin = await self._cache_client().get(pin_key(symbol, interval, is_index))
            return pin.get("publisher") if isinstance(pin, dict) else None
        except Exception:
            return None

    async def _write_pin(
        self, symbol: str, interval: str, is_index: bool, publisher: str,
    ) -> None:
        """Record the series' publisher pin.

        Written only after the data key, so a reader following the pin never
        lands on a series the cache hasn't filled — a weak ordering guarantee
        (pin-after-data), not an atomic validate-and-swap.
        """
        try:
            await self._cache_client().set(
                pin_key(symbol, interval, is_index), {"publisher": publisher}, ttl=_PIN_TTL,
            )
        except Exception:
            self._logger.debug("pin write failed for %s %s", symbol, interval, exc_info=True)

    async def _pinned_fetch(
        self, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str],
        is_index: bool, user_id: Optional[str],
    ) -> _FetchResult:
        """Full fetch honoring the series pin: pinned publisher first, then the
        normal fallback chain (which re-pins via the caller's ``_write_pin``)."""
        provider = await self._provider()
        pinned = await self._pinned_publisher(symbol, interval, is_index)
        if pinned:
            try:
                return await self._fetch_from(
                    provider, pinned, symbol, interval, from_date, to_date, is_index, user_id,
                )
            except Exception as e:
                self._logger.warning(
                    "Pinned publisher %s failed for %s %s (%s) — falling back to chain",
                    pinned, symbol, interval, e,
                )
        return await self._fetch_chain(
            provider, symbol, interval, from_date, to_date, is_index, user_id,
        )

    # -- delta refresh ----------------------------------------------------

    async def _delta_refresh(
        self, cache_key: str, symbol: str, interval: str,
        is_index: bool = False, user_id: Optional[str] = None,
    ) -> None:
        """Background delta refresh: fetch only from the watermark onward, merge."""
        lock = self._get_refresh_lock(cache_key)
        if lock.locked():
            self._logger.debug("%s already in progress for %s", self._log_delta, cache_key)
            return

        async with lock:
            try:
                cache = self._cache_client()
                provider = await self._provider()

                # Re-read envelope (may have been updated by another refresh)
                raw = await cache.get(cache_key)
                envelope = _parse_envelope(raw) if raw else None

                clock = clock_for(symbol, is_index)
                phase = clock.market_phase()
                closed = phase == "closed"

                if envelope and envelope.get("complete") and closed:
                    # Still closed — nothing to do
                    return

                watermark = envelope["watermark"] if envelope else None
                existing_bars = envelope["bars"] if envelope else []
                header = (envelope or {}).get("header") or {}
                publisher = header.get("publisher")
                revision = header.get("revision", 0)

                async def fetch(from_date, to_date=None):
                    # A pinned series only refills from its own publisher —
                    # splicing another provider's bars is silent blending.
                    if publisher:
                        return await self._fetch_from(
                            provider, publisher, symbol, interval,
                            from_date, to_date, is_index, user_id,
                        )
                    return await self._fetch_chain(
                        provider, symbol, interval,
                        from_date, to_date, is_index, user_id,
                    )

                if envelope and envelope.get("truncated"):
                    # Truncated base — full re-fetch instead of delta
                    delta, source, truncated = await fetch(None)
                    merged = delta
                else:
                    # Normal delta refresh. Watermark is Unix ms; the delta
                    # from_date window is exchange-local for the symbol.
                    delta_from = watermark_to_date_str(watermark, tz=clock.tz)
                    delta, source, truncated = await fetch(delta_from)

                    if watermark and existing_bars:
                        if splice_is_discontinuous(existing_bars, delta, watermark):
                            # Final history moved upstream (adjustment or
                            # correction) — never splice across it. Discard,
                            # full-refetch, bump the series revision.
                            self._logger.warning(
                                "%s for %s: delta disagrees with final history "
                                "→ full re-fetch (revision %d → %d)",
                                self._log_discontinuity, cache_key, revision, revision + 1,
                            )
                            delta, source, truncated = await fetch(None)
                            merged = delta
                            revision += 1
                        else:
                            merged = _merge_bars(existing_bars, delta, watermark)
                    else:
                        merged = delta

                # Build new envelope
                complete = closed and len(merged) > 0
                base_ttl = get_ohlcv_ttl(interval)
                effective = self._effective_ttl(base_ttl, complete, clock)
                instrument_key, schema = series_identity(symbol, interval, is_index)
                new_envelope = _build_envelope(
                    merged, phase, complete, stored_ttl=effective, truncated=truncated,
                    data_date=clock.current_trading_date(),
                    instrument_key=instrument_key, schema=schema,
                    publisher=source or publisher, revision=revision,
                )

                await cache.set(cache_key, new_envelope, ttl=effective)
                if source:
                    await self._write_pin(symbol, interval, is_index, source)

                self._logger.debug(
                    "%s for %s: fetched %d bars, total %d, phase=%s, complete=%s",
                    self._log_delta, cache_key, len(delta), len(merged), phase, complete,
                )

            except Exception as e:
                self._logger.warning("%s failed for %s: %s", self._log_delta, cache_key, e)
