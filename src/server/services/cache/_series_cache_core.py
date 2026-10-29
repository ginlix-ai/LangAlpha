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
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from src.config.settings import get_ohlcv_ttl
from src.data_client.instrument_clock import clock_for
from src.data_client.normalize import ref_for_key, series_lineage
from src.server.services.cache._ohlcv_envelope import (
    _EMPTY_RESULT_TTL,
    _bar_time,
    _build_envelope,
    _merge_bars,
    _parse_envelope,
    adopt_v3_envelope,
    canonical_series_key,
    is_settled_complete,
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

# TTL cap for a series a fallback filled on another basis than the chain head's.
_STAND_IN_TTL = 300

_FetchResult = Tuple[List[Dict[str, Any]], Optional[str], bool]


class _PinRead(NamedTuple):
    """The series pin as a full fetch read it, so the fill's basis check
    reuses the read and the chain-head verdict instead of repeating them."""

    publisher: Optional[str]
    current: bool


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

    @staticmethod
    def _delta_from(
        existing_bars: List[Dict[str, Any]], watermark, tz,
    ) -> Optional[str]:
        """Start date for a delta refetch: the last cached bar strictly BEFORE
        the watermark.

        Starting at the watermark itself (the forming bar) leaves the delta
        with no bar the discontinuity guard may compare — on a daily series
        that is *every* refresh, so an upstream re-base (a qfq provider
        re-basing its whole history on a corporate action, or any provider's
        split re-adjustment) splices in as a permanent step at the seam. One
        bar of overlap costs nothing and gives the guard something to check.
        """
        if watermark:
            for bar in reversed(existing_bars or []):
                t = _bar_time(bar)
                if t and t < watermark:
                    return watermark_to_date_str(t, tz=tz)
        return watermark_to_date_str(watermark, tz=tz)

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
        sources = provider.source_names_for(
            symbol, self._capability, interval=interval, is_index=is_index,
        )
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

    def _pin_is_current(
        self, provider, pinned: str, symbol: str, interval: str, is_index: bool,
    ) -> bool:
        """Whether *pinned* still heads the chain for this series.

        A pin keeps a series on one publisher across transient chain failures.
        Moving the head, whether a re-probed ruleset moves the cell or the
        configured provider order changes, is a deliberate operator decision
        and must win over that stickiness, or the old publisher would serve
        until the 7-day pin lapsed, which a watched symbol never lets happen.
        The chain's order is the provider's own, the ruleset cell when there
        is one and the configured order otherwise.
        """
        names = provider.source_names_for(
            symbol, self._capability, interval=interval, is_index=is_index,
        )
        return not names or names[0] == pinned

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

    async def _basis_transition(
        self, symbol: str, interval: str, is_index: bool, source: Optional[str], ref,
        *, pin: Optional[_PinRead] = None, basis: Optional[str] = None,
    ) -> Tuple[int, bool]:
        """``(revision_bump, may_repin)`` for a fill that landed on *source*.

        The revision follows the stored series: it bumps when *source*
        declares a different ``price_treatment`` from *basis*, the publisher
        of the series this fill extends, or from the pin when no series is in
        hand. The pin is separate. A substitute of another treatment standing
        in for a pin that still heads the chain leaves the pin alone, or a
        single timeout would freeze the substitute's basis in for the pin's
        whole lifetime; a same-treatment swap re-pins as before.
        """
        if not source:
            return 0, False
        pinned = pin.publisher if pin is not None else await self._pinned_publisher(
            symbol, interval, is_index,
        )
        source_treatment = series_lineage(source, ref)[0]
        prior = basis or pinned
        rev_bump = int(
            bool(prior) and prior != source
            and series_lineage(prior, ref)[0] != source_treatment
        )
        if not pinned or pinned == source:
            return rev_bump, True
        pinned_treatment = series_lineage(pinned, ref)[0]
        if pinned_treatment == source_treatment:
            return rev_bump, True
        current = pin.current if pin is not None else self._pin_is_current(
            await self._provider(), pinned, symbol, interval, is_index,
        )
        if not current:
            # Re-routed: the chain's head moved, so the new publisher takes the pin.
            self._logger.info(
                "Re-routed %s %s: pinned publisher %s (%s) superseded by %s (%s), "
                "re-pinning (revision bump %d)",
                symbol, interval, pinned, pinned_treatment.value,
                source, source_treatment.value, rev_bump,
            )
            return rev_bump, True
        self._logger.log(
            logging.WARNING if rev_bump else logging.INFO,
            "Pinned publisher %s (%s) unavailable for %s %s, %s (%s) answered: "
            "keeping the pin (revision bump %d)",
            pinned, pinned_treatment.value, symbol, interval,
            source, source_treatment.value, rev_bump,
        )
        return rev_bump, False

    @staticmethod
    def _history_moved(
        stored: Optional[Dict[str, Any]], bars: List[Dict[str, Any]],
    ) -> bool:
        """Whether *bars*, replacing *stored* whole, disagree with its final history.

        A refill rebuilds the series as surely as a discontinuous delta does
        (a split re-adjusted overnight arrives through the next full fetch),
        so it moves the revision by the same test, or a reader would merge
        the new tail onto history on the old basis.
        """
        if not stored or not bars:
            return False
        return splice_is_discontinuous(stored.get("bars") or [], bars, stored.get("watermark"))

    async def _fetch_and_store(
        self, cache_key: str, symbol: str, interval: str,
        from_date: Optional[str], to_date: Optional[str],
        is_index: bool, user_id: Optional[str],
        *, phase: str, clock, base_ttl: int,
    ) -> Tuple[List[Dict[str, Any]], bool, Dict[str, Any]]:
        """Full fetch honoring the series pin, stored under *cache_key*;
        returns ``(bars, truncated, envelope)``.

        The pinned publisher goes first, then the normal fallback chain (which
        re-pins on store). The pin is read and checked against the chain's
        head once, and the store's basis check reuses both. An empty pinned answer
        is a miss, as it is inside the chain: an empty fill never re-pins, so
        accepting it would keep a publisher that stopped serving the series
        pinned, and the series empty, until the pin lapsed. The series still
        stored under *cache_key*, the one a sync refill replaces, hands the
        fill its publisher and revision, as a delta refresh's does.
        """
        provider = await self._provider()
        stored = _parse_envelope(await self._cache_client().get(cache_key))
        pinned = await self._pinned_publisher(symbol, interval, is_index)
        current = bool(pinned) and self._pin_is_current(provider, pinned, symbol, interval, is_index)
        if pinned and not current:
            self._logger.info(
                "Pin %s for %s %s no longer heads the chain — using the chain",
                pinned, symbol, interval,
            )
        fetched: Optional[_FetchResult] = None
        if current:
            try:
                fetched = await self._fetch_from(
                    provider, pinned, symbol, interval, from_date, to_date, is_index, user_id,
                )
            except Exception as e:
                self._logger.warning(
                    "Pinned publisher %s failed for %s %s (%s) — falling back to chain",
                    pinned, symbol, interval, e,
                )
        pinned_empty: Optional[_FetchResult] = None
        if fetched is not None and not fetched[0]:
            self._logger.info(
                "Pinned publisher %s returned no bars for %s %s — falling back to chain",
                pinned, symbol, interval,
            )
            pinned_empty, fetched = fetched, None
        if fetched is None:
            try:
                fetched = await self._fetch_chain(
                    provider, symbol, interval, from_date, to_date, is_index, user_id,
                )
            except Exception:
                # The chain failing outright leaves the pinned empty answer as
                # the best one, and storing it keeps the empty-result TTL
                # damping repeat fetches.
                if pinned_empty is None:
                    raise
                fetched = pinned_empty
        bars, source, truncated = fetched
        header = (stored or {}).get("header") or {}
        envelope = await self._store_fill(
            cache_key, symbol, interval, is_index, bars, source, truncated,
            phase=phase, clock=clock, base_ttl=base_ttl, pin=_PinRead(pinned, current),
            publisher=header.get("publisher"), revision=header.get("revision", 0),
            rebuilt=self._history_moved(stored, bars),
        )
        return bars, truncated, envelope

    async def _store_fill(
        self, cache_key: str, symbol: str, interval: str, is_index: bool,
        bars: List[Dict[str, Any]], source: Optional[str], truncated: bool,
        *, phase: str, clock, base_ttl: int, pin: Optional[_PinRead] = None,
        publisher: Optional[str] = None, revision: int = 0, rebuilt: bool = False,
    ) -> Dict[str, Any]:
        """Write one fetched series, then its pin; returns the stored envelope.

        The series ref and lineage resolve once here and feed the basis check,
        the settle check and the header alike. An empty series, fetched or
        merged, gets the short empty TTL and never re-pins; a fill over a
        stored series (``publisher`` is the envelope's own, ``revision`` its
        current one) keeps both, and its revision moves once when *source*
        changes the series' basis or the fill *rebuilt* its final history.
        """
        instrument_key, schema = series_identity(symbol, interval, is_index)
        ref = ref_for_key(instrument_key)
        rev_bump, may_repin = await self._basis_transition(
            symbol, interval, is_index, source if bars else None, ref,
            pin=pin, basis=publisher,
        )
        publisher = source or publisher
        lineage = series_lineage(publisher, ref)
        complete = is_settled_complete(phase, bars, clock, lineage[1])
        ttl = self._effective_ttl(base_ttl, complete, clock)
        if not bars:
            ttl = _EMPTY_RESULT_TTL
        elif source:
            names = (await self._provider()).source_names_for(
                symbol, self._capability, interval=interval, is_index=is_index,
            )
            if names and names[0] != source and series_lineage(names[0], ref)[0] != lineage[0]:
                # Kept briefly: a window cached from the stand-in would outlive
                # the outage, and a chart paging back would join it to the
                # head's series across an adjustment seam.
                ttl = min(ttl, _STAND_IN_TTL)
        envelope = _build_envelope(
            bars, phase, complete, stored_ttl=ttl, truncated=truncated,
            data_date=clock.current_trading_date(),
            instrument_key=instrument_key, schema=schema,
            publisher=publisher, revision=revision + max(rev_bump, int(rebuilt)),
            lineage=lineage,
        )
        await self._cache_client().set(cache_key, envelope, ttl=ttl)
        if source and may_repin:
            await self._write_pin(symbol, interval, is_index, source)
        return envelope

    # -- delta refresh ----------------------------------------------------

    async def _keep_series(
        self, cache_key: str, raw: Dict[str, Any], envelope: Dict[str, Any],
    ) -> None:
        """Rewrite the stored series unchanged but for ``fetched_at``.

        A refresh that produced nothing still asked upstream, so restamping
        paces the next soft-TTL retry instead of letting every read spawn
        another. Bars, flags, publisher and revision stay as they were, and so
        does the key's expiry: ``stored_ttl`` becomes what is left of it.
        """
        now = time.time()
        remaining = int(envelope.get("stored_ttl", 0) - (now - envelope.get("fetched_at", 0)))
        ttl = max(remaining, _EMPTY_RESULT_TTL)
        kept = {**raw, "stored_ttl": ttl}
        if isinstance(raw.get("header"), dict):
            kept["header"] = {**raw["header"], "fetched_at": now}
        else:
            kept["fetched_at"] = now
        await self._cache_client().set(cache_key, kept, ttl=ttl)

    async def _retry_head(
        self, provider, head: str, cache_key: str, symbol: str, interval: str,
        is_index: bool, user_id: Optional[str],
    ) -> Optional[_FetchResult]:
        """The whole series from *head*, or None while it stays out.

        A series a fallback filled carries the substitute as its publisher,
        and so does one whose chain head moved after it was filled. The two
        may differ in basis, so the head can only replace the series whole,
        and until it answers the stored series keeps extending by delta. An
        outage then costs one failed call per refresh, not a full refetch.
        """
        try:
            fetched = await self._fetch_from(
                provider, head, symbol, interval, None, None, is_index, user_id,
            )
        except Exception as e:
            self._logger.info(
                "%s for %s: chain head %s still unavailable (%s)",
                self._log_delta, cache_key, head, e,
            )
            return None
        if not fetched[0]:
            self._logger.info(
                "%s for %s: chain head %s returned no bars",
                self._log_delta, cache_key, head,
            )
            return None
        return fetched

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

                if envelope and envelope.get("complete") and closed and not envelope.get("truncated"):
                    # Still closed — nothing to do. A truncated series is still
                    # short of its window, so it refills whole below.
                    return

                watermark = envelope["watermark"] if envelope else None
                existing_bars = envelope["bars"] if envelope else []
                header = (envelope or {}).get("header") or {}
                publisher = header.get("publisher")
                revision = header.get("revision", 0)
                read_from = publisher
                pin: Optional[_PinRead] = None
                refill: Optional[_FetchResult] = None
                rerouted = False
                chain = provider.source_names_for(
                    symbol, self._capability, interval=interval, is_index=is_index,
                )
                if publisher and chain and chain[0] != publisher:
                    # A fallback filled the series, or the chain's head moved
                    # since. Either way the head replaces it whole once it
                    # answers; until then the series extends from its own
                    # publisher, unless that publisher has left the chain.
                    pinned = await self._pinned_publisher(symbol, interval, is_index)
                    pin = _PinRead(pinned, pinned == chain[0])
                    refill = await self._retry_head(
                        provider, chain[0], cache_key, symbol, interval, is_index, user_id,
                    )
                    if refill is None and publisher not in chain:
                        rerouted = True
                        read_from = None
                        self._logger.info(
                            "%s for %s: publisher %s left the chain → full re-fetch",
                            self._log_delta, cache_key, publisher,
                        )

                async def fetch(from_date, to_date=None):
                    # A pinned series only refills from its own publisher —
                    # splicing another provider's bars is silent blending.
                    if read_from:
                        return await self._fetch_from(
                            provider, read_from, symbol, interval,
                            from_date, to_date, is_index, user_id,
                        )
                    return await self._fetch_chain(
                        provider, symbol, interval,
                        from_date, to_date, is_index, user_id,
                    )

                rebuilt = False
                if refill is not None:
                    delta, source, truncated = refill
                    merged = delta
                    rebuilt = self._history_moved(envelope, merged)
                elif rerouted or (envelope and envelope.get("truncated")):
                    # Truncated base or re-routed publisher — full re-fetch instead of delta
                    delta, source, truncated = await fetch(None)
                    merged = delta
                    rebuilt = self._history_moved(envelope, merged)
                else:
                    # Normal delta refresh. Watermark is Unix ms; the delta
                    # from_date window is exchange-local for the symbol.
                    delta_from = self._delta_from(existing_bars, watermark, clock.tz)
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

                if not merged and existing_bars:
                    # A full re-fetch that answered empty (an upstream 500
                    # read as no data) must not replace a good series.
                    self._logger.info(
                        "%s for %s: full re-fetch returned no bars, keeping %d cached",
                        self._log_delta, cache_key, len(existing_bars),
                    )
                    await self._keep_series(cache_key, raw, envelope)
                    return

                stored = await self._store_fill(
                    cache_key, symbol, interval, is_index, merged, source, truncated,
                    phase=phase, clock=clock, base_ttl=get_ohlcv_ttl(interval),
                    pin=pin, publisher=publisher, revision=revision, rebuilt=rebuilt,
                )
                self._logger.debug(
                    "%s for %s: fetched %d bars, total %d, phase=%s, complete=%s",
                    self._log_delta, cache_key, len(delta), len(merged), phase,
                    stored["complete"],
                )

            except Exception as e:
                self._logger.warning("%s failed for %s: %s", self._log_delta, cache_key, e)
