"""Per-symbol quote cache with batched upstream fill and in-flight dedup.

Replaces the inline snapshot caches that keyed on the full sorted symbol
list (overlapping watchlists never shared a quote) and the separate
single-symbol path. Keys are per-instrument and schema-versioned
(``quote:v{N}:{asset_class}:{instrument_key}``), reads are one MGET, misses fill via ONE
batched provider call, and TTL is phase-aware per the instrument's calendar.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.data_client import get_market_data_provider
from src.data_client.market_data_provider import snapshot_key
from src.data_client.ginlix_data.directory import fill_quote_names
from market_protocol import InstrumentRef, to_legacy_api
from market_protocol.calendars import MarketCalendar, get_calendar
from market_protocol.enums import AssetClass, MarketPhase, Tier
from src.server.services.cache._ohlcv_envelope import CLOSE_SETTLE_GRACE
from src.server.services.cache.quote_daily_fallback import fill_from_daily
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# Phase-aware TTLs (seconds). Closed markets hold until the next open,
# clamped to sane bounds in case of calendar anomalies.
_TTL_REGULAR = 8
_TTL_EXTENDED = 20
_TTL_CLOSED_MIN = 60
_TTL_CLOSED_MAX = 72 * 3600

# How long after a venue closes a quote may still predate the session's last
# prints: a closing auction that prints after the regular close (HKEX's runs to
# 16:10) plus their publication. A feed that is not declared realtime trails by
# its delay on top. Until then a closed-phase row keeps the short closed TTL,
# or a row read just after the bell would be held until the next open. The
# series cache settles on the same grace, so a quote and its chart agree.
_CLOSE_SETTLE = CLOSE_SETTLE_GRACE
_FEED_DELAY = timedelta(minutes=15)

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
# dollar early-late changes. v3: CN rows carried the Chinese ``name``.
# v4: CN/HK rows carry additive ``name_local``/``name_en``; ``name`` is the
# provider's own again. v5: rows carry ``tier`` / ``as_of`` (quote freshness).
# v6: ginlix-data rows carry ``as_of`` from the last trade's print time.
# v7: Shanghai rows carry ``.SH``, never the vendors' ``.SS``.
# v8: FMP and Yahoo rows carry ``as_of`` from the quote's print time, and they
# and daily-fallback rows say ``regular_only`` (no extended-hours prints).
# v9: keys carry the asset class, so an index-hinted miss for an equity's
# instrument_key (0700.HK) cannot negative-cache the equity read.
_QUOTE_SCHEMA_VERSION = 9


def _closed_recently(cal: MarketCalendar, now: datetime, window: timedelta) -> bool:
    """Whether a venue that is closed now closed within *window* before *now*.

    Any phase edge in that span is the close: it ends in the phase the venue
    is in now.
    """
    edge = cal.next_phase_change_ms(now - window)
    return edge is not None and edge <= int(now.timestamp() * 1000)


def _predates_last_session(cal: MarketCalendar, now: datetime, as_of: Any) -> bool:
    """Whether a row printed at *as_of* (Unix ms) predates the venue's last session.

    Such a row is a vendor that has not rolled to that session, and the closed
    TTL would hold its stale close until the next open. The bound is the start
    of the session's day, not its close: a thin name's last print comes before
    the bell, and an opening auction prints before the open. A row with no
    print time shows no lag.
    """
    if isinstance(as_of, bool) or not isinstance(as_of, (int, float)) or as_of <= 0:
        return False
    day = cal.expected_latest_daily_date(now)
    return as_of < datetime(day.year, day.month, day.day, tzinfo=cal.tz).timestamp() * 1000


def _asset_type(ref: InstrumentRef) -> str:
    """The provider call *ref* is fetched through, by its resolved asset class."""
    return "indices" if ref.asset_class is AssetClass.INDEX else "stocks"


def _quote_ttl(
    ref: InstrumentRef,
    now: Optional[datetime] = None,
    *,
    tier: Optional[str] = None,
    as_of: Any = None,
) -> int:
    """TTL for a quote of *ref* right now, from its market calendar.

    *tier* is the row's declared feed tier; anything not declared realtime is
    allowed the delayed feed's lag before its closed row is held to the open.
    *as_of* is the row's print time; a closed row older than the venue's last
    session keeps the short closed TTL, so it is refetched until it rolls.
    """
    now = now or datetime.now(timezone.utc)
    try:
        cal = get_calendar(ref.calendar_id)
        phase = cal.phase_at(now)
        if phase in (MarketPhase.REGULAR, MarketPhase.LUNCH):
            return _TTL_REGULAR
        if phase in (MarketPhase.PRE, MarketPhase.POST):
            return _TTL_EXTENDED
        settle = _CLOSE_SETTLE if tier == Tier.REALTIME.value else _CLOSE_SETTLE + _FEED_DELAY
        if _closed_recently(cal, now, settle) or _predates_last_session(cal, now, as_of):
            return _TTL_CLOSED_MIN
        secs = cal.seconds_until_next_open(now)
        if secs > 0:
            return max(_TTL_CLOSED_MIN, min(secs, _TTL_CLOSED_MAX))
        return _TTL_CLOSED_MIN
    except Exception:
        logger.warning("quote_cache.ttl_fallback | key=%s", ref.instrument_key, exc_info=True)
        return _TTL_EXTENDED


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
    def quote_key(ref: InstrumentRef) -> str:
        return f"quote:v{_QUOTE_SCHEMA_VERSION}:{ref.asset_class.value}:{ref.instrument_key}"

    async def get_quotes(
        self,
        refs: List[InstrumentRef],
        user_id: Optional[str] = None,
        *,
        daily_fallback: bool = False,
    ) -> List[Tuple[InstrumentRef, Dict[str, Any]]]:
        """Return ``(ref, row)`` per instrument in *refs*, in request order.

        Each row comes back with the identity it was keyed and fetched under,
        so a caller never re-resolves it from the vendor's echo. Instruments
        no provider can resolve are dropped (no null-field rows). One MGET
        serves cache hits; all misses fill via a single batched provider call;
        concurrent requests for the same instrument share one upstream fetch.

        ``daily_fallback`` (stock endpoints) appends a closing quote from the
        daily bars for a dropped instrument whose venue is closed (see
        :mod:`.quote_daily_fallback`), after the rows the chain served. Off by
        default: such a row is EOD, and a caller must be ready to show it as one.

        A failed fill is a miss for the instruments it was fetching, and raises
        only when nothing else answers: one listing no provider can quote would
        otherwise drop the cached rows and the daily fallback of every other
        instrument in a watchlist batch.
        """
        refs = list({self.quote_key(ref): ref for ref in refs}.values())
        quotes, failure = await self._cached_quotes(refs, user_id)
        if daily_fallback:
            quotes = await fill_from_daily(quotes, refs, user_id)
        if failure is not None and not quotes:
            raise failure
        # Names are identity, not quote data. A row filled while the directory
        # was unreachable would otherwise stay nameless for the whole
        # closed-phase TTL, and a daily-fallback row never had one; the
        # directory's own cache keeps this read cheap.
        await fill_quote_names(row for _, row in quotes)
        return quotes

    async def _cached_quotes(
        self, refs: List[InstrumentRef], user_id: Optional[str],
    ) -> Tuple[List[Tuple[InstrumentRef, Dict[str, Any]]], Optional[Exception]]:
        """The rows the cache, this request's fill and shared fills serve, and the fill's error."""
        if not refs:
            return [], None
        request = [(ref, self.quote_key(ref)) for ref in refs]

        cache = get_cache_client()
        cached = await cache.mget([key for _, key in request])
        rows: Dict[str, Dict[str, Any]] = {}
        to_fetch: list[tuple[InstrumentRef, str]] = []
        followers: list[tuple[str, asyncio.Future]] = []

        for (ref, key), hit in zip(request, cached):
            if hit is not None:
                if not hit.get("__no_data__"):
                    rows[key] = hit
            elif key in self._inflight:
                followers.append((key, self._inflight[key]))
            else:
                fut: asyncio.Future = asyncio.get_running_loop().create_future()
                self._inflight[key] = fut
                to_fetch.append((ref, key))

        failure: Optional[Exception] = None
        fetched: Dict[str, Dict[str, Any]] = {}
        if to_fetch:
            try:
                fetched, failure = await self._fetch_batch(to_fetch, user_id)
            except BaseException as exc:  # incl. CancelledError — never strand a future
                for _, key in to_fetch:
                    fut = self._inflight.pop(key, None)
                    if fut and not fut.done():
                        if isinstance(exc, Exception):
                            fut.set_exception(exc)
                            # Followers may or may not await; don't warn on GC.
                            fut.exception()
                        else:
                            fut.cancel()
                if not isinstance(exc, Exception):
                    raise
                failure = exc
            for _, key in to_fetch:
                row = fetched.get(key)
                if row is not None:
                    rows[key] = row
                fut = self._inflight.pop(key, None)
                if fut and not fut.done():
                    fut.set_result(row)

        for key, fut in followers:
            # The leader's failure is the leader's problem: a follower treats a
            # cancelled or errored shared future as a miss for this cycle (the
            # next poll refetches) instead of 500-ing its own request.
            try:
                row = await asyncio.shield(fut)
            except asyncio.CancelledError:
                if not fut.cancelled():
                    raise  # this request was cancelled, not the leader's
                continue
            except Exception:
                continue
            if row is not None:
                rows[key] = row

        return [(ref, rows[key]) for ref, key in request if key in rows], failure

    async def _fetch_batch(
        self,
        to_fetch: list[tuple[InstrumentRef, str]],
        user_id: Optional[str],
    ) -> Tuple[Dict[str, Dict[str, Any]], Optional[Exception]]:
        """Batched provider call per asset class; write-through with phase TTL.

        Partitioned by the RESOLVED instrument, not the caller's endpoint: a
        caret-spelled index reaching the stocks endpoint still fetches via the
        index path, so a miss there can never negative-cache the canonical
        index key out from under legitimate index readers. Each partition
        matches only its own rows, since GSPC the equity and GSPC the index
        echo the same string.

        A partition that raises is a miss for its own symbols only, and is not
        negative-cached: an outage is not "no provider has it". Its error comes
        back beside the rows rather than being dropped once another partition
        answers, since an answer may be empty (an index no provider has), and
        only :meth:`get_quotes` sees every row that could make the outage moot.
        """
        provider = await get_market_data_provider()

        async def _rows(refs: list[InstrumentRef], asset_type: str) -> Dict[str, Dict[str, Any]]:
            # Providers speak the legacy API form (bare family for indexes).
            legacy_syms = list(dict.fromkeys(to_legacy_api(r) for r in refs))
            raw = await provider.get_snapshots(legacy_syms, asset_type=asset_type, user_id=user_id)
            return {snapshot_key(r.get("symbol")): r for r in raw or []}

        partitions: Dict[str, list[InstrumentRef]] = {}
        for ref, _ in to_fetch:
            partitions.setdefault(_asset_type(ref), []).append(ref)
        results = await asyncio.gather(
            *(_rows(refs, asset_type) for asset_type, refs in partitions.items()),
            return_exceptions=True,
        )
        answered: Dict[str, Dict[str, Dict[str, Any]]] = {}
        failure: Optional[Exception] = None
        for (asset_type, refs), result in zip(partitions.items(), results):
            if not isinstance(result, BaseException):
                answered[asset_type] = result
                continue
            if not isinstance(result, Exception):
                raise result
            failure = result
            logger.warning(
                "quote_cache.partition_failed | asset_type=%s symbols=%d error=%s",
                asset_type, len(refs), result,
            )

        cache = get_cache_client()
        out: Dict[str, Dict[str, Any]] = {}
        writes: list[tuple[str, Any, int]] = []
        for ref, key in to_fetch:
            served = answered.get(_asset_type(ref))
            if served is None:
                continue  # its partition raised
            # Read back via the ref-derived legacy spelling so every alias of
            # one instrument (e.g. ^IXIC / ^COMP) finds its row.
            row = served.get(snapshot_key(to_legacy_api(ref)))
            if row is None:
                writes.append((key, _NO_DATA, _TTL_NEGATIVE))
                continue
            out[key] = row
            writes.append((key, row, _quote_ttl(ref, tier=row.get("tier"), as_of=row.get("as_of"))))
        # One pipelined round trip on one connection. The former gather of
        # per-key set() took a connection each, so a 250-symbol fill — the
        # batch cap — could exhaust a 150-slot pool from a single request.
        await cache.set_many(writes)
        return out, failure
