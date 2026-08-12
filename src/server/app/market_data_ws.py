"""
WebSocket proxy for ginlix-data real-time market aggregates.

Authenticates the frontend WebSocket via Supabase JWT, then registers
the client as a consumer of the MarketDataFeed.  Messages
from the shared upstream connection are forwarded to each client based
on their symbol subscriptions.

WS ticks are also written into the Redis OHLCV cache so that REST
reads always reflect near-real-time data (WS-fed cache).

The entire router is only registered when ``GINLIX_DATA_ENABLED`` is
true (i.e. ``GINLIX_DATA_WS_URL`` is set) — see ``setup.py``.
"""

import asyncio
import json
import logging
import time as _time
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from src.server.auth.ws_auth import authenticate_websocket
from src.server.services.cache._ohlcv_envelope import (
    _build_envelope,
    _parse_envelope,
    canonical_series_key,
    series_identity,
)
from src.server.services.cache._series_cache_core import spawn_bg_task
from src.server.services.market_data_feed import MarketDataFeed, to_protocol_record
from src.utils.cache.redis_cache import get_cache_client
from src.utils.market_hours import current_trading_date
from src.observability import (
    safe_add,
    safe_record,
    ws_connection_duration_seconds,
    ws_connections_active,
    ws_disconnects,
    ws_messages_sent,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_ALLOWED_MARKETS = {"stock", "index"}

# Map WS interval param → cache interval key
_WS_INTERVAL_TO_CACHE: dict[str, str] = {
    "second": "1s",
    "minute": "1min",
}

_WS_CACHE_TTL = 30  # seconds — longer TTL survives brief WS hiccups
_WS_SOURCE = "ginlix-data"  # must match config.yaml provider name

# ---------------------------------------------------------------------------
# Throttled tick buffer — avoids flooding Redis with one write per tick
# ---------------------------------------------------------------------------
_FLUSH_INTERVAL = 2.0  # seconds between Redis writes per cache key
_last_flush: dict[str, float] = {}  # cache_key → last flush time
_pending_bars: dict[str, list[dict]] = {}  # cache_key → bars since last flush
_scheduled_flushes: dict[str, asyncio.Task] = {}  # cache_key → pending delayed flush

# Per-connection hardening: bound subscribe frames so an authenticated client
# can't inflate feed refcounts or upstream subscription frames without limit.
_MAX_WS_SYMBOLS = 200
_MAX_SYMBOL_LEN = 24

# Track completed backfills to avoid re-triggering after TTL expiry
_backfill_done: dict[str, str] = {}  # cache_key → data_date
_backfill_in_progress: set[str] = set()

# Intervals whose cache gets a REST backfill on the first WS tick. The 1s
# cache is deliberately NOT backfilled: with the 1s chart interval removed,
# no REST provider serves second bars — the 1s key is a live tick buffer only.
_BACKFILL_INTERVALS = {"1min"}

# Periodic cleanup of stale entries in module-level dicts
_CLEANUP_INTERVAL = 60.0  # seconds between cleanup sweeps
_FLUSH_RETENTION = 24 * 3600.0  # drop idle _last_flush entries after a day
_last_cleanup: float = 0.0


def _cleanup_stale_entries() -> None:
    """Remove entries from _last_flush and _backfill_done for previous trading dates."""
    global _last_cleanup
    now = _time.monotonic()
    if now - _last_cleanup < _CLEANUP_INTERVAL:
        return
    _last_cleanup = now

    today = current_trading_date()
    stale_keys = [k for k, v in _backfill_done.items() if v != today]
    for k in stale_keys:
        _backfill_done.pop(k, None)
        _last_flush.pop(k, None)

    # 1s-interval keys never enter _backfill_done (backfill is 1min-only), so
    # prune _last_flush by its own idle age too or it grows one entry per
    # distinct 1s-streamed instrument for the process lifetime.
    idle = [k for k, t in _last_flush.items() if now - t > _FLUSH_RETENTION]
    for k in idle:
        _last_flush.pop(k, None)



def _requeue_pending(cache_key: str, bars: list[dict]) -> None:
    """Put an unflushed batch back at the head of the pending buffer.

    Ticks buffered while the batch was in flight are newer — on a shared bar
    time the buffered version wins.
    """
    pending = _pending_bars.get(cache_key)
    if not pending:
        _pending_bars[cache_key] = bars
        return
    _pending_bars[cache_key] = [b for b in bars if b["time"] < pending[0]["time"]] + pending


def _schedule_flush(cache_key: str) -> None:
    """One-shot delayed flush for deferred ticks (throttle window, refresh-lock
    contention). Without it a quiet symbol's last buffered bars strand in
    ``_pending_bars`` waiting for a next tick that may never come."""
    if cache_key in _scheduled_flushes:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop (sync caller) — the bars stay pending for the next
        # tick or the connection-close flush.
        return

    async def _later() -> None:
        try:
            await asyncio.sleep(_FLUSH_INTERVAL)
        finally:
            _scheduled_flushes.pop(cache_key, None)
        bars = _pending_bars.pop(cache_key, [])
        if bars:
            _last_flush[cache_key] = _time.monotonic()
            await _flush_to_redis(cache_key, bars)

    # The dict entry doubles as the strong reference keeping the task alive.
    _scheduled_flushes[cache_key] = loop.create_task(_later())


def _cap_subscribe(current: set[str], symbols: list) -> list[str]:
    """Bound one subscribe frame: drop malformed entries and enforce the
    per-connection symbol cap."""
    valid = [
        s for s in symbols
        if isinstance(s, str) and 0 < len(s.strip()) <= _MAX_SYMBOL_LEN
    ]
    seen = set(current)
    budget = _MAX_WS_SYMBOLS - len(seen)
    allowed: list[str] = []
    for s in valid:
        upper = s.strip().upper()
        if upper in seen:
            allowed.append(s)  # already subscribed — no budget cost
        elif budget > 0:
            allowed.append(s)
            seen.add(upper)
            budget -= 1
    if len(allowed) < len(symbols):
        logger.warning(
            "WS subscribe frame capped: %d requested, %d accepted (cap %d)",
            len(symbols), len(allowed), _MAX_WS_SYMBOLS,
        )
    return allowed


def _cache_key_for(symbol: str, market: str, cache_interval: str) -> str:
    # Canonical Phase 3 key — the same one REST reads, so WS ticks and REST
    # backfill/refresh converge on a single series per instrument (this also
    # ends the old WS-writes-SPX / REST-reads-GSPC index split).
    return canonical_series_key(symbol, cache_interval, is_index=market == "index")


def _series_ids(symbol: str, market: str, cache_interval: str) -> tuple[str, str]:
    return series_identity(symbol, cache_interval, is_index=market == "index")


def _outbound_frame(raw_msg: str, entry: Optional[dict], cmdp: bool) -> str:
    """Per-client outbound serializer (WS role 3).

    Default (``cmdp=False``) is the legacy raw upstream passthrough —
    byte-identical. ``cmdp=True`` wraps each aggregate bar as a protocol frame
    ``{"type": "ohlcv", **entry}`` from the precomputed ``to_protocol_record``
    entry; non-aggregate messages (``entry is None``: status, keepalive, or an
    un-canonicalizable bar) pass through raw in both modes. The choice is
    per-connection, so mixed clients on one feed each get their own format.
    """
    if cmdp and entry is not None:
        return json.dumps({"type": "ohlcv", **entry})
    return raw_msg


async def _backfill_from_rest(
    cache_key: str, symbol: str, market: str, cache_interval: str,
    user_id: Optional[str] = None,
) -> None:
    """Fetch historical bars via the REST data provider and merge into the WS cache.

    Called once per cache key when the first WS tick arrives with no existing
    cache.  Fetches today's data from the provider, merges with any bars
    already accumulated from WS ticks, and writes the result back.
    """
    try:
        from src.data_client import get_market_data_provider

        provider = await get_market_data_provider()
        is_index = market == "index"

        data, source, truncated = await provider.get_intraday_with_source(
            symbol=symbol,
            interval=cache_interval,
            from_date=None,
            to_date=None,
            is_index=is_index,
            user_id=user_id,
        )
        if not data:
            return

        from src.server.services.cache.intraday_cache_service import IntradayCacheService

        svc = IntradayCacheService.get_instance()
        lock = svc._get_refresh_lock(cache_key)

        async with lock:
            # Re-read current WS cache (may have accumulated ticks since we started)
            cache = get_cache_client()
            raw = await cache.get(cache_key)
            envelope = _parse_envelope(raw) if raw else None
            ws_bars = envelope["bars"] if envelope and envelope.get("bars") else []

            # Merge: REST as historical base, append only WS bars newer than REST's last bar
            if ws_bars:
                rest_watermark = data[-1].get("time", 0)
                newer_ws = [b for b in ws_bars if b.get("time", 0) > rest_watermark]
                merged = data + newer_ws
            else:
                merged = data

            from src.utils.market_hours import current_market_phase
            phase = current_market_phase()
            instrument_key, schema = _series_ids(symbol, market, cache_interval)
            # The merged base is REST data — declare its real publisher so
            # delta refresh pins refills to the same provider; only a pure
            # WS series carries the WS source. Revision survives the merge.
            prior = (envelope.get("header") or {}) if envelope else {}
            new_envelope = _build_envelope(
                merged, phase, complete=False, stored_ttl=_WS_CACHE_TTL, truncated=truncated,
                instrument_key=instrument_key, schema=schema,
                publisher=source or _WS_SOURCE,
                revision=prior.get("revision", 0),
            )
            await cache.set(cache_key, new_envelope, ttl=_WS_CACHE_TTL)

        _backfill_done[cache_key] = current_trading_date()
        logger.info(
            "WS backfill for %s: %d REST bars + %d WS bars → %d merged",
            cache_key, len(data), len(ws_bars), len(merged),
        )
    except asyncio.CancelledError:
        return
    except Exception:
        logger.warning("WS backfill failed for %s", cache_key, exc_info=True)
    finally:
        _backfill_in_progress.discard(cache_key)


async def _flush_to_redis(cache_key: str, bars: list[dict]) -> None:
    """Write buffered bars to Redis, merging with existing envelope.

    Coordinates with ``IntradayCacheService._delta_refresh`` via a shared
    per-key ``asyncio.Lock``.  If a delta refresh is in progress the batch
    is requeued and retried after the throttle interval — returning without
    requeueing would silently drop every tick that raced the refresh.
    """
    try:
        from src.server.services.cache.intraday_cache_service import IntradayCacheService

        svc = IntradayCacheService.get_instance()
        lock = svc._get_refresh_lock(cache_key)
        if lock.locked():
            _requeue_pending(cache_key, bars)
            _schedule_flush(cache_key)
            logger.debug("WS flush deferred for %s: delta refresh in progress", cache_key)
            return

        async with lock:
            cache = get_cache_client()
            raw = await cache.get(cache_key)
            envelope = _parse_envelope(raw) if raw else None

            if envelope and envelope.get("bars"):
                existing = envelope["bars"]
                # Merge buffered bars into existing: update in-place or append
                for new_bar in bars:
                    if existing[-1]["time"] == new_bar["time"]:
                        existing[-1] = new_bar
                    elif new_bar["time"] > existing[-1]["time"]:
                        existing.append(new_bar)
                merged = existing
            else:
                merged = bars

            phase = envelope.get("market_phase", "open") if envelope else "open"
            # Identity is embedded in the canonical key: ohlcv:{instrument_key}:{schema}
            _, instrument_key, schema = cache_key.split(":", 2)
            # Lineage survives a WS update: a REST-filled series keeps its
            # publisher (delta refresh pins refills on it) and its revision
            # counter — stamping the whole envelope as WS-published would pin
            # future refreshes to ginlix-data and mix conventions silently.
            prior = (envelope.get("header") or {}) if envelope else {}
            new_envelope = _build_envelope(
                merged, phase, complete=False, stored_ttl=_WS_CACHE_TTL, truncated=False,
                instrument_key=instrument_key, schema=schema,
                publisher=prior.get("publisher") or _WS_SOURCE,
                revision=prior.get("revision", 0),
            )
            await cache.set(cache_key, new_envelope, ttl=_WS_CACHE_TTL)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.debug("WS cache flush failed for %s", cache_key, exc_info=True)


def _buffer_tick(
    bar: dict, market: str, cache_interval: str, entry: Optional[dict],
    user_id: Optional[str] = None,
) -> None:
    """Buffer a tick's canonical record in memory; schedule a flush if the
    throttle interval elapsed.

    ``entry`` is the precomputed ``to_protocol_record`` result — shared with the
    outbound serializer so the mapping runs once per tick. ``None`` means the
    symbol couldn't be canonicalized and the tick is dropped, never buffered raw.
    """
    _cleanup_stale_entries()
    if entry is None:
        return  # un-canonicalizable symbol — drop the tick, never buffer raw
    cache_key = _cache_key_for(bar["symbol"], market, cache_interval)
    new_bar = entry["record"]

    # Accumulate in pending buffer (update-in-place or append)
    if cache_key not in _pending_bars:
        _pending_bars[cache_key] = [new_bar]
    else:
        buf = _pending_bars[cache_key]
        if buf[-1]["time"] == new_bar["time"]:
            buf[-1] = new_bar
        elif new_bar["time"] > buf[-1]["time"]:
            buf.append(new_bar)

    # Check if we should flush now
    now = _time.monotonic()
    last = _last_flush.get(cache_key, 0)
    if now - last < _FLUSH_INTERVAL:
        # Throttled. A delayed flush guarantees delivery even if this was the
        # symbol's last tick for a while (lull, session close).
        _schedule_flush(cache_key)
        return

    _last_flush[cache_key] = now
    bars_to_flush = _pending_bars.pop(cache_key, [])
    if not bars_to_flush:
        return

    today = current_trading_date()
    is_first_write = (
        cache_key not in _backfill_in_progress
        and _backfill_done.get(cache_key) != today
    )

    # Mark in-progress synchronously to prevent double-backfill from rapid ticks
    if is_first_write and cache_interval in _BACKFILL_INTERVALS:
        _backfill_in_progress.add(cache_key)

    async def _do_flush():
        await _flush_to_redis(cache_key, bars_to_flush)
        # On first write (cache was empty), trigger REST backfill for supported intervals
        if is_first_write and cache_interval in _BACKFILL_INTERVALS:
            await _backfill_from_rest(cache_key, bar["symbol"], market, cache_interval, user_id)

    spawn_bg_task(_do_flush())


@router.get("/ws/v1/market-data/status")
async def market_data_ws_status():
    """Lightweight probe — returns 200 when the WS proxy feature is enabled.
    Used by the frontend preflight check to avoid noisy WS handshake failures."""
    return {"enabled": True}


@router.websocket("/ws/v1/market-data/aggregates/{market}")
async def ws_market_data_proxy(
    websocket: WebSocket,
    market: str,
    interval: str = "second",
    tier: str = "realtime",
    fmt: str = Query("raw", alias="format"),
):
    """Proxy frontend WS via MarketDataFeed.

    ``?format=cmdp`` switches THIS client's outbound frames to CMDP protocol
    frames; the default is the legacy raw upstream passthrough. The Redis cache
    write is unaffected by this choice — it is always the canonical record.
    """

    if market not in _ALLOWED_MARKETS:
        await websocket.close(code=1008, reason=f"Invalid market: {market}")
        return

    # Index data is delayed-only; override any client-supplied tier
    if market == "index":
        tier = "delayed"

    if tier not in ("delayed", "realtime"):
        await websocket.close(code=1008, reason=f"Invalid tier: {tier}")
        return

    if interval not in _WS_INTERVAL_TO_CACHE:
        # Also bounds MarketDataFeed._instances — arbitrary interval strings
        # would mint unbounded, never-started feed objects.
        await websocket.close(code=1008, reason=f"Invalid interval: {interval}")
        return

    # Authenticate before accepting
    try:
        user_id = await authenticate_websocket(websocket)
    except Exception:
        return  # ws_auth already closed the socket

    await websocket.accept()
    logger.info("WS proxy opened: user=%s market=%s interval=%s tier=%s", user_id, market, interval, tier)

    _ws_labels = {"market": market, "interval": interval, "tier": tier}
    _ws_t0 = _time.monotonic()
    _disconnect_reason = "client_close"
    safe_add(ws_connections_active, 1, _ws_labels)

    shared_ws = MarketDataFeed.get_instance(market=market, interval=interval, tier=tier)
    # Feeds outside DEFAULT_WS_FEEDS are minted lazily and were never started —
    # without this a valid ?interval=minute client connects and silently gets
    # zero data. start() is idempotent for already-running feeds.
    await shared_ws.start()
    consumer_id = f"ws_proxy_{uuid4().hex[:12]}"
    cache_interval = _WS_INTERVAL_TO_CACHE.get(interval)
    cmdp = fmt.lower() == "cmdp"  # per-connection outbound format
    connection_keys: set[str] = set()
    _msg_count = 0
    disconnected = asyncio.Event()

    async def on_message(raw_msg: str, bar: Optional[dict]) -> None:
        """Callback from MarketDataFeed — forward to frontend client."""
        nonlocal _msg_count
        try:
            _msg_count += 1
            if _msg_count <= 5 or _msg_count % 50 == 0:
                logger.debug(
                    "shared→client %s (#%d): %s",
                    consumer_id, _msg_count,
                    raw_msg[:300] if isinstance(raw_msg, str) else str(raw_msg)[:300],
                )
            # Map the bar once; both the outbound frame (cmdp clients) and the
            # Redis cache write derive from this single protocol entry.
            entry = to_protocol_record(bar, market, interval) if bar is not None else None
            await websocket.send_text(_outbound_frame(raw_msg, entry, cmdp))
            safe_add(ws_messages_sent, 1, _ws_labels)

            # Buffer tick for throttled cache write — always the canonical
            # record, independent of this client's outbound format. Gated on
            # entry: an un-canonicalizable symbol (entry None) must be dropped,
            # not raise out of _cache_key_for and tear down the connection.
            if cache_interval and bar and entry is not None:
                key = _cache_key_for(bar["symbol"], market, cache_interval)
                connection_keys.add(key)
                _buffer_tick(bar, market, cache_interval, entry, user_id)
        except Exception:
            # Client likely disconnected — signal cleanup
            disconnected.set()

    handle = shared_ws.register_consumer(consumer_id, on_message)

    try:
        # Read client messages (subscribe/unsubscribe) and forward through handle.
        # Race receive_text against disconnected event so we unblock immediately
        # if the on_message callback detects a send failure.
        while True:
            receive_task = asyncio.ensure_future(websocket.receive_text())
            disconnect_task = asyncio.ensure_future(disconnected.wait())
            done, pending = await asyncio.wait(
                [receive_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()

            if disconnect_task in done:
                _disconnect_reason = "server_error"
                break

            try:
                msg = receive_task.result()
            except WebSocketDisconnect:
                _disconnect_reason = "client_close"
                break
            except Exception:
                _disconnect_reason = "server_error"
                break

            # Parse client subscribe/unsubscribe and route through handle
            try:
                parsed = json.loads(msg)
                if not isinstance(parsed, dict):
                    continue  # valid JSON but not a control object — ignore
                action = parsed.get("action", "")
                symbols = parsed.get("symbols", [])
                if not isinstance(symbols, list):
                    continue  # a string here would subscribe its characters
                if action == "subscribe" and symbols:
                    symbols = _cap_subscribe(handle.subscribed_symbols, symbols)
                    if symbols:
                        await handle.subscribe(symbols)
                elif action == "unsubscribe" and symbols:
                    await handle.unsubscribe([s for s in symbols if isinstance(s, str)])
                elif action == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except (json.JSONDecodeError, TypeError):
                pass
    finally:
        # Unregister consumer
        await handle.close()

        # Flush remaining buffered bars
        for key in list(connection_keys):
            bars = _pending_bars.pop(key, [])
            if bars:
                await _flush_to_redis(key, bars)

        try:
            await websocket.close()
        except Exception:
            pass
        logger.info("WS proxy closed: user=%s market=%s", user_id, market)

        safe_add(ws_connections_active, -1, _ws_labels)
        safe_record(ws_connection_duration_seconds, _time.monotonic() - _ws_t0, _ws_labels)
        safe_add(ws_disconnects, 1, {**_ws_labels, "reason": _disconnect_reason})
