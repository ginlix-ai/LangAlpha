"""
WebSocket proxy for ginlix-data real-time market aggregates.

Authenticates the frontend WebSocket via Supabase JWT, then opens a
backend WebSocket to ginlix-data using the internal service token.
Messages are forwarded bidirectionally until either side disconnects.

WS ticks are also written into the Redis OHLCV cache so that REST
reads always reflect near-real-time data (WS-fed cache).

The entire router is only registered when ``GINLIX_DATA_ENABLED`` is
true (i.e. ``GINLIX_DATA_WS_URL`` is set) — see ``setup.py``.
"""

import asyncio
import json
import logging
import os
import time as _time
from typing import Optional

import websockets
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.config.settings import GINLIX_DATA_WS_URL
from src.server.auth.ws_auth import authenticate_websocket
from src.server.services.cache._ohlcv_envelope import _build_envelope, _parse_envelope
from src.server.services.cache.intraday_cache_service import IntradayCacheKeyBuilder
from src.utils.cache.redis_cache import get_cache_client
from src.utils.market_hours import current_trading_date

logger = logging.getLogger(__name__)

router = APIRouter()

_INTERNAL_SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")
_ALLOWED_MARKETS = {"stock", "index", "crypto", "forex"}

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

# Track completed backfills to avoid re-triggering after TTL expiry
_backfill_done: dict[str, str] = {}  # cache_key → data_date
_backfill_in_progress: set[str] = set()

# Intervals where REST backfill is supported (ginlix-data supports both)
_BACKFILL_INTERVALS = {"1min", "1s"}

# Periodic cleanup of stale entries in module-level dicts
_CLEANUP_INTERVAL = 60.0  # seconds between cleanup sweeps
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


def _parse_ws_bar(raw_msg: str) -> Optional[dict]:
    """Parse a WS message into a normalised bar dict.

    Returns ``None`` for non-aggregate messages (status, keepalive, etc.).
    """
    try:
        msg = json.loads(raw_msg)
    except (json.JSONDecodeError, TypeError):
        return None

    symbol: Optional[str] = None
    o = h = l = c = v = ts = None

    if isinstance(msg, dict):
        ev = msg.get("ev")
        if ev in ("AM", "A"):
            # Raw Polygon-style aggregate
            symbol = msg.get("sym")
            o, h, l, c, v = msg.get("o"), msg.get("h"), msg.get("l"), msg.get("c"), msg.get("v")
            ts = msg.get("s") or msg.get("e")
        elif msg.get("type") == "aggregate" and isinstance(msg.get("data"), dict):
            # Wrapped format
            d = msg["data"]
            symbol = msg.get("symbol") or d.get("sym") or d.get("symbol")
            o = d.get("open", d.get("o"))
            h = d.get("high", d.get("h"))
            l = d.get("low", d.get("l"))
            c = d.get("close", d.get("c"))
            v = d.get("volume", d.get("v"))
            ts = d.get("time", d.get("timestamp", d.get("s", d.get("e"))))

    if not symbol or c is None or ts is None:
        return None

    # Normalise timestamp to Unix milliseconds
    if isinstance(ts, (int, float)):
        ts_ms = int(ts) if ts > 1e12 else int(ts * 1000)
    else:
        return None

    return {
        "symbol": symbol.upper(),
        "time": ts_ms,
        "open": float(o) if o is not None else 0.0,
        "high": float(h) if h is not None else 0.0,
        "low": float(l) if l is not None else 0.0,
        "close": float(c) if c is not None else 0.0,
        "volume": int(v) if v is not None else 0,
    }


def _cache_key_for(symbol: str, market: str, cache_interval: str) -> str:
    if market == "index":
        return IntradayCacheKeyBuilder.index_key(symbol, cache_interval, source=_WS_SOURCE)
    return IntradayCacheKeyBuilder.stock_key(symbol, cache_interval, source=_WS_SOURCE)


def _bar_fields(bar: dict) -> dict:
    return {
        "time": bar["time"],
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"],
    }


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

        data, _source, _truncated = await provider.get_intraday_with_source(
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
            new_envelope = _build_envelope(merged, phase, complete=False, stored_ttl=_WS_CACHE_TTL, truncated=False)
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
    per-key ``asyncio.Lock``.  If a delta refresh is in progress we skip
    this write — the REST result is at least as current as the WS buffer,
    and the ticks will be re-flushed on the next 2 s cycle.
    """
    try:
        from src.server.services.cache.intraday_cache_service import IntradayCacheService

        svc = IntradayCacheService.get_instance()
        lock = svc._get_refresh_lock(cache_key)
        if lock.locked():
            logger.debug("WS flush skipped for %s: delta refresh in progress", cache_key)
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
            new_envelope = _build_envelope(merged, phase, complete=False, stored_ttl=_WS_CACHE_TTL, truncated=False)
            await cache.set(cache_key, new_envelope, ttl=_WS_CACHE_TTL)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.debug("WS cache flush failed for %s", cache_key, exc_info=True)


def _buffer_tick(
    bar: dict, market: str, cache_interval: str, user_id: Optional[str] = None,
) -> None:
    """Buffer a tick in memory; schedule a flush if throttle interval elapsed."""
    _cleanup_stale_entries()
    cache_key = _cache_key_for(bar["symbol"], market, cache_interval)
    new_bar = _bar_fields(bar)

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
        return  # throttled — will be flushed on next tick past the interval

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

    asyncio.create_task(_do_flush())


@router.get("/ws/v1/market-data/status")
async def market_data_ws_status():
    """Lightweight probe — returns 200 when the WS proxy feature is enabled.
    Used by the frontend preflight check to avoid noisy WS handshake failures."""
    return {"enabled": True}


@router.websocket("/ws/v1/market-data/aggregates/{market}")
async def ws_market_data_proxy(
    websocket: WebSocket, market: str, interval: str = "minute", tier: str = "realtime",
):
    """Proxy frontend WS to ginlix-data aggregate stream."""

    if market not in _ALLOWED_MARKETS:
        await websocket.close(code=1008, reason=f"Invalid market: {market}")
        return

    if tier not in ("delayed", "realtime"):
        await websocket.close(code=1008, reason=f"Invalid tier: {tier}")
        return

    # Authenticate before accepting
    try:
        user_id = await authenticate_websocket(websocket)
    except Exception:
        return  # ws_auth already closed the socket

    await websocket.accept()
    logger.info("WS proxy opened: user=%s market=%s interval=%s tier=%s", user_id, market, interval, tier)

    # Build backend URL
    backend_url = f"{GINLIX_DATA_WS_URL}/ws/v1/data/aggregates/{market}?interval={interval}&tier={tier}"
    backend_headers = {"X-User-Id": user_id}
    if _INTERNAL_SERVICE_TOKEN:
        backend_headers["X-Service-Token"] = _INTERNAL_SERVICE_TOKEN

    try:
        async with websockets.connect(
            backend_url,
            additional_headers=backend_headers,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as backend_ws:

            _msg_count = 0

            async def client_to_backend():
                """Forward messages from the frontend client to ginlix-data."""
                try:
                    while True:
                        msg = await websocket.receive_text()
                        await backend_ws.send(msg)
                except WebSocketDisconnect:
                    pass  # Client disconnected
                except Exception as exc:
                    logger.debug("client_to_backend closed: %s", exc)

            cache_interval = _WS_INTERVAL_TO_CACHE.get(interval)
            connection_keys: set[str] = set()  # track cache keys owned by this connection

            async def backend_to_client():
                """Forward messages from ginlix-data to the frontend client."""
                nonlocal _msg_count
                try:
                    async for msg in backend_ws:
                        _msg_count += 1
                        if _msg_count <= 5 or _msg_count % 50 == 0:
                            logger.debug("backend→client (#%d): %s", _msg_count, msg[:300] if isinstance(msg, str) else str(msg)[:300])
                        await websocket.send_text(msg)

                        # Buffer tick for throttled cache write
                        if cache_interval:
                            bar = _parse_ws_bar(msg)
                            if bar:
                                key = _cache_key_for(bar["symbol"], market, cache_interval)
                                connection_keys.add(key)
                                _buffer_tick(bar, market, cache_interval, user_id)
                except websockets.exceptions.ConnectionClosed:
                    pass  # Backend disconnected
                except Exception as exc:
                    logger.debug("backend_to_client closed: %s", exc)

            # Flush only this connection's buffered ticks when WS closes
            async def _flush_remaining():
                for key in list(connection_keys):
                    bars = _pending_bars.pop(key, [])
                    if bars:
                        await _flush_to_redis(key, bars)

            # Run both directions concurrently; when either finishes, cancel the other
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(client_to_backend()),
                    asyncio.create_task(backend_to_client()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()

            # Flush remaining buffered bars
            await _flush_remaining()

    except (websockets.exceptions.WebSocketException, OSError) as exc:
        logger.warning("Backend WS connection failed: %s", exc)
    finally:
        # Ensure client socket is closed
        try:
            await websocket.close()
        except Exception:
            pass
        logger.info("WS proxy closed: user=%s market=%s", user_id, market)
