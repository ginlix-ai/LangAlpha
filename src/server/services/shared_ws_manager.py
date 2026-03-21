"""
SharedWSConnectionManager — Single upstream WebSocket to ginlix-data,
shared by all consumers (frontend WS proxy clients, PriceMonitorService, etc.).

Ref-counts symbol subscriptions so the upstream only subscribes/unsubscribes
when the aggregate count transitions 0↔1.
"""

import asyncio
import json
import logging
import os
from collections import defaultdict
from typing import Any, Callable, Coroutine, Optional

import websockets

from src.config.settings import GINLIX_DATA_WS_URL

logger = logging.getLogger(__name__)

_INTERNAL_SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")

# Reconnection
_INITIAL_BACKOFF = 1.0  # seconds
_MAX_BACKOFF = 30.0

# Default WS feed configurations: (market, interval, tier)
DEFAULT_WS_FEEDS: list[tuple[str, str, str]] = [
    ("stock", "second", "realtime"),
    ("index", "second", "delayed"),
]

# Type alias for consumer callbacks
OnMessage = Callable[[str, Optional[dict]], Coroutine[Any, Any, None]]
"""async def callback(raw_msg: str, parsed_bar: dict | None) -> None"""


def parse_ws_bar(raw_msg: str) -> Optional[dict]:
    """Parse a WS message into a normalised bar dict.

    Returns ``None`` for non-aggregate messages (status, keepalive, etc.).
    Extracted from market_data_ws.py for shared use.
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
            symbol = msg.get("sym")
            o, h, l, c, v = msg.get("o"), msg.get("h"), msg.get("l"), msg.get("c"), msg.get("v")
            ts = msg.get("s") or msg.get("e")
        elif msg.get("type") == "aggregate" and isinstance(msg.get("data"), dict):
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


class WSConsumerHandle:
    """Handle for a registered consumer to manage subscriptions."""

    def __init__(self, consumer_id: str, manager: "SharedWSConnectionManager"):
        self._consumer_id = consumer_id
        self._manager = manager
        self._subscribed_symbols: set[str] = set()

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    @property
    def subscribed_symbols(self) -> set[str]:
        return self._subscribed_symbols.copy()

    async def subscribe(self, symbols: list[str]) -> None:
        symbols_upper = [s.upper() for s in symbols]
        new_symbols = [s for s in symbols_upper if s not in self._subscribed_symbols]
        if not new_symbols:
            return
        self._subscribed_symbols.update(new_symbols)
        await self._manager._consumer_subscribe(self._consumer_id, new_symbols)

    async def unsubscribe(self, symbols: list[str]) -> None:
        symbols_upper = [s.upper() for s in symbols]
        remove = [s for s in symbols_upper if s in self._subscribed_symbols]
        if not remove:
            return
        self._subscribed_symbols -= set(remove)
        await self._manager._consumer_unsubscribe(self._consumer_id, remove)

    async def close(self) -> None:
        """Unregister this consumer and remove all its subscriptions."""
        if self._subscribed_symbols:
            await self._manager._consumer_unsubscribe(
                self._consumer_id, list(self._subscribed_symbols)
            )
            self._subscribed_symbols.clear()
        self._manager._remove_consumer(self._consumer_id)


class SharedWSConnectionManager:
    """Manages an upstream WS connection to ginlix-data with ref-counted subscriptions.

    Instances are keyed by (market, interval, tier) — one upstream connection per combo.
    """

    _instances: dict[tuple[str, str, str], "SharedWSConnectionManager"] = {}

    @classmethod
    def get_instance(
        cls,
        market: str = "stock",
        interval: str = "second",
        tier: str = "realtime",
    ) -> "SharedWSConnectionManager":
        key = (market, interval, tier)
        if key not in cls._instances:
            cls._instances[key] = cls(market=market, interval=interval, tier=tier)
        return cls._instances[key]

    @classmethod
    def all_instances(cls) -> list["SharedWSConnectionManager"]:
        """Return all created instances (for lifecycle management)."""
        return list(cls._instances.values())

    def __init__(self, *, market: str = "stock", interval: str = "second", tier: str = "realtime"):
        # Consumer tracking
        self._consumers: dict[str, OnMessage] = {}  # consumer_id → callback
        self._consumer_symbols: dict[str, set[str]] = {}  # consumer_id → {symbols}

        # Ref-counted symbol subscriptions
        self._symbol_refcount: dict[str, int] = defaultdict(int)
        self._subscribed_symbols: set[str] = set()  # symbols subscribed upstream

        # Connection state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        self._connection_task: Optional[asyncio.Task] = None

        # Configuration
        self._market = market
        self._interval = interval
        self._tier = tier

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._connected.is_set()

    # ─── Lifecycle ─────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the connection manager."""
        if not GINLIX_DATA_WS_URL:
            logger.warning("[SharedWS] GINLIX_DATA_WS_URL not set — WS disabled")
            return
        logger.info("[SharedWS] Starting SharedWSConnectionManager (%s/%s/%s)", self._market, self._interval, self._tier)
        self._shutdown_event.clear()
        self._connection_task = asyncio.create_task(
            self._connection_loop(), name=f"shared_ws_{self._market}_{self._interval}"
        )

    async def stop(self) -> None:
        """Stop the connection manager and close the upstream WS."""
        logger.info("[SharedWS] Stopping SharedWSConnectionManager")
        self._shutdown_event.set()
        self._connected.clear()

        if self._connection_task and not self._connection_task.done():
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass

        await self._close_ws()
        logger.info("[SharedWS] SharedWSConnectionManager stopped")

    # ─── Consumer Registration ─────────────────────────────────────

    def register_consumer(self, consumer_id: str, callback: OnMessage) -> WSConsumerHandle:
        """Register a consumer with a message callback. Returns a handle for subscription management."""
        self._consumers[consumer_id] = callback
        self._consumer_symbols[consumer_id] = set()
        logger.debug("[SharedWS] Consumer registered: %s", consumer_id)
        return WSConsumerHandle(consumer_id, self)

    def _remove_consumer(self, consumer_id: str) -> None:
        self._consumers.pop(consumer_id, None)
        self._consumer_symbols.pop(consumer_id, None)
        logger.debug("[SharedWS] Consumer removed: %s", consumer_id)

    # ─── Subscription Management ────────────────────────────────────

    async def _consumer_subscribe(self, consumer_id: str, symbols: list[str]) -> None:
        """Handle a consumer subscribing to symbols. Sends upstream if new."""
        if consumer_id not in self._consumer_symbols:
            return

        self._consumer_symbols[consumer_id].update(symbols)

        new_upstream: list[str] = []
        for sym in symbols:
            self._symbol_refcount[sym] += 1
            if self._symbol_refcount[sym] == 1:
                new_upstream.append(sym)

        if new_upstream:
            self._subscribed_symbols.update(new_upstream)
            await self._send_subscribe(new_upstream)

    async def _consumer_unsubscribe(self, consumer_id: str, symbols: list[str]) -> None:
        """Handle a consumer unsubscribing. Sends upstream unsubscribe if refcount hits 0."""
        remove_upstream: list[str] = []
        for sym in symbols:
            if self._symbol_refcount[sym] > 0:
                self._symbol_refcount[sym] -= 1
                if self._symbol_refcount[sym] == 0:
                    remove_upstream.append(sym)
                    del self._symbol_refcount[sym]

        if consumer_id in self._consumer_symbols:
            self._consumer_symbols[consumer_id] -= set(symbols)

        if remove_upstream:
            self._subscribed_symbols -= set(remove_upstream)
            await self._send_unsubscribe(remove_upstream)

    # ─── Upstream WS Communication ──────────────────────────────────

    async def _send_subscribe(self, symbols: list[str]) -> None:
        if not self._ws or not self.is_connected:
            return
        try:
            msg = json.dumps({"action": "subscribe", "symbols": symbols})
            await self._ws.send(msg)
            logger.debug("[SharedWS] Upstream subscribe: %s", symbols)
        except Exception as e:
            logger.warning("[SharedWS] Failed to send subscribe: %s", e)

    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        if not self._ws or not self.is_connected:
            return
        try:
            msg = json.dumps({"action": "unsubscribe", "symbols": symbols})
            await self._ws.send(msg)
            logger.debug("[SharedWS] Upstream unsubscribe: %s", symbols)
        except Exception as e:
            logger.warning("[SharedWS] Failed to send unsubscribe: %s", e)

    # ─── Connection Loop ────────────────────────────────────────────

    async def _connection_loop(self) -> None:
        """Maintain the upstream WS connection with exponential backoff."""
        backoff = _INITIAL_BACKOFF

        while not self._shutdown_event.is_set():
            try:
                await self._connect_and_receive()
                backoff = _INITIAL_BACKOFF  # reset on clean disconnect
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[SharedWS] Connection error: %s", e)

            self._connected.clear()

            if self._shutdown_event.is_set():
                return

            logger.info("[SharedWS] Reconnecting in %.1fs...", backoff)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=backoff
                )
                return  # shutdown requested during backoff
            except asyncio.TimeoutError:
                pass

            backoff = min(backoff * 2, _MAX_BACKOFF)

    async def _connect_and_receive(self) -> None:
        """Connect to ginlix-data WS and process messages."""
        url = (
            f"{GINLIX_DATA_WS_URL}/ws/v1/data/aggregates/{self._market}"
            f"?interval={self._interval}&tier={self._tier}"
        )
        headers = {}
        if _INTERNAL_SERVICE_TOKEN:
            headers["X-Service-Token"] = _INTERNAL_SERVICE_TOKEN
            headers["X-User-Id"] = "langalpha-service"

        logger.info("[SharedWS] Connecting to %s", url)

        async with websockets.connect(
            url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._connected.set()
            logger.info("[SharedWS] Connected")

            # Re-subscribe all currently tracked symbols
            if self._subscribed_symbols:
                await self._send_subscribe(list(self._subscribed_symbols))

            try:
                async for raw_msg in ws:
                    if self._shutdown_event.is_set():
                        return
                    await self._dispatch_message(raw_msg)
            except websockets.exceptions.ConnectionClosed:
                logger.info("[SharedWS] Upstream connection closed")
            finally:
                self._ws = None
                self._connected.clear()

    async def _dispatch_message(self, raw_msg: str) -> None:
        """Parse a message and dispatch to relevant consumers."""
        bar = parse_ws_bar(raw_msg)
        symbol = bar["symbol"] if bar else None

        # Build list of consumers to notify
        targets: list[OnMessage] = []
        for consumer_id, callback in list(self._consumers.items()):
            consumer_syms = self._consumer_symbols.get(consumer_id, set())
            # If it's an aggregate for a symbol the consumer subscribed to, or
            # it's a non-aggregate message (broadcast to all), deliver it
            if symbol is None or symbol in consumer_syms:
                targets.append(callback)

        # Dispatch to consumers concurrently
        if targets:
            await asyncio.gather(
                *(cb(raw_msg, bar) for cb in targets),
                return_exceptions=True,
            )

    async def _close_ws(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

