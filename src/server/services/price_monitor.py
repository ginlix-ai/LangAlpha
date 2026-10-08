"""
PriceMonitorService — Monitors prices via MarketDataFeed
and triggers price-based automations when conditions are met.

Supports stock and index markets. Uses Redis SET NX locks for
multi-instance deduplication. Falls back to REST snapshot polling when WS is
disconnected, and polls non-US symbols while their venue's session runs, since
the live-data WebSocket carries US venues only.
"""

import asyncio
import logging
import math
import numbers
import time
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from market_protocol import (
    AssetClass,
    InstrumentRef,
    MarketPhase,
    display_spelling,
    market_of,
    to_canonical,
)

from src.data_client.ginlix_data.data_source import INDEX_TICKERS, index_ticker
from src.server.models.automation import (
    MarketType,
    PriceConditionType,
    PriceTriggerConfig,
    RetriggerMode,
)
from src.data_client.instrument_clock import clock_for, clock_for_ref, is_us_class_share
from src.server.services.market_data_feed import MarketDataFeed

logger = logging.getLogger(__name__)

_REFRESH_INTERVAL = 60  # seconds — reload automations from DB
_POLL_INTERVAL = 30  # seconds — REST fallback polling
_REFERENCE_REFRESH_INTERVAL = 300  # seconds — refresh reference prices
_ONE_SHOT_DEDUP_TTL = 300  # seconds — must exceed _REFRESH_INTERVAL to avoid re-trigger races
_MIN_TRADING_DAY_TTL = 300  # 5 min floor for trading-day TTL

# Service-account identity for background service-to-service data calls.
# Matches the principal the live-data WS stream already sends so both
# transports authenticate the same way.
_SERVICE_USER_ID = "langalpha-service"

# ─── Symbol normalization ───────────────────────────────────────────

# Display symbol → bare symbol (for REST snapshot response → automation lookup)
# _normalize_snapshot maps e.g. I:SPX → GSPC, I:COMP → IXIC; we map those back to bare.
_DISPLAY_TO_BARE: Dict[str, str] = {
    display: wire.removeprefix("I:") for display, wire in INDEX_TICKERS.items()
}


def _to_ws_symbol(symbol: str, market: MarketType) -> str:
    """Bare symbol → ginlix-data wire format (for WS subscriptions)."""
    if market == MarketType.INDEX:
        return index_ticker(symbol)
    return symbol.upper()


def _from_ws_symbol(ws_symbol: str) -> str:
    """Wire format → bare symbol (for WS bar → automation lookup)."""
    if ws_symbol.startswith("I:"):
        return ws_symbol[2:]
    return ws_symbol


def _from_display_symbol(symbol: str) -> str:
    """Display symbol → bare symbol (for REST snapshot → automation lookup)."""
    return _DISPLAY_TO_BARE.get(symbol, symbol)


def _now_utc() -> datetime:
    """Return current UTC time. Extracted for testability."""
    return datetime.now(timezone.utc)


def _positive(value: Any) -> Optional[float]:
    """*value* as a float when it is a positive real number, else None.

    A snapshot row can hold one field null while others carry data (the
    provider drops only rows null throughout), so a number read off a row is
    checked before anything compares it.
    """
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        return None
    value = float(value)
    return value if math.isfinite(value) and value > 0 else None


@lru_cache(maxsize=4096)
def watched_listing(symbol: str, market: MarketType) -> Optional[InstrumentRef]:
    """The listing a price alert watches, or None for a spelling the protocol cannot read.

    The alert's market decides what a bare spelling names: ``HSI`` on an index
    alert is the Hang Seng, which trades in Hong Kong, where the same letters
    on a stock alert are a US ticker.
    """
    try:
        return to_canonical(
            symbol,
            asset_class=AssetClass.INDEX if market == MarketType.INDEX else AssetClass.EQUITY,
        )
    except ValueError:
        return None


def watched_market(symbol: str, market: MarketType) -> Optional[str]:
    """The market an alert's listing is watched in (``us``, ``cn``, ``hk`` ...), None when unreadable.

    A dotted class share (BRK.B, BF.B) parses to the unknown venue, since the
    protocol knows no such suffix, yet the WebSocket carries it as the US
    ticker it is. Any other unknown suffix stays ``other`` and is polled.
    """
    ref = watched_listing(symbol, market)
    if ref is None:
        return None
    return "us" if is_us_class_share(ref) else market_of(ref)


def _is_us_symbol(symbol: str, market: MarketType) -> bool:
    """True when the alert's listing trades on the US tape, the only one the
    live-data WebSocket carries, so everything else is evaluated by the REST poll."""
    return watched_market(symbol, market) in (None, "us")


def _session_day_start_ms(symbol: str, market: MarketType, now: datetime) -> Optional[int]:
    """Start of the venue's trading day while its regular session runs at *now*, else None.

    Only the regular session counts: a non-US quote is frozen outside it and
    through a lunch break, so a poll then spends an upstream call on a price
    that cannot cross anything. The day starts at local midnight rather than at
    the open because the opening auction prints before the open.
    """
    ref = watched_listing(symbol, market)
    if ref is None:
        return None
    try:
        clock = clock_for_ref(ref)
        if clock.phase(now) is not MarketPhase.REGULAR:
            return None
        day = now.astimezone(clock.tz).date()
    except Exception:
        # The poll pass covers every venue at once, so a calendar that fails
        # skips only its own listings rather than every alert in the pass.
        logger.error("[PriceMonitor] No session clock for %s", symbol, exc_info=True)
        return None
    return int(datetime(day.year, day.month, day.day, tzinfo=clock.tz).timestamp() * 1000)


def _seconds_until_next_market_open(
    symbol: Optional[str] = None, market: MarketType = MarketType.STOCK,
) -> int:
    """Seconds until the alert's venue opens its next session; floored at _MIN_TRADING_DAY_TTL.

    The clock is the listing the alert watches, read with the alert's market,
    so a CN or HK automation is not locked out until New York reopens and a
    stock alert on HSI, a US ticker, does not wait for Hong Kong. A missing or
    unreadable symbol is the US clock.
    """
    ref = watched_listing(symbol, market) if symbol else None
    clock = clock_for_ref(ref) if ref is not None else clock_for(None)
    return max(int(clock.seconds_until_next_session_open(_now_utc())), _MIN_TRADING_DAY_TTL)


async def _fetch_snapshots(
    provider, stock_syms: List[str], index_syms: List[str],
) -> List[tuple[str, dict]]:
    """``(bare symbol, snapshot)`` for every row either market returned.

    The two calls are independent, so one failing costs only its own rows.
    Index rows come back in display spelling (``GSPC``) and are mapped to the
    bare symbol an automation is keyed on (``SPX``). A row keeps the spelling
    of the provider that served it, so a fallback vendor's ``600519.SS`` is
    respelled to the ``600519.SH`` an automation is keyed on.
    """
    async def _fetch(symbols: List[str], asset_type: str) -> list[dict]:
        if not symbols:
            return []
        try:
            return await provider.get_snapshots(
                symbols, asset_type=asset_type, user_id=_SERVICE_USER_ID
            )
        except Exception:
            logger.debug("[PriceMonitor] %s snapshot fetch failed", asset_type, exc_info=True)
            return []

    stocks, indices = await asyncio.gather(
        _fetch(stock_syms, "stocks"), _fetch(index_syms, "indices")
    )
    pairs = [(display_spelling(str(snap.get("symbol") or "")), snap) for snap in stocks]
    pairs += [
        (_from_display_symbol(display_spelling(str(snap.get("symbol") or ""))), snap)
        for snap in indices
    ]
    return [(sym, snap) for sym, snap in pairs if sym]


class ConditionEvaluator:
    """Evaluates price conditions against current prices."""

    def __init__(self):
        # symbol → {previous_close, day_open}
        self._reference_prices: Dict[str, Dict[str, float]] = {}

    def set_reference(
        self, symbol: str, previous_close: Optional[float], day_open: Optional[float]
    ) -> None:
        # A reference a row left null reads as missing (0), which evaluate
        # already treats as no reference, instead of failing every compare.
        self._reference_prices[symbol] = {
            "previous_close": _positive(previous_close) or 0.0,
            "day_open": _positive(day_open) or 0.0,
        }

    def evaluate(
        self,
        condition_type: str,
        value: float,
        reference: str,
        current_price: float,
        symbol: str,
    ) -> bool:
        """Evaluate a single condition. Returns True if condition is met."""
        if condition_type == PriceConditionType.PRICE_ABOVE:
            return current_price > value
        elif condition_type == PriceConditionType.PRICE_BELOW:
            return current_price < value
        elif condition_type in (
            PriceConditionType.PCT_CHANGE_ABOVE,
            PriceConditionType.PCT_CHANGE_BELOW,
        ):
            ref_prices = self._reference_prices.get(symbol)
            if not ref_prices:
                return False
            ref_price = ref_prices.get(reference, 0)
            if ref_price <= 0:
                return False
            pct_change = ((current_price - ref_price) / ref_price) * 100
            if condition_type == PriceConditionType.PCT_CHANGE_ABOVE:
                return pct_change > value
            else:
                return pct_change < -value
        return False

    async def refresh_references(
        self,
        symbols: List[str],
        symbol_markets: Optional[Dict[str, MarketType]] = None,
    ) -> None:
        """Fetch reference prices (previous_close, day_open) via REST snapshots.

        Splits symbols by market type for correct asset_type routing.
        """
        if not symbols:
            return
        try:
            from src.data_client import get_market_data_provider

            provider = await get_market_data_provider()
            markets = symbol_markets or {}
            index_syms = [s for s in symbols if markets.get(s) == MarketType.INDEX]
            stock_syms = [s for s in symbols if markets.get(s) != MarketType.INDEX]
            for sym, snap in await _fetch_snapshots(provider, stock_syms, index_syms):
                self.set_reference(
                    sym,
                    previous_close=snap.get("previous_close"),
                    day_open=snap.get("open"),
                )
        except Exception:
            logger.warning("Failed to refresh reference prices", exc_info=True)


class PriceMonitorService:
    """Monitors prices and triggers price-based automations.

    Supports stock and index markets with separate WS connections.
    """

    _instance: Optional["PriceMonitorService"] = None

    @classmethod
    def get_instance(cls) -> "PriceMonitorService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        # WS instances and consumer handles per market
        self._stock_ws: Optional[MarketDataFeed] = None
        self._index_ws: Optional[MarketDataFeed] = None
        self._stock_handle = None
        self._index_handle = None

        self._evaluator = ConditionEvaluator()

        # bare symbol → [automation dicts]  (keyed by user-entered symbol)
        self._symbol_automations: Dict[str, List[Dict[str, Any]]] = {}
        # bare symbol → MarketType  (for routing REST calls)
        self._symbol_markets: Dict[str, MarketType] = {}
        # ws_symbol sets per market (for WS subscription diffing)
        self._stock_ws_symbols: set[str] = set()
        self._index_ws_symbols: set[str] = set()

        # In-memory dedup fallback when Redis is unavailable
        # automation_id → expiry timestamp (monotonic)
        self._local_locks: Dict[str, float] = {}
        self._redis_warned = False

        # Background tasks
        self._refresh_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._ref_refresh_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    @property
    def _monitored_symbols(self) -> set[str]:
        """All bare monitored symbols (for backward compat and logging)."""
        return set(self._symbol_automations.keys())

    async def start(self) -> None:
        """Start the price monitor."""
        self._stock_ws = MarketDataFeed.get_instance("stock", "second", "realtime")
        self._index_ws = MarketDataFeed.get_instance("index", "second", "delayed")
        self._shutdown_event.clear()

        # Initial load of price automations
        await self._load_automations()

        # Register as consumers on both WS instances
        self._stock_handle = self._stock_ws.register_consumer(
            "price_monitor_stock", self._on_message
        )
        self._index_handle = self._index_ws.register_consumer(
            "price_monitor_index", self._on_message
        )

        # Subscribe per market
        if self._stock_ws_symbols:
            await self._stock_handle.subscribe(list(self._stock_ws_symbols))
        if self._index_ws_symbols:
            await self._index_handle.subscribe(list(self._index_ws_symbols))

        # Start background loops
        self._refresh_task = asyncio.create_task(
            self._refresh_loop(), name="price_monitor_refresh"
        )
        self._poll_task = asyncio.create_task(
            self._poll_fallback_loop(), name="price_monitor_poll"
        )
        self._ref_refresh_task = asyncio.create_task(
            self._reference_refresh_loop(), name="price_monitor_ref_refresh"
        )

        logger.info(
            "[PriceMonitor] Started — monitoring %d symbols (%d stock, %d index) from %d automations",
            len(self._monitored_symbols),
            len(self._stock_ws_symbols), len(self._index_ws_symbols),
            sum(len(v) for v in self._symbol_automations.values()),
        )

    async def stop(self) -> None:
        """Stop the price monitor."""
        logger.info("[PriceMonitor] Stopping...")
        self._shutdown_event.set()

        for task in (self._refresh_task, self._poll_task, self._ref_refresh_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        for handle in (self._stock_handle, self._index_handle):
            if handle:
                await handle.close()
        self._stock_handle = None
        self._index_handle = None

        logger.info("[PriceMonitor] Stopped")

    # ─── Message Handling ────────────────────────────────────────────

    async def _on_message(self, raw_msg: str, bar: Optional[dict]) -> None:
        """Callback from MarketDataFeed on each price tick."""
        if not bar:
            return

        # Normalize wire symbol to bare (e.g. I:SPX → SPX)
        bare_symbol = _from_ws_symbol(bar["symbol"])
        current_price = bar["close"]

        automations = self._symbol_automations.get(bare_symbol, [])
        for automation in automations:
            try:
                await self._evaluate_and_trigger(automation, current_price)
            except Exception:
                logger.error(
                    "[PriceMonitor] Error evaluating automation %s",
                    automation.get("automation_id"),
                    exc_info=True,
                )

    async def _evaluate_and_trigger(
        self, automation: Dict[str, Any], current_price: float
    ) -> None:
        """Evaluate conditions for an automation and trigger if all are met."""
        config = automation.get("_parsed_config")
        if config is None:
            trigger_config = automation.get("trigger_config", {})
            try:
                config = PriceTriggerConfig(**trigger_config)
            except Exception:
                return

        symbol = config.symbol.upper()

        # All conditions must be met (AND logic)
        for condition in config.conditions:
            if not self._evaluator.evaluate(
                condition.type, condition.value, condition.reference,
                current_price, symbol,
            ):
                return  # At least one condition not met

        # All conditions met — try to trigger
        await self._try_trigger(automation, config, current_price)

    async def _try_trigger(
        self,
        automation: Dict[str, Any],
        config: PriceTriggerConfig,
        current_price: float,
    ) -> None:
        """Attempt to trigger an automation with Redis dedup lock."""
        automation_id = str(automation["automation_id"])
        lock_key = f"price_trigger:{automation_id}:lock"

        # Determine lock TTL based on retrigger mode
        retrigger = config.retrigger
        if retrigger.mode == RetriggerMode.ONE_SHOT:
            lock_ttl = _ONE_SHOT_DEDUP_TTL
        else:  # RECURRING
            if retrigger.cooldown_seconds is not None:
                lock_ttl = retrigger.cooldown_seconds
            else:
                lock_ttl = _seconds_until_next_market_open(config.symbol, config.market)

        # Try to acquire dedup lock — Redis preferred, in-memory fallback
        acquired = await self._acquire_lock(automation_id, lock_key, lock_ttl, current_price)
        if not acquired:
            return

        logger.info(
            "[PriceMonitor] Triggering automation %s — %s price=%.2f",
            automation_id, config.symbol, current_price,
        )

        from src.server.database import automation as auto_db
        from src.server.services.automation_scheduler import AutomationScheduler

        scheduler = AutomationScheduler.get_instance()
        try:
            # 'executing' before dispatch, so a reload excludes it.
            execution_id = await auto_db.claim_price_firing(
                automation_id, scheduler.server_id
            )
        except Exception:
            logger.error(
                "[PriceMonitor] Failed to claim a firing for %s",
                automation_id, exc_info=True,
            )
            await self._hold_until_reload(automation_id, lock_key, lock_ttl)
            return
        if execution_id is None:
            logger.info(
                "[PriceMonitor] Not triggering %s: no longer active",
                automation_id,
            )
            await self._hold_until_reload(automation_id, lock_key, lock_ttl)
            return

        scheduler.dispatch(
            automation, execution_id, name=f"price_exec_{automation_id[:8]}"
        )

    # ─── Dedup Locking ────────────────────────────────────────────────

    async def _acquire_lock(
        self, automation_id: str, lock_key: str, lock_ttl: int, current_price: float
    ) -> bool:
        """Acquire a dedup lock via Redis, falling back to in-memory."""
        # Try Redis first
        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if cache.enabled and cache.client:
                lock_value = f"{current_price}:{datetime.now(timezone.utc).isoformat()}"
                acquired = await cache.client.set(lock_key, lock_value, nx=True, ex=lock_ttl)
                return bool(acquired)
        except Exception:
            pass

        # In-memory fallback (single-instance dedup only)
        if not self._redis_warned:
            self._redis_warned = True
            logger.warning("[PriceMonitor] Redis unavailable — using in-memory dedup locks")

        now = time.monotonic()
        # Clean expired entries
        self._local_locks = {k: v for k, v in self._local_locks.items() if v > now}

        if automation_id in self._local_locks:
            return False  # still locked

        self._local_locks[automation_id] = now + lock_ttl
        return True

    async def _hold_until_reload(
        self, automation_id: str, lock_key: str, lock_ttl: int
    ) -> None:
        """Keep the lock of a trigger that fired nothing, refused or unclaimed,
        only until the next reload, which drops an alert that is no longer
        active: the full TTL would mute the alert for the day, even one paused
        and resumed within it."""
        if lock_ttl <= _REFRESH_INTERVAL:
            return
        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if cache.enabled and cache.client:
                await cache.client.expire(lock_key, _REFRESH_INTERVAL)
        except Exception:
            pass
        if automation_id in self._local_locks:
            self._local_locks[automation_id] = time.monotonic() + _REFRESH_INTERVAL

    # ─── Background Loops ────────────────────────────────────────────

    async def _refresh_loop(self) -> None:
        """Periodically reload price automations from DB and update subscriptions."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=_REFRESH_INTERVAL
                )
                return  # shutdown
            except asyncio.TimeoutError:
                pass

            try:
                await self._load_automations()
            except Exception:
                logger.error("[PriceMonitor] Refresh failed", exc_info=True)

    async def _load_automations(self) -> None:
        """Load active price automations and update symbol subscriptions."""
        from src.server.database import automation as auto_db

        automations = await auto_db.get_active_price_automations()

        new_symbol_map: Dict[str, List[Dict[str, Any]]] = {}
        new_symbol_markets: Dict[str, MarketType] = {}
        new_stock_ws: set[str] = set()
        new_index_ws: set[str] = set()

        for auto in automations:
            trigger_config = auto.get("trigger_config", {})
            try:
                config = PriceTriggerConfig(**trigger_config)
            except Exception:
                logger.warning(
                    "[PriceMonitor] Invalid trigger_config for automation %s",
                    auto.get("automation_id"),
                )
                continue

            bare = config.symbol.upper()
            market = config.market

            new_symbol_markets[bare] = market
            # The feeds carry the US tape only, where 600519.SH or I:HSI names
            # nothing; the REST poll covers every other venue in its session.
            if _is_us_symbol(bare, market):
                ws_symbols = new_index_ws if market == MarketType.INDEX else new_stock_ws
                ws_symbols.add(_to_ws_symbol(bare, market))

            auto["_parsed_config"] = config
            new_symbol_map.setdefault(bare, []).append(auto)

        # Diff stock subscriptions
        stock_added = new_stock_ws - self._stock_ws_symbols
        stock_removed = self._stock_ws_symbols - new_stock_ws
        if self._stock_handle:
            if stock_removed:
                await self._stock_handle.unsubscribe(list(stock_removed))
            if stock_added:
                await self._stock_handle.subscribe(list(stock_added))

        # Diff index subscriptions
        index_added = new_index_ws - self._index_ws_symbols
        index_removed = self._index_ws_symbols - new_index_ws
        if self._index_handle:
            if index_removed:
                await self._index_handle.unsubscribe(list(index_removed))
            if index_added:
                await self._index_handle.subscribe(list(index_added))

        # Refresh reference prices for newly added bare symbols
        old_bare = set(self._symbol_automations.keys())
        new_bare = set(new_symbol_map.keys())
        added_bare = new_bare - old_bare
        if added_bare:
            await self._evaluator.refresh_references(
                list(added_bare), symbol_markets=new_symbol_markets
            )

        self._symbol_automations = new_symbol_map
        self._symbol_markets = new_symbol_markets
        self._stock_ws_symbols = new_stock_ws
        self._index_ws_symbols = new_index_ws

        total_added = len(stock_added) + len(index_added)
        total_removed = len(stock_removed) + len(index_removed)
        if total_added or total_removed:
            logger.info(
                "[PriceMonitor] Subscriptions updated: +%d -%d (total %d symbols, %d automations)",
                total_added, total_removed, len(new_bare),
                sum(len(v) for v in new_symbol_map.values()),
            )

    async def _poll_fallback_loop(self) -> None:
        """REST poll: the fallback while a WS feed is down, and the only feed a
        non-US venue has. ``_poll_snapshots`` decides what each pass fetches."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=_POLL_INTERVAL
                )
                return
            except asyncio.TimeoutError:
                pass

            if not self._monitored_symbols:
                continue

            try:
                await self._poll_snapshots(
                    poll_stock=bool(self._stock_ws and not self._stock_ws.is_connected),
                    poll_index=bool(self._index_ws and not self._index_ws.is_connected),
                )
            except Exception:
                logger.error("[PriceMonitor] REST poll failed", exc_info=True)

    async def _poll_snapshots(
        self, poll_stock: bool = True, poll_index: bool = True
    ) -> None:
        """Fetch current prices via REST and evaluate conditions.

        A US symbol is polled while its WS feed is down. A non-US symbol has no
        WS feed, so it is polled while its venue's regular session runs and
        never outside it; with none in session and the feeds up, a pass makes
        no upstream call.
        """
        now = _now_utc()
        day_starts: Dict[str, int] = {}
        stock_syms: List[str] = []
        index_syms: List[str] = []
        for bare, market in self._symbol_markets.items():
            if _is_us_symbol(bare, market):
                if not (poll_index if market == MarketType.INDEX else poll_stock):
                    continue
            else:
                day_start = _session_day_start_ms(bare, market, now)
                if day_start is None:
                    continue
                day_starts[bare] = day_start
            (index_syms if market == MarketType.INDEX else stock_syms).append(bare)
        if not stock_syms and not index_syms:
            return

        from src.data_client import get_market_data_provider

        provider = await get_market_data_provider()
        for bare_symbol, snapshot in await _fetch_snapshots(provider, stock_syms, index_syms):
            current_price = _positive(snapshot.get("price"))
            if current_price is None:
                continue
            # A row printed before the session's day began has not rolled yet:
            # its change is the previous session's move, which a recurring
            # alert unlocked at the open would fire on. A row with no print
            # time cannot show it rolled, so it is skipped too. US rows have
            # no bound.
            as_of = _positive(snapshot.get("as_of"))
            day_start = day_starts.get(bare_symbol)
            if day_start is not None and (as_of is None or as_of < day_start):
                continue

            # Update reference prices
            if _positive(snapshot.get("previous_close")) is not None:
                self._evaluator.set_reference(
                    bare_symbol,
                    previous_close=snapshot.get("previous_close"),
                    day_open=snapshot.get("open"),
                )

            for automation in self._symbol_automations.get(bare_symbol, []):
                try:
                    await self._evaluate_and_trigger(automation, current_price)
                except Exception:
                    logger.error(
                        "[PriceMonitor] Poll eval error for %s",
                        automation.get("automation_id"),
                        exc_info=True,
                    )

    async def _reference_refresh_loop(self) -> None:
        """Periodically refresh reference prices for all monitored symbols."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=_REFERENCE_REFRESH_INTERVAL
                )
                return
            except asyncio.TimeoutError:
                pass

            if self._monitored_symbols:
                try:
                    await self._evaluator.refresh_references(
                        list(self._monitored_symbols),
                        symbol_markets=self._symbol_markets,
                    )
                except Exception:
                    logger.debug("[PriceMonitor] Reference refresh failed", exc_info=True)
