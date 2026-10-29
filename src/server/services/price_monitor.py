"""
PriceMonitorService — Monitors prices via MarketDataFeed
and triggers price-based automations when conditions are met.

Also watches the price targets threads set on their market watch, and the
price wakeups agents set: a hit is claimed in the database, which is the only
dedup either has, and the claim enqueues the notice for the outbox to
deliver. Both are held as target rows; a wakeup's is marked by its source.

Supports stock and index markets. Uses Redis SET NX locks for
multi-instance deduplication of automations. Falls back to REST snapshot
polling when WS is disconnected, and polls non-US symbols while their venue's
session runs, since the live-data WebSocket carries US venues only.
"""

import asyncio
import logging
import math
import numbers
import time
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Collection, Dict, List, Optional, Tuple

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
_TARGET_SWEEP_INTERVAL = 30  # seconds: claim due warnings, close lapsed targets
_TARGET_WARNING_BATCH = 100
_TARGET_RELOAD_DEBOUNCE = 1.0  # seconds: coalesce a burst of change nudges
_TARGET_SIGNAL_POLL = 5.0  # seconds: get_message window, bounds how fast stop is seen
_TARGET_SIGNAL_RETRY = 5.0  # seconds: before resubscribing to the change channel

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
# The reverse, for a snapshot request: the data service answers an index by
# its display spelling. Asked for bare ``COMP`` it returns no row at all, and
# bare ``SPX`` only reaches a fallback vendor.
_BARE_TO_DISPLAY: Dict[str, str] = {bare: display for display, bare in _DISPLAY_TO_BARE.items()}

# A bare symbol and the market it is read in, which is what names a listing:
# HSI is a US ticker as a stock and the Hang Seng as an index, so anything
# keyed by the bare symbol alone lets one stand in for the other.
Listing = Tuple[str, MarketType]


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


def _target_symbol(symbol: str, market: MarketType) -> str:
    """The bare key a target is watched under: the spelling its ticks and
    snapshot rows read back as.

    A target keeps the watch list's spelling, where a caret marks an index
    (``^GSPC``). The feed names that index ``I:SPX`` and a snapshot row
    ``GSPC``, both of which read back as ``SPX``, so keyed as stored it would
    never meet its own price.
    """
    spelled = display_spelling(symbol).upper()
    if market == MarketType.INDEX:
        return _from_display_symbol(spelled.lstrip("^"))
    return spelled


def _targets_by_listing(rows: List[Dict[str, Any]]) -> Dict[Listing, List[Dict[str, Any]]]:
    """Armed target rows by the listing they are watched under, each marked
    with that listing and the epoch ms it was set at."""
    by_listing: Dict[Listing, List[Dict[str, Any]]] = {}
    for row in rows:
        try:
            market = MarketType(row["market"])
        except ValueError:
            logger.warning(
                "[PriceMonitor] Unknown market %r on target %s",
                row.get("market"), row.get("target_id"),
            )
            continue
        listing = (_target_symbol(str(row["symbol"]), market), market)
        row["_listing"] = listing
        # Whole epoch ms, as ticks and snapshot rows stamp their prints,
        # so a print in the millisecond the target was set counts.
        row["_created_ms"] = int(row["created_at"].timestamp() * 1000)
        by_listing.setdefault(listing, []).append(row)
    return by_listing


# A price wakeup in the monitor is a target row with its own claim, so every
# path that reads targets (evaluation, drops, the scoped reload) reads it too.
_WAKEUP_SOURCE = "wakeup"


def _wakeup_as_target(row: Dict[str, Any]) -> Dict[str, Any]:
    """An armed price wakeup in the shape the monitor holds a target in."""
    return {
        "target_id": row["wakeup_id"],
        "_source": _WAKEUP_SOURCE,
        "conversation_thread_id": row["conversation_thread_id"],
        "symbol": row["symbol"],
        "market": row.get("market") or MarketType.STOCK.value,
        "kind": row.get("price_kind"),
        "value": row.get("price_value"),
        "pct_from": row.get("pct_from"),
        "armed_price": row.get("armed_price"),
        "expires_at": row["expires_at"],
        "created_at": row["created_at"],
    }


async def _armed_rows(thread_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Armed targets and price wakeups, of every thread or of *thread_ids*."""
    from src.server.database import market_targets as targets_db
    from src.server.database.wakeups import read as wakeups_read

    if thread_ids is None:
        targets = await targets_db.list_armed_targets()
        wakeups = await wakeups_read.list_armed_price_wakeups()
    else:
        targets = await targets_db.list_armed_targets(thread_ids=thread_ids)
        wakeups = await wakeups_read.list_armed_price_wakeups(thread_ids)
    return [*targets, *(_wakeup_as_target(row) for row in wakeups)]


def _thread_key(value: Any) -> Optional[str]:
    """*value* spelled as a target row spells its thread id, or None when it
    names no thread (no payload, or text that is not an id), which asks for
    a re-read of every thread."""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if value is None:
        return None
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def _now_utc() -> datetime:
    """Return current UTC time. Extracted for testability."""
    return datetime.now(timezone.utc)


def _nudge_hook_drainer() -> None:
    """Post-commit hint that a notice was enqueued; the drainer's poll in
    every worker is what delivers it, so a failed nudge is not an error."""
    try:
        from src.server.services.hook_outbox import HookOutboxDrainer

        HookOutboxDrainer.get_instance().nudge()
    except Exception:
        logger.debug("[PriceMonitor] hook drainer nudge failed", exc_info=True)


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
    user_id: str = _SERVICE_USER_ID,
) -> List[Tuple[Listing, dict]]:
    """``((bare symbol, market), snapshot)`` for every row either market returned.

    The two calls are independent, so one failing costs only its own rows.
    A row's market is the call that returned it, since both answer HSI as
    ``HSI``. Index rows come back in display spelling (``GSPC``) and are
    mapped to the bare symbol an automation is keyed on (``SPX``). A row
    keeps the spelling of the provider that served it, so a fallback vendor's
    ``600519.SS`` is respelled to the ``600519.SH`` an automation is keyed on.
    """
    async def _fetch(symbols: List[str], asset_type: str) -> list[dict]:
        if not symbols:
            return []
        try:
            return await provider.get_snapshots(
                symbols, asset_type=asset_type, user_id=user_id
            )
        except Exception:
            logger.debug("[PriceMonitor] %s snapshot fetch failed", asset_type, exc_info=True)
            return []

    stocks, indices = await asyncio.gather(
        _fetch(stock_syms, "stocks"),
        _fetch([_BARE_TO_DISPLAY.get(s, s) for s in index_syms], "indices"),
    )
    def _spelled(snap: dict) -> str:
        return display_spelling(str(snap.get("symbol") or ""))

    pairs = [((_spelled(snap), MarketType.STOCK), snap) for snap in stocks]
    pairs += [
        ((_from_display_symbol(_spelled(snap)), MarketType.INDEX), snap) for snap in indices
    ]
    return [(listing, snap) for listing, snap in pairs if listing[0]]


async def listing_snapshots(
    provider, listings: Collection[Listing], user_id: str = _SERVICE_USER_ID,
) -> Dict[Listing, dict]:
    """Snapshots by the listing the monitor reads prices by, fetched as it
    fetches its reference prices.

    Whatever previews a level for an alert must measure from the row the
    monitor will: an index fetched as a stock comes from another vendor,
    whose previous close can still be a session behind before the open.
    """
    stock_syms = sorted({key for key, market in listings if market is not MarketType.INDEX})
    index_syms = sorted({key for key, market in listings if market is MarketType.INDEX})
    return dict(await _fetch_snapshots(provider, stock_syms, index_syms, user_id=user_id))


_TARGET_CONDITIONS: Dict[str, PriceConditionType] = {
    "above": PriceConditionType.PRICE_ABOVE,
    "below": PriceConditionType.PRICE_BELOW,
    "pct_up": PriceConditionType.PCT_CHANGE_ABOVE,
    "pct_down": PriceConditionType.PCT_CHANGE_BELOW,
}


def _pct_crossed(
    condition_type: str,
    value: float,
    ref_price: float,
    current_price: float,
    *,
    inclusive: bool = False,
) -> bool:
    """A % condition against *ref_price*; no reference (0) never crosses.
    ``inclusive`` also counts a move that lands exactly on *value*."""
    if ref_price <= 0:
        return False
    pct_change = ((current_price - ref_price) / ref_price) * 100
    if condition_type == PriceConditionType.PCT_CHANGE_ABOVE:
        return pct_change >= value if inclusive else pct_change > value
    return pct_change <= -value if inclusive else pct_change < -value


def target_hit(kind: Any, value: float, ref_price: float, price: float) -> bool:
    """Whether a target of ``kind`` at ``value`` is hit at ``price``.

    A target's kind is an automation condition under another name, but a
    touch hits it where an automation needs a cross: the tool promises an
    alert when the price reaches the level and refuses to set one the last
    price already sits on, so a print exactly on it is the hit. ``ref_price``
    is the base a % kind measures from; no base (0) never hits.
    """
    condition_type = _TARGET_CONDITIONS.get(kind)
    if condition_type is None:
        return False
    if condition_type == PriceConditionType.PRICE_ABOVE:
        return price >= value
    if condition_type == PriceConditionType.PRICE_BELOW:
        return price <= value
    return _pct_crossed(condition_type, value, ref_price, price, inclusive=True)


class ConditionEvaluator:
    """Evaluates price conditions against current prices."""

    def __init__(self):
        # listing → {previous_close, day_open}
        self._reference_prices: Dict[Listing, Dict[str, float]] = {}

    def set_reference(
        self, listing: Listing, previous_close: Optional[float], day_open: Optional[float]
    ) -> None:
        # A reference a row left null reads as missing (0), which evaluate
        # already treats as no reference, instead of failing every compare.
        self._reference_prices[listing] = {
            "previous_close": _positive(previous_close) or 0.0,
            "day_open": _positive(day_open) or 0.0,
        }

    def evaluate(
        self,
        condition_type: str,
        value: float,
        reference: str,
        current_price: float,
        listing: Listing,
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
            ref_prices = self._reference_prices.get(listing)
            if not ref_prices:
                return False
            return _pct_crossed(
                condition_type, value, ref_prices.get(reference, 0), current_price
            )
        return False

    def evaluate_target(
        self, target: Dict[str, Any], current_price: float, listing: Listing
    ) -> bool:
        """Whether a market-watch target is hit at *current_price*, by
        `target_hit`. A % target set ``pct_from='armed'`` measures from the
        price the target was set at, not from the previous close.
        """
        value = _positive(target.get("value"))
        if value is None:
            return False
        if target.get("pct_from") == "armed":
            ref_price = _positive(target.get("armed_price")) or 0.0
        else:
            ref_price = self._reference_prices.get(listing, {}).get("previous_close", 0.0)
        return target_hit(target.get("kind"), value, ref_price, current_price)

    async def refresh_references(self, listings: Collection[Listing]) -> None:
        """Fetch reference prices (previous_close, day_open) via REST snapshots."""
        if not listings:
            return
        try:
            from src.data_client import get_market_data_provider

            provider = await get_market_data_provider()
            for listing, snap in (await listing_snapshots(provider, listings)).items():
                self.set_reference(
                    listing,
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

        # listing → [automation dicts]  (keyed by user-entered symbol)
        self._symbol_automations: Dict[Listing, List[Dict[str, Any]]] = {}
        # listing → [armed target rows]; a copy of the table as of the
        # last load, which the database claim in fire_targets always overrules
        self._symbol_targets: Dict[Listing, List[Dict[str, Any]]] = {}
        # listings per source as of its last load, and merged (what the feeds
        # and the REST poll read)
        self._automation_listings: set[Listing] = set()
        self._target_listings: set[Listing] = set()
        self._listings: set[Listing] = set()
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
        self._target_sweep_task: Optional[asyncio.Task] = None
        self._target_signal_task: Optional[asyncio.Task] = None
        self._target_reload_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Loads of either kind diff the same subscription sets, and two
        # target loads must not land out of order, so loads take turns.
        self._load_lock = asyncio.Lock()
        self._targets_reload_requested = asyncio.Event()
        # What the pending reload covers: the threads the nudges named, or
        # every thread once one named none.
        self._targets_reload_threads: set[str] = set()
        self._targets_full_reload = False

    @property
    def _monitored_listings(self) -> set[Listing]:
        return set(self._symbol_automations) | set(self._symbol_targets)

    async def start(self) -> None:
        """Start the price monitor."""
        self._stock_ws = MarketDataFeed.get_instance("stock", "second", "realtime")
        self._index_ws = MarketDataFeed.get_instance("index", "second", "delayed")
        self._shutdown_event.clear()

        # Initial load of price automations
        await self._load_automations()
        try:
            await self._load_targets()
        except Exception:
            # Automations keep running; the refresh loop retries the targets.
            logger.error("[PriceMonitor] Initial target load failed", exc_info=True)

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
        self._target_sweep_task = asyncio.create_task(
            self._target_sweep_loop(), name="price_monitor_target_sweep"
        )
        self._target_signal_task = asyncio.create_task(
            self._target_signal_loop(), name="price_monitor_target_signal"
        )
        self._target_reload_task = asyncio.create_task(
            self._target_reload_loop(), name="price_monitor_target_reload"
        )

        logger.info(
            "[PriceMonitor] Started — monitoring %d listings (%d stock, %d index) "
            "from %d automations and %d targets",
            len(self._monitored_listings),
            len(self._stock_ws_symbols), len(self._index_ws_symbols),
            sum(len(v) for v in self._symbol_automations.values()),
            sum(len(v) for v in self._symbol_targets.values()),
        )

    async def stop(self) -> None:
        """Stop the price monitor."""
        logger.info("[PriceMonitor] Stopping...")
        self._shutdown_event.set()

        for task in (
            self._refresh_task, self._poll_task, self._ref_refresh_task,
            self._target_sweep_task, self._target_signal_task,
            self._target_reload_task,
        ):
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

        # Normalize wire symbol to bare (e.g. I:SPX → SPX). Only the index
        # feed spells a symbol with the I: prefix.
        market = MarketType.INDEX if bar["symbol"].startswith("I:") else MarketType.STOCK
        listing = (_from_ws_symbol(bar["symbol"]), market)
        current_price = bar["close"]

        automations = self._symbol_automations.get(listing, [])
        for automation in automations:
            try:
                await self._evaluate_and_trigger(automation, current_price)
            except Exception:
                logger.error(
                    "[PriceMonitor] Error evaluating automation %s",
                    automation.get("automation_id"),
                    exc_info=True,
                )

        if listing in self._symbol_targets:
            await self._evaluate_targets(listing, current_price, _positive(bar.get("time")))

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

        listing = (config.symbol.upper(), config.market)

        # All conditions must be met (AND logic)
        for condition in config.conditions:
            if not self._evaluator.evaluate(
                condition.type, condition.value, condition.reference,
                current_price, listing,
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

    # ─── Targets ─────────────────────────────────────────────────────

    async def _evaluate_targets(
        self,
        listing: Listing,
        current_price: Any,
        printed_at: Optional[float],
    ) -> None:
        """Fire every target on *listing* the price hits.

        *printed_at* is when the price traded, in epoch ms, None when its
        source does not say. A target fires only on a price printed after it
        was set: the index feed runs about 15 minutes behind, and a closed
        venue's snapshot still carries the last session's move, neither of
        which is news to a target set since.
        """
        targets = self._symbol_targets.get(listing)
        price = _positive(current_price)
        if not targets or price is None:
            return
        now = _now_utc()
        hits: List[Dict[str, Any]] = []
        for target in tuple(targets):
            try:
                if target["expires_at"] <= now:
                    # Lapsed since the load; the sweep closes its row.
                    self._drop_target(target)
                    continue
                if printed_at is not None and printed_at < target["_created_ms"]:
                    continue
                hit = self._evaluator.evaluate_target(target, price, listing)
            except Exception:
                logger.error(
                    "[PriceMonitor] Error evaluating target %s",
                    target.get("target_id"), exc_info=True,
                )
                continue
            if hit:
                hits.append(target)
        if hits:
            await self._fire_targets(hits, price, printed_at)

    async def _fire_targets(
        self, targets: List[Dict[str, Any]], price: float, printed_at: Optional[float]
    ) -> None:
        """Claim the hits one price made, in one transaction per source: the
        feed waits on this, and a price crossing several targets would
        otherwise cost a round trip each. Every worker's monitor sees the
        same tick, and the guarded UPDATE in each claim is what lets exactly
        one of them win each target; the winner's transaction also enqueues
        the notice."""
        # Off this worker's books whatever the claim returns: a lost race, or
        # a claim that failed, would otherwise query on every tick until a
        # load restores a target that is still armed.
        for target in targets:
            self._drop_target(target)
        # The alert says when the price traded, which on a delayed feed is
        # well before the claim.
        traded_at = (
            datetime.fromtimestamp(printed_at / 1000, tz=timezone.utc)
            if printed_at is not None else None
        )
        wakeups = [t for t in targets if t.get("_source") == _WAKEUP_SOURCE]
        legacy = [t for t in targets if t.get("_source") != _WAKEUP_SOURCE]
        if legacy:
            await self._claim_targets(legacy, price, traded_at)
        if wakeups:
            await self._claim_wakeups(wakeups, price, traded_at)

    async def _claim_wakeups(
        self, targets: List[Dict[str, Any]], price: float, traded_at: Optional[datetime]
    ) -> None:
        from src.server.database.wakeups import fire as wakeups_fire

        wakeup_ids = [target["target_id"] for target in targets]
        try:
            rows = await wakeups_fire.fire_price_wakeups(
                wakeup_ids, price, printed_at=traded_at
            )
        except Exception:
            logger.error(
                "[PriceMonitor] Failed to claim hits for price wakeups %s",
                ", ".join(wakeup_ids), exc_info=True,
            )
            for thread_id in {target["conversation_thread_id"] for target in targets}:
                self.request_targets_reload(thread_id)
            return
        for row in rows:
            logger.info(
                "[PriceMonitor] Wakeup %s hit: %s %s %s price=%.4f thread=%s",
                row.get("wakeup_id"), row.get("symbol"), row.get("price_kind"),
                row.get("price_value"), price, row.get("conversation_thread_id"),
            )
        if rows:
            _nudge_hook_drainer()

    async def _claim_targets(
        self, targets: List[Dict[str, Any]], price: float, traded_at: Optional[datetime]
    ) -> None:
        from src.server.database import market_targets as targets_db

        target_ids = [target["target_id"] for target in targets]
        try:
            rows = await targets_db.fire_targets(target_ids, price, printed_at=traded_at)
        except Exception:
            logger.error(
                "[PriceMonitor] Failed to claim hits for targets %s",
                ", ".join(target_ids), exc_info=True,
            )
            # Nothing was claimed, so the batch's threads are re-read after
            # the debounce rather than at the timed reload: their targets
            # that are still armed come back to be hit by a later price.
            for thread_id in {target["conversation_thread_id"] for target in targets}:
                self.request_targets_reload(thread_id)
            return
        claimed = {row["target_id"] for row in rows}
        for target_id in target_ids:
            if target_id not in claimed:
                logger.debug(
                    "[PriceMonitor] Target %s no longer armed (claimed elsewhere or settled)",
                    target_id,
                )
        for row in rows:
            logger.info(
                "[PriceMonitor] Target %s hit: %s %s %s price=%.4f thread=%s",
                row.get("target_id"), row.get("symbol"), row.get("kind"),
                row.get("value"), price, row.get("conversation_thread_id"),
            )
        if rows:
            _nudge_hook_drainer()

    def _drop_target(self, target: Dict[str, Any]) -> None:
        listing = target["_listing"]
        remaining = [
            t for t in self._symbol_targets.get(listing, ())
            if t["target_id"] != target["target_id"]
        ]
        if remaining:
            self._symbol_targets[listing] = remaining
        else:
            self._symbol_targets.pop(listing, None)

    def request_targets_reload(self, thread_id: Any = None) -> None:
        """Ask for a prompt re-read of armed targets, debounced: of
        *thread_id*'s alone when it names a thread, of every thread's when
        it names none. A hint only: the timed reload is what any change is
        guaranteed to reach."""
        thread = _thread_key(thread_id)
        if thread is None:
            self._targets_full_reload = True
        else:
            self._targets_reload_threads.add(thread)
        self._targets_reload_requested.set()

    async def _target_reload_loop(self) -> None:
        """Coalesce a burst of change nudges, several targets set in one turn,
        into one re-read of the threads they name."""
        while not self._shutdown_event.is_set():
            await self._targets_reload_requested.wait()
            await asyncio.sleep(_TARGET_RELOAD_DEBOUNCE)
            self._targets_reload_requested.clear()
            full, threads = self._targets_full_reload, self._targets_reload_threads
            self._targets_full_reload, self._targets_reload_threads = False, set()
            if self._shutdown_event.is_set():
                return
            try:
                await self._load_targets(None if full else threads)
            except Exception:
                logger.error("[PriceMonitor] Target reload failed", exc_info=True)

    async def _target_signal_loop(self) -> None:
        """Turn change nudges from any worker into a prompt reload here.

        Best effort by design: Redis off, a dropped subscription or a missed
        message only delays a change until the timed reload re-reads the table.
        """
        from src.server.services.market_targets.signals import TARGETS_CHANGED_CHANNEL
        from src.server.services.workspace_status_pubsub import get_shared_pubsub_client

        while not self._shutdown_event.is_set():
            pubsub = None
            try:
                client = await get_shared_pubsub_client()
                if client is not None:
                    pubsub = client.pubsub()
                    await pubsub.subscribe(TARGETS_CHANGED_CHANNEL)
                    # Whatever was published before the subscribe landed is
                    # gone, so a fresh subscription starts with a re-read.
                    self.request_targets_reload()
                    while not self._shutdown_event.is_set():
                        msg = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=_TARGET_SIGNAL_POLL
                        )
                        if msg and msg.get("type") == "message":
                            self.request_targets_reload(msg.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[PriceMonitor] Target change listener error, resubscribing: %s", exc
                )
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass
            # Also the retry while Redis is off or its pub/sub pool is in a
            # rebuild cooldown: either can end, so the listener never retires.
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=_TARGET_SIGNAL_RETRY
                )
                return
            except asyncio.TimeoutError:
                pass

    async def _target_sweep_loop(self) -> None:
        """Warnings and expiry come due by the clock, not by a tick. Every
        worker sweeps; the claims are guarded, so none doubles another's."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=_TARGET_SWEEP_INTERVAL
                )
                return
            except asyncio.TimeoutError:
                pass
            await self._sweep_targets()

    async def _sweep_targets(self) -> None:
        from src.server.database import market_targets as targets_db

        try:
            warned = 0
            while True:
                claimed = await targets_db.claim_due_warnings(limit=_TARGET_WARNING_BATCH)
                warned += len(claimed)
                if len(claimed) < _TARGET_WARNING_BATCH:
                    break
            if warned:
                logger.info("[PriceMonitor] Enqueued %d target expiry warnings", warned)
                _nudge_hook_drainer()
        except Exception:
            logger.error("[PriceMonitor] Target warning sweep failed", exc_info=True)

        try:
            expired = await targets_db.expire_due_targets()
            if expired:
                logger.info("[PriceMonitor] Closed %d lapsed targets", expired)
        except Exception:
            logger.error("[PriceMonitor] Target expiry sweep failed", exc_info=True)

    # ─── Background Loops ────────────────────────────────────────────

    async def _refresh_loop(self) -> None:
        """Periodically reload price automations and armed targets from DB
        and update subscriptions. The re-read is what a target change is
        guaranteed to reach; the change nudge only makes it sooner."""
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
            try:
                await self._load_targets()
            except Exception:
                logger.error("[PriceMonitor] Target refresh failed", exc_info=True)

    async def _load_automations(self) -> None:
        """Load active price automations and update symbol subscriptions."""
        from src.server.database import automation as auto_db

        async with self._load_lock:
            automations = await auto_db.get_active_price_automations()

            new_symbol_map: Dict[Listing, List[Dict[str, Any]]] = {}

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

                auto["_parsed_config"] = config
                listing = (config.symbol.upper(), config.market)
                new_symbol_map.setdefault(listing, []).append(auto)

            await self._resubscribe(set(new_symbol_map), self._target_listings)
            self._symbol_automations = new_symbol_map
            self._automation_listings = set(new_symbol_map)

    async def _load_targets(self, thread_ids: Optional[Collection[str]] = None) -> None:
        """Load armed market-watch targets and price wakeups and update
        symbol subscriptions.

        With *thread_ids*, only those threads' targets are re-read and
        replaced, and every other thread's stay as this worker holds them: a
        change nudge names the thread it touched, and re-reading every armed
        target in every worker on each one is what that saves.
        """
        async with self._load_lock:
            if thread_ids is None:
                new_symbol_map = _targets_by_listing(await _armed_rows())
                await self._resubscribe(self._automation_listings, set(new_symbol_map))
                self._symbol_targets = new_symbol_map
                self._target_listings = set(new_symbol_map)
                return

            scope = set(thread_ids)
            if not scope:
                return
            fresh = _targets_by_listing(await _armed_rows(sorted(scope)))
            listings = set(self._merge_thread_targets(scope, fresh))
            await self._resubscribe(self._automation_listings, listings)
            # Merged from the map as it stands now: a target a claim dropped
            # during the resubscribe stays dropped, since no read returned it.
            self._symbol_targets = self._merge_thread_targets(scope, fresh)
            self._target_listings = listings

    def _merge_thread_targets(
        self, scope: set[str], fresh: Dict[Listing, List[Dict[str, Any]]]
    ) -> Dict[Listing, List[Dict[str, Any]]]:
        """This worker's targets with those of the threads in *scope*
        replaced by *fresh*, their armed rows as just read."""
        merged: Dict[Listing, List[Dict[str, Any]]] = {}
        for listing, targets in self._symbol_targets.items():
            kept = [t for t in targets if t["conversation_thread_id"] not in scope]
            if kept:
                merged[listing] = kept
        for listing, targets in fresh.items():
            merged.setdefault(listing, []).extend(targets)
        return merged

    async def _resubscribe(
        self,
        automation_listings: set[Listing],
        target_listings: set[Listing],
    ) -> None:
        """Bring the feeds and reference prices in line with both sources.

        Called under the load lock. Each listing is read in its own market: a
        bare symbol watched in two, whether by automations, targets or one of
        each, is subscribed and polled in both, since reading it in one would
        leave the other's watchers with no price of their own.
        """
        new_listings = automation_listings | target_listings
        new_stock_ws: set[str] = set()
        new_index_ws: set[str] = set()
        for bare, market in new_listings:
            # The feeds carry the US tape only, where 600519.SH or I:HSI names
            # nothing; the REST poll covers every other venue in its session.
            if _is_us_symbol(bare, market):
                ws_symbols = new_index_ws if market == MarketType.INDEX else new_stock_ws
                ws_symbols.add(_to_ws_symbol(bare, market))

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

        # Refresh reference prices for newly added listings
        added = new_listings - self._listings
        if added:
            await self._evaluator.refresh_references(added)

        self._listings = new_listings
        self._stock_ws_symbols = new_stock_ws
        self._index_ws_symbols = new_index_ws

        total_added = len(stock_added) + len(index_added)
        total_removed = len(stock_removed) + len(index_removed)
        if total_added or total_removed:
            logger.info(
                "[PriceMonitor] Subscriptions updated: +%d -%d (total %d listings, "
                "%d automation listings, %d target listings)",
                total_added, total_removed, len(new_listings),
                len(automation_listings), len(target_listings),
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

            if not self._monitored_listings:
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
        day_starts: Dict[Listing, int] = {}
        stock_syms: List[str] = []
        index_syms: List[str] = []
        for listing in sorted(self._listings):
            bare, market = listing
            if _is_us_symbol(bare, market):
                if not (poll_index if market == MarketType.INDEX else poll_stock):
                    continue
            else:
                day_start = _session_day_start_ms(bare, market, now)
                if day_start is None:
                    continue
                day_starts[listing] = day_start
            (index_syms if market == MarketType.INDEX else stock_syms).append(bare)
        if not stock_syms and not index_syms:
            return

        from src.data_client import get_market_data_provider

        provider = await get_market_data_provider()
        for listing, snapshot in await _fetch_snapshots(provider, stock_syms, index_syms):
            current_price = _positive(snapshot.get("price"))
            if current_price is None:
                continue
            # A row printed before the session's day began has not rolled yet:
            # its change is the previous session's move, which a recurring
            # alert unlocked at the open would fire on. A row with no print
            # time cannot show it rolled, so it is skipped too. US rows have
            # no bound.
            as_of = _positive(snapshot.get("as_of"))
            day_start = day_starts.get(listing)
            if day_start is not None and (as_of is None or as_of < day_start):
                continue

            # Update reference prices
            if _positive(snapshot.get("previous_close")) is not None:
                self._evaluator.set_reference(
                    listing,
                    previous_close=snapshot.get("previous_close"),
                    day_open=snapshot.get("open"),
                )

            for automation in self._symbol_automations.get(listing, []):
                try:
                    await self._evaluate_and_trigger(automation, current_price)
                except Exception:
                    logger.error(
                        "[PriceMonitor] Poll eval error for %s",
                        automation.get("automation_id"),
                        exc_info=True,
                    )

            # The day bound above leaves US rows alone, as an automation
            # wants. A target needs the print time instead, to tell a move
            # made since it was set from the last session's, which a closed
            # venue's row still carries; a row without one cannot show that.
            if as_of is not None:
                await self._evaluate_targets(listing, current_price, as_of)

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

            if self._monitored_listings:
                try:
                    await self._evaluator.refresh_references(self._monitored_listings)
                except Exception:
                    logger.debug("[PriceMonitor] Reference refresh failed", exc_info=True)
