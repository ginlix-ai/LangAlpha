"""How much each listing trades, from ginlix-data's daily liquidity tables.

ginlix-data builds one table per market after each session: every listing's
average daily traded value over its last twenty sessions. The tables change
once a day, so they are cached hard:

- Redis holds each market's table for every worker, beside a small marker
  (its ``as_of`` and when it was fetched). Redis is the shared copy; a worker's
  memory only caches it, and rereads the marker at most every minute, so a
  table another worker fetched is taken up within a minute.
- A table is fetched again once it is six hours old. Until the new one
  arrives the old one answers, so only a worker with no table at all waits.
- A failed fetch, or a market with no table (404), is remembered for five
  minutes, in Redis and in memory, so the route is not asked again on every
  search.
- A table whose ``as_of`` is more than ten days old is not used: ginlix-data
  has stopped building it, and its figures no longer say what trades now.

One fetch per market runs at a time in a worker; workers do not coordinate
beyond Redis, so after a deploy each may fetch once.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Awaitable, Callable, Mapping

from market_protocol import InstrumentRef, InvalidSymbol, parse_instrument_key

from src.data_client.ginlix_data import get_ginlix_data_client
from src.utils.cache.redis_cache import RedisCacheClient, get_cache_client

logger = logging.getLogger(__name__)

#: Markets ginlix-data builds a table for.
COVERED_MARKETS = frozenset({"us", "cn"})

REFRESH_S = 6 * 3600
FAIL_TTL_S = 300
#: How often a worker rereads the shared marker to take up another worker's fetch.
RECHECK_S = 60
#: Long enough to answer through a day of failed refreshes.
STORE_TTL_S = 2 * 24 * 3600
MAX_AS_OF_AGE_DAYS = 10
_KEY_VERSION = 1

Fetch = Callable[[str], Awaitable[dict[str, Any] | None]]


def _key(market: str, part: str = "table") -> str:
    return f"liquidity:v{_KEY_VERSION}:{market}:{part}"


def _unique_codes(adv: Mapping[str, float]) -> dict[str, float]:
    """*adv* by code alone, for the codes only one listing in it has."""
    codes: dict[str, float | None] = {}
    for key, value in adv.items():
        try:
            code, _ = parse_instrument_key(key)
        except InvalidSymbol:
            continue
        codes[code] = None if code in codes else value
    return {code: value for code, value in codes.items() if value is not None}


@dataclass(frozen=True)
class LiquidityTable:
    market: str
    currency: str
    as_of: str
    #: When it was fetched from ginlix-data (epoch seconds).
    fetched_at: float
    #: Average daily traded value, in ``currency``, by ``instrument_key``.
    adv: Mapping[str, float]
    #: ``adv`` by code alone, for the codes only one listing has.
    by_code: Mapping[str, float]

    @classmethod
    def of(cls, market: str, currency: str, as_of: str, fetched_at: float, adv: Mapping[str, float]) -> LiquidityTable:
        return cls(market, currency, as_of, fetched_at, adv, _unique_codes(adv))

    def value_of(self, ref: InstrumentRef) -> float | None:
        """The listing's traded value. A ref whose venue differs from the
        table's (a symbol resolved offline lands on a guessed venue: SPY as
        XNYS, listed on ARCX) still finds its code when only one listing in
        the table has it."""
        found = self.adv.get(ref.instrument_key)
        return found if found is not None else self.by_code.get(ref.symbol)

    def due(self, now: float) -> bool:
        return now - self.fetched_at >= REFRESH_S

    def stale(self, now: float) -> bool:
        try:
            built = date.fromisoformat(self.as_of)
        except ValueError:
            return True
        return (date.fromtimestamp(now) - built).days > MAX_AS_OF_AGE_DAYS

    def marker(self) -> dict[str, Any]:
        return {"as_of": self.as_of, "fetched_at": self.fetched_at}

    def stored(self) -> dict[str, Any]:
        return {**self.marker(), "market": self.market, "currency": self.currency, "adv": dict(self.adv)}

    @classmethod
    def from_stored(cls, body: Any) -> LiquidityTable | None:
        try:
            return cls.of(
                market=str(body["market"]), currency=str(body["currency"]), as_of=str(body["as_of"]),
                fetched_at=float(body["fetched_at"]), adv={str(k): float(v) for k, v in body["adv"].items()},
            )
        except (KeyError, TypeError, ValueError, AttributeError):
            return None

    @classmethod
    def from_wire(cls, market: str, body: dict[str, Any], fetched_at: float) -> LiquidityTable | None:
        """The route's body as a table; None when it holds no usable row."""
        adv: dict[str, float] = {}
        for row in body.get("rows") or []:
            try:
                key, value = str(row["instrument_key"]), float(row["adv"])
            except (KeyError, TypeError, ValueError):
                continue
            if key and value > 0:
                adv[key] = value
        if not adv or not body.get("currency") or not body.get("as_of"):
            return None
        return cls.of(market, str(body["currency"]), str(body["as_of"]), fetched_at, adv)


@dataclass
class _Held:
    checked_at: float
    table: LiquidityTable | None


class LiquidityTables:
    """Each covered market's table, read through Redis and a memory copy of it."""

    def __init__(
        self,
        fetch: Fetch,
        *,
        cache: Callable[[], RedisCacheClient] = get_cache_client,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._fetch = fetch
        self._cache = cache
        self._clock = clock
        self._held: dict[str, _Held] = {}
        self._failed_until: dict[str, float] = {}
        self._inflight: dict[str, asyncio.Task[LiquidityTable | None]] = {}

    async def table(self, market: str) -> LiquidityTable | None:
        """The market's table, or None when there is none fit to rank by.

        Waits on ginlix-data only when no copy is held anywhere; a copy that is
        due is answered while a fetch replaces it.
        """
        if market not in COVERED_MARKETS:
            return None
        now = self._clock()
        table = await self._read(market, now)
        if table is None or table.due(now):
            if not await self._failed_recently(market, now):
                fetch = self._refresh(market)
                if table is None:
                    table = await asyncio.shield(fetch)
        if table is None or table.stale(self._clock()):
            return None
        return table

    async def _read(self, market: str, now: float) -> LiquidityTable | None:
        held = self._held.get(market)
        if held is not None and now - held.checked_at < RECHECK_S:
            return held.table
        cache = self._cache()
        marker = await cache.get(_key(market, "marker"))
        if held is not None and held.table is not None and marker == held.table.marker():
            held.checked_at = now
            return held.table
        table = LiquidityTable.from_stored(await cache.get(_key(market))) if marker else None
        if table is None and held is not None:
            table = held.table  # Redis lost it or is down: the memory copy still answers
        self._held[market] = _Held(now, table)
        return table

    async def _failed_recently(self, market: str, now: float) -> bool:
        if self._failed_until.get(market, 0.0) > now:
            return True
        return bool(await self._cache().get(_key(market, "failed")))

    def _refresh(self, market: str) -> asyncio.Task[LiquidityTable | None]:
        task = self._inflight.get(market)
        if task is None:
            # A task, so a reader that is cancelled does not cancel the fetch the others wait on.
            task = asyncio.ensure_future(self._load(market))
            self._inflight[market] = task

            def settle(done: asyncio.Task) -> None:
                if self._inflight.get(market) is done:
                    del self._inflight[market]

            task.add_done_callback(settle)
        return task

    async def _load(self, market: str) -> LiquidityTable | None:
        try:
            body = await self._fetch(market)
        except Exception as exc:  # noqa: BLE001 (ranking falls back; the search still answers)
            logger.warning("liquidity.fetch_failed | market=%s error=%s", market, type(exc).__name__)
            body = None
        now = self._clock()
        table = LiquidityTable.from_wire(market, body, now) if isinstance(body, dict) else None
        cache = self._cache()
        if table is None:
            logger.info("liquidity.unavailable | market=%s", market)
            self._failed_until[market] = now + FAIL_TTL_S
            await cache.set(_key(market, "failed"), True, ttl=FAIL_TTL_S)
            return None
        await cache.set(_key(market), table.stored(), ttl=STORE_TTL_S)
        await cache.set(_key(market, "marker"), table.marker(), ttl=STORE_TTL_S)
        self._failed_until.pop(market, None)
        self._held[market] = _Held(now, table)
        return table


_tables: LiquidityTables | None = None


def get_liquidity_tables() -> LiquidityTables:
    """The process's reader of ginlix-data's tables (a cache of Redis, never the truth)."""
    global _tables
    if _tables is None:
        async def fetch(market: str) -> dict[str, Any] | None:
            return await (await get_ginlix_data_client()).get_liquidity_v2(market)

        _tables = LiquidityTables(fetch)
    return _tables
