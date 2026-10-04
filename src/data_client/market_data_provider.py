"""Composite market data provider with chain-of-responsibility fallback.

Wraps multiple :class:`MarketDataSource` implementations and routes
requests based on symbol market region, falling back to the next
provider on error.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from market_protocol import (
    AssetClass,
    InstrumentRef,
    MarketPhase,
    display_spelling,
    market_home,
    market_of,
    to_canonical,
)
from market_protocol.calendars import get_calendar
from market_protocol.intervals import to_schema
from market_protocol.routing import (
    Cell,
    CellKey,
    ProbedProvider,
    RoutingTable,
    Ruleset,
    Surface,
)

from .base import FetchResult, MarketDataSource
from .normalize import declared_venue, snapshot_tier

# Fills names onto snapshot rows in place (the CN/HK local and English names).
NameRows = Callable[[Iterable[dict[str, Any]]], Awaitable[None]]

logger = logging.getLogger(__name__)


def symbol_market(symbol: str) -> str:
    """Market the ``config.yaml`` provider chain routes a legacy spelling to.

    The market itself is the protocol's ``market_of``; what this adds is the
    one place the chain reads a spelling differently from ``to_canonical``.
    The chain and price monitor speak legacy REST spellings, so:

    - a dotless spelling routes as ``us`` unless it carries a caret (indices
      travel the chain caret-free, and ``HSI`` there has always meant the US
      chain; ``^HSI`` routes to ``hk``);
    - a dotted spelling in canonical form (``SAP.XETR``, ``BTC-USD.CRYPTO``)
      routes as ``other``, the token config market sets use for "no venue",
      because the chain's legacy providers do not read MIC spellings.

    The routing ruleset is read with the canonical market instead
    (:func:`_route_ref`), since ginlix-data keys the same ruleset by
    ``market_of``.
    """
    # Folded as to_canonical folds it, so a full-width 600519．SH is the
    # Shanghai listing here too.
    s = display_spelling(symbol)
    try:
        if "." not in s:
            return market_of(to_canonical(s)) if s.startswith("^") else "us"
        if s.endswith(".US"):
            return "us"
        suffix = s.rsplit(".", 1)[1]
        ref = to_canonical(s.lstrip("^"), asset_class=AssetClass.EQUITY)
        return "other" if ref.mic == suffix else market_of(ref)
    except ValueError:
        return "us" if "." not in s else "other"


def snapshot_key(symbol: Any) -> str:
    """Where a requested symbol and a served snapshot row meet, whatever either spelled.

    Providers answer an index caret-free (``^GSPC`` comes back ``GSPC``), so one
    leading caret drops; only one, so a malformed ``^^X`` never collapses onto a
    bare ``X`` request. A venue suffix in either spelling (``600519.SS``,
    ``600519.SH``) names one listing. The key is itself a routable spelling.
    """
    return display_spelling(str(symbol or "")).removeprefix("^")


# Surfaces the routing ruleset has cells for.
_ROUTED_SURFACES = frozenset({Surface.INTRADAY, Surface.DAILY, Surface.SNAPSHOT})


def _route_ref(symbol: str, is_index: bool) -> InstrumentRef | None:
    """The instrument a chain request names, or None when the protocol cannot read it.

    Index symbols travel caret-free through the chain, so the endpoint's idea
    of the symbol rides as a hint; the protocol still overrules it on the asset
    class, so a CN ETF asked for through an equity endpoint is a fund.
    """
    try:
        return to_canonical(symbol, asset_class=AssetClass.INDEX if is_index else AssetClass.EQUITY)
    except ValueError:
        return None


def is_us_symbol(symbol: str) -> bool:
    """True if symbol is a US equity (bare ticker or .US suffix)."""
    return symbol_market(symbol) == "us"


_SNAPSHOT_CORE_FIELDS = (
    "price", "change", "change_percent", "previous_close",
    "open", "high", "low", "volume",
)


def _is_null_row(snap: dict) -> bool:
    """True if a snapshot row carries no market data at all."""
    return all(snap.get(f) is None for f in _SNAPSHOT_CORE_FIELDS)


def calendar_market_status(market: str) -> dict[str, Any]:
    """Legacy market-status payload for a non-US *market* from its venues' calendar.

    Status is a clock, not a quote: no provider has to be consulted for it, and
    the providers that do implement it only know the US session.
    """
    home = market_home(market)
    if home is None:
        raise ValueError(f"No market calendar for market {market!r}")
    cal = get_calendar(home[0])
    now = datetime.now(cal.tz)
    return {
        "market": "open" if cal.phase_at(now) is MarketPhase.REGULAR else "closed",
        "afterHours": False,
        "earlyHours": False,
        "serverTime": now.isoformat(),
        "exchanges": None,
    }


def _market_matches(markets: set[str], market: str) -> bool:
    """True if a provider's market set covers *market*.

    ``"all"`` matches everything; ``"non-us"`` matches any market except
    ``us`` (used to slot a provider ahead of the catch-all for foreign
    symbols without touching US routing).
    """
    if "all" in markets or market in markets:
        return True
    return "non-us" in markets and market != "us"


@dataclass
class ProviderEntry:
    name: str
    source: MarketDataSource
    markets: set[str] = field(default_factory=lambda: {"all"})
    # Per-capability overrides — None falls back to `markets`. Lets one
    # provider hold different chain positions per capability (an entry may
    # appear twice in the list sharing the same source instance).
    intraday_markets: set[str] | None = None
    daily_markets: set[str] | None = None
    snapshot_markets: set[str] | None = None

    def markets_for(self, capability: str | None) -> set[str]:
        override = {
            "intraday": self.intraday_markets,
            "daily": self.daily_markets,
            "snapshot": self.snapshot_markets,
        }.get(capability or "")
        return self.markets if override is None else override

    @property
    def scope(self) -> set[str]:
        """Every market this entry is configured for, under any capability."""
        overrides = (self.intraday_markets, self.daily_markets, self.snapshot_markets)
        return self.markets.union(*(o for o in overrides if o))


class MarketDataProvider:
    """Chain-of-responsibility provider implementing :class:`MarketDataSource`.

    Iterates over an ordered list of ``ProviderEntry`` items.  For each
    request the chain is filtered to entries whose market set (per
    capability: intraday / daily / snapshot) covers the symbol's derived
    market region.  On failure the next candidate is tried.  Duplicate
    provider names collapse to their first covering entry, so a provider
    listed twice for per-capability priority is only tried once per request.
    """

    def __init__(
        self,
        entries: list[ProviderEntry],
        routing: RoutingTable | None = None,
        name_rows: NameRows | None = None,
    ) -> None:
        self.entries = entries
        self.routing = routing
        self._name_rows = name_rows
        self._unknown_names: set[str] = set()

    def _entry_by_name(self, name: str) -> ProviderEntry | None:
        """First entry with *name* — same-name entries share one source instance,
        so any of them stands in for the provider once the cell has ordered it."""
        for e in self.entries:
            if e.name == name:
                return e
        return None

    def _configured_in(self, name: str, market: str) -> bool:
        """Whether config lists provider *name* for *market* under any capability.

        A cell may promote a provider to a surface its entry leaves out (tushare
        intraday), never to a market: a CN-only source that answers a US canary
        is serving someone else's feed under its own name.
        """
        return any(e.name == name and _market_matches(e.scope, market) for e in self.entries)

    @functools.cached_property
    def _verdicts(self) -> RoutingTable:
        """The ruleset with every provider it measured read as routable.

        Looked up like the real table, it resolves the same cell and names every
        provider that cell has a verdict on, the excluded ones included.
        """
        ruleset = self.routing.ruleset
        return RoutingTable(Ruleset(generated_at=ruleset.generated_at, cells=[
            Cell(key=c.key, probed_at=c.probed_at,
                 providers=[ProbedProvider(name=p.name) for p in c.providers])
            for c in ruleset.cells
        ]))

    @functools.cached_property
    def _cell_keys(self) -> frozenset[CellKey]:
        return frozenset(c.key for c in self.routing.ruleset.cells)

    def _borrows_width(
        self, capability: str, market: str, asset_class: AssetClass, interval: str | None
    ) -> bool:
        """Whether the table answers *interval* from the cell of another bar width.

        ``RoutingTable.order_for`` takes the exact cell, then the interval-less
        one, then the nearest probed width; only that last step borrows.
        """
        if interval is None:
            return False
        return not any(
            CellKey(market=market, asset_class=asset_class, surface=capability, interval=i)
            in self._cell_keys
            for i in (interval, None)
        )

    def _routed_names(
        self,
        symbol: str,
        capability: str | None,
        configured: list[ProviderEntry],
        interval: str | None = None,
        is_index: bool = False,
    ) -> list[str] | None:
        """Provider names for *symbol*'s cell, or ``None`` to use config routing.

        The cell's routable providers lead in its order; configured providers it
        has no verdict on (added since the probe, or not built when it ran) follow
        in *configured* order, so a ruleset refines the configured chain and never
        narrows it to what one run probed. Providers the cell excluded stay out,
        unless the cell measured another bar width: then every configured provider
        follows, since a provider routable at ``1h`` may have no ``4h`` bars at all
        and the request must not end with only those.
        ``None`` also when that leaves nothing: a ruleset never empties the chain.
        """
        if self.routing is None or capability not in _ROUTED_SURFACES:
            return None
        if capability != Surface.INTRADAY:
            interval = None
        else:
            try:
                to_schema(interval or "")
            except ValueError:
                return None  # only a readable width has a cell
        ref = _route_ref(symbol, is_index)
        if ref is None:
            return None
        market, asset_class = market_of(ref), ref.asset_class
        order = self.routing.order_for(capability, market, asset_class, interval)
        if order is None:
            return None
        names: list[str] = []
        for name in order:
            if self._entry_by_name(name) is None:
                if name not in self._unknown_names:
                    self._unknown_names.add(name)
                    logger.debug(
                        "market_data.routing.unknown_provider | name=%s", name
                    )
                continue
            if name not in names and self._configured_in(name, market):
                names.append(name)
        if self._borrows_width(capability, market, asset_class, interval):
            mentioned: set[str] = set()
        else:
            mentioned = set(
                self._verdicts.order_for(capability, market, asset_class, interval) or ()
            )
        names += [e.name for e in configured if e.name not in mentioned and e.name not in names]
        if not names:
            logger.info(
                "market_data.routing.empty_cell | symbol=%s capability=%s cell=%s/%s",
                symbol, capability, market, asset_class.value,
            )
            return None
        return names

    def _sources_for(
        self,
        symbol: str,
        capability: str | None = None,
        interval: str | None = None,
        is_index: bool = False,
    ) -> list[ProviderEntry]:
        """Return entries that cover *symbol*, in priority order.

        The generated ruleset wins when it has a cell for this request; otherwise
        the ``config.yaml`` market sets decide, unchanged.
        """
        market = symbol_market(symbol)
        candidates = []
        seen: set[str] = set()
        for e in self.entries:
            if e.name not in seen and _market_matches(e.markets_for(capability), market):
                candidates.append(e)
                seen.add(e.name)
        routed = self._routed_names(symbol, capability, candidates, interval, is_index)
        if routed is not None:
            return [e for name in routed if (e := self._entry_by_name(name)) is not None]
        return candidates

    async def _try_chain(
        self, method: str, symbol: str, capability: str | None = None, **kwargs: Any
    ) -> Any:
        data, _, _ = await self._try_chain_with_source(method, symbol, capability, **kwargs)
        return data

    async def _try_chain_with_source(
        self, method: str, symbol: str, capability: str | None = None, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], str, bool]:
        """Like ``_try_chain`` but also returns the source name and truncated flag.

        Returns ``(bars, source_name, truncated)``.  Data sources may return
        a :class:`FetchResult` to signal truncation; plain ``list`` results
        are treated as non-truncated.
        """
        candidates = self._sources_for(
            symbol,
            capability,
            interval=kwargs.get("interval"),
            is_index=bool(kwargs.get("is_index")),
        )
        if not candidates:
            raise RuntimeError(f"No data source configured for market of {symbol}")
        last_exc: Exception | None = None
        first_empty: tuple[list[dict[str, Any]], str, bool] | None = None
        for entry in candidates:
            try:
                result = await getattr(entry.source, method)(symbol=symbol, **kwargs)
                if isinstance(result, FetchResult):
                    bars, truncated = result.bars, result.truncated
                else:
                    bars, truncated = result, False
                if not bars:
                    # Empty is a soft miss — a source may cover the market but
                    # not this symbol/window (e.g. lookback caps). Try the rest
                    # of the chain before accepting it.
                    if first_empty is None:
                        first_empty = (bars, entry.name, truncated)
                    logger.info(
                        "market_data.empty_fallthrough | source=%s symbol=%s",
                        entry.name,
                        symbol,
                    )
                    continue
                return bars, entry.name, truncated
            except Exception as exc:
                logger.warning(
                    "market_data.fallback | source=%s symbol=%s error=%s",
                    entry.name,
                    symbol,
                    exc,
                )
                last_exc = exc
        if first_empty is not None:
            return first_empty
        raise last_exc  # type: ignore[misc]

    async def _fetch_from(
        self, source_name: str, method: str, symbol: str, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], str, bool]:
        """Fetch from ONE named source — no fallback. Honors series pins:
        a pinned envelope must only ever be refilled by its own publisher."""
        for entry in self.entries:
            if entry.name == source_name:
                result = await getattr(entry.source, method)(symbol=symbol, **kwargs)
                if isinstance(result, FetchResult):
                    return result.bars, entry.name, result.truncated
                return result, entry.name, False
        raise RuntimeError(f"No data source named {source_name!r}")

    async def get_intraday_from(
        self,
        source_name: str,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, bool]:
        return await self._fetch_from(
            source_name, "get_intraday", symbol,
            interval=interval, from_date=from_date, to_date=to_date,
            is_index=is_index, user_id=user_id,
        )

    async def get_daily_from(
        self,
        source_name: str,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, bool]:
        return await self._fetch_from(
            source_name, "get_daily", symbol,
            from_date=from_date, to_date=to_date,
            is_index=is_index, user_id=user_id,
        )

    # -- MarketDataSource interface ------------------------------------------

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._try_chain(
            "get_intraday",
            symbol,
            capability="intraday",
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            is_index=is_index,
            user_id=user_id,
        )

    async def get_intraday_with_source(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, bool]:
        """Like ``get_intraday`` but also returns source name and truncated flag."""
        return await self._try_chain_with_source(
            "get_intraday",
            symbol,
            capability="intraday",
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            is_index=is_index,
            user_id=user_id,
        )

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._try_chain(
            "get_daily",
            symbol,
            capability="daily",
            from_date=from_date,
            to_date=to_date,
            is_index=is_index,
            user_id=user_id,
        )

    async def get_daily_with_source(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], str, bool]:
        """Like ``get_daily`` but also returns source name and truncated flag."""
        return await self._try_chain_with_source(
            "get_daily",
            symbol,
            capability="daily",
            from_date=from_date,
            to_date=to_date,
            is_index=is_index,
            user_id=user_id,
        )

    # -- Snapshot interface ---------------------------------------------------

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch batch snapshots with per-symbol market routing and fallback."""
        pending = [s for s in symbols if str(s).strip()]
        if not pending:
            return []

        is_index = asset_type == "indices"
        results_by_symbol: dict[str, dict[str, Any]] = {}
        venues: dict[str, str | None] = {}
        last_exc: Exception | None = None

        async def serve(entry: ProviderEntry, batch: list[Any]) -> set[str]:
            """Ask one provider for *batch*; return the symbols it resolved."""
            nonlocal last_exc
            try:
                snapshots = await entry.source.get_snapshots(
                    symbols=batch,
                    asset_type=asset_type,
                    user_id=user_id,
                )
            except Exception as exc:
                logger.warning(
                    "market_data.snapshot.fallback | source=%s error=%s",
                    entry.name, exc,
                )
                last_exc = exc
                return set()

            requested = {snapshot_key(s) for s in batch}
            resolved: set[str] = set()
            for snap in snapshots or []:
                symbol = snapshot_key(snap.get("symbol") or "")
                if symbol in requested:
                    if _is_null_row(snap):
                        # A row with no data does not resolve the symbol —
                        # let the next provider try; after chain exhaustion
                        # the symbol is simply absent from the results.
                        logger.debug(
                            "market_data.snapshot.null_row | source=%s symbol=%s",
                            entry.name, symbol,
                        )
                        continue
                    snap["source"] = entry.name
                    # A provider that knows its own freshness keeps it (a
                    # daily-derived row is EOD whoever built it).
                    snap.setdefault("tier", snapshot_tier(entry.name, venues[symbol]).value)
                    results_by_symbol[symbol] = snap
                    resolved.add(symbol)
                elif symbol:
                    logger.warning(
                        "market_data.snapshot.drop_unrequested | source=%s symbol=%s",
                        entry.name,
                        symbol,
                    )
                else:
                    logger.warning(
                        "market_data.snapshot.drop_unkeyed | source=%s item=%s",
                        entry.name,
                        snap,
                    )
            return resolved

        # Each symbol walks its own provider order (its routing cell, else the
        # configured chain). A round asks every symbol's next provider at once,
        # one batch per provider, and only the unresolved move on.
        queues: dict[str, list[ProviderEntry]] = {}
        for s in pending:
            key = snapshot_key(s)
            if key not in queues:
                # Routed on the caller's spelling: the key drops the caret, and
                # ^FTSE on the stocks endpoint is still the London index.
                queues[key] = self._sources_for(s, "snapshot", is_index=is_index)
                venues[key] = declared_venue(_route_ref(s, is_index))
        while pending:
            batches: dict[str, tuple[ProviderEntry, list[Any]]] = {}
            for s in pending:
                queue = queues[snapshot_key(s)]
                if queue:
                    batches.setdefault(queue[0].name, (queue[0], []))[1].append(s)
            if not batches:
                break
            for key in {snapshot_key(s) for _, batch in batches.values() for s in batch}:
                queues[key].pop(0)
            answered = await asyncio.gather(
                *(serve(entry, batch) for entry, batch in batches.values())
            )
            resolved = set().union(*answered)
            pending = [s for s in pending if snapshot_key(s) not in resolved]

        if results_by_symbol:
            if self._name_rows is not None:
                await self._name_rows(results_by_symbol.values())
            return [
                results_by_symbol[snapshot_key(symbol)]
                for symbol in symbols
                if snapshot_key(symbol) in results_by_symbol
            ]

        if last_exc:
            raise last_exc
        return []

    async def get_market_status(
        self,
        user_id: str | None = None,
        market: str = "us",
    ) -> dict[str, Any]:
        """Fetch market status for *market*.

        Status is a clock, not a quote: outside the US the venue calendar
        answers, since the providers that implement status only know the US
        session. For the US, covering providers are tried in order.
        """
        if market != "us":
            return calendar_market_status(market)
        last_exc: Exception | None = None
        tried: set[str] = set()
        for entry in self.entries:
            fn = getattr(entry.source, "get_market_status", None)
            if fn is None or entry.name in tried:
                continue
            if not _market_matches(entry.markets_for(None), market):
                continue
            tried.add(entry.name)
            try:
                return await fn(user_id=user_id)
            except Exception as exc:
                logger.warning(
                    "market_data.market_status.fallback | source=%s error=%s",
                    entry.name, exc,
                )
                last_exc = exc
        if last_exc:
            raise last_exc
        raise RuntimeError("No data source supports get_market_status")

    async def close(self) -> None:
        """Close all underlying sources, catching errors independently."""
        for entry in self.entries:
            try:
                await entry.source.close()
            except Exception:
                logger.warning("market_data.close | source=%s failed", entry.name, exc_info=True)

    @property
    def source_names(self) -> list[str]:
        return list(dict.fromkeys(e.name for e in self.entries))

    def source_names_for_market(self, market: str) -> list[str]:
        """Provider names configured for *market* under any capability, in chain order."""
        covering = {e.name for e in self.entries if _market_matches(e.scope, market)}
        return [name for name in self.source_names if name in covering]

    def source_names_for(
        self,
        symbol: str,
        capability: str | None = None,
        interval: str | None = None,
        is_index: bool = False,
    ) -> list[str]:
        """Provider names covering *symbol* for *capability*, in chain priority order."""
        return [e.name for e in self._sources_for(symbol, capability, interval, is_index)]

