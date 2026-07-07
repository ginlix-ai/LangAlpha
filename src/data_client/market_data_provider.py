"""Composite market data provider with chain-of-responsibility fallback.

Wraps multiple :class:`MarketDataSource` implementations and routes
requests based on symbol market region, falling back to the next
provider on error.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

from src.market_protocol import to_canonical

from .base import FetchResult, MarketDataSource

logger = logging.getLogger(__name__)

# Symbol suffix → market region
_SUFFIX_MAP: dict[str, str] = {
    "HK": "hk",
    "SS": "cn",
    "SZ": "cn",
    "L": "uk",
    "T": "jp",
    "TO": "ca",
    "AX": "au",
    "PA": "eu",
    "DE": "eu",
    "AS": "eu",
    "MI": "eu",
    "MC": "eu",
    "SW": "eu",
    "KS": "kr",
    "KQ": "kr",
    "TW": "tw",
    "SI": "sg",
    "BO": "in",
    "NS": "in",
}


def symbol_market(symbol: str) -> str:
    """Derive market region from a symbol's suffix (routing token).

    Bare symbols (no dot) and ``.US`` suffixes are treated as US. Kept for
    provider chain routing (``_market_matches``); timezone resolution moved to
    the protocol (:func:`symbol_timezone`).
    """
    if "." not in symbol or symbol.endswith(".US"):
        return "us"
    suffix = symbol.rsplit(".", 1)[-1].upper()
    return _SUFFIX_MAP.get(suffix, "other")


def symbol_timezone(symbol: str) -> ZoneInfo:
    """Return exchange-local timezone for a symbol, via the canonical instrument.

    Delegates to ``to_canonical(symbol).tz`` (the protocol's single tz authority),
    falling back to ET for anything unresolvable — offset-identical to the old
    region map, but per-venue accurate for European suffixes.
    """
    try:
        return ZoneInfo(to_canonical(symbol).tz)
    except Exception:
        return ZoneInfo("America/New_York")


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


class MarketDataProvider:
    """Chain-of-responsibility provider implementing :class:`MarketDataSource`.

    Iterates over an ordered list of ``ProviderEntry`` items.  For each
    request the chain is filtered to entries whose market set (per
    capability: intraday / daily / snapshot) covers the symbol's derived
    market region.  On failure the next candidate is tried.  Duplicate
    provider names collapse to their first covering entry, so a provider
    listed twice for per-capability priority is only tried once per request.
    """

    def __init__(self, entries: list[ProviderEntry]) -> None:
        self.entries = entries

    def _sources_for(self, symbol: str, capability: str | None = None) -> list[ProviderEntry]:
        """Return entries that cover *symbol*'s market, in priority order."""
        market = symbol_market(symbol)
        candidates = []
        seen: set[str] = set()
        for e in self.entries:
            if e.name not in seen and _market_matches(e.markets_for(capability), market):
                candidates.append(e)
                seen.add(e.name)
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
        candidates = self._sources_for(symbol, capability)
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
        def normalize_symbol(value: Any) -> str:
            # removeprefix("^") so a provider returning the Yahoo caret form
            # ("^GSPC") still matches the bare requested index symbol ("GSPC").
            # Strips exactly one leading caret (Yahoo's index prefix) — unlike
            # lstrip, a malformed "^^X" won't collapse onto a bare "X" request
            # and resolve it against the wrong source. Request symbols are
            # caret-free, so this is a no-op for them.
            return str(value).strip().upper().removeprefix("^")

        pending = [s for s in symbols if str(s).strip()]
        if not pending:
            return []

        results_by_symbol: dict[str, dict[str, Any]] = {}
        last_exc: Exception | None = None
        supports_snapshots = False

        tried: set[str] = set()
        for entry in self.entries:
            fn = getattr(entry.source, "get_snapshots", None)
            if fn is None or entry.name in tried:
                continue
            supports_snapshots = True

            snapshot_markets = entry.markets_for("snapshot")
            batch = [
                s
                for s in pending
                if _market_matches(snapshot_markets, symbol_market(normalize_symbol(s)))
            ]
            if not batch:
                # Not marked tried: an entry with no work must not shadow a
                # later same-name entry that does cover these symbols (the
                # intraday-only priority slot vs the catch-all fallback).
                continue
            tried.add(entry.name)

            try:
                snapshots = await fn(
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
                continue

            requested = {normalize_symbol(s) for s in batch}
            resolved: set[str] = set()
            for snap in snapshots or []:
                symbol = normalize_symbol(snap.get("symbol") or "")
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

            if resolved:
                pending = [s for s in pending if normalize_symbol(s) not in resolved]
                if not pending:
                    break

        if results_by_symbol:
            return [
                results_by_symbol[normalize_symbol(symbol)]
                for symbol in symbols
                if normalize_symbol(symbol) in results_by_symbol
            ]

        if last_exc:
            raise last_exc
        if supports_snapshots:
            return []
        raise RuntimeError("No data source supports get_snapshots")

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch market status, trying providers in order."""
        last_exc: Exception | None = None
        tried: set[str] = set()
        for entry in self.entries:
            fn = getattr(entry.source, "get_market_status", None)
            if fn is None or entry.name in tried:
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

    def source_names_for(self, symbol: str, capability: str | None = None) -> list[str]:
        """Provider names covering *symbol* for *capability*, in chain priority order."""
        return [e.name for e in self._sources_for(symbol, capability)]
