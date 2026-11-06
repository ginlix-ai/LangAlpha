"""Composite provider for fundamental + market intelligence data.

``RoutedFinancialSource`` / ``RoutedMarketIntelSource`` add per-market
dispatch in front of the default source. Call sites keep using
``provider.financial.X`` / ``provider.intel.X`` unchanged; a routed source
that errors or returns nothing falls back to the default.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .base import FinancialDataSource, MarketIntelSource
from .fallbacks import Fallback, error_label, record, surfaced
from .market_data_provider import NameRows, symbol_market
from .normalize import populated

logger = logging.getLogger(__name__)


def _never(_: Any) -> bool:
    return False


@dataclass(frozen=True)
class MarketRoute:
    """One market's source plus the predicates that send no-symbol calls to it.

    Symbol-keyed methods route on the symbol's market alone. A screen, a name
    search or an option contract carries no listing symbol, so the market that
    owns it says how to recognise one: ``screens`` claims a filter set,
    ``claims_query`` a search query (``None`` when the market has no name
    directory; a query it does not claim still falls back to it when the
    default finds nothing), ``claims_option`` an option ticker.
    """

    source: Any
    screens: Callable[[dict[str, Any]], bool] = _never
    claims_query: Callable[[str], bool] | None = None
    claims_option: Callable[[str], bool] = _never
    name: str | None = None


class _BySymbol:
    """A method dispatched on its first argument's market, empty when nothing serves it.

    A descriptor rather than ``__getattr__`` so every routed name is written in
    the class body, greppable and visible to ``dir()``; the signature lives on
    the protocol in ``base``.
    """

    def __init__(self, empty: Callable[[], Any]) -> None:
        self._empty = empty

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, obj: Any, owner: type | None = None) -> Any:
        if obj is None:
            return self
        return functools.partial(obj._dispatch, self.name, self._empty)


class _DefaultOnly(_BySymbol):
    """A method only the default source answers."""

    def __get__(self, obj: Any, owner: type | None = None) -> Any:
        if obj is None:
            return self
        return functools.partial(obj._on_default, self.name, self._empty)


def _results() -> dict[str, Any]:
    return {"results": []}


def _log_fallback(method: str, exc: Exception) -> None:
    # The type only: the first argument may be a user's search text, and an
    # exception's message or traceback can carry it too (an httpx error's URL).
    logger.warning(
        "routed_source.fallback | method=%s error=%s", method, type(exc).__name__
    )


async def _close_all(sources: Iterable[tuple[str, Any]]) -> None:
    """Close every source, then raise the first failure.

    One failed close must not leave the others' connections open.
    """
    first: Exception | None = None
    for name, source in sources:
        if source is None:
            continue
        try:
            await source.close()
        except Exception as exc:
            logger.warning("financial_data.close | source=%s failed", name, exc_info=True)
            first = first or exc
    if first is not None:
        raise first


class PartialSearch(Exception):
    """A search answered without a directory that failed: *rows* is what the rest found.

    Raised rather than returned so a caller that caches searches cannot store
    it as the definitive answer without handling it.
    """

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        super().__init__("a search directory failed")
        self.rows = rows


class _RoutedSource:
    """Shared dispatch: pick the per-market source, fall back to default.

    The names are what a recorded :class:`Fallback` calls the two sources.
    """

    def __init__(
        self,
        default: Any | None,
        by_market: dict[str, MarketRoute],
        default_name: str | None = None,
    ) -> None:
        self._default = default
        self._by_market = by_market
        self._default_name = default_name

    async def _on_default(
        self, method: str, empty: Callable[[], Any], /, *args: Any, **kwargs: Any
    ) -> Any:
        fn = getattr(self._default, method, None)
        if fn is None:
            return empty()
        return await fn(*args, **kwargs)

    async def _dispatch(
        self, method: str, empty: Callable[[], Any], symbol: str, /, *args: Any, **kwargs: Any
    ) -> Any:
        """The market's own answer, else the default's, named as a fallback."""
        market = symbol_market(str(symbol).upper())
        route = self._by_market.get(market)
        if route is None:
            return await self._on_default(method, empty, symbol, *args, **kwargs)
        return await self._routed(
            route, str(symbol), method, empty, symbol, *args, **kwargs
        )

    async def _routed(
        self,
        route: MarketRoute,
        subject: str,
        method: str,
        empty: Callable[[], Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """*route*'s answer, else the default's, recorded as a fallback for *subject*.

        A routed source that failed keeps its failure when the default has
        nothing or fails too, so an outage never reads as an empty answer. A
        dict whose every value is empty (``{"results": []}``) is empty too.
        """
        failure: Exception | None = None
        try:
            result = await getattr(route.source, method)(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 (the default may still answer)
            _log_fallback(method, exc)
            failure = exc
        else:
            if populated(result):
                return result
        try:
            answer = await self._on_default(method, empty, *args, **kwargs)
        except Exception as exc:
            if failure is None:
                raise
            _log_fallback(method, exc)
            answer = None
        served = populated(answer)
        source = route.name or "market source"
        if served or failure is not None:
            record(Fallback(
                symbol=subject,
                what=method.removeprefix("get_").replace("_", " "),
                source=source,
                error=error_label(failure) if failure is not None else None,
                served_by=(self._default_name or "default source") if served else None,
            ))
        if failure is not None and not served:
            raise surfaced(source, failure)
        return answer

    async def close(self) -> None:
        await _close_all([
            ("default", self._default),
            *((market, route.source) for market, route in self._by_market.items()),
        ])


class RoutedFinancialSource(_RoutedSource):
    """FinancialDataSource with per-market dispatch.

    ``get_realtime_quote`` deliberately stays on the default source: realtime
    for a routed market is served by the market-data chain's snapshots instead.
    ``name_rows`` adds local names to the default source's search rows, which
    know a listing only by its English name.
    """

    def __init__(
        self,
        default: Any | None,
        by_market: dict[str, MarketRoute],
        name_rows: NameRows | None = None,
        default_name: str | None = None,
    ) -> None:
        super().__init__(default, by_market, default_name)
        self._name_rows = name_rows

    get_company_profile = _BySymbol(list)
    get_income_statements = _BySymbol(list)
    get_cash_flows = _BySymbol(list)
    get_key_metrics = _BySymbol(list)
    get_financial_ratios = _BySymbol(list)
    get_price_performance = _BySymbol(list)
    get_analyst_price_targets = _BySymbol(list)
    get_analyst_ratings = _BySymbol(list)
    get_earnings_history = _BySymbol(list)
    get_revenue_by_segment = _BySymbol(list)

    get_realtime_quote = _DefaultOnly(list)
    get_sector_performance = _DefaultOnly(list)

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        for route in self._by_market.values():
            if route.screens(filters):
                return await self._routed(route, "screen", "screen_stocks", list, **filters)
        return await self._on_default("screen_stocks", list, **filters)

    async def search_stocks(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        """Directories that claim *query*, then the default, then the other directories.

        The caller caches an empty search as "no match", so nothing found is
        never plain ``[]`` while a source failed. A failed claiming directory
        or default raises its failure: the query had no answer. A failed
        fallback directory leaves the default's "no match" unconfirmed, which
        raises :class:`PartialSearch`.
        """
        searchers = [r for r in self._by_market.values() if r.claims_query is not None]
        claimed = [r for r in searchers if r.claims_query and r.claims_query(query)]
        failure: Exception | None = None

        async def ask(fn: Callable[..., Any]) -> list[dict[str, Any]]:
            nonlocal failure
            try:
                return await fn(query, limit=limit) or []
            except PartialSearch:
                # Names one directory found while another failed: served as the
                # answer, still marked partial so the caller does not cache it.
                raise
            except Exception as exc:  # noqa: BLE001 (another source may still answer)
                _log_fallback("search_stocks", exc)
                failure = failure or exc
                return []

        for route in claimed:
            if hit := await ask(route.source.search_stocks):
                return hit
        default = getattr(self._default, "search_stocks", None)
        if default is not None and (result := await ask(default)):
            if self._name_rows is not None:
                await self._name_rows(result)
            return result
        primary_failure = failure
        # The default came up empty (e.g. a pinyin abbreviation) or failed: ask
        # the directories that did not claim the query.
        for route in searchers:
            if route not in claimed and (hit := await ask(route.source.search_stocks)):
                return hit
        if primary_failure is not None:
            raise primary_failure
        if failure is not None:
            raise PartialSearch([])
        return []


class RoutedMarketIntelSource(_RoutedSource):
    """MarketIntelSource with per-market dispatch.

    ``get_options_ohlcv`` routes on the contract ticker (see
    :attr:`MarketRoute.claims_option`), since it names no underlying.
    """

    get_options_chain = _BySymbol(_results)
    get_short_interest = _BySymbol(list)
    get_short_volume = _BySymbol(list)
    get_float_shares = _BySymbol(dict)

    get_options_snapshot = _DefaultOnly(list)
    get_movers = _DefaultOnly(list)

    async def get_options_ohlcv(
        self, options_ticker: str, *args: Any, **kwargs: Any
    ) -> list[dict[str, Any]]:
        for route in self._by_market.values():
            if route.claims_option(str(options_ticker).upper()):
                return await self._routed(
                    route, options_ticker, "get_options_ohlcv", list, options_ticker, *args, **kwargs
                )
        return await self._on_default("get_options_ohlcv", list, options_ticker, *args, **kwargs)


class FinancialDataProvider:
    """Bundles a :class:`FinancialDataSource` and a :class:`MarketIntelSource`.

    Either source may be ``None`` if the backing service is unavailable.
    """

    def __init__(
        self,
        financial: FinancialDataSource | None = None,
        intel: MarketIntelSource | None = None,
    ) -> None:
        self.financial = financial
        self.intel = intel

    async def close(self) -> None:
        await _close_all([("financial", self.financial), ("intel", self.intel)])
