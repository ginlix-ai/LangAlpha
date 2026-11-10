"""Symbol search ranked by relevance, then by how much each listing trades.

No search source orders its rows usefully (FMP answers the shortest symbols
first, so ``nv`` showed NVDA thirteenth), and every symbol field reads the
same route, so the order is settled here once for every source. A wider set
of candidates is asked for than any caller shows, sorted into relevance tiers,
and ordered inside a tier by the day's dollar volume from the quote cache.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any, Callable

from market_protocol import to_canonical
from market_protocol.enums import AssetClass

logger = logging.getLogger(__name__)

#: Candidates asked of the source whatever the caller shows, so a liquid
#: listing the source ranks low still makes the cut.
MIN_CANDIDATES = 50
#: How long the ranking waits on quotes before answering in tier order.
LIQUIDITY_TIMEOUT_S = 1.5
#: A ranked answer stands as long as the unranked one used to.
_RANKED_TTL_S = 300
#: One ranked without quotes is kept briefly: the fill it gave up on keeps
#: running and warms the quote cache, so a search a moment later ranks.
_UNRANKED_TTL_S = 20
_KEY_VERSION = 2

#: How directly a row answers the query, closest first. Each row carries its
#: own as ``match``, so a client can tell a row the query names from one it
#: merely came back with.
MATCHES = EXACT, SYMBOL_PREFIX, NAME_WORD, OTHER = ("exact", "symbol_prefix", "name_word", "other")
_TIER = {match: tier for tier, match in enumerate(MATCHES)}

#: Rough dollars per unit of each listing currency the venue registry knows.
#: Only for ordering: listings worth comparing differ by multiples, which a
#: rate a fifth out cannot reorder, while an unconverted rupee or won figure
#: would outrank every US listing. London rows are served in pounds.
_USD_PER_UNIT = {
    "USD": 1.0, "CAD": 0.72, "AUD": 0.65, "CHF": 1.13, "CNY": 0.14, "EUR": 1.08,
    "GBP": 1.27, "GBX": 1.27, "HKD": 0.128, "INR": 0.0115, "JPY": 0.0067,
    "KRW": 0.00072, "SGD": 0.75, "TWD": 0.031,
}
#: A quote older than this says nothing about how the listing trades now
#: (FMP still serves a 2019 print for one delisted symbol). Longer than any
#: market holiday.
_STALE_AFTER_MS = 10 * 24 * 3600 * 1000

_CJK = re.compile("[\u3400-\u9fff\uf900-\ufaff]")
_NOT_SYMBOL = re.compile(r"[^A-Z0-9^]")

# Fills the ranking stopped waiting for, held so they run to the end and
# write their quotes through. Execution context only: nothing reads it.
_draining: set[asyncio.Task] = set()


class NoSearchSource(Exception):
    """No financial data provider is configured to search."""


def search_cache_key(query: str, limit: int) -> str:
    """The cache key of a ranked search. Versioned so an older order is never
    served as a ranked one. Keyed by the limit, since only the rows that can
    reach the first *limit* are ranked by their quotes."""
    return f"search:v{_KEY_VERSION}:{query.strip().lower()}:{limit}"


def candidate_count(limit: int) -> int:
    return max(limit, MIN_CANDIDATES)


def matcher(query: str) -> Callable[[dict[str, Any]], str]:
    """How directly a row answers *query*: the symbol itself, a symbol that
    starts with it, a name with a word that starts with it, or anything else.

    Symbols compare without punctuation, so ``brk.b`` is ``BRK-B``. A bare
    exchange code is its listing (``600519`` is ``600519.SH``), a Hong Kong
    one with its leading zeros or without (``700`` is ``0700.HK``). A name in
    a script written without spaces matches anywhere in it.
    """
    typed = query.strip().upper()
    compact_typed = _NOT_SYMBOL.sub("", typed)
    lowered = query.strip().lower()
    word_start = re.compile(r"(?<!\w)" + re.escape(lowered))
    anywhere = bool(_CJK.search(lowered))

    def match(row: dict[str, Any]) -> str:
        symbol = str(row.get("symbol") or "").upper()
        compact_symbol = _NOT_SYMBOL.sub("", symbol)
        if symbol == typed or (compact_typed and compact_symbol == compact_typed):
            return EXACT
        if typed.isascii() and typed.isdigit():
            code, _, venue = symbol.partition(".")
            if code == typed or (venue == "HK" and code.lstrip("0") == typed.lstrip("0")):
                return EXACT
        if compact_typed and compact_symbol.startswith(compact_typed):
            return SYMBOL_PREFIX
        for field in ("name", "nameLocal", "nameEn"):
            name = str(row.get(field) or "").lower()
            if name and (word_start.search(name) or (anywhere and lowered in name)):
                return NAME_WORD
        return OTHER

    return match


def rank_rows(rows: list[dict[str, Any]], figures: dict[str, float] | None) -> list[dict[str, Any]]:
    """*rows* by their ``match``, then by dollar volume (*figures*) inside one.

    A row with no figure (no quote, or no volume) follows those with one and
    keeps the source's order, as every row does when *figures* is None.
    """
    figures = figures or {}

    def key(indexed: tuple[int, dict[str, Any]]) -> tuple[int, int, float, int]:
        index, row = indexed
        value = figures.get(row["symbol"])
        known = value is not None and value > 0
        return _TIER[row["match"]], 0 if known else 1, -(value or 0.0), index

    return [row for _, row in sorted(enumerate(rows), key=key)]


def contested(rows: list[dict[str, Any]], limit: int) -> list[str]:
    """The symbols whose order decides the first *limit*: every row of the
    tiers that reach into them. A lower tier's rows fall past the cut
    whatever they trade, so their quotes are not asked for."""
    if len(rows) < 2:
        return []  # nothing to order
    cutoff = sorted(_TIER[row["match"]] for row in rows)[min(limit, len(rows)) - 1]
    return list(dict.fromkeys(row["symbol"] for row in rows if _TIER[row["match"]] <= cutoff))


def dollar_volume(row: dict[str, Any], currency: str | None, now_ms: int) -> float | None:
    """The day's traded value of a quote row in dollars; None for a row with
    no volume or price, an unknown currency, or a print long past."""
    as_of = row.get("as_of")
    if isinstance(as_of, (int, float)) and now_ms - as_of > _STALE_AFTER_MS:
        return None
    rate = _USD_PER_UNIT.get(str(row.get("currency") or currency or "").upper())
    if rate is None:
        return None
    price = row.get("price") or row.get("regular_close") or row.get("previous_close")
    try:
        value = float(row.get("volume")) * float(price) * rate
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


async def liquidity(
    symbols: list[str], user_id: str | None, timeout: float,
) -> dict[str, float] | None:
    """Day dollar volume per symbol from the quote cache, or None when the
    quotes did not come in time or failed.

    One batched fill for every miss. One that outlasts *timeout* is left to
    finish, so its quotes are cached for the next search.
    """
    from src.server.services.cache.quote_cache_service import QuoteCacheService

    by_key: dict[str, list[str]] = {}
    refs = []
    for symbol in symbols:
        try:
            ref = to_canonical(symbol, asset_class=AssetClass.EQUITY)
        except Exception:  # noqa: BLE001 (an unreadable symbol just has no figure)
            continue
        if ref.instrument_key not in by_key:
            refs.append(ref)
        by_key.setdefault(ref.instrument_key, []).append(symbol)
    if not refs:
        return {}

    fill = asyncio.ensure_future(QuoteCacheService.get_instance().get_quotes(refs, user_id=user_id))
    done, _ = await asyncio.wait({fill}, timeout=timeout)
    if not done:
        _draining.add(fill)
        fill.add_done_callback(_drained)
        logger.info("symbol_search.liquidity_timeout | symbols=%d", len(refs))
        return None
    try:
        quotes = fill.result()
    except Exception as exc:  # noqa: BLE001 (ranking is a nicety; the search still answers)
        logger.warning("symbol_search.liquidity_failed | error=%s", type(exc).__name__)
        return None
    from src.data_client.normalize import listing_currency

    now_ms = int(time.time() * 1000)
    out: dict[str, float] = {}
    for ref, row in quotes:
        value = dollar_volume(row, listing_currency(ref), now_ms)
        if value is not None:
            for symbol in by_key.get(ref.instrument_key, []):
                out[symbol] = value
    return out


def _drained(task: asyncio.Task) -> None:
    _draining.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logger.info("symbol_search.late_fill_failed | error=%s", type(task.exception()).__name__)


async def search_symbols(query: str, limit: int, user_id: str | None) -> list[dict[str, Any]]:
    """Every candidate for *query*, ranked for the first *limit*, read through the search cache.

    The caller filters and then cuts to its limit, so a filter that drops
    rows still leaves the next ones.
    """
    from src.data_client import get_financial_data_provider
    from src.data_client.financial_data_provider import PartialSearch
    from src.utils.cache.redis_cache import get_cache_client

    query = query.strip()
    candidates = candidate_count(limit)
    cache = get_cache_client()
    key = search_cache_key(query, limit)
    cached = await cache.get(key)
    if cached is not None:
        return cached["results"]

    provider = await get_financial_data_provider()
    if provider.financial is None:
        raise NoSearchSource()
    complete = True
    try:
        rows = await provider.financial.search_stocks(query=query, limit=candidates)
    except PartialSearch as partial:
        # A directory failed: answer with what the others found, uncached,
        # so the next search asks it again.
        rows, complete = partial.rows, False
    match = matcher(query)
    rows = [{**row, "match": match(row)} for row in rows or [] if row.get("symbol")]

    figures = await liquidity(contested(rows, limit), user_id, LIQUIDITY_TIMEOUT_S)
    ranked = rank_rows(rows, figures)
    if complete:
        ttl = _RANKED_TTL_S if figures is not None else _UNRANKED_TTL_S
        await cache.set(key, {"results": ranked}, ttl=ttl)
    return ranked
