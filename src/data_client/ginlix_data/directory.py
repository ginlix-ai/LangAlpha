"""Local-language display names for CN/HK listings, from ginlix-data's directory.

The directory is the only source in the stack with Chinese names for the full
A-share and HK universe, and the names barely move, so lookups are cached
per process: a hit for a day, a miss for five minutes (a new listing shows up
on the next try), a failed lookup for a minute. The cache only saves round trips; ginlix-data stays the
truth, so a stale or cold cache is never wrong for long.

Every entry point is a soft enrichment: ``(None, None)`` for anything that is
not CN/HK-spelled, when ginlix-data is not configured, or on any failure.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Iterable

from market_protocol import to_canonical, venue_suffixes

logger = logging.getLogger(__name__)

_HIT_TTL = 24 * 3600
_MISS_TTL = 300
# Short enough that names return soon after ginlix-data does, long enough that
# quote reads (which heal nameless cached rows) don't each retry a dead route.
_FAIL_TTL = 60
_LOCAL_NAME_SUFFIXES = tuple(f".{s}" for s in sorted(venue_suffixes("cn", "hk")))
# Unknown symbols are cached too (as misses), so a worker that sees many
# distinct bad symbols must not keep every one; oldest entries go first.
_MAX_ENTRIES = 4096

# Quote reads wait on the host lookup, so a slow ginlix-data must cost them
# seconds, not the client's full timeout.
_HOST_DEADLINE = 2.0

Lookup = Callable[[list[str]], Awaitable[dict[str, dict[str, Any]]]]
Names = tuple[str | None, str | None]
_Entry = tuple[float, str | None, str | None]  # (expires at, local, english)


class NameDirectory:
    """Per-process cache in front of a ``/api/v2/data/instruments`` lookup.

    ``lookup`` returns entries keyed by ``instrument_key``, as the route does.
    A symbol already being looked up is awaited, not asked for again, so a
    burst of readers costs one round trip. ``deadline`` bounds that round
    trip; a lookup that runs past it counts as failed.
    """

    def __init__(self, lookup: Lookup, *, deadline: float | None = None) -> None:
        self._lookup = lookup
        self._deadline = deadline
        self._cache: OrderedDict[str, _Entry] = OrderedDict()
        self._inflight: dict[str, asyncio.Task[dict[str, Names]]] = {}

    async def display_names(self, symbol_or_code: str) -> Names:
        """(local name, English name) for a CN/HK listing, else ``(None, None)``."""
        return (await self.display_names_many([symbol_or_code]))[symbol_or_code]

    async def display_names_many(self, symbols: Iterable[str]) -> dict[str, Names]:
        """``display_names`` for many symbols, keyed by the spelling passed in.

        Every cache miss rides one lookup call: the route is batched, so a
        250-row snapshot costs one round trip, not 250.
        """
        out: dict[str, Names] = {}
        misses: dict[str, str] = {}  # normalized symbol -> instrument_key
        unparsable: dict[str, _Entry] = {}
        pending: dict[str, asyncio.Task[dict[str, Names]]] = {}
        wanted: dict[str, str] = {}  # caller spelling -> normalized symbol
        now = time.monotonic()
        for raw in symbols:
            sym = str(raw).strip().upper()
            wanted[raw] = sym
            if sym in out or sym in misses or sym in pending:
                continue
            if not sym.endswith(_LOCAL_NAME_SUFFIXES):
                out[sym] = (None, None)
                continue
            if (cached := self._cached(sym, now)) is not None:
                out[sym] = cached
                continue
            if (task := self._inflight.get(sym)) is not None:
                pending[sym] = task
                continue
            try:
                misses[sym] = to_canonical(sym).instrument_key
            except ValueError:
                unparsable[sym] = (now + _MISS_TTL, None, None)
                out[sym] = (None, None)
        if unparsable:
            self._store(unparsable, now)
        if misses:
            # A task, not a plain await: a reader that gets cancelled must not
            # cancel the lookup the others are waiting on.
            task = asyncio.ensure_future(self._fetch(misses))
            for sym in misses:
                self._inflight[sym] = task
                pending[sym] = task
            task.add_done_callback(lambda t, syms=tuple(misses): self._settle(t, syms))
        for sym, task in pending.items():
            out[sym] = (await asyncio.shield(task))[sym]
        return {raw: out[sym] for raw, sym in wanted.items()}

    def _cached(self, sym: str, now: float) -> Names | None:
        entry = self._cache.get(sym)
        if entry is None:
            return None
        if entry[0] <= now:
            del self._cache[sym]
            return None
        return entry[1], entry[2]

    def _store(self, entries: dict[str, _Entry], now: float) -> None:
        for sym, entry in entries.items():
            self._cache[sym] = entry
            self._cache.move_to_end(sym)
        for sym in [s for s, e in self._cache.items() if e[0] <= now]:
            del self._cache[sym]
        while len(self._cache) > _MAX_ENTRIES:
            self._cache.popitem(last=False)

    def _settle(self, task: asyncio.Task, syms: tuple[str, ...]) -> None:
        for sym in syms:
            if self._inflight.get(sym) is task:
                del self._inflight[sym]

    async def _fetch(self, misses: dict[str, str]) -> dict[str, Names]:
        try:
            found = await asyncio.wait_for(self._lookup(list(misses)), self._deadline)
        except Exception as exc:  # noqa: BLE001 - enrichment never fails the caller
            logger.info("directory.lookup_failed | symbols=%d error=%s",
                        len(misses), type(exc).__name__)
            found = None
        now = time.monotonic()
        entries: dict[str, _Entry] = {}
        for sym, key in misses.items():
            if found is None:
                entries[sym] = (now + _FAIL_TTL, None, None)
                continue
            entry = found.get(key)
            if entry is None:
                entries[sym] = (now + _MISS_TTL, None, None)
                continue
            local = entry.get("name_local") or None
            english = (entry.get("name") if entry.get("name") != local else None) or None
            entries[sym] = (now + _HIT_TTL, local, english)
        self._store(entries, now)
        return {sym: (e[1], e[2]) for sym, e in entries.items()}


_host: NameDirectory | None = None


async def _host_lookup(keys: list[str]) -> dict[str, dict[str, Any]]:
    from . import get_ginlix_data_client

    client = await get_ginlix_data_client()
    return await client.get_instruments_v2(keys)


async def display_names_many(
    symbols: Iterable[str],
) -> dict[str, tuple[str | None, str | None]]:
    """(local name, English name) per CN/HK listing via the host's ginlix-data client."""
    from src.config.settings import GINLIX_DATA_URL

    symbols = list(symbols)
    if not GINLIX_DATA_URL:
        return {s: (None, None) for s in symbols}
    global _host
    if _host is None:
        _host = NameDirectory(_host_lookup, deadline=_HOST_DEADLINE)
    return await _host.display_names_many(symbols)


async def attach_names(
    rows: Iterable[dict[str, Any]], *, local: str, english: str, display: str | None = None,
) -> None:
    """Fill the local and English names a CN/HK row lacks, under the caller's field names.

    Rows ship both names because a cached row is shared across users, so display
    order belongs to the client. A name the row already has is never replaced;
    *display*, when given, also names a row that has no name at all.
    """
    pending = [
        (str(r.get("symbol") or ""), r) for r in rows if not (r.get(local) and r.get(english))
    ]
    pending = [(sym, row) for sym, row in pending if sym]
    if not pending:
        return
    names = await display_names_many({sym for sym, _ in pending})
    for sym, row in pending:
        local_name, en_name = names[sym]
        if local_name and not row.get(local):
            row[local] = local_name
        if en_name and not row.get(english):
            row[english] = en_name
        if display and not row.get(display) and (en_name or local_name):
            row[display] = en_name or local_name


async def fill_quote_names(rows: Iterable[dict[str, Any]]) -> None:
    """:func:`attach_names` in quote-row spelling (``name_local`` / ``name_en`` / ``name``)."""
    await attach_names(rows, local="name_local", english="name_en", display="name")


async def display_names(symbol_or_code: str) -> tuple[str | None, str | None]:
    """(local name, English name) for a CN/HK listing via the host's ginlix-data client."""
    return (await display_names_many([symbol_or_code]))[symbol_or_code]

