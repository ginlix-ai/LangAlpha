"""get_sec_filing's reads from ginlix-data, each of which the caller reads from EDGAR instead when it answers None.

ginlix-data holds a filer's company row, its whole filing history, its fiscal labels and its filing calendar, and each
parsed filing under the key edgar_engine's own caches write, so a filing reads the same from either side. The reads of
a filer (company, filings, labels, calendar) are HTTP calls to ginlix-data, never to SEC, so they run on a pool of
their own and leave the SEC pool to the reads waiting on SEC's rate limit; a parsed filing is read on the thread the
engine reads its cache on, since its Store is synchronous. Inside call_scope, once ginlix-data cannot be reached, the
call's remaining reads skip it rather than each waiting out its own timeout.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import partial
from typing import Callable, Mapping, TypeVar

import httpx
from edgar_engine.cache import PARSED_KINDS, Store, kind_of
from edgar_engine.calendar import rules
from edgar_engine.filings import labels_engine, labels_from_companyfacts, table_forms

from src.data_client.ginlix_data.sec_filings import (FilingCalendar, FiscalLabels, IndexedCompany, IndexedFilings,
                                                     SecFilingsClient, note_skew, sec_filings_client)

from .metering import note_cache
from .types import DELISTING_FORMS, EIGHT_K_FORMS, PERIODIC_FORMS

logger = logging.getLogger(__name__)

#: The notices of late filing and the Forms 15 a filing calendar names, read for their filing dates.
CALENDAR_FORMS = tuple(sorted({n for f in rules.PERIODIC_FORMS for n in f.notices} | rules.TERMINATIONS))
#: Every form this tool reads, as ginlix-data is asked for a filer's filings.
INDEXED_FORMS = (*sorted({f for form in PERIODIC_FORMS for f in table_forms(form, amendments=True)}), *EIGHT_K_FORMS,
                 *CALENDAR_FORMS, *DELISTING_FORMS)

T = TypeVar("T")

_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="sec-ginlix")
_unreachable: contextvars.ContextVar[threading.Event | None] = contextvars.ContextVar("sec_ginlix_unreachable",
                                                                                     default=None)


def cut_off() -> bool:
    """ginlix-data was found unreachable earlier in this call."""
    unreachable = _unreachable.get()
    return unreachable is not None and unreachable.is_set()


@contextmanager
def call_scope() -> Iterator[None]:
    """One tool call: after a read finds ginlix-data unreachable, the reads after it inside the scope skip it."""
    token = _unreachable.set(threading.Event())
    try:
        yield
    finally:
        _unreachable.reset(token)


def read(what: str, fn: Callable[[SecFilingsClient], T | None]) -> T | None:
    """fn over ginlix-data, or None when it is not configured, is unreachable in this call, does not have it, answers
    in another shape, or fails."""
    if (remote := sec_filings_client()) is None:
        return None
    if cut_off():
        note_cache(f"ginlix_{what}", "skipped")
        return None
    try:
        found = fn(remote)
    except Exception as e:
        if isinstance(e, httpx.TransportError) and (unreachable := _unreachable.get()) is not None:
            unreachable.set()
        logger.warning("sec_filing: could not read %s from ginlix-data", what, exc_info=True)
        note_cache(f"ginlix_{what}", "error")
        return None
    note_cache(f"ginlix_{what}", "miss" if found is None else "hit")
    return found


async def _read(what: str, fn: Callable[[SecFilingsClient], T | None]) -> T | None:
    if sec_filings_client() is None:
        return None
    ctx = contextvars.copy_context()
    return await asyncio.get_running_loop().run_in_executor(_pool, partial(ctx.run, read, what, fn))


async def company(symbol: str) -> tuple[IndexedCompany, IndexedFilings] | None:
    """The filer's company row and its whole history of INDEXED_FORMS, asked at the same time; None unless both
    answer for one filer and the row names it."""
    row, listed = await asyncio.gather(_read("company", lambda remote: remote.company(symbol)),
                                       _read("filings", lambda remote: remote.filings(symbol, INDEXED_FORMS)))
    if row is None or not row.name or listed is None or listed.cik != row.cik:
        return None
    return row, listed


async def labels(symbol: str) -> FiscalLabels | None:
    """The filer's fiscal labels, when ginlix-data computed them with this process's engine."""
    def computed_here(remote: SecFilingsClient) -> FiscalLabels | None:
        found = remote.fiscal_labels(symbol)
        if found is None or found.engine == labels_engine():
            return found
        note_skew("fiscal labels", found.engine, labels_engine())
        return None

    return await _read("fiscal_labels", computed_here)


async def calendar(symbol: str) -> FilingCalendar | None:
    return await _read("calendar", lambda remote: remote.calendar(symbol))


async def companyfacts_labels(cik: int) -> Mapping[tuple[str, str], tuple[int, str]] | None:
    """The filer's fiscal labels, computed here from ginlix-data's company facts."""
    def computed(remote: SecFilingsClient) -> Mapping[tuple[str, str], tuple[int, str]] | None:
        facts = remote.companyfacts(cik)
        return None if facts is None else labels_from_companyfacts(facts)

    return await _read("companyfacts", computed)


class CacheThenGinlixStore:
    """The cache, then ginlix-data's parsed filings for what it misses; writes go to the cache only. The cache comes
    first so a filing ginlix-data serves in a form this engine cannot read is parsed once and read locally after."""

    def __init__(self, cache: Store, cik: int):
        self.cache, self.cik = cache, cik

    def get(self, key: str) -> bytes | None:
        kind = kind_of(key)
        return self.cache.get(key) or (read(kind, lambda remote: remote.parsed(key, self.cik))
                                       if kind in PARSED_KINDS else None)

    def put(self, key: str, data: bytes) -> None:
        self.cache.put(key, data)

    def delete(self, key: str) -> None:
        self.cache.delete(key)
