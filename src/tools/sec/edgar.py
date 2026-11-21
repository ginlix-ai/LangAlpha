"""EDGAR reads for get_sec_filing, all through edgar_engine.

What this module adds around the engine: the deployment's object storage as the engine's cache store, so every
worker and container shares parsed filings; a statements cache; one read per filing however many calls in this
process want it at once (single flight); and a pool of its own, so reads waiting on SEC's rate limit never hold the
event loop's default executor that the rest of the worker shares.

With ginlix-data configured, the company, its filings, its fiscal labels and each parsed filing are read from
ginlix-data first (see indexed), and each falls back to EDGAR on its own when ginlix-data does not answer for it.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import partial, wraps
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, TypeVar
from zoneinfo import ZoneInfo

from edgar_engine import FilingSections, SubmissionDocument, statements
from edgar_engine.amendments import amended_sections, read
from edgar_engine.cache import DocumentsCache, LocalDirStore, SectionsCache, StatementsCache, Store, kind_of
from edgar_engine.documents import read_with_documents
from edgar_engine.filings import FilingRef, base_form, companyfacts_labels, refs_for_form, select_ref, table_forms
from edgar_engine.statements import Financials
from edgar_engine.submission import TruncatedSubmission

from src.data_client.ginlix_data.sec_filings import FilingCalendar, sec_filings_client

from . import indexed
from .metering import note_cache
from .types import EIGHT_K_FORMS, PERIODIC_FORMS

logger = logging.getLogger(__name__)

if not os.getenv("EDGAR_IDENTITY"):
    logger.warning("EDGAR_IDENTITY is not set: SEC asks every client to name itself with a name and an email "
                   "(\"Jane Doe jane@example.com\"), and may block requests without one")
    os.environ["EDGAR_IDENTITY"] = "OpenSource user@example.com"

#: EDGAR's recent page holds at least the last year or this many filings, so a page this long that starts within
#: the 8-K window may not reach back to its start.
RECENT_PAGE_MIN = 1000
EIGHT_K_DAYS = 90
SEC_TIME = ZoneInfo("America/New_York")

T = TypeVar("T")


class NotAFiler(Exception):
    pass


class UnreadableFiling(Exception):
    def __init__(self, accession: str, url: str):
        super().__init__(accession)
        self.accession, self.url = accession, url


def index_url(cik: int | str, accession: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}-index.htm"


class _StorageStore:
    """A Store over the deployment's object storage."""

    def get(self, key: str) -> bytes | None:
        from src.utils.storage import get_bytes
        return get_bytes(f"edgar/{key}")

    def put(self, key: str, data: bytes) -> None:
        from src.utils.storage import upload_bytes
        upload_bytes(f"edgar/{key}", data, content_type="application/gzip")

    def delete(self, key: str) -> None:
        from src.utils.storage import delete_object
        delete_object(f"edgar/{key}")


class _MeteredStore:
    """Records each read as a hit or a miss on the call's meter, under the key's first segment (sections,
    documents, statements)."""

    def __init__(self, inner: Store):
        self.inner = inner

    def get(self, key: str) -> bytes | None:
        try:
            data = self.inner.get(key)
        except Exception:
            logger.warning("sec_filing: could not read %s from the cache", key, exc_info=True)
            data = None
        note_cache(kind_of(key), "miss" if data is None else "hit")
        return data

    def put(self, key: str, data: bytes) -> None:
        try:
            self.inner.put(key, data)
        except Exception:
            logger.warning("sec_filing: could not cache %s", key, exc_info=True)

    def delete(self, key: str) -> None:
        try:
            self.inner.delete(key)
        except Exception:
            logger.warning("sec_filing: could not drop %s from the cache", key, exc_info=True)


_store: Store | None = None
_store_lock = threading.Lock()


def store() -> Store:
    global _store
    with _store_lock:
        if _store is None:
            from src.utils.storage import is_storage_enabled
            _store = _MeteredStore(_StorageStore() if is_storage_enabled() else LocalDirStore(
                os.getenv("SEC_FILINGS_CACHE_DIR") or Path.home() / ".cache" / "langalpha" / "edgar"))
        return _store


def filing_store(cik: int) -> Store:
    """store(), and ginlix-data's parsed filings behind it when ginlix-data is configured. cik names the filer of the
    filings read through it, which ginlix-data's parsed reads take."""
    return indexed.CacheThenGinlixStore(store(), cik) if sec_filings_client() else store()


_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sec")
_inflight: dict[str, Future] = {}
_inflight_lock = threading.Lock()


async def run(fn: Callable[..., T], *args: Any) -> T:
    """fn on the SEC pool, carrying the call's context (and so its meter) with it."""
    ctx = contextvars.copy_context()
    return await asyncio.get_running_loop().run_in_executor(_pool, partial(ctx.run, fn, *args))


def _shared(key: Callable[..., str]) -> Callable[[Callable[..., T]], Callable[..., Awaitable[T]]]:
    """Makes fn a coroutine that runs it on the SEC pool once per key however many calls of this process ask for it
    at the same time. A call waiting on another's read holds no pool thread, and a cancelled call never cancels the
    read the others wait on."""
    def wrap(fn: Callable[..., T]) -> Callable[..., Awaitable[T]]:
        @wraps(fn)
        async def shared(*args: Any) -> T:
            k = key(*args) + (":without-ginlix" if indexed.cut_off() else "")
            with _inflight_lock:
                fut = _inflight.get(k)
                owner = fut is None
                if owner:
                    fut = _inflight[k] = _pool.submit(contextvars.copy_context().run, fn, *args)
            if owner:
                fut.add_done_callback(partial(_forget, k))
            return await asyncio.shield(asyncio.wrap_future(fut))
        return shared
    return wrap


def _forget(key: str, fut: Future) -> None:
    with _inflight_lock:
        if _inflight.get(key) is fut:
            del _inflight[key]


Labels = Mapping[tuple[str, str], tuple[int, str]]
Rows = Callable[[list[str], bool], list[dict]]


@dataclass(frozen=True)
class Company:
    """A filer and its filings table: filings is EDGAR's recent page, or ginlix-data's whole history of
    indexed.INDEXED_FORMS. rows(forms, full_history) reads the table for those forms, the whole history when asked,
    which ginlix-data's always is."""
    symbol: str
    name: str
    cik: int
    rows: Rows = field(repr=False, compare=False)
    filings: list[dict]
    fiscal_year_end: str | None = None


def edgar_rows(c: Any) -> Rows:
    """The filings table of an edgar.Company: its recent page, or its whole history when asked."""
    def rows(forms: list[str], full_history: bool) -> list[dict]:
        table = c.get_filings(form=forms, trigger_full_load=full_history)
        return table.data.to_pylist() if table is not None else []
    return rows


def _edgar_company(symbol: str) -> Company:
    from edgar import Company as EdgarCompany
    from edgar.exceptions import CompanyNotFoundError

    try:
        c = EdgarCompany(symbol)
    except CompanyNotFoundError as e:
        raise NotAFiler(symbol) from e
    if not getattr(c, "cik", None) or getattr(c, "not_found", False):
        raise NotAFiler(symbol)
    table = c.get_filings(trigger_full_load=False)
    return Company((c.get_ticker() or symbol).upper(), c.name, int(c.cik), edgar_rows(c),
                   table.data.to_pylist() if table is not None else [], getattr(c, "fiscal_year_end", None))


async def company(symbol: str) -> Company:
    """The company and its filing table. Its symbol is its primary ticker, so every spelling of one company (GOOG
    and GOOGL, BRK.B and BRK-B, its CIK) reads as one."""
    if (found := await indexed.company(symbol)) is None:
        return await run(_edgar_company, symbol)
    row, listed = found
    table = listed.rows

    def rows(forms: list[str], full_history: bool) -> list[dict]:
        return [r for r in table if r["form"] in forms]

    return Company((row.tickers[0] if row.tickers else symbol).upper(), row.name, row.cik, rows, table,
                   row.fiscal_year_end)


@dataclass(frozen=True)
class Calendar:
    """A filer's filing calendar as read on today (in SEC's time zone), with the form and filing date of the notice
    and the Form 15 it names, as the filer's filings table holds them."""
    served: FilingCalendar
    filed: Mapping[str, tuple[str, str]]
    today: str


@dataclass(frozen=True)
class Filer:
    """A company, the fiscal labels of its filings, and its filing calendar when ginlix-data holds one."""
    co: Company
    labels: Labels
    calendar: Calendar | None


async def filer(symbol: str) -> Filer:
    """The company with its fiscal labels and filing calendar, all asked of ginlix-data at once. Labels and a
    calendar count only when they are of the company read: labels it does not answer for are computed from company
    facts, and a calendar it does not answer for is left out."""
    co, held, calendar = await asyncio.gather(company(symbol), indexed.labels(symbol), indexed.calendar(symbol))
    labels = held.labels if held is not None and held.cik == co.cik else await fiscal_labels(co)
    if calendar is None or calendar.cik != co.cik:
        return Filer(co, labels, None)
    named = {calendar.termination, calendar.next and calendar.next.notice} - {None}
    filed = {r["accession_number"]: (r["form"], str(r["filing_date"])) for r in co.filings
             if r["accession_number"] in named}
    return Filer(co, labels, Calendar(calendar, filed, datetime.now(SEC_TIME).date().isoformat()))


async def fiscal_labels(co: Company) -> Labels:
    """The fiscal year and period each of the company's filings declares in its XBRL facts, computed here from its
    company facts (ginlix-data's, else EDGAR's)."""
    found = await indexed.companyfacts_labels(co.cik)
    return found if found is not None else await run(companyfacts_labels, co.cik)


def filings_of(co: Company, form: str, labels: Labels, *, amendments: bool = False,
               full_history: bool = True) -> list[FilingRef]:
    return refs_for_form(co.rows(table_forms(form, amendments), full_history), co.cik, form, amendments=amendments,
                         fiscal_labels=labels, fiscal_year_end=co.fiscal_year_end)


def select(co: Company, form: str, labels: Labels, fiscal_year: int | None, fiscal_period: str | None) -> FilingRef | None:
    return select_ref(lambda full_history: filings_of(co, form, labels, full_history=full_history),
                      fiscal_year=fiscal_year, fiscal_period=fiscal_period)


def periodic_reports(co: Company, labels: Labels) -> list[FilingRef]:
    """The company's periodic originals on its filing table across 10-K, 10-Q, 20-F and 40-F, newest period first."""
    refs = {r.accession: r for form in PERIODIC_FORMS for r in filings_of(co, form, labels, full_history=False)
            if r.filing_status == "original" and base_form(r.form) in PERIODIC_FORMS}
    return sorted(refs.values(), key=lambda r: (r.report_date or "", r.filing_date), reverse=True)


@_shared(lambda co, ref: f"sections:{ref.accession}")
def sections(co: Company, ref: FilingRef) -> FilingSections:
    """The filing's sections as filed."""
    try:
        return read(ref, SectionsCache(filing_store(co.cik)))
    except TruncatedSubmission as e:
        raise UnreadableFiling(e.accession, index_url(co.cik, e.accession)) from e


@_shared(lambda co, labels, original: f"amendments:{original.accession}")
def amendments(co: Company, labels: Labels, original: FilingRef) -> tuple[list[FilingSections], list[FilingRef]]:
    """The original's amendments as filed, in filing order, each placed by its own text (see
    edgar_engine.amendments), and the ones the filing list places on it that could not be read."""
    on_recent_page = any(r.get("accession_number") == original.accession for r in co.filings)
    refs = filings_of(co, base_form(original.form), labels, amendments=True, full_history=not on_recent_page)
    if not any(r.filing_status == "amendment" for r in refs):
        return [], []
    unreadable: list[FilingRef] = []
    found = amended_sections(original, refs, SectionsCache(filing_store(co.cik)), unreadable=unreadable)
    for r in unreadable:
        logger.warning("sec_filing: amendment %s of %s unreadable", r.accession, original.accession)
    return found, [r for r in unreadable if r.amends == original.accession]


@_shared(lambda ref: f"statements:{ref.accession}")
def financials(ref: FilingRef) -> Financials:
    """The filing's statements and key metrics, cached by accession and edgartools version once every part read."""
    return statements.read(ref, StatementsCache(filing_store(ref.cik)))


@dataclass(frozen=True)
class EightKRef:
    ref: FilingRef
    items: tuple[str, ...]
    url: str


def recent_8ks(co: Company, after: FilingRef | None = None, days: int = EIGHT_K_DAYS) -> list[EightKRef]:
    """The 8-Ks and 8-K/As filed in the last days, newest first; with after, only those EDGAR accepted after that
    filing, a same-day 8-K included when its acceptance time is later or unknown."""
    def received(r: dict) -> tuple[str, str]:
        at = r.get("acceptanceDateTime") or ""
        return str(r["filing_date"]), at.isoformat() if hasattr(at, "isoformat") else str(at)

    def later(r: dict) -> bool:
        filed, at = received(r)
        return filed > after.filing_date or filed == after.filing_date and (not at or not after.accepted
                                                                              or at > after.accepted)

    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = co.filings
    if len(rows) >= RECENT_PAGE_MIN and min(str(r["filing_date"]) for r in rows) >= cutoff:
        rows = co.rows(list(EIGHT_K_FORMS), True)
    rows = [r for r in rows if r.get("form") in EIGHT_K_FORMS and str(r["filing_date"]) >= cutoff
            and (after is None or later(r))]
    rows.sort(key=received, reverse=True)
    return [EightKRef(FilingRef(co.cik, r["accession_number"], r["form"], str(r["filing_date"])),
                      tuple(i.strip() for i in str(r.get("items") or "").split(",") if i.strip()),
                      index_url(co.cik, r["accession_number"]))
            for r in rows]


@_shared(lambda k: f"8-K:{k.ref.accession}")
def eight_k(k: EightKRef) -> tuple[FilingSections, list[SubmissionDocument]]:
    """An 8-K's items and the documents it carries, from one download of its submission."""
    through = filing_store(k.ref.cik)
    return read_with_documents(k.ref, SectionsCache(through), DocumentsCache(through))
