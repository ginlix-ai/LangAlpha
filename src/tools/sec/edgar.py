"""EDGAR reads for get_sec_filing, all through edgar_engine.

What this module adds around the engine: the deployment's object storage as the engine's cache store, so every
worker and container shares parsed filings; a statements cache; one read per filing however many calls in this
process want it at once (single flight); and a pool of its own, so reads waiting on SEC's rate limit never hold the
event loop's default executor that the rest of the worker shares.
"""

from __future__ import annotations

import asyncio
import contextvars
import gzip
import json
import logging
import os
import threading
import zlib
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from functools import partial, wraps
from importlib.metadata import version
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, TypeVar

from edgar_engine import FilingSections, SubmissionDocument
from edgar_engine.amendments import amended_sections, read
from edgar_engine.cache import DocumentsCache, LocalDirStore, SectionsCache, Store
from edgar_engine.documents import read_with_documents
from edgar_engine.filings import FilingRef, base_form, get_filing, list_filings, select_filing
from edgar_engine.submission import TruncatedSubmission

from .metering import note_cache
from .types import PERIODIC_FORMS

logger = logging.getLogger(__name__)

if not os.getenv("EDGAR_IDENTITY"):
    logger.warning("EDGAR_IDENTITY is not set: SEC asks every client to name itself with a name and an email "
                   "(\"Jane Doe jane@example.com\"), and may block requests without one")
    os.environ["EDGAR_IDENTITY"] = "OpenSource user@example.com"

#: EDGAR's recent page holds at least the last year or this many filings, so a page this long that starts within
#: the 8-K window may not reach back to its start.
RECENT_PAGE_MIN = 1000
STATEMENTS_KEY = f"statements/v3/edgartools-{version('edgartools')}"
EIGHT_K_DAYS = 90
EIGHT_K_FORMS = ("8-K", "8-K/A")

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
        note_cache(key.split("/", 1)[0], "miss" if data is None else "hit")
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
            k = key(*args)
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


@dataclass(frozen=True)
class Company:
    symbol: str
    name: str
    cik: int
    obj: Any
    filings: list[dict]


def company(symbol: str) -> Company:
    """The company and its recent filing table, EDGAR's first submissions page (at least a year of filings). Its
    symbol is its primary ticker, so every spelling of one company (GOOG and GOOGL, BRK.B and BRK-B, its CIK)
    reads as one."""
    from edgar import Company as EdgarCompany
    from edgar.exceptions import CompanyNotFoundError

    try:
        c = EdgarCompany(symbol)
    except CompanyNotFoundError as e:
        raise NotAFiler(symbol) from e
    if not getattr(c, "cik", None) or getattr(c, "not_found", False):
        raise NotAFiler(symbol)
    table = c.get_filings(trigger_full_load=False)
    rows = table.data.to_pylist() if table is not None else []
    return Company((c.get_ticker() or symbol).upper(), c.name, int(c.cik), c, rows)


def filings_of(co: Company, form: str, labels: Labels, **kwargs: Any) -> list[FilingRef]:
    return list_filings(co.obj, form, fiscal_labels=labels, **kwargs)


def select(co: Company, form: str, labels: Labels, fiscal_year: int | None, fiscal_period: str | None) -> FilingRef | None:
    return select_filing(co.obj, form, fiscal_year=fiscal_year, fiscal_period=fiscal_period, fiscal_labels=labels)


def periodic_reports(co: Company, labels: Labels) -> list[FilingRef]:
    """The company's periodic originals on its recent page across 10-K, 10-Q, 20-F and 40-F, newest period first."""
    refs = {r.accession: r for form in PERIODIC_FORMS for r in filings_of(co, form, labels, full_history=False)
            if r.filing_status == "original" and base_form(r.form) in PERIODIC_FORMS}
    return sorted(refs.values(), key=lambda r: (r.report_date or "", r.filing_date), reverse=True)


@_shared(lambda co, ref: f"sections:{ref.accession}")
def sections(co: Company, ref: FilingRef) -> FilingSections:
    """The filing's sections as filed."""
    try:
        return read(ref, SectionsCache(store()))
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
    found = amended_sections(original, refs, SectionsCache(store()), unreadable=unreadable)
    for r in unreadable:
        logger.warning("sec_filing: amendment %s of %s unreadable", r.accession, original.accession)
    return found, [r for r in unreadable if r.amends == original.accession]


@dataclass(frozen=True)
class Financials:
    """Key metrics in the reporting currency (an ISO code, None when the filing does not state one) and the three
    statements the filing has as markdown, under the names in held. unread: the parts that could not be read this
    time, and failed: none could; neither is ever cached."""
    metrics: dict = field(default_factory=dict)
    statements: str | None = None
    currency: str | None = None
    failed: bool = False
    unread: tuple[str, ...] = ()
    held: tuple[str, ...] = ()


@_shared(lambda ref: f"statements:{ref.accession}")
def financials(ref: FilingRef) -> Financials:
    """The filing's statements and key metrics, cached by accession and edgartools version once every part read."""
    key = f"{STATEMENTS_KEY}/{ref.accession}.json.gz"
    raw = store().get(key)
    if raw is not None:
        try:
            return Financials(**json.loads(gzip.decompress(raw)))
        except (OSError, EOFError, ValueError, TypeError, zlib.error):
            pass

    from .financials import read_financials
    try:
        metrics, parts, currency, unread = read_financials(get_filing(ref))
    except Exception:
        logger.warning("sec_filing: statements of %s unavailable", ref.accession, exc_info=True)
        return Financials(failed=True)
    if unread and not (metrics or parts):
        return Financials(failed=True)
    fin = Financials(metrics, "\n\n".join(parts.values()) or None, currency, unread=tuple(unread), held=tuple(parts))
    if not unread:
        store().put(key, gzip.compress(json.dumps(asdict(fin)).encode(), mtime=0))
    return fin


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
        table = co.obj.get_filings(form=list(EIGHT_K_FORMS), trigger_full_load=True)
        rows = table.data.to_pylist() if table is not None else []
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
    return read_with_documents(k.ref, SectionsCache(store()), DocumentsCache(store()))
