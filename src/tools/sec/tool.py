"""get_sec_filing: one SEC filing as a short inline result, its full text written to files in the workspace.

PTC agents get the tool from create_sec_filing_tool(filesystem_backend), which writes each filing's items, full
text, statements and earnings call under data/sec/. The market-insight agent has no workspace, so its
get_sec_filing returns the same index with the MD&A and the earnings call inline, together cut at 40k tokens.

Each flow reads everything into one record first (Periodic, EightK), and the files, the index and the inline form
are each built from the record.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass, field, replace
from typing import Annotated, Any, Literal, NamedTuple

from edgar_engine import SECTIONS_VERSION, FilingSections, SubmissionDocument
from edgar_engine.amendments import merge, restates_statements
from edgar_engine.documents import exhibit_index
from edgar_engine.filings import FilingRef, base_form, companyfacts_labels
from edgar_engine.forms import item_order, item_title, mdna_item
from edgar_engine.sections.vocab import is_amendment
from langchain_core.tools import tool

from . import edgar, render
from .earnings_call import fetch_matching_earnings_call
from .metering import metered_call
from .types import FOREIGN_FORMS, FilingType

logger = logging.getLogger(__name__)

INSIGHT_CAP_CHARS = 160_000
_PRESS_RELEASE = re.compile(r"\b(?:press|news|earnings)\s+release\b", re.I)
FORMS = tuple(t.value for t in FilingType)
QUARTERS = ("Q1", "Q2", "Q3")

Symbol = Annotated[str, "Stock ticker symbol (e.g., 'AAPL', 'MSFT', 'NVDA') or SEC CIK"]
Form = Annotated[str, (
    '"10-K" for full-year depth (audited, filed ~60 days after fiscal year end); "10-Q" for the most recent '
    'quarter (filed ~45 days after quarter end); "20-F" or "40-F" for a foreign issuer\'s annual report; "8-K" '
    "for every event-driven filing of the last 90 days")]
FiscalYear = Annotated[int | None, "The fiscal year as the company labels it, e.g. 2023. Omit for the latest filing. Not for 8-K"]
FiscalPeriod = Annotated[str | None, '"Q1", "Q2" or "Q3", with a 10-Q. Omit for the latest quarter of fiscal_year']
ViewArg = Annotated[Literal["as_filed", "as_amended"], (
    '"as_filed" for the filing as first filed; "as_amended" to merge in its later amendments (not for 8-K), each item naming '
    "the filing its text comes from")]
IncludeCall = Annotated[bool, "Also return the earnings call transcript for the filing's period (10-K and 10-Q)"]


async def _call_note(symbol: str, ref: FilingRef, include: bool) -> tuple[render.CallNote | None, str]:
    if not include:
        return None, ""
    form = base_form(ref.form)
    if form not in ("10-K", "10-Q"):
        return render.CallNote("unsupported"), ""
    if not ref.report_date:
        return render.CallNote("no_match", period_end="not stated"), ""
    try:
        call = await fetch_matching_earnings_call(symbol, FilingType(form), ref.report_date)
    except Exception:
        logger.warning("sec_filing: earnings call for %s %s unavailable", symbol, ref.accession, exc_info=True)
        return render.CallNote("failed"), ""
    if call is None:
        return render.CallNote("no_match", period_end=ref.report_date), ""
    note = render.CallNote("included" if call.transcript else "unpublished", call.quarter, call.fiscal_year,
                           call.call_date.isoformat(), ref.report_date, len(call.transcript))
    return note, call.transcript


def _rows(fs: FilingSections, prefix: str = "") -> list[render.ItemRow]:
    return [render.ItemRow(s.item, render.title(fs, s), render.status_cell(fs, s), len(render.item_body(fs, s)),
                           prefix + render.item_file_name(s.item))
            for s in fs.items.values()]


def _item_files(symbol: str, cik: int, period: str, fs: FilingSections, filed: dict[str, str],
                prefix: str = "") -> dict[str, str]:
    return {prefix + render.item_file_name(s.item): render.item_file(
                symbol, fs.form, period, s, fs, edgar.index_url(cik, s.source), s.source, filed[s.source])
            for s in fs.items.values()}


class Index(NamedTuple):
    """An index file's body and the files it lists."""
    body: str
    files: dict[str, str]


def _mark(files: dict[str, str]) -> str:
    digest = hashlib.sha1()
    for name in sorted(files):
        digest.update(f"{name}\0{files[name]}\0".encode())
    return f"<!-- edgar-engine sections v{SECTIONS_VERSION}, files {digest.hexdigest()[:12]} -->"


async def _deliver(backend: Any, folder: str, indexes: dict[str, Index], always: dict[str, str]) -> str | None:
    """Write each index's files and then the index, which ends with a mark of those files' names and contents; the
    reason they could not all be written, or None. An index is written only after its files, so an index carrying
    the mark of its files shows they were all written as they are now, and they are not written again. always is
    written every call."""
    if backend.filesystem_config.enable_path_validation and not backend.validate_path(folder):
        return f"{folder} is outside the allowed directories"
    files, marked = dict(always), {}
    for name, index in indexes.items():
        mark = _mark(index.files)
        try:
            current = mark in (await backend.aread_text(f"{folder}/{name}") or "")
        except Exception:
            current = False
        if not current:
            files.update(index.files)
        marked[name] = f"{index.body}\n{mark}\n"
    for batch in (files, marked):
        if error := await _upload(backend, folder, batch):
            return error
    return None


async def _upload(backend: Any, folder: str, files: dict[str, str]) -> str | None:
    if not files:
        return None
    try:
        results = await backend.aupload_files([(f"{folder}/{name}", c.encode()) for name, c in files.items()])
    except Exception as e:
        logger.warning("sec_filing: writing %s failed", folder, exc_info=True)
        return (str(e) or type(e).__name__)[:200]
    errors = [f"{r.path}: {r.error}" for r in results if getattr(r, "error", None)]
    return errors[0] if errors else None


def _cap(text: str, budget: int, cut: str) -> tuple[str, int, bool]:
    if len(text) <= budget:
        return text, budget - len(text), False
    if budget < len(cut):
        return "", 0, True
    return text[:budget - len(cut) - 1].rstrip() + "\n" + cut, 0, True


@dataclass(frozen=True)
class View:
    """One way of reading the filing: as filed, or as amended. figures_from: the amendment the figures come from,
    when one restated the statements; prefix: where its files go in the folder."""
    name: str
    sections: FilingSections
    fin: edgar.Financials
    prefix: str = ""
    figures_from: FilingRef | None = None


@dataclass
class Periodic:
    co: edgar.Company
    ref: FilingRef
    views: list[View]
    amendments: list[FilingSections]
    unread: list[FilingRef]
    reports: list[FilingRef]
    recent_8k: list[edgar.EightKRef]
    call: render.CallNote | None
    transcript: str

    @property
    def shown(self) -> View:
        return self.views[-1]

    @property
    def url(self) -> str:
        return edgar.index_url(self.ref.cik, self.ref.accession)

    @property
    def folder(self) -> str:
        period = render.period_label(self.ref, short=True).replace(" ", "-")
        return f"data/sec/{self.co.symbol}/{base_form(self.ref.form)}_{period}_{self.ref.accession}"

    @property
    def call_included(self) -> bool:
        return self.call is not None and self.call.status == "included"


async def _read_periodic(symbol: str, form: str, fiscal_year: int | None, fiscal_period: str | None, view: str,
                         include_call: bool) -> Periodic | str:
    co = await edgar.run(edgar.company, symbol)
    labels = await edgar.run(companyfacts_labels, co.cik)
    ref = await edgar.run(edgar.select, co, form, labels, fiscal_year, fiscal_period)
    if ref is None:
        on_file = await edgar.run(lambda: [render.period_label(r, short=True)
                                           for r in edgar.filings_of(co, form, labels, full_history=False)[:5]])
        return render.no_filing(co.symbol, form, fiscal_year, fiscal_period, on_file)

    async def as_filed() -> View:
        sections = await edgar.sections(co, ref)
        return View("as_filed", sections, await edgar.financials(ref))

    async def recent_8ks() -> list[edgar.EightKRef]:
        """The 8-K reminder, which only adds to the result: when it cannot be read, the filing still returns."""
        if fiscal_year is not None or fiscal_period is not None:
            return []
        try:
            return await edgar.run(edgar.recent_8ks, co, ref)
        except Exception:
            logger.warning("sec_filing: recent 8-Ks of %s unavailable", co.symbol, exc_info=True)
            return []

    (call, transcript), filed, (amendments, unread), reports, recent_8k = await asyncio.gather(
        _call_note(co.symbol, ref, include_call), as_filed(), edgar.amendments(co, labels, ref),
        edgar.run(edgar.periodic_reports, co, labels), recent_8ks())
    views = [filed]
    if view == "as_amended" and amendments:
        restating = next((a for a in reversed(amendments) if restates_statements(a)), None)
        source = restating and FilingRef(co.cik, restating.accession, restating.form, restating.filing_date)
        fin = await edgar.financials(source) if source else filed.fin
        views.append(View("as_amended", merge(filed.sections, amendments), fin, "as_amended/", source))
    return Periodic(co, ref, views, amendments, unread, reports, recent_8k, call, transcript)


def _index(p: Periodic, v: View, folder: str | None, with_call: bool = True) -> str:
    notes = [render.AmendmentNote(a.accession or "", a.filing_date or "", a.scope,
                                  [k for k, s in a.items.items() if s.status != "stub"]) for a in p.amendments]
    unread = [render.AmendmentNote(r.accession, r.filing_date, None) for r in p.unread]
    figures = v.figures_from and (v.figures_from.accession, edgar.index_url(p.ref.cik, v.figures_from.accession))
    return render.periodic_inline(
        name=p.co.name, symbol=p.co.symbol, ref=p.ref, url=p.url, view_text=render.view_line(v.name, notes, unread),
        metrics=render.metrics_section(v.fin, p.ref.filing_date, figures), folder=folder,
        rows=_rows(v.sections, v.prefix),
        missing=[i for i in item_order(v.sections.form, v.sections.filing_date) if i not in v.sections.items],
        full_chars=len(p.views[0].sections.text),
        statements=(_statements_file(v), len(v.fin.statements), v.fin.held) if v.fin.statements else None,
        call=render.call_line(p.call, in_file=folder is not None) if with_call else None,
        recent=render.recent_reports_section(p.reports, p.ref, foreign=base_form(p.ref.form) in FOREIGN_FORMS),
        recent_8k=render.recent_8k_section(p.recent_8k), amended=v is not p.views[0])


def _statements_file(v: View) -> str:
    """The view's own statements file when an amendment restated them, else the filing's."""
    return f"{v.prefix}statements.md" if v.figures_from else "statements.md"


def _view_files(p: Periodic, v: View) -> dict[str, str]:
    period = render.period_label(p.ref, short=True)
    filed = {p.ref.accession: p.ref.filing_date, **{a.accession: a.filing_date for a in p.amendments}}
    files = _item_files(p.co.symbol, p.co.cik, period, v.sections, filed, v.prefix)
    if v is p.views[0]:
        files["full.md"] = v.sections.text
    if v.fin.statements and (v is p.views[0] or v.figures_from):
        files[_statements_file(v)] = v.fin.statements
    return files


def _periodic_insight(p: Periodic) -> str:
    """The index with the MD&A and the earnings call inline, together cut at INSIGHT_CAP_CHARS."""
    v = p.shown
    head = _index(p, v, None, with_call=not p.call_included)
    budget, parts = INSIGHT_CAP_CHARS - len(head) - 1, []
    section = v.sections.items.get(mdna_item(v.sections.form) or "")
    if section is not None:
        source = "" if section.source == p.ref.accession else render.from_accession(
            section.source, edgar.index_url(p.ref.cik, section.source))
        heading = f"## Item {section.item}: {render.title(v.sections, section)}\n{source}"
        text, budget, cut = _cap(render.item_body(v.sections, section), budget - len(heading), render.CUT_ITEM)
        if text:
            parts.append(heading + text)
        if cut:
            budget = 0
    c = p.call
    heading = c and (f"## Earnings call: Q{c.quarter} FY{c.fiscal_year}, held {c.call_date}, "
                     f"reporting the period ended {c.period_end}\n")
    if p.call_included and budget > 2 + len(heading):
        text, _, _ = _cap(p.transcript, budget - 2 - len(heading), render.CUT_CALL)
        if text:
            parts.append(heading + text)
    return head + ("\n" + "\n\n".join(parts) if parts else "")


def _periodic_artifact(p: Periodic) -> dict:
    return {
        "type": "sec_filing", "symbol": p.co.symbol, "filing_type": base_form(p.ref.form),
        "filing_date": p.ref.filing_date, "period_end": p.ref.report_date, "cik": str(p.ref.cik),
        "source_url": p.url, "accession": p.ref.accession, "fiscal_year": p.ref.fiscal_year,
        "fiscal_period": p.ref.fiscal_period, "view": p.shown.name, "sections_extracted": len(p.shown.sections.items),
        "has_earnings_call": p.call_included, "recent_8k_count": len(p.recent_8k),
    }


async def _periodic(backend: Any | None, symbol: str, form: str, fiscal_year: int | None, fiscal_period: str | None,
                    view: str, include_call: bool) -> tuple[str, dict]:
    p = await _read_periodic(symbol, form, fiscal_year, fiscal_period, view, include_call)
    if isinstance(p, str):
        return p, {}
    artifact = _periodic_artifact(p)
    if backend is None:
        return _periodic_insight(p), artifact
    indexes = {f"{v.prefix}index.md": Index(_index(p, v, p.folder), _view_files(p, v)) for v in p.views}
    always = {"earnings_call.md": render.call_file(p.co.symbol, base_form(p.ref.form), p.ref.accession, p.call,
                                                   p.transcript)} if p.call_included else {}
    error = await _deliver(backend, p.folder, indexes, always)
    if error:
        unwritten = _index(p, p.shown, None)
        return unwritten + "\n" + render.files_not_written(p.folder, error) + "\n", artifact
    artifact["folder"] = p.folder
    return indexes[f"{p.shown.prefix}index.md"].body, artifact


@dataclass
class EightK:
    """One 8-K as read: sections None when it could not be read. files are its item files (or, with no item found,
    its whole text as full.md) and its documents' files; documents lists each document as (file, type, label, size)."""
    k: edgar.EightKRef
    folder: str
    sections: FilingSections | None = None
    attached: list[SubmissionDocument] = field(default_factory=list)
    files: dict[str, str] = field(default_factory=dict)
    documents: list[tuple[str, str, str, int]] = field(default_factory=list)

    @property
    def items(self) -> tuple[str, ...]:
        """The items EDGAR lists for the 8-K, else the ones read from its text."""
        return self.k.items or (tuple(self.sections.items) if self.sections else ())

    def entry(self, written: bool) -> render.EightKEntry:
        k, fs = self.k, self.sections
        entry = render.EightKEntry(k.ref.filing_date, k.ref.form, k.ref.accession, k.url,
                                   render.items_phrase(self.items, k.ref.filing_date), unreadable=fs is None)
        if not written or fs is None:
            return entry
        own = ["full.md"] if _whole(fs) else [render.item_file_name(i) for i in fs.items]
        chars = len(fs.text) if _whole(fs) else sum(len(render.item_body(fs, s)) for s in fs.items.values())
        return replace(entry, folder=self.folder, item_files=own, item_chars=chars, documents=self.documents)

    def artifact(self, written: bool) -> dict:
        k = self.k
        row = {"filing_date": k.ref.filing_date, "form": k.ref.form, "accession": k.ref.accession,
               "items": list(self.items),
               "items_desc": [item_title("8-K", i, k.ref.filing_date) or "" for i in self.items],
               "source_url": k.url,
               "has_press_release": any(_PRESS_RELEASE.search(label) for _, _, label, _ in self.documents)}
        return {**row, "folder": self.folder} if written else row


def _whole(fs: FilingSections) -> bool:
    """An 8-K kept whole rather than as items: one with no item found, or an 8-K/A, whose explanatory note comes
    before its first item."""
    return not fs.items or is_amendment(fs.form)


async def _read_8k(symbol: str, k: edgar.EightKRef) -> EightK:
    record = EightK(k, f"data/sec/{symbol}/8-K_{k.ref.filing_date}_{k.ref.accession}")
    try:
        fs, attached = await edgar.eight_k(k)
    except Exception:
        logger.warning("sec_filing: 8-K %s unreadable", k.ref.accession, exc_info=True)
        return record
    filed, accession, url = k.ref.filing_date, k.ref.accession, k.url
    files = {"full.md": fs.text} if _whole(fs) else {
        render.item_file_name(s.item): render.item_file(symbol, fs.form, filed, s, fs, url, accession, filed)
        for s in fs.items.values()}
    exhibits, documents = exhibit_index(fs), []
    for d in attached:
        label = render.document_label(d, exhibits)
        name = render.document_file_name(d, label)
        stem, n = name[:-3], 2
        while name in files:
            name, n = f"{stem}-{n}.md", n + 1
        files[name] = render.document_file(symbol, filed, d, label, accession, url)
        documents.append((name, d.type, label, len(d.text)))
    return replace(record, sections=fs, attached=attached, files=files, documents=documents)


async def _eight_ks(backend: Any | None, symbol: str) -> tuple[str, dict]:
    co = await edgar.run(edgar.company, symbol)
    refs = await edgar.run(edgar.recent_8ks, co)
    if not refs:
        return render.no_8ks(co.symbol, edgar.EIGHT_K_DAYS), {}
    filings = await asyncio.gather(*(_read_8k(co.symbol, k) for k in refs))
    artifact = {"type": "sec_filing", "symbol": co.symbol, "filing_type": "8-K", "filing_count": len(refs),
                "days_range": edgar.EIGHT_K_DAYS}

    if backend is None:
        content = render.eight_k_inline(co.name, co.symbol, [f.entry(False) for f in filings], folders=False)
        budget = INSIGHT_CAP_CHARS - len(content)
        for text in (t for f in filings for t in f.files.values()):
            text, budget, cut = _cap(text, budget - 1, render.CUT_DOCUMENTS)
            content += "\n" + text
            if cut:
                break
        return content, {**artifact, "filings": [f.artifact(False) for f in filings]}

    async def write(f: EightK) -> str | None:
        if f.sections is None:
            return None
        index = render.eight_k_inline(co.name, co.symbol, [f.entry(True)])
        return await _deliver(backend, f.folder, {"index.md": Index(index, f.files)}, {})

    errors = await asyncio.gather(*(write(f) for f in filings))
    written = [f.sections is not None and not e for f, e in zip(filings, errors)]
    content = render.eight_k_inline(co.name, co.symbol, [f.entry(w) for f, w in zip(filings, written)])
    for f, error in zip(filings, errors):
        if error:
            content += render.files_not_written(f.folder, error) + "\n"
    return content, {**artifact, "filings": [f.artifact(w) for f, w in zip(filings, written)]}


async def get_sec_filing_async(symbol: str, filing_type: str = "10-K", fiscal_year: int | None = None,
                               fiscal_period: str | None = None, view: str = "as_filed",
                               include_earnings_call: bool = True, backend: Any | None = None) -> tuple[str, dict]:
    """The tool's content and artifact. backend None returns the market-insight agent's inline form."""
    form = filing_type.strip().upper()
    if form not in FORMS:
        return render.bad_form(filing_type), {}
    if view not in ("as_filed", "as_amended"):
        return render.bad_view(view), {}
    period = fiscal_period.strip().upper() if fiscal_period else None
    if form == "8-K" and (fiscal_year is not None or period):
        return render.eight_k_period(), {}
    if form == "8-K" and view == "as_amended":
        return render.eight_k_amended(), {}
    if period and form != "10-Q":
        return render.period_on_annual(form), {}
    if period and period not in QUARTERS:
        return render.bad_period(period), {}
    case = dict(symbol=symbol.upper(), form=form, view=view, latest=fiscal_year is None and period is None)
    with metered_call(**case) as meter:
        try:
            if form == "8-K":
                return await _eight_ks(backend, symbol)
            return await _periodic(backend, symbol, form, fiscal_year, period, view, include_earnings_call)
        except edgar.NotAFiler:
            meter.fail("NotAFiler")
            return render.not_a_filer(symbol.upper(), form), {}
        except edgar.UnreadableFiling as e:
            meter.fail("UnreadableFiling")
            return render.unreadable(e.accession, e.url), {}


DOCSTRING = """Fetch an SEC filing (10-K, 10-Q, 20-F, 40-F or 8-K) for any fiscal year,
as filed or with its amendments merged in.

US SEC filers only. A non-US listing ("0700.HK", "600519.SH") has no filing
to fetch.

Returns:
    An index of the filing's items and its key metrics, not the items' text:
    that is written to files in your workspace.

Read only the item files your question needs. The same filing always lands in the same folder, so read its item
files from data/sec/ instead of fetching it again; fetch again only to see amendments or filings made since. You
MUST cite the source URL inline for every
claim you take from the filing, so the user can verify each figure against
SEC.gov instead of taking it on trust.
"""

INSIGHT_DOCSTRING = """Fetch an SEC filing (10-K, 10-Q, 20-F, 40-F or 8-K) for any fiscal year,
as filed or with its amendments merged in.

US SEC filers only. A non-US listing ("0700.HK", "600519.SH") has no filing
to fetch.

Returns:
    An index of the filing's items, then its MD&A and earnings call (for an
    8-K, each filing's text and exhibits), together up to 40k tokens.

You MUST cite the source URL inline for every claim you take from the
filing, so the user can verify each figure against SEC.gov instead of taking
it on trust.
"""


def create_sec_filing_tool(filesystem_backend: Any) -> Any:
    """get_sec_filing bound to the agent's filesystem backend, which the filing's files are written through."""

    @tool("get_sec_filing", description=DOCSTRING, response_format="content_and_artifact")
    async def get_sec_filing(symbol: Symbol, filing_type: Form = "10-K", fiscal_year: FiscalYear = None,
                             fiscal_period: FiscalPeriod = None, view: ViewArg = "as_filed",
                             include_earnings_call: IncludeCall = True) -> tuple[str, dict]:
        return await get_sec_filing_async(symbol, filing_type, fiscal_year, fiscal_period, view,
                                          include_earnings_call, backend=filesystem_backend)

    return get_sec_filing


@tool("get_sec_filing", description=INSIGHT_DOCSTRING, response_format="content_and_artifact")
async def get_sec_filing(symbol: Symbol, filing_type: Form = "10-K", fiscal_year: FiscalYear = None,
                         fiscal_period: FiscalPeriod = None, view: ViewArg = "as_filed",
                         include_earnings_call: IncludeCall = True) -> tuple[str, dict]:
    return await get_sec_filing_async(symbol, filing_type, fiscal_year, fiscal_period, view, include_earnings_call)

