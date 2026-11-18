"""The sections pipeline: a full-text submission rendered, split into items, its stubs resolved and its
cross-reference index applied."""
from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING

from ..submission import parse_submission
from .boundaries import find_items
from .documents import decode, header, main_documents, require_complete, split
from .model import FilingSections, Section
from .pages import Pagination
from .rendering import render_documents
from .stubs import Resolver, is_stub
from . import xref
from .vocab import spec_for
from .version import SECTIONS_VERSION

if TYPE_CHECKING:
    from ..cache import SectionsCache, SubmissionCache

_ITEM_HEADING = re.compile(r"\W*(?:PART\s+[IV]+\W*)?ITEMS?\b", re.I)


def extract_sections(submission: bytes | str, *, form: str | None = None, filing_date: str | None = None,
                     accession: str | None = None) -> FilingSections:
    """Items of one filing from its full-text submission (the accession's .txt file).

    form, filing_date and accession default to the submission's SEC header; filing_date is YYYY-MM-DD or
    YYYYMMDD. Each item names the accession as its source. A submission with no document, a <DOCUMENT> left open, or an <SEC-DOCUMENT> never closed is
    refused: a partial download, or an HTTP cache that kept one, would otherwise parse into items cut short
    or missing with no sign of it.
    """
    text = decode(submission)
    require_complete(text)
    head = header(text)
    form = form or head.get("form")
    if not form:
        raise ValueError("form is not given and the submission header does not name it")
    filing_date = filing_date or head.get("filing_date")
    accession = accession or head.get("accession")
    if filing_date and re.fullmatch(r"\d{8}", filing_date):
        filing_date = f"{filing_date[:4]}-{filing_date[4:6]}-{filing_date[6:]}"
    if filing_date and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", filing_date):
        raise ValueError(f"filing_date {filing_date!r} is not YYYY-MM-DD")
    spec = spec_for(form, filing_date)
    rendering = render_documents(main_documents(split(text), form))
    lines, full = rendering.lines, rendering.text
    layout = find_items(rendering, spec)
    pages = Pagination(rendering)
    resolver = Resolver(rendering, pages, layout.spans, spec, layout.toc)

    items = {}
    for iid, sp in layout.spans.items():
        span = rendering.span(sp.start, sp.end)
        text = full[span.start:span.end].rstrip("\n")
        status, target = sp.status, None
        if status == "present" and spec.stubs and is_stub(_body(lines, sp.start, sp.end, spec)):
            status, target = "stub", resolver.resolve(iid, text, (sp.start, sp.end)).to_target(rendering)
        items[iid] = Section(item=iid, status=status, text=text, start=span.start, end=span.end,
                             heading=lines[sp.heading_line] if sp.heading_line >= 0 else "",
                             combined_with=sp.combined_with, target=target)
    if spec.xref_index:
        resolved = [iid for iid, s in items.items() if s.target is not None]
        xref.apply(items, rendering, pages, resolver, layout, list(spec.order))
        _retarget(items, resolved, rendering, layout, resolver)
    return FilingSections(accession=accession, form=form, filing_date=filing_date,
                          items={k: replace(s, source=accession) for k, s in items.items()}, text=full,
                          engine_version=SECTIONS_VERSION, documents=rendering.documents)


def _body(lines: list[str], start: int, end: int, spec) -> str:
    """An item's text below its heading, which is what makes it a stub or not."""
    headed = spec.titled or _ITEM_HEADING.match(lines[start])
    return "\n".join(lines[start + (1 if headed else 0):end]) or lines[start]


def _retarget(items: dict[str, Section], resolved: list[str], rendering, layout, resolver: Resolver) -> None:
    """Stub targets resolved before a cross-reference index cut short the items they point at, resolved again
    over the items as cut, so a target never carries the next item's pages. An item the index cut down to a
    pointer becomes a stub."""
    cut = {iid: rendering.line_at(s.end) for iid, s in items.items() if iid in layout.spans and s.status == "present"
           and s.end < rendering.offsets[layout.spans[iid].end]}
    if not cut:
        return
    spec = resolver.spec
    for iid, line in cut.items():
        layout.spans[iid].end = line
        if spec.stubs and is_stub(_body(rendering.lines, layout.spans[iid].start, line, spec)):
            items[iid].status = "stub"
            resolved.append(iid)
    for iid in resolved:
        s, sp = items.get(iid), layout.spans[iid]
        if s is not None and s.status == "stub":
            s.target = resolver.resolve(iid, s.text, (sp.start, sp.end)).to_target(rendering)


def sections_for_filing(filing, cache: SectionsCache | None = None,
                        submissions: SubmissionCache | None = None) -> FilingSections:
    """Items of an edgartools Filing as filed, each labeled with the filing's accession as its source, read
    through cache (a SectionsCache) when one is given. With
    submissions (a SubmissionCache), the raw submission is kept by accession and read from there; one that does not
    parse (a truncated download) is not kept. Without it, a download that does not parse is fetched once more
    (submission.parse_submission)."""
    def extract(raw):
        return extract_sections(raw, form=filing.form, filing_date=str(filing.filing_date), accession=filing.accession_no)

    def compute():
        if submissions is None:
            return parse_submission(filing, extract)
        raw = submissions.get_or_fetch(filing.accession_no, filing.full_text_submission)
        try:
            return extract(raw)
        except ValueError:
            submissions.delete(filing.accession_no)
            raise
    sections = compute() if cache is None else cache.get_or_compute(filing.accession_no, compute)
    return replace(sections, items={k: replace(s, source=s.source or sections.accession) for k, s in sections.items.items()})
