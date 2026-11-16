"""The sections pipeline: a full-text submission rendered, split into items, its stubs resolved and its
cross-reference index applied."""
from __future__ import annotations

import bisect
import re
from dataclasses import replace

from .boundaries import find_items
from .documents import decode, header, main_documents, split
from .model import FilingSections, Section, Target
from .pages import Pagination
from .rendering import render_documents
from .stubs import Resolver, is_stub
from . import xref
from .vocab import spec_for
from .version import SECTIONS_VERSION

_OPEN = re.compile(r"<DOCUMENT>", re.I)
_CLOSE = re.compile(r"</DOCUMENT>", re.I)


def extract_sections(submission: bytes | str, *, form: str | None = None, filing_date: str | None = None,
                     accession: str | None = None) -> FilingSections:
    """Items of one filing from its full-text submission (the accession's .txt file).

    form, filing_date and accession default to the submission's SEC header. A submission with a
    <DOCUMENT> left open is refused: a partial download, or an HTTP cache that kept one, would
    otherwise parse into items cut short with no sign of it.
    """
    text = decode(submission)
    if len(_OPEN.findall(text)) != len(_CLOSE.findall(text)):
        raise ValueError("the submission is truncated: a <DOCUMENT> is not closed")
    head = header(text)
    form = form or head.get("form")
    if not form:
        raise ValueError("form is not given and the submission header does not name it")
    filing_date = filing_date or head.get("filing_date")
    spec = spec_for(form, filing_date)
    rendering = render_documents(main_documents(split(text), form))
    lines, full = rendering.lines, rendering.text
    layout = find_items(rendering, spec)
    pages = Pagination(rendering)
    resolver = Resolver(rendering, pages, layout.spans, spec, layout.toc)

    def target_of(t):
        spans = [rendering.span(a, b) for a, b in t.lines]
        return Target(kind=t.kind, description=t.description, doc=t.doc, spans=spans,
                      text=rendering.text_of(spans) or None)

    items = {}
    for iid, sp in layout.spans.items():
        span = rendering.span(sp.start, sp.end)
        text = full[span.start:span.end].rstrip("\n")
        status, target = sp.status, None
        body = "\n".join(lines[sp.start + 1:sp.end]) or lines[sp.start]
        if status == "present" and spec.stubs and is_stub(body):
            status, target = "stub", target_of(resolver.resolve(iid, text, (sp.start, sp.end)))
        items[iid] = Section(item=iid, status=status, text=text, start=span.start, end=span.end,
                             heading=lines[sp.heading_line] if sp.heading_line >= 0 else "",
                             combined_with=sp.combined_with, target=target)
    vocab = list(spec.order) if spec.xref_index else []
    index = xref.find_index(lines, vocab) if vocab else None
    if index:
        _cross_referenced(index, rendering, pages, items)
    for iid in vocab:
        if iid in items or any(k[:-1] == iid and k[-1].isalpha() for k in items):
            continue
        rows = [h.line for h in layout.heads if h.kind == "item" and iid in h.ids and h.line in layout.toc]
        for row in rows:
            t = resolver.index_row(iid, row, [h for h in layout.heads if h.kind == "item" and h.line in layout.toc])
            if t.kind == "in_document" and t.lines:
                tg = target_of(t)
                items[iid] = Section(item=iid, status="xref", text=tg.text or "", start=tg.spans[0].start,
                                     end=tg.spans[-1].end, heading="", target=tg)
                break
    return FilingSections(accession=accession or head.get("accession"), form=form, filing_date=filing_date,
                          items=items, text=full, engine_version=SECTIONS_VERSION, documents=rendering.documents)


def _cross_referenced(index, rendering, pages, items) -> None:
    """Items a cross-reference index maps to pages and the body prints no heading for: xref sections of
    their page ranges, or stubs pointing at the index row when the pages cannot be found. An item printed
    with a heading ends where a later item's mapped pages begin, unless that item is its own lettered
    sub-item (market risk inside MD&A); the pages it prints itself are cut out of the mapped ranges."""
    lines, full, offs, line_at = rendering.lines, rendering.text, rendering.offsets, rendering.line_at
    rows = {row.line for row in index.rows if row.pages or row.na}
    for iid in [iid for iid, s in items.items() if s.status == "present" and line_at(s.start) in rows]:
        del items[iid]
    mapped = xref.item_ranges(index, pages)
    ranges = {iid: r for iid, r in mapped.items() if iid not in items}
    starts = sorted(line_at(s.start) for s in items.values() if s.status == "present")
    breaks = pages.breaks()
    for iid, sec in items.items():
        if sec.status != "present":
            continue
        line = line_at(sec.start)
        cut = min((r[0][0] for x, r in ranges.items() if not xref.is_sub_item(x, iid) and r[0][0] > line
                   and not _shared(r[0][0], starts, breaks)), default=None)
        own = next((b for a, b in mapped.get(iid, []) if a <= line < b), None)
        if own is not None and any(a <= own < b for x, r in ranges.items() if not xref.is_sub_item(iid, x) for a, b in r):
            cut = min(own, cut) if cut is not None else own
        if cut is not None and offs[cut] < sec.end:
            sec.end = offs[cut]
            sec.text = full[sec.start:sec.end].rstrip("\n")
    present = {iid: (line_at(s.start), line_at(s.end)) for iid, s in items.items() if s.status == "present"}
    for row in index.rows:
        for iid in row.items:
            if iid in items or not (iid in ranges or row.pages and mapped):
                continue
            desc = " ".join(lines[row.line].split())[:300]
            rngs = xref.carve(ranges.get(iid, []), [sp for p, sp in present.items() if not xref.is_sub_item(iid, p)])
            if not rngs:
                items[iid] = Section(item=iid, status="stub", text=lines[row.line], start=offs[row.line],
                                     end=offs[row.line + 1], target=Target(kind="in_document", description=desc))
                continue
            spans = [rendering.span(a, b) for a, b in rngs]
            text = rendering.text_of(spans)
            items[iid] = Section(item=iid, status="xref", text=text, start=spans[0].start, end=spans[-1].end,
                                 target=Target(kind="in_document", description=desc, spans=spans, text=text))


def _shared(line: int, starts: list[int], breaks: list[int]) -> bool:
    """A page that also opens an item printed with a heading, so it starts no new item where it begins."""
    end = breaks[bisect.bisect_left(breaks, line)] if line <= (breaks or [-1])[-1] else line
    k = bisect.bisect_left(starts, line)
    return k < len(starts) and starts[k] <= end


def sections_for_filing(filing, cache=None) -> FilingSections:
    """Items of an edgartools Filing as filed, each labeled with the filing's accession as its source, read
    through cache (a SectionsCache) when one is given."""
    def compute():
        return extract_sections(filing.full_text_submission(), form=filing.form, filing_date=str(filing.filing_date),
                                accession=filing.accession_no)
    sections = compute() if cache is None else cache.get_or_compute(filing.accession_no, compute)
    return replace(sections, items={k: replace(s, source=s.source or sections.accession) for k, s in sections.items.items()})
