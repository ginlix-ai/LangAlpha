"""Filing sections: split 10-K, 10-Q, 8-K, 20-F and 40-F filings into their items.

```python
from edgar_engine.sections import extract_sections
sections = extract_sections(submission_text)
sections["7"].text
```
"""
from __future__ import annotations

import re

from .boundaries import find_items
from .documents import decode, form_family, header, main_documents, split
from .model import FilingSections, Section, Span, Target
from .rendering import render_documents
from .stubs import Resolver, is_stub
from .vocab import order_for
from .version import SECTIONS_VERSION

__all__ = ["FilingSections", "SECTIONS_VERSION", "Section", "Span", "Target", "extract_sections", "get_sections",
           "sections_for_filing"]


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
    family = form_family(form)
    filing_date = filing_date or head.get("filing_date")
    docs = main_documents(split(text), form)
    rendering = render_documents(docs)
    full, offs = rendering.text_and_offsets()
    layout = find_items(rendering.lines, rendering.line_doc, offs, family, filing_date)
    lines = rendering.lines
    resolver = Resolver(lines, rendering.line_doc, rendering.documents, layout.spans, family, layout.toc)

    def target_of(t):
        spans = [Span(offs[a], offs[b]) for a, b in t.lines]
        text = "\n".join(full[s.start:s.end].rstrip("\n") for s in spans) or None
        return Target(kind=t.kind, description=t.description, doc=t.doc, spans=spans, text=text)

    items = {}
    for iid, sp in layout.spans.items():
        start, end = offs[sp.start], offs[sp.end]
        text = full[start:end].rstrip("\n")
        status, target = sp.status, None
        body = "\n".join(lines[sp.start + 1:sp.end]) or lines[sp.start]
        if status == "present" and family != "8-K" and is_stub(body):
            status, target = "stub", target_of(resolver.resolve(iid, text, (sp.start, sp.end)))
        items[iid] = Section(item=iid, status=status, text=text, start=start, end=end,
                             heading=lines[sp.heading_line] if sp.heading_line >= 0 else "",
                             combined_with=sp.combined_with, target=target)
    vocab = order_for(family, filing_date) if family in ("10-K", "20-F") else []
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


def sections_for_filing(filing, cache=None) -> FilingSections:
    """Items of an edgartools Filing, read through cache (a SectionsCache) when one is given."""
    def compute():
        return extract_sections(filing.full_text_submission(), form=filing.form, filing_date=str(filing.filing_date),
                                accession=filing.accession_no)
    if cache is None:
        return compute()
    return cache.get_or_compute(filing.accession_no, compute)


def get_sections(company, form: str, *, fiscal_year: int | None = None, fiscal_period: str | None = None,
                 period_end: str | None = None, cache=None) -> FilingSections | None:
    """Items of one company's filing for any period: the latest by default, else the original filing for
    fiscal_year/fiscal_period or for the report period ending on period_end. None when there is no such filing."""
    from ..filings import get_filing, select_filing

    ref = select_filing(company, form, fiscal_year=fiscal_year, fiscal_period=fiscal_period, period_end=period_end)
    return sections_for_filing(get_filing(ref), cache) if ref else None
