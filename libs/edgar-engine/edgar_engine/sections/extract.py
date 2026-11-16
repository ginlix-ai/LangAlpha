"""The sections pipeline: a full-text submission rendered, split into items, its stubs resolved and its
cross-reference index applied."""
from __future__ import annotations

import re
from dataclasses import replace

from .boundaries import find_items
from .documents import decode, header, main_documents, split
from .model import FilingSections, Section
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

    items = {}
    for iid, sp in layout.spans.items():
        span = rendering.span(sp.start, sp.end)
        text = full[span.start:span.end].rstrip("\n")
        status, target = sp.status, None
        body = "\n".join(lines[sp.start + 1:sp.end]) or lines[sp.start]
        if status == "present" and spec.stubs and is_stub(body):
            status, target = "stub", resolver.resolve(iid, text, (sp.start, sp.end)).to_target(rendering)
        items[iid] = Section(item=iid, status=status, text=text, start=span.start, end=span.end,
                             heading=lines[sp.heading_line] if sp.heading_line >= 0 else "",
                             combined_with=sp.combined_with, target=target)
    if spec.xref_index:
        xref.apply(items, rendering, pages, resolver, layout, list(spec.order))
    return FilingSections(accession=accession or head.get("accession"), form=form, filing_date=filing_date,
                          items=items, text=full, engine_version=SECTIONS_VERSION, documents=rendering.documents)


def sections_for_filing(filing, cache=None) -> FilingSections:
    """Items of an edgartools Filing as filed, each labeled with the filing's accession as its source, read
    through cache (a SectionsCache) when one is given."""
    def compute():
        return extract_sections(filing.full_text_submission(), form=filing.form, filing_date=str(filing.filing_date),
                                accession=filing.accession_no)
    sections = compute() if cache is None else cache.get_or_compute(filing.accession_no, compute)
    return replace(sections, items={k: replace(s, source=s.source or sections.accession) for k, s in sections.items.items()})
