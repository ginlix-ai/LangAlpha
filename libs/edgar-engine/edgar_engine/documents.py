"""Every readable document of a full-text submission, each rendered on its own, and a filing's exhibit list."""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from .sections.documents import main_documents, require_complete, split
from .sections.model import FilingSections
from .sections.rendering import render_documents
from .sections.vocab import spec_for
from .submission import parse_submission

if TYPE_CHECKING:
    from .cache import DocumentsCache, SectionsCache
    from .filings import FilingRef

__all__ = ["SubmissionDocument", "attached_documents", "exhibit_index", "read_with_documents", "readable",
           "submission_documents"]

_UNREADABLE_TYPES = ("EX-101", "EX-104", "GRAPHIC", "ZIP", "XML", "JSON", "CSS", "JS", "PDF", "EXCEL", "XLSX")
_UNREADABLE_FILES = re.compile(r"R\d+\.html?|.*\.(pdf|jpe?g|gif|png|zip|xlsx?)", re.I)
_EXHIBIT_LINE = re.compile(r"\|?\s*(?:Exhibit\s+)?(\d{1,3}(?:\.\d{1,3})?)\s*\|?\s*(\S.*)", re.I)


@dataclass(frozen=True)
class SubmissionDocument:
    seq: int
    type: str
    filename: str
    description: str
    text: str


def readable(doc_type: str, filename: str) -> bool:
    """A document a reader wants as text: not XBRL, a PDF, a graphic, an archive or a spreadsheet, by its type or its
    file name (an exhibit typed EX-99.1 can be a PDF, whose payload split() drops), and not one of the R pages EDGAR's
    financial report viewer renders from XBRL."""
    return not doc_type.upper().startswith(_UNREADABLE_TYPES) and not _UNREADABLE_FILES.fullmatch(filename)


def submission_documents(submission: str, keep: Callable[[str, str], bool] | None = readable) -> list[SubmissionDocument]:
    """The non-binary documents of a full-text submission in sequence order, each rendered with the engine's
    lossless rendering. keep(type, filename) is asked before a document is rendered, so XBRL and viewer files are
    skipped without paying for them; pass None to keep every one. A truncated submission is refused, so a partial
    set is never kept."""
    require_complete(submission)
    docs = sorted(split(submission), key=lambda d: d.seq)
    return [SubmissionDocument(d.seq, d.type, d.filename, d.description, "\n".join(render_documents([d]).lines))
            for d in docs if not d.is_binary and (keep is None or keep(d.type, d.filename))]


def attached_documents(submission: str, form: str) -> list[SubmissionDocument]:
    """The readable documents a filing carries beside the ones its sections are read from (extract_sections), such
    as an 8-K's press release and its other exhibits."""
    read = {d.seq for d in main_documents(split(submission), form)}
    return [d for d in submission_documents(submission) if d.seq not in read]


def read_with_documents(ref: FilingRef, cache: SectionsCache | None = None,
                        documents: DocumentsCache | None = None) -> tuple[FilingSections, list[SubmissionDocument]]:
    """A filing's sections as filed (amendments.read) and the documents attached to it (attached_documents), both
    from one download of its submission when the documents are not cached, each through its cache when given."""
    from .amendments import amendment_scope, read
    from .filings import get_filing
    from .sections.extract import extract_sections

    attached = documents.get(ref.accession) if documents is not None else None
    if attached is not None:
        return read(ref, cache), attached
    sections, attached = parse_submission(get_filing(ref), lambda raw: (
        extract_sections(raw, form=ref.form, filing_date=ref.filing_date, accession=ref.accession),
        attached_documents(raw, ref.form)))
    if cache is not None:
        cache.put(sections)
    if documents is not None:
        documents.put(ref.accession, attached)
    return replace(sections, amends=ref.amends, scope=amendment_scope(sections)
                   if sections.filing_status == "amendment" else sections.scope), attached


def exhibit_index(sections: FilingSections) -> dict[str, str]:
    """Exhibit number to its description, from the items that list the filing's exhibits (an 8-K's Item 9.01, a
    10-K's Item 15). A number listed twice keeps its first description."""
    spec = spec_for(sections.form, sections.filing_date)
    found: dict[str, str] = {}
    for s in sections.items.values():
        if not spec.lists_exhibits(s.item, s.heading):
            continue
        for line in s.text.splitlines():
            m = _EXHIBIT_LINE.fullmatch(line.strip())
            desc = " ".join(m.group(2).replace("|", " ").split()) if m else ""
            if re.search(r"[A-Za-z]", desc):
                found.setdefault(m.group(1), desc)
    return found
