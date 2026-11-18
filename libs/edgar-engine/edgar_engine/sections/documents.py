"""Split a full-text submission into its documents and pick the ones the sections engine reads."""
from __future__ import annotations

import re
from dataclasses import dataclass

from .vocab import is_amendment, spec_for

_DOC = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.S | re.I)
_TEXT = re.compile(r"<TEXT>(.*)</TEXT>", re.S | re.I)
_TAGS = {k: re.compile(rf"^\s*<{k}>([^\n<]*)", re.M | re.I) for k in ("TYPE", "SEQUENCE", "FILENAME", "DESCRIPTION")}
_PDF = re.compile(r"<PDF>.*?</PDF>", re.S | re.I)
_UUENCODED = re.compile(r"\s*begin \d{3} ")
_OPEN = re.compile(r"<DOCUMENT>", re.I)
_CLOSE = re.compile(r"</DOCUMENT>", re.I)
_SUBMISSION_OPEN = re.compile(r"<SEC-DOCUMENT>", re.I)
_SUBMISSION_CLOSE = re.compile(r"</SEC-DOCUMENT>", re.I)
_HEADER_FIELD = {
    "form": re.compile(r"^\s*CONFORMED SUBMISSION TYPE:\s*(\S+)", re.M),
    "filing_date": re.compile(r"^\s*FILED AS OF DATE:\s*(\d{8})", re.M),
    "accession": re.compile(r"^\s*ACCESSION NUMBER:\s*([\d-]+)", re.M),
    "period": re.compile(r"^\s*CONFORMED PERIOD OF REPORT:\s*(\d{8})", re.M),
}


@dataclass
class Document:
    seq: int
    type: str
    filename: str
    description: str
    body: str

    @property
    def is_binary(self) -> bool:
        head = self.body[:200]
        return bool(re.match(r"\s*begin \d{3} ", head)) or head.lstrip()[:5] == "%PDF-"


def decode(raw: bytes | str) -> str:
    if isinstance(raw, str):
        return raw
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def require_complete(submission: str) -> None:
    """Refuse a submission with no document, a <DOCUMENT> left open, or an <SEC-DOCUMENT> never closed: a partial
    download, or an HTTP cache that kept one, would otherwise parse into documents cut short or missing with no
    sign of it."""
    opened = len(_OPEN.findall(submission))
    if not opened or opened != len(_CLOSE.findall(submission)) or (
            _SUBMISSION_OPEN.search(submission[:1000]) and not _SUBMISSION_CLOSE.search(submission[-1000:])):
        raise ValueError("the submission is truncated or holds no document")


def header(submission: str) -> dict:
    """Form, filing date, accession and period from the SEC header (dates as YYYY-MM-DD)."""
    head = submission[:20000]
    out = {}
    for key, rx in _HEADER_FIELD.items():
        m = rx.search(head)
        if not m:
            continue
        v = m.group(1)
        out[key] = f"{v[:4]}-{v[4:6]}-{v[6:]}" if key in ("filing_date", "period") else v
    return out


def split(submission: str) -> list[Document]:
    """The submission's documents. A uuencoded one (a graphic, a ZIP) keeps only the head that marks it binary,
    so a large one is never copied."""
    docs = []
    for i, m in enumerate(_DOC.finditer(submission)):
        start, end = m.span(1)
        head = submission[start:min(end, start + 2000)]

        def tag(k):
            t = _TAGS[k].search(head)
            return t.group(1).strip() if t else ""

        body = ""
        if t := _TEXT.search(submission, start, end):
            a, b = t.span(1)
            body = submission[a:min(b, a + 200)] if _UUENCODED.match(submission, a) else _PDF.sub("", submission[a:b])
        seq = tag("SEQUENCE")
        docs.append(Document(int(seq) if seq.isdigit() else i + 1, tag("TYPE"), tag("FILENAME"), tag("DESCRIPTION"), body))
    return docs


def main_documents(docs: list[Document], form: str) -> list[Document]:
    """The main document and, for annual reports, the Exhibit 13 documents items may point into; for a 40-F
    amendment, the EX-99 exhibits that refile its principal documents.

    The main document is the first one whose declared type is the form itself, else the first of the form's
    family: some submissions carry a second copy or a wrapper later on, an amendment can carry a copy typed
    as the original, and exhibits never qualify. A submission whose types do not name the form falls back to
    its first text document.
    """
    if not docs:
        return []
    spec = spec_for(form)
    ordered = sorted(docs, key=lambda d: d.seq)
    main = next((d for d in ordered if d.type.upper() == form.upper() and not d.is_binary), None) or next(
        (d for d in ordered if d.type.upper().startswith(spec.family) and not d.is_binary),
        next((d for d in ordered if not d.is_binary), ordered[0]))
    extra = spec.exhibit_documents + (spec.amendment_documents if is_amendment(form) else ())
    return [main] + [d for d in ordered if d is not main and d.type.upper().startswith(extra) and not d.is_binary]
