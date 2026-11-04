"""Split a full-text submission into its documents and pick the ones the sections engine reads."""
from __future__ import annotations

import re
from dataclasses import dataclass

_DOC = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.S | re.I)
_TEXT = re.compile(r"<TEXT>(.*)</TEXT>", re.S | re.I)
_TAGS = {k: re.compile(rf"^\s*<{k}>([^\n<]*)", re.M | re.I) for k in ("TYPE", "SEQUENCE", "FILENAME", "DESCRIPTION")}
_PDF = re.compile(r"<PDF>.*?</PDF>", re.S | re.I)
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
    docs = []
    for i, m in enumerate(_DOC.finditer(submission)):
        block = m.group(1)
        head = block[:2000]

        def tag(k):
            t = _TAGS[k].search(head)
            return t.group(1).strip() if t else ""

        body = _TEXT.search(block)
        seq = tag("SEQUENCE")
        docs.append(Document(int(seq) if seq.isdigit() else i + 1, tag("TYPE"), tag("FILENAME"),
                             tag("DESCRIPTION"), _PDF.sub("", body.group(1) if body else "")))
    return docs


def form_family(form: str) -> str:
    f = form.upper().replace("/A", "")
    if f.startswith("10-K"):
        return "10-K"
    if f.startswith("10-Q"):
        return "10-Q"
    for fam in ("8-K", "20-F", "40-F"):
        if f.startswith(fam):
            return fam
    raise ValueError(f"unsupported form {form!r}")


def main_documents(docs: list[Document], form: str) -> list[Document]:
    """The main document and, for annual reports, the Exhibit 13 documents items may point into.

    The main document is the first one whose declared type is the form itself: some submissions carry
    a second copy or a wrapper later on, and exhibits never qualify. A submission whose types do not
    name the form falls back to its first document.
    """
    if not docs:
        return []
    fam = form_family(form)
    ordered = sorted(docs, key=lambda d: d.seq)
    main = next((d for d in ordered if d.type.upper().startswith(fam) and not d.is_binary), ordered[0])
    keep = [main]
    if fam == "10-K":
        keep += [d for d in ordered if d is not main and d.type.upper().startswith("EX-13") and not d.is_binary]
    return sorted(keep, key=lambda d: d.seq)
