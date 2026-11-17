"""Cross-reference stubs: items that only say where their content is, and the in-document targets they name.

A stub is a short item whose own text is a pointer ("incorporated by reference to the proxy statement",
"see Note 7", "is set forth under 'Market Risk' on pages 20 to 23 of the Annual Report"). Its target is
resolved only inside this filing (the main document and any Exhibit 13): another item, a note to the
statements, a captioned section, the exhibit index, the financial-statement pages, or a printed page
range. When the pointer names nothing that can be found, the stub keeps its kind and description and no
span, never a guessed one.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .boundaries import ItemSpan
from .model import Target, TargetKind
from .pages import Pagination
from .rendering import Rendering
from .text import EXHIBIT_INDEX, STATEMENTS_OPENER, normalize
from .vocab import FormSpec

_POINTER = re.compile(
    r"incorporated (?:herein |in this \w+ )?by reference|(?:is|are|will be|has been|have been|appears?|is being)\s+"
    r"(?:included|contained|set forth|presented|incorporated|provided|disclosed|found|filed|furnished|listed|described|reported)"
    r"|\bsee\b|refer(?:ence is made)? to|\bset forth (?:in|under|on)|\bcontained (?:in|under)|\bappears? (?:on|under|in)", re.I)
_ANSWER = re.compile(r"^(?:none|not applicable|n/?a|omitted|reserved|\[reserved\])\W*$", re.I)
_OWN_ANSWER = re.compile(r"\bno material changes?\b|\bthere (?:have|has) been no\b|\belected to (?:provide|furnish|prepare)\b|"
                         r"\bnot applicable\b|\bwe lease\b|\b(?:is|are) not (?:a )?party\b", re.I)
_PROXY = re.compile(r"proxy statement|information statement|proxy materials", re.I)
_ANNUAL = re.compile(r"annual report to (?:share|stock)holders|exhibit 13|annual report\b(?! on form)", re.I)
_OTHER_FILING = re.compile(r"(?<!this )(?<!our )(?:(?:annual|quarterly) report on form 10-[KQ]|form 10-K for the (?:fiscal )?year)"
                           r"|previously filed|registration statement", re.I)
_EXHIBIT = re.compile(r"\bexhibits? (?:99|13|1\d|\d)[\d.]*\b|filed (?:herewith )?as (?:an )?exhibit|attached (?:hereto )?as exhibit", re.I)
_ITEM_REF = re.compile(r"\b(?:Part\s+(?P<part>IV|I{1,3})\s*,?\s*)?Items?\s+(?P<num>\d{1,2}(?:\.\d{2})?[A-K]?)\b", re.I)
_TRAILING_PART = re.compile(r"\s*,?\s*(?:of|in)\s+(?:this\s+)?Part\s+(IV|I{1,3})\b", re.I)
_NOTE_REF = re.compile(r"\b(?:Notes?|Footnotes?)\s+(?P<n>\d{1,2})\b", re.I)
_SUB_REF = re.compile(r"\bItems?\s+\d{1,2}[A-K]?\s*\.\s*[A-Z]\b|\bItems?\s+\d{1,2}\([a-z]\)", re.I)
_CAPTION = re.compile(r"[\"\u201c\u201d](?P<c>[^\"\u201c\u201d]{3,120})[\"\u201c\u201d]|(?:caption|captioned|heading|headings|section|entitled)\s+(?P<d>[A-Z][\w,&'\u2019\- ]{3,100}?)(?=\s+(?:on|in|of|at|beginning)\b|[.;,]|$)")
_PAGE_ONE = r"(?:F-\s?\d{1,3}|\d{1,3})(?!\d)"
_PAGE_TO = r"\s*(?:-|\u2013|\u2014|to|through)\s*"
_PAGE_SPAN = re.compile(rf"(?P<a>{_PAGE_ONE})(?:{_PAGE_TO}(?P<b>{_PAGE_ONE}))?", re.I)
_ANY_SPAN = rf"{_PAGE_ONE}(?:{_PAGE_TO}{_PAGE_ONE})?"
_PAGES = re.compile(rf"\bpages?\s+(?P<list>{_ANY_SPAN}(?:\s*(?:,|and|&)\s*{_ANY_SPAN})*)", re.I)
_EXHIBIT_INDEX_REF = re.compile(r"exhibit index|index to exhibits|exhibits index", re.I)
_FS_REF = re.compile(r"page F-\s?\d|pages F-|index to (?:the )?(?:consolidated )?financial statements|financial statements (?:and (?:the )?notes thereto )?(?:beginning|commencing|which begin|that begin|appear|included|filed)", re.I)
_SENT = re.compile(r"[.;:]\s*$")


@dataclass
class TargetSpan:
    kind: TargetKind
    description: str
    lines: list[tuple[int, int]] = field(default_factory=list)

    def to_target(self, rendering: Rendering) -> Target:
        spans = [rendering.span(a, b) for a, b in self.lines]
        return Target(kind=self.kind, description=self.description, spans=spans,
                      text=rendering.text_of(spans) or None)


def _words(s: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", normalize(s).lower()))


def _heading_like(line: str) -> bool:
    s = normalize(line)
    if not s or len(s) > 120 or " | " in s or _SENT.search(s) and len(s) > 60:
        return False
    words = [w for w in re.findall(r"[A-Za-z][\w'\u2019]*", s)]
    if not words:
        return False
    caps = sum(1 for w in words if w[0].isupper() or w.lower() in ("and", "of", "the", "to", "in", "for", "on", "or", "a", "an"))
    return caps >= 0.8 * len(words)


def is_stub(body: str) -> bool:
    """The item's own text only points elsewhere: short, holds a pointer, and little else."""
    text = " ".join(body.split())
    if not text or len(text) > 2500 or _ANSWER.match(text):
        return False
    sentences = [x for x in re.split(r"(?<=[.;])\s+", text) if x.strip()]
    pointer = [x for x in sentences if _POINTER.search(x)]
    if not pointer:
        return False
    if any(_OWN_ANSWER.search(x) for x in sentences):
        return False
    other = sum(sum(ch.isalnum() for ch in x) for x in sentences if not _POINTER.search(x) and not _ANSWER.match(x))
    return other <= 80


def _note_style(line: str) -> str:
    """How a note heading is numbered: "Note 7.", "(7)" or "7.". A heading numbered another way and lower
    than the note is one of its own subsections ("1. Significant Estimates" inside Note 7)."""
    return "worded" if re.match(r"NOTE", line, re.I) else "paren" if line.startswith("(") else "plain"


class Resolver:
    def __init__(self, rendering: Rendering, pages: Pagination, spans: dict[str, ItemSpan], spec: FormSpec,
                 toc: set[int]):
        self.rendering, self.pages, self.spans, self.spec, self.toc = rendering, pages, spans, spec, toc
        self.lines = rendering.lines
        self.ex13 = [i for i, d in enumerate(rendering.documents) if d.type.upper().startswith("EX-13")]

    def _docs_for(self, pointer: str) -> list[int]:
        if self.ex13 and _ANNUAL.search(pointer):
            return self.ex13 + [0]
        return [0] + self.ex13

    def _outside(self, ln: int, own: tuple[int, int]) -> bool:
        return not (own[0] <= ln < own[1]) and ln not in self.toc

    def resolve(self, iid: str, pointer: str, own: tuple[int, int], index_row: bool = False) -> TargetSpan:
        """index_row: the pointer is a contents or cross-reference row, which names its own item."""
        kind = ("proxy" if _PROXY.search(pointer) else "annual_report" if _ANNUAL.search(pointer) and not self.ex13
                else "exhibit" if _EXHIBIT.search(pointer) and not _ANNUAL.search(pointer)
                else "other_filing" if _OTHER_FILING.search(pointer) else "in_document")
        t = TargetSpan(kind, " ".join(pointer.split())[:300])
        if kind in ("proxy", "other_filing", "annual_report"):
            return t
        finders = (self.located,) if index_row else (
            self._statements, self._note, self._exhibit_index, self._item, self.located)
        if kind == "exhibit":
            finders = (self._exhibit_index,)
        for finder in finders:
            rng = finder(iid, pointer, own)
            if rng:
                t.kind, t.lines = "in_document", rng
                return t
        if kind == "in_document" and not re.search(r"\bpages?\b|\bnote\b|\bitem\b|caption|heading|exhibit 13|annual report", pointer, re.I):
            t.kind = "none_stated"
        return t

    def _item(self, iid, pointer, own):
        """Only a pointer to a whole item: one that also names a note, a caption or a sub-item points inside
        the item, and a statements item pointing at the exhibits item means the statements it lists."""
        if _NOTE_REF.search(pointer) or _CAPTION.search(pointer) or _SUB_REF.search(pointer):
            return None
        for m in _ITEM_REF.finditer(pointer):
            if re.search(r"(?:this|such)\s+(?:Part\s+\w+\s*,?\s*)?$", pointer[max(0, m.start() - 20):m.start()], re.I):
                continue
            num = m.group("num").upper()
            key = num
            if self.spec.parts_split:
                after = _TRAILING_PART.match(pointer, m.end())
                part = m.group("part") or (after and after.group(1))
                parts = ("II", "I") if iid.startswith("II-") else ("I", "II")
                key = f"{part.upper()}-{num}" if part else next(
                    (k for k in (f"{p}-{num}" for p in parts) if k in self.spans and k != iid), None)
            sp = self.spans.get(key)
            if not sp or key == iid or sp.start == own[0]:
                continue
            if iid in self.spec.statement_items and self.spec.lists_exhibits(
                    key, self.lines[sp.heading_line] if sp.heading_line >= 0 else ""):
                return None
            return [(sp.start, sp.end)]
        return None

    def _note(self, iid, pointer, own):
        m = _NOTE_REF.search(pointer)
        if not m:
            return None
        n = m.group("n")
        head = re.compile(rf"(?:NOTE\s*{n}\b|\(\s*{n}\s*\)\s*[A-Z]|{n}\s*[.:\-\u2013\u2014)]\s*[A-Z])", re.I)
        any_note = re.compile(r"(?:NOTE\s*(\d{1,2}|[A-Z])\b|\(\s*\d{1,2}\s*\)\s*[A-Z]|(\d{1,2})\s*[.:\-\u2013\u2014)]\s*[A-Z][A-Za-z ,&'\u2019\-]{3,90}$)", re.I)
        for d in self._docs_for(pointer):
            a, b = self.rendering.doc_range(d)
            strong = [ln for ln in range(a, b) if self._outside(ln, own) and re.match(rf"(?:NOTE\s*{n}\s*[.:\-\u2013\u2014)]?|[Nn]ote\s*{n}\s*[.:\-\u2013\u2014)])\s*[A-Z]", normalize(self.lines[ln]))]
            hits = strong or [ln for ln in range(a, b) if self._outside(ln, own) and len(self.lines[ln]) <= 150
                              and head.match(normalize(self.lines[ln])) and _heading_like(re.sub(r"^\W*\d+\W*", "", normalize(self.lines[ln])))]
            if not hits:
                continue
            s = hits[0]
            cap = next((sp.end for sp in self.spans.values() if sp.start <= s < sp.end), b)
            style = _note_style(normalize(self.lines[s]))

            def next_note(line: str) -> bool:
                if not any_note.match(line) or head.match(line):
                    return False
                num = re.match(r"\W*(?:NOTE\s*)?(\d{1,2})\b", line, re.I)
                return _note_style(line) == style or num is None or int(num.group(1)) > int(n)
            e = next((ln for ln in range(s + 1, cap) if len(self.lines[ln]) <= 150 and next_note(normalize(self.lines[ln]))), cap)
            return [(s, e)]
        return None

    def _exhibit_index(self, iid, pointer, own):
        if not _EXHIBIT_INDEX_REF.search(pointer):
            return None
        for d in [0] + self.ex13:
            a, b = self.rendering.doc_range(d)
            for ln in range(a, b):
                if self._outside(ln, own) and EXHIBIT_INDEX.fullmatch(normalize(self.lines[ln])):
                    return [(ln, b)]
        return None

    def _statements(self, iid, pointer, own):
        """The financial-statement pages: from their opener to the last F-page footer, else to the exhibit
        index, SIGNATURES or the end of the document. A pointer that names a note and no pages means the note."""
        if iid not in self.spec.statement_items or (_PAGES.search(pointer) and not _FS_REF.search(pointer)) or (
                _NOTE_REF.search(pointer) and not _PAGES.search(pointer)):
            return None
        back = max(sp.end for sp in self.spans.values())
        docs = self.ex13 if self.ex13 and _ANNUAL.search(pointer) else [0] + self.ex13
        for d in docs:
            a, b = self.rendering.doc_range(d)
            for ln in range(max(a, back) if d == 0 else a, b):
                s = normalize(self.lines[ln])
                if self._outside(ln, own) and len(s) <= 120 and STATEMENTS_OPENER.fullmatch(s) and not re.search(r"\d\s*$", s):
                    end = next((x for x in range(ln + 1, b) if EXHIBIT_INDEX.fullmatch(normalize(self.lines[x]))
                                or re.fullmatch(r"SIGNATURES?", normalize(self.lines[x]))), b)
                    fpages = [x for p, x in self.pages.footers(d).items() if p.startswith("F") and ln < x < end]
                    return [(ln, max(fpages) + 1 if fpages else end)]
        return None

    def located(self, iid, pointer, own):
        """Captioned sections and printed page ranges. A caption found inside the cited pages starts there;
        a caption with no pages runs to the next heading of the same style."""
        caps = [(m.group("c") or m.group("d") or "").strip(" .,;") for m in _CAPTION.finditer(pointer)]
        caps = [c for c in caps if len(_words(c).split()) >= 2 and not re.match(r"(?i)item\s+\d", c)]
        pages = [(s["a"].replace(" ", "").upper(), (s["b"] or s["a"]).replace(" ", "").upper())
                 for m in _PAGES.finditer(pointer) for s in _PAGE_SPAN.finditer(m["list"])]
        pages = [(a, f"F-{b}" if a.startswith("F-") and b.isdigit() else b) for a, b in pages]
        for d in self._docs_for(pointer):
            out = []
            confirmed = not (caps and pages)
            missing = False
            for lo, hi in pages:
                rng = self.pages.page_range(d, lo, hi)
                if rng:
                    out.append(rng)
                else:
                    missing = True
            for cap in caps:
                w = _words(cap)
                a, b = self.rendering.doc_range(d)
                hits = [ln for ln in range(a, b) if self._outside(ln, own) and _heading_like(self.lines[ln])
                        and _words(self.lines[ln]).startswith(w) and len(_words(self.lines[ln])) <= len(w) + 40]
                inside = [ln for ln in hits for r0, r1 in out if r0 <= ln < r1]
                if inside:
                    out = [(max(r0, inside[0]) if r0 <= inside[0] < r1 else r0, r1) for r0, r1 in out]
                    confirmed = True
                    continue
                if not hits or pages:
                    continue
                hit = hits[0]
                upper = normalize(self.lines[hit]).isupper()
                end = next((ln for ln in range(hit + 1, b) if _heading_like(self.lines[ln]) and normalize(self.lines[ln]).isupper() == upper
                            and sum(ch.isalpha() for ch in self.lines[ln]) >= 8 and not normalize(self.lines[ln]).startswith("(")
                            and any(len(x) >= 80 and " | " not in x for x in self.lines[ln + 1:ln + 3])), b)
                out.append((hit, end))
            if out and confirmed and not missing:
                return sorted(set(out))
        return None
