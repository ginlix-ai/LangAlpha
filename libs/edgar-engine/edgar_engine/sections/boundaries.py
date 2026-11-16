"""Item boundaries: headings at line starts, longest stretch to the next present item, structural end markers.

An item heading is the word Item and the item number at the start of a line. For each item, in the form's
order, every heading of it after the previous item's end is a possible start, and the stretch from a start
to the first heading of the nearest later item that has one is a candidate. The longest candidate wins: a
table-of-contents entry only reaches the next contents line, so the body heading outvotes it. A stretch
then ends early at the first structural line inside it: a PART heading of a later part, SIGNATURES or the
signature paragraph, an 8-K section divider, a numbered heading outside the vocabulary, or the end of the
document.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from .vocab import PART_NUMERAL, ROMAN, FormSpec

_DASH = re.compile(r"[\u2010-\u2015\u2212]")
_LEAD = re.compile(r"^[\s|*\u2022\u00b7\u25cf\u25aa>]+")
_SPACED = re.compile(r"^((?:[A-Za-z] ){2,}[A-Za-z])(?=[\s\d.:\-]|$)")
_SPACED_WORDS = {"ITEM", "ITEMS", "PART", "SIGNATURE", "SIGNATURES"}
_WS = re.compile(r"\s+")

_ITEM = re.compile(r"(?:PART\s*(?:IV|I{1,3}|[1-4])\s*[,.:\-]?\s*)?(?P<kw>ITEMS?)\s*"
                   r"(?P<num>\d{1,2}|[IVX]{1,5}(?![A-Za-z]))(?:(?P<sp>\s?|\.)(?P<let>[A-Za-z])(?![A-Za-z]))?", re.I)
_ITEM_8K = re.compile(r"(?P<kw>ITEMS?)\s*(?P<a>\d{1,2})\s*\.\s*(?P<b>\d{2})(?!\d)", re.I)
_MORE = re.compile(r"\s*(?P<conn>,|and|&|through|thru|to|-)\s*(?:and\s+)?(?:items?\s*)?"
                   r"(?P<num>\d{1,2}(?:\.\d{2})?)(?:(?P<sp>\s?)(?P<let>[A-Za-z])(?![A-Za-z]))?(?!\d)", re.I)
_PART = re.compile(r"PART\s*(?P<p>IV|I{1,3}|[1-4]|ONE|TWO|THREE|FOUR)(?![A-Za-z0-9])", re.I)
_SIG = re.compile(r"SIGNATURES?(?:\s*\(S\))?(?:\s+PAGES?)?\s*[.:]?", re.I)
_SIG_PARA = re.compile(r"Pursuant to the requirements of\b", re.I)
_SECTION_8K = re.compile(r"SECTION\s+\d{1,2}\s*[-.:](?!\d)\s*[A-Z]", re.I)
_REG_AB = re.compile(r"ITEMS?\s*11\d\d\b", re.I)
_SUB16 = re.compile(r"16\s?\.?\s?(?P<let>[A-K])\s?[.:)\-]\s*[A-Z]")
_SUB16_TITLES = {"A": "audit committee financial expert", "B": "code of ethics", "C": "principal accountant",
                 "D": "exemptions from the listing standards", "E": "purchases of equity securities",
                 "F": "change in registrant", "G": "corporate governance", "H": "mine safety disclosure",
                 "I": "disclosure regarding foreign jurisdictions", "J": "insider trading polic", "K": "cybersecurity"}
_SUB16_LINE = re.compile(r"(?:(?P<let>[A-K])\s?[.)]\s*)?(?P<title>[A-Za-z][^.]{0,110})\.?")
_BARE16 = re.compile(r"ITEM\s*16\s*[.:]?", re.I)
_FPAGES = re.compile(r"(?:REPORTS? OF INDEPENDENT (?:REGISTERED PUBLIC ACCOUNTING FIRMS?|AUDITORS?|ACCOUNTANTS?|"
                     r"CERTIFIED PUBLIC ACCOUNTANTS?)|INDEX TO (?:THE )?(?:CONSOLIDATED )?FINANCIAL STATEMENTS\b.*|"
                     r"REPORT AND (?:CONSOLIDATED )?FINANCIAL STATEMENTS|CONSOLIDATED BALANCE SHEETS?)\s*[.:]?", re.I)
_INCLUDED_HERE = re.compile(r"included herein|herein included|included in this (?:report|item)|"
                            r"(?:on|beginning on|at) pages? F-\d|set forth (?:herein|below)|follow(?:s|ing)? (?:this|the signature)", re.I)
_PAGE_END = re.compile(r"(?:\b\d{1,3}|\b[ivx]{1,5}|F-\d{1,3})\s*$", re.I)
_CONTENTS = re.compile(r"\b(?:TABLE OF CONTENTS|CONTENTS|INDEX)\b", re.I)
_SENTENCE = re.compile(r"\w\W*\s+\S.*[.;]$")
_PART_ORDER = {"I": 1, "II": 2, "III": 3, "IV": 4}
_ANSWERED = re.compile(r"\b(?:none|not applicable|n/a|omitted|inapplicable)\b", re.I)


@dataclass
class Heading:
    line: int
    kind: str
    ids: tuple[str, ...] = ()
    part: str | None = None
    upper: bool = False
    bare: bool = False


@dataclass
class ItemSpan:
    item: str
    start: int
    end: int
    heading_line: int
    combined_with: list[str]
    status: str = "present"


def normalize(line: str) -> str:
    s = _DASH.sub("-", line.replace("\xa0", " "))
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = _WS.sub(" ", _LEAD.sub("", s)).strip()
    m = _SPACED.match(s)
    if m and m.group(1).replace(" ", "").upper() in _SPACED_WORDS:
        s = m.group(1).replace(" ", "") + s[m.end():]
    return s


_OMITTED = re.compile(r"\W*(?:omitted|not applicable)\s+(?:as|because|since|in|pursuant)\b", re.I)
_VERB = re.compile(r"\s*(?:are|is|was|were|have|has|had|will|shall|does|do|of|in|to|and|or|for)\b", re.I)


def _rest_ok(rest: str) -> bool:
    """What follows the number reads as a heading, not a sentence ("Item 7 of this report",
    "Items 2, 3 and 5 are not applicable")."""
    if not rest:
        return True
    if rest[0] in ".:-|)*~;(":
        return not _VERB.match(rest[1:]) or rest[0] == "("
    if not rest[0].isspace():
        return False
    r = rest.lstrip()
    if _VERB.match(r):
        return False
    return not r or r[0].isupper() or r[0] in "\"'([-.:|*"


def _item_id(num: str, sp: str | None, let: str | None, order: list[str]) -> str:
    """"4.A" and "16.A" name sub-items only where the form has them; elsewhere "5.A" is a sub-heading of 5."""
    n = str(ROMAN[num.upper()]) if not num.isdigit() else str(int(num))
    if not let or let in "Tt" or (sp == " " and not let.isupper()):
        return n
    if sp == "." and n + let.upper() not in order:
        return n
    return n + let.upper()


def _expand(order: list[str], ids: list[str], ranges: list[tuple[str, str]]) -> tuple[str, ...]:
    """The vocabulary items a grouped heading names, "Items 10 through 13" filled in, in form order."""
    out = set(ids)
    for a, b in ranges:
        if a in order and b in order and order.index(a) < order.index(b):
            out.update(order[order.index(a) + 1:order.index(b)])
    return tuple(i for i in order if i in out)


def _item_list(s: str, pos: int, first: str, order: list[str]) -> tuple[list[str], list[tuple[str, str]], int]:
    """The items a list continues with after its first ("Items 10, 11 and 13", "10 through 13"), the ranges it
    names, and where it ends. A dotted number (\"11.01\") is kept as printed; _expand drops it when the form has
    no such item."""
    ids, ranges, prev = [first], [], first
    while (mm := _MORE.match(s, pos)):
        nxt = mm.group("num") if "." in mm.group("num") else _item_id(mm.group("num"), mm.group("sp"), mm.group("let"), order)
        if mm.group("conn").lower() in ("through", "thru", "to", "-"):
            ranges.append((prev, nxt))
        ids.append(nxt)
        prev, pos = nxt, mm.end()
    return ids, ranges, pos


def classify(line: str, spec: FormSpec) -> Heading | None:
    """The structural role of one line, or None for body text."""
    s = normalize(line)
    if not s:
        return None
    if _REG_AB.match(s):
        return Heading(0, "other_item")
    if spec.lettered_16 and (m := _SUB16.match(s)):
        return Heading(0, "item", ("16" + m.group("let"),), upper=s.isupper(), bare=True)
    order = list(spec.order)
    m = (_ITEM_8K if spec.dotted_items else _ITEM).match(s)
    if m and not (spec.undotted_items and re.match(r"\s*\.\s*\d", s[m.end():])):
        if spec.dotted_items:
            first = f"{int(m.group('a'))}.{m.group('b')}"
        elif not m.group("num").isdigit() and not spec.roman_items:
            first = None
        else:
            first = _item_id(m.group("num"), m.group("sp"), m.group("let"), order)
        if first is not None:
            ids, ranges, pos = _item_list(s, m.end(), first, order)
            if _rest_ok(s[pos:]) and not (len(ids) > 1 and _OMITTED.match(s[pos:])):
                upper = m.group("kw").isupper()
                known = _expand(spec.headings, ids, ranges)
                if known:
                    return Heading(0, "item", known, upper=upper)
                return Heading(0, "other_item", tuple(ids), upper=upper)
    m = _PART.match(s)
    if m and len(s) <= 150 and _rest_ok(s[m.end():]):
        return Heading(0, "part", part=PART_NUMERAL[m.group("p").upper()], upper=s[:4].isupper())
    if len(s) <= 40 and _SIG.fullmatch(s):
        return Heading(0, "sig", upper=s[:9].isupper())
    if spec.section_dividers and len(s) <= 160 and _SECTION_8K.match(s):
        return Heading(0, "section")
    if spec.statement_pages and len(s) <= 90 and _FPAGES.fullmatch(s) and not _paged([s], 0, strict=True):
        return Heading(0, "fpages", upper=s.isupper())
    return None


def find_headings(lines: list[str], spec: FormSpec) -> list[Heading]:
    out = []
    for i, ln in enumerate(lines):
        if len(ln) > 3000:
            continue
        h = classify(ln, spec)
        if h is None and _SIG_PARA.match(normalize(ln)) and "duly caused" in normalize(" ".join(lines[i:i + 3]))[:500].lower():
            h = Heading(i, "sig")
        if h:
            h.line = i
            out.append(h)
    if spec.lettered_16:
        out = _titled_sub16(out, lines)
    for a, b in zip(out, out[1:]):
        if (spec.lettered_16 and a.ids == ("16",) and b.ids == ("16A",) and b.bare and b.line - a.line <= 2
                and _BARE16.fullmatch(normalize(lines[a.line]))):
            a.ids = ("16A",)
    return out


def _titled_sub16(heads: list[Heading], lines: list[str]) -> list[Heading]:
    """Under a 20-F Item 16 heading the sub-items are often printed as "A. Audit Committee Financial
    Expert" or the bare title, without the "16". Only lines whose title is the form's own 16.x title,
    between Item 16 and the next numbered item, count."""
    added = []
    for k, h in enumerate(heads):
        if h.kind != "item" or h.ids != ("16",):
            continue
        stop = next((o.line for o in heads[k + 1:] if o.kind != "item" or not o.ids[0].startswith("16")), len(lines))
        taken = {o.line for o in heads}
        for j in range(h.line + 1, stop):
            s = normalize(lines[j])
            m = _SUB16_LINE.fullmatch(s) if j not in taken and len(s) <= 120 else None
            if not m:
                continue
            title = m.group("title").lower().replace("\u2019", "'")
            let = next((x for x, t in _SUB16_TITLES.items() if title.startswith(t)), None)
            if let and (m.group("let") in (None, let)):
                added.append(Heading(j, "item", ("16" + let,), upper=s.isupper(), bare=True))
    return sorted(heads + added, key=lambda o: o.line)


def toc_lines(heads: list[Heading], lines: list[str], offs: list[int], order: list[str]) -> set[int]:
    """Lines of headings that sit in a table of contents: a dense run of headings in form order that
    carries page numbers or follows a contents title. Body runs of short items ("Item 3. None.") have
    neither, and a run ends where the order goes back (the body's PART I right after the contents)."""
    toc: set[int] = set()
    run: list[Heading] = []

    def close():
        items = [h for h in run if h.kind == "item"]
        lines_in = {h.line for h in run}
        prose = any(_SENTENCE.search(normalize(lines[j])) for j in range(run[0].line, run[-1].line + 1)
                    if j not in lines_in) if run else False
        if len(items) >= 3 and not prose:
            paged = sum(1 for h in items if _paged(lines, h.line))
            first = run[0].line
            titled = any(_CONTENTS.search(lines[j]) and len(lines[j]) < 80 for j in range(max(0, first - 4), first + 1))
            repeated = sum(1 for h in items if any(o.line > run[-1].line and set(o.ids) & set(h.ids) for o in later))
            if paged * 2 >= len(items) or (titled and repeated * 3 >= 2 * len(items)):
                toc.update(h.line for h in run)
        run.clear()

    later = [h for h in heads if h.kind == "item"]
    last = (0, -1)
    for h in heads:
        if h.kind == "part":
            rank = (_PART_ORDER[h.part], -1)
        elif h.kind == "item":
            rank = (last[0], min(order.index(i) for i in h.ids))
        else:
            rank = last
        gap = sum(len(lines[j]) + 1 for j in range(run[-1].line + 1, h.line) if not _paged(lines, j, strict=True)) if run else 0
        backwards = h.kind in ("part", "item") and rank <= last and not _paged(lines, h.line, strict=True)
        if run and (gap > 150 or backwards):
            close()
            last = (0, -1) if h.kind == "item" else last
            rank = (last[0], rank[1]) if h.kind == "item" else rank
        last = rank
        run.append(h)
    close()
    for h in later:
        if h.line not in toc and _paged(lines, h.line, strict=True) and any(o.line > h.line and set(o.ids) & set(h.ids) for o in later):
            toc.add(h.line)
    return toc


def _paged(lines: list[str], i: int, strict: bool = False) -> bool:
    """The heading line ends with a page number, or (loosely) the next line is only one."""
    s = lines[i].rstrip()
    if (re.search(r"(?:\|\s*|\.{3,}\s*|\s{2,})(?:\d{1,3}|F-\d{1,3})$", s) if strict else _PAGE_END.search(s)):
        return True
    return not strict and i + 1 < len(lines) and bool(_PAGE_END.fullmatch(lines[i + 1].strip() or "x"))


class _Doc:
    def __init__(self, lines, line_doc, offs, heads, spec: FormSpec):
        self.lines, self.line_doc, self.offs, self.heads, self.spec = lines, line_doc, offs, heads, spec
        self.toc = toc_lines(heads, lines, offs, spec.headings)
        self.markers = {h.line: h for h in heads if h.kind != "item"}

    def by_id(self) -> dict[str, list[Heading]]:
        d: dict[str, list[Heading]] = {}
        for h in self.heads:
            if h.kind == "item":
                for i in h.ids:
                    d.setdefault(i, []).append(h)
        return d

    def cut(self, item: str, s: int, e: int, last: bool = False) -> int:
        """End of the stretch s..e at the first structural line inside it. The financial-statement pages
        end only the last item, and a 10-K's last item keeps them when its own text says they are included."""
        part = self.spec.part(item)
        for ln in range(s + 1, e):
            if self.line_doc[ln] != self.line_doc[s]:
                return ln
            h = self.markers.get(ln)
            if h is None or ln in self.toc:
                continue
            if h.kind in ("sig", "section"):
                return ln
            if h.kind == "other_item":
                return self._title_block_start(s, ln)
            if h.kind == "fpages" and last and h.upper and not (
                    self.spec.included_statements and _INCLUDED_HERE.search(" ".join(self.lines[s:ln]))):
                return self._title_block_start(s, ln)
            if h.kind == "part" and not self.spec.parts_split and part and _PART_ORDER[h.part] > _PART_ORDER[part]:
                return ln
        return e

    def _title_block_start(self, s: int, ln: int) -> int:
        """An unnumbered title block printed right above a marker (a company name over the statements, a
        section title over Regulation AB items) belongs to what follows."""
        start = ln
        while start - 1 > s and ln - start < 3:
            t = normalize(self.lines[start - 1])
            letters = [ch for ch in t if ch.isalpha()]
            if not t or len(t) > 80 or _SENTENCE.search(t) or not letters or sum(ch.isupper() for ch in letters) < 0.6 * len(letters):
                break
            start -= 1
        return start

    def select(self, order: list[str], by_id: dict[str, list[Heading]], r0: int, r1: int, key=lambda i: i):
        spans: dict[str, ItemSpan] = {}
        pos = r0
        for i, x in enumerate(order):
            if key(x) in spans:
                continue
            body = [h for h in by_id.get(x, []) if max(r0, pos) <= h.line < r1 and h.line not in self.toc]
            if not body:
                continue
            best = None
            for y in order[i + 1:]:
                ys = [h for h in by_id.get(y, []) if r0 <= h.line < r1]
                for upper_only in (True, False):
                    cs = [h for h in body if h.upper or not upper_only]
                    yy = [h for h in ys if h.upper or not upper_only]
                    st = []
                    for s in cs:
                        e = next((h.line for h in yy if h.line > s.line), None)
                        if e is not None:
                            st.append((self.offs[e] - self.offs[s.line], -s.line, s, e))
                    if st:
                        best = max(st, key=lambda t: (t[0], t[1]))
                        break
                if best:
                    break
            s, e = (best[2], best[3]) if best else (body[0], r1)
            e = self.cut(x, s.line, e, last=best is None)
            group = [g for g in s.ids if g in order and order.index(g) >= i and key(g) not in spans]
            for g in group:
                spans[key(g)] = ItemSpan(key(g), s.line, e, s.line, [key(o) for o in group if o != g])
            pos = e
        return self._stack(spans) if self.spec.stacked_headings else spans

    def _stack(self, spans: dict[str, ItemSpan]) -> dict[str, ItemSpan]:
        """8-K headings stacked with nothing between them share the body that follows ("Item 2.02 ..." on
        one line and "Item 7.01 ..." on the next, then one paragraph): one span from the first heading. A
        heading line that carries its own answer ("Not applicable.") is an item of its own."""
        ordered = sorted(spans.values(), key=lambda sp: sp.start)
        for a, b in zip(ordered, ordered[1:]):
            if a.end == a.start + 1 == b.start and b.end > b.start + 1 and not _ANSWERED.search(self.lines[a.start]):
                group = sorted({a.item, b.item, *a.combined_with, *b.combined_with}, key=lambda i: spans[i].start)
                for i in group:
                    spans[i].start, spans[i].end = min(spans[g].start for g in group), b.end
                    spans[i].combined_with = [g for g in group if g != i]
        return spans


def _tenq_split(doc: _Doc, r0: int, r1: int) -> tuple[tuple[int, int], tuple[int, int] | None]:
    parts = [h for h in doc.heads if h.kind == "part" and h.line not in doc.toc and r0 <= h.line < r1]
    p1 = [h.line for h in parts if h.part == "I"]
    p2 = [h.line for h in parts if h.part == "II"]
    sigs = [h.line for h in doc.heads if h.kind == "sig" and h.line not in doc.toc]
    if not p2:
        items = [h for h in doc.heads if h.kind == "item" and h.line not in doc.toc and r0 <= h.line < r1]
        seen_later = False
        for h in items:
            if any(i != "1" for i in h.ids):
                seen_later = True
            elif seen_later and h.ids[0] == "1":
                p2 = [h.line]
                break
    if not p2:
        return (r0, r1), None
    best = None
    for a in p1:
        b = next((x for x in p2 if x > a), None)
        if b is not None and (best is None or doc.offs[b] - doc.offs[a] > doc.offs[best[1]] - doc.offs[best[0]]):
            best = (a, b)
    if best is None:
        best = (r0, p2[0])
    two = next((x for x in p2 if x >= best[1]), p2[-1])
    end = next((x for x in sigs if x > two), r1)
    return (best[0] if p1 else r0, two), (two, end)


@dataclass
class Layout:
    spans: dict[str, ItemSpan]
    toc: set[int]
    heads: list[Heading]


def find_items(lines: list[str], line_doc: list[int], offs: list[int], spec: FormSpec) -> Layout:
    if spec.titled:
        from .fortyf import fortyf
        return fortyf(lines, line_doc, offs)
    heads = find_headings(lines, spec)
    doc = _Doc(lines, line_doc, offs, heads, spec)
    return Layout(_spans(doc, lines, line_doc, spec), doc.toc, heads)


_ADDRESS = re.compile(r"(?:The )?(?:information|disclosures?|responses?) (?:required|called for|requested) (?:by|under|in|pursuant to) "
                      r"(?:this )?(?:Part\s+(?:I{1,3}|IV)\s*(?:\(|,|of this \w+)?\s*)?(?P<kw>Items?)\s*(?P<num>\d{1,2})(?P<let>[A-C])?", re.I)


def _addressed(lines: list[str], doc: _Doc, spans: dict[str, ItemSpan], order: list[str], main_end: int) -> None:
    """A sentence that names items with no headings of their own ("The information required by Items 10
    through 13 is incorporated by reference to the proxy statement") makes them one combined stub."""
    first = min((sp.start for sp in spans.values()), default=main_end)
    for ln in range(first, main_end):
        if ln in doc.toc or len(lines[ln]) > 3000:
            continue
        s = normalize(" ".join(lines[ln:ln + 2]))[:400]
        m = _ADDRESS.match(s)
        if not m:
            continue
        ids, ranges, _ = _item_list(s, m.end(), m.group("num") + (m.group("let") or ""), order)
        missing = [i for i in _expand(order, ids, ranges) if i not in spans]
        if not missing:
            continue
        end = next((x for x in range(ln + 1, main_end) if x in doc.markers or any(h.line == x for h in doc.heads if h.kind == "item")), main_end)
        for sp in spans.values():
            if sp.start < ln < sp.end:
                sp.end = ln
        for i in missing:
            spans[i] = ItemSpan(i, ln, end, ln, [o for o in missing if o != i])


def _spans(doc: _Doc, lines: list[str], line_doc: list[int], spec: FormSpec) -> dict[str, ItemSpan]:
    main_end = next((i for i, d in enumerate(line_doc) if d != 0), len(lines))
    if spec.parts_split:
        (a0, a1), two = _tenq_split(doc, 0, main_end)
        by_id = doc.by_id()
        spans = doc.select(spec.part_items("I"), by_id, a0, a1, key=lambda i: f"I-{i}")
        if "I-1" not in spans and "I-2" in spans:
            start = max([a0] + [ln + 1 for ln in doc.toc if a0 <= ln < spans["I-2"].start])
            start = next((ln for ln in range(start, spans["I-2"].start) if not _paged(lines, ln, strict=True)
                          and not (doc.markers.get(ln) and doc.markers[ln].kind == "part")), start)
            if start < spans["I-2"].start:
                spans["I-1"] = ItemSpan("I-1", start, spans["I-2"].start, -1, [], "xref")
        if two:
            spans.update(doc.select(spec.part_items("II"), by_id, two[0], two[1], key=lambda i: f"II-{i}"))
        return spans
    spans = doc.select(list(spec.order), doc.by_id(), 0, main_end)
    if spec.addressed_sentences:
        _addressed(lines, doc, spans, list(spec.order), main_end)
    return spans

