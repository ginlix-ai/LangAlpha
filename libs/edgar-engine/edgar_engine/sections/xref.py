"""Cross-reference indexes: the table some annual reports print instead of item headings, mapping each
item to the printed pages that answer it ("Item 7 ... 23-45, 67"). Rows are read in one of two modes. A
table whose rows list page ranges or several pages is literal: each item is exactly its pages. A table
whose rows each give one page in rising order is a contents table: an item runs from its page to the
page before the next item's row. Printed pages are found through the page-number chain the rendering
keeps (pages.Pagination), so an item whose pages cannot be located is left to the caller, never
given a guessed span.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .boundaries import normalize
from .pages import Pagination

TITLE = re.compile(r"cross[- ]?reference|\b(?:form\s+)?(?:10-K|20-F)\s+(?:items?|index|reference|cross)|\bsec\s+items?\b|"
                   r"\bitem\s+(?:item\s+)?caption\b|location\s+in\s+(?:this|the)\s+(?:document|report)", re.I)
_PAGE = r"(?:F-\s?)?\d{1,3}(?:\s*[-\u2013\u2014]\s*(?:F-\s?)?\d{1,3})?"
_PAGES = re.compile(rf"(?:^|(?<=[\s|*,]))(?P<pages>{_PAGE}(?:\s*[,;]\s*{_PAGE})*)\s*[,;*]?\s*$")
_ONLY_PAGES = re.compile(rf"\s*(?:pages?\s+)?{_PAGE}(?:\s*[,;]\s*{_PAGE})*\s*[,;]?\s*")
_NA = re.compile(r"\b(?:not\s+applicable|omitted)\b|\b(?:none|n/a|not\s+required)\b[\s.*]*$", re.I)
_ROW = re.compile(r"(?:PART\s+(?:IV|I{1,3})\W*)?(?P<kw>ITEMS?\s*)?(?P<ids>\d{1,2}[A-K]?(?:\s*(?:,|and|&)\s*\d{1,2}[A-K]?)*)"
                  r"(?![\d.,]\d)\.?(?P<rest>(?:\s+\S.*)?)$", re.I)
WINDOW = 200
GAP = 40


@dataclass
class IndexRow:
    items: tuple[str, ...]
    line: int
    pages: list[tuple[str, str]] = field(default_factory=list)
    na: bool = False
    listed: bool = False


@dataclass
class Index:
    rows: list[IndexRow]
    lines: tuple[int, int]
    literal: bool


def _split(pages: str) -> list[tuple[str, str]]:
    out = []
    for tok in re.split(r"\s*[,;]\s*", pages.strip(" ,;*")):
        a, _, b = (x.strip().replace(" ", "").upper() for x in re.split(r"(\s*[-\u2013\u2014]\s*)", tok, maxsplit=1)) \
            if re.search(r"\d\s*[-\u2013\u2014]\s*(?:F-)?\d", tok) else (tok.replace(" ", "").upper(), "", "")
        b = b or a
        if b.startswith("F-") or not a.startswith("F-"):
            if a.startswith("F-") and not b.startswith("F-"):
                b = "F-" + b
            out.append((a, b))
    return out


def find_index(lines: list[str], order: list[str]) -> Index | None:
    """The cross-reference index: rows naming form items with their pages, under a cross-reference title
    or written with the word Item, at least four of them in form order, most with pages or an answer."""
    blocks: list[tuple[list[IndexRow], bool]] = []
    title_at = -10 ** 9
    i, n = 0, len(lines)
    while i < n:
        s = _flat(lines[i])
        if s and len(s) <= 120 and TITLE.search(s):
            title_at = i
        m = _ROW.fullmatch(s) if s and len(s) <= 220 else None
        ids = _ids(m, order) if m else ()
        if not ids or not (m.group("kw") or 0 <= i - title_at <= WINDOW):
            i += 1
            continue
        rows, j, cur, quiet = [], i, None, 0
        while j < n and quiet <= 3:
            t = _flat(lines[j])
            if not t:
                j += 1
                continue
            mm = _ROW.fullmatch(t) if len(t) <= 220 else None
            nid = _ids(mm, order) if mm else ()
            if nid and (mm.group("kw") or 0 <= j - title_at <= WINDOW):
                cur = IndexRow(nid, j)
                rows.append(cur)
                _read(cur, mm.group("rest"))
                quiet = 0
            elif cur is not None and len(t) <= 160 and not re.search(r"[a-z]{3,}[.;:]\s+[A-Z]", t) \
                    and not re.fullmatch(r"\d{1,3}", t):
                before = (len(cur.pages), cur.na)
                _read(cur, t)
                quiet = 0 if (len(cur.pages), cur.na) != before or _ONLY_PAGES.fullmatch(t) else quiet + 1
            else:
                quiet += 1
            j += 1
        if rows:
            blocks.append((rows, 0 <= i - title_at <= WINDOW))
        i = max(j, i + 1)
    best: Index | None = None
    for k, (rows, titled) in enumerate(blocks):
        rows = list(rows)
        for nxt, _ in blocks[k + 1:]:
            if nxt[0].line - rows[-1].line > GAP or not _continues(rows, nxt, order):
                break
            rows += nxt
        idx = _accept(rows, order, titled)
        if idx and (best is None or len(idx.rows) > len(best.rows)):
            best = idx
    return best


def _continues(rows: list[IndexRow], nxt: list[IndexRow], order: list[str]) -> bool:
    """A table carried over a page break: the next block's first item with pages comes after the last one so far."""
    last = [order.index(r.items[-1]) for r in rows if r.pages]
    first = [order.index(r.items[0]) for r in nxt if r.pages]
    return bool(last and first) and first[0] > last[-1]


def _flat(line: str) -> str:
    return re.sub(r"\s*\|\s*", " ", normalize(line)).strip()


def _ids(m, order: list[str]) -> tuple[str, ...]:
    ids = tuple(x.upper() for x in re.findall(r"\d{1,2}[A-K]?", m.group("ids")))
    rest = m.group("rest").strip()
    if not ids or any(x not in order for x in ids) or not (rest or m.group("kw")):
        return ()
    if rest and not (rest[0].isalpha() or rest[0] in "[(\"\u201c*" or _ONLY_PAGES.fullmatch(rest)):
        return ()
    return ids


def _read(row: IndexRow, text: str) -> None:
    text = text.strip()
    m = _PAGES.search(text)
    if m and not re.search(r"\b(?:19|20)\d\d\s*$|\b0\d", m.group("pages")):
        got = _split(m.group("pages"))
        row.listed = row.listed or len(got) > 1 or any(a != b for a, b in got)
        row.pages += got
    elif _NA.search(text):
        row.na = True


def _accept(rows: list[IndexRow], order: list[str], titled: bool) -> Index | None:
    if len(rows) < 4:
        return None
    pos = [order.index(r.items[0]) for r in rows]
    rising = sum(1 for a, b in zip(pos, pos[1:]) if b >= a)
    answered = sum(1 for r in rows if r.pages or r.na)
    if rising < 0.7 * (len(rows) - 1) or answered < 0.6 * len(rows) or not any(r.pages for r in rows):
        return None
    if not titled:
        return None
    literal = any(r.listed for r in rows)
    return Index(rows, (rows[0].line, rows[-1].line + 1), literal)


def item_pages(index: Index) -> dict[str, list[tuple[str, str]]]:
    """Printed page ranges per item, in the order the index lists them."""
    out: dict[str, list[tuple[str, str]]] = {}
    if index.literal:
        for r in index.rows:
            for iid in r.items:
                out.setdefault(iid, [])
                out[iid] += [p for p in r.pages if p not in out[iid]]
        return out
    rows = [(r, [int(a) for a, _ in r.pages if a.isdigit()]) for r in index.rows]
    rows = [(r, ps) for r, ps in rows if ps]
    high = 0
    for k, (r, ps) in enumerate(rows):
        last = max(ps)
        if ps[0] < high:
            span = [(str(p), str(p)) for p in ps]
        else:
            nxt = next((min(q for q in qs if q > last) for o, qs in rows[k + 1:] if not set(o.items) & set(r.items)
                        and any(q > last for q in qs)), None)
            span = [(str(ps[0]), str(nxt - 1 if nxt else last))]
        high = max(high, last)
        for iid in r.items:
            out.setdefault(iid, span)
    return out


def merge(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Line ranges in document order, overlapping or touching ones joined."""
    out: list[tuple[int, int]] = []
    for a, b in sorted(ranges):
        if out and a <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], b))
        else:
            out.append((a, b))
    return out


def item_ranges(index: Index, pages: Pagination) -> dict[str, list[tuple[int, int]]]:
    """Line ranges per item in the one document whose printed pages the index cites (the main document or
    an Exhibit 13), merged in document order. An item with pages none of which can be located is left out."""
    printed = item_pages(index)
    if not printed:
        return {}
    cited = {p for v in printed.values() for a, b in v for p in (a, b)}
    docs = range(len(pages.rendering.documents))
    d = max(docs, key=lambda d: (len(cited & set(pages.footers(d))), -d), default=None)
    out: dict[str, list[tuple[int, int]]] = {}
    if d is None:
        return out
    for iid, prs in printed.items():
        rngs = [r for a, b in prs if (r := pages.page_range(d, a, b))]
        rngs = merge([r for r in rngs if not any(index.lines[0] <= ln < index.lines[1] for ln in r[:1])])
        if rngs:
            out[iid] = rngs
    return out


def carve(ranges: list[tuple[int, int]], out: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """ranges without the lines in out."""
    for k0, k1 in out:
        ranges = [x for a, b in ranges for x in ((a, min(b, k0)), (max(a, k1), b)) if x[0] < x[1]]
    return ranges


def is_sub_item(child: str, parent: str) -> bool:
    """7A of 7, 16C of 16: a lettered item printed inside its parent's section is not a boundary of it."""
    return child[:-1] == parent and child[-1:].isalpha()
