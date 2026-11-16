"""Amendments: which original an amendment amends, what it restates, and the original as amended.

The filing list links most amendments to their original by period of report (filings.link_amendments).
Every amendment read is then checked against its own text, which wins when it places the amendment: the
cover page names the period it reports on, and the explanatory note usually names the date the original
was filed. That corrects a period of report typed wrong on either filing, and places an amendment the
list could not (amends=None). An amendment read for an original is kept only when its text, or failing
that the list, names that original; one the list links elsewhere is not read for it.

Scope. An amendment restates the items it prints (Rule 12b-15 asks for the complete text of each item
amended). It is "complete" when it prints an item from every part of the form, leaving out the exhibit
list: a full refiling. Otherwise it is "partial" (Part III only, restated statements), "exhibits" when
the only item it prints is its exhibit list, and "cover" when it prints no item at all (a cover page,
an explanatory note and signatures, or exhibits with no item around them).

Merge. The as-amended view starts from the original and applies each amendment in filing order. A
complete amendment replaces the item set. A partial one replaces the items it prints and keeps the
rest, except the exhibit list: a partial amendment's exhibit list names only the exhibits filed with
it, so the report's own list stays. An exhibits or cover amendment changes no item. Every item names
its source accession.
"""
from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from .filings import FilingRef, base_form, get_filing, list_filings, nth_amendment, pick_ref, select_filing
from .sections import sections_for_filing
from .sections.documents import form_family
from .sections.model import FilingSections, Scope, Section, Span, View
from .sections.vocab import order_for, part_of

_MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov",
                                       "dec"], 1)}
_MONTH = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?"
_DATE = (rf"(?:(?P<m1>{_MONTH})\s*(?P<d1>\d{{1,2}})\s*,?\s*(?P<y1>(?:19|20)\d\d)"
         rf"|(?P<d2>\d{{1,2}})\s+(?P<m2>{_MONTH})\s*,?\s*(?P<y2>(?:19|20)\d\d))")
_COVER_PERIOD = re.compile(
    r"\bfor\s+the\s+(?:fiscal\s+|transition\s+)?(?:year|quarter(?:ly\s+period)?|period|(?:three|six|nine|twelve)\s+months)"
    rf"\s+ended\s*:?\s*{_DATE}", re.I)
_FILED_ON = re.compile(rf"\bfiled\b[^\n;]{{0,160}}?\bon\s+{_DATE}", re.I)
_ORIGINAL = re.compile(r"\boriginal|\bannual\s+report|\bquarterly\s+report|\bform\s+(?:10-K|10-Q|20-F|40-F)(?![\w-]*/A)", re.I)
_REPORT = re.compile(r"\boriginal|\b(?:annual|quarterly|current)\s+report|\bform\s+[\w-]+(?:/A)?", re.I)
_SENTENCE_END = re.compile(r"[.!?]\s+(?=[A-Z(\u201c\"])")
_EXHIBIT_HEADING = re.compile(r"\bexhibits?\b", re.I)
_EXHIBIT_ITEMS = {"10-K": {"15"}, "10-Q": {"II-6"}, "20-F": {"19"}}
_ANNUAL_DOCUMENTS = {"AIF", "AFS", "MDA"}
_PARTS = {"10-K": {"I", "II", "III"}, "10-Q": {"I", "II"}, "20-F": {"I", "II", "III"}, "40-F": {"documents", "form"}}
COVER_HEAD = 6_000
NOTE_HEAD = 40_000


def _date(m: re.Match) -> str | None:
    month, day, year = (m["m1"], m["d1"], m["y1"]) if m["m1"] else (m["m2"], m["d2"], m["y2"])
    try:
        return f"{int(year):04d}-{_MONTHS[month[:3].lower()]:02d}-{int(day):02d}"
    except (KeyError, ValueError):
        return None


def _order(filing) -> tuple[str, str]:
    return filing.filing_date or "", filing.accession or ""


def cover_period(text: str) -> str | None:
    """The period an amendment's cover page says it reports on ("for the fiscal year ended ..."), as YYYY-MM-DD."""
    m = _COVER_PERIOD.search(text[:COVER_HEAD])
    return _date(m) if m else None


def _unwrap(text: str) -> str:
    """Hard-wrapped prose joined into its paragraph: a line break that cuts a sentence, after a line long
    enough to be wrapped or before a lowercase word, read as a space."""
    lines = text.split("\n")
    for k in range(len(lines) - 1):
        s, nxt = lines[k].rstrip(), lines[k + 1].lstrip()
        if s and nxt and s[-1] not in ".!?:;" and (len(s) >= 40 or nxt[0].islower()):
            lines[k] += " "
    return re.sub(r" \n", " ", "\n".join(lines))


def original_filed_on(text: str) -> list[str]:
    """Dates the explanatory note gives for filing the report being amended, as YYYY-MM-DD.

    A date counts only when the last report its sentence names before the date is the original or the report, so
    neither "our Form 8-K filed on ..." nor "this Form 10-K/A discusses our Form 8-K filed on ..." does.
    """
    head, dates = re.sub(r"[\u2010-\u2015\u2212]", "-", _unwrap(text[:NOTE_HEAD])), []
    for m in _FILED_ON.finditer(head):
        before = head[head.rfind("\n", 0, m.start()) + 1:m.start()]
        ends = list(_SENTENCE_END.finditer(before))
        sentence = before[ends[-1].end():] if ends else before
        mentions = _REPORT.findall(sentence + head[m.start():m.start("m1") if m["m1"] else m.start("d2")])
        if mentions and _ORIGINAL.match(mentions[-1]) and (d := _date(m)) and d not in dates:
            dates.append(d)
    return dates


def resolve_amends(amendment: FilingSections, refs: list[FilingRef]) -> str | None:
    """The original an amendment amends, read from its own text: the original with the period its cover states,
    else the one filed on the first date its explanatory note gives for the original. None when neither places it."""
    originals = [r for r in refs if r.filing_status == "original" and base_form(r.form) == base_form(amendment.form)
                 and r.filing_date <= (amendment.filing_date or "9999")]
    period = cover_period(amendment.text)
    by_period = [r for r in originals if period and r.report_date == period]
    if by_period:
        return max(by_period, key=_order).accession
    for day in original_filed_on(amendment.text):
        by_date = [r for r in originals if r.filing_date == day]
        if by_date:
            return by_date[0].accession if len(by_date) == 1 else None
    return None


def is_exhibit_item(family: str, section: Section) -> bool:
    """The item listing the report's exhibits: Item 15 of a 10-K (Item 14 before 2003), Part II Item 6 of a 10-Q,
    Item 19 of a 20-F. Its heading names exhibits; a section with no heading goes by its number."""
    return section.item in _EXHIBIT_ITEMS.get(family, ()) or bool(_EXHIBIT_HEADING.search(section.heading))


def _part(family: str, item: str) -> str | None:
    if family == "40-F":
        return "documents" if item in _ANNUAL_DOCUMENTS else "form"
    return part_of(family, item)


def amendment_scope(sections: FilingSections) -> Scope:
    """What an amendment restates: "complete", "partial", "exhibits" or "cover" (see the module docstring)."""
    if not sections.items:
        return "cover"
    family = form_family(sections.form)
    printed = [s for s in sections.items.values() if not is_exhibit_item(family, s)]
    if not printed:
        return "exhibits"
    required = _PARTS.get(family)
    return "complete" if required and required <= {_part(family, s.item) for s in printed} else "partial"


def _shifted(s: Section, chars: int, source: str | None) -> Section:
    target = s.target and replace(s.target, spans=[Span(sp.start + chars, sp.end + chars) for sp in s.target.spans])
    return replace(s, start=s.start + chars, end=s.end + chars, target=target, source=s.source or source)


def merge(original: FilingSections, amendments: list[FilingSections]) -> FilingSections:
    """The original as amended by amendments, applied in filing order (see the module docstring)."""
    family = form_family(original.form)
    amendments = sorted(amendments, key=_order)
    items = {k: _shifted(s, 0, original.accession) for k, s in original.items.items()}
    text = original.text
    documents = [{**d, "accession": original.accession} for d in original.documents]
    for a in amendments:
        text += "\n" if text and not text.endswith("\n") else ""
        chars, lines = len(text), text.count("\n")
        text += a.text
        documents += [{**d, "accession": a.accession, "line_start": d["line_start"] + lines,
                       "line_end": d["line_end"] + lines} for d in a.documents]
        printed = {k: _shifted(s, chars, a.accession) for k, s in a.items.items()}
        scope = a.scope or amendment_scope(a)
        if scope == "complete":
            items = printed
        elif scope == "partial":
            items.update({k: s for k, s in printed.items() if not is_exhibit_item(family, s)})
    rank = {k: i for i, k in enumerate(order_for(family, original.filing_date))} if family != "40-F" else {}
    keys = sorted(items, key=lambda k: rank.get(k, len(rank)))
    return FilingSections(accession=original.accession, form=original.form, filing_date=original.filing_date,
                          items={k: items[k] for k in keys}, text=text, engine_version=original.engine_version,
                          documents=documents, filing_status="original", view="as_amended",
                          amendments=[a.accession for a in amendments])


def read(ref: FilingRef, cache=None) -> FilingSections:
    """A filing's sections as filed, with amends from the filing list and, for an amendment, its scope."""
    sections = sections_for_filing(get_filing(ref), cache)
    sections.amends = ref.amends
    if sections.filing_status == "amendment":
        sections.scope = amendment_scope(sections)
    return sections


def amended_sections(original: FilingRef, refs: list[FilingRef], cache=None) -> list[FilingSections]:
    """The amendments of original as filed, in filing order: those the filing list links to it or cannot place,
    kept when their own text names it (or, when the text places them nowhere, the list does)."""
    candidates = [r for r in refs if r.filing_status == "amendment" and base_form(r.form) == base_form(original.form)
                  and (r.amends == original.accession or (r.amends is None and r.filing_date >= original.filing_date))]
    found = []
    for ref in sorted(candidates, key=_order):
        sections = read(ref, cache)
        sections.amends = resolve_amends(sections, refs) or ref.amends
        if sections.amends == original.accession:
            found.append(sections)
    return found


def get_sections(company: Any, form: str, *, fiscal_year: int | None = None, fiscal_period: str | None = None,
                 period_end: str | None = None, filed_on_or_before: str | None = None, view: View = "as_filed",
                 amendment: int | None = None, cache=None) -> FilingSections | None:
    """See edgar_engine.sections.get_sections."""
    if view not in ("as_filed", "as_amended"):
        raise ValueError(f"view must be 'as_filed' or 'as_amended', not {view!r}")
    period = dict(fiscal_year=fiscal_year, fiscal_period=fiscal_period, period_end=period_end)
    if view == "as_filed" and amendment is None:
        ref = select_filing(company, form, filed_on_or_before=filed_on_or_before, **period)
        return read(ref, cache) if ref else None
    refs = list_filings(company, form, amendments=True, filed_on_or_before=filed_on_or_before)
    original = pick_ref([r for r in refs if r.filing_status == "original"], **period)
    if original is None:
        return None
    amendments = amended_sections(original, refs, cache)
    if amendment is not None:
        picked = nth_amendment(amendments, amendment)
        if picked is None or view == "as_filed":
            return picked
        amendments = amendments[:next(i for i, a in enumerate(amendments) if a is picked) + 1]
    return merge(read(original, cache), amendments)
