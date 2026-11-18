"""Amendments: which original an amendment amends, what it restates, and the original as amended.

The filing list links most amendments to their original by period of report (filings.link_amendments).
Every amendment read is then checked against its own text, which wins when it places the amendment: the
cover page names the period it reports on, and the explanatory note usually names the date the original
was filed. That corrects a period of report typed wrong on either filing, and places an amendment the
list could not (amends=None). An amendment read for an original is kept only when its text, or failing
that the list, names that original.

Scope. An amendment restates the items it prints (Rule 12b-15 asks for the complete text of each item
amended). It is "complete" when it prints an item from every part of the form and at least half the
form's items (for a 40-F, every principal document), leaving out the exhibit list: a full refiling.
Otherwise it is "partial" (Part III only, restated statements), "exhibits" when the only item it prints
is its exhibit list, and "cover" when it prints no item at all (a cover page, an explanatory note and
signatures, or exhibits with no item around them) or says before its first item that it carries an employee
plan's annual report (Form 11-K under Rule 15d-21), whose numbered items are the plan's, not the form's.

Merge. The as-amended view starts from the original and applies each amendment in filing order. A
complete amendment replaces the item set. A cover amendment changes no item. Any other replaces the items
it prints, its exhibit list included, and keeps the rest. Every item names its source accession.
"""
from __future__ import annotations

import re
from dataclasses import replace
from typing import TypeVar

from .cache import SectionsCache, SubmissionCache
from .filings import FilingRef, base_form, filing_order, get_filing
from .sections import sections_for_filing
from .sections.model import FilingSections, Scope, Section, Span
from .sections.vocab import spec_for

__all__ = ["amended_sections", "amendment_scope", "merge", "read", "restates_statements"]

T = TypeVar("T")
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
_REPORT = re.compile(r"\b(?:annual|quarterly|current)\s+report|\bform\s+[\w-]+(?:/A)?", re.I)
_PLAN_REPORT = re.compile(r"\bForm\s+11-K\b|\bRule\s+15d-21\b", re.I)
_SENTENCE_END = re.compile(r"[.!?]\s+(?=[A-Z(\u201c\"])")
COVER_HEAD = 6_000
NOTE_HEAD = 40_000


def _date(m: re.Match) -> str | None:
    month, day, year = (m["m1"], m["d1"], m["y1"]) if m["m1"] else (m["m2"], m["d2"], m["y2"])
    try:
        return f"{int(year):04d}-{_MONTHS[month[:3].lower()]:02d}-{int(day):02d}"
    except (KeyError, ValueError):
        return None


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

    A date counts when the last report its sentence names before the date is the kind being amended (an annual or
    quarterly report, a Form 10-K, 10-Q, 20-F or 40-F, not an amendment), or when the sentence names no report
    but calls the filing the original. So neither "our Form 8-K originally filed on ..." nor "this Form 10-K/A
    discusses our Form 8-K filed on ..." does.
    """
    head, dates = re.sub(r"[\u2010-\u2015\u2212]", "-", _unwrap(text[:NOTE_HEAD])), []
    for m in _FILED_ON.finditer(head):
        before = head[head.rfind("\n", 0, m.start()) + 1:m.start()]
        ends = list(_SENTENCE_END.finditer(before))
        sentence = before[ends[-1].end():] if ends else before
        context = sentence + head[m.start():m.start("m1") if m["m1"] else m.start("d2")]
        mentions = _REPORT.findall(context)
        named = _ORIGINAL.match(mentions[-1]) if mentions else re.search(r"\boriginal", context, re.I)
        if named and (d := _date(m)) and d not in dates:
            dates.append(d)
    return dates


def resolve_amends(amendment: FilingSections, refs: list[FilingRef]) -> str | None:
    """The original an amendment amends, read from its own text: the original with the period its cover states,
    else the one filed on the first date its explanatory note gives for the original. None when neither places it.
    Only originals filed before the amendment count, ordered by the filing list's acceptance times when the list
    holds the amendment, else by filing date."""
    own = next((r for r in refs if r.accession == amendment.accession), None)

    def filed_before(r: FilingRef) -> bool:
        if own is not None:
            return filing_order(r) <= filing_order(own)
        return amendment.filing_date is None or r.filing_date <= amendment.filing_date

    originals = [r for r in refs if r.filing_status == "original" and base_form(r.form) == base_form(amendment.form)
                 and filed_before(r)]
    period = cover_period(amendment.text)
    by_period = [r for r in originals if period and r.report_date == period]
    if by_period:
        return max(by_period, key=filing_order).accession
    days = original_filed_on(amendment.text)
    by_date = [r for r in originals if days and r.filing_date == days[0]]
    return by_date[0].accession if len(by_date) == 1 else None


def amendment_scope(sections: FilingSections) -> Scope:
    """What an amendment restates: "complete", "partial", "exhibits" or "cover" (see the module docstring)."""
    if not sections.items:
        return "cover"
    spec = spec_for(sections.form, sections.filing_date)
    printed = [s for s in sections.items.values() if not spec.lists_exhibits(s.item, s.heading)]
    if not printed:
        return "exhibits"
    keys = {s.item for s in printed}
    whole = spec.principal_documents <= keys if spec.titled else 2 * len(keys) >= len(spec.order)
    if not whole and _PLAN_REPORT.search(sections.text[:min(NOTE_HEAD, min(s.start for s in sections.items.values()))]):
        return "cover"
    required = spec.required_parts
    return "complete" if whole and required and required <= {spec.part(k) for k in keys} else "partial"


def _shifted(s: Section, chars: int, source: str | None) -> Section:
    target = s.target and replace(s.target, spans=[Span(sp.start + chars, sp.end + chars) for sp in s.target.spans])
    return replace(s, start=s.start + chars, end=s.end + chars, target=target, source=s.source or source)


def merge(original: FilingSections, amendments: list[FilingSections]) -> FilingSections:
    """The original as amended by amendments, applied in filing order (see the module docstring): by filing date,
    and amendments of one day in the order given, which amended_sections gives as EDGAR accepted them."""
    amendments = sorted(amendments, key=lambda a: a.filing_date or "")
    items = {k: _shifted(s, 0, original.accession) for k, s in original.items.items()}
    text = original.text
    documents = [replace(d, accession=original.accession) for d in original.documents]
    for a in amendments:
        text += "\n" if text and not text.endswith("\n") else ""
        chars, lines = len(text), text.count("\n")
        text += a.text
        documents += [replace(d, accession=a.accession, line_start=d.line_start + lines, line_end=d.line_end + lines)
                      for d in a.documents]
        printed = {k: _shifted(s, chars, a.accession) for k, s in a.items.items()}
        scope = a.scope or amendment_scope(a)
        if scope == "complete":
            items = printed
        elif scope != "cover":
            items.update(printed)
    rank = {k: i for i, k in enumerate(spec_for(original.form, original.filing_date).order)}
    keys = sorted(items, key=lambda k: rank.get(k, len(rank)))
    return FilingSections(accession=original.accession, form=original.form, filing_date=original.filing_date,
                          items={k: items[k] for k in keys}, text=text, engine_version=original.engine_version,
                          documents=documents, filing_status="original", view="as_amended",
                          amendments=[a.accession for a in amendments])


def read(ref: FilingRef, cache: SectionsCache | None = None, submissions: SubmissionCache | None = None) -> FilingSections:
    """A filing's sections as filed, with amends from the filing list and, for an amendment, its scope."""
    sections = sections_for_filing(get_filing(ref), cache, submissions)
    return replace(sections, amends=ref.amends,
                   scope=amendment_scope(sections) if sections.filing_status == "amendment" else sections.scope)


def amended_sections(original: FilingRef, refs: list[FilingRef], cache: SectionsCache | None = None,
                     submissions: SubmissionCache | None = None, limit: int | None = None,
                     unreadable: list[FilingRef] | None = None) -> list[FilingSections]:
    """The amendments of original as filed, in filing order: every amendment of its form filed after it, kept
    when its own text names original (or, when the text places it nowhere, the filing list does), so a period
    of report typed wrong on the list cannot hide an amendment from its original. With limit, reading stops
    at the first limit amendments, so a later submission that cannot be read does not fail them.

    A candidate that cannot be read raises, unless unreadable is given: then it is appended there and the rest are
    still read, so one amendment EDGAR serves cut short, or fails to serve, does not hide the others or the
    original."""
    candidates = [r for r in refs if r.filing_status == "amendment" and base_form(r.form) == base_form(original.form)
                  and (r.amends == original.accession or r.filing_date >= original.filing_date)]
    found = []
    for ref in sorted(candidates, key=filing_order):
        try:
            sections = read(ref, cache, submissions)
        except Exception:
            if unreadable is None:
                raise
            unreadable.append(ref)
            continue
        sections = replace(sections, amends=resolve_amends(sections, refs) or ref.amends)
        if sections.amends == original.accession:
            found.append(sections)
            if len(found) == limit:
                break
    return found


def restates_statements(amendment: FilingSections) -> bool:
    """The amendment restates the financial statements: it refiles the whole report, or prints an item that holds
    the statements (10-K Item 8, 10-Q Part I Item 1, 20-F Items 17 and 18, a 40-F's audited statements)."""
    scope = amendment.scope or amendment_scope(amendment)
    spec = spec_for(amendment.form, amendment.filing_date)
    held = spec.statement_items | ({"AFS"} & spec.principal_documents)
    return scope == "complete" or any(k in held for k, s in amendment.items.items() if s.status != "stub")


def nth_amendment(amendments: list[T], n: int) -> T | None:
    """The n-th of amendments in filing order: 1 is the first, -1 the latest. None when there is no such one."""
    if n == 0:
        raise ValueError("amendment counts from 1 (the first) or from -1 (the latest)")
    i = n - 1 if n > 0 else n
    return amendments[i] if -len(amendments) <= i < len(amendments) else None
