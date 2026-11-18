"""Any-period filing selection with fiscal year and period labels.

Fiscal labels come from SEC companyfacts, where every fact carries the accession,
fiscal year and fiscal period its filing declared, so the label is the filer's own.
Filings without XBRL facts (pre-2009 and small filers) fall back to a period rule:
the fiscal year is the calendar year in which the fiscal year ends, except a year
ending in the first 3 weeks of January, which belongs to the previous year (the
52/53-week calendars that close just after New Year). A 10-K is "FY"; a 10-Q is
Q1 to Q3 by the quarters between the prior fiscal year end (Company.fiscal_year_end,
"MMDD") and the report date, rounded to the nearest quarter.

An amendment (10-K/A, 10-Q/A, 20-F/A, 40-F/A) amends the latest original of the same base form
(10-K405 counting as 10-K) with the same period of report, filed on or before it. EDGAR records no
link between the two, but the period of report is the filer's own key for the report, and it holds
for amendments filed years later and for several amendments to one original. An amendment whose
period matches no original gets amends=None here; edgar_engine.amendments reads the amendment's own
cover page and explanatory note, which place it and correct a period of report typed wrong.

refs_from_rows, link_amendments, period_rule and pick_ref are the steps list_filings and
select_filing are built from, exposed as test seams, not as API.

Only APIs that edgartools 5.62 and 6.0 share are used. A company with no filings of the form
yields an empty list on both; other errors propagate.
"""
from __future__ import annotations

import calendar
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import date, timedelta
from types import MappingProxyType
from typing import Any, Iterable, Literal, Mapping

from .sections.model import FilingStatus
from .sections.vocab import base_form, filed_forms, is_amendment

FiscalSource = Literal["xbrl", "period_rule"]

__all__ = ["FilingRef", "base_form", "companyfacts_labels", "get_filing", "list_filings", "select_filing"]


@dataclass(frozen=True)
class FilingRef:
    cik: int
    accession: str
    form: str
    filing_date: str
    report_date: str | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None
    fiscal_source: FiscalSource | None = None
    amends: str | None = None
    accepted: str | None = None

    @property
    def filing_status(self) -> FilingStatus:
        return "amendment" if is_amendment(self.form) else "original"


def filing_order(filing) -> tuple[str, str, str]:
    """Filing date, then the time EDGAR accepted it, then accession: the order EDGAR received filings in (for a
    FilingRef or FilingSections). An accession's prefix names the filer agent, so accessions alone do not order
    two agents' filings of one day."""
    return filing.filing_date or "", getattr(filing, "accepted", None) or "", filing.accession or ""


def link_amendments(refs: Iterable[FilingRef]) -> list[FilingRef]:
    """refs with each amendment's amends set to the latest original of its base form and period filed on or before it."""
    refs = list(refs)
    originals: dict[tuple[str, str], list[FilingRef]] = defaultdict(list)
    for r in refs:
        if r.filing_status == "original" and r.report_date:
            originals[(base_form(r.form), r.report_date)].append(r)
    linked = []
    for r in refs:
        if r.filing_status == "amendment" and r.report_date:
            prior = [o for o in originals[(base_form(r.form), r.report_date)] if filing_order(o) <= filing_order(r)]
            if prior:
                r = replace(r, amends=max(prior, key=filing_order).accession)
        linked.append(r)
    return linked


def _on(year: int, month: int, day: int) -> date:
    return date(year, month, min(day, calendar.monthrange(year, month)[1]))


def _fiscal_year(year_end: date) -> int:
    return year_end.year - (1 if year_end.month == 1 and year_end.day <= 21 else 0)


def period_rule(form: str, report_date: str, fiscal_year_end: str | None) -> tuple[int, str | None]:
    """Fiscal (year, period) for a filing with no XBRL facts; the period is None when it cannot be told."""
    end = date.fromisoformat(report_date)
    if form.startswith(("10-K", "20-F", "40-F")):
        return _fiscal_year(end), "FY"
    if not fiscal_year_end:
        return _fiscal_year(end), None
    month, day = int(fiscal_year_end[:2]), int(fiscal_year_end[2:])
    anchor = end - timedelta(days=21)
    anniversary = _on(anchor.year, month, day)
    if anniversary < anchor:
        anniversary = _on(anchor.year + 1, month, day)
    if not form.startswith("10-Q"):
        return _fiscal_year(anniversary), None
    quarter = round((end - _on(anniversary.year - 1, month, day)).days / 91.3)
    return _fiscal_year(anniversary), f"Q{quarter}" if 1 <= quarter <= 3 else None


def _last_year_end(filing_date: str, fiscal_year_end: str) -> str:
    """The fiscal year end an annual report filed on filing_date covers: the last one before it."""
    filed = date.fromisoformat(filing_date)
    end = _on(filed.year, int(fiscal_year_end[:2]), int(fiscal_year_end[2:]))
    return (end if end < filed else _on(filed.year - 1, end.month, int(fiscal_year_end[2:]))).isoformat()


def refs_from_rows(
    rows: Iterable[dict[str, Any]],
    cik: int,
    forms: Iterable[str],
    *,
    amendments: bool = False,
    filed_on_or_before: str | None = None,
    fiscal_labels: Mapping[tuple[str, str], tuple[int, str]] | None = None,
    fiscal_year_end: str | None = None,
) -> list[FilingRef]:
    """Newest-first refs for the rows of a filings table (accession_number, form, filing_date, reportDate). An annual
    report the table gives no period of report covers the last fiscal year end before it was filed; an amendment with
    none is left for its own text to place. A quarter is counted from the year end of the next annual report in the
    table within a year, so a filer that moved its year end keeps its old quarters, and from fiscal_year_end
    when no annual report follows it. A filing with no
    XBRL label is labeled by the period rule, moved by the years the filer's own XBRL labels differ from it on
    filings with the same year-end month (a January year end is the prior fiscal year for some filers and the
    same one for others, and a filer that moved its year end may label each convention differently)."""
    wanted = set(forms)
    if amendments:
        wanted |= {f"{f}/A" for f in wanted}
    fiscal_labels = fiscal_labels or {}
    rows = list(rows)
    dated_annual = sorted((str(r["filing_date"]), str(r["reportDate"])) for r in rows if r.get("reportDate")
                          and not is_amendment(r["form"]) and base_form(r["form"]) in ("10-K", "20-F", "40-F"))
    annual_ends = sorted({end for _, end in dated_annual})

    def undated_year_end(filing_date: str) -> str | None:
        """The year end of the annual report filed nearest to filing_date when it ends in another month than
        fiscal_year_end, so a filer that moved its year end keeps its old years."""
        if not dated_annual:
            return fiscal_year_end
        filed = date.fromisoformat(filing_date)
        _, end = min(dated_annual, key=lambda d: abs((date.fromisoformat(d[0]) - filed).days))
        if fiscal_year_end and fiscal_year_end[:2] == end[5:7]:
            return fiscal_year_end
        return f"{end[5:7]}{end[8:10]}"

    def year_end_for(form: str, report_date: str | None) -> str | None:
        if not report_date or form.startswith(("10-K", "20-F", "40-F")):
            return fiscal_year_end
        within = (date.fromisoformat(report_date) + timedelta(days=380)).isoformat()
        end = next((e for e in annual_ends if report_date <= e <= within), None)
        return f"{end[5:7]}{end[8:10]}" if end else fiscal_year_end

    picked, accepted = [], {}
    for row in rows:
        form, accession = row["form"], row["accession_number"]
        filing_date = str(row["filing_date"])
        if form not in wanted or (filed_on_or_before and filing_date > filed_on_or_before):
            continue
        if at := row.get("acceptanceDateTime"):
            accepted[accession] = at.isoformat() if hasattr(at, "isoformat") else str(at)
        report_date = str(row.get("reportDate") or "") or None
        if report_date is None and not is_amendment(form) and base_form(form) in ("10-K", "20-F", "40-F") \
                and (year_end := undated_year_end(filing_date)):
            report_date = _last_year_end(filing_date, year_end)
        rule = period_rule(form, report_date, year_end_for(form, report_date)) if report_date else None
        picked.append((form, accession, filing_date, report_date, fiscal_labels.get((accession, form)), rule))

    def year_end_month(form: str, report_date: str | None) -> int | None:
        fye = year_end_for(form, report_date)
        if form.startswith(("10-K", "20-F", "40-F")) and report_date:
            end = date.fromisoformat(report_date)
        elif fye:
            end = _on(2001, int(fye[:2]), int(fye[2:]))
        else:
            return None
        return (end - timedelta(days=21)).month

    shifts: dict[int | None, int] = {}
    for key in {year_end_month(form, report_date) for form, _, _, report_date, label, rule in picked if rule and not label}:
        labeled = [(form, label[0] - rule[0]) for form, _, _, report_date, label, rule in picked
                   if label and rule and year_end_month(form, report_date) == key]
        counts = Counter(s for form, s in labeled if not is_amendment(form)) or Counter(s for _, s in labeled)
        shifts[key] = counts.most_common(1)[0][0] if counts else 0
    refs = []
    for form, accession, filing_date, report_date, label, rule in picked:
        source = "xbrl" if label else "period_rule" if rule else None
        shift = shifts.get(year_end_month(form, report_date), 0)
        year, period = label or ((rule[0] + shift, rule[1]) if rule else (None, None))
        refs.append(FilingRef(cik, accession, form, filing_date, report_date, year, period, source,
                              accepted=accepted.get(accession)))
    return sorted(link_amendments(refs), key=filing_order, reverse=True)


def pick_ref(
    refs: list[FilingRef],
    *,
    fiscal_year: int | None = None,
    fiscal_period: str | None = None,
    period_end: str | None = None,
) -> FilingRef | None:
    """Latest ref when no period is given, else the latest original filed for the period: a report filed twice
    is superseded by its refile, which its amendments link to (link_amendments). With no original, the earliest
    filed match."""
    if fiscal_year is None and fiscal_period is None and period_end is None:
        return refs[0] if refs else None
    matches = [
        r
        for r in refs
        if (fiscal_year is None or r.fiscal_year == fiscal_year)
        and (fiscal_period is None or r.fiscal_period == fiscal_period)
        and (period_end is None or (r.report_date or "").startswith(period_end))
    ]
    originals = [r for r in matches if r.filing_status == "original"]
    return max(originals, key=filing_order) if originals else min(matches, key=filing_order, default=None)


def companyfacts_labels(cik: int) -> Mapping[tuple[str, str], tuple[int, str]]:
    """(accession, form) -> (fiscal year, fiscal period), by majority vote over the companyfacts facts of that filing.
    Read from SEC on every call and kept nowhere, since a new filing adds labels; a caller that lists several forms
    reads them once and passes them to list_filings as fiscal_labels. Read-only, so callers can share one result."""
    from edgar.entity.entity_facts import download_company_facts_from_sec
    from edgar.exceptions import CompanyFactsNotFoundError

    try:
        facts = download_company_facts_from_sec(cik)
    except CompanyFactsNotFoundError:
        return MappingProxyType({})
    votes: dict[tuple[str, str], Counter] = defaultdict(Counter)
    for concepts in (facts or {}).get("facts", {}).values():
        for concept in concepts.values():
            for unit_facts in concept.get("units", {}).values():
                for fact in unit_facts:
                    if fact.get("accn") and fact.get("form") and fact.get("fy") and fact.get("fp"):
                        votes[(fact["accn"], fact["form"])][(int(fact["fy"]), fact["fp"])] += 1
    return MappingProxyType({key: counter.most_common(1)[0][0] for key, counter in votes.items()})


def _company(company: Any):
    from edgar import Company

    if isinstance(company, Company):
        return company
    if isinstance(company, str) and company.strip().isdigit():
        company = int(company)
    return Company(company)


def list_filings(
    company: Any,
    form: str,
    *,
    amendments: bool = False,
    filed_on_or_before: str | None = None,
    full_history: bool = True,
    fiscal_labels: Mapping[tuple[str, str], tuple[int, str]] | None = None,
) -> list[FilingRef]:
    """Filings of a form, newest first. company is a ticker, a CIK (int or str) or an edgar.Company.

    full_history=False reads only the filings an edgar.Company loads first, EDGAR's recent page (at least the
    last year, or the last 1,000 filings), unless that Company has already loaded the rest. A filer with tens of
    thousands of filings (a bank issuing notes) spreads the rest over dozens of further requests."""
    company = _company(company)
    forms = filed_forms(form)
    annual = filed_forms("10-K") if base_form(form) == "10-Q" else ()
    filings = company.get_filings(form=[*forms, *(f"{f}/A" for f in forms), *annual] if amendments else [*forms, *annual],
                                  trigger_full_load=full_history)
    if filings is None or filings.data.num_rows == 0:
        return []
    fiscal_year_end = getattr(company, "fiscal_year_end", None)
    return refs_from_rows(
        filings.data.to_pylist(),
        int(company.cik),
        forms,
        amendments=amendments,
        filed_on_or_before=filed_on_or_before,
        fiscal_labels=companyfacts_labels(int(company.cik)) if fiscal_labels is None else fiscal_labels,
        fiscal_year_end=fiscal_year_end,
    )


def select_filing(
    company: Any,
    form: str,
    *,
    fiscal_year: int | None = None,
    fiscal_period: str | None = None,
    period_end: str | None = None,
    filed_on_or_before: str | None = None,
    full_history: bool = True,
    fiscal_labels: Mapping[tuple[str, str], tuple[int, str]] | None = None,
) -> FilingRef | None:
    """One original filing: the latest, or the one for a fiscal year/period or for a report period ending on
    period_end (YYYY-MM-DD or YYYY-MM). Its amendments are read with edgar_engine.get_sections, which checks each
    one's own text.

    EDGAR's recent page is read first, and the whole history (full_history) only when the filing can be older than
    that page: nothing on it matches, and the period asked for is not later than every period its XBRL labels on
    it. A match labeled by the period rule is also read again from the whole history, since older filings can move
    the years that rule infers (refs_from_rows)."""
    company = _company(company)
    listing = dict(filed_on_or_before=filed_on_or_before, fiscal_labels=fiscal_labels)
    period = dict(fiscal_year=fiscal_year, fiscal_period=fiscal_period, period_end=period_end)
    recent = list_filings(company, form, full_history=False, **listing)
    ref = pick_ref(recent, **period)
    settled = ref is not None and (ref.fiscal_source != "period_rule" or fiscal_year is None and fiscal_period is None)
    if settled or not full_history or _later_than_all(recent, fiscal_year, period_end):
        return ref
    return pick_ref(list_filings(company, form, **listing), **period)


def _later_than_all(refs: list[FilingRef], fiscal_year: int | None, period_end: str | None) -> bool:
    """The period asked for is later than every period refs cover, so no older page can hold it. Only XBRL labels
    count for years: a year the period rule inferred can move once older filings are read."""
    years = [r.fiscal_year for r in refs if r.fiscal_year is not None and r.fiscal_source == "xbrl"]
    ends = [r.report_date for r in refs if r.report_date]
    return bool((fiscal_year is not None and years and fiscal_year > max(years))
                or (period_end and ends and period_end > max(ends)[:len(period_end)]))


def get_filing(ref: FilingRef):
    """The edgartools Filing for a ref. Its company name is left blank; the submission header carries it."""
    from edgar import Filing

    return Filing(cik=ref.cik, company="", form=ref.form, filing_date=ref.filing_date, accession_no=ref.accession)
