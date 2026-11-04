"""Any-period filing selection with fiscal year and period labels.

Fiscal labels come from SEC companyfacts, where every fact carries the accession,
fiscal year and fiscal period its filing declared, so the label is the filer's own.
Filings without XBRL facts (pre-2009 and small filers) fall back to a period rule:
the fiscal year is the calendar year in which the fiscal year ends, except a year
ending in the first 3 weeks of January, which belongs to the previous year (the
52/53-week calendars that close just after New Year). A 10-K is "FY"; a 10-Q is
Q1 to Q3 by the quarters between the prior fiscal year end (Company.fiscal_year_end,
"MMDD") and the report date, rounded to the nearest quarter.

Only APIs that edgartools 5.62 and 6.0 share are used. A company with no filings of the form
yields an empty list on both; other errors propagate.
"""
from __future__ import annotations

import calendar
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from types import MappingProxyType
from typing import Any, Iterable, Literal, Mapping

FiscalSource = Literal["xbrl", "period_rule"]


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
    """Newest-first refs for the rows of a filings table (accession_number, form, filing_date, reportDate)."""
    wanted = set(forms)
    if amendments:
        wanted |= {f"{f}/A" for f in wanted}
    fiscal_labels = fiscal_labels or {}
    refs = []
    for row in rows:
        form, accession = row["form"], row["accession_number"]
        filing_date = str(row["filing_date"])
        if form not in wanted or (filed_on_or_before and filing_date > filed_on_or_before):
            continue
        report_date = str(row.get("reportDate") or "") or None
        label = fiscal_labels.get((accession, form))
        source = "xbrl" if label else None
        if label is None and report_date:
            label, source = period_rule(form, report_date, fiscal_year_end), "period_rule"
        year, period = label or (None, None)
        refs.append(FilingRef(cik, accession, form, filing_date, report_date, year, period, source))
    return sorted(refs, key=lambda r: (r.filing_date, r.accession), reverse=True)


def pick_ref(
    refs: list[FilingRef],
    *,
    fiscal_year: int | None = None,
    fiscal_period: str | None = None,
    period_end: str | None = None,
) -> FilingRef | None:
    """Latest ref when no period is given, else the earliest filed match, which is the original of a refile."""
    if fiscal_year is None and fiscal_period is None and period_end is None:
        return refs[0] if refs else None
    matches = [
        r
        for r in refs
        if (fiscal_year is None or r.fiscal_year == fiscal_year)
        and (fiscal_period is None or r.fiscal_period == fiscal_period)
        and (period_end is None or (r.report_date or "").startswith(period_end))
    ]
    return min(matches, key=lambda r: (r.filing_date, r.accession), default=None)


def companyfacts_labels(cik: int) -> Mapping[tuple[str, str], tuple[int, str]]:
    """(accession, form) -> (fiscal year, fiscal period), by majority vote over the companyfacts facts of that filing.
    Cached for the day, so a long-lived process sees later filings' labels, and read-only, because every caller
    shares the cached result. A company with no companyfacts yet is not cached."""
    from edgar.exceptions import CompanyFactsNotFoundError

    try:
        return _companyfacts_labels(cik, date.today())
    except CompanyFactsNotFoundError:
        return MappingProxyType({})


@lru_cache(maxsize=64)
def _companyfacts_labels(cik: int, day: date) -> Mapping[tuple[str, str], tuple[int, str]]:
    from edgar.entity.entity_facts import download_company_facts_from_sec

    facts = download_company_facts_from_sec(cik)
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
) -> list[FilingRef]:
    """Filings of a form, newest first. company is a ticker, a CIK (int or str) or an edgar.Company."""
    company = _company(company)
    forms = ("10-K", "10-K405") if form == "10-K" else (form,)
    filings = company.get_filings(form=[*forms, *(f"{f}/A" for f in forms)] if amendments else list(forms))
    if filings is None or filings.data.num_rows == 0:
        return []
    fiscal_year_end = getattr(company, "fiscal_year_end", None)
    return refs_from_rows(
        filings.data.to_pylist(),
        int(company.cik),
        forms,
        amendments=amendments,
        filed_on_or_before=filed_on_or_before,
        fiscal_labels=companyfacts_labels(int(company.cik)),
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
    amendments: bool = False,
) -> FilingRef | None:
    """One filing: the latest, or the original for a fiscal year/period or for a report period ending on period_end (YYYY-MM-DD or YYYY-MM)."""
    refs = list_filings(company, form, amendments=amendments, filed_on_or_before=filed_on_or_before)
    return pick_ref(refs, fiscal_year=fiscal_year, fiscal_period=fiscal_period, period_end=period_end)


def get_filing(ref: FilingRef):
    """The edgartools Filing for a ref. Its company name is left blank; the submission header carries it."""
    from edgar import Filing

    return Filing(cik=ref.cik, company="", form=ref.form, filing_date=ref.filing_date, accession_no=ref.accession)
