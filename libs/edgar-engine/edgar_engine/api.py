"""One company's filings for any period, as filed or as amended: the layer that knows filings, amendments, fetching
and cache."""
from __future__ import annotations

import warnings
from dataclasses import dataclass, replace
from typing import Any

import pandas as pd

from .amendments import amended_sections, merge, nth_amendment, read
from .cache import SectionsCache, SubmissionCache
from .filings import _company, list_filings, pick_ref, select_filing
from .ownership import OwnershipFiling, belongs_to_issuer, parse_ownership
from .sections.model import FilingSections, View
from .submission import decode, fetch_submission, header
from .thirteenf import ThirteenFFiling, holding_rows, parse_13f, resolve_amendments

OWNERSHIP_XML_FROM = "2003-06-30"


def get_sections(company: Any, form: str, *, fiscal_year: int | None = None, fiscal_period: str | None = None,
                 period_end: str | None = None, filed_on_or_before: str | None = None, view: View = "as_filed",
                 amendment: int | None = None, cache: SectionsCache | None = None,
                 submissions: SubmissionCache | None = None) -> FilingSections | None:
    """Items of one company's filing for any period: the latest original by default, else the original for
    fiscal_year/fiscal_period or for the report period ending on period_end. None when there is no such filing.

    view="as_filed" (the default) returns that original as filed; view="as_amended" returns it with its
    amendments merged in filing order, each item labeled with its source accession. amendment=n selects the
    n-th amendment of that original (1 the first, -1 the latest): as filed, or with view="as_amended" the
    original as amended through it. filed_on_or_before leaves out filings made after that date, so an
    as-amended view can be taken as of a day. See edgar_engine.amendments for the rules. cache keeps parsed
    sections and submissions keeps raw submissions, each by accession.
    """
    if view not in ("as_filed", "as_amended"):
        raise ValueError(f"view must be 'as_filed' or 'as_amended', not {view!r}")
    if amendment == 0:
        raise ValueError("amendment counts from 1 (the first) or from -1 (the latest)")
    period = dict(fiscal_year=fiscal_year, fiscal_period=fiscal_period, period_end=period_end)
    if view == "as_filed" and amendment is None:
        ref = select_filing(company, form, filed_on_or_before=filed_on_or_before, **period)
        return read(ref, cache, submissions) if ref else None
    refs = list_filings(company, form, amendments=True, filed_on_or_before=filed_on_or_before)
    original = pick_ref([r for r in refs if r.filing_status == "original"], **period)
    if original is None:
        return None
    amendments = amended_sections(original, refs, cache, submissions, limit=amendment if amendment and amendment > 0 else None)
    if amendment is not None:
        picked = nth_amendment(amendments, amendment)
        if picked is None or view == "as_filed":
            return picked
        amendments = amendments[:next(i for i, a in enumerate(amendments) if a is picked) + 1]
    return merge(read(original, cache, submissions), amendments)


def get_insider_filings(company: Any, *, start: str | None = None, end: str | None = None,
                        forms: tuple[str, ...] = ("4", "4/A"),
                        submissions: SubmissionCache | None = None) -> list[OwnershipFiling]:
    """A company's ownership filings as issuer, filed between start and end (YYYY-MM-DD, inclusive), newest first.

    forms is matched exactly (("4",) leaves out 4/A). start and end select on filing date, so statistics built from
    the result miss late filings for its last quarter and hold only late ones for a quarter before start. EDGAR
    lists the company's filings as a reporting owner too; those are dropped on the parsed issuer CIK. Each filing
    costs one request (its .txt), or none when submissions holds it. Ownership documents are XML since 2003-06-30,
    so an earlier start raises ValueError; a later filing with no readable ownership document is left out with a
    warning naming it. A filing EDGAR serves cut short twice raises TruncatedSubmission, a failure worth retrying,
    rather than leaving the list short without saying so.
    """
    if start is not None and start < OWNERSHIP_XML_FROM:
        raise ValueError(f"ownership filings are read from {OWNERSHIP_XML_FROM}, the start of XML ownership documents")
    if not forms:
        return []
    company = _company(company)
    listed = company.get_filings(form=list(forms), filing_date=f"{start or OWNERSHIP_XML_FROM}:{end or ''}")
    if listed is None:
        return []
    out, accepted = [], {}
    for row in listed.data.to_pylist():
        if row["form"] not in forms:
            continue
        accession = row["accession_number"]
        if at := row.get("acceptanceDateTime"):
            accepted[accession] = at.isoformat() if hasattr(at, "isoformat") else str(at)
        filing = parse_ownership(fetch_submission(company.cik, accession, submissions), accession=accession)
        if filing is None:
            warnings.warn(f"{accession}: no readable ownership document", stacklevel=2)
        elif belongs_to_issuer(filing, company.cik):
            out.append(filing)
    return sorted(out, key=lambda f: (f.filed or "", accepted.get(f.accession, ""), f.accession or ""), reverse=True)


@dataclass(frozen=True)
class ThirteenFReports:
    """The reports that make up a filer's holdings for a period (filer_cik's), and the reports left out: unreadable,
    those listed for the period whose document or information table could not be read (one that declares zero
    entries is read), and other_filer, (accession, CIK) for those filed under another filer's CIK."""
    filer_cik: int
    reports: tuple[ThirteenFFiling, ...]
    unreadable: tuple[str, ...]
    other_filer: tuple[tuple[str, int], ...]


def get_13f_reports(filer: Any, period: str, *, submissions: SubmissionCache | None = None) -> ThirteenFReports:
    """The reports that make up one filer's holdings for a period (YYYY-MM-DD), after amendment resolution, in
    filing order, with the reports left out and why. Each report costs one request, or none when submissions holds
    it. A report with no readable 13F document is placed by its header's period of report, and counts for the period
    asked when that is missing too. An unreadable report never replaces a readable one, but the holdings may be incomplete when it is not
    superseded, so a caller decides what to do with unreadable. A report EDGAR serves cut short twice raises
    TruncatedSubmission, since resolving amendments without it could return the wrong holdings."""
    company = _company(filer)
    listed = company.get_filings(form=["13F-HR", "13F-HR/A"])
    if listed is None:
        return ThirteenFReports(int(company.cik), (), (), ())
    reports, unreadable, other_filer = [], [], []
    for row in listed.data.to_pylist():
        if str(row.get("reportDate") or "")[:10] not in ("", period):
            continue
        accession = row["accession_number"]
        body = fetch_submission(company.cik, accession, submissions)
        f = parse_13f(body, accession=accession)
        if (f.period if f is not None else header(decode(body)).get("period") or period) != period:
            continue
        if f is None or (not f.has_information_table and f.declared_entries != 0):
            unreadable.append(accession)
        elif f.filer_cik is not None and f.filer_cik != int(company.cik):
            other_filer.append((accession, f.filer_cik))
        else:
            reports.append(replace(f, filer_cik=int(company.cik)))
    if not reports:
        return ThirteenFReports(int(company.cik), (), tuple(unreadable), tuple(other_filer))
    filings = pd.DataFrame([{"accession": f.accession, "filer_cik": f.filer_cik, "period": f.period, "form": f.form,
                             "filed": f.filed, "amendment_type": f.amendment_type, "amendment_no": f.amendment_no}
                            for f in reports])
    rows = pd.DataFrame([r for f in reports for r in holding_rows(f)])
    order = {a: i for i, a in enumerate(resolve_amendments(filings, rows if len(rows) else None))}
    kept = sorted((f for f in reports if f.accession in order), key=lambda f: order[f.accession])
    return ThirteenFReports(int(company.cik), tuple(kept), tuple(unreadable), tuple(other_filer))


def get_13f_holdings(filer: Any, period: str, *, submissions: SubmissionCache | None = None) -> list[ThirteenFFiling]:
    """The reports of get_13f_reports, with a warning naming each report it left out: one whose document or
    information table is missing or unreadable (and that does not declare zero entries), or one filed under another
    CIK. Use get_13f_reports to act on those instead."""
    got = get_13f_reports(filer, period, submissions=submissions)
    for accession in got.unreadable:
        warnings.warn(f"{accession}: no readable 13F document or information table", stacklevel=2)
    for accession, cik in got.other_filer:
        warnings.warn(f"{accession}: filed under CIK {cik}, not {got.filer_cik}", stacklevel=2)
    return list(got.reports)
