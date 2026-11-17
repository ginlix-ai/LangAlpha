"""One company's filing for any period, as filed or as amended: the layer that knows filings, amendments and cache."""
from __future__ import annotations

from typing import Any

from .amendments import amended_sections, merge, nth_amendment, read
from .cache import SectionsCache, SubmissionCache
from .filings import list_filings, pick_ref, select_filing
from .sections.model import FilingSections, View


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
