"""Fiscal-period names for statement dates, earnings dates and SEC filings.

An issuer's fiscal quarters need not follow the calendar (Apple's Q1 ends in
December), so every label is read off the issuer's own income statements.
"""

from datetime import datetime
from typing import Collection, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# An A-share's annual report is due by April 30: 121 days after December 31 in a leap year.
REPORT_LAG_MAX_DAYS = 125
# An interim report comes far sooner (a 10-Q within 45 days, an A-share's or HK
# interim within about two months), so a report this late is the next period's.
INTERIM_LAG_MAX_DAYS = 95
DAYS_PER_QUARTER = 90  # Approximate days per fiscal quarter


def fiscal_period_lookup(income_stmt: List[Dict]) -> Dict[str, str]:
    """Build a lookup dict mapping fiscal end dates to period names (e.g., 'Q3 FY2026')."""
    lookup = {}
    for stmt in income_stmt:
        stmt_date = stmt.get("date")
        period = stmt.get("period")  # Q1, Q2, Q3, Q4
        fiscal_year = stmt.get("fiscalYear")
        if stmt_date and period and fiscal_year:
            lookup[stmt_date] = f"{period} FY{fiscal_year}"
    return lookup


def infer_fiscal_period(
    fiscal_ending: str, lookup: Dict[str, str]
) -> Optional[str]:
    """
    Infer fiscal period name for a date not in the lookup.
    Uses the pattern from existing quarters to estimate future quarters.
    """
    if not fiscal_ending or not lookup:
        return None

    try:
        fe_date = datetime.strptime(fiscal_ending, "%Y-%m-%d")

        # Find the most recent known quarter
        for date_str, period_str in sorted(lookup.items(), reverse=True):
            if not period_str.startswith("Q"):
                continue

            last_date = datetime.strptime(date_str, "%Y-%m-%d")
            last_q = int(period_str[1])
            last_fy = int(period_str.split("FY")[1])

            # Calculate quarter offset from days difference
            days_diff = (fe_date - last_date).days
            quarters_ahead = round(days_diff / DAYS_PER_QUARTER)
            next_q = last_q + quarters_ahead
            next_fy = last_fy

            # Handle fiscal year rollover
            while next_q > 4:
                next_q -= 4
                next_fy += 1
            while next_q < 1:
                next_q += 4
                next_fy -= 1

            return f"Q{next_q} FY{next_fy}"

    except (ValueError, KeyError) as e:
        logger.debug(f"Could not infer fiscal period for {fiscal_ending}: {e}")

    return None


def report_period_ends(
    report_dates: List[str], period_ends: List[str], annual_ends: Collection[str] = ()
) -> Dict[str, str]:
    """Map each earnings announcement date to the fiscal period end it reports.

    FMP's stable earnings rows no longer carry ``fiscalDateEnding``, so the two
    dated sequences are aligned in order: each period end pairs with at most
    one later report within ``REPORT_LAG_MAX_DAYS`` of a fiscal year end (one of
    *annual_ends*) or ``INTERIM_LAG_MAX_DAYS`` of any other, and the alignment
    with the least total lag wins, a skipped end or report costing the most a
    lag can.
    So an A-share's annual report (late April, after Q1 has ended) stays on Q4
    and its Q1 report on Q1, and a statement older than the earnings history
    is left out instead of shifting every quarter by one.
    """
    reports = sorted({str(d)[:10] for d in report_dates if d})
    ends = sorted({str(e)[:10] for e in period_ends if e})
    annual = {str(e)[:10] for e in annual_ends}

    def lag(end: str, report: str) -> Optional[int]:
        try:
            days = (datetime.strptime(report, "%Y-%m-%d") - datetime.strptime(end, "%Y-%m-%d")).days
        except ValueError:
            return None
        cap = REPORT_LAG_MAX_DAYS if end in annual else INTERIM_LAG_MAX_DAYS
        return days if 0 < days <= cap else None

    skip = REPORT_LAG_MAX_DAYS
    n, m = len(ends), len(reports)
    # cost[i][j]: the cheapest alignment of ends[i:] with reports[j:].
    cost = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n, -1, -1):
        for j in range(m, -1, -1):
            options = [skip + cost[i + 1][j]] if i < n else []
            options += [skip + cost[i][j + 1]] if j < m else []
            paired = lag(ends[i], reports[j]) if i < n and j < m else None
            options += [paired + cost[i + 1][j + 1]] if paired is not None else []
            cost[i][j] = min(options, default=0)

    out: Dict[str, str] = {}
    i = j = 0
    while i < n and j < m:
        paired = lag(ends[i], reports[j])
        if paired is not None and cost[i][j] == paired + cost[i + 1][j + 1]:
            out[reports[j]] = ends[i]
            i, j = i + 1, j + 1
        elif cost[i][j] == skip + cost[i + 1][j]:
            i += 1
        else:
            j += 1
    return out


def period_name(period_end: Optional[str], lookup: Dict[str, str]) -> Optional[str]:
    """The fiscal name for *period_end*, read off the statements or inferred."""
    if not period_end:
        return None
    return lookup.get(period_end) or infer_fiscal_period(period_end, lookup)
