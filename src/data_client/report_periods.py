"""Which fiscal period end each earnings announcement reports.

An earnings row dates its announcement, not its period, so reports and the
statements' period ends are aligned here, for the overview and the
fundamentals tools alike.
"""

from datetime import datetime
from typing import Collection, Dict, List, Optional

# An A-share's annual report is due by April 30: 121 days after December 31 in a leap year.
REPORT_LAG_MAX_DAYS = 125
# An interim report comes far sooner (a 10-Q within 45 days, an A-share's or HK
# interim within about two months), so a report this late is the next period's.
INTERIM_LAG_MAX_DAYS = 95


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
