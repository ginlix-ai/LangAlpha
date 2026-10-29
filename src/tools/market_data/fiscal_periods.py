"""Fiscal-period names for statement dates, earnings dates and SEC filings.

An issuer's fiscal quarters need not follow the calendar (Apple's Q1 ends in
December), so every label is read off the issuer's own income statements.
"""

from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

FILING_DATE_TOLERANCE_DAYS = 5  # a filing lands within days of its earnings release
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


def match_filing_to_fiscal_period(
    filing_date: str,
    earnings_calendar: List[Dict],
    lookup: Dict[str, str],
) -> str:
    """
    Match a SEC filing date to its fiscal period using earnings calendar.
    Returns the fiscal period name or 'Quarterly' if no match found.
    """
    if not earnings_calendar or not filing_date or filing_date == "N/A":
        return "Quarterly"

    try:
        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        best_match = None
        min_diff = float("inf")

        for cal in earnings_calendar:
            cal_date = cal.get("date")
            fiscal_ending = cal.get("fiscalDateEnding")
            if not cal_date or not fiscal_ending:
                continue

            try:
                cal_dt = datetime.strptime(cal_date, "%Y-%m-%d")
                diff = abs((filing_dt - cal_dt).days)
                if diff < min_diff and diff <= FILING_DATE_TOLERANCE_DAYS:
                    min_diff = diff
                    if fiscal_ending in lookup:
                        best_match = lookup[fiscal_ending]
            except ValueError:
                continue

        return best_match or "Quarterly"

    except ValueError:
        return "Quarterly"
