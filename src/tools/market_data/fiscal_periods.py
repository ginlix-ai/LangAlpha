"""Fiscal-period names for statement dates, earnings dates and SEC filings.

An issuer's fiscal quarters need not follow the calendar (Apple's Q1 ends in
December), so every label is read off the issuer's own income statements.
"""

from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

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


def period_name(period_end: Optional[str], lookup: Dict[str, str]) -> Optional[str]:
    """The fiscal name for *period_end*, read off the statements or inferred."""
    if not period_end:
        return None
    return lookup.get(period_end) or infer_fiscal_period(period_end, lookup)
