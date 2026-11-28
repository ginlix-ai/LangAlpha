"""13F report periods: the day each is due and the latest one a reader can expect to be complete on a day.

Every 13F-HR for a quarter is due 45 days after it ends, on the next business day when that day is not one
(Exchange Act Rule 0-3, edgar_engine.calendar.roll). Late filings keep landing for a few days after, so a quarter is
the expected one only once its deadline is SETTLE_DAYS behind.
"""
from __future__ import annotations

from datetime import date, timedelta

from .calendar import roll

__all__ = ["FILING_DEADLINE_DAYS", "SETTLE_DAYS", "expected_period", "period_deadline", "prior_period"]

FILING_DEADLINE_DAYS = 45
SETTLE_DAYS = 3


def prior_period(period: str) -> str:
    """The quarter end before the quarter a day (YYYY-MM-DD) falls in, so the one before a quarter end."""
    day = date.fromisoformat(period)
    return (date(day.year, 3 * ((day.month - 1) // 3) + 1, 1) - timedelta(days=1)).isoformat()


def period_deadline(period: str) -> str:
    """The day every 13F-HR for a quarter end (YYYY-MM-DD) is due: 45 days after it, rolled to a business day."""
    return roll(date.fromisoformat(period) + timedelta(days=FILING_DEADLINE_DAYS)).isoformat()


def expected_period(today: date) -> str:
    """The latest quarter end whose 13F deadline fell at least SETTLE_DAYS days before today: the period a reader
    takes when none is given."""
    quarter = (today.month - 1) // 3
    period = prior_period(date(today.year + quarter // 3, (3 * quarter + 3) % 12 + 1, 1).isoformat())
    while date.fromisoformat(period_deadline(period)) + timedelta(days=SETTLE_DAYS) > today:
        period = prior_period(period)
    return period
