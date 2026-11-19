"""Calendar arithmetic the filing calendar is built from: month ends, months added, weekdays of a month."""
from __future__ import annotations

import calendar
from datetime import date, timedelta

__all__ = ["add_months", "is_month_end", "last_weekday", "month_end", "nearest_weekday", "nth_weekday", "on",
           "parse_mmdd"]


def month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def is_month_end(day: date) -> bool:
    return day.day == calendar.monthrange(day.year, day.month)[1]


def on(year: int, month: int, day: int) -> date:
    """The day of a month, held to its last day (February 30 is February 28 or 29)."""
    return date(year, month, min(day, calendar.monthrange(year, month)[1]))


def add_months(day: date, months: int) -> date:
    """The same day months later; a month end stays a month end (June 30 plus 4 months is October 31)."""
    m = day.month - 1 + months
    year, month = day.year + m // 12, m % 12 + 1
    return month_end(year, month) if is_month_end(day) else on(year, month, day.day)


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th weekday (0 is Monday) of a month; n=-1 is the last."""
    if n < 0:
        last = month_end(year, month)
        return last - timedelta(days=(last.weekday() - weekday) % 7)
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def last_weekday(year: int, month: int, weekday: int) -> date:
    return nth_weekday(year, month, weekday, -1)


def nearest_weekday(year: int, month: int, weekday: int) -> date:
    """The weekday nearest the last day of a month, which can fall in the first days of the next."""
    end = month_end(year, month)
    back = (end.weekday() - weekday) % 7
    return end - timedelta(days=back) if back <= 3 else end + timedelta(days=7 - back)


def parse_mmdd(value: str | None) -> tuple[int, int] | None:
    """(month, day) from EDGAR's fiscal year end \"MMDD\"; None when it is missing or malformed."""
    text = (value or "").strip()
    if len(text) != 4 or not text.isdigit():
        return None
    month, day = int(text[:2]), int(text[2:])
    return (month, day) if 1 <= month <= 12 and 1 <= day <= calendar.monthrange(2000, month)[1] else None
