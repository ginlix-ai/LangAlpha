"""A filer's fiscal calendar, read from the period ends of the reports it has filed, and projected forward.

Most filers close their year and quarters on the last day of a month. Filers on a 52/53-week year close on a fixed
weekday: the last one of a month, or the one nearest the end of a month; a few close on a fixed date. The rule is
read from the filer's own year ends, never assumed. A 52/53-week quarter is 13 weeks after the period before it, and
the year end, which follows the rule, takes up the 53rd week when there is one. A month-end year's quarters end on the
last day of every third month from the year end, whatever day a filed quarter ended on. A projected period end is a
forecast: a report whose period ends within a few days of it covers it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

from .dates import (
    add_months,
    is_month_end,
    last_weekday,
    month_end,
    nearest_weekday,
    on,
)

__all__ = ["COVER_DAYS", "YEAR_END_GRACE_DAYS", "FiscalCalendar", "FiscalPeriod", "Period", "nominal_month",
           "same_year_end"]

FiscalPeriod = Literal["Q1", "Q2", "Q3", "FY"]
YearRule = Literal["month_end", "last_weekday", "nearest_weekday", "same_day"]
QUARTERS: tuple[FiscalPeriod, ...] = ("Q1", "Q2", "Q3")

YEAR_END_GRACE_DAYS = 7
"""A 52/53-week year end in the first days of a month closes the month before (a year ending January 2 is the
December year)."""
COVER_DAYS = 10
"""A report whose period ends this many days or fewer from a projected period end covers that period."""


@dataclass(frozen=True)
class Period:
    fiscal_period: FiscalPeriod
    end: date
    projected: bool


def nominal_month(day: date) -> tuple[int, int]:
    """The (year, month) a year end closes: a 52/53-week year end in the first days of a month closes the month
    before."""
    shifted = day - timedelta(days=YEAR_END_GRACE_DAYS)
    return (shifted.year, shifted.month) if day.day <= YEAR_END_GRACE_DAYS else (day.year, day.month)


def same_year_end(a: date, b: date) -> bool:
    """Whether two year ends follow one fiscal calendar: they close the same month, and lie within a week of each
    other measured from that month's end (a 52/53-week year end moves by up to six days; a move from December 31 to
    December 15 is a change of fiscal year end)."""
    def from_month_end(day: date) -> int:
        return (month_end(*nominal_month(day)) - day).days
    return nominal_month(a)[1] == nominal_month(b)[1] and abs(from_month_end(a) - from_month_end(b)) < 7


def _year_rule(ends: list[date]) -> YearRule:
    """The rule every year end follows, else the one all but one of three or more follow. A weekday rule must fit the
    weekday of the latest year end, the one it projects from, and one that fits every year end wins over month ends
    with an outlier. A tie between the two weekday rules goes to nearest_weekday: where they differ it is the later
    date, so an ambiguous history can only move a due date later, never raise a false overdue."""
    month_ends = sum(is_month_end(e) for e in ends)
    if month_ends == len(ends):
        return "month_end"
    if len(ends) > 1 and len({(e.month, e.day) for e in ends}) == 1:
        return "same_day"
    weekday = ends[-1].weekday()
    last = sum(last_weekday(*nominal_month(e), weekday) == e for e in ends)
    nearest = sum(nearest_weekday(*nominal_month(e), weekday) == e for e in ends)
    weekly: YearRule = "last_weekday" if last > nearest else "nearest_weekday"
    if max(last, nearest) == len(ends):
        return weekly
    if len(ends) > 2 and month_ends == len(ends) - 1:
        return "month_end"
    return weekly if max(last, nearest) >= max(1, len(ends) - 1) else "same_day"


def quarter_of(period_end: date, year_end_before: date) -> int:
    """The quarter (1 to 4) a period ending period_end is of the fiscal year after year_end_before."""
    return max(1, min(4, round((period_end - year_end_before).days / 91.3)))


@dataclass(frozen=True)
class FiscalCalendar:
    """annual_ends: the period ends of the filer's annual reports, oldest first. quarterly: whether it also owes
    quarterly reports. fiscal_year_end: the (month, day) EDGAR states, used when no annual report is on file."""
    annual_ends: tuple[date, ...]
    rule: YearRule
    quarterly: bool = True
    fiscal_year_end: tuple[int, int] | None = None

    @classmethod
    def infer(cls, annual_ends: tuple[date, ...], *, quarterly: bool = True,
              fiscal_year_end: tuple[int, int] | None = None) -> FiscalCalendar:
        """The calendar the filer's year ends show, read from the year ends since it last moved its year end; with none,
        the stated fiscal year end's: a month end, else that date every year."""
        ends = list(annual_ends)
        since = len(ends) - 1
        while since > 0 and same_year_end(ends[since - 1], ends[since]):
            since -= 1
        recent = ends[since:][-8:]
        if recent:
            rule = _year_rule(recent)
        else:
            stated_day = fiscal_year_end and fiscal_year_end[1] < month_end(2001, fiscal_year_end[0]).day
            rule = "same_day" if stated_day else "month_end"
        return cls(tuple(ends), rule, quarterly, fiscal_year_end)

    def year_end_after(self, year_end: date) -> date:
        if self.rule == "month_end":
            y, m = nominal_month(year_end)
            return month_end(y + 1, m)
        if self.rule == "same_day":
            return on(year_end.year + 1, year_end.month, year_end.day)
        y, m = nominal_month(year_end)
        place = last_weekday if self.rule == "last_weekday" else nearest_weekday
        return place(y + 1, m, year_end.weekday())

    def latest_year_end(self, before: date) -> date | None:
        """The last fiscal year end before before: a reported one rolled forward by the rule, or the stated
        fiscal year end when no annual report is on file."""
        ends = [e for e in self.annual_ends if e < before]
        if ends:
            latest = max(ends)
            while (nxt := self.year_end_after(latest)) < before:
                latest = nxt
            return latest
        if self.fiscal_year_end:
            month, day = self.fiscal_year_end
            end = on(before.year, month, day)
            return end if end < before else on(before.year - 1, month, day)
        return None

    def fiscal_year_end_of(self, period: Period) -> date:
        """The end of the fiscal year a period belongs to: the period's own end for a year."""
        if period.fiscal_period == "FY":
            return period.end
        start = self.latest_year_end(period.end)
        assert start is not None
        return self.year_end_after(start)

    @property
    def weekly(self) -> bool:
        return self.rule in ("last_weekday", "nearest_weekday")

    def latest_end(self, period: Period) -> date:
        """The latest a period can end: a projected 52/53-week quarter can be the 14-week one."""
        if period.projected and self.weekly and period.fiscal_period != "FY":
            return period.end + timedelta(weeks=1)
        return period.end

    def quarter_label(self, period_end: date) -> FiscalPeriod | None:
        """Q1 to Q3 for a quarterly report's period end, None when it cannot be placed."""
        start = self.latest_year_end(period_end - timedelta(days=COVER_DAYS * 2))
        if start is None:
            return None
        q = quarter_of(period_end, start)
        return QUARTERS[q - 1] if q <= 3 else None

    def _next(self, period: Period, year_end: date) -> Period:
        """The period after one, in a year ending year_end."""
        if period.fiscal_period == "Q3" or not self.quarterly:
            return Period("FY", year_end, True)
        index = 0 if period.fiscal_period == "FY" else QUARTERS.index(period.fiscal_period) + 1
        end = period.end + timedelta(weeks=13) if self.weekly else add_months(year_end, 3 * (index + 1) - 12)
        return Period(QUARTERS[index], end, True)

    def periods_after(self, last: Period, through: date) -> list[Period]:
        """The periods after last (the latest one reported) ending on or before through, then the first one ending
        later: what the filer is expected to report next."""
        start = self.latest_year_end(last.end + timedelta(days=1)) if last.fiscal_period != "FY" else last.end
        if start is None:
            return []
        year_end = self.year_end_after(start)
        out: list[Period] = []
        period = last
        while True:
            period = self._next(period, year_end)
            if period.fiscal_period == "FY":
                year_end = self.year_end_after(year_end)
            out.append(period)
            if period.end > through:
                return out
