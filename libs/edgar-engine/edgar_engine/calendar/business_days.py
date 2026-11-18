"""EDGAR business days: the days a report can be filed on, and the day a due date rolls to.

Exchange Act Rule 0-3 lets a report due on a Saturday, Sunday or federal holiday be filed on the next business
day. The federal holidays are the ones 5 U.S.C. 6103 names, observed on the Friday before when they fall on a
Saturday and the Monday after when they fall on a Sunday, plus the days the government was closed by executive
order or for a national day of mourning, listed in CLOSURES. A future closure is not known until it is announced,
so a due date computed today can move by a day when one is.
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import cache

from .dates import nth_weekday

__all__ = ["CLOSURES", "federal_holidays", "is_business_day", "nearest_business_day", "next_business_day", "roll"]

# Days the federal government was closed by executive order or for a national day of mourning, which the SEC treats
# as federal holidays for filing purposes. EDGAR's own filing dates agree: no filing, or a few percent of a normal
# day's, carries any of these dates. The one day EDGAR itself was shut, with filings due that day deemed timely the next
# business day, counts too. Relief granted to affected filers only (Hurricane Sandy, 2012) is not a closure.
CLOSURES: dict[date, str] = {
    date(1997, 12, 26): "Executive order closing federal agencies",
    date(2000, 11, 24): "EDGAR shut down for a system upgrade (SEC release 2000-148); filings due were deemed timely the next business day",
    date(2001, 12, 24): "Executive order closing federal agencies",
    date(2003, 12, 26): "Executive order closing federal agencies",
    date(2004, 6, 11): "National day of mourning (President Reagan)",
    date(2007, 1, 2): "National day of mourning (President Ford)",
    date(2007, 12, 24): "Executive order closing federal agencies",
    date(2008, 12, 26): "Executive order closing federal agencies",
    date(2012, 12, 24): "Executive order closing federal agencies",
    date(2014, 12, 26): "Executive order closing federal agencies",
    date(2018, 12, 5): "National day of mourning (President George H. W. Bush)",
    date(2018, 12, 24): "Executive order closing federal agencies",
    date(2019, 12, 24): "Executive order closing federal agencies",
    date(2020, 12, 24): "Executive order closing federal agencies",
    date(2024, 12, 24): "Executive order closing federal agencies",
    date(2025, 1, 9): "National day of mourning (President Carter)",
    date(2025, 12, 24): "Executive order closing federal agencies",
    date(2025, 12, 26): "Executive order closing federal agencies",
}


def _observed(day: date) -> date:
    return day - timedelta(days=1) if day.weekday() == 5 else day + timedelta(days=1) if day.weekday() == 6 else day


@cache
def federal_holidays(year: int) -> frozenset[date]:
    """The federal holidays observed in a year under the rules in force that year. New Year's Day of the next year
    is observed on December 31 when it falls on a Saturday, so it is counted in this year."""
    days = {
        _observed(date(year, 1, 1)),
        nth_weekday(year, 2, 0, 3),
        nth_weekday(year, 5, 0, -1),
        _observed(date(year, 7, 4)),
        nth_weekday(year, 9, 0, 1),
        nth_weekday(year, 10, 0, 2),
        nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
        _observed(date(year, 11, 11)) if year >= 1978 else nth_weekday(year, 10, 0, 4),
    }
    if year >= 1986:
        days.add(nth_weekday(year, 1, 0, 3))
    if year >= 2021:
        days.add(_observed(date(year, 6, 19)))
    if date(year + 1, 1, 1).weekday() == 5:
        days.add(date(year, 12, 31))
    return frozenset(d for d in days if d.year == year)


def is_business_day(day: date) -> bool:
    return day.weekday() < 5 and day not in federal_holidays(day.year) and day not in CLOSURES


def next_business_day(day: date) -> date:
    """The first business day after day."""
    day += timedelta(days=1)
    while not is_business_day(day):
        day += timedelta(days=1)
    return day


def roll(day: date) -> date:
    """day when it is a business day, else the next one (Rule 0-3)."""
    return day if is_business_day(day) else next_business_day(day)


def nearest_business_day(day: date) -> date:
    """The business day nearest day, the earlier one on a tie: an expected filing day can only be a business day."""
    back, ahead = day, day
    while True:
        if is_business_day(back):
            return back
        if is_business_day(ahead):
            return ahead
        back, ahead = back - timedelta(days=1), ahead + timedelta(days=1)
