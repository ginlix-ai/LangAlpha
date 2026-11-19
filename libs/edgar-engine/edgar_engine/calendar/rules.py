"""Periodic reports and their statutory due dates, as data.

PERIODIC_FORMS says what each periodic report is: the form names it is filed under, whether it is annual, the
quarterly report its filers also owe, and its Rule 12b-25 notice and extension. DEADLINE_RULES holds one row per rule
in force for a form: the filer categories it binds, the fiscal years it covers (by the end of the fiscal year the
report covers, or for a quarterly report the fiscal year the quarter belongs to, which is how the SEC phased the
accelerated deadlines in), the period it allows and its source. A new form or rule is one more row; nothing else
tests a form's name.

Due date: the period end plus the rule's days or months, rolled to the next business day when it is not one (Rule
0-3). A timely Rule 12b-25 notice extends an annual report by 15 calendar days and a quarterly report by 5, counted
from that rolled due date and rolled again (staff interpretations 135.03 and 135.04). A 40-F is due when its home
jurisdiction's annual filing is (Canada: 140 days, then 90 for non-venture and 120 for venture issuers under NI 51-102
from 2004) and has no 12b-25 extension.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal, cast

from .business_days import next_business_day, roll
from .dates import add_months

__all__ = ["ASSET_BACKED", "DEADLINE_RULES", "EARNINGS_RELEASE", "PERIODIC_FORMS", "TERMINATIONS", "DeadlineRule",
           "FilerCategory", "PeriodicForm", "annual_of", "deadline", "deadline_rule", "extended_deadline",
           "latest_deadline", "normalize_category", "notice_is_timely", "periodic_form"]

FilerCategory = Literal["large_accelerated", "accelerated", "non_accelerated"]
"""The category that sets a domestic filer's deadlines. A smaller reporting company is non-accelerated unless it is
also an accelerated filer (\"smaller reporting accelerated filer\"); a small business issuer filing 10-KSB was
non-accelerated. With none known, the non-accelerated deadlines apply, the latest any domestic filer may use."""

ALL: frozenset[FilerCategory] = frozenset({"large_accelerated", "accelerated", "non_accelerated"})
_ACCELERATED: frozenset[FilerCategory] = frozenset({"large_accelerated", "accelerated"})


@dataclass(frozen=True)
class PeriodicForm:
    """One periodic report. names: every form it is filed under; small_business: those always on the
    non-accelerated deadline; transition: those covering the transition period of a change of fiscal year, which ends
    on a year end of the new calendar; companion: the quarterly report a filer of this annual report also owes;
    notices: the Rule 12b-25 forms that extend it by extension_days, none when nothing extends it. Older EDGAR
    submission types drop the hyphens (10KSB, NTN 10K)."""
    family: str
    names: frozenset[str]
    annual: bool
    companion: str | None = None
    notices: frozenset[str] = frozenset()
    extension_days: int | None = None
    small_business: frozenset[str] = frozenset()
    transition: frozenset[str] = frozenset()


PERIODIC_FORMS: tuple[PeriodicForm, ...] = (
    PeriodicForm("10-K", frozenset({"10-K", "10-K405", "10-KSB", "10-KSB40", "10KSB", "10KSB40", "10-KT", "10-KT405"}),
                 annual=True, companion="10-Q", notices=frozenset({"NT 10-K", "NTN 10K"}), extension_days=15,
                 small_business=frozenset({"10-KSB", "10-KSB40", "10KSB", "10KSB40"}),
                 transition=frozenset({"10-KT", "10-KT405"})),
    PeriodicForm("10-Q", frozenset({"10-Q", "10-QSB", "10QSB", "10-QT"}), annual=False,
                 notices=frozenset({"NT 10-Q", "NTN 10Q"}), extension_days=5, small_business=frozenset({"10-QSB", "10QSB"}),
                 transition=frozenset({"10-QT"})),
    PeriodicForm("20-F", frozenset({"20-F"}), annual=True, notices=frozenset({"NT 20-F"}), extension_days=15),
    PeriodicForm("40-F", frozenset({"40-F"}), annual=True),
)
_BY_NAME = {name: spec for spec in PERIODIC_FORMS for name in spec.names}

TERMINATIONS = frozenset({"15-12B", "15-12G", "15-15D", "15F-12B", "15F-12G", "15F-15D"})
"""Forms that end or suspend the duty to report for the class of securities under the file number they name."""
ASSET_BACKED = "10-D"
"""The distribution report of an asset-backed issuer, which files 10-Ks but no 10-Qs."""
EARNINGS_RELEASE = ("8-K", "2.02")
"""The current report and item that announce a period's results, which the periodic report almost never precedes."""


def periodic_form(form: str) -> PeriodicForm | None:
    """The periodic report a form is filed as, None for any other form; an amendment is None, since it has no due
    date of its own."""
    return _BY_NAME.get(form.upper())


def annual_of(spec: PeriodicForm) -> PeriodicForm:
    """The annual report a periodic report's filer owes: the report itself, or the one whose companion it is."""
    return spec if spec.annual else next(s for s in PERIODIC_FORMS if s.companion == spec.family)


@dataclass(frozen=True)
class DeadlineRule:
    family: str
    categories: frozenset[FilerCategory]
    days: int = 0
    months: int = 0
    fiscal_years_from: date | None = None
    fiscal_years_before: date | None = None
    latest_days: int | None = None
    source: str = ""

    def applies(self, category: FilerCategory, fiscal_year_end: date) -> bool:
        return (category in self.categories
                and (self.fiscal_years_from is None or fiscal_year_end >= self.fiscal_years_from)
                and (self.fiscal_years_before is None or fiscal_year_end < self.fiscal_years_before))

    def due(self, period_end: date) -> date:
        return add_months(period_end, self.months) if self.months else period_end + timedelta(days=self.days)


_D03, _D04, _D06 = date(2003, 12, 15), date(2004, 12, 15), date(2006, 12, 15)
_R8128 = "Release 33-8128 (2002): accelerated filer deadlines phased in"
_R8644 = "Release 33-8644 (2005): large accelerated filers, final deadlines"
_FORMS = "Form 10-K and 10-Q General Instructions A"

DEADLINE_RULES: tuple[DeadlineRule, ...] = (
    DeadlineRule("10-K", ALL - _ACCELERATED, days=90, source=_FORMS),
    DeadlineRule("10-K", _ACCELERATED, days=90, fiscal_years_before=_D03, source=_R8128),
    DeadlineRule("10-K", _ACCELERATED, days=75, fiscal_years_from=_D03, fiscal_years_before=_D06,
                 source="Releases 33-8128, 33-8507 and 33-8644: 75 days until the 60-day deadline"),
    DeadlineRule("10-K", frozenset({"accelerated"}), days=75, fiscal_years_from=_D06, source=_R8644),
    DeadlineRule("10-K", frozenset({"large_accelerated"}), days=60, fiscal_years_from=_D06, source=_R8644),
    DeadlineRule("10-Q", ALL - _ACCELERATED, days=45, source=_FORMS),
    DeadlineRule("10-Q", _ACCELERATED, days=45, fiscal_years_before=_D04, source=_R8128),
    DeadlineRule("10-Q", _ACCELERATED, days=40, fiscal_years_from=_D04,
                 source="Releases 33-8507 and 33-8644: 40 days for fiscal years ending on or after 2004-12-15"),
    DeadlineRule("20-F", ALL, months=6, fiscal_years_before=date(2011, 12, 15),
                 source="Form 20-F General Instruction A(b), before Release 33-8959"),
    DeadlineRule("20-F", ALL, months=4, fiscal_years_from=date(2011, 12, 15),
                 source="Release 33-8959 (2008): four months for fiscal years ending on or after 2011-12-15"),
    DeadlineRule("40-F", ALL, days=140, fiscal_years_before=date(2004, 12, 31),
                 source="Form 40-F General Instruction D: due with the Canadian annual filing, then 140 days"),
    DeadlineRule("40-F", ALL, days=90, fiscal_years_from=date(2004, 12, 31), latest_days=120,
                 source="Form 40-F General Instruction D with NI 51-102 (years beginning 2004 on): 90 days, 120 for "
                        "venture issuers"),
)


def deadline_rule(form: str, category: str | None, fiscal_year_end: date) -> DeadlineRule | None:
    """The rule in force for a report. category is the filer category in any spelling normalize_category reads; an
    unknown one reads as non-accelerated, and a small business form always is. None for a transition report (10-KT,
    10-QT): Rules 13a-10 and 15d-10 count its due date from the later of the transition period's end and the date the
    change of fiscal year was decided, which the filing list does not hold."""
    spec = periodic_form(form)
    if spec is None or form.upper() in spec.transition:
        return None
    cat = normalize_category(category) or "non_accelerated"
    if form.upper() in spec.small_business:
        cat = "non_accelerated"
    return next((r for r in DEADLINE_RULES if r.family == spec.family and r.applies(cat, fiscal_year_end)), None)


def deadline(form: str, period_end: date, category: str | None, fiscal_year_end: date | None = None) -> date | None:
    """The due date of a report for the period ending period_end, rolled to a business day. fiscal_year_end is the
    end of the fiscal year a quarter belongs to (the period end itself for an annual report); it only matters for
    quarters in the 2003 to 2006 phase-in."""
    rule = deadline_rule(form, category, fiscal_year_end or period_end)
    return roll(rule.due(period_end)) if rule else None


def extended_deadline(form: str, period_end: date, category: str | None,
                      fiscal_year_end: date | None = None) -> date | None:
    """The due date with a timely Rule 12b-25 notice; None for a form nothing extends (40-F)."""
    spec, due = periodic_form(form), deadline(form, period_end, category, fiscal_year_end)
    if spec is None or spec.extension_days is None or due is None:
        return None
    return roll(due + timedelta(days=spec.extension_days))


def notice_is_timely(notice_filed: date, due: date) -> bool:
    """A Rule 12b-25 notice counts when filed by the business day after the (rolled) due date."""
    return notice_filed <= next_business_day(due)


def latest_deadline(form: str, period_end: date, category: str | None, fiscal_year_end: date | None = None, *,
                    extended: bool = False) -> date | None:
    """The latest the report can still be on time, given what EDGAR does not tell in advance: an annual report's
    category is set at the year end it covers and can be any lower one than the last one stated (a filer whose float
    or revenue drops can go straight to non-accelerated), and a 40-F filer may be a venture issuer with a longer home
    deadline. extended: a timely Rule 12b-25 notice is on file, which extends that date too. A report filed after
    this is late whatever is unknown."""
    spec = periodic_form(form)
    if spec is None:
        return None
    rule = deadline_rule(form, "non_accelerated" if spec.annual else category, fiscal_year_end or period_end)
    if rule is None:
        return None
    latest = roll(period_end + timedelta(days=rule.latest_days)) if rule.latest_days else roll(rule.due(period_end))
    if extended and spec.extension_days is not None:
        latest = roll(latest + timedelta(days=spec.extension_days))
    return latest


_CODES: dict[str, FilerCategory] = {"1-LAF": "large_accelerated", "2-ACC": "accelerated", "3-SRA": "accelerated",
                                    "4-NON": "non_accelerated", "5-SML": "non_accelerated"}


def normalize_category(value: str | None) -> FilerCategory | None:
    """The deadline category from any way EDGAR states it: XBRL dei:EntityFilerCategory (\"Large Accelerated
    Filer\", \"Non-accelerated Filer\", \"Smaller Reporting Company\", \"Smaller Reporting Accelerated Filer\"), the
    Financial Statement Data Sets' afs code (\"1-LAF\" to \"5-SML\"), the category line of EDGAR's submissions
    (\"Large accelerated filer<br>Well-known Seasoned Issuer\"), or this module's own names. None when it names none."""
    if not value:
        return None
    text = value.strip()
    if text in ALL:
        return cast(FilerCategory, text)
    if text.upper() in _CODES:
        return _CODES[text.upper()]
    text = re.sub(r"[\s_\-\u2010-\u2015]+", " ", re.split(r"<br\s*/?>", text, flags=re.IGNORECASE)[0]).lower()
    if "large accelerated" in text:
        return "large_accelerated"
    if "non accelerated" in text or "nonaccelerated" in text:
        return "non_accelerated"
    if "accelerated" in text:
        return "accelerated"
    if "smaller reporting" in text:
        return "non_accelerated"
    return None
