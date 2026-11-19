"""When a filer's next periodic report is expected and due, and whether its latest one is current.

Pure functions over one filer's filing list. The expected date is the filer's own habit: the number of days after
period end it filed the same period a year earlier, never past the due date under the current category when that habit
was within it. Before a habit exists, it is the due date less the median slack of filers of its category. Two later signals change it: an
earnings release (an 8-K with Item 2.02) for the period, which the report almost never precedes, so the expected date
moves to the release when the habit put it earlier; and a timely notice of late filing (NT 10-K, NT 10-Q, NT 20-F),
after which the report is expected on the extended due date.

The window around the expected date and the population figures were measured on the development split of the
filing-calendar benchmark; see CALIBRATION.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Any, Literal

from .business_days import nearest_business_day
from .dates import add_months, on, parse_mmdd
from .fiscal import (
    COVER_DAYS,
    FiscalCalendar,
    FiscalPeriod,
    Period,
    nominal_month,
    same_year_end,
)
from .rules import (
    ASSET_BACKED,
    EARNINGS_RELEASE,
    TERMINATIONS,
    FilerCategory,
    PeriodicForm,
    annual_of,
    deadline,
    extended_deadline,
    latest_deadline,
    normalize_category,
    notice_is_timely,
    periodic_form,
)

__all__ = ["CALIBRATION", "Calibration", "ExpectedFiling", "Filing", "ReportCurrency", "expected_filings",
           "filings_from_rows", "report_currency"]

Basis = Literal["history", "category", "earnings_release", "late_notice"]
Verdict = Literal["current", "expected", "extended", "overdue", "deregistered", "unknown"]

HISTORY_YEARS = 2
"""A filer that filed a 10-D and no 10-Q in this many years is an asset-backed issuer, which owes no quarterly
reports; an operating company co-files the 10-Ds of a securitization it sponsors and keeps filing its 10-Qs."""
RELEASE_WINDOW_DAYS = 120
"""An earnings release counts for a period when filed this many days or fewer after its end."""
GAP_LOOKBACK_DAYS = 400
"""A period with no report this many days or fewer before the latest one is owed too (a filer can file a later report
without curing an earlier one); a gap further back is history, not an owed report."""
HABIT_GRACE_DAYS = 3
"""A habit no more than this many days past the due date counts as on time, so the expected date is held to it."""


@dataclass(frozen=True)
class Calibration:
    """Population figures from the benchmark's development split: per basis, the days from the expected date to
    the actual filing that bound the middle 80% of outcomes (the window); per form and category, the days before
    the due date a filer with no history of its own files at the median (absent: on the due date); and the days
    from the extended due date to the filing after a notice of late filing, at the median."""
    window: Mapping[Basis, tuple[int, int]]
    slack: Mapping[tuple[str, FilerCategory], int]
    notice_gap: int
    source: str


CALIBRATION = Calibration(
    window={"history": (-7, 6), "category": (-14, 6), "earnings_release": (-6, 4), "late_notice": (-7, 107)},
    slack={("10-K", "non_accelerated"): 12, ("10-Q", "non_accelerated"): 1},
    notice_gap=0,
    source="filing-calendar benchmark, development split, frozen 2026-11-17",
)


@dataclass(frozen=True)
class Filing:
    form: str
    filing_date: date
    report_date: date | None = None
    accession: str | None = None
    items: tuple[str, ...] = ()


def _day(value: Any) -> date | None:
    if value is None or str(value) in ("", "nan", "NaT"):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def filings_from_rows(rows: Iterable[Mapping[str, Any]]) -> list[Filing]:
    """Filings from rows of edgartools' filings table (form, filing_date, reportDate, accession_number, items) or of
    EDGAR's submissions JSON (form, filingDate, reportDate, accessionNumber, items)."""
    out = []
    for r in rows:
        filed = _day(r.get("filing_date") or r.get("filingDate"))
        if filed is None or not r.get("form"):
            continue
        items = r.get("items")
        if isinstance(items, str):
            items = tuple(i.strip() for i in items.split(",") if i.strip())
        elif not isinstance(items, (list, tuple)):
            items = ()
        out.append(Filing(str(r["form"]), filed, _day(r.get("reportDate") or r.get("report_date")),
                          r.get("accession_number") or r.get("accessionNumber"), tuple(items)))
    return out


@dataclass(frozen=True)
class ExpectedFiling:
    """One report the filer owes or will owe. expected is the likeliest filing day and window the days that held
    80% of outcomes on the benchmark for this basis; deadline is the statutory due date (rolled to a business day),
    extended_deadline the date a Rule 12b-25 notice moves it to (None for a 40-F), latest_deadline the latest it can
    be on time given what EDGAR does not tell in advance (a category drop at the year end, a 40-F venture issuer, a
    14-week quarter) and the timely notice when there is one;
    notice and release are the accessions of a timely notice of late filing and of the earnings release the expected
    date waits for. history holds the filer's lags (days from period end to filing) the expectation was read from,
    newest first. category is the filer category the dates use: None for a period whose category was set at a year
    end before the latest annual report's (a quarter's at the year end before it, an annual report's at its own),
    which the filing list does not hold, so the non-accelerated deadlines apply."""
    form: str
    fiscal_period: FiscalPeriod
    period_end: date
    period_end_projected: bool
    expected: date
    window: tuple[date, date]
    deadline: date
    extended_deadline: date | None
    latest_deadline: date
    category: FilerCategory | None
    basis: Basis
    history: tuple[int, ...] = ()
    notice: str | None = None
    release: str | None = None


@dataclass(frozen=True)
class ReportCurrency:
    """Whether the latest periodic report is current on as_of. verdict: current (the next report is not due yet and
    not expected yet), expected (inside the next report's window, before it can be late), extended (past the due date
    with a timely notice of late filing, before the extended date; the filing list does not say whether the notice
    claims the extension in its Part II, which nearly every timely notice does), overdue (past every date it could
    still be on time), deregistered (a Form 15 or 15F is on file and the next report is overdue, or a year and an
    annual report's latest due date have passed since the latest period, which wins over unknown), unknown (the list
    has no periodic report, the fiscal calendar cannot be read, or the filer's stated fiscal year end disagrees with
    the calendar its reports follow, as during a change of year end before its transition report; reason says which).
    termination is the accession of a Form 15 or 15F filed after the latest report's period end: it can retire one
    class of securities while the filer keeps reporting, so it ends the duty to report only once a report goes
    unfiled. owed holds every report owed since the latest one, then the next period's."""
    as_of: date
    latest: Filing | None
    next: ExpectedFiling | None
    verdict: Verdict
    owed: tuple[ExpectedFiling, ...] = field(default_factory=tuple)
    termination: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class _Report:
    filing: Filing
    period: Period
    transition: bool = False


@dataclass(frozen=True)
class FilerHistory:
    """What a filer's filings on file at as_of say: its original periodic reports with their fiscal periods, oldest
    first, its fiscal calendar and annual report, and the filings that move an expectation."""
    as_of: date
    filings: tuple[Filing, ...]
    reports: tuple[_Report, ...]
    calendar: FiscalCalendar | None
    annual: PeriodicForm | None
    category: FilerCategory | None
    year_end_changed: bool

    @property
    def latest(self) -> _Report | None:
        return max(self.reports, key=lambda r: (r.period.end, r.filing.filing_date), default=None)

    def lags(self, period: FiscalPeriod) -> tuple[int, ...]:
        """The filer's lags for the same fiscal period, newest first, else for any period of its kind, from the reports
        of the forms it files now; a transition report's lag says nothing of the filer's habit."""
        def lag(r: _Report) -> int:
            return (r.filing.filing_date - r.period.end).days
        newest = sorted((r for r in self.active_reports() if not r.transition), key=lambda r: r.period.end, reverse=True)
        same = [lag(r) for r in newest if r.period.fiscal_period == period][:3]
        kind = [lag(r) for r in newest if (r.period.fiscal_period == "FY") == (period == "FY")][:4]
        return tuple(same or kind)

    def active_reports(self) -> list[_Report]:
        """The reports filed under the forms the filer files now, since it last filed under others (a filer that moved
        from 20-F to 10-K and 10-Q leaves its 20-Fs behind, and its 10-Ks from before them)."""
        if self.annual is None:
            return []
        current = [r for r in self.reports if annual_of(periodic_form(r.filing.form)) == self.annual]
        other = max((r.period.end for r in self.reports if r not in current), default=date.min)
        return [r for r in current if r.period.end >= other]

    def notice_for(self, form: str, period_end: date) -> Filing | None:
        spec = periodic_form(form)
        wanted = spec.notices if spec else frozenset()
        hits = [f for f in self.filings if f.form.upper() in wanted and f.filing_date > period_end
                and (f.report_date is None or abs((f.report_date - period_end).days) <= COVER_DAYS)]
        return min(hits, key=lambda f: f.filing_date, default=None)

    def release_after(self, period_end: date) -> Filing | None:
        form, item = EARNINGS_RELEASE
        hits = [f for f in self.filings if f.form.upper() == form and item in f.items
                and period_end < f.filing_date <= period_end + timedelta(days=RELEASE_WINDOW_DAYS)]
        return min(hits, key=lambda f: f.filing_date, default=None)

    def termination(self, period_end: date) -> Filing | None:
        """The latest Form 15 or 15F filed after the period of the latest report: a filer going dark often files it
        before its last report."""
        hits = [f for f in self.filings if f.form.upper() in TERMINATIONS and f.filing_date > period_end]
        return max(hits, key=lambda f: f.filing_date, default=None)


@dataclass(frozen=True)
class _Originals:
    """Each period's first original periodic report of each form family (a filer moving from 20-F to 10-K can file
    both for one year): annual ones by period end, quarterly ones, transition reports
    for a quarter (whose period end is a year end of the new calendar), and reports with no period of report."""
    annual: list[Filing]
    quarterly: list[Filing]
    transition_quarters: list[Filing]
    undated: list[Filing]


def _originals(on_file: tuple[Filing, ...]) -> _Originals:
    first: dict[tuple[str, date], Filing] = {}
    undated: list[Filing] = []
    for f in on_file:
        spec = periodic_form(f.form)
        if spec is None:
            continue
        if f.report_date is None:
            undated.append(f)
        else:
            first.setdefault((spec.family, f.report_date), f)
    annual = sorted((f for f in first.values() if periodic_form(f.form).annual), key=lambda f: f.report_date)
    quarters = [f for f in first.values() if not periodic_form(f.form).annual]
    transition = [f for f in quarters if _is_transition(f)]
    return _Originals(annual, [f for f in quarters if f not in transition], transition, undated)


def _is_transition(f: Filing) -> bool:
    spec = periodic_form(f.form)
    return spec is not None and f.form.upper() in spec.transition


def _gap_anchor(reports: Iterable[_Report]) -> tuple[Period, date] | None:
    """Where missing reports are looked for: the period to project from and the first day a missing period can end
    on, GAP_LOOKBACK_DAYS before the latest report. The anchor is the last report before that day, else the earliest
    after it, and never one before the latest transition report. A transition report for a year end starts the new
    calendar; one for a quarter inside a longer transition period is no period of either calendar. A filer's first
    report anchors even when it is a quarter: a new registrant's first 10-Q follows the periods its registration
    statement covered, so the quarters before it are not owed."""
    reports = list(reports)
    latest = max((r.period.end for r in reports), default=None)
    if latest is None:
        return None
    since = latest - timedelta(days=GAP_LOOKBACK_DAYS)
    moved = max((r.period.end for r in reports if r.transition), default=date.min)
    anchors = [r.period for r in reports if r.period.end >= moved and not (r.transition and r.period.fiscal_period != "FY")]
    before = [p for p in anchors if p.end < since]
    anchor = max(before, key=lambda p: p.end) if before else min(anchors, key=lambda p: p.end, default=None)
    return (anchor, since) if anchor else None


def _uncovered(calendar: FiscalCalendar, after: Period, through: date, reports: Iterable[_Report],
               ends: Iterable[date] = ()) -> list[Period]:
    """The periods after one through a day, then the next, that no report covers: none ends within COVER_DAYS of it,
    and none is the same fiscal period of the same fiscal year (the first quarter after a change of year end can be
    reported for a period of the old calendar). A 52/53-week quarter reported to end on another week moves the
    periods after it."""
    def label(p: Period) -> tuple[date, FiscalPeriod] | None:
        if p.fiscal_period == "FY":
            return p.end, "FY"
        start = calendar.latest_year_end(p.end)
        return (calendar.year_end_after(start), p.fiscal_period) if start else None
    reports = list(reports)
    near = [r.period.end for r in reports] + list(ends)
    labels = {label(r.period) for r in reports} - {None}
    out: list[Period] = []
    start = after
    while True:
        for p in calendar.periods_after(start, through):
            hits = [end for end in near if abs((p.end - end).days) <= COVER_DAYS]
            if calendar.weekly and p.fiscal_period != "FY" and hits and p.end not in hits:
                start = replace(p, end=min(hits, key=lambda end: abs((p.end - end).days)), projected=False)
                break
            if not hits and label(p) not in labels:
                out.append(p)
        else:
            return out


def _place_undated(calendar: FiscalCalendar, reports: list[_Report], undated: list[Filing]) -> list[_Report]:
    """Reports with no period of report: an annual one covers the last fiscal year end before it was filed; a
    quarterly one, the first quarter owed when it was filed."""
    placed = list(reports)
    for f in undated:
        spec = periodic_form(f.form)
        assert spec is not None
        if spec.annual:
            end = calendar.latest_year_end(f.filing_date)
            if end and not any(abs((end - r.period.end).days) <= COVER_DAYS
                               and periodic_form(r.filing.form).family == spec.family for r in placed):
                placed.append(_Report(f, Period("FY", end, False), _is_transition(f)))
            continue
        prior = [r for r in placed if r.filing.filing_date <= f.filing_date and r.filing is not f]
        if not prior:
            continue
        anchor, since = _gap_anchor(prior) or (max(prior, key=lambda r: r.period.end).period, date.min)
        owed = [p for p in _uncovered(calendar, anchor, f.filing_date, placed)
                if since <= p.end < f.filing_date and p.fiscal_period != "FY"]
        if owed:
            placed.append(_Report(f, replace(owed[0], projected=False)))
    return placed


def history_of(filings: Iterable[Filing], as_of: date, category: str | None = None,
               fiscal_year_end: str | None = None) -> FilerHistory:
    """Read a filer's filings on file at as_of. fiscal_year_end is EDGAR's "MMDD" as of as_of.

    A quarterly transition report (10-QT) ends on a year end of the new calendar when it ends in the month the
    stated fiscal year end closes; otherwise it covers a quarter inside a longer transition period, and the fiscal
    year end is changing until the next annual report. A 10-QT with no period of report could be either, so it too
    leaves the fiscal year end changing until the next annual report.

    A report is filed after the period it covers, so a period of report on or after the filing date, which EDGAR
    records for some filings, names no period: the filing reads as undated."""
    on_file = tuple(sorted((replace(f, report_date=None) if f.report_date and f.report_date >= f.filing_date else f
                            for f in filings if f.filing_date <= as_of), key=lambda f: f.filing_date))
    o = _originals(on_file)
    stated = parse_mmdd(fiscal_year_end)
    stated_month = nominal_month(on(2000, *stated))[1] if stated else None
    year_end_quarters = [f for f in o.transition_quarters if nominal_month(f.report_date)[1] == stated_month]
    quarterly = o.quarterly + [f for f in o.transition_quarters if f not in year_end_quarters]
    undated_quarters = [f for f in o.undated if not periodic_form(f.form).annual]
    dated_ends = tuple(sorted({f.report_date for f in o.annual}))
    known = FiscalCalendar.infer(dated_ends, fiscal_year_end=stated) if dated_ends or stated else None

    def period_of(f: Filing) -> date:
        if f.report_date or known is None or not periodic_form(f.form).annual:
            return f.report_date or f.filing_date
        return known.latest_year_end(f.filing_date) or f.filing_date
    newest = max((f for f in on_file if periodic_form(f.form)), key=lambda f: (period_of(f), f.filing_date), default=None)
    annual = annual_of(periodic_form(newest.form)) if newest else None
    asset_backed = any(f.form.upper() == ASSET_BACKED and f.filing_date > as_of - timedelta(days=365 * HISTORY_YEARS)
                       for f in on_file)
    files_quarters = any(f.filing_date > as_of - timedelta(days=365 * HISTORY_YEARS) for f in quarterly + undated_quarters)
    owes_quarters = bool(annual and annual.companion and (files_quarters or not asset_backed))
    ends = tuple(sorted({f.report_date for f in o.annual + year_end_quarters}))
    calendar = (FiscalCalendar.infer(ends, quarterly=owes_quarters, fiscal_year_end=stated)
                if annual and (ends or stated) else None)
    reports = [_Report(f, Period("FY", f.report_date, False), _is_transition(f)) for f in o.annual]
    reports += [_Report(f, Period("FY", f.report_date, False), True) for f in year_end_quarters
                if not any(abs((f.report_date - r.period.end).days) <= COVER_DAYS for r in reports)]
    if calendar is not None:
        for f in quarterly:
            label = calendar.quarter_label(f.report_date)
            if label:
                reports.append(_Report(f, Period(label, f.report_date, False), _is_transition(f)))
        reports = _place_undated(calendar, reports, o.undated)
    last_annual = max(o.annual, key=lambda f: f.report_date, default=None)

    def after_last_annual(f: Filing) -> bool:
        if last_annual is None:
            return True
        return f.report_date > last_annual.report_date if f.report_date else f.filing_date > last_annual.filing_date
    mid_transition = any(_is_transition(f) and after_last_annual(f) for f in quarterly + undated_quarters)
    stated_end = on(ends[-1].year, *stated) if stated and ends else None
    fixed_date = (calendar is not None and calendar.rule == "same_day"
                  or len(ends) > 1 and (ends[-2].month, ends[-2].day) == (ends[-1].month, ends[-1].day))
    changed = bool(stated_end and (not same_year_end(stated_end, ends[-1]) or fixed_date and stated_end != ends[-1]))
    changed = changed or mid_transition
    return FilerHistory(as_of, on_file, tuple(sorted(reports, key=lambda r: r.period.end)), calendar, annual,
                        normalize_category(category), changed)


def expect(history: FilerHistory, period: Period, calibration: Calibration = CALIBRATION) -> ExpectedFiling:
    """What the filer is expected to file for one period, and when."""
    calendar, annual = history.calendar, history.annual
    assert calendar is not None and annual is not None
    form = annual.family if period.fiscal_period == "FY" else annual.companion
    assert form is not None
    active = history.active_reports()
    last_annual = max((r for r in active if r.period.fiscal_period == "FY"), key=lambda r: r.period.end, default=None)
    last_quarter = max((r for r in active if r.period.fiscal_period != "FY"), key=lambda r: r.period.end, default=None)
    if period.fiscal_period == "FY":
        filed_as = last_annual.filing.form.upper() if last_annual else None
    elif last_annual and (last_quarter is None or last_annual.period.end > last_quarter.period.end):
        filed_as = form + "SB" if last_annual.filing.form.upper() in annual.small_business else None
    else:
        filed_as = last_quarter.filing.form.upper() if last_quarter else None
    if filed_as in periodic_form(form).small_business:
        form = filed_as
    notice = history.notice_for(form, period.end)
    if notice is not None and notice.report_date is not None:
        named = min(notice.report_date, calendar.latest_end(period))
        period = replace(period, end=named, projected=False)
    fy_end = calendar.fiscal_year_end_of(period)
    set_at = period.end if period.fiscal_period == "FY" else calendar.latest_year_end(period.end)
    last_annual = max((r.period.end for r in history.reports if r.period.fiscal_period == "FY"), default=None)
    cat = None if set_at and last_annual and set_at < last_annual else history.category
    due = deadline(form, period.end, cat, fy_end)
    extended = extended_deadline(form, period.end, cat, fy_end)
    latest_end = calendar.latest_end(period)
    latest = latest_deadline(form, latest_end, cat, fy_end)
    assert due is not None and latest is not None
    if notice is not None and not notice_is_timely(notice.filing_date, latest):
        notice = None
    if notice is not None:
        latest = latest_deadline(form, latest_end, cat, fy_end, extended=True)
        assert latest is not None
    release = history.release_after(period.end)
    lags = history.lags(period.fiscal_period)
    if lags:
        basis: Basis = "history"
        expected = period.end + timedelta(days=lags[0])
        if expected > due and lags[0] <= (due - period.end).days + HABIT_GRACE_DAYS:
            expected = due
    else:
        basis = "category"
        expected = due - timedelta(days=calibration.slack.get((form, cat or "non_accelerated"), 0))
    if notice is not None and extended is not None:
        basis = "late_notice"
        moved_to = extended if notice_is_timely(notice.filing_date, due) else latest
        expected = moved_to + timedelta(days=calibration.notice_gap)
    elif release is not None:
        basis = "earnings_release"
        expected = max(expected, release.filing_date)
    expected = nearest_business_day(expected)
    lo, hi = calibration.window[basis]
    return ExpectedFiling(
        form=form, fiscal_period=period.fiscal_period, period_end=period.end, period_end_projected=period.projected,
        expected=expected, window=(nearest_business_day(expected + timedelta(days=lo)),
                                   nearest_business_day(expected + timedelta(days=hi))),
        deadline=due, extended_deadline=extended, latest_deadline=latest, category=cat, basis=basis, history=lags,
        notice=notice.accession if notice else None, release=release.accession if release else None)


def _owed(history: FilerHistory, calibration: Calibration) -> list[ExpectedFiling]:
    latest, calendar = history.latest, history.calendar
    if calendar is None:
        return []
    if latest is None:
        start = calendar.latest_year_end(history.as_of)
        if start is None:
            return []
        last, since = Period("FY", start, True), date.min
    else:
        last, since = _gap_anchor(history.active_reports()) or (latest.period, date.min)
    ends = [f.report_date for f in history.filings if f.report_date and periodic_form(f.form)]

    def after(period: Period) -> list[Period]:
        return [p for p in _uncovered(calendar, period, history.as_of, history.reports, ends) if p.end >= since]
    out: list[ExpectedFiling] = []
    periods = after(last)
    while periods:
        period = periods.pop(0)
        out.append(expect(history, period, calibration))
        if out[-1].period_end != period.end:
            periods = after(replace(period, end=out[-1].period_end, projected=False))
    return out


def expected_filings(filings: Iterable[Filing], *, as_of: date, category: str | None = None,
                     fiscal_year_end: str | None = None,
                     calibration: Calibration = CALIBRATION) -> list[ExpectedFiling]:
    """The reports a filer owes on as_of: every period that has ended since its latest report without one, oldest
    first, then the next period to end. Empty when the fiscal calendar cannot be read (no periodic report and no
    fiscal_year_end). Pass the filer's whole filing list: a list cut to recent filings can lose the annual report the
    calendar is read from. category is the filer category as EDGAR states it anywhere (normalize_category); with
    none, the non-accelerated deadlines apply. fiscal_year_end is EDGAR's \"MMDD\" as of as_of."""
    return _owed(history_of(filings, as_of, category, fiscal_year_end), calibration)


def _dark_after(annual: PeriodicForm, period_end: date) -> date:
    """The day after which a filer with a Form 15 on file has gone dark whatever its calendar says: a year and an
    annual report's latest due date past the period of its latest report."""
    due = latest_deadline(annual.family, add_months(period_end, 12), None)
    assert due is not None
    return due


def report_currency(filings: Iterable[Filing], *, as_of: date, category: str | None = None,
                    fiscal_year_end: str | None = None, calibration: Calibration = CALIBRATION) -> ReportCurrency:
    """Whether the latest periodic report on file is still the latest one there should be on as_of."""
    history = history_of(filings, as_of, category, fiscal_year_end)
    if history.calendar is None or history.latest is None:
        dated = max((f for f in history.filings if periodic_form(f.form) and f.report_date),
                    key=lambda f: (f.report_date, f.filing_date), default=None)
        ended = history.termination(dated.report_date) if dated and dated.report_date else None
        if dated and dated.report_date and history.annual and ended and as_of > _dark_after(history.annual,
                                                                                             dated.report_date):
            return ReportCurrency(as_of, dated, None, "deregistered", termination=ended.accession)
        periodic = any(periodic_form(f.form) for f in history.filings)
        reason = "fiscal calendar unknown" if periodic else "no periodic report on file"
        latest = history.latest.filing if history.latest else dated
        return ReportCurrency(as_of, latest, None, "unknown", termination=ended.accession if ended else None,
                              reason=reason)
    latest = history.latest.filing
    owed = _owed(history, calibration)
    nxt = owed[0] if owed else None
    ended = history.termination(history.latest.period.end)
    if ended is not None and as_of > _dark_after(history.annual, history.latest.period.end):
        return ReportCurrency(as_of, latest, nxt, "deregistered", tuple(owed), termination=ended.accession)
    if history.year_end_changed:
        return ReportCurrency(as_of, latest, None, "unknown", tuple(owed), reason="fiscal year end changing")
    verdict: Verdict = "current"
    if nxt is not None and nxt.period_end < as_of:
        if as_of > nxt.latest_deadline:
            verdict = "overdue"
        elif nxt.basis == "late_notice" and as_of > nxt.deadline:
            verdict = "extended"
        elif as_of >= nxt.window[0]:
            verdict = "expected"
    if ended is not None and verdict == "overdue":
        verdict = "deregistered"
    return ReportCurrency(as_of, latest, nxt, verdict, tuple(owed), termination=ended.accession if ended else None)
