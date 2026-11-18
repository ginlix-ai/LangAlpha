from datetime import date as d
from datetime import datetime, timedelta

import pytest
from edgar_engine.calendar import (
    Filing,
    FiscalCalendar,
    Period,
    deadline,
    expected_filings,
    extended_deadline,
    filer_category_from_document,
    filings_from_rows,
    is_business_day,
    latest_deadline,
    normalize_category,
    report_currency,
    roll,
)
from edgar_engine.calendar.business_days import federal_holidays
from edgar_engine.calendar.dates import nearest_weekday
from edgar_engine.calendar.rules import periodic_form


@pytest.mark.parametrize("form,end,category,fy_end,due", [
    ("10-K", d(2025, 12, 31), "large_accelerated", None, d(2026, 3, 2)),      # day 60 is a Sunday
    ("10-K", d(2025, 12, 31), "accelerated", None, d(2026, 3, 16)),
    ("10-K", d(2025, 12, 31), "non_accelerated", None, d(2026, 3, 31)),
    ("10-K", d(2025, 12, 31), None, None, d(2026, 3, 31)),
    ("10-KSB", d(2007, 12, 31), "accelerated", None, d(2008, 3, 31)),
    ("10-K", d(2005, 12, 31), "large_accelerated", None, d(2006, 3, 16)),     # 75 days before FYE 2006-12-15
    ("10-K", d(2006, 12, 31), "large_accelerated", None, d(2007, 3, 1)),
    ("10-K", d(2003, 6, 30), "accelerated", None, d(2003, 9, 29)),            # 90 days before FYE 2003-12-15
    ("10-Q", d(2026, 3, 31), "large_accelerated", None, d(2026, 5, 11)),      # day 40 is a Sunday
    ("10-Q", d(2026, 3, 31), "non_accelerated", None, d(2026, 5, 15)),
    ("10-Q", d(2004, 3, 31), "accelerated", d(2004, 12, 31), d(2004, 5, 10)),
    ("10-Q", d(2003, 9, 30), "accelerated", d(2003, 12, 31), d(2003, 11, 14)),
    ("20-F", d(2025, 12, 31), None, None, d(2026, 4, 30)),
    ("20-F", d(2025, 6, 30), None, None, d(2025, 10, 31)),
    ("20-F", d(2010, 12, 31), None, None, d(2011, 6, 30)),
    ("40-F", d(2025, 12, 31), None, None, d(2026, 3, 31)),
    ("8-K", d(2025, 12, 31), None, None, None),
    ("10-K/A", d(2025, 12, 31), None, None, None),
])
def test_deadline(form, end, category, fy_end, due):
    assert deadline(form, end, category, fy_end) == due


def test_extension_counts_from_the_rolled_due_date():
    # Due Saturday 2024-03-30, so filed timely on Monday 2024-04-01; 15 days later is Tuesday 2024-04-16.
    assert deadline("10-K", d(2023, 12, 31), "non_accelerated") == d(2024, 4, 1)
    assert extended_deadline("10-K", d(2023, 12, 31), "non_accelerated") == d(2024, 4, 16)
    assert extended_deadline("10-Q", d(2026, 3, 31), "large_accelerated") == d(2026, 5, 18)
    assert extended_deadline("40-F", d(2025, 12, 31), None) is None


def test_business_days():
    assert d(2021, 12, 31) in federal_holidays(2021)          # New Year's Day 2022 fell on a Saturday
    assert d(2021, 6, 18) in federal_holidays(2021)           # first Juneteenth, observed Friday
    assert d(2020, 6, 19) not in federal_holidays(2020)
    assert not is_business_day(d(2025, 1, 9))                 # declared closure
    assert roll(d(2026, 11, 26)) == d(2026, 11, 27)           # Thanksgiving
    assert roll(d(2026, 11, 27)) == d(2026, 11, 27)


@pytest.mark.parametrize("text,category", [
    ("Large Accelerated Filer", "large_accelerated"), ("Accelerated Filer", "accelerated"),
    ("Non-accelerated Filer", "non_accelerated"), ("Smaller Reporting Company", "non_accelerated"),
    ("Smaller Reporting Accelerated Filer", "accelerated"), ("1-LAF", "large_accelerated"), ("5-SML", "non_accelerated"),
    ("Non-accelerated filer<br>Smaller reporting company", "non_accelerated"), ("", None), ("Well-known Seasoned Issuer", None),
])
def test_normalize_category(text, category):
    assert normalize_category(text) == category


def test_category_from_document():
    inline = ('<td><ix:nonNumeric contextRef="c-1" name="dei:EntityFilerCategory" id="f-9">Large Accelerated '
              '<span>Filer</span></ix:nonNumeric></td>')
    instance = '<dei:EntityFilerCategory contextRef="D2012">Smaller Reporting Company</dei:EntityFilerCategory>'
    assert filer_category_from_document(inline) == "large_accelerated"
    assert filer_category_from_document(instance) == "non_accelerated"
    assert filer_category_from_document("<html>no tags</html>") is None


def test_fiscal_calendar_rules():
    # 2022-09-24 is the last Saturday of September but not the one nearest its end, which is October 1.
    last_sat = FiscalCalendar.infer((d(2022, 9, 24), d(2023, 9, 30), d(2024, 9, 28), d(2025, 9, 27)))
    assert last_sat.rule == "last_weekday"
    assert [(p.fiscal_period, p.end) for p in last_sat.periods_after(Period("FY", d(2025, 9, 27), False), d(2026, 9, 26))] == [
        ("Q1", d(2025, 12, 27)), ("Q2", d(2026, 3, 28)), ("Q3", d(2026, 6, 27)), ("FY", d(2026, 9, 26)),
        ("Q1", d(2026, 12, 26))]
    nearest = FiscalCalendar.infer((d(2023, 12, 30), d(2024, 12, 28), d(2026, 1, 3)))
    assert nearest.rule == "nearest_weekday"
    assert nearest.year_end_after(d(2026, 1, 3)) == d(2027, 1, 2)
    month = FiscalCalendar.infer((d(2024, 6, 30), d(2025, 6, 30)), quarterly=False)
    assert [(p.fiscal_period, p.end) for p in month.periods_after(Period("FY", d(2025, 6, 30), False), d(2025, 7, 1))] == [
        ("FY", d(2026, 6, 30))]
    assert FiscalCalendar.infer((d(2022, 1, 15), d(2023, 1, 15), d(2024, 1, 15))).year_end_after(d(2024, 1, 15)) == d(2025, 1, 15)
    assert FiscalCalendar.infer((d(2022, 6, 28), d(2023, 6, 28), d(2024, 6, 28))).year_end_after(d(2024, 6, 28)) == d(2025, 6, 28)


def test_a_tie_between_weekday_rules_goes_to_the_later_year_end():
    # Mondays nearest January 31; every year end before 2008 is also the last Monday of January.
    ends = (d(2005, 1, 31), d(2006, 1, 30), d(2007, 1, 29), d(2008, 2, 4))
    assert FiscalCalendar.infer(ends).year_end_after(d(2008, 2, 4)) == d(2009, 2, 2)
    assert FiscalCalendar.infer((d(2023, 9, 30), d(2024, 9, 28), d(2025, 9, 27))).year_end_after(d(2025, 9, 27)) == d(2026, 10, 3)


def _history(lag_q=30, lag_k=55):
    rows = []
    for y in (2023, 2024, 2025):
        rows.append(Filing("10-K", d(y, 1, 1) + timedelta(days=lag_k - 1), d(y - 1, 12, 31), f"k{y}"))
        for m, last in ((3, 31), (6, 30), (9, 30)):
            end = d(y, m, last)
            rows.append(Filing("10-Q", end + timedelta(days=lag_q), end, f"q{y}{m}"))
    return rows


def test_expected_from_history_and_signals():
    fl = _history()
    exp = expected_filings(fl, as_of=d(2025, 12, 31), category="Large Accelerated Filer")
    nxt = exp[0]
    assert (nxt.form, nxt.fiscal_period, nxt.period_end, nxt.basis, nxt.history) == ("10-K", "FY", d(2025, 12, 31), "history", (55, 55, 55))
    assert nxt.expected == d(2026, 2, 24) and nxt.deadline == d(2026, 3, 2)
    assert nxt.window[0] <= nxt.expected <= nxt.window[1]
    released = fl + [Filing("8-K", d(2026, 2, 5), None, "e1", ("2.02", "9.01"))]
    assert expected_filings(released, as_of=d(2026, 2, 6), category="1-LAF")[0].basis == "earnings_release"
    late = fl + [Filing("NT 10-K", d(2026, 3, 3), d(2025, 12, 31), "nt1")]
    e = expected_filings(late, as_of=d(2026, 3, 3), category="1-LAF")[0]
    assert (e.basis, e.notice, e.extended_deadline) == ("late_notice", "nt1", d(2026, 3, 17))


def test_report_currency_verdicts():
    fl = _history()
    # A 10-K's category is set at the year end it covers and can drop to non-accelerated, so overdue waits for day 90.
    cases = {d(2026, 1, 15): "current", d(2026, 2, 24): "expected", d(2026, 3, 20): "expected", d(2026, 4, 1): "overdue"}
    for as_of, verdict in cases.items():
        assert report_currency(fl, as_of=as_of, category="1-LAF").verdict == verdict
    late = fl + [Filing("NT 10-K", d(2026, 3, 3), d(2025, 12, 31), "nt1")]
    assert report_currency(late, as_of=d(2026, 3, 10), category="1-LAF").verdict == "extended"
    after_a_drop = fl + [Filing("NT 10-K", d(2026, 3, 17), d(2025, 12, 31), "nt2")]
    rc = report_currency(after_a_drop, as_of=d(2026, 4, 10), category="1-LAF")
    assert (rc.verdict, rc.next.notice, rc.next.latest_deadline) == ("extended", "nt2", d(2026, 4, 15))
    too_late = fl + [Filing("NT 10-K", d(2026, 4, 2), d(2025, 12, 31), "nt3")]
    rc = report_currency(too_late, as_of=d(2026, 4, 3), category="1-LAF")
    assert (rc.verdict, rc.next.notice) == ("overdue", None)
    filed = fl + [Filing("10-K", d(2026, 2, 20), d(2025, 12, 31), "k2026")]
    rc = report_currency(filed, as_of=d(2026, 3, 3), category="1-LAF")
    assert rc.latest.accession == "k2026" and rc.next.fiscal_period == "Q1" and rc.verdict == "current"


def test_a_form_15_ends_reporting_once_a_report_goes_unfiled():
    fl = _history() + [Filing("15-12G", d(2025, 11, 20), None, "t0"), Filing("15-15D", d(2025, 12, 1), None, "t1")]
    rc = report_currency(fl[::-1], as_of=d(2026, 6, 1), category="1-LAF")
    assert (rc.verdict, rc.termination) == ("deregistered", "t1")
    relisted = fl + [Filing("10-K", d(2026, 4, 20), d(2025, 12, 31), "k2026"), Filing("10-Q", d(2026, 5, 1), d(2026, 3, 31), "q2026")]
    assert report_currency(relisted, as_of=d(2026, 6, 1), category="1-LAF").verdict == "current"


def test_a_10k_filer_that_never_filed_a_10q_still_owes_them():
    # Rules 13a-13 and 15d-13 exempt classes of issuer, never a filer for not having filed.
    fl = [Filing("10-K", d(y, 3, 25), d(y - 1, 12, 31), f"k{y}") for y in range(2020, 2026)]
    rc = report_currency(fl, as_of=d(2025, 6, 1))
    assert (rc.verdict, rc.next.fiscal_period) == ("overdue", "Q1")


def test_filings_from_rows_reads_both_shapes():
    rows = [{"form": "10-Q", "filingDate": "2026-05-01", "reportDate": "2026-03-31", "accessionNumber": "a", "items": ""},
            {"form": "8-K", "filing_date": d(2026, 4, 30), "reportDate": None, "accession_number": "b", "items": "2.02,9.01"}]
    assert filings_from_rows(rows) == [Filing("10-Q", d(2026, 5, 1), d(2026, 3, 31), "a"),
                                       Filing("8-K", d(2026, 4, 30), None, "b", ("2.02", "9.01"))]


def test_retiring_one_class_is_not_deregistering():
    fl = _history() + [Filing("15-12B", d(2025, 12, 1), None, "notes")]
    rc = report_currency(fl, as_of=d(2026, 1, 15), category="1-LAF")
    assert (rc.verdict, rc.termination) == ("current", "notes")
    filed = fl + [Filing("10-K", d(2026, 2, 20), d(2025, 12, 31), "k2026")]
    assert report_currency(filed, as_of=d(2026, 6, 1), category="1-LAF").termination is None


def _week_53_in_q1():
    # A 53rd week in Q1 of fiscal 2023: Q1 ends 14 weeks after the 2022 year end.
    years = [d(2021, 9, 25), d(2022, 9, 24), d(2023, 9, 30), d(2024, 9, 28)]
    quarters = [d(2021, 12, 25), d(2022, 3, 26), d(2022, 6, 25), d(2022, 12, 31), d(2023, 4, 1), d(2023, 7, 1),
                d(2023, 12, 30), d(2024, 3, 30), d(2024, 6, 29)]
    return ([Filing("10-K", e + timedelta(days=35), e) for e in years]
            + [Filing("10-Q", q + timedelta(days=35), q) for q in quarters])


@pytest.mark.parametrize("as_of", [d(2024, 2, 16), d(2024, 3, 15), d(2024, 5, 17), d(2024, 6, 15), d(2024, 8, 20)])
def test_a_filed_report_covers_its_period_when_the_week_count_shifts(as_of):
    rc = report_currency(_week_53_in_q1(), as_of=as_of, category="Large Accelerated Filer")
    assert rc.verdict == "current" and len(rc.owed) == 1


def test_weekday_quarters_in_a_month_end_year_are_covered():
    years = [d(2022, 12, 31), d(2023, 12, 31), d(2024, 12, 31)]
    quarters = [nearest_weekday(y, m, 4) for y in (2023, 2024, 2025) for m in (3, 6, 9)]
    fl = [Filing("10-K", e + timedelta(days=50), e) for e in years] + [Filing("10-Q", q + timedelta(days=30), q) for q in quarters]
    for q in quarters[3:]:
        assert report_currency(fl, as_of=q + timedelta(days=60), category="1-LAF").verdict == "current"


def test_unknown_without_a_periodic_report_or_a_fiscal_calendar():
    assert report_currency([], as_of=d(2026, 1, 1)).reason == "no periodic report on file"
    assert report_currency([Filing("8-K", d(2025, 5, 1))], as_of=d(2026, 1, 1)).verdict == "unknown"
    quarters = [Filing("10-Q", end + timedelta(days=40), end) for end in (d(2025, 3, 31), d(2025, 6, 30), d(2025, 9, 30))]
    assert report_currency(quarters, as_of=d(2026, 11, 1)).reason == "fiscal calendar unknown"
    assert report_currency(quarters, as_of=d(2026, 11, 1), fiscal_year_end="1231").verdict == "overdue"


def test_a_changing_fiscal_year_end_is_unknown():
    fl = _changing_to_march()
    rc = report_currency(fl, as_of=d(2025, 4, 20), category="1-LAF", fiscal_year_end="0331")
    assert (rc.verdict, rc.next, rc.reason) == ("unknown", None, "fiscal year end changing")
    assert report_currency(fl, as_of=d(2025, 4, 20), category="1-LAF", fiscal_year_end="1231").verdict == "current"


def test_first_10k_after_a_20f_owes_quarters():
    fl = [Filing("20-F", d(2024, 4, 20), d(2023, 12, 31)), Filing("10-K", d(2025, 3, 20), d(2024, 12, 31))]
    rc = report_currency(fl, as_of=d(2025, 8, 1))
    assert (rc.verdict, rc.next.form, rc.next.fiscal_period) == ("overdue", "10-Q", "Q1")


def test_asset_backed_issuer_owes_no_quarters():
    fl = [Filing("10-K", d(2025, 3, 25), d(2024, 12, 31)), Filing("10-D", d(2025, 6, 15), d(2025, 5, 31))]
    assert report_currency(fl, as_of=d(2025, 8, 1)).next.fiscal_period == "FY"


def test_annual_report_without_a_period_of_report_covers_the_last_year_end():
    fl = [Filing("10-K", d(2024, 3, 1), d(2023, 12, 31))]
    fl += [Filing("10-Q", end + timedelta(days=40), end) for end in (d(2024, 3, 31), d(2024, 6, 30), d(2024, 9, 30))]
    fl += [Filing("10-K", d(2025, 3, 1), None)]
    rc = report_currency(fl, as_of=d(2025, 4, 1))
    assert (rc.verdict, rc.next.fiscal_period, rc.next.period_end) == ("current", "Q1", d(2025, 3, 31))


def test_deadline_reads_edgar_category_spellings():
    assert deadline("10-K", d(2025, 12, 31), "Large accelerated filer") == d(2026, 3, 2)
    assert deadline("10-K", d(2025, 12, 31), "Large accelerated filer<br>Well-known Seasoned Issuer") == d(2026, 3, 2)


def test_latest_deadline():
    assert latest_deadline("10-K", d(2025, 12, 31), "large_accelerated") == d(2026, 3, 31)
    assert latest_deadline("10-K", d(2025, 12, 31), "large_accelerated", extended=True) == d(2026, 4, 15)
    assert latest_deadline("10-K", d(2025, 12, 31), "non_accelerated") == d(2026, 3, 31)
    assert latest_deadline("10-Q", d(2026, 3, 31), "large_accelerated") == d(2026, 5, 11)
    assert latest_deadline("40-F", d(2025, 12, 31), None) == d(2026, 4, 30)


def test_40f_followed_the_canadian_deadline_before_2004():
    assert deadline("40-F", d(2002, 12, 31), None) == d(2003, 5, 20)


def test_filings_from_rows_reads_timestamps_and_missing_values():
    rows = [{"form": "10-K", "filing_date": datetime(2025, 2, 20, 16, 0), "reportDate": float("nan")}]
    assert filings_from_rows(rows) == [Filing("10-K", d(2025, 2, 20))]


def _changing_to_march():
    fl = [Filing("10-K", d(y, 2, 25), d(y - 1, 12, 31)) for y in (2023, 2024, 2025)]
    fl += [Filing("10-Q", d(y, m, 10), end) for y in (2023, 2024) for m, end in ((5, d(y, 3, 31)), (8, d(y, 6, 30)), (11, d(y, 9, 30)))]
    return fl + [Filing("10-QT", d(2025, 5, 10), d(2025, 3, 31)), Filing("10-Q", d(2025, 8, 10), d(2025, 6, 30)),
                 Filing("10-Q", d(2025, 11, 10), d(2025, 9, 30)), Filing("10-Q", d(2026, 2, 10), d(2025, 12, 31))]


def test_a_transition_report_moves_the_fiscal_calendar():
    for as_of in (d(2026, 4, 15), d(2026, 5, 20)):
        rc = report_currency(_changing_to_march(), as_of=as_of, category="1-LAF", fiscal_year_end="0331")
        assert (rc.verdict, rc.next.fiscal_period, rc.next.period_end) == ("current" if as_of.month == 4 else "expected",
                                                                          "FY", d(2026, 3, 31))
    rc = report_currency(_changing_to_march(), as_of=d(2026, 4, 15), category="1-LAF")
    assert (rc.verdict, rc.reason) == ("unknown", "fiscal year end changing")


def test_a_quarterly_transition_report_inside_a_longer_transition_period():
    # February to December: a 10-QT for a quarter of the ten-month transition period, then its 10-K.
    fl = [Filing("10-K", d(2023, 5, 25), d(2023, 2, 28)), Filing("10-K", d(2024, 5, 20), d(2024, 2, 29)),
          Filing("10-Q", d(2023, 7, 10), d(2023, 5, 31)), Filing("10-QT", d(2024, 7, 18), d(2024, 6, 30))]
    for fye in (None, "1231"):
        assert report_currency(fl, as_of=d(2025, 3, 1), fiscal_year_end=fye).verdict == "unknown"
    filed = fl + [Filing("10-K", d(2025, 4, 15), d(2024, 12, 31))]
    rc = report_currency(filed, as_of=d(2025, 4, 20), fiscal_year_end="1231")
    assert (rc.verdict, rc.next.fiscal_period, rc.next.period_end) == ("current", "Q1", d(2025, 3, 31))


def test_month_end_quarters_follow_the_year_end_not_a_stray_quarter():
    # New year end April 30; the next report is an old-calendar quarter to June 30, then Q2 ends October 31.
    fl = [Filing("10-K", d(2024, 3, 10), d(2023, 12, 31)), Filing("10-QT", d(2024, 6, 10), d(2024, 4, 30)),
          Filing("10-Q", d(2024, 8, 10), d(2024, 6, 30))]
    rc = report_currency(fl, as_of=d(2024, 11, 20), fiscal_year_end="0430")
    assert (rc.verdict, rc.next.period_end) == ("current", d(2024, 10, 31))


def test_a_form_15_before_the_last_report_still_counts():
    fl = _history() + [Filing("15-12G", d(2026, 2, 1), None, "t"), Filing("10-K", d(2026, 2, 20), d(2025, 12, 31), "k")]
    rc = report_currency(fl, as_of=d(2026, 6, 1), category="1-LAF")
    assert (rc.verdict, rc.termination) == ("deregistered", "t")


def test_a_stale_fiscal_year_end_does_not_hide_a_deregistration():
    fl = [Filing("10-K", d(y, 12, 15), d(y, 9, 30)) for y in (2020, 2021, 2022)] + [Filing("15-12G", d(2025, 3, 1), None, "t")]
    assert report_currency(fl, as_of=d(2025, 6, 1), fiscal_year_end="1231").verdict == "deregistered"


def test_a_projected_weekly_quarter_can_be_the_14_week_one():
    # Q1 of fiscal 2023 runs 14 weeks to 2022-12-31; the projection says 2022-12-24, due 40 days later on 2023-02-02.
    fl = [f for f in _week_53_in_q1() if f.filing_date <= d(2023, 2, 3)]
    rc = report_currency(fl, as_of=d(2023, 2, 3), category="Large Accelerated Filer")
    assert rc.next.period_end == d(2022, 12, 24) and rc.next.latest_deadline == d(2023, 2, 9) and rc.verdict != "overdue"


def test_a_quarterly_report_without_a_period_of_report_fills_the_quarter_owed():
    fl = [Filing("10-K", d(2024, 3, 1), d(2023, 12, 31)), Filing("10-Q", d(2024, 5, 10), d(2024, 3, 31)),
          Filing("10-Q", d(2024, 8, 10), None)]
    rc = report_currency(fl, as_of=d(2024, 9, 1))
    assert (rc.verdict, rc.next.fiscal_period) == ("current", "Q3")


def test_the_latest_periodic_report_sets_the_form_family():
    fl = [Filing("20-F", d(y, 4, 20), d(y - 1, 12, 31)) for y in (2023, 2024)] + [Filing("10-Q", d(2025, 5, 10), d(2025, 3, 31))]
    rc = report_currency(fl, as_of=d(2025, 7, 1))
    assert (rc.next.form, rc.next.fiscal_period, rc.next.period_end) == ("10-Q", "Q2", d(2025, 6, 30))


def test_a_sole_undated_annual_report_is_placed_by_the_stated_year_end():
    rc = report_currency([Filing("10-K", d(2025, 3, 20))], as_of=d(2025, 4, 1), fiscal_year_end="1231")
    assert (rc.verdict, rc.next.fiscal_period, rc.next.period_end) == ("current", "Q1", d(2025, 3, 31))


def test_a_notice_timely_only_after_a_category_drop_expects_the_later_extension():
    late = _history() + [Filing("NT 10-K", d(2026, 4, 1), d(2025, 12, 31), "nt")]
    e = expected_filings(late, as_of=d(2026, 4, 2), category="1-LAF")[0]
    assert (e.basis, e.expected, e.latest_deadline) == ("late_notice", d(2026, 4, 15), d(2026, 4, 15))


def test_a_notice_without_an_accession_still_extends():
    late = _history() + [Filing("NT 10-K", d(2026, 3, 3), d(2025, 12, 31))]
    assert report_currency(late, as_of=d(2026, 3, 10), category="1-LAF").verdict == "extended"


def test_the_edgar_shutdown_of_november_2000_rolls_a_due_date():
    assert not is_business_day(d(2000, 11, 24)) and roll(d(2000, 11, 24)) == d(2000, 11, 27)


def test_an_impossible_fiscal_year_end_reads_as_none():
    quarters = [Filing("10-Q", end + timedelta(days=40), end) for end in (d(2025, 3, 31), d(2025, 6, 30), d(2025, 9, 30))]
    assert report_currency(quarters, as_of=d(2026, 11, 1), fiscal_year_end="0231").reason == "fiscal calendar unknown"
    assert report_currency(quarters, as_of=d(2026, 11, 1), fiscal_year_end="0229").verdict != "unknown"


def test_filings_from_rows_reads_missing_items_as_none():
    rows = [{"form": "8-K", "filing_date": "2025-02-20", "items": float("nan")}]
    assert filings_from_rows(rows) == [Filing("8-K", d(2025, 2, 20))]


@pytest.mark.parametrize("dash", ["\u2010", "\u2011", "\u2013", "\u2014"])
def test_normalize_category_reads_unicode_dashes(dash):
    assert normalize_category(f"Non{dash}accelerated Filer") == "non_accelerated"


def test_a_year_end_moved_within_its_month_starts_a_new_calendar():
    ends = (d(2022, 12, 31), d(2023, 12, 31), d(2024, 12, 15))
    assert FiscalCalendar.infer(ends).year_end_after(d(2024, 12, 15)) == d(2025, 12, 15)
    fl = [Filing("10-K", e + timedelta(days=70), e) for e in ends[:2]]
    rc = report_currency(fl, as_of=d(2024, 10, 1), fiscal_year_end="1215")
    assert (rc.verdict, rc.reason) == ("unknown", "fiscal year end changing")


@pytest.mark.parametrize("closed", [d(2003, 12, 26), d(2008, 12, 26)])
def test_december_26_closures_roll_to_monday(closed):
    assert roll(closed) == d(closed.year, 12, 29)


def test_a_padded_fiscal_year_end_parses():
    quarters = [Filing("10-Q", end + timedelta(days=40), end) for end in (d(2025, 3, 31), d(2025, 6, 30), d(2025, 9, 30))]
    assert report_currency(quarters, as_of=d(2026, 11, 1), fiscal_year_end="  1231").verdict == "overdue"


def test_a_10k_for_a_period_a_20f_already_covered_sets_the_family():
    fl = [Filing("20-F", d(2024, 4, 20), d(2023, 12, 31)), Filing("20-F", d(2025, 3, 1), d(2024, 12, 31)),
          Filing("10-K", d(2025, 3, 20), d(2024, 12, 31))]
    rc = report_currency(fl, as_of=d(2025, 4, 1))
    assert (rc.next.form, rc.next.fiscal_period) == ("10-Q", "Q1")


def test_a_later_report_does_not_cure_an_earlier_gap():
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 31)), Filing("10-Q", d(2025, 8, 10), d(2025, 6, 30))]
    rc = report_currency(fl, as_of=d(2025, 9, 15))
    assert (rc.verdict, rc.next.fiscal_period, rc.next.period_end) == ("overdue", "Q1", d(2025, 3, 31))
    old_gap = fl + [Filing("10-K", d(2026, 3, 1), d(2025, 12, 31)), Filing("10-Q", d(2026, 5, 10), d(2026, 3, 31)),
                    Filing("10-Q", d(2026, 8, 10), d(2026, 6, 30)), Filing("10-Q", d(2026, 11, 9), d(2026, 9, 30)),
                    Filing("10-K", d(2027, 3, 1), d(2026, 12, 31))]
    assert report_currency(old_gap, as_of=d(2027, 3, 15)).verdict == "current"


def test_a_delinquent_old_report_does_not_switch_the_form_family_back():
    fl = [Filing("20-F", d(2022, 4, 20), d(2021, 12, 31)), Filing("10-K", d(2025, 3, 20), d(2024, 12, 31)),
          Filing("10-Q", d(2025, 5, 10), d(2025, 3, 31)), Filing("20-F", d(2025, 6, 1), d(2022, 12, 31))]
    assert report_currency(fl, as_of=d(2025, 6, 15)).next.form == "10-Q"


def test_lags_come_from_the_forms_filed_now():
    fl = [Filing("20-F", d(y, 4, 28), d(y - 1, 12, 31)) for y in (2022, 2023, 2024)]
    fl += [Filing("10-Q", d(2025, m, 10), end) for m, end in ((5, d(2025, 3, 31)), (8, d(2025, 6, 30)), (11, d(2025, 9, 30)))]
    nxt = report_currency(fl, as_of=d(2026, 1, 15)).next
    assert (nxt.form, nxt.basis) == ("10-K", "category")

def test_a_weekday_rule_must_fit_the_latest_year_ends_weekday():
    # A Friday, a Tuesday and a Wednesday: each is nearest December 31 for its own weekday, but no one weekday fits all.
    cal = FiscalCalendar.infer((d(2023, 12, 29), d(2024, 12, 31), d(2025, 12, 31)))
    assert cal.year_end_after(d(2025, 12, 31)) == d(2026, 12, 31)


@pytest.mark.parametrize("named, verdict", [(d(2022, 12, 24), "overdue"), (d(2022, 12, 31), "extended")])
def test_a_dated_notice_is_timely_against_the_period_it_names(named, verdict):
    # The projected quarter ends 2022-12-24, due 2023-02-02; a 14-week quarter to 2022-12-31 would be due 2023-02-09.
    fl = [f for f in _week_53_in_q1() if f.filing_date <= d(2023, 2, 3)] + [Filing("NT 10-Q", d(2023, 2, 10), named, "nt")]
    assert report_currency(fl, as_of=d(2023, 2, 10), category="Large Accelerated Filer").verdict == verdict


def test_an_undated_quarterly_transition_report_leaves_the_year_end_changing():
    fl = [Filing("10-K", d(y, 2, 25), d(y - 1, 12, 31)) for y in (2023, 2024, 2025)] + [Filing("10-QT", d(2025, 5, 10))]
    rc = report_currency(fl, as_of=d(2025, 6, 1), fiscal_year_end="1231")
    assert (rc.verdict, rc.reason) == ("unknown", "fiscal year end changing")


def test_a_transition_report_has_no_statutory_deadline():
    assert deadline("10-KT", d(2025, 3, 31), None) is None
    assert extended_deadline("10-QT", d(2025, 3, 31), None) is None
    assert latest_deadline("10-KT", d(2025, 3, 31), None) is None


def test_filings_from_rows_reads_a_malformed_date_as_none():
    rows = [{"form": "10-K", "filing_date": "2025-02-20", "reportDate": "12/31/2024"}, {"form": "10-Q", "filing_date": "n/a"}]
    assert filings_from_rows(rows) == [Filing("10-K", d(2025, 2, 20))]

def test_one_stray_year_end_keeps_a_month_end_calendar():
    cal = FiscalCalendar.infer((d(2022, 12, 31), d(2023, 12, 31), d(2024, 12, 30)))
    assert cal.year_end_after(d(2024, 12, 30)) == d(2025, 12, 31)


def test_a_10k_after_a_20f_for_the_same_year_is_the_latest_report():
    fl = [Filing("20-F", d(2024, 4, 20), d(2023, 12, 31)), Filing("20-F", d(2025, 4, 20), d(2024, 12, 31)),
          Filing("10-K", d(2025, 6, 2), d(2024, 12, 31))]
    assert report_currency(fl, as_of=d(2025, 6, 3)).latest.form == "10-K"


def test_a_late_transition_report_for_a_period_before_the_latest_annual_is_settled():
    fl = ([Filing("10-K", d(y, 2, 25), d(y - 1, 12, 31)) for y in (2023, 2024, 2025)]
          + [Filing("10-Q", d(2024, m + 1, 10), d(2024, m, 30)) for m in (3, 6, 9)]
          + [Filing("10-QT", d(2025, 3, 15), d(2023, 6, 30))])
    assert report_currency(fl, as_of=d(2025, 4, 1), fiscal_year_end="1231").verdict == "current"

def test_an_undated_quarterly_report_fills_an_earlier_gap():
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 31)), Filing("10-Q", d(2025, 8, 10), d(2025, 6, 30)),
          Filing("10-Q", d(2025, 9, 10))]
    rc = report_currency(fl, as_of=d(2025, 9, 15))
    assert (rc.verdict, rc.next.period_end) == ("current", d(2025, 9, 30))


def test_a_late_annual_report_for_an_old_period_does_not_settle_a_transition():
    fl = [Filing("10-K", d(2023, 5, 25), d(2023, 2, 28)), Filing("10-K", d(2024, 5, 20), d(2024, 2, 29)),
          Filing("10-Q", d(2023, 7, 10), d(2023, 5, 31)), Filing("10-QT", d(2024, 7, 18), d(2024, 6, 30)),
          Filing("10-K", d(2024, 8, 15), d(2022, 2, 28))]
    assert report_currency(fl, as_of=d(2025, 3, 1)).verdict == "unknown"


def test_a_quarter_missing_right_after_a_transition_is_owed():
    fl = [f for f in _changing_to_march() if f.report_date != d(2025, 6, 30)]
    rc = report_currency(fl, as_of=d(2026, 1, 15), category="1-LAF", fiscal_year_end="0331")
    assert (rc.verdict, rc.next.period_end) == ("overdue", d(2025, 6, 30))

def test_a_stated_fixed_date_year_end_holds_without_an_annual_report():
    fl = [Filing("10-Q", d(2025, m + 1, 25), d(2025, m, 15)) for m in (3, 6, 9)]
    nxt = report_currency(fl, as_of=d(2025, 11, 1), fiscal_year_end="1215").next
    assert (nxt.fiscal_period, nxt.period_end, nxt.deadline) == ("FY", d(2025, 12, 15), d(2026, 3, 16))


def test_a_notice_naming_the_14_week_end_moves_every_due_date():
    fl = [f for f in _week_53_in_q1() if f.filing_date <= d(2023, 2, 3)] + [Filing("NT 10-Q", d(2023, 2, 9), d(2022, 12, 31), "nt")]
    rc = report_currency(fl, as_of=d(2023, 2, 9), category="Large Accelerated Filer")
    assert (rc.next.period_end, rc.next.deadline, rc.next.extended_deadline) == (d(2022, 12, 31), d(2023, 2, 9), d(2023, 2, 14))
    assert rc.verdict != "extended"

def test_a_gap_just_inside_the_lookback_is_owed():
    # Q1 2025 is unfiled; on 2026-05-20 the lookback starts 2025-02-24, after the 2024 annual report it projects from.
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 31))] + [Filing("10-Q", e + timedelta(days=40), e) for e in (
        d(2025, 6, 30), d(2025, 9, 30), d(2026, 3, 31))] + [Filing("10-K", d(2026, 3, 1), d(2025, 12, 31))]
    rc = report_currency(fl, as_of=d(2026, 5, 20))
    assert (rc.verdict, rc.next.period_end) == ("overdue", d(2025, 3, 31))

def test_a_stated_fixed_date_year_end_sets_the_quarter_ends():
    fl = [Filing("10-Q", d(2025, 5, 20), d(2025, 4, 15))]
    rc = report_currency(fl, as_of=d(2025, 9, 5), fiscal_year_end="0115")
    assert (rc.verdict, rc.next.period_end, rc.next.deadline) == ("overdue", d(2025, 7, 15), d(2025, 8, 29))

def test_a_notice_naming_an_earlier_period_end_moves_the_due_dates_earlier():
    # A projected March 31 quarter that the notice says ended March 28: due May 12, so a notice of May 16 is late.
    fl = [Filing("10-K", d(2024, 3, 1), d(2023, 12, 31)), Filing("10-K", d(2025, 3, 1), d(2024, 12, 31))]
    fl += [Filing("10-Q", e + timedelta(days=40), e) for e in (d(2024, 3, 31), d(2024, 6, 30), d(2024, 9, 30))]
    rc = report_currency(fl + [Filing("NT 10-Q", d(2025, 5, 16), d(2025, 3, 28), "nt")], as_of=d(2025, 5, 16))
    assert (rc.verdict, rc.next.period_end, rc.next.deadline) == ("overdue", d(2025, 3, 28), d(2025, 5, 12))

def test_a_fixed_date_year_end_moved_by_days_is_a_change():
    fl = [Filing("10-K", d(y + 1, 3, 1), d(y, 12, 15)) for y in (2022, 2023, 2024)]
    rc = report_currency(fl, as_of=d(2025, 4, 1), fiscal_year_end="1220")
    assert (rc.verdict, rc.reason) == ("unknown", "fiscal year end changing")


def test_an_undated_10k_for_a_year_a_20f_covered_is_the_latest_report():
    fl = [Filing("20-F", d(y, 4, 20), d(y - 1, 12, 31)) for y in (2024, 2025)] + [Filing("10-K", d(2025, 6, 2))]
    assert report_currency(fl, as_of=d(2025, 6, 3), fiscal_year_end="1231").latest.form == "10-K"


def test_an_undated_10q_filed_with_its_annual_report_fills_the_quarter():
    fl = [Filing("10-K", d(2025, 5, 20), d(2024, 12, 31)), Filing("10-Q", d(2025, 5, 20))]
    rc = report_currency(fl, as_of=d(2025, 5, 21))
    assert (rc.verdict, rc.next.period_end) == ("current", d(2025, 6, 30))


def test_a_gap_in_a_past_fiscal_year_uses_no_category():
    # Q1 2024 is unfiled; its notice was timely only on the non-accelerated deadline (May 15, so May 16).
    fl = [Filing("10-K", d(2024, 2, 20), d(2023, 12, 31)), Filing("10-K", d(2025, 2, 20), d(2024, 12, 31)),
          Filing("10-Q", d(2024, 8, 5), d(2024, 6, 30)), Filing("10-Q", d(2024, 11, 5), d(2024, 9, 30)),
          Filing("NT 10-Q", d(2024, 5, 16), d(2024, 3, 31), "nt")]
    gap = report_currency(fl, as_of=d(2025, 6, 1), category="Large Accelerated Filer").owed[0]
    assert (gap.period_end, gap.deadline, gap.notice, gap.category) == (d(2024, 3, 31), d(2024, 5, 15), "nt", None)


def test_hyphenless_legacy_submission_types():
    assert (periodic_form("10KSB").family, periodic_form("10QSB").family) == ("10-K", "10-Q")
    fl = [Filing("10-K", d(2023, 3, 25), d(2022, 12, 31))] + [Filing("10-Q", e + timedelta(days=40), e) for e in (
        d(2023, 3, 31), d(2023, 6, 30), d(2023, 9, 30))] + [Filing("NTN 10K", d(2024, 4, 1), d(2023, 12, 31), "nt")]
    assert report_currency(fl, as_of=d(2024, 4, 3)).verdict == "extended"

def test_a_new_registrants_first_quarterly_report_owes_no_earlier_quarter():
    # Q1 was in the registration statement; the first 10-Q is for Q2.
    fl = [Filing("10-Q", d(2025, 8, 10), d(2025, 6, 30)), Filing("10-Q", d(2025, 11, 10), d(2025, 9, 30))]
    rc = report_currency(fl, as_of=d(2025, 11, 20), fiscal_year_end="1231")
    assert (rc.verdict, rc.next.period_end) == ("current", d(2025, 12, 31))


def test_a_late_undated_20f_does_not_switch_a_domestic_filer_back():
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 31)), Filing("10-Q", d(2025, 5, 10), d(2025, 3, 31)),
          Filing("10-Q", d(2025, 8, 10), d(2025, 6, 30)), Filing("20-F", d(2025, 9, 15))]
    rc = report_currency(fl, as_of=d(2025, 11, 20), fiscal_year_end="1231")
    assert (rc.next.form, rc.next.period_end, rc.verdict) == ("10-Q", d(2025, 9, 30), "overdue")


def test_a_notice_naming_the_14_week_end_moves_the_later_quarters():
    # Q1 runs 14 weeks to 2022-12-31, so Q2 ends 2023-04-01 (not 2023-03-25), due 2023-05-11.
    fl = [f for f in _week_53_in_q1() if f.filing_date <= d(2023, 2, 3)] + [Filing("NT 10-Q", d(2023, 2, 9), d(2022, 12, 31), "nt")]
    owed = report_currency(fl, as_of=d(2023, 5, 8), category="Large Accelerated Filer").owed
    assert [(e.fiscal_period, e.period_end, e.deadline) for e in owed[:2]] == [
        ("Q1", d(2022, 12, 31), d(2023, 2, 9)), ("Q2", d(2023, 4, 1), d(2023, 5, 11))]


def test_a_form_15_ends_reporting_without_a_fiscal_calendar():
    fl = [Filing("10-Q", d(2023, 8, 10), d(2023, 6, 30), "q"), Filing("15-12G", d(2023, 9, 15), None, "t")]
    rc = report_currency(fl, as_of=d(2025, 1, 15))
    assert (rc.verdict, rc.latest.accession, rc.termination) == ("deregistered", "q", "t")
    assert report_currency(fl, as_of=d(2024, 6, 1)).verdict == "unknown"

def test_a_late_undated_20f_ranks_by_the_year_end_the_10k_shows():
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 31)), Filing("10-Q", d(2025, 5, 10), d(2025, 3, 31)),
          Filing("10-Q", d(2025, 8, 10), d(2025, 6, 30)), Filing("20-F", d(2025, 9, 15))]
    rc = report_currency(fl, as_of=d(2025, 11, 20))
    assert (rc.next.form, rc.next.period_end, rc.verdict) == ("10-Q", d(2025, 9, 30), "overdue")


def test_a_small_business_filer_keeps_the_non_accelerated_deadline():
    fl = [Filing("10-KSB", d(y, 3, 25), d(y - 1, 12, 31)) for y in (2004, 2005)]
    fl += [Filing("10-QSB", e + timedelta(days=40), e) for e in (d(2004, 3, 31), d(2004, 6, 30), d(2004, 9, 30))]
    rc = report_currency(fl, as_of=d(2005, 5, 12), category="Accelerated Filer")
    assert (rc.next.form, rc.next.deadline, rc.verdict) == ("10-QSB", d(2005, 5, 16), "expected")


def test_a_filed_14_week_quarter_moves_the_quarters_after_it():
    # Q1 of fiscal 2023 was filed for the 14 weeks to 2022-12-31, so the missing Q2 ends 2023-04-01, due 2023-05-11.
    fl = [f for f in _week_53_in_q1() if f.filing_date <= d(2023, 5, 8) and f.report_date != d(2023, 4, 1)]
    rc = report_currency(fl, as_of=d(2023, 5, 8), category="Large Accelerated Filer")
    assert (rc.next.period_end, rc.next.deadline) == (d(2023, 4, 1), d(2023, 5, 11)) and rc.verdict != "overdue"

def test_a_sole_fixed_date_year_end_moved_by_days_is_a_change():
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 15))]
    rc = report_currency(fl, as_of=d(2025, 5, 1), fiscal_year_end="1220")
    assert (rc.verdict, rc.reason) == ("unknown", "fiscal year end changing")


def test_a_small_business_annual_filer_owes_small_business_quarters():
    rc = report_currency([Filing("10-KSB", d(2005, 3, 25), d(2004, 12, 31))], as_of=d(2005, 5, 12), category="Accelerated Filer")
    assert (rc.next.form, rc.next.deadline) == ("10-QSB", d(2005, 5, 16)) and rc.verdict != "overdue"


def test_the_40f_deadline_shortens_for_years_beginning_in_2004():
    assert deadline("40-F", d(2004, 12, 30), None) == d(2005, 5, 19)
    assert deadline("40-F", d(2004, 12, 31), None) == d(2005, 3, 31)

def test_a_return_to_domestic_reporting_owes_nothing_for_the_foreign_years():
    fl = [Filing("10-K", d(2025, 3, 1), d(2024, 12, 31)), Filing("20-F", d(2026, 4, 20), d(2025, 12, 31)),
          Filing("10-Q", d(2026, 5, 10), d(2026, 3, 31))]
    rc = report_currency(fl, as_of=d(2026, 6, 1))
    assert (rc.verdict, rc.next.period_end) == ("current", d(2026, 6, 30))


def test_a_switch_to_a_small_business_annual_moves_the_quarters_with_it():
    fl = [Filing("10-Q", e + timedelta(days=40), e) for e in (d(2004, 3, 31), d(2004, 6, 30), d(2004, 9, 30))]
    fl += [Filing("10-K", d(2004, 3, 1), d(2003, 12, 31)), Filing("10-KSB", d(2005, 3, 25), d(2004, 12, 31))]
    rc = report_currency(fl, as_of=d(2005, 5, 12), category="Accelerated Filer")
    assert (rc.next.form, rc.next.deadline) == ("10-QSB", d(2005, 5, 16)) and rc.verdict != "overdue"

def test_a_sponsor_that_co_files_10ds_still_owes_its_10qs():
    fl = [Filing("10-K", d(y, 2, 20), d(y - 1, 12, 31)) for y in (2024, 2025)]
    fl += [Filing("10-Q", e + timedelta(days=40), e) for e in (d(2024, 3, 31), d(2024, 6, 30), d(2024, 9, 30))]
    fl += [Filing("10-D", d(2025, m, 15)) for m in (1, 4, 7, 10)]
    rc = report_currency(fl, as_of=d(2025, 11, 20))
    assert (rc.verdict, rc.next.period_end) == ("overdue", d(2025, 3, 31))
