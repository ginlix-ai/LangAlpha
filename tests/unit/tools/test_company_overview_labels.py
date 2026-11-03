"""Regressions from an AAPL overview taken on its FY26 Q4 report day.

FMP's filings search ignored ``formType`` (an 8-K and Form 4s printed as
10-Qs), its stable earnings rows carry no ``fiscalDateEnding`` (every fiscal
label read N/A), its actual EPS is on the consensus basis, and the last
after-hours trade headlined a closed market unlabeled.
"""

from datetime import datetime

import pytest

from src.tools.market_data import earnings
from src.tools.market_data.company import _extended_session
from src.tools.market_data.earnings import join_earnings
from src.tools.market_data.fiscal_periods import fiscal_period_lookup, report_period_ends
from src.tools.market_data.sec_filings import _rows


def test_each_report_maps_to_the_quarter_it_announced():
    reports = ["2027-02-04", "2026-11-02", "2026-07-30", "2026-04-30"]
    ends = ["2026-09-26", "2026-06-27", "2026-03-28"]
    assert report_period_ends(reports, ends) == {
        "2026-11-02": "2026-09-26",
        "2026-07-30": "2026-06-27",
        "2026-04-30": "2026-03-28",
    }


def test_an_annual_report_after_the_next_quarter_ends_keeps_its_own_quarter():
    # A-shares publish the annual report in late April, after Q1 has ended and
    # days before the Q1 report.
    reports = ["2025-10-25", "2026-04-17", "2026-04-24"]
    ends = ["2025-09-30", "2025-12-31", "2026-03-31"]
    assert report_period_ends(reports, ends, annual_ends=["2025-12-31"]) == {
        "2025-10-25": "2025-09-30",
        "2026-04-17": "2025-12-31",
        "2026-04-24": "2026-03-31",
    }


def test_a_statement_older_than_the_earnings_history_shifts_no_quarter():
    # JPM reports two weeks after quarter end; the 2025-10-14 report is missing.
    reports = ["2026-01-13", "2026-04-14"]
    ends = ["2025-09-30", "2025-12-31", "2026-03-31"]
    assert report_period_ends(reports, ends) == {
        "2026-01-13": "2025-12-31",
        "2026-04-14": "2026-03-31",
    }


def test_a_report_whose_statement_is_not_out_borrows_no_earlier_quarter():
    # The 2026-11-02 report's statement is missing: the 2026-06-27 quarter was
    # announced on 2026-07-30, so the newer report stays unmatched.
    assert report_period_ends(["2026-11-02", "2026-07-30"], ["2026-06-27"]) == {
        "2026-07-30": "2026-06-27",
    }


def test_filings_keep_only_exact_periodic_forms_once_each():
    payload = {"hits": {"hits": [
        {"_id": "0000320193-26-000020:a.htm", "_source": {
            "form": "10-Q", "file_date": "2026-07-31", "period_ending": "2026-06-27"}},
        {"_id": "0000320193-26-000020:ex31.htm", "_source": {
            "form": "10-Q", "file_date": "2026-07-31", "period_ending": "2026-06-27"}},
        {"_id": "0000320193-26-000024:a.htm", "_source": {
            "form": "8-K", "file_date": "2026-11-02"}},
        {"_id": "0000320193-26-000030:a.htm", "_source": {
            "form": "10-Q/A", "file_date": "2026-08-15", "period_ending": "2026-06-27"}},
        {"_id": "0000320193-25-000079:a.htm", "_source": {
            "form": "10-K", "file_date": "2025-10-31", "period_ending": "2025-09-27"}},
    ]}}
    assert [(r["form"], r["filingDate"], r["periodOfReport"]) for r in _rows(payload)] == [
        ("10-Q", "2026-07-31", "2026-06-27"),
        ("10-K", "2025-10-31", "2025-09-27"),
    ]


def test_a_closed_market_still_names_the_extended_hours_trade():
    def label(status, last=329.03, late_change=-9.29):
        snap = {"price": 338.32, "last_trade_price": last, "market_status": status,
                "late_trading_change": late_change}
        named = _extended_session(snap)
        return named and named[0]

    assert label("closed") == "Last Extended-Hours"
    assert label("late_trading") == "After-Hours"
    assert not label("open")
    # A snapshot that names no session (CN, yfinance, FMP rows) is not guessed at.
    assert not label(None)
    assert not label("closed", last=338.32)
    # An odd-lot print off the close, with no late-session move, is no extended trade.
    assert not label("closed", last=338.31, late_change=0.0)
    assert not label("closed", last=float("nan"))


@pytest.fixture
def report_day(monkeypatch):
    """The AAPL report day, so the 2027-02-04 report stays upcoming."""

    class _ReportDay(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 3, tzinfo=tz)

    monkeypatch.setattr(earnings, "datetime", _ReportDay)


def test_a_report_without_its_statement_is_named_by_its_distance_from_one_with(report_day):
    income = [
        {"date": "2026-09-26", "period": "Q4", "fiscalYear": "2026", "epsDiluted": 2.10,
         "reportedCurrency": "USD"},
        {"date": "2026-06-27", "period": "Q3", "fiscalYear": "2026", "epsDiluted": 2.02,
         "reportedCurrency": "USD"},
    ]
    rows = [
        {"date": "2027-02-04", "epsActual": None, "epsEstimated": 2.88},
        {"date": "2026-11-02", "epsActual": 2.05, "epsEstimated": 1.99},
        {"date": "2026-07-30", "epsActual": 2.02, "epsEstimated": 1.89},
        {"date": "2026-04-30", "epsActual": 2.01, "epsEstimated": 1.95},  # older than the statements
    ]
    reports = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency="USD")
    assert [(r.date, r.period, r.diluted_eps) for r in reports] == [
        ("2027-02-04", "Q1 FY2027", None),
        ("2026-11-02", "Q4 FY2026", 2.10),
        ("2026-07-30", "Q3 FY2026", 2.02),
        ("2026-04-30", "Q2 FY2026", None),
    ]
    # Another currency's statement EPS is no comparison, so it stays out, per statement.
    other = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency="TWD")
    assert all(r.diluted_eps is None for r in other)
    income[1]["reportedCurrency"] = "EUR"
    mixed = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency="USD")
    assert [r.diluted_eps for r in mixed] == [None, 2.10, None, None]


def test_a_half_yearly_reporters_next_report_is_the_next_half():
    income = [
        {"date": "2026-06-30", "period": "Q2", "fiscalYear": "2026"},
        {"date": "2025-12-31", "period": "Q4", "fiscalYear": "2025"},
        {"date": "2025-06-30", "period": "Q2", "fiscalYear": "2025"},
    ]
    rows = [
        {"date": "2099-03-05", "epsActual": None},
        {"date": "2026-07-24", "epsActual": 0.31},
        {"date": "2026-03-06", "epsActual": 0.29},
        {"date": "2025-07-25", "epsActual": 0.28},
        {"date": "2024-11-03", "epsActual": None},  # a past row never filled in
    ]
    reports = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency=None)
    assert [(r.date, r.period) for r in reports] == [
        ("2099-03-05", "Q4 FY2026"),
        ("2026-07-24", "Q2 FY2026"),
        ("2026-03-06", "Q4 FY2025"),
        ("2025-07-25", "Q2 FY2025"),
    ]


def test_a_row_that_names_its_quarter_keeps_it_even_on_a_shared_day(report_day):
    # ginlix-data's CN rows: 000568.SZ put its FY2025 annual and Q1 FY2026 out
    # on one day, each naming its quarter and carrying the statement's basic EPS.
    income = [
        {"date": "2026-03-31", "period": "Q1", "fiscalYear": "2026", "epsDiluted": 2.52,
         "reportedCurrency": "CNY"},
        {"date": "2025-12-31", "period": "Q4", "fiscalYear": "2025", "epsDiluted": 0.05,
         "reportedCurrency": "CNY"},
    ]
    rows = [
        {"date": "2026-04-29", "fiscalDateEnding": "2026-03-31", "epsActual": 2.52, "kind": "report"},
        {"date": "2026-04-29", "fiscalDateEnding": "2025-12-31", "epsActual": 0.0464, "kind": "report"},
    ]
    reports = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency="CNY")
    assert [(r.period, r.eps_actual, r.diluted_eps) for r in reports] == [
        ("Q1 FY2026", 2.52, 2.52),
        ("Q4 FY2025", 0.0464, 0.05),
    ]
    text = "\n".join(earnings.earnings_lines(reports, "CNY"))
    assert "EPS (basic, reported)" in text and "consensus" not in text


def test_two_unnamed_reports_on_one_day_are_not_guessed_apart(report_day):
    income = [
        {"date": "2026-03-31", "period": "Q1", "fiscalYear": "2026"},
        {"date": "2025-12-31", "period": "Q4", "fiscalYear": "2025"},
        {"date": "2025-09-30", "period": "Q3", "fiscalYear": "2025"},
    ]
    rows = [
        {"date": "2026-04-29", "epsActual": 2.52},
        {"date": "2026-04-29", "epsActual": 0.05},
        {"date": "2025-10-31", "epsActual": 2.11},
    ]
    reports = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency=None)
    assert [(r.date, r.period) for r in reports] == [("2025-10-31", "Q3 FY2025")]


def test_a_leap_year_annual_report_on_april_30_still_pairs():
    assert report_period_ends(["2028-04-30"], ["2027-12-31"], annual_ends=["2027-12-31"]) == {
        "2028-04-30": "2027-12-31"
    }


def test_a_report_four_months_after_an_interim_end_is_not_that_interims():
    # Q2's report is missing and Q3's statement is not out yet: the Q3 report,
    # 122 days after the Q2 end, must not take Q2's label and diluted EPS.
    assert report_period_ends(["2026-04-30", "2026-10-30"], ["2026-03-31", "2026-06-30"]) == {
        "2026-04-30": "2026-03-31",
    }


def test_a_missing_report_does_not_make_a_quarterly_issuer_half_yearly(report_day):
    income = [
        {"date": d, "period": p, "fiscalYear": "2026"}
        for d, p in [("2026-09-30", "Q3"), ("2026-06-30", "Q2"), ("2026-03-31", "Q1")]
    ]
    # The Q2 report is missing from the history, so the matched ends are half a year apart.
    rows = [
        {"date": "2027-01-28", "epsActual": None},
        {"date": "2026-10-29", "epsActual": 1.1},
        {"date": "2026-04-30", "epsActual": 1.0},
    ]
    reports = join_earnings(rows, income, fiscal_period_lookup(income), eps_currency=None)
    assert reports[0].period == "Q4 FY2026"
