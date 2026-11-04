"""Regressions from an AAPL overview taken on its FY26 Q4 report day.

FMP's filings search ignored ``formType`` (an 8-K and Form 4s printed as
10-Qs), its stable earnings rows carry no ``fiscalDateEnding`` (every fiscal
label read N/A), its actual EPS is on the consensus basis, and the last
after-hours trade headlined a closed market unlabeled.
"""

from datetime import datetime, timedelta, timezone

import pytest

from market_protocol import MarketPhase
from src.tools.market_data import company, earnings
from src.tools.market_data.company import CompanyInputs, _company_text, _extended_session, _quote_lines
from src.tools.market_data.display import _symbol_currency, resolve_listing
from src.tools.market_data.earnings import join_earnings
from src.tools.market_data.fiscal_periods import fiscal_period_lookup, report_period_ends
from src.tools.market_data.sec_filings import _rows
from src.tools.market_data.segments import SegmentBreakdown


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


def _inputs(symbol, *, quote=None, snapshot=None, profile=None):
    ref, _ = resolve_listing(symbol)
    return CompanyInputs(
        symbol=symbol, ref=ref, cur=_symbol_currency(ref), profile=profile or {}, name=symbol, name_en=None,
        income=[], earnings=[], cash_flow=[], performance=None, metrics=None, ratios=None,
        price_target=None, ratings=None, quote=quote, segments=SegmentBreakdown(None, None),
        stmt_cur=None, cash_flow_cur=None, fiscal={}, snapshot=snapshot,
    )


def _quote_text(symbol, **kw):
    return "\n".join(_quote_lines(_inputs(symbol, **kw)))


def test_a_pence_listing_provider_quote_keeps_the_places_a_pence_move_needs():
    text = _quote_text("VOD.L", quote={
        "price": 1.172, "change": 0.0035, "changePercentage": 0.3,
        "yearLow": 0.85235, "yearHigh": 1.32072,
    })
    assert "**Price:** £1.1720 (+0.0035 / +0.30%)" in text
    assert "£0.8524 - £1.3207" in text


def test_a_pence_listing_snapshot_keeps_the_places_a_pence_move_needs(monkeypatch):
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.CLOSED)
    text = _quote_text("VOD.L", snapshot={
        "price": 1.172, "change": 0.0035, "change_percent": 0.3,
        "regular_trading_change": 0.0035, "regular_trading_change_percent": 0.3,
        "open": 1.168, "previous_close": 1.1685, "low": 1.16, "high": 1.18,
    })
    assert "**Regular Close:** £1.1720 (+0.0035 / +0.300%)" in text
    assert "**Day Change (from prev close):** +0.0035 / +0.300%" in text


def test_a_pence_listing_headline_price_matches_its_quote_block():
    quote = {"price": 1.172, "change": 0.0035, "changePercentage": 0.3}
    text = _company_text(_inputs("VOD.L", quote=quote, profile={"price": 1.172}))
    assert "Current Price: £1.1720" in text
    assert "**Price:** £1.1720" in text


@pytest.mark.parametrize("phase, freshness, label", [
    (MarketPhase.REGULAR, {"label": "live", "measured": True}, "Last Price"),
    (MarketPhase.LUNCH, {"label": "live", "measured": True}, "Last Price"),
    (MarketPhase.CLOSED, {"label": "delayed", "measured": True, "closed": True}, "Last Price"),
    (MarketPhase.CLOSED, {"label": "live", "measured": True, "closed": True}, "Regular Close"),
    (MarketPhase.POST, {"label": "live", "measured": True}, "Regular Close"),
])
def test_the_snapshot_price_is_a_regular_close_only_once_it_settles(monkeypatch, phase, freshness, label):
    monkeypatch.setattr(company, "venue_phase", lambda ref: phase)
    monkeypatch.setattr(company, "stamp_quote", lambda snap, ref=None: {"freshness": freshness})
    text = _quote_text("0700.HK", snapshot={"price": 401.6, "tier": "delayed_15m"})
    assert f"**{label}:** HK$401.60" in text


def test_a_sub_dollar_close_keeps_four_places_beside_a_trade_above_a_dollar(monkeypatch):
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.POST)
    monkeypatch.setattr(company, "stamp_quote", lambda snap, ref=None: {
        "freshness": {"label": "live", "measured": True}})
    snapshot = {"price": 0.9834, "last_trade_price": 1.02, "previous_close": 0.95, "market_status": "late_trading",
                "late_trading_change": 0.0366, "late_trading_change_percent": 3.72, "tier": "realtime"}
    assert "**Regular Close:** $0.9834\n" in _quote_text("SUBD", snapshot=snapshot)
    moved = {**snapshot, "regular_trading_change": 0.0334, "regular_trading_change_percent": 3.516}
    assert "**Regular Close:** $0.9834 (+0.03 / +3.516%)" in _quote_text("SUBD", snapshot=moved)


def test_an_unsettled_session_shows_the_last_trade_and_one_move(monkeypatch):
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.REGULAR)
    text = _quote_text("TSM", snapshot={
        "price": 405.30, "last_trade_price": 405.60, "previous_close": 400.0, "market_status": "open",
        "change": 5.30, "change_percent": 1.325, "regular_trading_change": 5.30,
        "regular_trading_change_percent": 1.325, "tier": "realtime",
    })
    assert "**Last Price:** $405.60\n**Day Change (from prev close):** +5.60 / +1.400%" in text
    assert "405.30" not in text


def test_a_pre_market_snapshot_shows_its_trade_not_the_prior_session(monkeypatch):
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.PRE)
    snapshot = {
        "last_trade_price": 332.21, "previous_close": 331.9, "market_status": "early_trading",
        "change": 0.31, "change_percent": 0.0934, "early_trading_change": 0.31,
        "early_trading_change_percent": 0.0934, "volume": 149_088, "tier": "realtime",
    }
    prior = {"price": 331.9, "change": -6.42, "changePercentage": -1.9, "open": 325.58, "previousClose": 338.32}
    text = _company_text(_inputs("AAPL", quote=prior, snapshot=snapshot, profile={"price": 331.9}))
    assert "Current Price: $332.21 (extended-hours trade; see Quote)" in text
    assert "**Pre-Market Price:** $332.21\n**Day Change (from prev close):** +0.31 / +0.093%" in text
    assert "| Previous Close | $331.90 |" in text
    assert "**Price:**" not in text and "325.58" not in text and "338.32" not in text


def test_a_pre_market_status_with_no_pre_market_trade_yet_is_no_pre_market_price(monkeypatch):
    # Right after the pre-market opens, the last trade can still be the day before's.
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.PRE)
    # Two days back is an earlier ET date even across a 25-hour fall-back day.
    earlier = int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000)
    snapshot = {
        "last_trade_price": 332.21, "last_trade_at": earlier, "previous_close": 331.9,
        "market_status": "early_trading", "change": 0.31, "change_percent": 0.0934, "tier": "realtime",
    }
    prior = {"price": 331.9, "change": -6.42, "changePercentage": -1.9, "open": 325.58, "previousClose": 338.32}
    text = _company_text(_inputs("AAPL", quote=prior, snapshot=snapshot, profile={"price": 331.9}))
    assert "**Last Price:** $332.21" in text
    assert "Pre-Market Price" not in text


def test_a_closed_market_still_settling_names_only_an_extended_trade(monkeypatch):
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.CLOSED)
    monkeypatch.setattr(company, "stamp_quote", lambda snap, ref=None: {
        "freshness": {"label": "delayed", "measured": True, "closed": True}})
    snapshot = {"price": 338.32, "last_trade_price": 338.32, "previous_close": 331.9,
                "market_status": "closed", "tier": "delayed_15m"}
    assert "**Last Price:** $338.32" in _quote_text("AAPL", snapshot=snapshot)
    late = {**snapshot, "last_trade_price": 329.03, "late_trading_change": -9.29}
    assert "**Last Extended-Hours Price:** $329.03" in _quote_text("AAPL", snapshot=late)
    odd_lot = {**snapshot, "last_trade_price": 338.31, "late_trading_change": 0.0, "market_status": "late_trading"}
    assert "**Last Price:** $338.31" in _quote_text("AAPL", snapshot=odd_lot)


def test_a_us_quote_whose_trade_is_older_than_the_live_window_says_when_it_printed(monkeypatch):
    monkeypatch.setattr(company, "venue_phase", lambda ref: MarketPhase.PRE)
    monkeypatch.setattr(company, "stamp_quote", lambda snap, ref=None: {
        "freshness": {"label": "live", "measured": True}})
    thin = int(datetime(2026, 11, 6, 9, 4, 20, tzinfo=timezone.utc).timestamp() * 1000)
    snapshot = {"last_trade_price": 1939.86, "last_trade_at": thin, "previous_close": 1950.0,
                "market_status": "early_trading", "early_trading_change": -10.14, "tier": "realtime"}
    assert "| **Price as of:** 2026-11-06 04:04:20 ET" in _quote_text("MKL", snapshot=snapshot)
    fresh = {**snapshot, "last_trade_at": int(datetime.now(timezone.utc).timestamp() * 1000)}
    assert "Price as of" not in _quote_text("MKL", snapshot=fresh)


def test_a_foreign_quote_says_when_its_price_printed():
    close = int(datetime(2026, 7, 1, 7, tzinfo=timezone.utc).timestamp() * 1000)
    text = _quote_text("600519.SH", snapshot={"price": 1446.44, "as_of": close, "tier": "realtime"})
    assert "| **Price as of:** 2026-07-01 15:00:00 UTC+8" in text


def test_a_sub_dollar_quote_keeps_four_places():
    quote = {"price": 0.3734, "change": 0.0123, "changePercentage": 3.4}
    text = _company_text(_inputs("SUBD", quote=quote, profile={"price": 0.3734}))
    assert "Current Price: $0.3734" in text
    assert "**Price:** $0.3734 (+0.0123 / +3.40%)" in text


def test_each_detail_figure_takes_its_own_precision():
    quote = {"price": 0.3734, "change": 0.0123, "changePercentage": 3.4, "yearLow": 0.21, "yearHigh": 12.0}
    text = _quote_text("SUBD", quote=quote)
    assert "| 52-Week Range | $0.2100 - $12.00 |" in text


def test_a_us_quote_keeps_two_places():
    text = _quote_text("AAPL", quote={"price": 331.9, "change": 1.25, "changePercentage": 0.38})
    assert "**Price:** $331.90 (+1.25 / +0.38%)" in text
