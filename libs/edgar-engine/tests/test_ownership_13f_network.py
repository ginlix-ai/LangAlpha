"""Reads public filings from EDGAR (pytest -m network)."""
import pytest

from edgar_engine import get_13f_holdings, get_form144_notices, get_insider_filings
from edgar_engine.ownership import RELATIONSHIPS, insider_statistics, notice_rows, transaction_rows
from edgar_engine.thirteenf import holding_rows

pytestmark = pytest.mark.network


def test_insider_filings_are_the_issuers_own():
    import pandas as pd

    filings = get_insider_filings(320193, start="2024-10-01", end="2024-10-31")
    assert filings and all(f.issuer_cik == 320193 for f in filings)
    stats = insider_statistics(pd.DataFrame([r for f in filings for r in transaction_rows(f)]))
    assert set(stats.issuer_cik) == {320193}


def test_13f_holdings_for_a_period():
    reports = get_13f_holdings(1067983, "2024-06-30")
    assert reports and all(f.period == "2024-06-30" and f.filer_cik == 1067983 for f in reports)
    rows = [r for f in reports for r in holding_rows(f)]
    assert any(r["cusip"] == "037833100" for r in rows)


def test_form144_notices_page_newest_first():
    first = get_form144_notices(1045810, limit=3)
    assert first.total >= 3 and first.next_offset == 3 and len(first.notices) == 3
    assert [n.filed for n in first.notices] == sorted((n.filed for n in first.notices), reverse=True)
    rows = [r for n in first.notices for r in notice_rows(n)]
    assert all(r["issuer_cik"] == 1045810 and set(r["relationship"]) <= set(RELATIONSHIPS) for r in rows)
    second = get_form144_notices(1045810, limit=3, offset=3)
    assert not {n.accession for n in first.notices} & {n.accession for n in second.notices}
