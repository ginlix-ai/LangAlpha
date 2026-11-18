"""Reads public filings from EDGAR (pytest -m network)."""
import pytest

from edgar_engine import get_13f_holdings, get_insider_filings
from edgar_engine.ownership import insider_statistics, transaction_rows
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
