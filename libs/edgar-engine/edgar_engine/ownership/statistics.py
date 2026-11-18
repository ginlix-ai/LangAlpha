"""Per-quarter insider statistics: how many lines acquired and disposed, the shares behind them, and the
open-market purchases and sales, by the calendar quarter of the transaction date.

This is the definition market-data vendors publish as quarterly insider statistics, reproduced from filings:
Forms 4 and 4/A (a form's spec says whether it counts; an amendment's lines count beside its original's), both
tables, a line repeated identically within one filing (every value of the line equal; footnotes are not compared,
as the vendors do not) counted once, and zero-share lines kept. A joint filing's line counts once for all its owners; related owners who
each file their own Form 4 for one transaction are counted once per filing.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .forms import form_spec
from .model import Transaction

STATISTICS_COLUMNS = (
    "issuer_cik", "year", "quarter", "acquired_transactions", "disposed_transactions", "acquired_disposed_ratio",
    "total_acquired", "total_disposed", "average_acquired", "average_disposed", "total_purchases", "total_sales",
)
# Every field that makes two lines of one filing the same line.
_IDENTICAL = [f for f in Transaction.__dataclass_fields__ if f not in ("seq", "footnote_ids")]


def lines_counted(rows: pd.DataFrame) -> pd.DataFrame:
    """The transaction lines the statistics count, with year and quarter of the transaction date.

    Raises ValueError on a line with no issuer CIK, which no statistic can be filed under, or with no accession, without
    which two filings' identical lines would count once (pass accession to parse_ownership).
    """
    if rows.empty:
        return rows.assign(year=pd.Series(dtype="Int64"), quarter=pd.Series(dtype="Int64"))
    if rows.issuer_cik.isna().any():
        raise ValueError("every line needs an issuer_cik")
    if rows.accession.isna().any():
        raise ValueError("every line needs an accession")
    d = rows[rows.form.map(lambda f: bool((s := form_spec(f)) and s.in_statistics))]
    key = [c for c in ["accession", *_IDENTICAL] if c in d]
    d = d[~d[key].astype(str).duplicated()]
    when = pd.to_datetime(d.date, format="%Y-%m-%d", errors="coerce")
    d = d.assign(year=when.dt.year.astype("Int64"), quarter=when.dt.quarter.astype("Int64"))
    return d[d.year.notna()]


def insider_statistics(rows: pd.DataFrame) -> pd.DataFrame:
    """One row per issuer and calendar quarter of transaction date (STATISTICS_COLUMNS), newest quarter first.

    rows has the columns of transaction_rows (or read_insider_dataset): at least accession, form, issuer_cik and the
    line columns. The ratio is acquired over disposed lines (0 when nothing was disposed), rounded to 4 places;
    the averages are shares per line (0 when there are no lines).
    """
    d = lines_counted(rows)
    if d.empty:
        return pd.DataFrame(columns=list(STATISTICS_COLUMNS))
    d = d.assign(shares=d.shares.fillna(0.0).astype(float))
    keys = ["issuer_cik", "year", "quarter"]
    a, z = d[d.acquired_disposed == "A"], d[d.acquired_disposed == "D"]
    out = pd.DataFrame({
        "acquired_transactions": a.groupby(keys).size(),
        "disposed_transactions": z.groupby(keys).size(),
        "total_acquired": a.groupby(keys).shares.sum(),
        "total_disposed": z.groupby(keys).shares.sum(),
        "total_purchases": d[d.code == "P"].groupby(keys).size(),
        "total_sales": d[d.code == "S"].groupby(keys).size(),
    }).reindex(d.groupby(keys).size().index).fillna(0.0)
    for c in ("acquired_transactions", "disposed_transactions", "total_purchases", "total_sales"):
        out[c] = out[c].astype(int)
    acq, disp = out.acquired_transactions, out.disposed_transactions
    out["acquired_disposed_ratio"] = np.where(disp > 0, acq / disp.where(disp > 0, 1), 0.0).round(4)
    out["average_acquired"] = np.where(acq > 0, out.total_acquired / acq.where(acq > 0, 1), 0.0)
    out["average_disposed"] = np.where(disp > 0, out.total_disposed / disp.where(disp > 0, 1), 0.0)
    out = out.reset_index().sort_values(["issuer_cik", "year", "quarter"], ascending=[True, False, False])
    return out[list(STATISTICS_COLUMNS)].reset_index(drop=True)
