"""SEC Form 13F Data Sets (three-month filing windows since 2024, quarterly before) in the engine's shapes.

A window holds every 13F filed in it, so it carries stray older periods: select on period. Values are as filed
(the data set copies a filer's unit errors); value_usd applies the same per-report unit resolution parse_13f does.
The data sets carry no schema version, so the filing date stands in for it: a report filed since the dollar schema
took effect is read under it whatever its period (a late or amended report for a 2022 quarter reports dollars).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .._datasets import number, read_table, sec_date
from .parse import HOLDING_COLUMNS
from .units import resolve_value_unit

DOLLAR_SCHEMA = "X0202"
DOLLAR_SCHEMA_FROM = "2023-01-03"

FILING_COLUMNS = ("accession", "filer_cik", "filer_name", "form", "period", "filed", "report_type", "amendment_no",
                  "amendment_type", "declared_entries", "declared_value")


def _upper(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()


def read_13f_dataset(path: str | Path, period: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(filings, rows) of one data set, holdings forms only, optionally one period (YYYY-MM-DD), with the columns
    of FILING_COLUMNS and HOLDING_COLUMNS."""
    path = Path(path)
    x = (read_table(path, "SUBMISSION").merge(read_table(path, "COVERPAGE"), on="ACCESSION_NUMBER", how="left")
         .merge(read_table(path, "SUMMARYPAGE"), on="ACCESSION_NUMBER", how="left"))
    filings = pd.DataFrame({
        "accession": x.ACCESSION_NUMBER, "filer_cik": pd.to_numeric(x.CIK, errors="coerce").astype("Int64"),
        "filer_name": x.FILINGMANAGER_NAME, "form": x.SUBMISSIONTYPE, "period": sec_date(x.PERIODOFREPORT),
        "filed": sec_date(x.FILING_DATE), "report_type": _upper(x.REPORTTYPE),
        "amendment_no": pd.to_numeric(x.AMENDMENTNO, errors="coerce").astype("Int64"),
        "amendment_type": _upper(x.AMENDMENTTYPE),
        "declared_entries": pd.to_numeric(x.TABLEENTRYTOTAL, errors="coerce").astype("Int64"),
        "declared_value": number(x.TABLEVALUETOTAL),
    })
    filings = filings[filings.form.isin(["13F-HR", "13F-HR/A"])]
    if period:
        filings = filings[filings.period == period]
    filings = filings.drop_duplicates("accession").reset_index(drop=True)
    t = read_table(path, "INFOTABLE")
    t = t[t.ACCESSION_NUMBER.isin(set(filings.accession))]
    t = t.assign(_sk=t.INFOTABLE_SK.astype(int)).sort_values(["ACCESSION_NUMBER", "_sk"])
    f = filings.set_index("accession")
    rows = pd.DataFrame({
        "accession": t.ACCESSION_NUMBER, "filer_cik": t.ACCESSION_NUMBER.map(f.filer_cik),
        "period": t.ACCESSION_NUMBER.map(f.period), "seq": t.groupby("ACCESSION_NUMBER").cumcount(),
        "issuer": t.NAMEOFISSUER.str.strip(), "title_class": t.TITLEOFCLASS.str.strip(),
        "cusip": _upper(t.CUSIP), "figi": t.FIGI, "value": number(t.VALUE), "shares": number(t.SSHPRNAMT),
        "sh_prn": _upper(t.SSHPRNAMTTYPE), "put_call": t.PUTCALL.str.strip(),
        "discretion": _upper(t.INVESTMENTDISCRETION), "other_manager": t.OTHERMANAGER,
        "vote_sole": number(t.VOTING_AUTH_SOLE), "vote_shared": number(t.VOTING_AUTH_SHARED),
        "vote_none": number(t.VOTING_AUTH_NONE),
    }).reset_index(drop=True)
    def schema(acc: str) -> str | None:
        filed = f.at[acc, "filed"]
        return DOLLAR_SCHEMA if isinstance(filed, str) and filed >= DOLLAR_SCHEMA_FROM else None

    mult = {acc: resolve_value_unit(zip(g.value, g.shares, g.sh_prn, g.put_call), schema(acc), f.at[acc, "period"])
            .multiplier for acc, g in rows.groupby("accession")}
    rows["value_usd"] = rows.value * rows.accession.map(mult)
    return filings[list(FILING_COLUMNS)], rows[list(HOLDING_COLUMNS)]
