"""SEC Form N-PORT Data Sets (quarterly, by filing date) in the engine's shapes.

A quarter's set holds every NPORT-P and NPORT-P/A filed in it, as filed. Its holdings table runs to millions of rows,
mostly debt, so it is streamed and cut to the asset categories asked for as it is read (a quarter's equity reads in
about 20 seconds with under 2 GB resident). The data sets carry no class ids (class_ids is null) and no document order:
seq follows the set's holding key, which SEC assigns in the order the document lists the investments. The set also
flattens each nested derivative's reference instrument into a holdings row of its own; those rows are dropped.
"""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from .._datasets import number, read_table, read_table_chunks, sec_date
from .parse import FILING_COLUMNS, FORMS, HOLDING_COLUMNS

_TEXT = pd.ArrowDtype(pa.string())


def _ends(a: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Text as filed but for its ends, null where empty (as the parse reads an identifier)."""
    a = pc.utf8_trim_whitespace(a)
    return pc.if_else(pc.equal(a, ""), pa.scalar(None, pa.string()), a)


def _arrow_text(a: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Text as filed with runs of whitespace closed up, null where empty (as the parse reads an element)."""
    return _ends(pc.replace_substring_regex(a, r"\s+", " "))


def _text(s: pd.Series) -> pd.Series:
    a = pa.array(s.astype(object).where(s.notna(), None), type=pa.string())
    return pd.Series(_arrow_text(a).to_pylist(), index=s.index, dtype=_TEXT)


def _upper(s: pd.Series) -> pd.Series:
    return _text(s).str.upper()


def _flag(s: pd.Series) -> pd.Series:
    return _upper(s).astype(object).map({"Y": True, "N": False})


def _identifiers(t: pa.Table) -> pd.DataFrame:
    """Per holding id: its first ISIN, its first ticker and its other identifiers as "desc:value" joined by ";", in
    the set's identifier order, each trimmed at its ends as the parse trims it (spaces inside one are part of it)."""
    t = pa.table({"HOLDING_ID": t["HOLDING_ID"], "_iid": pc.cast(t["IDENTIFIERS_ID"], pa.int64()),
                  **{c: _ends(t[c]) for c in ("IDENTIFIER_ISIN", "IDENTIFIER_TICKER", "OTHER_IDENTIFIER",
                                              "OTHER_IDENTIFIER_DESC")}}).sort_by([("_iid", "ascending")])
    other = pc.binary_join_element_wise(pc.fill_null(t["OTHER_IDENTIFIER_DESC"], ""),
                                        pc.fill_null(t["OTHER_IDENTIFIER"], ""), ":")
    has_other = pc.or_(pc.is_valid(t["OTHER_IDENTIFIER"]), pc.is_valid(t["OTHER_IDENTIFIER_DESC"]))
    g = t.group_by("HOLDING_ID", use_threads=False).aggregate([("IDENTIFIER_ISIN", "first"), ("IDENTIFIER_TICKER", "first")])
    o = (t.append_column("_other", other).filter(has_other)
         .group_by("HOLDING_ID", use_threads=False).aggregate([("_other", "list")]))
    others = pd.Series(pc.binary_join(o["_other_list"], ";").to_pylist(), index=o["HOLDING_ID"].to_pylist(),
                       dtype=_TEXT)
    out = pd.DataFrame({"isin": pd.Series(g["IDENTIFIER_ISIN_first"].to_pylist(), dtype=_TEXT).array,
                        "ticker": pd.Series(g["IDENTIFIER_TICKER_first"].to_pylist(), dtype=_TEXT).array},
                       index=g["HOLDING_ID"].to_pylist())
    return out.assign(other_ids=others)


def read_nport_dataset(path: str | Path, asset_categories: Iterable[str] | None = None,
                       accessions: Iterable[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(filings, holdings) of one data set with the columns of FILING_COLUMNS and HOLDING_COLUMNS, NPORT-P and
    NPORT-P/A only; holdings only of asset_categories (e.g. EQUITY) and filings only of accessions, when given."""
    path = Path(path)
    # The file number is the registrant's (REGISTRANT); SUBMISSION's own column is empty.
    sub = read_table(path, "SUBMISSION").drop(columns=["FILE_NUM"], errors="ignore")
    sub = sub[sub.SUB_TYPE.str.strip().str.upper().isin(FORMS)]
    if accessions is not None:
        sub = sub[sub.ACCESSION_NUMBER.isin(set(accessions))]
    x = (sub.merge(read_table(path, "REGISTRANT"), on="ACCESSION_NUMBER", how="left")
         .merge(read_table(path, "FUND_REPORTED_INFO"), on="ACCESSION_NUMBER", how="left"))
    filings = pd.DataFrame({
        "accession": x.ACCESSION_NUMBER, "form": _upper(x.SUB_TYPE), "filed": sec_date(x.FILING_DATE),
        "cik": pd.to_numeric(x.CIK, errors="coerce").astype("Int64"), "registrant": _text(x.REGISTRANT_NAME),
        "registrant_lei": _text(x.LEI), "file_number": _text(x.FILE_NUM), "series_id": _upper(x.SERIES_ID),
        "series_name": _text(x.SERIES_NAME), "series_lei": _text(x.SERIES_LEI), "class_ids": None,
        "report_date": sec_date(x.REPORT_DATE), "fiscal_year_end": sec_date(x.REPORT_ENDING_PERIOD),
        "total_assets": number(x.TOTAL_ASSETS), "total_liabilities": number(x.TOTAL_LIABILITIES),
        "net_assets": number(x.NET_ASSETS), "is_final": _flag(x.IS_LAST_FILING),
    }).drop_duplicates("accession").reset_index(drop=True)
    wanted = pa.array(filings.accession.astype(str).tolist(), pa.string())
    cats = None if asset_categories is None else pa.array(list(asset_categories), pa.string())

    def keep(b: pa.RecordBatch) -> pa.Array:
        # A nested derivative's reference instrument (Item C.11.c.iii.1, derivAddlInfo) is flattened into a row of
        # its own with no key to its parent; it is no investment of Part C, and alone has neither a payoff profile
        # nor a restricted flag, which every investment states.
        own = pc.or_(pc.is_valid(b["PAYOFF_PROFILE"]), pc.is_valid(b["IS_RESTRICTED_SECURITY"]))
        hit = pc.and_(pc.is_in(b["ACCESSION_NUMBER"], value_set=wanted), own)
        return hit if cats is None else pc.and_(hit, pc.is_in(pc.utf8_trim_whitespace(b["ASSET_CAT"]), value_set=cats))

    t = read_table_chunks(path, "FUND_REPORTED_HOLDING", keep)
    if t.num_rows == 0:
        return filings[list(FILING_COLUMNS)], pd.DataFrame(columns=list(HOLDING_COLUMNS))
    t = t.append_column("_id", pc.cast(t["HOLDING_ID"], pa.int64())).sort_by(
        [("ACCESSION_NUMBER", "ascending"), ("_id", "ascending")])
    ids = read_table_chunks(path, "IDENTIFIERS", lambda b: pc.is_in(b["HOLDING_ID"], value_set=t["HOLDING_ID"]))
    idf = _identifiers(ids)
    del ids
    def text(c):
        return pd.Series(pd.arrays.ArrowExtensionArray(_arrow_text(t[c])))

    def upper(c):
        return text(c).str.upper()

    def num(c):
        return pd.Series(pc.cast(t[c], pa.float64()).to_numpy(zero_copy_only=False))

    acc = t["ACCESSION_NUMBER"].to_pandas()
    hid = t["HOLDING_ID"].to_pandas()
    f = filings.set_index("accession")
    holdings = pd.DataFrame({
        "accession": acc, "series_id": acc.map(f.series_id), "report_date": acc.map(f.report_date),
        "seq": acc.groupby(acc).cumcount(), "name": text("ISSUER_NAME"), "lei": text("ISSUER_LEI"),
        "title": text("ISSUER_TITLE"), "cusip": upper("ISSUER_CUSIP"),
        "isin": hid.map(idf["isin"]), "ticker": hid.map(idf["ticker"]), "other_ids": hid.map(idf["other_ids"]),
        "balance": num("BALANCE"), "units": text("UNIT"), "other_units": text("OTHER_UNIT_DESC"),
        "currency": text("CURRENCY_CODE"), "exchange_rate": num("EXCHANGE_RATE"), "value_usd": num("CURRENCY_VALUE"),
        "pct_val": num("PERCENTAGE"), "payoff_profile": text("PAYOFF_PROFILE"), "asset_cat": text("ASSET_CAT"),
        "other_asset": text("OTHER_ASSET"), "issuer_cat": text("ISSUER_TYPE"), "other_issuer": text("OTHER_ISSUER"),
        "country": text("INVESTMENT_COUNTRY"), "restricted": upper("IS_RESTRICTED_SECURITY").astype(object).map(
            {"Y": True, "N": False}),
        "fair_value_level": text("FAIR_VALUE_LEVEL"), "derivative_cat": text("DERIVATIVE_CAT"),
    })
    return filings[list(FILING_COLUMNS)], holdings[list(HOLDING_COLUMNS)]
