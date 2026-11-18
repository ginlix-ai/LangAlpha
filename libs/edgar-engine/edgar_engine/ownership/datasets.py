"""SEC Insider Transactions Data Sets (quarterly, Forms 3, 4 and 5) as the rows transaction_rows returns.

The data sets are SEC's own extraction of the same documents. Two things differ from a parse of the filing: numbers
are stored to 2 decimals, and in multi-line filings a line's nature of ownership (and rarely a price or holding)
sometimes sits on a neighbouring line. Their row order is not document order, so seq follows the data set's key.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .._datasets import number, read_table, sec_date
from .model import ReportingOwner
from .parse import TRANSACTION_COLUMNS

_TABLES = (("NONDERIV_TRANS", "nonderivative", "NONDERIV_TRANS_SK"), ("DERIV_TRANS", "derivative", "DERIV_TRANS_SK"))
# The data sets' per-field footnote columns, as the parse names the fields.
_FOOTNOTE_COLUMNS = {
    "security_title": "SECURITY_TITLE_FN", "exercise_price": "CONV_EXERCISE_PRICE_FN", "date": "TRANS_DATE_FN",
    "deemed_date": "DEEMED_EXECUTION_DATE_FN", "code": "EQUITY_SWAP_TRANS_CD_FN", "shares": "TRANS_SHARES_FN",
    "total_value": "TRANS_TOTAL_VALUE_FN", "price": "TRANS_PRICEPERSHARE_FN",
    "acquired_disposed": "TRANS_ACQUIRED_DISP_CD_FN", "exercise_date": "EXCERCISE_DATE_FN",
    "expiration_date": "EXPIRATION_DATE_FN", "underlying_title": "UNDLYNG_SEC_TITLE_FN",
    "underlying_shares": "UNDLYNG_SEC_SHARES_FN", "owned_after": "SHRS_OWND_FOLWNG_TRANS_FN",
    "value_owned_after": "VALU_OWND_FOLWNG_TRANS_FN", "direct_indirect": "DIRECT_INDIRECT_OWNERSHIP_FN",
    "nature_of_ownership": "NATURE_OF_OWNERSHIP_FN",
}
_DERIVATIVE_ONLY = {"exercise_price": "CONV_EXERCISE_PRICE", "exercise_date": "EXCERCISE_DATE",
                    "expiration_date": "EXPIRATION_DATE", "underlying_title": "UNDLYNG_SEC_TITLE",
                    "underlying_shares": "UNDLYNG_SEC_SHARES", "total_value": "TRANS_TOTAL_VALUE"}


def _flag(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower().map({"1": True, "true": True, "0": False, "false": False})


def _text(v) -> str | None:
    return v if isinstance(v, str) else None


def _owner(r) -> ReportingOwner:
    rel = {x.strip().lower() for x in str(_text(r.RPTOWNER_RELATIONSHIP) or "").split(",")}
    return ReportingOwner(cik=int(r.RPTOWNERCIK) if str(r.RPTOWNERCIK).isdigit() else None,
                          name=_text(r.RPTOWNERNAME), is_director="director" in rel, is_officer="officer" in rel,
                          officer_title=_text(r.RPTOWNER_TITLE), is_ten_percent_owner="tenpercentowner" in rel,
                          is_other="other" in rel, other_text=_text(r.RPTOWNER_TXT))


def _footnotes(t: pd.DataFrame) -> list[dict]:
    """footnote_ids per line, {field: [ids]} like the parse, from the "F1, F2" footnote columns."""
    cols = [(name, col) for name, col in _FOOTNOTE_COLUMNS.items() if col in t]
    if not cols:
        return [{} for _ in range(len(t))]
    return [{name: ids for (name, _), v in zip(cols, values)
             if isinstance(v, str) and (ids := [x.strip() for x in v.split(",") if x.strip()])}
            for values in zip(*(t[col] for _, col in cols))]


def _line_columns(t: pd.DataFrame, table: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "accession": t.ACCESSION_NUMBER, "table": table, "seq": t.groupby("ACCESSION_NUMBER").cumcount(),
        "derivative": table == "derivative", "security_title": t.SECURITY_TITLE.str.strip(),
        "date": sec_date(t.TRANS_DATE), "deemed_date": sec_date(t.DEEMED_EXECUTION_DATE),
        "form_type": t.TRANS_FORM_TYPE, "code": t.TRANS_CODE.str.strip().str.upper(),
        "equity_swap": _flag(t.EQUITY_SWAP_INVOLVED), "shares": number(t.TRANS_SHARES),
        "price": number(t.TRANS_PRICEPERSHARE), "acquired_disposed": t.TRANS_ACQUIRED_DISP_CD.str.strip().str.upper(),
        "owned_after": number(t.SHRS_OWND_FOLWNG_TRANS), "value_owned_after": number(t.VALU_OWND_FOLWNG_TRANS),
        "direct_indirect": t.DIRECT_INDIRECT_OWNERSHIP.str.strip().str.upper(),
        "nature_of_ownership": t.NATURE_OF_OWNERSHIP.str.strip(),
        "footnote_ids": _footnotes(t),
    })
    for name, col in _DERIVATIVE_ONLY.items():
        v = t[col] if col in t else pd.Series(None, index=t.index, dtype=object)
        out[name] = sec_date(v) if name.endswith("_date") else number(v) if name != "underlying_title" else v
    return out


def read_insider_dataset(path: str | Path) -> pd.DataFrame:
    """Every transaction line of one quarterly data set (the zip, or the directory it unzips to), with the columns
    of transaction_rows. Footnote ids come from the per-field footnote columns; the 10b5-1 flag is None in data sets
    from before it was reported."""
    path = Path(path)
    sub = read_table(path, "SUBMISSION").set_index("ACCESSION_NUMBER")
    own = read_table(path, "REPORTINGOWNER").drop_duplicates(["ACCESSION_NUMBER", "RPTOWNERCIK"])
    owners = {acc: [_owner(r) for r in g.itertuples(index=False)] for acc, g in own.groupby("ACCESSION_NUMBER")}
    parts = []
    for name, table, sk in _TABLES:
        t = read_table(path, name)
        t = t.assign(_sk=t[sk].astype(int)).sort_values(["ACCESSION_NUMBER", "_sk"])
        parts.append(_line_columns(t, table))
    rows = pd.concat(parts, ignore_index=True)
    acc = rows.accession
    first = {a: o[0] for a, o in owners.items()}
    rows["form"] = acc.map(sub.DOCUMENT_TYPE)
    rows["filed"] = sec_date(acc.map(sub.FILING_DATE))
    rows["period"] = sec_date(acc.map(sub.PERIOD_OF_REPORT))
    rows["issuer_cik"] = pd.to_numeric(acc.map(sub.ISSUERCIK), errors="coerce").astype("Int64")
    rows["issuer_name"] = acc.map(sub.ISSUERNAME)
    rows["issuer_ticker"] = acc.map(sub.ISSUERTRADINGSYMBOL)
    aff = sub.AFF10B5ONE if "AFF10B5ONE" in sub else pd.Series("", index=sub.index)
    rows["aff10b5_one"] = _flag(acc.map(aff).fillna(""))
    rows["owner_cik"] = pd.array(acc.map(lambda a: first[a].cik if a in first else None), dtype="Int64")
    rows["owner_ciks"] = acc.map(lambda a: ";".join(str(o.cik) for o in owners.get(a, ()) if o.cik is not None))
    rows["owner_name"] = acc.map(lambda a: first[a].name if a in first else None)
    rows["owner_relationship"] = acc.map(lambda a: first[a].relationship if a in first else None)
    return rows[list(TRANSACTION_COLUMNS)]
