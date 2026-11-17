"""Reading SEC's structured data sets (tab-separated tables in a zip, or the directory it unzips to)."""
from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd


def read_table(path: Path, name: str) -> pd.DataFrame:
    """Table name (SUBMISSION, INFOTABLE, ...) as strings, empty cells as NaN; a zip may nest it in a folder."""
    kw = dict(sep="\t", dtype=str, keep_default_na=False, na_values=[""], quoting=3)
    if path.is_dir():
        return pd.read_csv(path / f"{name}.tsv", **kw)
    with zipfile.ZipFile(path) as z:
        member = next(n for n in z.namelist() if n.rsplit("/", 1)[-1] == f"{name}.tsv")
        with z.open(member) as f:
            return pd.read_csv(f, **kw)


def sec_date(s: pd.Series) -> pd.Series:
    """The data sets' DD-MON-YYYY dates as YYYY-MM-DD."""
    return pd.to_datetime(s, format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")


def number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")
