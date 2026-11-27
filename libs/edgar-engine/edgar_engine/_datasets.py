"""Reading SEC's structured data sets (tab-separated tables in a zip, or the directory it unzips to)."""
from __future__ import annotations

import csv
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv


def _member(z: zipfile.ZipFile, name: str) -> str:
    return next(n for n in z.namelist() if n.rsplit("/", 1)[-1] == f"{name}.tsv")


def read_table(path: Path, name: str) -> pd.DataFrame:
    """Table name (SUBMISSION, INFOTABLE, ...) as strings, empty cells as NaN; a zip may nest it in a folder. Raises
    ValueError when a cell opens a quote it never closes, which would join the rows after it into that cell."""
    if path.is_dir():
        return _read(lambda: (path / f"{name}.tsv").open("rb"), name)
    with zipfile.ZipFile(path) as z:
        member = _member(z, name)
        return _read(lambda: z.open(member), name)


def _read(open_table, name: str) -> pd.DataFrame:
    with open_table() as f:
        t = pd.read_csv(f, sep="\t", dtype=str, keep_default_na=False, na_values=[""])
    with open_table() as f:
        if len(t) >= _lines(f) - 1:
            return t
    with open_table() as f:
        if _quote_ran_on(f, len(t)):
            raise ValueError(f"{name}: a cell opens a quote it never closes, joining the rows after it")
    return t


def _lines(f) -> int:
    """Lines in a binary file, a last line with no line break included."""
    n, last = 0, b"\n"
    while chunk := f.read(1 << 24):
        n, last = n + chunk.count(b"\n"), chunk[-1:]
    return n + (last != b"\n")


def _quote_ran_on(f, rows: int) -> bool:
    """Whether a table read as rows rows lost some into a quoted cell: every line after the header that is not blank
    holds a whole row, and there are more such lines than rows. A line's cells are split at its quotes when they
    pair up within it, else at every tab. A cell SEC quotes because it holds a line break leaves a line short of a
    row, so it never reads as lost rows."""
    cells = f.readline().count(b"\t") + 1
    lines = 0
    for line in f:
        if not line.strip():
            continue
        if b'"' in line and line.count(b'"') % 2 == 0:
            n = len(next(csv.reader([line.rstrip(b"\r\n").decode("utf-8", "replace")], delimiter="\t")))
        else:
            n = line.count(b"\t") + 1
        if n != cells:
            return False
        lines += 1
    return lines > rows


def read_arrow_table(path: Path, name: str, column_types: dict[str, pa.DataType]) -> pa.Table:
    """The columns of column_types of table name, in those Arrow types, cells split and unquoted as read_table
    splits them (a quoted cell may hold a tab or a line break) and empty cells null."""
    options = dict(
        parse_options=pacsv.ParseOptions(delimiter="\t", newlines_in_values=True),
        convert_options=pacsv.ConvertOptions(column_types=column_types, include_columns=list(column_types),
                                             null_values=[""], strings_can_be_null=True))
    if path.is_dir():
        return pacsv.read_csv(path / f"{name}.tsv", **options)
    with zipfile.ZipFile(path) as z, z.open(_member(z, name)) as f:
        return pacsv.read_csv(f, **options)


def sec_date(s: pd.Series) -> pd.Series:
    """The data sets' DD-MON-YYYY dates as YYYY-MM-DD."""
    return pd.to_datetime(s, format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")


def number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")
