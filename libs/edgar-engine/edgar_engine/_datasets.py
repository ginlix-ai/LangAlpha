"""Reading SEC's structured data sets (tab-separated tables in a zip, or the directory it unzips to)."""
from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv


def _member(z: zipfile.ZipFile, name: str) -> str:
    return next(n for n in z.namelist() if n.rsplit("/", 1)[-1] == f"{name}.tsv")


@contextmanager
def _open(path: Path, name: str) -> Iterator[IO[bytes]]:
    """Table name's file, in the directory or the zip (which may nest it in a folder)."""
    if path.is_dir():
        with open(path / f"{name}.tsv", "rb") as f:
            yield f
        return
    with zipfile.ZipFile(path) as z, z.open(_member(z, name)) as f:
        yield f


def read_table(path: Path, name: str) -> pd.DataFrame:
    """Table name (SUBMISSION, INFOTABLE, ...) as strings, empty cells as NaN; a zip may nest it in a folder. Raises
    ValueError when a cell opens a quote it never closes, which would join the rows after it into that cell."""
    return _read(lambda: _open(path, name), name)


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


# Bytes of a table read per block by read_table_chunks.
CHUNK_BYTES = 64 << 20


def read_table_chunks(path: Path, name: str, keep: Callable[[pa.RecordBatch], pa.Array]) -> pa.Table:
    """Table name streamed CHUNK_BYTES at a time, each block cut to the rows the mask keep(block) marks before the
    next is read, so a table larger than memory is held only as its kept rows. Cells are strings, empty ones null,
    split and unquoted as read_table does (a quoted cell may hold a tab, a line break or a doubled quote, read as one),
    and a cell that opens a quote it never closes raises ValueError as there."""
    with _open(path, name) as f:
        columns = next(filter(None, csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", newline=""), delimiter="\t")), [])
    with _open(path, name) as f:
        raw = _Counted(f)
        reader = pacsv.open_csv(
            raw, read_options=pacsv.ReadOptions(block_size=CHUNK_BYTES),
            parse_options=pacsv.ParseOptions(delimiter="\t", newlines_in_values=True),
            convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in columns},
                                                 strings_can_be_null=True, null_values=[""]))
        parts, rows, last = [], 0, None
        for batch in reader:
            if batch.num_rows:
                rows += batch.num_rows
                last = batch.column(batch.num_columns - 1)[batch.num_rows - 1].as_py() or ""
            parts.append(batch.filter(keep(batch), null_selection_behavior="drop"))
    if last is not None:
        cell = last.encode("utf-8").replace(b'"', b'""')
        tail = raw.tail
        if len(cell) + 2 > len(tail) and raw.size > len(tail):
            with _open(path, name) as f:
                tail = _last_bytes(f, len(cell) + 2)
        if _ends_in_open_cell(tail, cell):
            raise ValueError(f"{name}: a cell opens a quote it never closes, joining the rows after it")
    if rows < raw.lines() - 1:
        with _open(path, name) as f:
            if _quote_ran_on(f, rows):
                raise ValueError(f"{name}: a cell opens a quote it never closes, joining the rows after it")
    return pa.Table.from_batches(parts, schema=reader.schema)


def _ends_in_open_cell(tail: bytes, cell: bytes) -> bool:
    """Whether a file ending in tail ends inside a quote that opens its last cell, cell (the last row's last cell as
    read, its quotes doubled again, empty when null): the reader then takes the rest of the file, line breaks and
    tabs included, as that cell, so the file ends in the cell's own bytes right after a quote that starts a cell. A
    cell whose quote closes ends in that quote and the line's end instead, which is read as closed even where both
    fit (a cell holding only a line break), and an unquoted one does not follow a quote."""
    if tail.rstrip(b"\r\n").endswith(b'"' + cell + b'"'):
        return False
    at = len(tail) - len(cell)
    return (at >= 1 and tail.endswith(cell) and tail[at - 1:at] == b'"'
            and (at == 1 or tail[at - 2:at - 1] in (b"\t", b"\n")))


def _last_bytes(f: IO[bytes], n: int) -> bytes:
    """The last n bytes of a binary file, read through."""
    tail = b""
    while chunk := f.read(1 << 24):
        tail = (tail + chunk[-n:])[-n:]
    return tail


class _Counted:
    """A binary file read through, counting its bytes and line breaks and keeping its last _KEEP bytes, so
    read_table_chunks checks what it read without reading the file again."""

    _KEEP = 1 << 20

    def __init__(self, f: IO[bytes]):
        self._f, self._breaks, self.size, self.tail = f, 0, 0, b""

    def read(self, n: int = -1) -> bytes:
        chunk = self._f.read(n)
        if chunk:
            self._breaks += chunk.count(b"\n")
            self.size += len(chunk)
            self.tail = (self.tail + chunk[-self._KEEP:])[-self._KEEP:]
        return chunk

    @property
    def closed(self) -> bool:
        return False

    def lines(self) -> int:
        """Lines read, a last line with no line break included."""
        return self._breaks + (self.size > 0 and not self.tail.endswith(b"\n"))


def sec_date(s: pd.Series) -> pd.Series:
    """The data sets' DD-MON-YYYY dates as YYYY-MM-DD."""
    return pd.to_datetime(s, format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")


def number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")
