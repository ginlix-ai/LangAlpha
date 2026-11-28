"""SEC Form 13F Data Sets (three-month filing windows since 2024, quarterly before) in the engine's shapes.

A window holds every 13F filed in it, so it carries stray older periods: select on period. Values are as filed
(the data set copies a filer's unit errors); value_usd applies the same per-report unit resolution parse_13f does.
The data sets carry no schema version, so the filing date stands in for it: a report filed since the dollar schema
took effect is read under it whatever its period (a late or amended report for a 2022 quarter reports dollars).

A window's information table runs to millions of lines over a few thousand filings and securities, so it is read
with Arrow: each text column as codes into its distinct values, each number column as integers.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from .._datasets import number, read_arrow_table, read_table, sec_date
from .parse import HOLDING_COLUMNS
from .units import resolve_value_units

DOLLAR_SCHEMA = "X0202"
DOLLAR_SCHEMA_FROM = "2023-01-03"

FILING_COLUMNS = ("accession", "filer_cik", "filer_name", "form", "period", "filed", "report_type", "amendment_no",
                  "amendment_type", "declared_entries", "declared_value", "confidential_omitted")

_CODES = pa.dictionary(pa.int32(), pa.string())
_NUMBER = "number"  # a number column: integers, floats when a cell is empty


def _strip_upper(s: str) -> str:
    return pc.utf8_upper(pa.scalar(s.strip(), pa.string())).as_py()


# The INFOTABLE columns a line is read from: (column, INFOTABLE column, _NUMBER or the text column's cleaner, applied
# to each distinct value; None keeps it as filed). accession, filer_cik, period, seq and value_usd are derived.
_LINE_COLUMNS = (
    ("issuer", "NAMEOFISSUER", str.strip), ("title_class", "TITLEOFCLASS", str.strip),
    ("cusip", "CUSIP", _strip_upper), ("figi", "FIGI", None), ("value", "VALUE", _NUMBER),
    ("shares", "SSHPRNAMT", _NUMBER), ("sh_prn", "SSHPRNAMTTYPE", _strip_upper), ("put_call", "PUTCALL", str.strip),
    ("discretion", "INVESTMENTDISCRETION", _strip_upper), ("other_manager", "OTHERMANAGER", None),
    ("vote_sole", "VOTING_AUTH_SOLE", _NUMBER), ("vote_shared", "VOTING_AUTH_SHARED", _NUMBER),
    ("vote_none", "VOTING_AUTH_NONE", _NUMBER),
)
_TEXT = ("ACCESSION_NUMBER",) + tuple(source for _, source, clean in _LINE_COLUMNS if clean != _NUMBER)
_NUMBERS = ("INFOTABLE_SK",) + tuple(source for _, source, clean in _LINE_COLUMNS if clean == _NUMBER)


def _upper(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()


def _holds_line_break(t: pa.Table) -> bool:
    """Whether a text cell of t holds a line break. SEC's windows hold none, while a quote left open runs on through
    the lines after it into one cell, where Arrow ends the table without a word and read_table raises."""
    for col in t.columns:
        for chunk in col.chunks:
            values = chunk.dictionary if pa.types.is_dictionary(chunk.type) else chunk
            if pa.types.is_string(values.type) and pc.any(pc.match_substring_regex(values, "[\r\n]")).as_py():
                return True
    return False


def _infotable(path: Path) -> pa.Table:
    """INFOTABLE with text columns as codes and number columns as integers. A number column holding anything but
    whole numbers is read as text instead, for _numbers to convert the way number() does; a table Arrow cannot read
    as text (a line short of cells, a line break outside quotes) or with a line break in a cell is read by read_table,
    which fills the missing cells or raises as it always has."""
    text = dict.fromkeys(_TEXT, _CODES)
    for numbers in (pa.int64(), pa.string()):
        try:
            t = read_arrow_table(path, "INFOTABLE", {**text, **dict.fromkeys(_NUMBERS, numbers)})
        except pa.ArrowInvalid as e:
            if str(e).startswith("CSV parse error"):
                break
            continue
        if not _holds_line_break(t):
            return t
        break
    t = read_table(path, "INFOTABLE")
    return pa.table({c: pa.array(t[c], pa.string(), from_pandas=True) for c in _NUMBERS}
                    | {c: pa.array(t[c], pa.string(), from_pandas=True).dictionary_encode() for c in _TEXT})


def _numbers(col: pa.ChunkedArray) -> pa.Array:
    """A number column as number() makes it of the same text: integers, floats when any cell is empty (an integer
    past 2**53 rounded, as pandas rounds it)."""
    if pa.types.is_integer(col.type):
        col = col.combine_chunks()
        return col.cast(pa.float64(), safe=False) if col.null_count else col
    return pa.array(number(col.to_pandas()), from_pandas=True)


def _recode(col: pa.ChunkedArray, clean=None) -> pa.DictionaryArray:
    """One dictionary array of a dictionary column whose chunks share a dictionary: only the values its rows use,
    each passed through clean (a str method), each result once."""
    col = col.combine_chunks()
    used = pc.unique(col.indices).drop_null()
    values = col.dictionary.take(used)
    if clean is not None:
        values = pa.array([clean(v) for v in values.to_pylist()], pa.string())
    encoded = pc.dictionary_encode(values)
    remap = np.zeros(len(col.dictionary), np.int32)
    remap[used.to_numpy()] = encoded.indices.to_numpy()
    return pa.DictionaryArray.from_arrays(pa.array(remap).take(col.indices), encoded.dictionary)


def _kept_order(accession: pa.ChunkedArray, sk: pa.ChunkedArray, kept_accessions: pd.Series) -> pa.Array:
    """The indices of the lines of kept_accessions, ordered by accession and then INFOTABLE_SK, worked out on the
    accession codes rather than their strings."""
    names = accession.chunk(0).dictionary if accession.num_chunks else pa.array([], pa.string())
    wanted = pc.is_in(names, value_set=pa.array(kept_accessions, pa.string()))
    kept = pc.indices_nonzero(pa.chunked_array([wanted.take(c.indices) for c in accession.chunks], pa.bool_()))
    rank = pc.rank(names, sort_keys="ascending")
    sk = sk.take(kept)
    if sk.null_count:
        raise ValueError("a line of a kept filing has no INFOTABLE_SK")
    if not pa.types.is_integer(sk.type):
        sk = pa.array(sk.to_pandas().astype(int))
    by = pa.table({"accession": pa.chunked_array([rank.take(c.indices) for c in accession.chunks], rank.type)
                   .take(kept), "sk": sk})
    return kept.take(pc.sort_indices(by, sort_keys=[("accession", "ascending"), ("sk", "ascending")]))


def read_13f_dataset_table(path: str | Path, period: str | None = None) -> tuple[pd.DataFrame, pa.Table]:
    """read_13f_dataset with the rows as an Arrow table: the same columns and values, each text column dictionary
    encoded, each number column int64 (float64 when it has an empty cell)."""
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
        "confidential_omitted": _upper(x.get("ISCONFIDENTIALOMITTED", pd.Series(index=x.index, dtype=object)))
        .map({"Y": True, "N": False}).astype("boolean"),
    })
    filings = filings[filings.form.isin(["13F-HR", "13F-HR/A"])]
    if period:
        filings = filings[filings.period == period]
    filings = filings.drop_duplicates("accession").reset_index(drop=True)

    # Each step lets go of what it no longer needs and hands the freed memory back (release_unused trims the system
    # allocator's heap too), so the read peaks at what it holds, not at what it once held.
    pool = pa.default_memory_pool()
    t = _infotable(path).unify_dictionaries()
    cols = dict(zip(t.column_names, t.columns))
    del t
    pool.release_unused()
    order = _kept_order(cols["ACCESSION_NUMBER"], cols.pop("INFOTABLE_SK"), filings.accession)
    pool.release_unused()
    rows = {"accession": _recode(cols.pop("ACCESSION_NUMBER").take(order))}

    def take(names: set[str]) -> None:
        """The named line columns' kept lines in order; each whole column is let go as it is taken, so one is copied
        at a time."""
        for name, source, clean in _LINE_COLUMNS:
            if name in names:
                col = cols.pop(source).take(order)
                rows[name] = _numbers(col) if clean == _NUMBER else _recode(col, clean)

    # The columns the value units are resolved from first, so the resolution's working copies are gone before the
    # other columns are taken.
    take({"value", "shares", "sh_prn", "put_call"})
    accession = rows["accession"]
    codes = accession.indices.to_numpy(zero_copy_only=False)
    first = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]]) if len(codes) else np.array([], np.int64)
    f = filings.set_index("accession").reindex(accession.dictionary.to_pylist())
    reports = pd.DataFrame({
        "accession": f.index, "period": f.period,
        "schema_version": [DOLLAR_SCHEMA if isinstance(d, str) and d >= DOLLAR_SCHEMA_FROM else None for d in f.filed],
    })
    value = rows["value"].to_numpy(zero_copy_only=False)
    lines = pd.DataFrame({"accession": pd.Categorical.from_codes(codes, categories=pd.Index(f.index)),
                          "value": value, "shares": rows["shares"].to_numpy(zero_copy_only=False),
                          "sh_prn": rows["sh_prn"].to_pandas(), "put_call": rows["put_call"].to_pandas()})
    units = resolve_value_units(lines, reports)
    del lines
    pool.release_unused()
    take({name for name, _, _ in _LINE_COLUMNS} - rows.keys())
    multiplier = np.array([units[a].multiplier for a in f.index], dtype=np.int64)[codes]
    periods = pc.dictionary_encode(pa.array(f.period, pa.string(), from_pandas=True))
    rows |= {
        "filer_cik": pa.array(f.filer_cik.array, pa.int64()).take(accession.indices),
        "period": pa.DictionaryArray.from_arrays(periods.indices.take(accession.indices), periods.dictionary),
        "seq": pa.array(np.arange(len(codes)) - np.repeat(first, np.diff(np.r_[first, len(codes)])), pa.int64()),
        "value_usd": pa.array(value * multiplier, from_pandas=True),
    }
    return filings[list(FILING_COLUMNS)], pa.table({c: rows.pop(c) for c in HOLDING_COLUMNS})


def read_13f_dataset(path: str | Path, period: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(filings, rows) of one data set, holdings forms only, optionally one period (YYYY-MM-DD), with the columns
    of FILING_COLUMNS and HOLDING_COLUMNS."""
    filings, rows = read_13f_dataset_table(path, period)
    columns = dict(zip(rows.column_names, rows.columns))
    del rows
    frame = {}
    for name in list(columns):
        col = columns.pop(name)
        if pa.types.is_dictionary(col.type):
            frame[name] = pd.Series(col.cast(pa.string()).to_pandas(), dtype="str")
        elif name == "filer_cik":
            frame[name] = col.to_pandas().astype("Int64")
        else:
            frame[name] = col.to_pandas()
    return filings, pd.DataFrame(frame)
