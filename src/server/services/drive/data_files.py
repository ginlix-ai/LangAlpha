"""Files the Drive keeps as data, never as documents, by their names.

A CSV or JSON Lines file is UTF-8 text by its bytes, but it is a dataset: read
by code, often far larger than any document, and useless to fetch ahead to a
computer or to search line by line. With object storage configured these are
kept there at any size and are never text; the classification and any preview
read this one list.
"""

from __future__ import annotations

#: Lowercase suffixes, the dot included. A compound name (``prices.csv.gz``)
#: matches on its last suffix.
DATA_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".parquet",
        ".feather",
        ".arrow",
        ".h5",
        ".hdf5",
        ".jsonl",
        ".ndjson",
        ".npy",
        ".npz",
        ".pkl",
        ".xlsx",
        ".xls",
        ".sqlite",
        ".db",
        ".duckdb",
        ".sas7bdat",
        ".dta",
        ".zip",
        ".gz",
        ".zst",
        ".tar",
    }
)


def is_data_file(name: str) -> bool:
    """Whether a file named ``name`` (a path or its last part) is data."""
    base = name.rsplit("/", 1)[-1].lower()
    dot = base.rfind(".")
    return dot > 0 and base[dot:] in DATA_SUFFIXES
