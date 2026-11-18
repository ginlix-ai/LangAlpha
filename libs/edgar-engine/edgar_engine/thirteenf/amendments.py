"""Which reports make up a filer's holdings for a period.

Only holdings forms count (a 13F notice lists none). The latest original or RESTATEMENT is the base; a NEW
HOLDINGS amendment filed on or after it adds its rows. An amendment with no stated type is read as a restatement.
Reports filed the same day go original first, then by amendment number: the accession number names the filing
agent, not the order of filing.

Some filers mark a full re-filing as NEW HOLDINGS, which read literally counts their holdings twice, so a NEW
HOLDINGS amendment is a re-filing when most of its rows repeat a row of the holdings so far (same CUSIP, shares,
share or principal type and put or call; values may be repriced) and those repeats cover most of them. Over two
quarters of filings the share of repeated rows splits into amendments that add holdings (under a third repeated)
and re-filings (over two thirds), so the line sits between them; requiring the repeats to cover the holdings too
keeps a one-row correction from replacing the whole report. A NEW HOLDINGS amendment that re-files an earlier one
replaces it. Row count alone cannot tell a partial restatement from a correct short one, so none is guessed.
"""
from __future__ import annotations

from collections import Counter

import pandas as pd

from .forms import form_spec

# Share of both reports' rows that must repeat (CUSIP, shares, type, put/call) for one to re-file the other.
REPEAT_SHARE = 0.5
_REPEAT_KEY = ("cusip", "shares", "sh_prn", "put_call")


def _kind(form: str, amendment_type: object) -> str:
    spec = form_spec(form)
    if spec is None:
        raise ValueError(f"not a 13F form: {form!r}")
    if not spec.amendment:
        return "original"
    return "new" if isinstance(amendment_type, str) and "NEW HOLDINGS" in amendment_type.upper() else "restatement"


def _repeats(new: list[tuple], held: list[tuple]) -> int:
    remaining = Counter(held)
    hits = 0
    for k in new:
        if remaining[k] > 0:
            remaining[k] -= 1
            hits += 1
    return hits


def _refiles(new: list[tuple], held: list[tuple]) -> bool:
    if not new or not held:
        return False
    hits = _repeats(new, held)
    return hits >= REPEAT_SHARE * len(new) and hits >= REPEAT_SHARE * len(held)


def _row_keys(rows: pd.DataFrame) -> dict[str, list[tuple]]:
    cols = [c for c in _REPEAT_KEY if c in rows]
    r = rows[["accession", *cols]].copy()
    for c in cols:
        r[c] = r[c].astype(str) if c == "shares" else r[c].fillna("").astype(str).str.strip().str.upper()
    return {acc: list(g[cols].itertuples(index=False, name=None)) for acc, g in r.groupby("accession")}


def resolve_amendments(filings: pd.DataFrame, rows: pd.DataFrame | None = None) -> list[str]:
    """Accessions that make up each (filer_cik, period)'s holdings. filings has accession, filer_cik, period,
    form, filed, amendment_type and optionally amendment_no; rows (accession, cusip, shares and, when known, sh_prn
    and put_call) lets a repeating NEW HOLDINGS amendment be read as a re-filing. Raises ValueError on a form that
    is not a 13F or a filing with no filer CIK."""
    if filings.empty:
        return []
    if filings.filer_cik.isna().any():
        raise ValueError("every filing needs a filer_cik")
    f = filings.assign(kind=[_kind(fm, at) for fm, at in zip(filings.form, filings.amendment_type)])
    f = f[[form_spec(fm).holdings for fm in f.form]]
    number = f.amendment_no if "amendment_no" in f else pd.Series(0, index=f.index)
    f = f.assign(_amended=f.kind != "original", _no=pd.to_numeric(number, errors="coerce").fillna(0))
    f = f.sort_values(["filer_cik", "period", "filed", "_amended", "_no", "accession"])
    keys = _row_keys(rows[rows.accession.isin(set(f.accession))]) if rows is not None and len(rows) else {}
    keep: list[str] = []
    for _, g in f.groupby(["filer_cik", "period"], sort=False):
        base = None
        added: list[str] = []
        for r in g.itertuples(index=False):
            if r.kind != "new":
                base, added = r.accession, []
                continue
            new = keys.get(r.accession, [])
            held = keys.get(base, []) + [k for a in added for k in keys.get(a, [])]
            refiled = [a for a in added if _refiles(new, keys.get(a, []))]
            if base is not None and _refiles(new, held) \
                    and _repeats(new, keys.get(base, [])) >= REPEAT_SHARE * len(keys.get(base, [])):
                base, added = r.accession, []
            else:
                added = [a for a in added if a not in refiled] + [r.accession]
        keep += ([base] if base else []) + added
    return keep
