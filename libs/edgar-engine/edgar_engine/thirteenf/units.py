"""The unit of a 13F value column, and a sanity check of it against every other filer's prices.

Since the 2023 schema a 13F reports market value in whole dollars; before, in thousands. Some filers still report
thousands, repeat shares in the value column or mix scales. Two generic checks find them, and together they miss
what either misses alone:
- resolve_value_unit (edgartools' resolver) reads one report alone: its rows' implied prices, its schema and period;
- check_units compares each row's value per share with the median price every other filer implies for the same
  CUSIP in the same period, one vote per filer, and flags a report whose rows sit more than 100x away.
Neither names a filer: the evidence is the report's own rows and the quarter's.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
from edgar.thirteenf.units import resolve_value_unit as _edgartools_resolver

from .model import UnitResolution

MIN_FILERS = 5        # a CUSIP needs this many other filers for a consensus price
MIN_ROWS = 3          # a report needs this many comparable rows to be judged
OFF = 2.0             # |log10(price ratio)| beyond this is a scale error (100x), not a price difference
MIXED_SHARE = 0.2     # a report with this share of its rows off either way, and not 1 - this one way, mixes scales


def resolve_value_unit(rows: Iterable[tuple], schema_version: str | None, period: str | None) -> UnitResolution:
    """Unit of one report from (value, shares, sh_prn, put_call) rows, its schema version and period (YYYY-MM-DD),
    by edgartools' resolver: a majority of sub-dollar implied prices means thousands, else the schema and period."""
    df = pd.DataFrame(list(rows), columns=["Value", "SharesPrnAmount", "Type", "PutCall"])
    df["Type"] = df.Type.map({"SH": "Shares", "PRN": "Principal"})
    df["PutCall"] = df.PutCall.astype(object).fillna("")
    r = _edgartools_resolver(df, schema_version, datetime.strptime(period, "%Y-%m-%d") if period else None)
    return UnitResolution(r.unit, r.source, bool(r.ambiguous), int(r.priceable_rows or 0))


def _per_filer_prices(rows: pd.DataFrame) -> pd.DataFrame:
    """Median implied price (value per share, as filed) per CUSIP and filer, over priceable share rows."""
    pc = rows.put_call.fillna("").astype(str).str.strip()
    x = rows[(rows.sh_prn == "SH") & (pc == "") & (rows.shares > 0) & (rows.value > 0)]
    return x.assign(px=x.value / x.shares).groupby(["cusip", "filer_cik"]).px.median().reset_index()


def consensus_prices(rows: pd.DataFrame, min_filers: int = MIN_FILERS) -> pd.Series:
    """Median implied price per CUSIP across filers, one vote per filer; only CUSIPs with at least min_filers.
    rows has filer_cik, cusip, value, shares, sh_prn, put_call, all of one period."""
    g = _per_filer_prices(rows).groupby("cusip").px
    c = g.median()
    return c[g.size() >= min_filers]


def _others_median(sorted_px: np.ndarray) -> np.ndarray:
    """For each element of a sorted array, the median of the others."""
    n = len(sorted_px)
    i = np.arange(n)
    if n < 2:
        return np.full(n, np.nan)

    def without_i(j: int) -> np.ndarray:
        return np.where(j < i, sorted_px[min(j, n - 1)], sorted_px[min(j + 1, n - 1)])

    m = n - 1
    return without_i(m // 2) if m % 2 else (without_i(m // 2 - 1) + without_i(m // 2)) / 2


def check_units(rows: pd.DataFrame) -> pd.DataFrame:
    """Per accession of rows (one period, every filer's rows): n_cmp (rows compared), median_lr (median log10 of a
    row's price over the median of the other filers' prices for its CUSIP), frac_low, frac_high, verdict (ok,
    too_small, too_large, mixed, undetermined) and scale_guess (the power of ten that brings the median back, 1
    unless too_small or too_large). A report in thousands reads too_small with scale_guess 1000. A report whose
    median row is off but with a fifth of its compared rows not off that way reads mixed, since one multiplier
    would break its correct rows. Every accession of rows has a verdict: undetermined, with n_cmp 0, when none
    of its rows could be compared. A filing's own vote never enters the consensus it is judged by."""
    cols = ["accession", "n_cmp", "median_lr", "frac_low", "frac_high", "verdict", "scale_guess"]
    accessions = pd.Index(rows.accession.unique(), name="accession")
    out = pd.DataFrame({"n_cmp": 0, "median_lr": np.nan, "frac_low": np.nan, "frac_high": np.nan}, index=accessions)
    pf = _per_filer_prices(rows).sort_values(["cusip", "px"])
    pf["others"] = pf.groupby("cusip").px.transform(lambda s: _others_median(s.to_numpy()))
    pf["n_others"] = pf.groupby("cusip").px.transform("size") - 1
    pf = pf[pf.n_others >= MIN_FILERS]
    pc = rows.put_call.fillna("").astype(str).str.strip()
    x = rows[(rows.sh_prn == "SH") & (pc == "") & (rows.shares > 0) & (rows.value > 0)]
    x = x.merge(pf[["cusip", "filer_cik", "others"]], on=["cusip", "filer_cik"])
    if len(x):
        x = x.assign(lr=np.log10((x.value / x.shares) / x.others))
        x = x.assign(low=x.lr < -OFF, high=x.lr > OFF)
        g = x.groupby("accession")
        out.loc[g.size().index, ["n_cmp", "median_lr", "frac_low", "frac_high"]] = pd.DataFrame(
            {"n_cmp": g.size(), "median_lr": g.lr.median(), "frac_low": g.low.mean(), "frac_high": g.high.mean()})
    out["n_cmp"] = out.n_cmp.astype(int)
    out["verdict"] = "ok"
    out.loc[out.median_lr < -OFF, "verdict"] = "too_small"
    out.loc[out.median_lr > OFF, "verdict"] = "too_large"
    split = (((out.verdict == "too_small") & (out.frac_low <= 1 - MIXED_SHARE))
             | ((out.verdict == "too_large") & (out.frac_high <= 1 - MIXED_SHARE)))
    mixed = (out.verdict == "ok") & ((out.frac_low >= MIXED_SHARE) | (out.frac_high >= MIXED_SHARE))
    out.loc[split | mixed, "verdict"] = "mixed"
    out.loc[out.n_cmp < MIN_ROWS, "verdict"] = "undetermined"
    out["scale_guess"] = np.where(out.verdict.isin(["too_small", "too_large"]), 10 ** (-out.median_lr.round(0)), 1.0)
    return out.reset_index()[cols]


def unit_flags(resolution: UnitResolution | None, verdict: str | None) -> bool:
    """True when either check says the report's values are not whole dollars."""
    return bool((resolution is not None and resolution.unit != "dollars") or verdict in ("too_small", "too_large", "mixed"))
