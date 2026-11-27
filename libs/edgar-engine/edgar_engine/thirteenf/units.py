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
from functools import lru_cache
from typing import Iterable

import numpy as np
import pandas as pd
from edgar.thirteenf import units as _edgartools_units
from edgar.thirteenf.units import resolve_value_unit as _edgartools_resolver

from .model import ThirteenFFiling, UnitResolution

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


def value_multiplier(filing: ThirteenFFiling) -> tuple[float, str]:
    """What a report's value column is multiplied by for whole dollars, and the status of that: 1000.0 and "rescaled"
    when its schema and period say thousands (before the 2023 schema) or its own rows' implied prices say thousands
    unambiguously, else 1.0 and "as_filed" (whole dollars since the 2023 schema)."""
    by_filing = _by_metadata(filing.schema_version, filing.period).unit == "thousands"
    unit = filing.value_unit
    mult = 1000.0 if by_filing or (unit.unit == "thousands" and not unit.ambiguous) else 1.0
    return mult, "as_filed" if mult == 1.0 else "rescaled"


@lru_cache(maxsize=None)
def _by_metadata(schema_version: str | None, period: str | None) -> UnitResolution:
    return resolve_value_unit([], schema_version, period)


def resolve_value_units(rows: pd.DataFrame, reports: pd.DataFrame) -> dict[str, UnitResolution]:
    """resolve_value_unit of many reports at once, the resolution it gives each: rows has accession, value, shares,
    sh_prn and put_call; reports one row per accession with schema_version and period (a missing one is None). A
    report with no rows resolves by its schema version and period alone.

    The resolver judges a report by its count of priceable rows and how many of them imply a sub-dollar price, so
    those two are counted for every report in one pass; the schema and period fallback is the resolver's own."""
    t = _edgartools_units
    codes, distinct = pd.factorize(rows.accession)
    codes = np.r_[pd.Index(reports.accession).get_indexer(distinct), -1][codes]
    priceable, values, shares = _priceable_mask(rows, strip_put_call=False)
    priceable &= (codes >= 0) & (shares < np.inf) & (values < np.inf)
    with np.errstate(over="ignore", divide="ignore"):
        price = values[priceable] / shares[priceable]
    at = codes[priceable]
    seen = (price > 0) & (price < np.inf)
    count = np.bincount(at[seen], minlength=len(reports))
    low = np.bincount(at[seen & (price < t._13F_IMPLIED_PRICE_THOUSANDS_THRESHOLD)], minlength=len(reports))
    out = {}
    for acc, schema_version, period, n, k in zip(reports.accession, reports.schema_version, reports.period,
                                                 count.tolist(), low.tolist()):
        fraction = k / n if n else None
        if (fraction is not None and fraction >= t._13F_FRAC_THOUSANDS
                and not t._13F_FRAC_DEAD_BAND[0] < fraction < t._13F_FRAC_DEAD_BAND[1]):
            out[acc] = UnitResolution("thousands", "implied_prices", n < t._13F_MIN_PRICEABLE_ROWS, n)
        else:
            fallback = _by_metadata(*(v if isinstance(v, str) else None for v in (schema_version, period)))
            out[acc] = UnitResolution(fallback.unit, fallback.source, True, n)
    return out


def _priceable_mask(rows: pd.DataFrame, strip_put_call: bool) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(priceable, value, shares): which rows a price can be read from (SH, no put or call, value and shares above
    zero; a missing cell is not priceable), and the two number columns as floats. edgartools' resolver takes a put or
    call of spaces for an option and the peer check takes it for none, so strip_put_call says which to follow."""
    values = pd.to_numeric(rows.value, errors="coerce").to_numpy(float, na_value=np.nan)
    shares = pd.to_numeric(rows.shares, errors="coerce").to_numpy(float, na_value=np.nan)
    put_call, distinct = pd.factorize(rows.put_call)
    blank = np.array([(str(v).strip() if strip_put_call else v) == "" for v in distinct] + [True], bool)
    with np.errstate(invalid="ignore"):
        priceable = ((rows.sh_prn == "SH").to_numpy(bool, na_value=False) & blank[put_call]
                     & (values > 0) & (shares > 0))
    return priceable, values, shares


def _priceable(rows: pd.DataFrame) -> pd.DataFrame:
    """The priceable share rows (_priceable_mask), with only the columns the price checks read."""
    keep = _priceable_mask(rows, strip_put_call=True)[0]
    return rows.loc[keep, [c for c in ("accession", "cusip", "filer_cik", "value", "shares") if c in rows.columns]]


def _per_filer_prices(x: pd.DataFrame) -> pd.DataFrame:
    """Median implied price (value per share, as filed) per CUSIP and filer, over priceable rows."""
    return x.assign(px=x.value / x.shares).groupby(["cusip", "filer_cik"]).px.median().reset_index()


def consensus_prices(rows: pd.DataFrame, min_filers: int = MIN_FILERS) -> pd.Series:
    """Median implied price per CUSIP across filers, one vote per filer; only CUSIPs with at least min_filers.
    rows has filer_cik, cusip, value, shares, sh_prn, put_call, all of one period."""
    g = _per_filer_prices(_priceable(rows)).groupby("cusip").px
    c = g.median()
    return c[g.size() >= min_filers]


def _runs(*keys: np.ndarray) -> np.ndarray:
    """Where each run of equal keys starts, in arrays sorted by those keys."""
    change = np.zeros(len(keys[0]), bool)
    change[:1] = True
    for k in keys:
        change[1:] |= k[1:] != k[:-1]
    return np.flatnonzero(change)


def _medians(values: np.ndarray, start: np.ndarray, size: np.ndarray) -> np.ndarray:
    """The median of each run of sorted values, a run being size values from start."""
    lo, hi = start + (size - 1) // 2, start + size // 2
    return np.where(size % 2 == 1, values[lo], (values[lo] + values[hi]) / 2)


def _others_medians(values: np.ndarray, start: np.ndarray, size: np.ndarray) -> np.ndarray:
    """For each element of runs of sorted values (start and size given per element), the median of the others in its
    run; NaN in a run of one."""
    i = np.arange(len(values)) - start
    last = size - 1

    def without_i(j: np.ndarray) -> np.ndarray:
        return values[start + np.clip(np.where(j < i, j, j + 1), 0, last)]

    m = size - 1
    odd = without_i(m // 2)
    even = (without_i(m // 2 - 1) + without_i(m // 2)) / 2
    return np.where(size < 2, np.nan, np.where(m % 2 == 1, odd, even))


def _filer_votes(cusip: np.ndarray, filer: np.ndarray, px: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """One vote per filer and CUSIP, the median of its prices: (vote, vote_cusip, voter), voter giving each price's
    vote."""
    order = np.lexsort((px, filer, cusip))
    first = _runs(cusip[order], filer[order])
    count = np.diff(np.r_[first, len(order)])
    voter = np.empty(len(order), np.int64)
    voter[order] = np.repeat(np.arange(len(first)), count)
    return _medians(px[order], first, count), cusip[order][first], voter


def _others_median_by_cusip(vote: np.ndarray, vote_cusip: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """For each vote, the median of the other votes for its CUSIP (NaN when it is the only one), and how many votes
    its CUSIP has."""
    order = np.lexsort((vote, vote_cusip))
    first = _runs(vote_cusip[order])
    count = np.diff(np.r_[first, len(order)])
    others, n_votes = np.empty(len(order)), np.empty(len(order), np.int64)
    others[order] = _others_medians(vote[order], np.repeat(first, count), np.repeat(count, count))
    n_votes[order] = np.repeat(count, count)
    return others, n_votes


def check_units(rows: pd.DataFrame) -> pd.DataFrame:
    """Per accession of rows (one period, every filer's rows): n_cmp (rows compared), median_lr (median log10 of a
    row's price over the median of the other filers' prices for its CUSIP), frac_low, frac_high, verdict (ok,
    too_small, too_large, mixed, undetermined) and scale_guess (the power of ten that brings the median back, 1
    unless too_small or too_large). A report in thousands reads too_small with scale_guess 1000. A report whose
    median row is off but with a fifth of its compared rows not off that way reads mixed, since one multiplier
    would break its correct rows. Every accession of rows has a verdict: undetermined, with n_cmp 0, when none
    of its rows could be compared. A filing's own vote never enters the consensus it is judged by. A row with no
    accession, CUSIP or filer, or whose price is not a finite number above zero, is left out of every vote and
    comparison."""
    cols = ["accession", "n_cmp", "median_lr", "frac_low", "frac_high", "verdict", "scale_guess"]
    accessions = pd.Index(rows.accession.unique(), name="accession")
    out = pd.DataFrame({"n_cmp": 0, "median_lr": np.nan, "frac_low": np.nan, "frac_high": np.nan}, index=accessions)
    priceable, values, shares = _priceable_mask(rows, strip_put_call=True)
    accession, names = pd.factorize(rows.accession)
    cusip = pd.factorize(rows.cusip)[0]
    filer = pd.factorize(rows.filer_cik)[0]
    with np.errstate(invalid="ignore", over="ignore", divide="ignore"):
        px = values / shares
    known = priceable & (accession >= 0) & (cusip >= 0) & (filer >= 0) & np.isfinite(px) & (px > 0)
    accession, cusip, filer = (a[known].astype(np.int32) for a in (accession, cusip, filer))
    px = px[known]
    vote, vote_cusip, voter = _filer_votes(cusip, filer, px)
    others, n_votes = _others_median_by_cusip(vote, vote_cusip)
    compared = n_votes[voter] - 1 >= MIN_FILERS
    if compared.any():
        lr = np.log10(px[compared] / others[voter[compared]])
        acc = accession[compared]
        order = np.lexsort((lr, acc))
        acc, lr = acc[order], lr[order]
        first = _runs(acc)
        count = np.diff(np.r_[first, len(acc)])
        out.loc[names[acc[first]], ["n_cmp", "median_lr", "frac_low", "frac_high"]] = np.column_stack([
            count, _medians(lr, first, count), np.add.reduceat(lr < -OFF, first) / count,
            np.add.reduceat(lr > OFF, first) / count])
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
