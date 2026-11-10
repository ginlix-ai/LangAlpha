"""Technical analysis with TradingView Pine v5 semantics.

Every function takes pandas Series (or the OHLCV DataFrame) and returns Series aligned to the
input index, NaN until the indicator is warmed up. Seeds and edge rules follow Pine, so values
match a TradingView chart of the same bars.
"""
from __future__ import annotations

import math
from collections import namedtuple

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from ._util import _fail, _short, as_int, bars_frame, is_daily, local_index, num, parse_time

BBands = namedtuple("BBands", "mid upper lower")
MACD = namedtuple("MACD", "macd signal hist")
Donchian = namedtuple("Donchian", "upper lower mid")
SuperTrend = namedtuple("SuperTrend", "line direction")
LinReg = namedtuple("LinReg", "value slope")
Stoch = namedtuple("Stoch", "k d")

__all__ = [
    "sma", "ema", "rma", "wma", "stdev", "highest", "lowest", "change", "roc", "rsi", "tr",
    "atr", "bbands", "macd", "donchian", "supertrend", "linreg", "zscore", "stoch", "crossover",
    "crossunder", "pivothigh", "pivotlow", "vwap",
]


# --------------------------------------------------------------------------- argument checks

def _as_series(x, call: str, arg: str = "src", *, numeric: bool = True) -> pd.Series:
    """Validate a Series argument; the messages are written for an agent that guessed wrong."""
    if isinstance(x, pd.DataFrame):
        _fail(call, arg, f"expected a pandas Series, got a DataFrame with columns {list(x.columns)}. "
                         f"Pass one column, e.g. df.close.")
    if isinstance(x, tuple):
        fields = getattr(x, "_fields", None)
        what = f"a tuple ({', '.join(fields)})" if fields else f"a tuple of {len(x)} values"
        _fail(call, arg, f"expected a pandas Series, got {what}. Functions such as ta.bbands, ta.macd, "
                         f"ta.donchian, ta.supertrend and ta.linreg return several Series; unpack them "
                         f"first (e.g. mid, upper, lower = ta.bbands(df.close, 20, 2)) and pass one.")
    if not isinstance(x, pd.Series):
        if isinstance(x, (np.ndarray, list)):
            _fail(call, arg, f"expected a pandas Series indexed by date, got {type(x).__name__}. "
                             f"Wrap it: pd.Series(values, index=df.index).")
        _fail(call, arg, f"expected a pandas Series, got {type(x).__name__} {_short(x)}.")
    if not numeric:
        return x
    try:
        s = x.astype("float64")
    except (TypeError, ValueError):
        _fail(call, arg, f"Series has non-numeric dtype {x.dtype}; it must hold numbers.")
    # An infinite value is a missing one: the indicators pass over NaN and carry their state.
    return s.where(np.isfinite(s)) if np.isinf(s.to_numpy()).any() else s



# --------------------------------------------------------------------------- numpy kernels

def _out(arr: np.ndarray, index) -> pd.Series:
    return pd.Series(arr, index=index, dtype="float64")


def _roll(x: np.ndarray, n: int, fn) -> np.ndarray:
    """``fn`` over each window of the last n valid values. Pine's built-ins ignore an na
    input rather than carry it through the window, so a gap leaves its own bar empty and
    the windows close over it."""
    out = np.full(len(x), np.nan)
    valid = ~np.isnan(x)
    v = x[valid]
    if len(v) >= n:
        rolled = np.full(len(v), np.nan)
        # Some BLAS builds flag matmul spuriously, and NaN results (a zero range) are
        # expected; the agent's output should not fill with RuntimeWarnings.
        with np.errstate(all="ignore"):
            rolled[n - 1:] = fn(sliding_window_view(v, n))
        out[valid] = rolled
    return out


def _clean(a: np.ndarray) -> np.ndarray:
    """Pine yields na where a float division is undefined; keep that instead of inf."""
    return np.where(np.isfinite(a), a, np.nan)


def _sma(x, n):
    return _roll(x, n, lambda w: w.mean(axis=1))


def _stdev(x, n):
    def f(w):
        d = w - w.mean(axis=1, keepdims=True)
        return np.sqrt((d * d).mean(axis=1))
    return _roll(x, n, f)


def _recursive(x: np.ndarray, n: int, kind: str) -> np.ndarray:
    """Pine ema/rma: seed with the SMA of the first n valid values, then recurse.

    The built-ins ignore an na input rather than restart (the `na(sum[1])` reset belongs
    to the reference script, not to `ta.ema`), so a gap leaves its bar empty and the
    state carries over it.
    """
    out = np.full(len(x), np.nan)
    prev = math.nan
    seed: list[float] = []
    alpha = 2.0 / (n + 1) if kind == "ema" else 1.0 / n
    for i, v in enumerate(x):
        if not math.isfinite(v):
            continue
        if math.isnan(prev):
            seed.append(float(v))
            if len(seed) < n:
                continue
            prev = sum(seed) / n
        elif kind == "ema":
            prev = alpha * v + (1.0 - alpha) * prev
        else:
            prev = (v + (n - 1) * prev) / n
        out[i] = prev
    return out


def _tr(h: np.ndarray, l: np.ndarray, c: np.ndarray) -> np.ndarray:
    pc = np.concatenate(([np.nan], c[:-1]))
    rng = h - l
    full = np.maximum(rng, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    return np.where(np.isnan(pc), rng, full)


def _ohlc(df, call, cols, *, times=False):
    df = bars_frame(df, call, cols, times=times)
    return [df[c].to_numpy(dtype="float64") for c in cols], df.index


# --------------------------------------------------------------------------- moving averages

def sma(src, n) -> pd.Series:
    s = _as_series(src, "ta.sma")
    n = as_int(n, "ta.sma", "n")
    return _out(_sma(s.to_numpy(), n), s.index)


def ema(src, n) -> pd.Series:
    s = _as_series(src, "ta.ema")
    n = as_int(n, "ta.ema", "n")
    return _out(_recursive(s.to_numpy(), n, "ema"), s.index)


def rma(src, n) -> pd.Series:
    s = _as_series(src, "ta.rma")
    n = as_int(n, "ta.rma", "n")
    return _out(_recursive(s.to_numpy(), n, "rma"), s.index)


def wma(src, n) -> pd.Series:
    s = _as_series(src, "ta.wma")
    n = as_int(n, "ta.wma", "n")
    w = np.arange(1, n + 1, dtype="float64")
    return _out(_roll(s.to_numpy(), n, lambda win: (win @ w) / w.sum()), s.index)


def stdev(src, n) -> pd.Series:
    """Population standard deviation (Pine's biased=true default)."""
    s = _as_series(src, "ta.stdev")
    n = as_int(n, "ta.stdev", "n")
    return _out(_stdev(s.to_numpy(), n), s.index)


def highest(src, n) -> pd.Series:
    s = _as_series(src, "ta.highest")
    n = as_int(n, "ta.highest", "n")
    return _out(_roll(s.to_numpy(), n, lambda w: w.max(axis=1)), s.index)


def lowest(src, n) -> pd.Series:
    s = _as_series(src, "ta.lowest")
    n = as_int(n, "ta.lowest", "n")
    return _out(_roll(s.to_numpy(), n, lambda w: w.min(axis=1)), s.index)


def change(src, n=1) -> pd.Series:
    s = _as_series(src, "ta.change")
    n = as_int(n, "ta.change", "n")
    x = s.to_numpy()
    out = np.full(len(x), np.nan)
    if len(x) > n:
        out[n:] = x[n:] - x[:-n]
    return _out(out, s.index)


def roc(src, n) -> pd.Series:
    s = _as_series(src, "ta.roc")
    n = as_int(n, "ta.roc", "n")
    x = s.to_numpy()
    out = np.full(len(x), np.nan)
    if len(x) > n:
        with np.errstate(divide="ignore", invalid="ignore"):
            out[n:] = 100.0 * (x[n:] - x[:-n]) / x[:-n]
    return _out(_clean(out), s.index)


def rsi(src, n) -> pd.Series:
    s = _as_series(src, "ta.rsi")
    n = as_int(n, "ta.rsi", "n")
    x = s.to_numpy()
    # Each change is from the last valid value, so the move across a gap counts and only the
    # gap's own bar is empty, as the averages below treat it.
    ch = np.full(len(x), np.nan)
    at = np.flatnonzero(~np.isnan(x))
    ch[at[1:]] = np.diff(x[at])
    up = np.where(np.isnan(ch), np.nan, np.maximum(ch, 0.0))
    dn = np.where(np.isnan(ch), np.nan, np.maximum(-ch, 0.0))
    ru, rd = _recursive(up, n, "rma"), _recursive(dn, n, "rma")
    with np.errstate(divide="ignore", invalid="ignore"):
        val = 100.0 - 100.0 / (1.0 + ru / rd)
    out = np.where(rd == 0, 100.0, np.where(ru == 0, 0.0, val))
    out = np.where(np.isnan(ru) | np.isnan(rd), np.nan, out)
    return _out(out, s.index)


# --------------------------------------------------------------------------- volatility / bands

def tr(df) -> pd.Series:
    (h, l, c), idx = _ohlc(df, "ta.tr", ("high", "low", "close"))
    return _out(_tr(h, l, c), idx)


def atr(df, n) -> pd.Series:
    n = as_int(n, "ta.atr", "n")
    (h, l, c), idx = _ohlc(df, "ta.atr", ("high", "low", "close"))
    return _out(_recursive(_tr(h, l, c), n, "rma"), idx)


def bbands(src, n, mult) -> BBands:
    s = _as_series(src, "ta.bbands")
    n = as_int(n, "ta.bbands", "n")
    mult = num(mult, "ta.bbands", "mult")
    x = s.to_numpy()
    mid, dev = _sma(x, n), mult * _stdev(x, n)
    return BBands(_out(mid, s.index), _out(mid + dev, s.index), _out(mid - dev, s.index))


def macd(src, fast=12, slow=26, signal=9) -> MACD:
    s = _as_series(src, "ta.macd")
    fast = as_int(fast, "ta.macd", "fast")
    slow = as_int(slow, "ta.macd", "slow")
    signal = as_int(signal, "ta.macd", "signal")
    x = s.to_numpy()
    line = _recursive(x, fast, "ema") - _recursive(x, slow, "ema")
    sig = _recursive(line, signal, "ema")
    return MACD(_out(line, s.index), _out(sig, s.index), _out(line - sig, s.index))


def donchian(df, n) -> Donchian:
    n = as_int(n, "ta.donchian", "n")
    (h, l), idx = _ohlc(df, "ta.donchian", ("high", "low"))
    up = _roll(h, n, lambda w: w.max(axis=1))
    lo = _roll(l, n, lambda w: w.min(axis=1))
    return Donchian(_out(up, idx), _out(lo, idx), _out((up + lo) / 2.0, idx))


def supertrend(df, period=10, mult=3) -> SuperTrend:
    """Pine ta.supertrend(mult, period) with the direction flipped: 1 up, -1 down.

    Warmup bars (ATR not yet defined) are NaN in both outputs. The first defined bar is a
    downtrend on the upper band, as in Pine. A missing bar is NaN too, and the next bar goes on
    from the bands and direction of the last one that had a value, not from a fresh start.
    """
    period = as_int(period, "ta.supertrend", "period")
    mult = num(mult, "ta.supertrend", "mult")
    (h, l, c), idx = _ohlc(df, "ta.supertrend", ("high", "low", "close"))
    a = _recursive(_tr(h, l, c), period, "rma")
    n = len(c)
    line = np.full(n, np.nan)
    dirn = np.full(n, np.nan)
    lb_prev = ub_prev = 0.0   # the previous ratcheted bands; Pine's nz() is 0 before the first
    st_prev = math.nan
    pc = math.nan             # the last close seen
    started = False
    for i in range(n):
        if not (np.isfinite(a[i]) and np.isfinite(h[i]) and np.isfinite(l[i]) and np.isfinite(c[i])):
            # Warmup, or a missing bar: no value, and the state waits for the next bar that has one.
            if np.isfinite(c[i]):
                pc = c[i]
            continue
        src = (h[i] + l[i]) / 2.0
        ub = src + mult * a[i]
        lb = src - mult * a[i]
        lb_f = lb if (lb > lb_prev or pc < lb_prev) else lb_prev
        ub_f = ub if (ub < ub_prev or pc > ub_prev) else ub_prev
        if not started:
            d = 1                          # Pine: 1 is a downtrend
            started = True
        elif st_prev == ub_prev:
            d = -1 if c[i] > ub_f else 1
        else:
            d = 1 if c[i] < lb_f else -1
        st = lb_f if d == -1 else ub_f
        line[i] = st
        dirn[i] = -d                       # flipped: 1 up, -1 down
        st_prev, lb_prev, ub_prev, pc = st, lb_f, ub_f, c[i]
    return SuperTrend(_out(line, idx), _out(dirn, idx))


def linreg(src, n) -> LinReg:
    """Least-squares line over the last n bars: value at the current bar, and slope per bar."""
    s = _as_series(src, "ta.linreg")
    n = as_int(n, "ta.linreg", "n", minimum=2)
    x = s.to_numpy()
    xc = np.arange(n, dtype="float64") - (n - 1) / 2.0
    sxx = float((xc * xc).sum())

    def slope_f(w):
        return (w - w.mean(axis=1, keepdims=True)) @ xc / sxx

    slope = _roll(x, n, slope_f)
    mean = _sma(x, n)
    value = mean + slope * ((n - 1) / 2.0)
    return LinReg(_out(value, s.index), _out(slope, s.index))


def zscore(src, n) -> pd.Series:
    s = _as_series(src, "ta.zscore")
    n = as_int(n, "ta.zscore", "n")
    x = s.to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        z = (x - _sma(x, n)) / _stdev(x, n)
    return _out(_clean(z), s.index)


def stoch(df, n=14, smooth_k=1, smooth_d=3) -> Stoch:
    """TradingView's Stochastic: k = sma(100 * (close - lowest low) / (highest high - lowest low), smooth_k), d = sma(k, smooth_d)."""
    n = as_int(n, "ta.stoch", "n")
    smooth_k = as_int(smooth_k, "ta.stoch", "smooth_k")
    smooth_d = as_int(smooth_d, "ta.stoch", "smooth_d")
    (h, l, c), idx = _ohlc(df, "ta.stoch", ("high", "low", "close"))
    hh = _roll(h, n, lambda w: w.max(axis=1))
    ll = _roll(l, n, lambda w: w.min(axis=1))
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = _clean(100.0 * (c - ll) / (hh - ll))
    k = _sma(raw, smooth_k) if smooth_k > 1 else raw
    return Stoch(_out(k, idx), _out(_sma(k, smooth_d), idx))


# --------------------------------------------------------------------------- signals

def _cross(a, b, call: str, up: bool) -> pd.Series:
    a_is_s, b_is_s = isinstance(a, (pd.Series, pd.DataFrame, tuple)), isinstance(b, (pd.Series, pd.DataFrame, tuple))
    if not a_is_s and not b_is_s:
        _fail(call, "a", "at least one of a, b must be a Series.")
    if a_is_s:
        sa = _as_series(a, call, "a")
        index = sa.index
        xa = sa.to_numpy()
    if b_is_s:
        sb = _as_series(b, call, "b")
        if not a_is_s:
            index = sb.index
        elif not sb.index.equals(index):
            sb = sb.reindex(index)
        xb = sb.to_numpy()
    if not a_is_s:
        xa = np.full(len(index), num(a, call, "a"))
    if not b_is_s:
        xb = np.full(len(index), num(b, call, "b"))
    out = np.zeros(len(index), dtype=bool)
    if len(index) > 1:
        pa, pb, ca, cb = xa[:-1], xb[:-1], xa[1:], xb[1:]
        with np.errstate(invalid="ignore"):
            out[1:] = (ca > cb) & (pa <= pb) if up else (ca < cb) & (pa >= pb)
    return pd.Series(out, index=index)


def crossover(a, b) -> pd.Series:
    """a > b now and a <= b on the previous bar. b may be a number."""
    return _cross(a, b, "ta.crossover", True)


def crossunder(a, b) -> pd.Series:
    """a < b now and a >= b on the previous bar. b may be a number."""
    return _cross(a, b, "ta.crossunder", False)


def _pivot(src, left, right, call: str, high: bool) -> pd.Series:
    s = _as_series(src, call)
    left = as_int(left, call, "left")
    right = as_int(right, call, "right")
    x = s.to_numpy()
    out = np.full(len(x), np.nan)
    width = left + right + 1
    if len(x) >= width:
        w = sliding_window_view(x, width)
        center = w[:, left]
        others = np.delete(w, left, axis=1)
        with np.errstate(invalid="ignore"):
            ok = center > others.max(axis=1) if high else center < others.min(axis=1)
        out[left: len(x) - right] = np.where(ok, center, np.nan)
    return _out(out, s.index)


def pivothigh(src, left, right) -> pd.Series:
    """The bar is strictly higher than `left` bars before and `right` after; the last `right` bars are NaN."""
    return _pivot(src, left, right, "ta.pivothigh", True)


def pivotlow(src, left, right) -> pd.Series:
    return _pivot(src, left, right, "ta.pivotlow", False)


def vwap(df, anchor=None) -> pd.Series:
    """Volume-weighted average of hlc3.

    ``anchor=None`` resets at each session (calendar day), Pine's default; on daily bars that is
    just hlc3. ``anchor=time`` accumulates from the first bar at or after that time (anchored
    VWAP) and is NaN before it.
    """
    (h, l, c, v), idx = _ohlc(df, "ta.vwap", ("high", "low", "close", "volume"), times=True)
    wall = local_index(idx, None)
    pv_all = (h + l + c) / 3.0 * v
    # A bar missing a price or its volume adds nothing, and is NaN itself: the sums go on past it.
    ok = np.isfinite(pv_all) & np.isfinite(v)
    pv_all = np.where(ok, pv_all, np.nan)
    v = np.where(ok, v, np.nan)
    if anchor is None:
        day = wall.normalize()
        key = pd.Series(day, index=range(len(day)))
        pv = pd.Series(pv_all).groupby(key.to_numpy()).cumsum().to_numpy()
        vv = pd.Series(v).groupby(key.to_numpy()).cumsum().to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            return _out(_clean(pv / vv), idx)
    ts = parse_time(anchor, "ta.vwap", "anchor", str(idx.tz) if idx.tz is not None else None)
    if is_daily(wall):
        ts = ts.normalize()
    start = wall >= ts.as_unit("ns")
    if not start.any():
        _fail("ta.vwap", "anchor", f"{ts} is after the last bar {wall[-1]}; anchor on a bar inside df.")
    pv = np.where(start & ok, pv_all, 0.0)
    vv = np.where(start & ok, v, 0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.cumsum(pv) / np.cumsum(vv)
    return _out(np.where(start & ok, _clean(out), np.nan), idx)
