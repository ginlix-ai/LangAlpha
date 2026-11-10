"""Analysis helpers: turn bars into the points and values the drawing methods take.

Each helper returns plain values. The ones that feed one drawing return a dict whose keys are
that method's arguments plus ``info`` (numbers worth quoting), so ``c.channel(**ch)`` draws it
and ``ch["info"]`` holds the stats; every drawing method accepts and ignores ``info``. Points
are ``Point(time, price)`` tuples with ``time`` taken from ``df.index``.
"""

from __future__ import annotations

import math
import numbers
from collections import namedtuple

import numpy as np
import pandas as pd

from ._util import _fail, _short, as_int, bars_frame, is_daily, local_index, num, parse_time, point
from ._util import timeframe as _timeframe
from .schema import MAX_CONE_BANDS, MAX_PATH_POINTS, MAX_PROFILE_ROWS, MAX_SERIES_POINTS
from .ta import pivothigh, pivotlow

Point = namedtuple("Point", "time price")
Swings = namedtuple("Swings", "highs lows")

_HLC = ("high", "low", "close")
DEFAULT_FIB = (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0)

__all__ = [
    "Point", "future_times", "swings", "zigzag", "key_levels", "regression_channel",
    "trend_channel", "fib_levels", "fib_from_swing", "volume_profile", "vol_cone",
    "measured_move",
]


# --------------------------------------------------------------------------- shared checks

def _tz(idx) -> str | None:
    return str(idx.tz) if idx.tz is not None else None


def _pos(df, t, call: str, arg: str) -> int:
    """Bar position of time ``t``: the bar at or before it, or a position past the end for future times."""
    wall = local_index(df.index, None)
    ts = parse_time(t, call, arg, _tz(df.index))
    if is_daily(wall):
        ts = ts.normalize()
    i = int(wall.searchsorted(ts.as_unit("ns"), side="right")) - 1
    if i < 0:
        _fail(call, arg, f"{ts} is before the first bar {wall[0]}.")
    if i == len(wall) - 1 and ts > wall[-1]:
        # Enough slots to reach the time, as far as a series may run past the bars.
        n = 500
        fut = local_index(future_times(df, n), None)
        while fut[-1] < ts and n < MAX_SERIES_POINTS:
            n = min(n * 4, MAX_SERIES_POINTS)
            fut = local_index(future_times(df, n), None)
        # The slot at or before it, as on the bars; a time short of the first slot is the last bar.
        i = len(wall) - 1 + int(fut.searchsorted(ts.as_unit("ns"), side="right"))
    return i


def _time_at(df, i: int):
    n = len(df)
    if i < n:
        return df.index[i]
    return future_times(df, i - n + 1)[-1]


# --------------------------------------------------------------------------- time

def future_times(df, n, tz=None, timeframe=None, symbol=None) -> pd.DatetimeIndex:
    """The next ``n`` bar times after the last bar, for drawing into the future.

    Daily bars step by weekday (every day when ``df`` already has weekend bars, as crypto does);
    exchange holidays are not known. Intraday bars repeat the times of day seen over the last two
    weeks of ``df`` (so session hours and lunch breaks carry over) on the following weekdays.
    When ``df`` holds a single day, which cannot show where a session ends, the venue's own hours
    for ``symbol`` (default ``df.attrs['symbol']``) are stepped by the interval (``timeframe``,
    default ``df.attrs['timeframe']``, else the bars' spacing); with no known venue it rolls over.
    A zone-aware index is read in ``tz`` (default ``df.attrs['tz']``, else its own zone), so
    session times survive a daylight-saving change.
    """
    idx = df if isinstance(df, pd.DatetimeIndex) else getattr(df, "index", None)
    if not isinstance(idx, pd.DatetimeIndex) or len(idx) == 0:
        _fail("future_times", "df", "expected the bars DataFrame with a non-empty DatetimeIndex.")
    if isinstance(n, bool) or not isinstance(n, numbers.Integral) or n < 1:
        _fail("future_times", "n", f"expected a positive whole number of bars, got {_short(n)}.")
    if n > MAX_SERIES_POINTS:
        _fail("future_times", "n", f"is {n}; a layer reaches at most {MAX_SERIES_POINTS} bars past the last one.")
    n = int(n)
    attrs = getattr(df, "attrs", None) or {}
    tz = tz or attrs.get("tz")
    timeframe = timeframe or attrs.get("timeframe")
    symbol = symbol or attrs.get("symbol")
    if idx.tz is not None and tz:
        idx = idx.tz_convert(tz)
    wall = local_index(idx, None)
    last = wall[-1]
    weekends = bool((wall.dayofweek >= 5).any())
    if is_daily(wall):
        out = pd.date_range(last + pd.Timedelta(days=1), periods=n, freq="D" if weekends else "B")
    else:
        recent = wall[wall >= last.normalize() - pd.Timedelta(days=14)]
        slots = sorted(set((recent - recent.normalize()).tolist()))
        times = [last.normalize() + s for s in slots if last.normalize() + s > last]
        # One day of bars cannot tell a session in progress from a finished one, so the venue's
        # calendar decides; with no venue to ask, the day is taken as finished, as before.
        if not times and (recent.normalize() == last.normalize()).all() and (step := _step(timeframe, wall)):
            times = _session_times(symbol, _tz(idx) or tz, last, step, n) or []
        day = last.normalize()
        while len(times) < n:
            day += pd.Timedelta(days=1)
            if not weekends and day.dayofweek >= 5:
                continue
            times.extend(day + s for s in slots)
        out = pd.DatetimeIndex(times[:n])
    out = out.as_unit("ns")
    if idx.tz is not None:
        out = out.tz_localize(idx.tz, ambiguous=False, nonexistent="shift_forward")
    return out


def _step(tf, wall: pd.DatetimeIndex) -> pd.Timedelta | None:
    """The bar interval: the declared timeframe, else the closest spacing of the bars (None for one bar)."""
    if tf is not None:
        step = pd.Timedelta(_timeframe(tf, "future_times"))
        return step if step < pd.Timedelta(days=1) else None
    gaps = np.diff(wall.asi8)
    gaps = gaps[gaps > 0]
    return pd.Timedelta(int(gaps.min())) if len(gaps) else None


def _session_times(symbol, zone, last: pd.Timestamp, step: pd.Timedelta, n: int) -> list | None:
    """The next ``n`` bar starts after ``last`` within the venue's sessions, in ``last``'s wall clock.

    None when the venue or its hours are unknown. A bar is stamped at its start, so the last one
    of a session starts a step before the close; a lunch break holds no bar.
    """
    if not symbol:
        return None
    try:
        from market_protocol import to_canonical
        from market_protocol.calendars import session_bounds

        ref = to_canonical(symbol)
    except (ImportError, ValueError):
        return None
    if not ref.calendar_id:
        return None
    zone = zone or ref.tz

    def wall(ms):
        return None if ms is None else pd.Timestamp(ms, unit="ms", tz="UTC").tz_convert(zone).tz_localize(None)

    out: list[pd.Timestamp] = []
    # From the day before, since the venue's date and the wall clock's can differ near midnight.
    day, idle = last.normalize() - pd.Timedelta(days=1), 0
    while len(out) < n:
        try:
            bounds = session_bounds(ref.calendar_id, day.date().isoformat())
        except ValueError:
            return None
        day += pd.Timedelta(days=1)
        if bounds is None:
            idle += 1
            if idle > 31:
                return None
            continue
        idle = 0
        open_, close, break_start, break_end = map(wall, bounds)
        t = last + step if open_ <= last < close else open_
        while t < close and len(out) < n:
            if break_start is not None and break_start <= t < break_end:
                t = break_end
                continue
            if t > last:
                out.append(t)
            t += step
    return out


# --------------------------------------------------------------------------- structure

def swings(df, left=5, right=5) -> Swings:
    """Confirmed pivot highs (on ``high``) and lows (on ``low``) as lists of Point, oldest first.

    A pivot is strictly beyond ``left`` bars before and ``right`` bars after it, so the last
    ``right`` bars cannot hold one yet.
    """
    df = bars_frame(df, "analysis.swings", ("high", "low"))
    ph = pivothigh(df["high"], left, right)
    pl = pivotlow(df["low"], left, right)
    highs = [Point(t, float(v)) for t, v in ph.dropna().items()]
    lows = [Point(t, float(v)) for t, v in pl.dropna().items()]
    return Swings(highs, lows)


def zigzag(df, pct=5.0) -> list:
    """Alternating swing highs and lows, each leg at least ``pct`` percent, as Points.

    The last point is the extreme of the leg still in progress (not confirmed). Its prices come
    from ``high`` and ``low``, so it suits pattern paths (H&S, ABCD, Elliott counts).
    """
    df = bars_frame(df, "analysis.zigzag", ("high", "low"))
    p = num(pct, "analysis.zigzag", "pct")
    if not 0 < p < 100:
        _fail("analysis.zigzag", "pct", f"is a percent move between 0 and 100, got {p:g}.")
    p /= 100.0
    h = df["high"].to_numpy(dtype="float64")
    l = df["low"].to_numpy(dtype="float64")
    idx = df.index
    piv: list[Point] = []
    # Seeded from the first bar with a range: a NaN extreme would fail every comparison after it.
    ok = np.flatnonzero(np.isfinite(h) & np.isfinite(l))
    if not len(ok):
        return piv
    k = int(ok[0])
    hi, hi_i, lo, lo_i = h[k], k, l[k], k
    trend = 0
    ext, ext_i = math.nan, 0
    for i in range(k + 1, len(h)):
        if trend == 0:
            if h[i] > hi:
                hi, hi_i = h[i], i
            if l[i] < lo:
                lo, lo_i = l[i], i
            if hi >= lo * (1 + p) and hi_i > lo_i:
                piv.append(Point(idx[lo_i], float(lo)))
                trend, ext, ext_i = 1, hi, hi_i
            elif lo <= hi * (1 - p) and lo_i > hi_i:
                piv.append(Point(idx[hi_i], float(hi)))
                trend, ext, ext_i = -1, lo, lo_i
        elif trend == 1:
            if h[i] > ext:
                ext, ext_i = h[i], i
            elif l[i] <= ext * (1 - p):
                piv.append(Point(idx[ext_i], float(ext)))
                trend, ext, ext_i = -1, l[i], i
        else:
            if l[i] < ext:
                ext, ext_i = l[i], i
            elif h[i] >= ext * (1 + p):
                piv.append(Point(idx[ext_i], float(ext)))
                trend, ext, ext_i = 1, h[i], i
    if trend != 0:
        piv.append(Point(idx[ext_i], float(ext)))
    return piv


def key_levels(df, left=5, right=5, tolerance=0.01, min_touches=2, max_levels=6) -> list:
    """Horizontal levels where swing highs and lows cluster.

    Pivots within ``tolerance`` (a fraction of price) of a cluster's mean join it. Returns up to
    ``max_levels`` dicts ``{"price", "touches", "last"}`` (most touched, then most recent, first),
    sorted by price; pass ``lv["price"]`` to ``c.level``.
    """
    call = "analysis.key_levels"
    tol = num(tolerance, call, "tolerance")
    if not 0 < tol < 0.5:
        _fail(call, "tolerance", f"is a fraction of price like 0.01 (1%), got {tol:g}.")
    min_touches = as_int(min_touches, call, "min_touches")
    max_levels = as_int(max_levels, call, "max_levels")
    hs, ls = swings(df, left, right)
    pts = sorted(hs + ls, key=lambda q: q.price)
    clusters: list[list[Point]] = []
    for q in pts:
        if clusters:
            mean = sum(c.price for c in clusters[-1]) / len(clusters[-1])
            if abs(q.price - mean) <= tol * mean:
                clusters[-1].append(q)
                continue
        clusters.append([q])
    out = [{"price": float(np.mean([q.price for q in c])), "touches": len(c),
            "last": max(q.time for q in c)} for c in clusters if len(c) >= min_touches]
    out.sort(key=lambda d: (d["touches"], d["last"]), reverse=True)
    return sorted(out[:max_levels], key=lambda d: d["price"])


# --------------------------------------------------------------------------- channels

def regression_channel(df, n=100, k=2.0, src="close") -> dict:
    """Least-squares line through the last ``n`` closes, ``k`` residual standard deviations wide.

    Returns ``{"p1", "p2", "p3", "info"}`` for ``c.channel``: p1-p2 the upper line, p3 on the
    lower. ``info``: slope_per_bar, slope_pct_per_bar (None when the line ends at 0), r2, upper, mid,
    lower (at the last bar).
    """
    call = "analysis.regression_channel"
    df = bars_frame(df, call, (src,))
    n = as_int(n, call, "n", minimum=3)
    if n > len(df):
        _fail(call, "n", f"is {n} bars but df has only {len(df)}.")
    k = num(k, call, "k")
    y = df[src].to_numpy(dtype="float64")[-n:]
    if not np.isfinite(y).all():
        _fail(call, "src", f"df[{src!r}] has NaN or infinite values in the last {n} bars.")
    x = np.arange(n, dtype="float64")
    slope, intercept = np.polyfit(x, y, 1)
    fit = intercept + slope * x
    resid = y - fit
    sd = float(resid.std())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - float((resid ** 2).sum()) / ss_tot if ss_tot > 0 else 1.0
    off = k * sd
    t0, t1 = df.index[-n], df.index[-1]
    end = float(fit[-1])
    return {
        "p1": Point(t0, float(fit[0] + off)),
        "p2": Point(t1, end + off),
        "p3": Point(t0, float(fit[0] - off)),
        "info": {"slope_per_bar": float(slope),
                 "slope_pct_per_bar": 100.0 * float(slope) / end if end else None,
                 "r2": r2, "upper": end + off, "mid": end, "lower": end - off, "bars": n},
    }


def trend_channel(df, left=5, right=5, direction="auto") -> dict:
    """A channel on the two latest swing pivots, its parallel through the farthest bar since.

    Rising: the line through the last two swing lows, the parallel through the high farthest
    above it. Falling: the last two swing highs and the farthest low below. When that pair moves
    against the direction (lows falling while highs rise), the other pair carries the line, so
    the drawn lines always slope the way ``direction`` says. ``direction="auto"`` picks rising
    on higher lows and highs, falling on lower ones, else by the slope of closes since the older
    pivot. Returns ``{"p1", "p2", "p3", "info"}`` for ``c.channel``; ``info``: direction,
    anchor ('lows' or 'highs'), slope_per_bar, upper, lower (at the last bar).
    """
    call = "analysis.trend_channel"
    df = bars_frame(df, call, _HLC)
    if direction not in ("auto", "up", "down"):
        _fail(call, "direction", f"must be 'auto', 'up' or 'down', got {_short(direction)}.")
    hs, ls = swings(df, left, right)
    pos = {t: i for i, t in enumerate(df.index)}

    def move(piv) -> float:
        return piv[-1].price - piv[-2].price if len(piv) >= 2 else 0.0

    def pair(d):
        """The usual side's pivots if they move with ``d``, else the other side's, else None."""
        sign = 1.0 if d == "up" else -1.0
        sides = (("lows", ls), ("highs", hs)) if d == "up" else (("highs", hs), ("lows", ls))
        return next(((name, piv) for name, piv in sides if sign * move(piv) > 0), None)

    forced = direction != "auto"
    if not forced:
        if len(hs) < 2 or len(ls) < 2:
            _fail(call, "df", f"found {len(hs)} swing highs and {len(ls)} swing lows with left={left}, "
                              f"right={right}; need two of each. Use smaller left/right or more bars.")
        if move(ls) > 0 and move(hs) > 0:
            direction = "up"
        elif move(ls) < 0 and move(hs) < 0:
            direction = "down"
        else:
            since = min(pos[ls[-2].time], pos[hs[-2].time])
            c = df["close"].to_numpy(dtype="float64")[since:]
            # The closes' trend, read from the bars that have one.
            ok = np.isfinite(c)
            if ok.sum() < 2:
                _fail(call, "df", f"has {int(ok.sum())} closes since the swing at {df.index[since]} to tell the "
                                  f"swings' direction from; pass direction='up' or 'down'.")
            direction = "up" if np.polyfit(np.flatnonzero(ok), c[ok], 1)[0] >= 0 else "down"
            if pair(direction) is None and pair("down" if direction == "up" else "up") is not None:
                # Neither pair moves with the closes (one flat, the other against them).
                direction = "down" if direction == "up" else "up"
    pick = pair(direction)
    if pick is None:
        side = "lows" if direction == "up" else "highs"
        piv = ls if direction == "up" else hs
        if len(piv) < 2:
            _fail(call, "df", f"found {len(piv)} swing {side} with left={left}, right={right}; need two. "
                              f"Use smaller left/right or more bars.")
        if forced and (len(ls) >= 2 and move(ls) != 0 or len(hs) >= 2 and move(hs) != 0):
            want = "rise" if direction == "up" else "fall"
            _fail(call, "direction", f"is {direction!r} but neither the last two swing lows nor the last "
                                     f"two swing highs {want}; pass direction='auto' or the other one.")
        pick = (side, piv)  # both pairs level: a flat channel
    anchor, piv = pick
    a, b = piv[-2], piv[-1]
    ia, ib = pos[a.time], pos[b.time]
    slope = (b.price - a.price) / (ib - ia)
    i = np.arange(ia, len(df))
    line = a.price + slope * (i - ia)
    edge = "high" if anchor == "lows" else "low"
    d = df[edge].to_numpy(dtype="float64")[ia:] - line
    # A bar with no high or low, or an infinite one, has no say in how far the other rail sits.
    d = np.where(np.isfinite(d), d, np.nan)
    if not np.isfinite(d).any():
        _fail(call, "df", f"has no {edge} values from the anchor {a.time} on to place the other rail.")
    j = int(np.nanargmax(d) if anchor == "lows" else np.nanargmin(d))
    p3 = Point(df.index[ia + j], float(df[edge].iloc[ia + j]))
    base_now = a.price + slope * (len(df) - 1 - ia)
    other_now = base_now + float(d[j])
    upper, lower = (other_now, base_now) if anchor == "lows" else (base_now, other_now)
    return {
        "p1": a, "p2": b, "p3": p3,
        "info": {"direction": direction, "anchor": anchor, "slope_per_bar": float(slope),
                 "upper": float(upper), "lower": float(lower)},
    }


# --------------------------------------------------------------------------- fibonacci

def fib_levels(p1, p2, levels=None) -> dict:
    """``{ratio: price}`` as ``c.fib`` draws them: ratio 0 at p2, 1 at p1.

    p1 and p2 are prices or (time, price) points. Ratios above 1 extend past p1; negative ratios
    extend past p2 (e.g. -0.618 for a 1.618 extension of the p1 -> p2 move).
    """
    a = p1[1] if isinstance(p1, (tuple, list)) else p1
    b = p2[1] if isinstance(p2, (tuple, list)) else p2
    a = num(a, "analysis.fib_levels", "p1")
    b = num(b, "analysis.fib_levels", "p2")
    ratios = DEFAULT_FIB if levels is None else levels
    return {float(r): b + (a - b) * float(r) for r in ratios}


def fib_from_swing(df, lookback=120) -> dict:
    """Fib retracement of the dominant move in the last ``lookback`` bars.

    Takes the highest high and lowest low in the window; the later of the two is p2 (ratio 0),
    so levels measure the pullback from where price went last. Returns ``{"p1", "p2", "info"}``
    for ``c.fib``; ``info``: direction ('up' if the low came first), levels ``{ratio: price}``,
    retraced (how much of the move the last close has given back, 0..1+).
    """
    call = "analysis.fib_from_swing"
    df = bars_frame(df, call, _HLC)
    lookback = as_int(lookback, call, "lookback", minimum=2)
    w = df.iloc[-lookback:]
    highs, lows = w["high"].to_numpy(dtype="float64"), w["low"].to_numpy(dtype="float64")
    for name, v in (("high", highs), ("low", lows)):
        if not np.isfinite(v).any():
            _fail(call, "df", f"has no {name} values in the last {len(w)} bars to take the swing from.")
    hi_i = int(np.nanargmax(highs))
    lo_i = int(np.nanargmin(lows))
    hi = Point(w.index[hi_i], float(w["high"].iloc[hi_i]))
    lo = Point(w.index[lo_i], float(w["low"].iloc[lo_i]))
    up = lo_i <= hi_i
    p1, p2 = (lo, hi) if up else (hi, lo)
    close = float(df["close"].iloc[-1])
    span = p2.price - p1.price
    return {
        "p1": p1, "p2": p2,
        "info": {"direction": "up" if up else "down", "levels": fib_levels(p1, p2),
                 "retraced": (p2.price - close) / span if span else 0.0},
    }


# --------------------------------------------------------------------------- volume

def volume_profile(df, start=None, end=None, bins=40, value_area=0.70) -> dict:
    """Fixed-range volume profile of the bars from ``start`` to ``end`` (default: all of df).

    Each bar's volume is spread evenly over its high-low range; ``up_volume`` counts bars that
    closed at or above their open. Returns ``{"start", "end", "rows", "poc", "value_area",
    "info"}`` for ``c.profile``: poc is the busiest row's mid price, value_area the
    ``[low, high]`` around it holding ``value_area`` (70%) of the volume.
    """
    call = "analysis.volume_profile"
    df = bars_frame(df, call, ("open", "high", "low", "close", "volume"))
    bins = as_int(bins, call, "bins", minimum=2)
    if bins > MAX_PROFILE_ROWS:
        _fail(call, "bins", f"is {bins}; a profile holds at most {MAX_PROFILE_ROWS} rows.")
    va = num(value_area, call, "value_area")
    if not 0 < va <= 1:
        _fail(call, "value_area", f"is a fraction like 0.7, got {va:g}.")
    wall = local_index(df.index, None)
    daily = is_daily(wall)
    mask = np.ones(len(df), dtype=bool)
    for arg, t, op in (("start", start, "ge"), ("end", end, "le")):
        if t is None:
            continue
        ts = parse_time(t, call, arg, _tz(df.index))
        ts = ts.normalize() if daily else ts
        mask &= (wall >= ts) if op == "ge" else (wall <= ts)
    sub = df[mask]
    if len(sub) == 0:
        _fail(call, "start", f"no bars between {start} and {end}.")
    h = sub["high"].to_numpy(dtype="float64")
    l = sub["low"].to_numpy(dtype="float64")
    v = sub["volume"].to_numpy(dtype="float64")
    v = np.where(np.isfinite(v) & (v >= 0), v, 0.0)  # a bar with no usable volume adds none
    up = sub["close"].to_numpy(dtype="float64") >= sub["open"].to_numpy(dtype="float64")
    ok = np.isfinite(h) & np.isfinite(l)
    if not ok.any():
        _fail(call, "df", f"has no bar with a high and a low from {sub.index[0]} to {sub.index[-1]}.")
    lo, hi = float(l[ok].min()), float(h[ok].max())
    if hi <= lo:
        hi = lo + max(abs(lo) * 1e-6, 1e-9)
    edges = np.linspace(lo, hi, bins + 1)
    e_lo, e_hi = edges[:-1][None, :], edges[1:][None, :]
    rng = (h - l)[:, None]
    overlap = np.clip(np.minimum(h[:, None], e_hi) - np.maximum(l[:, None], e_lo), 0.0, None)
    with np.errstate(divide="ignore", invalid="ignore"):
        share = np.where(rng > 0, overlap / rng, 0.0)
    flat = (rng[:, 0] <= 0)
    if flat.any():
        k = np.clip(np.searchsorted(edges, h[flat], side="right") - 1, 0, bins - 1)
        share[np.flatnonzero(flat), k] = 1.0
    vol = (share * v[:, None]).sum(axis=0)
    up_vol = np.minimum((share * (v * up)[:, None]).sum(axis=0), vol)
    total = float(vol.sum())
    if not total > 0:
        _fail(call, "df", f"has no volume from {sub.index[0]} to {sub.index[-1]} to profile.")
    poc_i = int(np.argmax(vol))
    a = b = poc_i
    acc = vol[poc_i]
    while acc < va * total and (a > 0 or b < bins - 1):
        below = vol[a - 1] if a > 0 else -1.0
        above = vol[b + 1] if b < bins - 1 else -1.0
        if above >= below:
            b += 1
            acc += vol[b]
        else:
            a -= 1
            acc += vol[a]
    rows = [{"low": float(edges[i]), "high": float(edges[i + 1]), "volume": float(vol[i]),
             "up_volume": float(up_vol[i])} for i in range(bins)]
    poc = float((edges[poc_i] + edges[poc_i + 1]) / 2)
    va_range = [float(edges[a]), float(edges[b + 1])]
    return {
        "start": sub.index[0], "end": sub.index[-1], "rows": rows, "poc": poc,
        "value_area": va_range,
        "info": {"poc": poc, "va_low": va_range[0], "va_high": va_range[1],
                 "total_volume": total, "bars": int(len(sub))},
    }


# --------------------------------------------------------------------------- forward-looking

def vol_cone(df, horizon=20, window=20, sigmas=(1, 2), price=None) -> dict:
    """Lognormal volatility cone from the last bar, ``horizon`` bars forward.

    Volatility is the sample standard deviation of the last ``window`` log returns (per bar);
    band ``s`` is ``price * exp(±s * vol * sqrt(k))`` at step k, with zero drift. Times come
    from ``future_times``. Returns ``{"origin", "bands", "info"}`` for ``c.cone``, widest band
    first; ``info``: vol_per_bar, vol_annual (daily bars, x sqrt(252)), and each band's
    ``(lower, upper)`` at the horizon.
    """
    call = "analysis.vol_cone"
    df = bars_frame(df, call, ("close",))
    horizon = as_int(horizon, call, "horizon")
    if horizon > MAX_PATH_POINTS:
        # c.cone takes a band of one point per bar, up to its path limit.
        _fail(call, "horizon", f"is {horizon}; a cone band holds one point per bar, at most {MAX_PATH_POINTS}.")
    window = as_int(window, call, "window", minimum=2)
    if len(df) < window + 1:
        _fail(call, "window", f"needs {window + 1} bars for {window} returns; df has {len(df)}.")
    if isinstance(sigmas, numbers.Real):
        sigmas = (sigmas,)
    sig = sorted({num(s, call, "sigmas") for s in sigmas}, reverse=True)
    if not sig or sig[-1] <= 0 or len(sig) > MAX_CONE_BANDS:
        _fail(call, "sigmas", f"expected 1-{MAX_CONE_BANDS} positive multiples like (1, 2), got {_short(sigmas)}.")
    close = df["close"].to_numpy(dtype="float64")
    span = close[-(window + 1):]
    if not (np.isfinite(span).all() and (span > 0).all()):
        _fail(call, "df", f"needs {window + 1} positive closes for {window} returns; the last {window + 1} "
                          f"include a missing, infinite or non-positive one.")
    r = np.diff(np.log(span))
    vol = float(np.std(r, ddof=1))
    p0 = float(close[-1]) if price is None else num(price, call, "price")
    if not p0 > 0:
        _fail(call, "price", f"is {p0:g}; a lognormal cone starts from a positive price.")
    times = future_times(df, horizon)
    k = np.arange(1, horizon + 1, dtype="float64")
    bands, ends = [], {}
    for s in sig:
        up = p0 * np.exp(s * vol * np.sqrt(k))
        dn = p0 * np.exp(-s * vol * np.sqrt(k))
        label = f"±{s:g}σ"
        bands.append({"label": label,
                      "upper": [Point(t, float(p)) for t, p in zip(times, up)],
                      "lower": [Point(t, float(p)) for t, p in zip(times, dn)]})
        ends[label] = (float(dn[-1]), float(up[-1]))
    daily = is_daily(local_index(df.index, None))
    return {
        "origin": Point(df.index[-1], p0), "bands": bands,
        "info": {"vol_per_bar": vol, "vol_annual": vol * math.sqrt(252) if daily else None,
                 "horizon": horizon, "end": ends},
    }




def measured_move(df, a, b, c) -> dict:
    """Equal-legs target: the a -> b move repeated from c, over the same number of bars.

    a, b, c are (time, price) points (e.g. from ``zigzag``). Returns ``{"p1", "p2", "info"}``
    for ``c.forecast(p2.price, p2.time, origin=p1)`` (or ``c.arrow``, ``c.measure``): p1 is c, p2 the
    target; ``info``: target, move, pct (None from a c of price 0).
    """
    call = "analysis.measured_move"
    df = bars_frame(df, call, ())
    ta_, pa = point(a, call, "a")
    tb, pb = point(b, call, "b")
    tc, pc = point(c, call, "c")
    ia, ib, ic = _pos(df, ta_, call, "a"), _pos(df, tb, call, "b"), _pos(df, tc, call, "c")
    if not ia < ib <= ic:
        _fail(call, "c", "points must be in time order: a before b, and c at or after b.")
    target = pc + (pb - pa)
    return {
        "p1": Point(_time_at(df, ic), pc), "p2": Point(_time_at(df, ic + (ib - ia)), target),
        "info": {"target": target, "move": pb - pa, "pct": 100.0 * (target / pc - 1.0) if pc else None},
    }
