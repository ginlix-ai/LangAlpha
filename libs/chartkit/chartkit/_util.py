"""Argument checks, times and colors shared by every chartkit module.

Errors are ``ChartError`` (a ValueError) worded for the agent that made the call: they name the
method and the argument, say what was wrong, and say what to write instead. They are raised at
the call that went wrong, not at publish, so the traceback points at the line to fix.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import math
import numbers
import re
from dataclasses import dataclass

import numpy as np
import pandas as pd
from market_protocol import to_canonical
from pydantic import TypeAdapter, ValidationError

from . import schema
from .schema import ELEMENT_ID_PATTERN, LAYER_ID_PATTERN, TIMEFRAMES, is_color as _layer_color


class ChartError(ValueError):
    """A chartkit call was given something it cannot draw."""


def _short(x, limit: int = 60) -> str:
    r = repr(x)
    return r if len(r) <= limit else r[: limit - 3] + "..."


def _fail(call: str, arg: str, msg: str):
    raise ChartError(f"{call}({arg}=...): {msg}")


def first_line(e) -> str:
    """An exception's first line, for a one-line report."""
    text = str(e).strip()
    return text.splitlines()[0][:200] if text else type(e).__name__


def clip(text: str, limit: int = 160) -> str:
    """One line of an upstream message, cut where the markup of a proxy's error page starts."""
    text = re.sub(r"\s+", " ", str(text).split("<", 1)[0]).strip().rstrip(":").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


# --------------------------------------------------------------------------- timeframes / zones

_TF_ALIASES = {
    "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
    "60min": "1hour", "1h": "1hour", "60m": "1hour", "4h": "4hour", "240min": "4hour",
    "1d": "1day", "d": "1day", "day": "1day", "daily": "1day",
}


def timeframe(tf, call: str) -> str:
    if not isinstance(tf, str):
        _fail(call, "timeframe", f"expected one of {list(TIMEFRAMES)}, got {type(tf).__name__} {_short(tf)}.")
    key = tf.strip().lower()
    key = _TF_ALIASES.get(key, key)
    if key not in TIMEFRAMES:
        hint = ""
        if " " in tf.strip() or len(tf) > 8:
            hint = " It looks like a title; pass it as title=..."
        _fail(call, "timeframe", f"must be one of {list(TIMEFRAMES)}, got {_short(tf)}.{hint}")
    return key


def tz_for_symbol(symbol: str) -> str:
    """The listing's zone, as the market-data layer resolves it; New York for a symbol it cannot read."""
    try:
        return to_canonical(symbol).tz
    except ValueError:
        return "America/New_York"


def check_tz(tz, call: str) -> str:
    if not isinstance(tz, str) or not tz:
        _fail(call, "tz", f"expected an IANA zone name like 'America/New_York', got {_short(tz)}.")
    try:
        pd.Timestamp("2026-01-02", tz=tz)
    except Exception:
        _fail(call, "tz", f"unknown time zone {tz!r}; use an IANA name like 'Asia/Hong_Kong'.")
    return tz


# --------------------------------------------------------------------------- times

_TIME_TYPES = (str, _dt.date, _dt.datetime, pd.Timestamp, np.datetime64)


def parse_time(t, call: str, arg: str, tz: str | None = None) -> pd.Timestamp:
    """A naive exchange-local Timestamp. Aware inputs are converted to ``tz`` first.

    Bare numbers are refused: ``pd.Timestamp(150.0)`` is 1970, and a number in a time slot almost
    always means the (time, price) pair was written backwards.
    """
    if isinstance(t, bool) or not isinstance(t, _TIME_TYPES):
        extra = ""
        if isinstance(t, numbers.Real):
            extra = " A bare number is not a time; points are (time, price), check the order."
        _fail(call, arg, f"expected a time (str like '2026-03-04', datetime, or pd.Timestamp), got "
                         f"{type(t).__name__} {_short(t)}.{extra}")
    try:
        ts = pd.Timestamp(t)
    except (ValueError, TypeError, OverflowError):
        _fail(call, arg, f"cannot parse {_short(t)} as a time; use 'YYYY-MM-DD' or "
                         f"'YYYY-MM-DD HH:MM' (exchange-local) or a pd.Timestamp.")
    if pd.isna(ts):
        _fail(call, arg, "time is NaT/missing.")
    if ts.tzinfo is not None:
        ts = ts.tz_convert(tz) if tz else ts
        ts = ts.tz_localize(None)
    return ts


def local_index(index, tz: str | None) -> pd.DatetimeIndex:
    """The index as naive exchange-local wall clock (aware indexes are converted to ``tz``)."""
    if index.tz is not None:
        index = index.tz_convert(tz) if tz else index
        index = index.tz_localize(None)
    return index.as_unit("ns")


def is_daily(index: pd.DatetimeIndex) -> bool:
    """Every bar sits at midnight, which is how a daily chart's times read."""
    return bool((index == index.normalize()).all())


_TIME_FORMATS = {
    "layer": "%Y-%m-%dT%H:%M:%S",  # a layer's times
    "view": "%Y-%m-%dT%H:%M",  # the chart's window convention
    "text": "%Y-%m-%d %H:%M",  # prose, and the market-data tool's dates
}


def fmt_time(ts: pd.Timestamp, daily: bool, form: str = "layer") -> str:
    """A bar time as a date on a daily chart, else in ``form``."""
    return ts.strftime("%Y-%m-%d" if daily else _TIME_FORMATS[form])


# --------------------------------------------------------------------------- scalars / strings

def finite(x) -> float | None:
    """``x`` as a float when it is a real, finite number (not a bool), else None."""
    if isinstance(x, bool) or not isinstance(x, numbers.Real):
        return None
    v = float(x)
    return v if math.isfinite(v) else None


def num(x, call: str, arg: str) -> float:
    if isinstance(x, (pd.Series, pd.DataFrame, np.ndarray, list, tuple)):
        _fail(call, arg, f"expected a single number, got {type(x).__name__}. Use a scalar such as "
                         f"float(s.iloc[-1]).")
    if isinstance(x, bool) or not isinstance(x, numbers.Real):
        _fail(call, arg, f"expected a number, got {type(x).__name__} {_short(x)}.")
    v = finite(x)
    if v is None:
        _fail(call, arg, f"must be finite, got {x}. (NaN usually means an indicator is not warmed up "
                         f"at that bar.)")
    return v


def as_int(n, call: str, arg: str, minimum: int = 1) -> int:
    """A whole number of bars (or steps) of at least ``minimum``."""
    v = finite(n)
    if v is None or v != int(v):
        _fail(call, arg, f"expected an integer bar count, got {type(n).__name__} {_short(n)}.")
    if int(v) < minimum:
        _fail(call, arg, f"must be >= {minimum}, got {int(v)}.")
    return int(v)


def whole(x, call: str, arg: str) -> int | float:
    """A number as a plain int when it is whole, so the schema's integer field takes ``2.0`` or ``np.int64(2)``."""
    v = num(x, call, arg)
    return int(v) if v == int(v) else v


def text(x, call: str, arg: str, adapter: TypeAdapter | None = None) -> str:
    """A non-blank string, within the length cap of ``adapter``'s schema type when one is given."""
    if not isinstance(x, str):
        _fail(call, arg, f"expected a string, got {type(x).__name__} {_short(x)}.")
    if not x.strip():
        _fail(call, arg, "must not be empty.")
    return x if adapter is None else field(adapter, x, call, arg)


def opt_text(x, call: str, arg: str, adapter: TypeAdapter | None = None) -> str | None:
    return None if x is None else text(x, call, arg, adapter)


# --------------------------------------------------------------------------- ids

_LAYER_ID = re.compile(LAYER_ID_PATTERN)
_ELEMENT_ID = re.compile(ELEMENT_ID_PATTERN)


def slug(s: str, fallback: str) -> str:
    out = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:48].strip("-")
    if out:
        return out
    # A title with no ASCII letters still needs a stable id, so republishing replaces it.
    return f"{fallback}-{hashlib.sha1(s.encode('utf-8')).hexdigest()[:8]}"


def check_id(v, call: str, arg: str = "id") -> str:
    """A layer's id."""
    if not isinstance(v, str) or not _LAYER_ID.match(v):
        _fail(call, arg, f"must be 1-48 characters of lowercase a-z, 0-9 and '-', got {_short(v)}.")
    return v


def check_element_id(v, call: str, arg: str = "id") -> str:
    """The id of a pane, series, drawing or alert inside a layer."""
    if not isinstance(v, str) or not _ELEMENT_ID.match(v):
        _fail(call, arg, f"must be 1-48 characters of letters, digits, '_', '.', ':' and '-', starting with a "
                         f"letter or digit, got {_short(v)}.")
    return v


# --------------------------------------------------------------------------- colors

# Named colors are converted, since the contract takes hex or rgb[a] only.
_NAMED = {
    "red": "#ef5350", "green": "#26a69a", "blue": "#2962ff", "orange": "#ff9800",
    "yellow": "#fdd835", "purple": "#7e57c2", "pink": "#ec407a", "teal": "#009688",
    "cyan": "#00bcd4", "magenta": "#e040fb", "lime": "#cddc39", "navy": "#1a237e",
    "maroon": "#880e4f", "olive": "#827717", "gray": "#787b86", "grey": "#787b86",
    "white": "#ffffff", "black": "#000000", "brown": "#8d6e63", "gold": "#f9a825",
    "indigo": "#3f51b5", "violet": "#9c27b0", "crimson": "#d32f2f", "coral": "#ff7043",
    "silver": "#b2b5be", "transparent": "rgba(0,0,0,0)",
}


def is_color(c) -> bool:
    return isinstance(c, str) and (_layer_color(c) or c.strip().lower() in _NAMED)


def color(c, call: str, arg: str = "color") -> str:
    if not is_color(c):
        extra = " A Series of colors is only accepted by c.plot()." if isinstance(c, pd.Series) else ""
        _fail(call, arg, f"expected a hex or rgb()/rgba() color like '#2962ff' or "
                         f"'rgba(41,98,255,0.12)', got {_short(c)}.{extra}")
    c = c.strip()
    return _NAMED.get(c.lower(), c)


def with_alpha(c: str, alpha: float) -> str:
    """A translucent version of an opaque color (fills derived from a line color). A color
    that already carries its alpha (an 8-digit hex, ``rgba()``) is the caller's choice and stays."""
    h = c.lstrip("#")
    if c.startswith("#") and len(h) in (3, 6):
        if len(h) == 3:
            h = "".join(ch * 2 for ch in h)
        r, g, b = (int(h[i: i + 2], 16) for i in (0, 2, 4))
        return f"rgba({r},{g},{b},{alpha:g})"
    if m := re.fullmatch(r"rgb\(\s*([^,()]+?)\s*,\s*([^,()]+?)\s*,\s*([^,()]+?)\s*\)", c.strip(), re.I):
        return f"rgba({','.join(m.groups())},{alpha:g})"
    return c


# --------------------------------------------------------------------------- bars and points

def bars_frame(df, call: str, cols: tuple[str, ...] = (), *, times: bool = True) -> pd.DataFrame:
    """The OHLCV DataFrame a call was given, with ``cols`` present and (``times``) bars to read."""
    if isinstance(df, pd.Series):
        _fail(call, "df", "expected the bars DataFrame (e.g. chartkit.bars('NVDA')), got a Series. "
                          "Pass df itself, not df.close.")
    if not isinstance(df, pd.DataFrame):
        _fail(call, "df", f"expected the bars DataFrame (e.g. chartkit.bars('NVDA')), got {type(df).__name__}.")
    if times and (not isinstance(df.index, pd.DatetimeIndex) or len(df) == 0):
        _fail(call, "df", "must be non-empty with a DatetimeIndex of bar times. If the times are a "
                          "column, use df = df.set_index(pd.to_datetime(df['date'])).")
    missing = [c for c in cols if c not in df.columns]
    if missing:
        _fail(call, "df", f"is missing columns {missing}; it has {list(df.columns)}.")
    # An infinite price or volume is missing data: every reader passes over NaN.
    nums = df.select_dtypes("number")
    if np.isinf(nums.to_numpy(dtype="float64")).any():
        df = df.copy()
        df[nums.columns] = nums.replace([np.inf, -np.inf], np.nan)
    return df


@dataclass(frozen=True)
class Basis:
    """What a chart or a snapshot stands on: the symbol, interval and zone, and the bars' local times
    (None when no bars were given, as a snapshot of a bare symbol)."""

    symbol: str
    timeframe: str
    tz: str
    index: pd.DatetimeIndex | None


def chart_bars(df, symbol, tf, tz, call: str) -> Basis:
    """Resolve a chart's symbol, timeframe, zone and bar times from the call's arguments and ``df.attrs``.

    ``df`` may be None (a symbol alone). The bars must read as one chart: in time order, one per
    time, and midnights exactly when the interval is a day.
    """
    attrs = {}
    if df is not None:
        bars_frame(df, call)
        attrs = getattr(df, "attrs", None) or {}
    symbol = symbol if symbol is not None else attrs.get("symbol")
    if symbol is None:
        _fail(call, "symbol", "is required when df has no df.attrs['symbol'] (chartkit.bars() sets it), "
                              "e.g. 'NVDA'.")
    symbol = text(symbol, call, "symbol").strip()
    if any(ch.isspace() for ch in symbol):
        _fail(call, "symbol", f"must be one ticker with no spaces, got {symbol!r}.")
    field(SYMBOL, symbol, call, "symbol")
    tf = timeframe(tf if tf is not None else attrs.get("timeframe", "1day"), call)
    tz = check_tz(tz if tz is not None else attrs.get("tz") or tz_for_symbol(symbol), call)
    if df is None:
        return Basis(symbol, tf, tz, None)
    idx = local_index(df.index, tz)
    if not idx.is_monotonic_increasing:
        _fail(call, "df", "bars must be in time order, oldest first; use df = df.sort_index().")
    if tf == "1day":
        if not is_daily(idx):
            odd = idx[idx != idx.normalize()][0]
            _fail(call, "timeframe", f"is '1day' but df has intraday bars (e.g. {odd}); pass the bars' "
                                     f"interval, e.g. timeframe='5min'.")
    elif len(idx) > 1 and is_daily(idx) and np.diff(idx.asi8).min() >= 86_400 * 10**9:
        _fail(call, "timeframe", f"is {tf!r} but df holds one bar per day; pass timeframe='1day'.")
    if idx.has_duplicates:
        dup = idx[idx.duplicated()][0]
        _fail(call, "df", f"has more than one bar at {dup}; drop duplicates (df[~df.index.duplicated()]).")
    return Basis(symbol, tf, tz, idx)


_POINT_FORM = "(time, price), e.g. ('2026-03-04', 150.0) or (df.index[-1], float(df.close.iloc[-1]))"


def point(p, call: str, arg: str, when=None) -> tuple:
    """One ``(time, price)`` point (a pair, or a dict with those keys) as ``(time, price)``.

    The time passes through ``when(time, call, label)`` first when given, so a time in the price
    slot is reported as a time, which is how a backwards pair shows.
    """
    if isinstance(p, dict):
        if not {"time", "price"} <= set(p):
            _fail(call, arg, f"a dict point needs the keys 'time' and 'price', got {sorted(p)}.")
        p = (p["time"], p["price"])
    if isinstance(p, (pd.DataFrame, pd.Series, np.ndarray)):
        _fail(call, arg, f"expected one point {_POINT_FORM}, got {type(p).__name__}.")
    if not isinstance(p, (tuple, list)):
        _fail(call, arg, f"expected a (time, price) pair, got {type(p).__name__} {_short(p)}; "
                         f"write it as {_POINT_FORM}.")
    if len(p) != 2 or isinstance(p[0], (tuple, list, dict)):
        _fail(call, arg, f"expected one (time, price) point, got {_short(p)}. This looks like a list of "
                         f"points; this argument takes a single point.")
    when_ = p[0] if when is None else when(p[0], call, f"{arg}[0] (time)")
    return when_, num(p[1], call, f"{arg}[1] (price)")


# --------------------------------------------------------------------------- schema elements

_NO_GOT = {"value_error", "assertion_error", "string_too_long", "string_too_short", "missing",
           "extra_forbidden", "too_long", "too_short"}
_MAX_PROBLEMS = 4
#: What a list field holds, where its name does not say.
_NOUNS = {"data": "points", "upper": "points", "lower": "points"}


def _says(err: dict, model=None) -> str:
    """The schema's complaint in the words an agent can act on."""
    kind, ctx = err.get("type"), err.get("ctx") or {}
    names = [p for p in err.get("loc", ()) if isinstance(p, str)]
    noun = _NOUNS.get(names[-1], names[-1]) if names else "items"
    given = err.get("input")
    if kind == "extra_forbidden":
        return "is not a field of this element; check its spelling"
    if kind == "too_long":
        return f"has {ctx.get('actual_length')} {noun}; the limit is {ctx.get('max_length')}."
    if kind == "too_short":
        return f"has {ctx.get('actual_length')} {noun}; it needs at least {ctx.get('min_length')}."
    if kind == "string_too_long":
        return f"is {len(str(given))} characters; the limit is {ctx.get('max_length')}. Shorten it."
    return schema.plain_problem(err, model, got=kind not in _NO_GOT)


def _problems(call: str, e: ValidationError, prefix: tuple = (), advice: dict | None = None,
              args: dict[str, str] | None = None, model=None) -> ChartError:
    lines = []
    for err in e.errors(include_url=False):
        loc = ""
        for part in (*prefix, *err.get("loc", ())):
            loc += f"[{part}]" if isinstance(part, int) else (f".{part}" if loc else str(part))
        loc = (args or {}).get(loc, loc)
        says = _says(err, model)
        line = f"{call}({loc}=...): {says}" if loc else f"{call}(): {says}"
        over_or_under = {"too_long": "over", "too_short": "under"}.get(err.get("type"))
        if over_or_under:
            line += next((f" {advice[p, over_or_under]}" for p in err["loc"] if advice and (p, over_or_under) in advice), "")
        lines.append(line)
    more = len(lines) - _MAX_PROBLEMS
    if more > 0:
        lines = [*lines[:_MAX_PROBLEMS], f"... and {more} more problem(s)"]
    return ChartError("\n".join(lines))


def element(model, call: str, *, advice: dict | None = None, args: dict[str, str] | None = None,
            **fields) -> dict:
    """One layer element built from plain values, checked by its schema model and returned as the
    dict a published layer holds. A rejection is the schema's, each problem located by the call and
    field. What only the builder knows rides along: ``advice`` maps ``(field, "over" | "under")`` to
    a tip for a list past its cap or short of its floor, ``args`` renames a schema field to the
    argument the agent passed."""
    try:
        return model.model_validate(fields).model_dump(mode="json", by_alias=True, exclude_none=True)
    except ValidationError as e:
        raise _problems(call, e, advice=advice, args=args, model=model) from None


def field(adapter: TypeAdapter, value, call: str, arg: str):
    """``value`` checked by one schema type (a title, a summary, a symbol)."""
    try:
        return adapter.validate_python(value)
    except ValidationError as e:
        raise _problems(call, e, prefix=(arg,)) from None


TITLE = TypeAdapter(schema.Title)
SUMMARY = TypeAdapter(schema.Summary)
SYMBOL = TypeAdapter(schema.Symbol)
