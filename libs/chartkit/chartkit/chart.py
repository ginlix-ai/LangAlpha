"""``Chart``: build one chart layer from pandas and publish it to the user's chart."""

from __future__ import annotations

import builtins
import json
import math
import numbers
import os

import numpy as np
import pandas as pd

from . import schema
from .protocol import CHART_FILE_ENV
from ._util import (
    SUMMARY,
    TITLE,
    ChartError,
    _fail,
    _short,
    chart_bars,
    check_element_id,
    check_id,
    color as _color,
    element,
    finite,
    fmt_time,
    local_index,
    num,
    opt_text,
    parse_time,
    point as _parse_point,
    slug,
    text as _text,
    whole,
    with_alpha,
)
from .ta import _as_series

PALETTE = ["#2962ff", "#ff6d00", "#26a69a", "#e91e63", "#7e57c2", "#f9a825", "#00acc1", "#8d6e63"]
_UP, _DOWN, _NEUTRAL, _GRAY = "#26a69a", "#ef5350", "#2962ff", "#787b86"
_HIST_UP, _HIST_DOWN = "rgba(38,166,154,0.6)", "rgba(239,83,80,0.6)"
_CONE_FILL = "rgba(41,98,255,{:.2f})"
_POINT_LIST = "expected a list of (time, price) points"



def _ohlcv_lowercase(df):
    """``df`` with its OHLCV columns spelled in lowercase, as the builders read them. A snapshot
    reads ``Close`` as ``close`` already, so the builders that fall back to the last close
    (``c.forecast``, ``c.analog``) must too. A lowercase column already there wins."""
    if df is None:
        return df
    have = {str(c) for c in df.columns}
    ren = {c: str(c).lower() for c in df.columns
           if str(c).lower() in ("open", "high", "low", "close", "volume") and str(c) != str(c).lower()
           and str(c).lower() not in have}
    return df.rename(columns=ren) if ren else df

class Chart:
    """One chart layer over ``df``'s bars. Add elements, then ``publish()``."""

    def __init__(self, df, symbol=None, timeframe=None, title=None, *, id=None, summary=None, tz=None):
        call = "Chart"
        basis = chart_bars(df, symbol, timeframe, tz, call)
        self.symbol, self.timeframe, self.tz, self._idx = basis.symbol, basis.timeframe, basis.tz, basis.index
        if title is None:
            _fail(call, "title", "is required: a short name for this layer, e.g. title='NVDA rising channel'.")
        self.title = _text(title, call, "title", TITLE)
        self.id = check_id(id, call) if id is not None else slug(self.title, "layer")
        self.summary = opt_text(summary, call, "summary", SUMMARY)
        self.daily = self.timeframe == "1day"
        self._df = _ohlcv_lowercase(df)
        self._times = [fmt_time(t, self.daily) for t in self._idx]
        self.as_of = self._times[-1]

        self._panes: list[dict] = []
        self._series: list[dict] = []
        self._fills: list[dict] = []
        self._levels: list[dict] = []
        self._markers: list[dict] = []
        self._regimes: list[dict] = []
        self._drawings: list[dict] = []
        self._alerts: list[dict] = []
        self._notes: list[str] = []

    def __repr__(self) -> str:
        return (f"Chart({self.symbol} {self.timeframe} {self.title!r}, id={self.id!r}, "
                f"{self._counts() or 'empty'})")

    # ------------------------------------------------------------------ conversion

    def _t(self, t, call: str, arg: str) -> str:
        ts = parse_time(t, call, arg, self.tz)
        return fmt_time(ts.normalize() if self.daily else ts, self.daily)

    def _point(self, p, call: str, arg: str) -> dict:
        when, price = _parse_point(p, call, arg, self._t)
        return {"time": when, "price": price}

    def _points(self, pts, call: str, arg: str) -> list[dict]:
        if isinstance(pts, (pd.DataFrame, pd.Series, np.ndarray)):
            _fail(call, arg, f"{_POINT_LIST}, got {type(pts).__name__}. Build it with "
                             f"list(zip(s.index, s.values)).")
        if isinstance(pts, (str, bytes, dict)) or not hasattr(pts, "__iter__"):
            _fail(call, arg, f"{_POINT_LIST}, got {type(pts).__name__} {_short(pts)}.")
        return [self._point(p, call, f"{arg}[{i}]") for i, p in enumerate(pts)]

    def _on_bars(self, x, call: str, arg: str) -> pd.Series:
        """``x`` as a Series on the chart's clock. A list or array must have one value per bar of df;
        a Series is matched by time (a repeated time keeps its last value)."""
        n = len(self._idx)
        if isinstance(x, (np.ndarray, list)):
            arr = np.asarray(x)
            if arr.ndim != 1 or len(arr) != n:
                _fail(call, arg, f"an array or list needs exactly one value per bar of df ({n}), got shape "
                                 f"{arr.shape}. Or pass a Series indexed by time.")
            return pd.Series(arr, index=self._idx)
        s = _as_series(x, call, arg, numeric=False)
        if not isinstance(s.index, pd.DatetimeIndex):
            hint = " Build it with pd.Series(values, index=df.index)." if len(s) == n else ""
            _fail(call, arg, f"Series index must be times like df's, got {type(s.index).__name__}.{hint}")
        si = local_index(s.index, self.tz)
        s = pd.Series(s.to_numpy(), index=si.normalize() if self.daily else si, dtype=s.dtype)
        return s[~s.index.duplicated(keep="last")]

    def _mask(self, x, call: str, arg: str, hint: str = "") -> np.ndarray:
        """A boolean Series (or one value per bar) as a mask over df's bars; bars it lacks are False."""
        s = self._on_bars(x, call, arg)
        if s.dtype.kind != "b" and str(s.dtype) != "boolean":
            _fail(call, arg, f"expected a boolean Series (e.g. df.close > ma, or ta.crossover(...)), got "
                             f"dtype {s.dtype}. Convert with .astype(bool) if it is 0/1.{hint}")
        return s.reindex(self._idx).fillna(False).astype(bool).to_numpy()

    def _numbers(self, x, call: str, arg: str) -> pd.Series:
        """``x`` on the chart's clock as floats, including any values after the last bar."""
        s = self._on_bars(x, call, arg)
        try:
            return s.astype("float64")
        except (TypeError, ValueError):
            _fail(call, arg, f"has non-numeric dtype {s.dtype}; it must hold numbers.")

    def _values(self, x, call: str, arg: str) -> np.ndarray:
        """``x`` as one float per bar of df (NaN where it has none); values past the last bar are dropped."""
        return self._numbers(x, call, arg).reindex(self._idx).to_numpy(dtype="float64")

    def _future(self, s: pd.Series) -> list[tuple[str, float]]:
        """The values of ``s`` after the last bar, as (time, value): a forward projection."""
        tail = s[s.index > self._idx[-1]].sort_index()
        return [(fmt_time(t, self.daily), float(v)) for t, v in tail.items() if not math.isnan(v)]

    def _pane(self, pane, call: str) -> str:
        if pane == "price":
            return pane
        ids = [p["id"] for p in self._panes]
        if pane not in ids:
            names = ["price", *ids]
            close = [n for n in names if isinstance(pane, str) and n.lower() == pane.lower()]
            hint = f" Did you mean {close[0]!r}?" if close else ""
            make = slug(pane, "pane") if isinstance(pane, str) else "rsi"
            _fail(call, "pane", f"no pane named {_short(pane)}; panes: {names}.{hint} "
                                f"Create it first with c.pane({make!r}) and pass pane={make!r}.")
        return pane

    def _unique(self, items: list[dict], base: str) -> str:
        taken = {x.get("id") for x in items}
        if base not in taken:
            return base
        k = 2
        while f"{base[:44]}-{k}" in taken:
            k += 1
        return f"{base[:44]}-{k}"

    def _id(self, id, items, call, fallback_text, fallback_kind) -> str:
        if id is not None:
            check_element_id(id, call)
            if any(x.get("id") == id for x in items):
                _fail(call, "id", f"{id!r} is already used by another element of this kind.")
            return id
        return self._unique(items, slug(fallback_text, fallback_kind) if fallback_text else fallback_kind)

    @staticmethod
    def _element(model, call: str, note, **fields) -> dict:
        """One element of the layer, checked by its schema model; ``note`` rides along when given."""
        return element(model, call, note=opt_text(note, call, "note"), **fields)

    def _room(self, items, cap, adding, call, arg, what):
        if len(items) + adding > cap:
            _fail(call, arg, f"would bring the layer to {len(items) + adding} {what}; the limit is {cap}.")

    # ------------------------------------------------------------------ panes, series, fills, levels

    def pane(self, id, title=None, height=0.25) -> str:
        """A lower pane below price (``'price'`` always exists). Returns its id."""
        call = "c.pane"
        check_element_id(id, call)
        if id == "price" or any(p["id"] == id for p in self._panes):
            _fail(call, "id", f"pane {id!r} already exists; panes: {['price'] + [p['id'] for p in self._panes]}.")
        if len(self._panes) >= schema.MAX_PANES:
            _fail(call, "id", f"a layer holds at most {schema.MAX_PANES} extra panes; put related lines in one pane.")
        self._panes.append(element(schema.LayerPane, call, id=id, title=opt_text(title, call, "title"),
                                   height=num(height, call, "height")))
        return id

    def plot(self, series, title, pane="price", type="line", color=None, width=2, style="solid",
             note=None, id=None) -> str:
        """An indicator line, area or histogram. Returns the series id (for ``c.fill``)."""
        call = "c.plot"
        if isinstance(title, (pd.Series, pd.DataFrame, tuple, np.ndarray)):
            _fail(call, "title", f"expected a string, got {builtins.type(title).__name__}; the signature is "
                                 f"c.plot(series, title, ...) with the values first.")
        _text(title, call, "title")
        if isinstance(series, tuple):
            _as_series(series, call, "series")  # raises the unpack-first message
        if len(self._series) >= schema.MAX_SERIES:
            _fail(call, "series", f"a layer holds at most {schema.MAX_SERIES} series.")
        pane = self._pane(pane, call)
        s = self._numbers(series, call, "series")
        vals = s.reindex(self._idx).to_numpy(dtype="float64")
        future = self._future(s)
        if np.isinf(vals).any():
            _fail(call, "series", f"{title!r} contains inf; replace it (e.g. .replace([np.inf, -np.inf], np.nan)).")
        keep = ~np.isnan(vals)
        if not keep.any() and not future:
            _fail(call, "series", f"{title!r} has no values on df's bars (all NaN after matching times). Check the "
                                  f"indicator's warmup against the {len(vals)} bars and that its index is df's.")
        per_point = None
        ahead: dict = {}
        base = PALETTE[len(self._series) % len(PALETTE)]
        if isinstance(color, (pd.Series, np.ndarray, list)):
            hue = self._on_bars(color, call, "color").astype(object)
            per_point = hue.reindex(self._idx).to_numpy()
            # A projected point takes the color the Series gives its time, as a bar does.
            later = {fmt_time(t, self.daily): v for t, v in hue[hue.index > self._idx[-1]].items()}
            ahead = {t: later.get(t) for t, _ in future}
            used = pd.Series([*per_point, *ahead.values()], dtype=object).dropna()
            hues = {v: _color(v, call) for v in pd.unique(used)}
            per_point = np.array([hues.get(v) for v in per_point], dtype=object)
            ahead = {t: hues.get(v) for t, v in ahead.items()}
        elif color is not None:
            base = _color(color, call)
        elif type == "histogram":
            per_point = np.where(vals >= 0, _HIST_UP, _HIST_DOWN).astype(object)
        data = []
        for i in np.flatnonzero(keep):
            pt = {"time": self._times[i], "value": float(vals[i])}
            if per_point is not None and isinstance(per_point[i], str):
                pt["color"] = per_point[i]
            data.append(pt)
        signed = type == "histogram" and color is None
        for t, v in future:
            pt = {"time": t, "value": v}
            if signed:
                pt["color"] = _HIST_UP if v >= 0 else _HIST_DOWN
            elif isinstance(ahead.get(t), str):
                pt["color"] = ahead[t]
            data.append(pt)
        sid = self._id(id, self._series, call, title, "series")
        self._series.append(self._element(
            schema.LayerSeries, call, note, id=sid, title=title, pane=pane, type=type, color=base,
            width=whole(width, call, "width"), style=style, data=data,
            args={"data": "series"}, advice={("data", "over"): "Plot a shorter window (e.g. s.iloc[-5000:])."}))
        return sid

    def fill(self, a, b, color=None, note=None) -> None:
        """Shade between two series in the same pane (ids from ``c.plot``)."""
        call = "c.fill"
        by_id = {s["id"]: s for s in self._series}
        for arg, v in (("a", a), ("b", b)):
            if not isinstance(v, str):
                _fail(call, arg, f"expected a series id returned by c.plot(), got {type(v).__name__}. Keep the "
                                 f"return value: up = c.plot(...); c.fill(up, lo).")
            if v not in by_id:
                _fail(call, arg, f"no series with id {v!r}; series ids: {list(by_id)}.")
        col = _color(color, call) if color is not None else with_alpha(by_id[a]["color"], 0.1)
        self._room(self._fills, schema.MAX_FILLS, 1, call, "a", "fills")
        self._fills.append(self._element(schema.LayerFill, call, note, a=a, b=b, color=col))

    def level(self, price, title=None, pane="price", color=None, style="dashed", note=None) -> None:
        """A horizontal level across the chart (support, resistance, a threshold in a pane)."""
        call = "c.level"
        self._room(self._levels, schema.MAX_LEVELS, 1, call, "price", "levels")
        self._levels.append(self._element(
            schema.LayerLevel, call, note, price=num(price, call, "price"), pane=self._pane(pane, call),
            style=style, title=opt_text(title, call, "title"),
            color=_color(color, call) if color is not None else _GRAY))

    # ------------------------------------------------------------------ per-bar marks

    def _marker(self, time_str, position, shape, color, text_, price, note, call) -> dict:
        return self._element(schema.LayerMarker, call, note, time=time_str, position=position, shape=shape,
                             color=color, text=text_ or None, price=price)

    @staticmethod
    def _marker_style(position, shape, color, call):
        if shape is None:
            shape = {"aboveBar": "arrowDown", "belowBar": "arrowUp"}.get(position, "circle")
        if color is None:
            color = {"aboveBar": _DOWN, "belowBar": _UP}.get(position, _NEUTRAL)
        return shape, _color(color, call)

    def marker(self, time, text=None, position="belowBar", shape=None, color=None, price=None, note=None) -> None:
        """One marker on one bar. ``position='atPrice'`` needs ``price``."""
        call = "c.marker"
        shape, col = self._marker_style(position, shape, color, call)
        t = self._t(time, call, "time")
        p = num(price, call, "price") if price is not None else None
        self._room(self._markers, schema.MAX_MARKERS, 1, call, "time", "markers")
        self._markers.append(self._marker(t, position, shape, col, opt_text(text, call, "text"), p, note, call))

    def markers(self, mask, text=None, position="belowBar", shape=None, color=None, price=None, note=None) -> int:
        """A marker on every bar where the boolean ``mask`` is True. Returns how many were added."""
        call = "c.markers"
        m = self._mask(mask, call, "mask")
        shape, col = self._marker_style(position, shape, color, call)
        txt = opt_text(text, call, "text")
        prices = None
        if price is not None:
            if isinstance(price, (pd.Series, np.ndarray, list)):
                prices = self._values(price, call, "price")
            else:
                prices = np.full(len(m), num(price, call, "price"))
        elif position == "atPrice":
            _fail(call, "price", "is required with position='atPrice' (a number or a Series like df.low).")
        # The style arguments are checked even when the mask selects nothing.
        self._marker(self._times[0], position, shape, col, txt, 0.0 if prices is not None else None, note, call)
        hits = np.flatnonzero(m)
        if prices is not None:
            hits = hits[~np.isnan(prices[hits])]  # a bar with no price gets no marker
        self._room(self._markers, schema.MAX_MARKERS, len(hits), call, "mask", "markers")
        added = 0
        for i in hits:
            p = None if prices is None else float(prices[i])
            self._markers.append(self._marker(self._times[i], position, shape, col, txt, p, note, call))
            added += 1
        if not added:
            self._notes.append(f"c.markers(text={text!r}): mask is all False, nothing added")
        return added

    def regime(self, when, color=None, label=None, note=None) -> int:
        """Shade time ranges: a boolean mask (each run of True bars becomes one range, ending on its
        last bar) or a ``(start, end)`` pair. Returns how many ranges were added."""
        call = "c.regime"
        col = _color(color, call) if color is not None else "rgba(41,98,255,0.08)"
        lab = opt_text(label, call, "label")
        if isinstance(when, tuple) and len(when) == 2 and not isinstance(when[0], (bool, np.bool_)):
            ranges = [(self._t(when[0], call, "when[0] (start)"), self._t(when[1], call, "when[1] (end)"))]
        else:
            m = self._mask(when, call, "when", hint=" Or pass a (start, end) pair of times.")
            ranges = [(self._times[i], self._times[j]) for i, j in _runs(m)]
        self._room(self._regimes, schema.MAX_REGIMES, len(ranges), call, "when", "regimes")
        for a, b in ranges:
            self._regimes.append(self._element(schema.LayerRegime, call, note, **{"from": a}, to=b, color=col,
                                               label=lab))
        if not ranges:
            self._notes.append(f"c.regime(label={label!r}): mask is all False, nothing added")
        return len(ranges)

    def alert(self, price, title, direction="either", note=None, id=None) -> str:
        """A price-cross alert drawn as a bell on its level. Returns its id."""
        call = "c.alert"
        level = num(price, call, "price")
        _text(title, call, "title")
        self._room(self._alerts, schema.MAX_ALERTS, 1, call, "price", "alerts")
        aid = self._id(id, self._alerts, call, title, "alert")
        self._alerts.append(self._element(
            schema.LayerAlert, call, note, id=aid, title=title,
            condition={"kind": "price_cross", "level": level, "direction": direction},
            args={"condition.direction": "direction"}))
        return aid

    # ------------------------------------------------------------------ drawings

    def _draw(self, call, type, fields, color, label, note, id, default_color=PALETTE[0], **hints) -> str:
        if len(self._drawings) >= schema.MAX_DRAWINGS:
            _fail(call, "id", f"a layer holds at most {schema.MAX_DRAWINGS} drawings.")
        lab = opt_text(label, call, "label")
        did = self._id(id, self._drawings, call, lab, type)
        col = _color(color, call) if color is not None else default_color
        self._drawings.append(self._element(schema.DRAWING_MODELS[type], call, note, type=type, id=did,
                                            label=lab, color=col, **fields, **hints))
        return did

    def segment(self, p1, p2, extend="none", style="solid", width=2, *, color=None, label=None, note=None,
                id=None, info=None) -> str:
        """A line between two points; ``extend`` makes it a ray."""
        call = "c.segment"
        f = {"p1": self._point(p1, call, "p1"), "p2": self._point(p2, call, "p2"), "extend": extend,
             "style": style, "width": whole(width, call, "width")}
        return self._draw(call, "segment", f, color, label, note, id)

    def vline(self, time, style="dashed", *, color=None, label=None, note=None, id=None, info=None) -> str:
        """A vertical line at a time, past or future (e.g. the next earnings date)."""
        call = "c.vline"
        return self._draw(call, "vline", {"time": self._t(time, call, "time"), "style": style}, color, label,
                          note, id, _GRAY)

    def channel(self, p1, p2, p3, extend="right", fill=None, *, color=None, label=None, note=None, id=None,
                info=None) -> str:
        """A parallel channel: the line p1-p2 and its parallel through p3, shaded between."""
        call = "c.channel"
        col = _color(color, call) if color is not None else PALETTE[0]
        f = {"p1": self._point(p1, call, "p1"), "p2": self._point(p2, call, "p2"),
             "p3": self._point(p3, call, "p3"), "extend": extend,
             "fill": _color(fill, call, "fill") if fill is not None else with_alpha(col, 0.08)}
        return self._draw(call, "channel", f, col, label, note, id)

    def pitchfork(self, p1, p2, p3, extend="right", *, color=None, label=None, note=None, id=None,
                  info=None) -> str:
        """Andrews pitchfork: median from p1 through the midpoint of p2-p3, parallels through p2 and p3."""
        call = "c.pitchfork"
        f = {"p1": self._point(p1, call, "p1"), "p2": self._point(p2, call, "p2"),
             "p3": self._point(p3, call, "p3"), "extend": extend}
        return self._draw(call, "pitchfork", f, color, label, note, id)

    def box(self, p1, p2, fill=None, *, color=None, label=None, note=None, id=None, info=None) -> str:
        """A zone between two opposite corners."""
        call = "c.box"
        col = _color(color, call) if color is not None else PALETTE[0]
        f = {"p1": self._point(p1, call, "p1"), "p2": self._point(p2, call, "p2"),
             "fill": _color(fill, call, "fill") if fill is not None else with_alpha(col, 0.12)}
        return self._draw(call, "box", f, col, label, note, id)

    def label(self, at, text, position="above", *, color=None, note=None, id=None, info=None) -> str:
        """A text chip at a point."""
        call = "c.label"
        f = {"at": self._point(at, call, "at"), "text": _text(text, call, "text"), "position": position}
        return self._draw(call, "label", f, color, None, note, id, _GRAY)

    def arrow(self, p1, p2, width=2, *, color=None, label=None, note=None, id=None, info=None) -> str:
        """An arrow from p1 to p2 (a projected move)."""
        call = "c.arrow"
        p1d, p2d = self._point(p1, call, "p1"), self._point(p2, call, "p2")
        default = _UP if p2d["price"] >= p1d["price"] else _DOWN
        f = {"p1": p1d, "p2": p2d, "width": whole(width, call, "width")}
        return self._draw(call, "arrow", f, color, label, note, id, default)

    def path(self, points, point_labels=None, style="solid", width=2, closed=False, *, color=None, label=None,
             note=None, id=None, info=None) -> str:
        """A polyline; ``point_labels`` (one per point, None to skip) turn it into a labelled pattern."""
        call = "c.path"
        f = {"points": self._points(points, call, "points"), "style": style,
             "width": whole(width, call, "width"), "closed": bool(closed)}
        if point_labels is not None:
            if isinstance(point_labels, str) or not isinstance(point_labels, (list, tuple)):
                _fail(call, "point_labels", f"expected a list with one label (or None) per point, got "
                                            f"{_short(point_labels)}.")
            f["point_labels"] = [None if x is None else _text(x, call, f"point_labels[{i}]")
                                 for i, x in enumerate(point_labels)]
        return self._draw(call, "path", f, color, label, note, id, PALETTE[1],
                          advice={("points", "over"): "Thin them (e.g. pts[::2])."})

    def fib(self, p1, p2, levels=None, extend="right", *, color=None, label=None, note=None, id=None,
            info=None) -> str:
        """Fibonacci levels between two points: ratio 1 at p1, 0 at p2 (draw p1 = swing start)."""
        call = "c.fib"
        f = {"p1": self._point(p1, call, "p1"), "p2": self._point(p2, call, "p2"), "extend": extend}
        if levels is not None:
            if isinstance(levels, (str, numbers.Real)) or not hasattr(levels, "__iter__"):
                _fail(call, "levels", f"expected a list of ratios like [0, 0.382, 0.618, 1], got {_short(levels)}.")
            f["levels"] = [num(x, call, f"levels[{i}]") for i, x in enumerate(levels)]
        return self._draw(call, "fib", f, color, label, note, id, _GRAY)

    def cone(self, origin, bands, *, color=None, label=None, note=None, id=None, info=None) -> str:
        """Forecast bands from ``origin``: ``[{'label', 'upper': [pts], 'lower': [pts], 'fill'?}]``, widest first."""
        call = "c.cone"
        o = self._point(origin, call, "origin")
        if isinstance(bands, dict) or not isinstance(bands, (list, tuple)):
            _fail(call, "bands", "expected a non-empty list of dicts: "
                                 "[{'label': '±1σ', 'upper': [(t, p), ...], 'lower': [(t, p), ...]}].")
        out = []
        for i, b in enumerate(bands):
            where = f"bands[{i}]"
            if not isinstance(b, dict):
                _fail(call, where, f"expected a dict with 'upper' and 'lower' point lists, got {type(b).__name__}.")
            band = {k: v for k, v in b.items() if k not in ("upper", "lower", "fill")}
            for k in ("upper", "lower"):
                if k in b:
                    band[k] = self._points(b[k], call, f"{where}['{k}']")
            if "label" in b:
                band["label"] = _text(b["label"], call, f"{where}['label']")
            band["fill"] = (_color(b["fill"], call, f"{where}['fill']") if b.get("fill") is not None
                            else _CONE_FILL.format(0.08 + 0.06 * i))
            out.append(band)
        return self._draw(call, "cone", {"origin": o, "bands": out}, color, label, note, id, advice={
            ("bands", "under"): "Pass [{'label': '±1σ', 'upper': [(t, p), ...], 'lower': [(t, p), ...]}].",
            ("upper", "over"): "Thin them (e.g. pts[::2]).", ("lower", "over"): "Thin them (e.g. pts[::2])."})

    def forecast(self, target, by, *, origin=None, probability=None, color=None, label=None, note=None, id=None,
                 info=None) -> str:
        """A forecast: price reaches ``target`` by ``by`` (a time, or a number of bars after ``origin``).

        Drawn from ``origin`` (default the last close) to the target, as TradingView's forecast; once
        ``by`` has passed, the chart marks it hit or missed from the bars in between.
        """
        call = "c.forecast"
        o = self._point(origin, call, "origin") if origin is not None else self._last_close(call)
        price = num(target, call, "target")
        if isinstance(by, numbers.Integral) and not isinstance(by, bool):
            if by < 1:
                _fail(call, "by", f"a number of bars must be at least 1, got {by}.")
            t = self._bars_after(o["time"], int(by), call, "by")[-1]
        else:
            t = self._t(by, call, "by")
        f = {"origin": o, "target": {"time": t, "price": price},
             "probability": num(probability, call, "probability") if probability is not None else None}
        return self._draw(call, "forecast", f, color, label, note, id, _UP if price >= o["price"] else _DOWN)

    def analog(self, start, end, *, origin=None, color=None, label=None, note=None, id=None, info=None) -> str:
        """The bars from ``start`` to ``end`` replayed from ``origin`` (default the last close).

        Each replayed bar keeps its percent distance from the close before ``start``, rescaled to the
        origin's price, and lands on the bars after the origin (future ones past the last bar).
        """
        call = "c.analog"
        missing = [k for k in ("open", "high", "low", "close") if k not in self._df.columns]
        if missing:
            _fail(call, "df", f"replaying bars needs open, high, low and close columns; df lacks {missing}.")
        a, b = self._t(start, call, "start"), self._t(end, call, "end")
        pos = [i for i, t in enumerate(self._times) if a <= t <= b]
        if not pos:
            _fail(call, "start", f"no bars of df fall from {a} to {b}." + (" The end is before the start." if b < a else ""))
        if len(pos) > schema.MAX_ANALOG_BARS:
            _fail(call, "end", f"{a} to {b} holds {len(pos)} bars; the limit is {schema.MAX_ANALOG_BARS}.")
        o = self._point(origin, call, "origin") if origin is not None else self._last_close(call)
        if a <= o["time"] <= b:
            _fail(call, "origin", f"{o['time']} falls inside the stretch it replays ({a} to {b}).")
        src = self._df.iloc[pos[0]:pos[-1] + 1]
        base = float(self._df["close"].iloc[pos[0] - 1]) if pos[0] > 0 else float(src["open"].iloc[0])
        if not base > 0:
            _fail(call, "start", f"the close before {a} is {base:g}; replaying needs a positive base price.")
        k = o["price"] / base
        times = self._bars_after(o["time"], len(src), call, "end")
        bars = [{"time": t, "open": float(r.open) * k, "high": float(r.high) * k, "low": float(r.low) * k,
                 "close": float(r.close) * k} for t, r in zip(times, src.itertuples(index=False))]
        f = {"origin": o, "source_start": a, "source_end": b, "bars": bars}
        return self._draw(call, "analog", f, color, label, note, id, _GRAY)

    def _last_close(self, call: str) -> dict:
        """The default origin of a drawing that starts at the last close."""
        price = finite(self._df["close"].iloc[-1]) if "close" in self._df.columns else None
        if price is None:
            _fail(call, "origin", "defaults to the last close, which df does not have; pass origin=(time, price).")
        return {"time": self._times[-1], "price": price}

    def _bars_after(self, time: str, n: int, call: str, arg: str) -> list[str]:
        """The ``n`` bar times after ``time``: df's own bars first, then future ones.

        An origin past df's last bar counts its future bars from there, not from df's end.
        """
        later = [t for t in self._times if t > time][:n]
        if len(later) == n:
            return later
        # Future slots run from df's end; read enough of them to get past the origin, then n more,
        # as far as future slots go.
        cap = schema.MAX_SERIES_POINTS
        size = min(n - len(later) + 8, cap)
        while True:
            ahead = [fmt_time(t, self.daily) for t in self.future(size)]
            ahead = [t for t in ahead if t > time]
            if len(ahead) >= n - len(later):
                break
            if size >= cap:
                _fail(call, arg, f"reaches past {cap} bars after df's last bar, the furthest a layer goes.")
            size = min(size * 2, cap)
        return later + ahead[:n - len(later)]

    def position(self, side, entry, stop, targets, until=None, *, color=None, label=None, note=None, id=None,
                 info=None) -> str:
        """A long/short setup: entry point, stop and target prices, shaded risk and reward with R multiples."""
        call = "c.position"
        e = self._point(entry, call, "entry")
        if isinstance(targets, numbers.Real) and not isinstance(targets, bool):
            targets = [targets]
        if isinstance(targets, (str, dict)) or not hasattr(targets, "__iter__"):
            _fail(call, "targets", f"expected a list of prices like [160.0, 170.0], got {_short(targets)}.")
        tg = [num(t, call, f"targets[{i}]") for i, t in enumerate(targets)]
        f = {"side": side, "entry": e, "stop": num(stop, call, "stop"), "targets": sorted(tg, reverse=side != "long"),
             "until": self._t(until, call, "until") if until is not None else None}
        return self._draw(call, "position", f, color, label, note, id, _UP if side == "long" else _DOWN)

    def measure(self, p1, p2, *, color=None, label=None, note=None, id=None, info=None) -> str:
        """A ruler from p1 to p2: price change, percent, bars and days."""
        call = "c.measure"
        f = {"p1": self._point(p1, call, "p1"), "p2": self._point(p2, call, "p2")}
        return self._draw(call, "measure", f, color, label, note, id, _GRAY)

    def profile(self, start, end, rows, poc=None, value_area=None, *, color=None, label=None, note=None,
                id=None, info=None) -> str:
        """A fixed-range volume profile drawn rightward from ``start``; feed it ``analysis.volume_profile``."""
        call = "c.profile"
        if isinstance(rows, (dict, str)) or not isinstance(rows, (list, tuple)):
            _fail(call, "rows", "expected a non-empty list of {'low', 'high', 'volume', 'up_volume'?} dicts; "
                                "build it with chartkit.analysis.volume_profile(df, start, end).")
        out = []
        for i, r in enumerate(rows):
            where = f"rows[{i}]"
            if not isinstance(r, dict) or not {"low", "high", "volume"} <= set(r):
                _fail(call, where, f"expected a dict with low, high, volume (and up_volume), got {_short(r)}.")
            row = {k: num(r[k], call, f"{where}['{k}']") for k in ("low", "high", "volume")}
            if r.get("up_volume") is not None:
                up = num(r["up_volume"], call, f"{where}['up_volume']")
                # Rounding in a profile built elsewhere may leave the up share a hair over the total.
                row["up_volume"] = row["volume"] if row["volume"] < up <= row["volume"] * (1 + 1e-9) else up
            out.append(row)
        f = {"from": self._t(start, call, "start"), "to": self._t(end, call, "end"), "rows": out,
             "poc": num(poc, call, "poc") if poc is not None else None}
        if value_area is not None:
            if not isinstance(value_area, (list, tuple)) or len(value_area) != 2:
                _fail(call, "value_area", f"expected [low, high], got {_short(value_area)}.")
            f["value_area"] = [num(value_area[0], call, "value_area[0]"), num(value_area[1], call, "value_area[1]")]
        return self._draw(call, "profile", f, color, label, note, id, _NEUTRAL, advice={
            ("rows", "under"): "Build it with chartkit.analysis.volume_profile(df, start, end)."})

    # ------------------------------------------------------------------ helpers for the agent

    def future(self, n) -> pd.DatetimeIndex:
        """The next ``n`` bar times after df's last bar (see ``chartkit.future_times``)."""
        from .analysis import future_times

        return future_times(self._df, n, self.tz, self.timeframe, self.symbol)

    # ------------------------------------------------------------------ publish

    def _counts(self) -> str:
        shown = []
        for kind, n in schema.counts(self.to_dict()).items():
            if n:
                shown.append(f"{n} {kind if n != 1 or kind == 'series' else kind[:-1]}")
        return ", ".join(shown)

    def _drawable(self) -> bool:
        return any(schema.counts(self.to_dict()).values())

    def _checked(self) -> tuple[dict, str, list[str]]:
        """The layer, its JSON line, and why the server would refuse that line (empty when it would not)."""
        layer = self.to_dict()
        line = json.dumps(layer, ensure_ascii=False, separators=(",", ":"))
        try:
            schema.parse_layer(line)
        except schema.LayerRejected as e:
            return layer, line, list(e.problems)
        return layer, line, []

    def to_dict(self) -> dict:
        """The layer as it would be published (no validation, nothing written)."""
        layer = {"version": schema.LAYER_VERSION, "id": self.id, "title": self.title}
        if self.summary:
            layer["summary"] = self.summary
        layer.update({
            "symbol": self.symbol, "timeframe": self.timeframe, "as_of": self.as_of,
            "source": {"kind": "python"}, "panes": self._panes, "series": self._series, "fills": self._fills,
            "levels": self._levels, "markers": self._markers, "regimes": self._regimes,
            "drawings": self._drawings, "alerts": self._alerts,
        })
        return layer

    def snapshot(self, path=None, *, theme=None, width=None, height=None, view=None) -> dict:
        """The chart as the user will see it with this layer, as a PNG; nothing is published.

        The chart's own bars, at the user's theme, size and language when their message carried their
        chart's view, framed as the layer opens there; ``theme``, ``width``, ``height`` and ``view`` override.
        Returns ``{"path", "report"}`` and prints the report. A computer that cannot render
        prints why and returns ``path`` None, so a script goes on to publish either way.
        """
        from ._snapshot import snapshot_chart

        return snapshot_chart(self, path, theme=theme, width=width, height=height, view=view)

    def publish(self, summary=None, **framing) -> dict:
        """Validate, then queue the layer for the user's chart. Returns the layer dict."""
        if framing:
            name = next(iter(framing))
            _fail("c.publish", name, "publish takes only summary=. Framing (view, theme, width, height) is "
                                     "for c.snapshot; the user's chart frames the layer itself when it lands.")
        if summary is not None:
            self.summary = _text(summary, "c.publish", "summary", SUMMARY)
        _, line, errors = self._checked()
        if not self._drawable():
            skipped = f" Skipped: {'; '.join(self._notes)}." if self._notes else ""
            errors.append("nothing to draw: add a series, level, marker, regime, drawing or alert first." + skipped)
        if errors:
            raise ChartError("c.publish(): the layer is not valid:\n  - " + "\n  - ".join(errors))
        path = os.environ.get(CHART_FILE_ENV)
        if not path:
            raise RuntimeError(
                "c.publish(): charts publish only from ExecuteCode/Bash runs in the sandbox "
                f"({CHART_FILE_ENV} is not set). Use c.to_dict() to inspect the layer without publishing."
            )
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        print(f'chart layer "{self.title}" queued for {self.symbol} {self.timeframe} ({self._counts()})')
        for n in self._notes:
            print(f"  note: {n}")
        return json.loads(line)


def _runs(mask: np.ndarray) -> list[tuple[int, int]]:
    """(first, last) bar positions of each run of True."""
    m = np.asarray(mask, dtype=bool)
    if not m.any():
        return []
    d = np.diff(np.concatenate(([0], m.view(np.int8), [0])))
    starts = np.flatnonzero(d == 1)
    ends = np.flatnonzero(d == -1) - 1
    return list(zip(starts.tolist(), ends.tolist()))
