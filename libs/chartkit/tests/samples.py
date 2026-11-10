"""ChartLayer v1 samples as a script would publish them: every element kind and
every drawing type, on a daily and an intraday chart."""

from __future__ import annotations

import copy
from datetime import date, timedelta
from typing import Any


def _sessions(start: str, n: int) -> list[str]:
    day, out = date.fromisoformat(start), []
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day += timedelta(days=1)
    return out


T = _sessions("2026-06-01", 110)
F = _sessions("2026-10-29", 30)  # past the last bar


def p(time: str, price: float) -> dict[str, Any]:
    return {"time": time, "price": price}


INDICATORS: dict[str, Any] = {
    "version": 1,
    "id": "nvda-trend-kit",
    "title": "NVDA EMA20 + RSI regime",
    "summary": "Price holds above a rising EMA20; RSI cycles between 30 and 70.",
    "symbol": "NVDA",
    "timeframe": "1day",
    "as_of": T[-1],
    "source": {"kind": "python"},
    "panes": [{"id": "rsi", "title": "RSI 14", "height": 0.25}],
    "series": [
        {
            "id": "ema20", "title": "EMA 20", "pane": "price", "type": "line",
            "color": "#2962ff", "width": 2, "style": "solid", "note": "Trend filter.",
            "data": [{"time": t, "value": 150 + i * 0.5} for i, t in enumerate(T)],
        },
        {
            "id": "ema20-lo", "title": "EMA 20 -5%", "color": "rgba(41,98,255,0.4)", "style": "dashed",
            "data": [{"time": t, "value": 142 + i * 0.5} for i, t in enumerate(T)],
        },
        {
            "id": "rsi14", "title": "RSI 14", "pane": "rsi", "color": "#7e57c2",
            "data": [{"time": t, "value": 50 + (i % 20) - 10} for i, t in enumerate(T)],
        },
        {
            "id": "vol", "title": "Volume delta", "pane": "rsi", "type": "histogram",
            "data": [{"time": t, "value": i % 7 - 3, "color": "#26a69a"} for i, t in enumerate(T)],
        },
    ],
    "fills": [{"a": "ema20", "b": "ema20-lo", "color": "rgba(41,98,255,0.08)", "note": "5% band"}],
    "levels": [
        {"price": 70, "pane": "rsi", "title": "OB", "style": "dashed"},
        {"price": 30, "pane": "rsi", "title": "OS", "style": "dashed"},
        {"price": 205.5, "title": "Resistance", "color": "#ef5350", "note": "Three rejections."},
    ],
    "markers": [
        {"time": T[20], "position": "belowBar", "shape": "arrowUp", "text": "RSI<30"},
        {"time": T[60], "position": "atPrice", "price": 180.2, "shape": "circle", "text": "gap fill"},
    ],
    "regimes": [
        {"from": T[10], "to": T[35], "color": "rgba(38,166,154,0.08)", "label": "uptrend"},
        {"from": T[36], "to": T[50], "color": "rgba(239,83,80,0.08)", "label": "pullback"},
    ],
    "alerts": [
        {
            "id": "break-205", "title": "Break above 205.50",
            "condition": {"kind": "price_cross", "level": 205.5, "direction": "above"},
        }
    ],
}

DRAWINGS: dict[str, Any] = {
    "version": 1,
    "id": "nvda-setup",
    "title": "NVDA channel, long setup and a retest forecast",
    "summary": "Rising channel since June; long at the lower rail with 2 targets.",
    "symbol": "NVDA",
    "timeframe": "1day",
    "as_of": "2026-10-28",
    "drawings": [
        {"type": "segment", "id": "trend", "p1": p(T[5], 140), "p2": p(T[90], 190), "extend": "right", "width": 2},
        {"type": "vline", "id": "earnings", "time": F[15], "label": "Q3 earnings", "style": "dashed"},
        {"type": "channel", "id": "chan", "p1": p(T[0], 148.0), "p2": p(T[100], 196.4), "p3": p(T[40], 182.1),
         "extend": "right", "fill": "rgba(41,98,255,0.06)"},
        {"type": "pitchfork", "p1": p(T[10], 160), "p2": p(T[30], 185), "p3": p(T[45], 165), "extend": "right"},
        {"type": "box", "label": "Demand zone", "p1": p(T[80], 176), "p2": p(T[60], 170), "fill": "#26a69a26"},
        {"type": "label", "at": p(T[70], 210), "text": "Distribution?", "position": "above"},
        {"type": "arrow", "p1": p(T[-1], 196), "p2": p(F[20], 225), "width": 2, "label": "measured move"},
        {"type": "path", "label": "H&S",
         "points": [p(T[50], 180), p(T[55], 195), p(T[60], 182), p(T[66], 205), p(T[72], 183), p(T[78], 196), p(T[84], 181)],
         "point_labels": [None, "LS", None, "H", None, "RS", None]},
        {"type": "fib", "p1": p(T[20], 150.2), "p2": p(T[66], 205.0), "levels": [0, 0.382, 0.5, 0.618, 1]},
        {"type": "cone", "label": "IV cone", "origin": p(T[-1], 196.0), "bands": [
            {"label": "2σ", "upper": [p(F[9], 222), p(F[19], 234)], "lower": [p(F[9], 170), p(F[19], 158)]},
            {"label": "1σ", "upper": [p(F[9], 209), p(F[19], 215)], "lower": [p(F[9], 183), p(F[19], 177)]},
        ]},
        {"type": "forecast", "id": "retest", "origin": p(T[-1], 196.0), "target": p(F[10], 215.0),
         "probability": 0.35, "label": "Retest high"},
        {"type": "analog", "label": "June base", "origin": p(T[-1], 196.0), "source_start": T[20], "source_end": T[23],
         "bars": [
             {"time": F[2], "open": 196.0, "high": 199.0, "low": 194.0, "close": 198.0},
             {"time": F[3], "open": 198.0, "high": 202.0, "low": 197.0, "close": 201.0},
             {"time": F[4], "open": 201.0, "high": 203.0, "low": 198.0, "close": 199.0},
             {"time": F[5], "open": 199.0, "high": 205.0, "low": 198.5, "close": 204.0},
         ]},
        {"type": "position", "id": "long-1", "side": "long", "entry": p("2026-10-28", 658.52), "stop": 618.59,
         "targets": [738.38, 778.32], "until": F[25], "note": "Entry at the lower rail."},
        {"type": "measure", "p1": p(T[20], 150.2), "p2": p(T[66], 205.0)},
        {"type": "profile", "from": T[40], "to": T[-1],
         "rows": [{"low": 170 + i * 2, "high": 172 + i * 2, "volume": 1000 + i, "up_volume": 500} for i in range(14)],
         "poc": 183.0, "value_area": [176.0, 190.0]},
    ],
}

INTRADAY: dict[str, Any] = {
    "version": 1,
    "id": "spy-orb",
    "title": "SPY opening range",
    "symbol": "spy",
    "timeframe": "5min",
    "as_of": "2026-10-29T15:55",
    "levels": [{"price": 581.2, "title": "ORH"}, {"price": 578.9, "title": "ORL"}],
    "markers": [{"time": "2026-10-29T10:05:00", "position": "aboveBar", "shape": "arrowDown"}],
    "drawings": [{"type": "box", "p1": p("2026-10-29T09:30", 578.9), "p2": p("2026-10-29T10:00", 581.2)}],
}


def sample(name: str = "drawings") -> dict[str, Any]:
    """A fresh copy, to mutate."""
    return copy.deepcopy({"indicators": INDICATORS, "drawings": DRAWINGS, "intraday": INTRADAY}[name])
