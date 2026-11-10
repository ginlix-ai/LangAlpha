"""Two pins on what chartkit hands out: the layer JSON a script publishes and the snapshot report
text an agent reads. Both are contracts, so a refactor of the builders or the report must leave the
fixtures' output untouched (``UPDATE_GOLDEN=1 pytest tests/test_golden.py`` rewrites them on purpose)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from chartkit import Chart, analysis, ta
from chartkit import _renderer, _report, _view
from chartkit._bars_source import ChartBars, bar_rows
from chartkit._renderer import SnapshotJob

GOLDEN = Path(__file__).parent / "golden"


def _stable(value):
    """Floats to six places: numpy's last bits differ between platforms, and the pin is on what
    the layer says, not on a regression's rounding."""
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {k: _stable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_stable(v) for v in value]
    return value


def _lines(layer: dict) -> str:
    """The layer with one element per line, so a drift diffs to the element that moved."""
    out = []
    for key, value in sorted(_stable(layer).items()):
        if isinstance(value, list):
            out += [f"{key}[{i}] " + json.dumps(v, sort_keys=True) for i, v in enumerate(value)] or [f"{key} []"]
        else:
            out.append(f"{key} " + json.dumps(value, sort_keys=True))
    return "\n".join(out) + "\n"


def _check(name: str, text: str) -> None:
    path = GOLDEN / name
    if os.environ.get("UPDATE_GOLDEN"):
        path.write_text(text, encoding="utf-8")
    assert text == path.read_text(encoding="utf-8")


def _daily(n: int = 120) -> pd.DataFrame:
    idx = pd.bdate_range("2026-01-02", periods=n)
    close = 100 + np.sin(np.arange(n) / 4) * 5 + np.arange(n) * 0.2
    return pd.DataFrame(
        {"open": close - 0.5, "high": close + 1, "low": close - 1, "close": close,
         "volume": 1e6 + np.arange(n) * 1e3}, index=idx)


def _full_daily() -> Chart:
    df = _daily()
    c = Chart(df, "NVDA", title="Everything on a daily chart", summary="One of each element.")
    rsi = c.pane("rsi", "RSI 14", height=0.25)
    c.pane("macd", height=0.2)
    sma = c.plot(ta.sma(df.close, 5), "SMA 5", note="trend")
    up = c.plot(ta.sma(df.close, 20) * 1.02, "Upper", color="orange", width=1, style="dotted")
    lo = c.plot(ta.sma(df.close, 20) * 0.98, "Lower", type="area", id="lower-band")
    c.fill(up, lo)
    c.fill(sma, lo, color="rgba(1,2,3,0.5)", note="shaded")
    c.plot(ta.rsi(df.close, 14), "RSI", pane=rsi)
    c.plot(ta.macd(df.close, 3, 6, 3).hist, "Hist", pane="macd", type="histogram")
    ramp = pd.Series(np.where(np.arange(len(df)) % 2 == 0, "#26a69a", "red"), index=df.index)
    c.plot(df.close.diff().fillna(0), "Delta", pane="macd", type="histogram", color=ramp)
    c.plot(pd.Series([101.0, 102.5], index=c.future(2)), "Projection")
    c.level(110, "Resistance", note="prior high")
    c.level(70, "OB", pane=rsi, color="#ff0000", style="dotted")
    c.marker(df.index[10], "Earnings", position="aboveBar")
    c.marker(df.index[20], position="atPrice", price=105, shape="square", color="blue", note="fill")
    c.markers(ta.crossover(df.close, ta.sma(df.close, 20)), "x", price=df.low)
    c.markers(df.close > df.close.max() - 1, position="inBar")
    c.regime(df.close > df.close.rolling(20).mean(), label="uptrend", note="above MA")
    c.regime((df.index[3], df.index[8]), color="rgba(239,83,80,0.1)")
    c.alert(112.5, "Breakout", "above", note="watch")
    c.alert(95, "Breakdown", id="bd")
    t0, t1, t2 = df.index[10], df.index[40], df.index[25]
    a, b, d3 = (t0, 100.0), (t1, 110.0), (t2, 104.0)
    fut = c.future(10)
    c.segment(a, b, extend="right", style="dashed", width=3, color="teal", label="trend", note="n")
    c.vline(fut[5], label="Earnings", style="dotted")
    c.channel(**analysis.regression_channel(df, 40))
    c.channel(a, b, d3, extend="both", fill="rgba(5,5,5,0.2)", color="#112233")
    c.pitchfork(a, b, d3, extend="left", id="pf")
    c.box(a, b, fill="rgba(9,9,9,0.3)")
    c.label(b, "Top", position="right", color="gold")
    c.arrow(b, (fut[-1], 120.0), width=3)
    c.arrow(b, (fut[-1], 90.0))
    c.path([a, d3, b], ["A", None, "C"], closed=True, style="dotted", width=1)
    c.path([a, d3, b])
    c.fib(a, b, levels=[0, 0.5, 1], extend="left")
    c.fib(a, b)
    c.cone(**analysis.vol_cone(df, 10))
    c.cone(b, [{"label": "x", "upper": [(fut[0], 112.0)], "lower": [(fut[0], 108.0)], "fill": "silver"}])
    c.forecast(130, 10, probability=0.6, label="Breakout")
    c.forecast(90, fut[3], origin=(df.index[-1], 100.0), note="down")
    c.analog(df.index[10], df.index[14])
    c.position("long", b, 105, [120, 115], until=fut[-1])
    c.position("short", (t1, 110.0), 115, 100, label="short")
    c.measure(a, b)
    c.profile(**analysis.volume_profile(df, df.index[20], None, bins=12))
    c.profile(t0, t1, [{"low": 1, "high": 2, "volume": 3, "up_volume": 1}], poc=1.5, value_area=(1, 2))
    return c


def _full_intraday() -> Chart:
    idx = pd.date_range("2026-10-29 09:30", periods=80, freq="5min", tz="America/New_York")
    close = 200 + np.cos(np.arange(80) / 6) * 3
    df = pd.DataFrame({"open": close - 0.2, "high": close + 0.5, "low": close - 0.5, "close": close,
                       "volume": 1e4}, index=idx)
    c = Chart(df, "AAPL", "5min", title="Intraday")
    c.plot(ta.ema(df.close, 8), "EMA 8")
    c.plot(ta.vwap(df), "VWAP", color="#7e57c2")
    c.level(201, "Pivot")
    c.marker(idx[5], "Buy")
    c.markers(df.close > df.close.rolling(10).mean(), position="belowBar")
    c.regime((idx[10], idx[20]), label="chop")
    c.vline(idx[30], label="News")
    c.segment((idx[2], 199.0), (idx[40], 203.0))
    c.forecast(205, 12)
    c.analog(idx[5], idx[9])
    c.position("long", (idx[20], 200.0), 198, [203, 206])
    return c


def test_the_published_layer_is_pinned_daily():
    layer = json.loads(json.dumps(_full_daily().to_dict()))
    _check("layer_daily.txt", _lines(layer))


def test_the_published_layer_is_pinned_intraday():
    layer = json.loads(json.dumps(_full_intraday().to_dict()))
    _check("layer_intraday.txt", _lines(layer))


def _report_text() -> str:
    days = ["2026-10-29", "2026-10-30"]
    regular = pd.DatetimeIndex([f"{d} {h:02d}:{m:02d}" for d in days for h in range(9, 16) for m in range(60)
                                if (h, m) >= (9, 30)])
    df = pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 100.0}, index=regular)
    answer = {"symbol": "NVDA", "interval": "1min", "currency": "USD", "timezone": "America/New_York",
              "source": "ginlix-data",
              "data": [{"date": str(t), "open": 99, "high": 101, "low": 98, "close": 100.5, "volume": 5}
                       for t in pd.date_range("2026-10-29 04:00", "2026-10-30 19:59", freq="min")]}
    bars = ChartBars("NVDA", "1min", "America/New_York", bar_rows(df, regular, "America/New_York"),
                     tool=lambda **kw: answer)
    bars.answer("2026-10-28", "2026-10-31")
    bars.misses.append("2025-01-01 to 2025-12-31 (RuntimeError: connection reset)")
    rep = {
        "view": {"futureSlots": 12, "panes": ["price", "rsi"]},
        "layers": [{"id": "a", "title": "A"}, {"id": "b", "title": "B"}],
        "droppedLabels": [{"layer": "a", "element": "res", "text": "Resistance"},
                          {"layer": "b", "element": "sup", "text": "Support"}],
        "offscreen": [{"layer": "a", "element": "old", "kind": "vline"}, {"element": "lone"}],
        "cutText": [{"layer": "a", "element": "t", "where": "title", "text": "A very long title", "shown": "A very…"},
                    {"layer": "a", "element": "u", "where": "axis", "text": "Prior", "shown": "Pri…"}],
        "coveredLabels": [{"layer": "a", "element": "fomc", "kind": "vline", "by": "chip"},
                          {"layer": "b", "element": "n", "kind": "label", "by": "tag", "byText": "Close"}],
        "markersOverCandles": [{"text": f"Marker {i}"} for i in range(10)],
        "forecasts": [{"layer": "a", "element": "up", "label": "Run", "state": "hit", "at": "2026-10-13"},
                      {"layer": "b", "element": "dn", "state": "missed", "at": "2026-10-12"},
                      {"layer": "a", "element": "p", "state": "pending", "at": "2026-11-05T09:30"}],
        "notes": ["first note", "second note"],
    }
    rendered = _renderer.Rendered(
        report=rep, view={"from": "2026-10-30T04:00", "to": "2026-10-30T19:59"}, framed="ok", size=(740, 762),
        unanswered=["GET /x", "GET /x", "GET /y"], page_errors=["boom"])
    user = _view.UserView("NVDA", "1min", "light", 740, 762, "2026-10-29T09:30", "2026-10-29T16:00")
    job = SnapshotJob(
        symbol="NVDA", timeframe="1min", tz="America/New_York", layer={"id": "a", "title": "Layer A"},
        path="/tmp/NVDA-1min-a.png", theme="light", width=740, height=762, bars=bars,
        frame=_view.Frame("layer", layer_id="a"), user=user, df=df, idx=regular, look_line="Look line.",
        notes=("Layer not drawn: nothing yet.", "c.markers(text='x'): mask is all False, nothing added"),
        counts="2 series, 1 level")
    return _report.report(job, rendered)


def test_the_report_text_is_pinned():
    _check("report.txt", _report_text() + "\n")
