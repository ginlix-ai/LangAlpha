"""Snapshots: the user's chart as a PNG, drawn by the app's own chart code.

The render page is the app's ``MarketChart`` alone, run in the sandbox's headless Chromium
(``_renderer``). The chart paints the user's bars from the market-data tool, asked for the windows
the app's chart asks for (``_bars_source``), not the agent's DataFrame, which draws the candles
only when the tool cannot answer. Theme, size and window are the user's when their message carried
their chart's view (``_view``), and a layer is framed by the rule the live chart frames it with
when it lands. The printed report (``_report``) says what the picture cannot.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ._bars_source import ChartBars, bar_rows
from .protocol import SNAPSHOT_DIR_ENV
from ._renderer import RENDERER, SnapshotJob, SnapshotUnavailable
from ._report import report
from ._util import _fail, chart_bars
from ._view import choose_frame, choose_look, user_locale, user_view
from .data import ohlc_missing


@dataclass(frozen=True)
class _Asked:
    """The call's own output and look arguments, unchecked."""

    path: Any = None
    theme: Any = None
    width: Any = None
    height: Any = None
    view: Any = None


# --------------------------------------------------------------------------- public entry points


def snapshot_chart(chart, path=None, *, theme=None, width=None, height=None, view=None) -> dict:
    """``Chart.snapshot``: the draft layer on the user's chart."""
    layer, _, errors = chart._checked()
    notes: list[str] = []
    if errors:
        notes.append("Layer not drawn: c.publish() would refuse it: " + "; ".join(errors[:4])
                     + (f" (+{len(errors) - 4} more)" if len(errors) > 4 else ""))
        layer = None
    elif not chart._drawable():
        notes.append("Layer not drawn: it has nothing to draw yet.")
        layer = None
    return _take(
        "c.snapshot", _Asked(path, theme, width, height, view),
        symbol=chart.symbol, tf=chart.timeframe, tz=chart.tz, df=chart._df, idx=chart._idx, layer=layer,
        name=f"{_file_part(chart.symbol)}-{chart.timeframe}-{chart.id}.png", notes=notes, counts=chart._counts(),
    )


def snapshot(data, timeframe=None, path=None, *, symbol=None, theme=None, width=None, height=None,
             view=None) -> dict:
    """``chartkit.snapshot``: the plain chart, no layer, for a symbol or a bars DataFrame."""
    call = "chartkit.snapshot"
    if isinstance(data, str):
        df, symbol = None, symbol if symbol is not None else data
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        _fail(call, "data", f"expected a symbol like 'NVDA' or a bars DataFrame, got {type(data).__name__}.")
    basis = chart_bars(df, symbol, timeframe, None, call)
    return _take(call, _Asked(path, theme, width, height, view), symbol=basis.symbol, tf=basis.timeframe,
                 tz=basis.tz, df=df, idx=basis.index, layer=None,
                 name=f"{_file_part(basis.symbol)}-{basis.timeframe}.png")


# --------------------------------------------------------------------------- the snapshot


def _take(call: str, asked: _Asked, *, symbol: str, tf: str, tz: str, df, idx, layer: dict | None, name: str,
          notes: list[str] | None = None, counts: str = "") -> dict:
    user, others = user_view(symbol, tf)
    look = choose_look(call, symbol, tf, user, others, asked.theme, asked.width, asked.height)
    frame = choose_frame(call, asked.view, layer, user, tf, tz)
    out = _out_path(asked.path, name, call)
    df_rows, df_gap = None, None
    if df is not None:
        missing = ohlc_missing(df)
        if missing:
            df_gap = f"your DataFrame has no {'/'.join(missing)} column"
        else:
            df_rows = bar_rows(df, idx, tz)
    job = SnapshotJob(
        symbol=symbol, timeframe=tf, tz=tz, layer=layer, path=str(out),
        theme=look.theme, width=look.width, height=look.height, locale=user_locale() or "en-US",
        bars=ChartBars(symbol, tf, tz, df_rows, df_gap), frame=frame, user=user,
        currency=(getattr(df, "attrs", {}) or {}).get("currency") if df is not None else None,
        df=df, idx=idx, look_line=look.line, notes=tuple(notes or ()), counts=counts,
    )
    try:
        rendered = RENDERER.run(job)
    except SnapshotUnavailable as e:
        msg = f"{call}(): no snapshot: {e}"
        print(msg)
        return {"path": None, "report": msg}
    text = report(job, rendered)
    print(text)
    return {"path": str(out), "report": text}


def _file_part(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-") or "chart"


def _out_path(path, name: str, call: str) -> Path:
    if path is None:
        base = os.environ.get(SNAPSHOT_DIR_ENV) or os.path.join(os.getcwd(), ".agents", "charts")
        out = Path(base) / name
    else:
        if not isinstance(path, (str, os.PathLike)):
            _fail(call, "path", f"expected a file path ending in .png, got {type(path).__name__}.")
        out = Path(path)
        if out.suffix.lower() != ".png":
            _fail(call, "path", f"must end in .png, got {str(path)!r}.")
    out = out.expanduser().absolute()
    out.parent.mkdir(parents=True, exist_ok=True)
    return out
