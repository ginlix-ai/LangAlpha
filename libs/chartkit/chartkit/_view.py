"""How a snapshot looks and what it frames: the user's chart view, theme, size and language, the window.

The user's charts on screen arrive in the job's env (``PTC_CHART_VIEWS``) when their message
carried them. A snapshot of one of those charts takes its theme, size and window unless the call
says otherwise; any other chart gets MarketView's default look.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import pandas as pd
from market_protocol import display_spelling

from .protocol import CHART_VIEWS_ENV
from ._util import _fail, finite, fmt_time, parse_time
from .schema import TIMEFRAMES

THEMES = ("light", "dark")
#: MarketView's chart region at 1440x900 with the chat panel open, for a user who sent no view.
DEFAULT_THEME, DEFAULT_SIZE = "light", (740, 762)
WIDTH_RANGE, HEIGHT_RANGE = (300, 3000), (200, 2000)
#: The chart's bottom-bar presets and the interval each charts with (web/src/lib/bars/rangePresets.ts).
RANGE_PRESETS = {"1D": "1min", "5D": "15min", "1M": "1hour", "3M": "4hour", "6M": "1day",
                 "YTD": "1day", "1Y": "1day", "5Y": "1day", "All": "1day"}
_SPAN_DAYS = {"5D": 7, "1M": 30, "3M": 91, "6M": 182, "1Y": 365, "5Y": 1826}


@dataclass(frozen=True)
class UserView:
    """One chart the user had on screen. ``symbol`` is in the spelling the server stores."""

    symbol: str
    timeframe: str
    theme: str | None = None
    width: int | None = None
    height: int | None = None
    start: str | None = None
    end: str | None = None
    #: The reader's language (``en-US``, ``zh-CN``), for the chart's own text.
    locale: str | None = None

    @property
    def window(self) -> tuple[str, str] | None:
        return (self.start, self.end) if self.start and self.end else None


@dataclass(frozen=True)
class Look:
    theme: str
    width: int
    height: int
    #: The report's line on where the look came from.
    line: str


@dataclass(frozen=True)
class Frame:
    """What the chart is framed on once its bars are in.

    ``kind`` is ``default`` (the chart's opening view), ``preset`` (``preset`` names it),
    ``range`` or ``user`` (``start`` to ``end``), or ``layer`` (``layer_id``, its annotation id).
    """

    kind: str
    start: str | None = None
    end: str | None = None
    preset: str | None = None
    layer_id: str | None = None


def user_views() -> list[UserView]:
    """The user's charts on screen, from the job's env; anything unreadable is skipped."""
    raw = os.environ.get(CHART_VIEWS_ENV)
    if not raw:
        return []
    try:
        items = json.loads(raw)
    except ValueError:
        return []
    if isinstance(items, dict):
        items = [items]
    views = []
    for v in items if isinstance(items, list) else []:
        if not isinstance(v, dict) or v.get("type", "chart_view") != "chart_view":
            continue
        sym, tf = v.get("symbol"), v.get("timeframe")
        if not isinstance(sym, str) or not sym.strip() or tf not in TIMEFRAMES:
            continue
        views.append(UserView(
            symbol=display_spelling(sym), timeframe=tf,
            theme=v.get("theme") if v.get("theme") in THEMES else None,
            width=_clamp(v.get("width"), *WIDTH_RANGE), height=_clamp(v.get("height"), *HEIGHT_RANGE),
            start=v.get("from") if isinstance(v.get("from"), str) and v.get("from") else None,
            end=v.get("to") if isinstance(v.get("to"), str) and v.get("to") else None,
            locale=v.get("locale") if isinstance(v.get("locale"), str) and len(v["locale"]) <= 35 else None,
        ))
    return views


def user_locale() -> str | None:
    """The reader's language, from any chart they sent: it is theirs, not one chart's."""
    return next((v.locale for v in user_views() if v.locale), None)


def _clamp(v, lo, hi) -> int | None:
    n = finite(v)
    return None if n is None or n <= 0 else int(round(min(max(n, lo), hi)))


def user_view(symbol: str, tf: str) -> tuple[UserView | None, list[str]]:
    """The user's view of this chart, if they sent one, and the other charts they had on screen.

    Symbols compare in the server's stored spelling, so ``700.HK`` finds a view of ``0700.HK``.
    """
    views = user_views()
    want = display_spelling(symbol)
    for v in views:
        if v.symbol == want and v.timeframe == tf:
            return v, []
    return None, [f"{v.symbol} {v.timeframe}" for v in views]


def _size(v, call, arg, lo, hi) -> int:
    n = finite(v)
    if n is None or not lo <= n <= hi:
        _fail(call, arg, f"expected a number of pixels from {lo} to {hi}, got {v!r}.")
    return int(round(n))


def choose_look(call, symbol, tf, user: UserView | None, others: list[str], theme, width, height) -> Look:
    """Theme and size: what the call asked, else the user's chart, else MarketView's default."""
    if theme is not None:
        theme = str(theme).lower() if isinstance(theme, str) else theme
        if theme not in THEMES:
            _fail(call, "theme", f"expected 'light' or 'dark', got {theme!r}.")
    if width is not None:
        width = _size(width, call, "width", *WIDTH_RANGE)
    if height is not None:
        height = _size(height, call, "height", *HEIGHT_RANGE)
    base_theme, (base_w, base_h) = DEFAULT_THEME, DEFAULT_SIZE
    if user:
        base_theme = user.theme or base_theme
        base_w, base_h = user.width or base_w, user.height or base_h
    look = (theme or base_theme, width or base_w, height or base_h)
    drawn = f"{look[0]} {look[1]}x{look[2]}"
    if user:
        theirs = f"{base_theme}, {base_w}x{base_h}"
        if look == (base_theme, base_w, base_h):
            line = f"Matches the user's chart: {theirs}, {symbol} {tf}."
        else:
            line = f"The user's {symbol} {tf} chart is {theirs}; drawn {drawn} as you asked."
    else:
        seen = f" (on their screen: {', '.join(others)})" if others else ""
        line = f"No chart view from the user for {symbol} {tf}{seen}; using {drawn}."
    return Look(*look, line=line)


def choose_frame(call, view, layer: dict | None, user: UserView | None, tf: str, tz: str) -> Frame:
    """What the chart is framed on once its bars are in: an asked view, the layer, the user's window."""
    if isinstance(view, str) and view.strip().lower() == "user":
        if not (user and user.window):
            _fail(call, "view", "'user' needs the user's chart view, and their message carried none for "
                                "this chart. Leave view out, or pass a preset or a (start, end) pair.")
        return Frame("user", start=user.start, end=user.end)
    if view is not None:
        if isinstance(view, str):
            key = {k.lower(): k for k in RANGE_PRESETS}.get(view.strip().lower())
            if key is None:
                _fail(call, "view", f"expected 'user', one of {' '.join(RANGE_PRESETS)}, or a (start, end) "
                                    f"pair, got {view!r}.")
            return Frame("preset", preset=key)
        if isinstance(view, (tuple, list)) and len(view) == 2:
            a, b = (parse_time(t, call, f"view[{i}]", tz) for i, t in enumerate(view))
            daily = tf == "1day"
            return Frame("range", start=fmt_time(min(a, b), daily, "view"), end=fmt_time(max(a, b), daily, "view"))
        _fail(call, "view", f"expected 'user', one of {' '.join(RANGE_PRESETS)}, or a (start, end) pair, "
                            f"got {view!r}.")
    if layer is not None:
        return Frame("layer", layer_id=layer["id"])
    if user and user.window:
        return Frame("user", start=user.start, end=user.end)
    return Frame("default")


def preset_window(key: str, times: pd.DatetimeIndex, tf: str) -> tuple[str, str] | None:
    """A preset's window on these bars, by the bottom bar's rule (``rangeStartChartSec``)."""
    if len(times) == 0:
        return None
    last = times[-1]
    if key == "All":
        start = times[0]
    elif key == "1D":
        start = last.normalize()
    elif key == "YTD":
        start = pd.Timestamp(year=last.year, month=1, day=1)
    else:
        start = last - pd.Timedelta(days=_SPAN_DAYS[key])
    daily = tf == "1day"
    return fmt_time(start, daily, "view"), fmt_time(last, daily, "view")
