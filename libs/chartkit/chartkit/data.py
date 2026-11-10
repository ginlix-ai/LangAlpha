"""The chart's own OHLCV, through the sandbox's market-data tool: ``bars()`` and ``fetch_bars()``."""

from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

from ._util import ChartError, _fail, _short, clip, first_line, fmt_time, local_index, parse_time
from ._util import timeframe as _timeframe

# Intraday calls need both dates; without them, fetch about this many calendar days back.
_INTRADAY_DAYS = {"1min": 5, "5min": 20, "15min": 40, "30min": 60, "1hour": 120, "4hour": 365}
_OHLC = ("open", "high", "low", "close")
TOOLS_MISSING = "the market-data tools (tools.price_data) are not importable in this run"


class BarsUnavailable(Exception):
    """The market-data tool gave no bars to use. ``reason`` says why, in a clause that reads after
    "the market-data tool failed: "."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class ToolsUnavailable(BarsUnavailable):
    """This run cannot import the market-data tool at all."""


@dataclass(frozen=True)
class Fetched:
    """One answer of the tool. ``frame`` holds float columns open, high, low, close (and volume when
    sent), indexed by naive exchange-local bar time named ``time``, oldest first, one row per time;
    it is empty when the tool had no bars for the window."""

    frame: pd.DataFrame
    symbol: str | None
    timezone: str | None
    source: str | None
    currency: str | None


def ohlc_missing(df) -> list[str]:
    cols = {str(c).lower() for c in df.columns}
    return [c for c in _OHLC if c not in cols]


def fetch_bars(symbol: str, timeframe: str, start: str | None = None, end: str | None = None, *,
               tool=None) -> Fetched:
    """``get_stock_data`` for one window, read into a frame; raises ``BarsUnavailable``.

    ``start``/``end`` are the tool's own date strings. ``tool`` stands in for the generated
    wrapper, which only exists in the sandbox.
    """
    if tool is None:
        try:
            from tools.price_data import get_stock_data as tool  # generated MCP wrapper, sandbox only
        except ImportError:
            raise ToolsUnavailable(TOOLS_MISSING) from None
    args: dict = {"symbol": symbol, "interval": timeframe}
    if start:
        args["start_date"] = start
    if end:
        args["end_date"] = end
    try:
        res = tool(**args)
    except Exception as e:  # noqa: BLE001 - the tool's failure is the reason there are no bars
        raise BarsUnavailable(f"{type(e).__name__}: {clip(first_line(e))}") from e
    if isinstance(res, str):
        try:
            res = json.loads(res)
        except ValueError:
            raise BarsUnavailable(f"it returned text, not bars: {clip(res)}") from None
    if not isinstance(res, dict):
        raise BarsUnavailable(f"it returned {type(res).__name__}, not bars")
    if res.get("error"):
        raise BarsUnavailable(clip(f"{res.get('error')}: {res.get('detail') or ''}".rstrip(": ")))
    tz = res.get("timezone") or None
    return Fetched(_frame(res.get("data") or [], timeframe, tz), res.get("symbol") or None, tz,
                   res.get("source") or None, res.get("currency") or None)


def _frame(rows: list, timeframe: str, tz: str | None) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(_OHLC), index=pd.DatetimeIndex([], name="time"), dtype="float64")
    try:
        df = pd.DataFrame(rows)
        index = local_index(pd.DatetimeIndex(pd.to_datetime(df.pop("date"))), tz)
    except (KeyError, ValueError, TypeError) as e:
        raise BarsUnavailable(f"its bars are unreadable ({type(e).__name__})") from None
    missing = ohlc_missing(df)
    if missing:
        raise BarsUnavailable(f"its bars have no {'/'.join(missing)}")
    if timeframe == "1day":
        index = index.normalize()
    cols = {str(c).lower(): c for c in df.columns}
    # A value that is not a finite number is a missing one, as an unreadable one is.
    df = pd.DataFrame({c: pd.to_numeric(df[cols[c]], errors="coerce").astype("float64")
                       for c in (*_OHLC, "volume") if c in cols}).replace([np.inf, -np.inf], np.nan)
    df.index = index.rename("time")
    return df[~df.index.duplicated(keep="last")].sort_index()


def bars(symbol, timeframe="1day", start=None, end=None) -> pd.DataFrame:
    """OHLCV bars, oldest first, indexed by exchange-local bar time (naive).

    Calls ``get_stock_data`` from the price_data market-data server, so the agent computes on the
    same bars the chart shows. ``df.attrs`` carries symbol, timeframe, tz and currency, so
    ``Chart(df, title=...)`` needs nothing else.
    """
    call = "chartkit.bars"
    if not isinstance(symbol, str) or not symbol.strip():
        _fail(call, "symbol", f"expected a ticker like 'NVDA' or '0700.HK', got {_short(symbol)}.")
    symbol = symbol.strip()
    tf = _timeframe(timeframe, call)
    daily = tf == "1day"
    begin = parse_time(start, call, "start") if start is not None else None
    stop = parse_time(end, call, "end") if end is not None else None
    if daily or (begin is not None and stop is not None):
        window = [None if t is None else fmt_time(t, daily, "text") for t in (begin, stop)]
    else:
        # Intraday asks for both dates: fill the missing one from today and how far back this interval reaches.
        stop = stop if stop is not None else pd.Timestamp.now().normalize() + pd.Timedelta(days=1)
        begin = begin if begin is not None else stop - pd.Timedelta(days=_INTRADAY_DAYS[tf])
        window = [t.strftime("%Y-%m-%d") for t in (begin, stop)]
    try:
        got = fetch_bars(symbol, tf, *window)
    except ToolsUnavailable as e:
        raise ChartError(
            f"chartkit.bars(): {e.reason}. Load bars yourself into a DataFrame with a DatetimeIndex and columns "
            "open, high, low, close, volume, and pass it to Chart."
        ) from None
    except BarsUnavailable as e:
        raise ChartError(f"chartkit.bars({symbol!r}, {tf!r}): {e.reason}") from None
    if got.frame.empty:
        asked = {"symbol": symbol, "interval": tf, **{k: v for k, v in zip(("start_date", "end_date"), window) if v}}
        raise ChartError(f"chartkit.bars({symbol!r}, {tf!r}): no bars returned for {asked}.")
    df = got.frame.copy()
    df.index = df.index.as_unit("ns")
    df.attrs.update({"symbol": got.symbol or symbol, "timeframe": tf})
    for key, value in (("tz", got.timezone), ("currency", got.currency)):
        if value:
            df.attrs[key] = value
    return df
