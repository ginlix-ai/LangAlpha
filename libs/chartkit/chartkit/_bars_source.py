"""The snapshot chart's bars: its bar requests answered as the app's backend would, from the market-data tool."""

from __future__ import annotations

import math
import threading
import time

import pandas as pd
from market_protocol.intervals import schema_seconds, to_schema

from .data import BarsUnavailable, fetch_bars

FETCH_TIMEOUT_S = 30.0
#: The backend's daily window when the chart names none (ginlix-data's ``_DAILY_LOOKBACK_DAYS``).
_DAILY_DEFAULT_DAYS = 730


def bar_rows(df, idx: pd.DatetimeIndex, tz: str) -> list[dict]:
    """Bars as the app's bars routes send them: epoch ms of the exchange-local bar time."""
    cols = {str(c).lower(): c for c in df.columns}
    utc = idx.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward").tz_convert("UTC")
    valid = ~utc.isna()
    ms = utc.as_unit("ms").asi8
    ohlc = [pd.to_numeric(df[cols[c]], errors="coerce").to_numpy(dtype="float64")
            for c in ("open", "high", "low", "close")]
    vol = (pd.to_numeric(df[cols["volume"]], errors="coerce").to_numpy(dtype="float64")
           if "volume" in cols else None)
    rows = {}
    for i in range(len(idx)):
        if not valid[i] or any(not math.isfinite(a[i]) for a in ohlc):
            continue
        v = vol[i] if vol is not None and math.isfinite(vol[i]) else 0.0
        rows[int(ms[i])] = {"time": int(ms[i]), "open": float(ohlc[0][i]), "high": float(ohlc[1][i]),
                            "low": float(ohlc[2][i]), "close": float(ohlc[3][i]), "volume": float(v),
                            "_day": idx[i].strftime("%Y-%m-%d")}
    return [rows[k] for k in sorted(rows)]


def _in_thread(fn, timeout: float):
    """Run ``fn`` off the renderer's thread (whatever the tool's client does with event loops stays
    out of Playwright's), giving up after ``timeout``. Daemon, so a hung call never holds the job."""
    box: dict = {}

    def run():
        try:
            box["value"] = fn()
        except BaseException as e:  # noqa: BLE001 - handed to the caller
            box["error"] = e

    t = threading.Thread(target=run, name="chartkit-bars", daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError
    if "error" in box:
        raise box["error"]
    return box["value"]


class ChartBars:
    """The chart's bar requests, answered as the app's backend would: from the market-data tool.

    The first request decides the source. When the tool cannot answer it, the agent's DataFrame
    draws the candles instead (every bar up to the request's end, whatever its start asks, so an
    earlier page finds nothing new), and the report says so. A later request the tool misses gets
    no bars, and the report names its window. ``tool`` stands in for the generated wrapper.
    """

    def __init__(self, symbol, tf, tz, df_rows, df_gap=None, tool=None):
        self.symbol, self.tf, self.tz = symbol, tf, tz
        self.df_rows, self.df_gap = df_rows, df_gap
        self._tool_fn = tool
        self.mode: str | None = None  # "tool" | "df"
        self.failure: str | None = None
        self.source: str | None = None
        self.currency: str | None = None
        self.misses: list[str] = []
        self.served: dict[int, dict] = {}
        self._cache: dict = {}

    def answer(self, start: str | None, end: str | None) -> list[dict]:
        if self.mode != "df":
            got = self._tool(start, end)
            if isinstance(got, list) and (got or self.mode == "tool"):
                self.mode = "tool"
                return self._serve(got)
            why = got if isinstance(got, str) else f"it returned no bars for {_window(start, end)}"
            if self.mode == "tool":
                self.misses.append(f"{_window(start, end)} ({why})")
                return []
            self.failure, self.mode = why, "df"
        day = (end or "")[:10]
        return self._serve([r for r in self.df_rows or [] if not day or r["_day"] <= day])

    def _serve(self, rows: list[dict]) -> list[dict]:
        for r in rows:
            self.served[r["time"]] = r
        return [{k: v for k, v in r.items() if k != "_day"} for r in rows]

    def _tool(self, start, end):
        key = (start, end)
        if key not in self._cache:
            self._cache[key] = self._call(start, end)
        return self._cache[key]

    def _call(self, start, end) -> list[dict] | str:
        """The tool's bars as route rows, or why there are none."""
        if self.tf == "1day" and not start:
            # The chart's daily load names no window; the backend then serves its own default, which
            # the tool would not (undated, it skips ginlix-data for FMP's shorter window).
            end = end or pd.Timestamp.now().strftime("%Y-%m-%d")
            start = (pd.Timestamp(end) - pd.Timedelta(days=_DAILY_DEFAULT_DAYS)).strftime("%Y-%m-%d")
        try:
            got = _in_thread(lambda: fetch_bars(self.symbol, self.tf, start, end, tool=self._tool_fn),
                             FETCH_TIMEOUT_S)
        except TimeoutError:
            return f"no answer in {FETCH_TIMEOUT_S:.0f}s"
        except BarsUnavailable as e:
            return e.reason
        if got.frame.empty:
            return []
        self.source = got.source or self.source
        self.currency = got.currency or self.currency
        rows = bar_rows(got.frame, got.frame.index, got.timezone or self.tz)
        for r in rows:
            # Whole shares, as the backend's data sources hand the chart its volume (``int()``).
            r["volume"] = float(int(r["volume"]))
        return rows

    def phase(self) -> str | None:
        """The venue's phase as the backend would send it, as far as the bars tell it.

        ``closed`` once the chart's last intraday bar is well behind the clock (a session over, a
        holiday), so a settled tape reads as settled ("AH Close", not a live "After"); otherwise
        None, which leaves the chart to its own date rules, as before the backend sent phases.
        """
        if self.mode != "tool" or not self.served or self.tf == "1day":
            return None
        step = schema_seconds(to_schema(self.tf))
        behind = time.time() - max(self.served) / 1000
        return "closed" if behind > max(30 * 60, 2 * step + 30 * 60) else None

    def times(self) -> pd.Series:
        """The bars the chart holds, closes indexed by exchange-local bar time, oldest first."""
        if not self.served:
            return pd.Series(dtype="float64", index=pd.DatetimeIndex([]))
        ms = sorted(self.served)
        idx = pd.to_datetime(ms, unit="ms", utc=True).tz_convert(self.tz).tz_localize(None)
        if self.tf == "1day":
            idx = idx.normalize()
        return pd.Series([self.served[m]["close"] for m in ms], index=idx)


def _window(start, end) -> str:
    return f"{start or '...'} to {end or 'today'}"
