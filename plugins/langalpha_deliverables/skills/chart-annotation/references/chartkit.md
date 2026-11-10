# chartkit: computed analysis

The steps, the chart, times and the simple elements are in `SKILL.md`. This file adds what you compute from the bars: indicator panes, the analysis drawings, the helpers that find them, worked examples and limits.

## Series and panes

`pane` defaults to `"price"`.

| Method | Draws |
|---|---|
| `c.pane(id, title=None, height=0.25)` | A lower pane, named by `id` (`"rsi"`). Up to 4, heights total at most 0.75. |
| `c.plot(series, title, pane="price", type="line", color=None, width=2, style="solid")` → id | `type` `line`, `area` or `histogram` (green/red by sign by default). `color` may be a Series or array of per-bar colors. |
| `c.fill(a, b, color=None)` | Shade between two plotted ids in one pane. |
| `c.level(price, title=None, pane="price", ...)` | A level in any pane, e.g. RSI 70 and 30. |

**Values.** A Series is matched to df's bars by time, so indicators computed on longer history plot fine; NaN warmup is dropped, and values after the last bar are kept as a projection. A numpy array or list needs one value per bar.

## Analysis drawings

Each also takes `color=`, `label=`, `note=`, `id=` (keyword only) and returns the drawing's id. `extend` is `none`, `right`, `left` or `both`; `style` is `solid`, `dashed` or `dotted`.

| Method | Draws |
|---|---|
| `c.channel(p1, p2, p3, extend="right", fill=None)` | Line p1-p2 and its parallel through p3, shaded. |
| `c.pitchfork(p1, p2, p3, extend="right")` | Andrews pitchfork from pivot p1 through the midpoint of p2-p3. |
| `c.path(points, point_labels=None, style="solid", width=2, closed=False)` | Polyline. `point_labels`, one per point (`None` to skip), name a pattern: `[None, "LS", None, "H", None, "RS", None]`, `["1", "2", "3", "4", "5"]`, `["A", "B", "C", "D"]`. |
| `c.cone(origin, bands)` | Forecast bands `[{"label", "upper": [pts], "lower": [pts], "fill"?}]`, widest first, up to 4. |
| `c.forecast(target, by, *, origin=None, probability=None)` | An arrow from `origin` (default the last close) to the `target` price at `by` (a time, or a number of bars after the origin), with the move in a box. Once `by` passes, the chart marks it hit or missed from the real bars. Draw one per outcome; pass `probability` only when you computed one. |
| `c.analog(start, end, *, origin=None)` | The bars from `start` to `end` replayed from `origin` (default the last close) as ghost candles, each keeping its percent move from the close before `start`; the source stretch is shaded. Say in `note=` why that stretch compares. |
| `c.position(side, entry, stop, targets, until=None)` | Long/short setup: `entry` a point, `stop` and `targets` prices; risk/reward shaded with R multiples. |
| `c.measure(p1, p2)` | Ruler: price change, percent, bars, days. |
| `c.profile(start, end, rows, poc=None, value_area=None)` | Fixed-range volume profile; feed it `an.volume_profile`. |

## Analysis helpers (`chartkit.analysis`)

They return plain values. A helper that builds one drawing returns that method's arguments plus `info`, numbers worth quoting: `c.channel(**ch)` draws it, `ch["info"]` explains it. Points are `Point(time, price)`.

| Helper | Returns |
|---|---|
| `swings(df, left=5, right=5)` | `(highs, lows)`: confirmed pivot Points. The last `right` bars cannot be pivots yet. |
| `zigzag(df, pct=5)` | Alternating swing Points, each leg at least `pct`%; the last is the unfinished leg. Use for pattern paths. |
| `key_levels(df, left=5, right=5, tolerance=0.01, min_touches=2, max_levels=6)` | `[{"price", "touches", "last"}]` where swing pivots cluster. |
| `regression_channel(df, n=100, k=2.0)` | For `c.channel`: least-squares fit of the last `n` closes, `k` residual sigmas wide. `info`: slope_per_bar, slope_pct_per_bar, r2, upper, mid, lower. |
| `trend_channel(df, left=5, right=5, direction="auto")` | For `c.channel`: line on the last two swing lows (rising) or highs (falling), or the other pair when that one moves against the trend; parallel through the farthest bar since. `info`: direction, anchor, slope_per_bar, upper, lower. |
| `fib_from_swing(df, lookback=120)` | For `c.fib`: the window's high and low, the later one as p2. `info`: direction, levels `{ratio: price}`, retraced. |
| `fib_levels(p1, p2, levels=None)` | `{ratio: price}` exactly as `c.fib` draws them. |
| `volume_profile(df, start=None, end=None, bins=40, value_area=0.70)` | For `c.profile`: rows, POC, value area. `info`: poc, va_low, va_high, total_volume. |
| `vol_cone(df, horizon=20, window=20, sigmas=(1, 2))` | For `c.cone`: lognormal bands from realized volatility of the last `window` returns. `info`: vol_per_bar, vol_annual, horizon, end `{band label: (low, high)}` at the horizon. |
| `measured_move(df, a, b, c)` | The a-b move repeated from c over as many bars: `c.forecast(mm["p2"].price, mm["p2"].time, origin=mm["p1"])`. `info`: target, move, pct. |
| `future_times(df, n)` | Next `n` bar times: weekdays for daily bars (holidays unknown), the df's own session times for intraday. |

## Indicators (`chartkit.ta`, TradingView Pine semantics)

Series in, Series out, NaN until warmed up: `sma ema rma wma stdev highest lowest (src, n)`, `change(src, n=1)`, `roc(src, n)`, `rsi(src, n)`, `zscore(src, n)`, `tr(df)`, `atr(df, n)`, `bbands(src, n, mult)` → `(mid, upper, lower)`, `macd(src, 12, 26, 9)` → `(macd, signal, hist)`, `stoch(df, n=14, smooth_k=1, smooth_d=3)` → `(k, d)`, `donchian(df, n)` → `(upper, lower, mid)`, `supertrend(df, period=10, mult=3)` → `(line, direction)` (1 up, -1 down), `linreg(src, n)` → `(value, slope)`, `crossover(a, b)`, `crossunder(a, b)` → boolean Series (`b` may be a number), `pivothigh(src, left, right)`, `pivotlow(...)` → the pivot value at its bar, else NaN, `vwap(df, anchor=None)` → session VWAP, or anchored from `anchor`.

## Snapshots

`c.snapshot(path=None, *, theme=None, width=None, height=None, view=None)`; `view` is in `SKILL.md`. `snapshot("NVDA", "1day")` from `chartkit` shows the plain chart with no layer, for looking at a setup before deciding what to draw. When the report says your DataFrame differs from the chart's bars, compute on `bars()`.

## Examples

**Indicators in their own panes.**

```python
from chartkit import Chart, bars, ta

df = bars("NVDA", start="2025-06-01")
c = Chart(df, title="NVDA momentum")
mid, up, lo = ta.bbands(df.close, 20, 2)
c.fill(c.plot(up, "BB upper", color="#90a4ae", width=1), c.plot(lo, "BB lower", color="#90a4ae", width=1))
ema = ta.ema(df.close, 50)
held = int((df.close.iloc[-60:] > ema.iloc[-60:]).sum())
c.plot(ema, "EMA 50", note=f"Price closed above it on {held} of the last 60 sessions.")
rsi = ta.rsi(df.close, 14)
c.pane("rsi", "RSI 14", height=0.2)
c.plot(rsi, "RSI 14", pane="rsi", color="#7e57c2")
c.level(70, pane="rsi", style="dotted")
c.level(30, pane="rsi", style="dotted")
macd, signal, hist = ta.macd(df.close)
c.pane("macd", "MACD 12/26/9", height=0.2)
c.plot(hist, "Histogram", pane="macd", type="histogram")
c.markers(ta.crossover(rsi, 30), "RSI 30", note="Oversold bounce: RSI back above 30.")
c.publish(summary=f"RSI {rsi.iloc[-1]:.0f} with the MACD histogram {'rising' if hist.iloc[-1] > hist.iloc[-2] else 'falling'}; "
                  f"the trend read fails on a close below the EMA 50 at {ema.iloc[-1]:.2f}.")
```

**The nearest levels as forecasts into earnings.**

```python
from chartkit import Chart, bars, analysis as an

df = bars("AAPL", "1day")
earnings = "2027-01-29"                       # from this turn's earnings-calendar call
close = df.close.iloc[-1]
levels = an.key_levels(df.iloc[-250:])        # where a year of swing pivots cluster
up = min((lv for lv in levels if lv["price"] > close), key=lambda lv: lv["price"], default=None)
down = max((lv for lv in levels if lv["price"] < close), key=lambda lv: lv["price"], default=None)

c = Chart(df, title="AAPL levels into earnings")
for lv, name in ((up, "Next resistance"), (down, "Next support")):
    if lv:
        c.forecast(lv["price"], earnings, label=name,
                   note=f"{lv['touches']} swing pivots near {lv['price']:.2f}, last on {lv['last']:%b %d}; "
                        f"{lv['price'] / close - 1:+.1%} from the close.")
c.vline(earnings, label="Earnings", note="Both forecasts are judged at the report.")
c.publish(summary=f"Between {down['price']:.0f} and {up['price']:.0f} into the report; a close past either decides it."
          if up and down else "No pivot cluster on one side of the close.")
```

**What followed the last time.**

```python
from chartkit import Chart, bars

df = bars("AAPL", "1day")
low60 = df.close <= df.close.rolling(60).min()
prior = low60[:-30]                           # lows with 30 bars after them
start = prior[prior].index[-1]
i = df.index.get_loc(start)
after = df.close.iloc[i + 30] / df.close.iloc[i] - 1
c = Chart(df, title="AAPL after a 60-day low")
c.analog(df.index[i + 1], df.index[i + 30], label=f"After the {start:%b %d} low",
         note=f"The 30 bars after the 60-day closing low of {start:%b %d, %Y}: {after:+.1%}.")
c.publish(summary=f"The {start:%b %d} 60-day low was followed by {after:+.1%} over 30 bars; one case, not a forecast.")
```

**A long setup.**

```python
from chartkit import Chart, bars, ta, analysis as an

df = bars("TSLA", start="2025-10-01")
c = Chart(df, title="TSLA base breakout")
atr = float(ta.atr(df, 14).iloc[-1])
base = df.iloc[-25:-1]
top, bottom = float(base.high.max()), float(base.low.min())
vol_chg = base.volume.iloc[-5:].mean() / base.volume.iloc[:5].mean() - 1
c.box((base.index[0], top), (base.index[-1], bottom), label="Base",
      note=f"{len(base)} bars between {bottom:.2f} and {top:.2f}; volume {vol_chg:+.0%} from its first week to its last.")
entry = round(top * 1.005, 2)
stop = round(entry - 1.5 * atr, 2)
risk = entry - stop
c.position("long", (df.index[-1], entry), stop, [entry + 2 * risk, entry + 3 * risk], until=c.future(20)[-1],
           note=f"Buy-stop over the base; stop 1.5 ATR ({atr:.2f}) below; targets at 2R and 3R.")
for lv in an.key_levels(df, max_levels=3):
    c.level(lv["price"], f"{lv['touches']} touches", note=f"{lv['touches']} swing pivots cluster here.")
c.publish(summary=f"Long above {entry}, stop {stop}, risk {risk:.2f} a share.")
```

**A volume profile with an anchored VWAP.**

```python
from chartkit import Chart, bars, ta, analysis as an

df = bars("SPY", start="2025-04-01")
c = Chart(df, title="SPY value area")
vp = an.volume_profile(df, start=df.index[-126])
c.profile(**vp, note=f"POC {vp['poc']:.2f}; 70% of volume traded {vp['value_area'][0]:.2f}-{vp['value_area'][1]:.2f}.")
low = df.low.iloc[-126:].idxmin()
avwap = ta.vwap(df, low)
c.plot(avwap, "AVWAP from low", color="#26a69a", note=f"Average cost of everyone who bought since the {low.date()} low.")
c.marker(low, "AVWAP anchor")
close, (va_low, va_high) = df.close.iloc[-1], vp["value_area"]
c.publish(summary=f"The close {close:.2f} is {'inside' if va_low <= close <= va_high else 'outside'} the value area "
                  f"{va_low:.2f}-{va_high:.2f}, POC {vp['poc']:.2f}; a close below the anchored VWAP at "
                  f"{avwap.iloc[-1]:.2f} puts the average buyer since the low under water.")
```

## Limits

Per layer: 24 series of up to 20 000 points, 4 extra panes, 300 drawings, 3 000 markers, 2 000 regimes, 1.5 MB in all. Per drawing: 500 path points, 200 profile rows, 500 replayed bars, 4 cone bands. Titles and labels up to 120 characters, `c.label` text up to 200, notes up to 600. A mistake raises `ChartError` naming the method and argument at the line that made it, or at `c.snapshot()`/`c.publish()` for a rule across elements (the lower panes' total height, a fill's two series in one pane); fix that call and run again.
