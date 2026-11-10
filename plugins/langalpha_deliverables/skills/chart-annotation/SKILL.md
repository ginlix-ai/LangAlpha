---
name: chart-annotation
description: Drawing on the user's chart from Python with chartkit, as one chart layer per analysis (levels, lines, zones, indicators, channels, fibs, projections, trade setups). Also answering a chart selection the user sends.
---

# Drawing on the chart

A **layer** is one analysis drawn onto a stock's chart: its levels, lines, zones, markers, alerts and anything you compute. You build it with `chartkit` in the ExecuteCode run where you compute the analysis, so the numbers and the drawing come from the same DataFrame. Every element carries a **note**, your reason for it, which the user reads on hover and can send back to you as a question.

The layer lands on the user's live chart if they have it open. Anywhere else it shows as a chart card in the chat that opens the chart.

## Steps

1. **Build** the layer on `bars()`, the same bars the user's chart shows. In the same run, print the numbers you will quote and end with `c.snapshot()`:

   ```python
   from chartkit import Chart, bars

   df = bars("MSFT", "1day")
   window = df.iloc[-60:]                        # the stretch your read is about
   top, top_day = window.high.max(), window.high.idxmax()
   floor, floor_day = window.low.min(), window.low.idxmin()
   close = df.close.iloc[-1]
   vol_x = df.volume[floor_day] / df.volume.rolling(20).mean()[floor_day]

   c = Chart(df, title="MSFT 60-day range")
   c.level(top, "Range top", note=f"Sellers turned price back on {top_day:%b %d}; {top / close - 1:.1%} above the close.")
   c.level(floor, "Range floor",
           note=f"The {floor_day:%b %d} low came on {vol_x:.1f}x average volume; a close below it ends the range.")
   c.marker(floor_day, "Range low")
   print(f"close {close:.2f}, range {floor:.2f} to {top:.2f}, low volume {vol_x:.1f}x")
   c.snapshot()
   ```

   What to take from it is the method: every price and date comes from data, each note gives the evidence behind its element, and one note names the price where the read fails. Your own read decides which elements it needs.

   Done when the run prints the snapshot report.

2. **Look**: read the PNG at the path the report prints. Fix what the report names or the picture clearly shows (overlap, an element out of view, text cut short) and snapshot again; a reworded label waits for the publish run. Done when neither shows anything left to fix. A computer that cannot take snapshots says so; go straight to publishing.

   The snapshot frames the layer as the user's chart will when it lands. `c.snapshot(view=...)` frames the picture another way and changes nothing on the user's chart: `"user"` for the window on their screen now, a range button (`1D 5D 1M 3M 6M YTD 1Y 5Y All`), or a `(start, end)` pair. A range ends on the last bar, so a pair is the one that keeps future times in view.

3. **Publish**: run the same code with `c.publish(summary=...)` in place of `c.snapshot()`. Each ExecuteCode run starts fresh, so the whole script runs again. The `summary` (up to 1200 characters) is your thesis, shown with the layer; write it from the same variables, after your last change. Done when the tool result carries the **read-back**: the layer's elements and their coordinates as the server stored them. A run handed off to the background publishes when it ends, and its notice carries the read-back.

4. **Reply** with the numbers from your variables or a helper's `info`, the same ones on the chart.

## The chart

A chart is a **ticker and timeframe**, `SYMBOL:timeframe` (`NVDA:1day`). A layer on `NVDA:1day` is not on `NVDA:1hour`. Use the ticker and timeframe the user is viewing; timeframes are `1min 5min 15min 30min 1hour 4hour 1day`. For several charts in one turn, build one `Chart` per ticker and timeframe and publish each.

`bars(symbol, timeframe="1day", start=None, end=None)` returns OHLCV oldest first, indexed by exchange-local bar time. `Chart(df, title=...)` takes symbol, timeframe and zone from it.

**One layer per idea.** Its `id` defaults to the title as a slug. Publishing the same id again replaces that layer, which is how you change one; a new title adds another layer beside it. A layer is fixed when published: it does not follow new bars.

**Times.** A point is `(time, price)`; `time` is anything pandas reads: `df.index[i]`, `"2026-03-04"`, `"2026-03-04 10:30"`. A time with no zone is exchange-local. Times past the last bar are allowed everywhere and land on their trading session; `c.future(n)` gives the next `n` bar times.

## Elements

Each method takes the arguments its row shows, plus `note=` (up to 600 characters). Colors are hex or `rgb()`/`rgba()`. Where a row has them, `style` is `solid`, `dashed` or `dotted`, and `extend` is `none`, `right`, `left` or `both`.

| Method | Draws |
|---|---|
| `c.level(price, title=None, pane="price", color=None, style="dashed")` | Horizontal level. |
| `c.alert(price, title, direction="either")` | A bell on a price, for a cross `above`, `below` or `either`. The bell itself notifies no one. |
| `c.marker(time, text=None, position="belowBar", shape=None, color=None, price=None)` | One marker; `position` `aboveBar`, `belowBar`, `inBar`, or `atPrice` with `price`. |
| `c.markers(mask, text=None, position="belowBar", ...)` | A marker on every bar where the boolean `mask` is True; returns the count. |
| `c.vline(time, style="dashed", label=None)` | Vertical line: an earnings date, a split, the start of a move. |
| `c.segment(p1, p2, extend="none", style="solid", width=2, label=None)` | Trend line, or a ray with `extend`. |
| `c.box(p1, p2, fill=None, label=None)` | Zone between opposite corners. |
| `c.label(at, text, position="above")` | Text chip at a point; `position` also `below`, `left`, `right`. |
| `c.fib(p1, p2, levels=None, extend="right")` | Fib levels, ratio 1 at p1 (where the move started) and 0 at p2. |
| `c.arrow(p1, p2)` | An arrow between two points. |
| `c.forecast(target, by, *, probability=None)` | A target price by a date (or a number of bars), arrowed from the last close. Once the date passes, the chart marks it hit or missed from the real bars. |
| `c.regime(when, color=None, label=None)` | Shaded time: a `(start, end)` pair or a boolean mask. |

Drawings (`vline` through `forecast`) also take `color=`, `id=` and return their id.

For anything computed from the bars, read `.agents/skills/chart-annotation/references/chartkit.md` first: the `ta` and `analysis` signatures, indicators and their panes, swing pivots and key levels, regression and pivot channels, fibs from a swing, patterns, volatility cones, forecasts, replays of a past stretch, measured moves, trade positions with R multiples, volume profiles, snapshot framing, worked examples and limits.

## A good layer

- A layer draws what the user asked for. "Mark 200 as support" is one level with a short note; analysis the user didn't ask for stays out.
- It answers one question, and the summary states the answer and the price where it would be wrong.
- Every number on it is a variable: computed in the script from data fetched this turn, or fetched there through a tool's import, and written into notes, labels and the summary with f-strings. A value only a tool call outside the script returned is assigned once, with a comment naming that call.
- Each note gives the evidence: the date, the volume, the distance from price.
- The chart already draws volume, simple MA 20 and 50, and RSI 14 by default; the layer adds what the chart lacks.
- A title is a few words giving the reason ("Range top"); the price is already on the axis.
- A bell tells the user they will hear when price gets there, so it goes only on a price you have set a price wakeup for (`schedule_wakeup`); a price that is only worth watching is a level. The bell draws its own line, so that price needs no level as well.
- A forecast's target comes from something you computed (a level, a fib extension, a measured move), and the chart will grade it.
- Keep to about 8 labels per layer, and extend a line only when the projection is the point.

## Managing layers

`manage_chart_layers` lists the layers on a chart with their ids, and deletes them by id or all at once. The chart's own lines (52-week high, analyst targets, earnings markers) are the app's and stay.

## A chart selection from the user

A `<chart-selection>` block means the user selected part of the chart and sent it. Its `selection_type` is `region` (a time and price box, with the OHLCV bars inside it) or `price_level` (one price they tapped). Treat each block on its own; a `User note:` line is their comment on that selection. Analyse it, then draw your read back on the same chart: `c.box((time_start, price_high), (time_end, price_low))` over a region, `c.level(price)` at a level, adjusted to where your analysis lands.

## A chart for a report

A layer is for the live chart. A chart that goes into a report, PDF or deck is an image you render in code. When the user asks to see the live chart, link the snapshot PNG: it is their chart.
