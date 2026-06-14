---
name: chart-annotation
description: Annotate a stock's price chart with price lines, trendlines, zones, and event markers. Drawings appear live on the MarketView chart and as a clickable preview card in any other chat.
---

# Chart Annotation Skill

## When to use

You want to call out a technical level, a pattern, or an event on a stock's
price chart. Drawing directly on the chart is almost always clearer than
describing it in prose. Reach for this skill whenever you would otherwise
say "look at the level around 205" or "notice the downtrend from October to
December".

You do **not** need the user to be on the MarketView page. If they are, the
drawing appears on their live chart immediately. If they are in any other
chat, the same drawing renders as a clickable preview card that expands into
MarketView — so annotate freely whenever it helps, then mention the user can
click it to open the full chart.

This skill provides two tools:

- `draw_chart_annotation` — add a single annotation to a chart.
- `manage_chart_annotations` — list, remove, or clear annotations.

## Charts are identified by `SYMBOL:timeframe`

Every annotation belongs to a chart identified by its **ticker + timeframe**
(e.g. `NVDA:1day`) — that pair *is* the chart's id:

- Pass the same `symbol` + `timeframe` again to **add to / edit that same
  chart** (annotations accumulate on it).
- Use a **different ticker or timeframe** to start a **separate** chart — so
  you can draw several charts in one turn (e.g. `AAPL:1day` and `AAPL:1hour`,
  or `AAPL:1day` and `MSFT:1day`), each rendered as its own preview.

Always pass the ticker the user is discussing. `timeframe` defaults to
`1day`; set it to match the interval the user is viewing (one of `1min`,
`5min`, `15min`, `30min`, `1hour`, `4hour`, `1day`). Annotations are scoped to
that one chart instance — a line drawn on `NVDA:1day` does **not** appear on
`NVDA:1hour`.

---

## Picking the right variant

`draw_chart_annotation` takes an `annotation` object discriminated by its
`type` field.

### `price_line` — horizontal level

Use for anything flat on the y-axis: support, resistance, a target, a
stop, an analyst price target, a 52-week high.

```json
{
  "type": "price_line",
  "price": 205.0,
  "label": "Resistance 205",
  "style": "dashed"
}
```

### `trendline` — two anchor points

Use to connect two `(time, price)` points on the chart: channel tops,
pattern boundaries, connecting highs/lows across dates.

`time` must be an ISO 8601 datetime aligned to a bar on the chart (daily
bars: midnight UTC of that day is safest).

```json
{
  "type": "trendline",
  "point1": {"time": "2024-10-16T00:00:00Z", "price": 145.2},
  "point2": {"time": "2024-12-20T00:00:00Z", "price": 138.7},
  "label": "Descending trend"
}
```

### `marker` — single-bar event

Use for a callout at one specific date: earnings beat, entry signal,
news event, grade change.

```json
{
  "type": "marker",
  "time": "2024-11-14T00:00:00Z",
  "shape": "arrowUp",
  "position": "belowBar",
  "text": "Earnings beat"
}
```

`shape` options: `arrowUp`, `arrowDown`, `circle`, `square`.
`position` options: `aboveBar`, `belowBar`, `inBar`.

### `vertical_line` — a moment in time

Use to mark a single date across the whole chart: an earnings date, a
split, an FOMC meeting, the start of a move.

```json
{
  "type": "vertical_line",
  "time": "2024-11-14T00:00:00Z",
  "label": "Earnings",
  "style": "dashed"
}
```

### `rectangle` — a zone

Use for supply/demand zones, consolidation ranges, or any box over a
region of the chart. `point1` and `point2` are two opposite corners (the
fill is translucent so candles stay visible).

```json
{
  "type": "rectangle",
  "point1": {"time": "2024-10-16T00:00:00Z", "price": 150.0},
  "point2": {"time": "2024-11-20T00:00:00Z", "price": 140.0},
  "label": "Demand zone"
}
```

### `text` — a free-floating label

Use for a callout that isn't tied to a marker or level. Anchored at a
`(time, price)` point.

```json
{
  "type": "text",
  "time": "2024-11-14T00:00:00Z",
  "price": 205.0,
  "text": "Breakout"
}
```

### `fib_retracement` — Fibonacci levels

Use to map retracement targets of a move. Pass the two ends of the swing
(e.g. swing low → swing high); standard levels (0, 0.236, 0.382, 0.5,
0.618, 0.786, 1.0) are drawn between them automatically.

```json
{
  "type": "fib_retracement",
  "point1": {"time": "2024-10-16T00:00:00Z", "price": 100.0},
  "point2": {"time": "2024-12-20T00:00:00Z", "price": 200.0},
  "label": "Oct–Dec move"
}
```

---

## Managing annotations

`manage_chart_annotations` covers list / remove / clear_all:

```python
# See what's there
manage_chart_annotations(symbol="NVDA", action="list")

# Remove specific ones (get ids from `list`)
manage_chart_annotations(symbol="NVDA", action="remove", ids=["ann_ab12..."])

# Wipe everything for the symbol
manage_chart_annotations(symbol="NVDA", action="clear_all")
```

- `remove` requires a non-empty `ids` list. The tool will reject an empty
  call.
- `clear_all` must not be given `ids`. Use `remove` for partial deletion.
- Existing chart primitives the user set up themselves (52W high,
  analyst target lines, earnings markers) are **not** managed by this
  skill and are never touched by clear_all.

---

## Tips

- **Short labels.** Chart space is tight — aim for a few words
  ("Resistance 205", "Entry", not "Strong resistance level we should
  watch"). Put the reasoning in the chat message, not the label.
- **One annotation per tool call.** If you want three levels, call
  `draw_chart_annotation` three times. Each call is cheap.
- **Bar alignment matters for trendline/marker.** If the time doesn't
  match a bar on the chart (e.g. you pass a minute-resolution time on a
  daily chart), the drawing still renders but may look offset.
- **Clean up stale work.** If you drew provisional levels and the
  conversation moved on, offer to `clear_all` before drawing a fresh set.
- **Provenance is visible.** Agent-drawn items render with a subtle
  dashed style so the user can tell them apart from their own drawings.
