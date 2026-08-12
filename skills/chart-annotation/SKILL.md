---
name: chart-annotation
description: Draw price lines, trendlines, zones, and event markers directly on a stock's price chart — reach for it whenever you'd otherwise describe a level, pattern, or event in prose. Renders live on MarketView and as a clickable preview card in any other chat.
---

# Chart Annotation Skill

## When to use

You want to call out a technical level, a pattern, or an event on a stock's
price chart. Drawing directly on the chart is almost always clearer than
describing it in prose. Reach for this skill whenever you would otherwise
say "look at the level around 205" or "notice the downtrend from October to
December".

**MarketView** is the app's live, TradingView-style price chart page (pan,
zoom, switch timeframes). You do **not** need the user to be on it to
annotate. If they are, the drawing appears on their live chart immediately. If
they are in any other chat, the same drawing renders as a clickable preview
card that expands into MarketView — so annotate freely whenever it helps, then
mention the user can click it to open the full chart.

This skill provides two tools:

- `draw_chart_annotation` — add a single annotation to a chart.
- `manage_chart_annotations` — list, remove, or clear annotations.

## Interactive chart vs. a Python chart (deliverable)

There are two ways to show price information visually — pick by what the user
needs:

- **This skill (interactive).** Annotations land on the live, pannable
  MarketView chart (or a preview card that opens it). Best when the user just
  wants to *see and explore* a level, pattern, or event themselves — quick,
  in-the-moment, nothing to hand off.
- **A Python chart (deliverable).** A static image you render with code and
  embed in a report or document. Best when the output is a *deliverable* the
  user keeps, shares, or exports — a research note, PDF, or deck.

The two aren't exclusive: draw on the live chart for a quick look, render a
Python chart when it belongs in a written artifact, or do both.

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

**Time format (any annotation with a `time` field).** Pass ISO 8601 datetimes
(e.g. `2024-11-14T00:00:00Z`) aligned to a bar on the chart — for daily bars,
midnight UTC of that day is safest. A time that doesn't land on a bar still
renders but may look offset. Applies to `trendline`, `marker`, `vertical_line`,
`text`, `event`, and `fib_retracement`.

## Reacting to a user's chart selection

A `<chart-selection>` block in the user's turn means they selected something on
the chart and sent it to you. Its `selection_type` is one of:

- `region` — a time×price box. Bounds come as a time range + price range, with
  the OHLCV `bars` inside it.
- `price_level` — a single horizontal price they tapped.

The user may send **several** blocks in one turn — treat each independently.
A block may carry a `User note:` line: that is the user's own comment about
*that* selection (separate from their message text) — let it steer what you
look for there.

Analyze each bounded area (lean on the supplied `bars` and/or your market-data
path), then, when it helps, draw your read back onto the **same** `symbol` +
`timeframe` with `draw_chart_annotation` — a `rectangle` over a `region`, or a
`price_line` at a `price_level`. Each block already spells out the matching
`draw_chart_annotation(...)` call; adjust it to the level or zone your analysis
actually lands on.

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

### `event` — news/event badge with detail

Use when a callout needs more than a one-line label: an earnings report, an
acquisition, an analyst upgrade, a product launch. Anchored at a `(time,
price)` point, it shows a short `title` badge on the chart; the `detail` (a
few sentences) is revealed on hover (desktop) or tap (mobile). Prefer this
over `marker`/`text` when you want to explain *why* the event matters.

```json
{
  "type": "event",
  "time": "2024-11-14T00:00:00Z",
  "price": 205.0,
  "title": "Q3 earnings beat",
  "detail": "Reported EPS of $1.40 vs $1.25 consensus and raised full-year guidance ~5%. Shares gapped up the next session on the print and the brighter outlook."
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
  `draw_chart_annotation` three times.
- **Clean up stale work.** If you drew provisional levels and the
  conversation moved on, offer to `clear_all` before drawing a fresh set.
- **No need to flag your drawings.** Agent-drawn items render with a subtle
  dashed style, so the user can already tell them apart from their own — you
  don't have to call out which annotations you added.
