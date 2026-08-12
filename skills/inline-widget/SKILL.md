---
name: inline-widget
description: "Inline HTML widgets: charts, dashboards, data tables rendered directly in the chat via ShowWidget"
---

# Inline Widget

Render interactive HTML/SVG widgets directly inside the chat conversation using `ShowWidget`. Widgets appear inline between text — no sandbox, no preview URL, no side panel. They run JavaScript, so lean into making them **interactive and explorable** where it helps — something the user can sort, filter, toggle, and hover over, not just a static picture.

## When to Use

- User wants a **quick visualization** embedded in the conversation (chart, metric card, data table)
- The visualization is **self-contained** — all data is embedded in the HTML, no server needed
- User wants **interactivity** within the chat: buttons, toggles, hover effects, animated charts
- The output is a **single view** — not a multi-page app or dashboard that needs routing

**Use `interactive-dashboard` instead if:** User needs a multi-page web app, server-side data, live data refresh, or complex interactivity requiring React/FastAPI.

Read `.agents/skills/ui-design/SKILL.md` for design quality — its color discipline, chart restraint, and anti-slop principles apply here too. Its font pairings and type scale, though, are for full documents; a widget sits on the chat surface, so use the host-font typography rules below instead.

## ShowWidget API

```
ShowWidget(html: str, title: str | None = None, data_files: list[str] | None = None)
```

- `html`: Raw HTML fragment — no `<!DOCTYPE>`, `<html>`, `<head>`, or `<body>` tags
- `title`: Optional metadata (not displayed to user)
- `data_files`: Optional list of sandbox file paths to make available as `window.__WIDGET_DATA__`

The HTML is rendered in a sandboxed iframe with:
- **CDN libraries**: `cdnjs.cloudflare.com`, `cdn.jsdelivr.net`, `unpkg.com`, `esm.sh`
- **CSS theme variables**: automatically injected (see Theme section)
- **`sendPrompt('text')`**: global function to trigger follow-up chat messages
- **`window.__WIDGET_DATA__`**: dict of filename→content for files passed via `data_files`
- **No network to non-CDN origins**: `fetch()` / `XMLHttpRequest` to arbitrary URLs are blocked by CSP — only CDN domains (cdnjs, jsdelivr, unpkg, esm.sh) are allowed. Use `data_files` for sandbox files, or embed small data directly in HTML

## Layout Rules (CRITICAL)

The widget sits directly on the chat surface inside a transparent iframe. Follow these rules for seamless integration:

### Outer Element — Transparent Shell

The **outermost HTML element** must have:
- **NO background** (or `background: transparent`)
- **NO border**
- **NO border-radius**
- **NO box-shadow**
- **NO padding** — add padding on inner sections only

```html
<!-- CORRECT: transparent outer shell -->
<div>
  <div style="background: var(--color-bg-card, #ffffff); border-radius: 8px; padding: 16px; ...">
    ...inner card content...
  </div>
</div>

<!-- WRONG: styled outer wrapper — will be rejected -->
<div style="background: var(--color-bg-page); border: 1px solid ...; border-radius: 8px; padding: 20px;">
  ...content...
</div>
```

### Inner Elements — Use Theme Variables

Inner cards, sections, and components should use CSS variables for styling, always in the fallback form:

```css
/* Card */
background: var(--color-bg-card, #ffffff);
border: 0.5px solid var(--color-border-muted, #e4e1dc);
border-radius: 8px;
padding: 16px;

/* Metric card */
background: var(--color-bg-subtle, #f4f2ee);
border: 0.5px solid var(--color-border-muted, #e4e1dc);
border-radius: 8px;
```

In chat the variables are always injected, so the fallback never shows there — it is what renders when the widget is saved as a workspace `.html` file and previewed before theme injection or opened standalone. Use the light literals from the html-report skill's table.

### Positioning

- **NO `position: fixed`** — breaks iframe auto-sizing (elements collapse to 0 height)
- Use `position: relative` for chart containers
- No nested scrolling — the iframe auto-sizes to fit all content

## Theme Variables

These CSS variables are automatically injected and resolve correctly in both light and dark mode:

| Variable | Purpose |
|----------|---------|
| `--color-bg-page` | Page background |
| `--color-bg-card` | Card/panel background |
| `--color-bg-elevated` | Elevated surface |
| `--color-bg-subtle` | Subtle/muted background |
| `--color-bg-hover` | Hover state background |
| `--color-text-primary` | Primary text |
| `--color-text-secondary` | Secondary/muted text |
| `--color-text-tertiary` | Hint/label text |
| `--color-border-muted` | Default border (use with 0.5px) |
| `--color-accent-primary` | Brand/accent color |
| `--color-profit` | Positive/gain (green) |
| `--color-loss` | Negative/loss (red) |
| `--color-warning` | Warning (amber) |
| `--color-info` | Info (blue) |
| `--color-success` | Success (green) |

**Never hardcode colors** like `#333` or `rgb(...)` for text, backgrounds, or borders — they break in dark mode. Author every color as `var(--color-x, #lightLiteral)`: the injected variable always wins in chat, and the fallback keeps the widget legible on its secondary surfaces. The only exception is chart canvas colors — Chart.js canvas cannot read CSS variables; resolve them via `getComputedStyle` with a literal fallback.

## Charts (Chart.js)

Load Chart.js from CDN and follow these rules:

```html
<!-- Wrapper div with explicit height — REQUIRED -->
<div style="position: relative; height: 200px;">
  <canvas id="myChart"></canvas>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
  // Read CSS variables for chart colors (canvas can't use var());
  // the literal fallback covers the widget opened outside the chat surface.
  var cs = getComputedStyle(document.documentElement);
  function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }
  var accent = pick('--color-accent-primary', '#1f5fb4');
  var border = pick('--color-border-muted', '#e4e1dc');

  new Chart(document.getElementById('myChart'), {
    type: 'line',
    data: {
      labels: [...],
      datasets: [{
        data: [...],
        borderColor: accent,
        backgroundColor: accent + '20',
        tension: 0.4,
        fill: true
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: true } },
      scales: {
        y: { grid: { color: border } },
        x: { grid: { display: false } }
      }
    }
  });
</script>
```

**Key rules:**
- Set height on the **wrapper div**, never on the canvas
- Always use `responsive: true, maintainAspectRatio: false`
- Use UMD build from CDN (sets `window.Chart` global)
- Read CSS variables via `getComputedStyle` with a literal fallback (`pick()`) for chart colors

## Typography

- Font: inherited from host (`-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`)
- Weight: favor **400** (regular) and **500** (medium); use 600 sparingly for a key figure or heading. Avoid 700 — it reads heavy on the chat surface
- Heading sizes: h1 = 22px, h2 = 18px, h3 = 16px (all weight 500)
- Body: 14-16px, weight 400
- Use **sentence case** — no Title Case or ALL CAPS (except short metric labels)

## Interactivity

### Make it explorable

A widget runs JavaScript, so prefer something the user can **poke at**, not just read. When the data supports it, reach for:

- **Sortable / filterable tables** — click a header to sort by P&L or weight; filter to a sector or watchlist.
- **Series & metric toggles** — show/hide chart series, switch price ↔ % change, flip timeframe (1M / 6M / 1Y).
- **Hover detail** — tooltips on chart points and table rows that surface the underlying numbers.
- **What-if inputs** — a slider or field that recomputes a figure live (drag a growth rate, watch the projection update).
- **Tabs / segmented views** — split a dense widget (Overview / Holdings / Performance) so the reader drills in.

It all runs **client-side over the embedded data** — never `fetch()` a non-CDN origin (CSP blocks it; use `data_files` for anything large). Keep a meaningful default state so the widget reads correctly before any interaction. Use `sendPrompt()` only when the next step genuinely belongs back in the chat (a new query, a deeper analysis) — handle exploration the widget can do itself in-place.

### sendPrompt()

Call `sendPrompt('text')` from buttons to trigger a follow-up chat message:

```html
<button onclick="sendPrompt('Show detailed revenue breakdown')"
        style="padding: 8px 16px; background: var(--color-accent-primary); color: white;
               border: none; border-radius: 6px; cursor: pointer;">
  Revenue Details ↗
</button>
```

Add a **↗ arrow** on buttons that call `sendPrompt()` to signal they trigger a chat action.

### Refresh / Animation

`setInterval` and `requestAnimationFrame` work normally for animations and live tickers:

```javascript
setInterval(function() {
  // Update prices, rotate data, animate
  updateDisplay();
}, 3000);
```

## File Data

Use `data_files` to load data from sandbox files instead of inlining everything in the HTML string. This is especially useful for larger datasets.

### Workflow

1. Generate data files via Python
2. Pass file paths to `ShowWidget` via `data_files`
3. Access data in the widget via `window.__WIDGET_DATA__["filename"]`

```python
# Step 1: Generate data
import json
data = {"labels": ["Q1", "Q2", "Q3"], "values": [100, 150, 200]}
with open("work/<task_name>/chart_data.json", "w") as f:
    json.dump(data, f)

# Step 2: Agent calls ShowWidget with data_files
ShowWidget(
    html='<div id="chart">...</div><script>var d = JSON.parse(__WIDGET_DATA__["chart_data.json"]); ...</script>',
    data_files=["work/<task_name>/chart_data.json"]
)
```

### Widget access

```javascript
// Text files (json, csv, txt, etc.) — returned as strings
var data = JSON.parse(window.__WIDGET_DATA__["chart_data.json"]);
var csvText = window.__WIDGET_DATA__["results.csv"];

// Binary files (png, jpg, etc.) — returned as data URLs
document.getElementById("img").src = window.__WIDGET_DATA__["chart.png"];
```

### Supported file types

- **Text** (returned as strings): `.json`, `.csv`, `.txt`, `.html`, `.xml`, `.svg`, `.md`, `.yaml`, `.yml`, `.tsv`, `.geojson`, `.topojson`
- **Binary** (returned as data URLs): `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`, `.ico`

### Size limits

Total inline data is capped at 500KB across all files. Keep datasets concise — aggregate or sample large files before passing them.

## Blocked Patterns

The following will cause `ShowWidget` to **reject** your HTML with an error. Fix and retry:

| Pattern | Why blocked |
|---------|-------------|
| `new ResizeObserver(...)` | Host handles iframe sizing — your observer creates infinite resize loops |
| `parent.postMessage(...)` | Use `sendPrompt()` instead — direct postMessage bypasses the bridge |
| `window.top.*` / `window.parent.*` | Sandboxed iframe — parent access is blocked |
| `position: fixed` | Breaks iframe auto-sizing |
| Background/border on outermost element | Breaks seamless integration with chat surface |

## Design Patterns

### Metric Cards Row

```html
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 16px;">
  <div style="background: var(--color-bg-subtle); padding: 14px 16px; border-radius: 8px; border: 0.5px solid var(--color-border-muted);">
    <div style="font-size: 11px; color: var(--color-text-tertiary); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px;">Revenue</div>
    <div style="font-size: 24px; font-weight: 500;">$2.4M</div>
    <div style="font-size: 12px; color: var(--color-profit);">+12.5%</div>
  </div>
  <!-- more cards... -->
</div>
```

### Data Table

```html
<table style="width: 100%; border-collapse: collapse; font-size: 14px;">
  <thead>
    <tr style="border-bottom: 0.5px solid var(--color-border-muted);">
      <th style="text-align: left; padding: 8px; color: var(--color-text-secondary); font-weight: 500; font-size: 12px;">Symbol</th>
      <th style="text-align: right; padding: 8px; color: var(--color-text-secondary); font-weight: 500; font-size: 12px;">Price</th>
    </tr>
  </thead>
  <tbody>
    <tr style="border-bottom: 0.5px solid var(--color-border-muted);">
      <td style="padding: 8px; font-weight: 500;">AAPL</td>
      <td style="text-align: right; padding: 8px;">$213.18</td>
    </tr>
  </tbody>
</table>
```

### Section with Chart

```html
<div style="background: var(--color-bg-card); border-radius: 8px; border: 0.5px solid var(--color-border-muted); padding: 16px; margin-bottom: 16px;">
  <div style="font-size: 16px; font-weight: 500; margin-bottom: 12px;">Performance</div>
  <div style="position: relative; height: 200px;">
    <canvas id="perfChart"></canvas>
  </div>
</div>
```
