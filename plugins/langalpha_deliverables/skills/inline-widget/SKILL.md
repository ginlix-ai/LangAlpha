---
name: inline-widget
description: "Inline HTML widgets: charts, dashboards, tables, calculators, galleries and questions the user answers by clicking, rendered in the chat via ShowWidget"
---

# Inline Widget

`ShowWidget` renders an HTML/SVG fragment inline in the chat, between your text. It runs JavaScript, so make it something the user can **explore**: sort, filter, toggle, hover, drag a slider. A static picture is the weaker answer.

## When to use

- A quick visual in the conversation: a chart, metric cards, a table, a payoff diagram, a gallery.
- Interaction the user does in place: toggles, what-if inputs, tabs, click to enlarge.
- A question the user answers by clicking, where seeing the data makes the choice easier (see Asking the user).
- One view. A multi-page app or anything with a server of its own is `interactive-dashboard`; a document the user keeps or shares is an HTML report (`html-report`).

Read `.agents/skills/ui-design/SKILL.md` for colour discipline and chart restraint. Its font pairings and type scale are for full documents; a widget uses the host-font rules below.

## What a widget can use

- **Anything a web page loads**: libraries from any CDN (`cdn.jsdelivr.net`, `esm.sh`, `unpkg.com`, `cdnjs.cloudflare.com` are reliable), public APIs, remote images, Google Fonts, stylesheets, WebAssembly (DuckDB, sql.js) and Web Workers. A third-party API still needs to allow cross-origin requests; one that does not fails in the browser, so give the widget that data yourself.
- **Workspace files by relative URL**, at any size: `<img src="charts/q3.png">`, `fetch("data/bars.json")`, `new Worker("sim/worker.js")`. Paths are relative to the workspace folder. They load only for people who can open this workspace: a public share of the thread shows the widget without them, and a downloaded copy keeps the `<img>` files your markup names but not what your script loads. Use URLs for images and large data, and `data_files` for data the widget needs wherever it is opened.
- **`localStorage`**, kept for this widget on the user's device (about 256 KB), for state worth keeping such as a checklist or the selected tab. `sessionStorage` and cookies live only in one view of the widget: full screen and a reload start them empty. IndexedDB reads as absent, so keep to `localStorage`.
- **`sendPrompt(text, data?)`** sends a chat message as the user, with optional JSON `data` (up to 32 KB) attached for you to read, so a form can hand you its values without you parsing prose.
- **`draftPrompt(text, data?)`** adds `text` to the user's message box, after anything already typed there, with `data` attached and without sending, so the user can add to it first.
- **`startThread(text, data?)`** opens a new thread in this workspace with `text` as its first message and `data` attached, and takes the user there, for a line of work that should not continue in this thread.
- These three work only from a click or key press in the widget, one call per click; called on load or from a timer, they are ignored.
- **`data-zoom`** on an `<img>` (or `data-zoom="<url>"` on any element) opens it in the app's full-screen image viewer on click.
- **Forms** handled by a `submit` listener (a form never navigates), **`alert`/`confirm`, writing to the clipboard and `requestFullscreen()`.**
- **Links**: a link to another site opens in a new tab. A link to a workspace file opens nothing unless it has `download`, which saves the file; so does a `download` link to a `blob:` or `data:` URL your script makes.
- **Theme variables** (below) for every colour, and a `themechange` event on `window` when the user switches theme.

A widget cannot read the app's cookies, storage or session, or navigate the chat page.

## After the call

The user sees the widget as soon as the call runs. If the report or screenshot shows an error, a failed load, a blank widget or a layout problem, fix it and call `ShowWidget` again, and say in your text that the new widget replaces the first. The check renders at 768 px in the light theme and measures the widget again at phone width, so the dark theme depends on the theme variables below.

## Layout

The widget sits on the chat surface in a frame that grows to fit its content and fills the chat column: up to 768 px wide on a desktop, and about 350 px on a phone, in the phone app and in the market view's chat panel. Lay it out first for the device this turn's surface line names, and keep it working across that whole range, since the user may reopen the thread on another device.

- **Fluid widths**: size with `%`, `fr` and `minmax()`. A grid of cards written as `repeat(auto-fit, minmax(150px, 1fr))` wraps to fewer columns as the frame narrows. Keep any fixed width under 320 px.
- **Wide content**: put a table with many columns in a wrapper with `overflow-x: auto`, and leave out columns a phone reader does not need.
- **Charts**: set the height on the wrapper and let the width follow; keep axis labels and the legend short enough to fit at 350 px.
- **Touch**: a phone has no hover, so detail shown on hover also shows on tap.

Elements you leave unstyled already match the app in both themes: 14 px body text, headings, links, full-width tables with row rules and tabular figures, buttons, inputs, selects, checkboxes, code, and images, SVGs and canvases capped at the frame's width. These defaults have zero specificity, so any style you write wins, except on the outer element (below); style what the design needs rather than every element.

- **Outer element** (the first element after your `<style>`): the host keeps it transparent, with no border, radius or shadow, so it blends into the chat. Put cards and padding on inner elements.
- **Cards** use theme variables:

```css
/* Card */
background: var(--color-bg-card);
border: 0.5px solid var(--color-border-muted);
border-radius: 8px;
padding: 16px;

/* Metric card */
background: var(--color-bg-subtle);
border-radius: 8px;
```

- **No `position: fixed`.** It places an element against the widget's own frame, which is only as tall as its in-flow content, so a fixed overlay covers the widget or collapses it. Use `data-zoom` to enlarge an image, keep an overlay in flow, or call `requestFullscreen()` on an element.
- **Let the frame grow** rather than scrolling inside it.
- Use `position: relative` for chart containers.

## Theme variables

Injected in both light and dark mode:

| Variable | Use |
|----------|-----|
| `--color-bg-page` | Page background |
| `--color-bg-card` | Card background |
| `--color-bg-elevated` | Raised surface |
| `--color-bg-subtle` | Muted background, metric cards |
| `--color-bg-surface` | Panel surface |
| `--color-bg-input` | Field background |
| `--color-bg-hover` | Hover background |
| `--color-text-primary` | Body text |
| `--color-text-secondary` | Supporting text |
| `--color-text-tertiary` | Labels, hints |
| `--color-text-quaternary`, `--color-text-muted` | Faint text |
| `--color-border-muted` | Default hairline (use at 0.5px) |
| `--color-border-default`, `--color-border-subtle`, `--color-border-elevated` | Stronger, fainter and raised borders |
| `--color-accent-primary`, `--color-accent-soft` | Accent and its tint |
| `--color-profit`, `--color-profit-soft` | Gain and its tint |
| `--color-loss`, `--color-loss-soft` | Loss and its tint |
| `--color-warning`, `--color-info`, `--color-success` | Status |
| `--color-chart-1` to `--color-chart-8` | Chart series, in this order: blue, orange, aqua, yellow, magenta, green, violet, red |

**Never hardcode a colour**; write `var(--color-x)`. The variables are set wherever the widget is shown (the chat, full screen, a download, a PDF) and switch with the theme, while a literal stays the same and disappears or glares in the other theme. A canvas or an SVG drawn from script cannot use `var()`, so read the values with `pick()` (below) when drawing and redraw on `themechange`.

Give series the chart colours in order, one per series, and keep a series' colour when a filter hides others. Past eight series, group the rest as Other. Three of the light colours are faint on white, so name every series in a legend or a direct label, and never let colour alone carry meaning: write the sign on a change (`+1.2%`) beside its profit or loss colour.

What still breaks in one theme or on one device:

- **SVG text and shapes are black by default.** Set `fill` and `stroke` from `pick()` (in D3, `.attr('fill', pick('--color-text-secondary'))`) and redraw on `themechange`.
- **An image keeps its own background.** A chart saved as a PNG with a white background is a white box in dark mode, and its text stays dark. Draw the chart in the widget instead of embedding a picture of it.
- **`vh` sizes against the widget's own frame,** which is only as tall as its content. Set heights in px.
- **A library imported as an ES module** (`import Chart from "https://esm.sh/chart.js"`) does not get the theme defaults below; load Chart.js with `<script src>`.

## Charts (Chart.js)

Chart.js loaded with `<script src>` follows the theme by itself: its text, grid lines and font use the theme, a dataset you give no colour gets the next chart colour, and all of it switches with the theme. Colour a dataset yourself only when the colour means something, such as profit and loss, and then repaint it on `themechange`.

```html
<div style="position: relative; height: 220px;">
  <canvas id="perf"></canvas>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
  function pick(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }
  var chart = new Chart(document.getElementById('perf'), {
    type: 'line',
    data: {
      labels: ['Jan', 'Feb', 'Mar', 'Apr'],
      datasets: [
        { label: 'NVDA', data: [100, 112, 108, 121], tension: 0.3 },
        { label: 'AMD', data: [100, 96, 104, 110], tension: 0.3 },
        { label: 'S&P 500', data: [100, 101, 103, 104], tension: 0.3, borderDash: [4, 4] }
      ]
    },
    options: { responsive: true, maintainAspectRatio: false, scales: { x: { grid: { display: false } } } }
  });
</script>
```

- Set the height on the wrapper, never on the canvas.
- Use `responsive: true, maintainAspectRatio: false`.
- For another library (ECharts, Plotly, D3), read the colours with `pick()` and redraw on `themechange`.

## Typography

- The font, body size and heading sizes (h1 22 px, h2 18 px, h3 16 px, weight 500) come from the frame; set a font family only when the design calls for one.
- Weights 400 and 500; 600 sparingly for a key figure. Avoid 700.
- Sentence case. No Title Case or ALL CAPS, except short metric labels.

## Interactivity

When the data supports it, reach for:

- **Sortable and filterable tables**: sort by P&L or weight, filter to a sector.
- **Series and metric toggles**: show or hide series, price vs % change, 1M / 6M / 1Y.
- **Hover detail**: tooltips with the underlying numbers.
- **What-if inputs**: a slider or field that recomputes a figure live.
- **Tabs or segmented views** for a dense widget.

Keep a meaningful default state, so the widget reads correctly before anyone touches it. Handle exploration in the widget; use `sendPrompt()` only when the next step needs you (a new query, a deeper analysis), and mark those buttons with **↗**:

```html
<button onclick="sendPrompt('Break down revenue by segment')">Revenue by segment ↗</button>

<form onsubmit="event.preventDefault(); sendPrompt('Draft this alert', {symbol: this.symbol.value, above: +this.above.value})">
  <input name="symbol" value="NVDA"> <input name="above" type="number" value="160">
  <button>Draft alert ↗</button>
</form>
```

`setInterval` and `requestAnimationFrame` work for animation. A widget's data is what you gave it when you drew it; for fresh market data, draw it again.

## Asking the user

A widget can ask the user a question while showing what the answer depends on: scenarios side by side, rows picked from a screen, a few assumptions to set. Draw the data and the options, say in your reply what you are waiting for, and end your turn. The user's choice arrives as their next message with its `data` attached, and a typed reply works as well.

- Give each option a button that sends it with `sendPrompt`, carrying what was chosen in `data`.
- Offer "Add a note first" with `draftPrompt` when the user may want to qualify the choice.
- Offer a new thread with `startThread` for an option that opens separate work, such as researching one name from a screen.
- Make the options mutually exclusive, and mark one as your recommendation where you have one.

```html
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px;">
  <div style="border: 1px solid var(--color-border-default); border-radius: 8px; padding: 12px;">
    <h4>Base</h4>
    <p style="color: var(--color-text-secondary);">+38% revenue, data center flat</p>
    <button onclick="sendPrompt('Model the base case', {scenario: 'base', growth: 0.38})">Choose ↗</button>
  </div>
  <div style="border: 2px solid var(--color-accent-primary); border-radius: 8px; padding: 12px;">
    <h4>Bull (recommended)</h4>
    <p style="color: var(--color-text-secondary);">+71% revenue, Blackwell ramp</p>
    <button onclick="sendPrompt('Model the bull case', {scenario: 'bull', growth: 0.71})">Choose ↗</button>
  </div>
</div>
<p style="margin-top: 10px;">
  <button onclick="draftPrompt('Model the bull case, but ', {scenario: 'bull'})">Add a note first</button>
  <button onclick="startThread('Research the bear case for NVDA: export limits and their revenue impact', {scenario: 'bear'})">Bear case in a new thread ↗</button>
</p>
```

## File data

Write the data with Python, then hand the file to the widget, by URL for anything large:

```python
df.to_json("q3_review/bars.json", orient="records")
```

```html
<script>
  fetch("q3_review/bars.json").then(r => r.json()).then(draw);
</script>
```

or embed small files, which travel with the widget into a share or a download, with `data_files=["q3_review/summary.json"]` and read `JSON.parse(window.__WIDGET_DATA__["summary.json"])`.

## Design patterns

### Metric cards row

```html
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 16px;">
  <div style="background: var(--color-bg-subtle); padding: 14px 16px; border-radius: 8px;">
    <div style="font-size: 12px; color: var(--color-text-tertiary); margin-bottom: 6px;">Revenue</div>
    <div style="font-size: 24px; font-weight: 500;">$2.4M</div>
    <div style="font-size: 12px; color: var(--color-profit);">+12.5%</div>
  </div>
</div>
```

### Data table

The frame already styles a plain table; add only alignment and colour that carry meaning:

```html
<div style="overflow-x: auto;">
  <table>
    <thead><tr><th>Symbol</th><th style="text-align: right;">Price</th><th style="text-align: right;">Change</th></tr></thead>
    <tbody>
      <tr><td>AAPL</td><td style="text-align: right;">$213.18</td><td style="text-align: right; color: var(--color-profit);">+1.2%</td></tr>
      <tr><td>MSFT</td><td style="text-align: right;">$441.06</td><td style="text-align: right; color: var(--color-loss);">−0.4%</td></tr>
    </tbody>
  </table>
</div>
```

### Image gallery

```html
<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 8px;">
  <img src="charts/revenue_by_segment.png" data-zoom alt="Revenue by segment, FY2021 to FY2025" style="width: 100%; border-radius: 6px;">
  <img src="charts/gross_margin.png" data-zoom alt="Gross margin by quarter" style="width: 100%; border-radius: 6px;">
</div>
```
