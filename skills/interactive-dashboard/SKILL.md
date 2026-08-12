---
name: interactive-dashboard
description: "Interactive web dashboards: stock trackers, sector heatmaps, portfolio monitors — served via preview URL"
---

# Interactive Dashboard

Build interactive web dashboards inside the sandbox and expose them to the user via `GetPreviewUrl`. Use this skill for any request involving dashboards, trackers, monitors, live visualizations, or interactive web apps.

## When to Use

Use this skill for a **live, served web app** — one that needs a running server, not a single file:

- User asks for a **dashboard**, **tracker**, or **monitor** that **refreshes live data** (polling, auto-update)
- The app needs **server-side logic** — filtering/screening over a large dataset, on-demand fetches, computed endpoints
- **Multi-page / routed** apps, or anything that needs React-level component interactivity
- The dataset is **too large to embed** in a single HTML file
- User explicitly says "preview", "web view", "web app", or wants it running at a URL

**Do NOT use if:**
- User wants a **self-contained HTML report** — even an *interactive* one (sortable tables, tabs, hover/zoom charts) over a **data snapshot**. That's `.agents/skills/html-report/SKILL.md`: one file in `results/`, keepable, printable, PDF-exportable, share-linkable. Interactivity by itself does **not** require a dashboard.
- User wants a **static chart image** → matplotlib/plotly `savefig`.
- User wants an **in-chat figure** → `inline-widget` (`ShowWidget`).

### Dashboard vs. HTML Report

Both can be interactive, so the divide is **live served app vs. self-contained snapshot file**, not static vs. interactive:

| | interactive-dashboard (this skill) | html-report |
|---|---|---|
| Delivery | A **running server**, exposed via `GetPreviewUrl` | One **`.html` file** in `results/` |
| Data | **Live / refreshing**, fetched from a backend; large datasets OK | A **snapshot** embedded in the file |
| Interactivity | Full app — routing, server-side filtering, live updates | Client-side over the snapshot — sort, filter, tabs, chart hover/zoom |
| Keep / print / share | A URL, live only while the workspace runs | Downloadable, PDF-exportable, share-linkable as one artifact |
| Pick when | Data must be live, or compute/scale needs a server | The answer is a deliverable the user keeps |

## Architecture

Choose the tier based on complexity:

| Tier | When | Stack | Serve command |
|------|------|-------|---------------|
| **Simple** | Snapshot-at-load data, few charts, no backend logic (still served via preview URL) | Self-contained HTML + CDN libs | `python -m http.server 8050 --bind 0.0.0.0` |
| **FastAPI + HTML** | Live data refresh, server-side logic, no React needed | FastAPI serves `static/` + `fetch()` polling | `bash start.sh` |
| **Complex** | Filtering, routing, component interactivity, multi-page | FastAPI backend + Vite/React frontend | `bash start.sh` |

**Decision rule:** Start with Simple. Escalate to FastAPI + HTML when user needs live data refresh or server-side logic. Escalate to Complex only when user needs React-level component interactivity, client-side routing, or a multi-page SPA.

**Port convention:** Use port **8050** (default). Range 8050-8059 for dashboards.

### CSP / Iframe Safety

The preview iframe enforces Content Security Policy (CSP). Certain patterns are **silently blocked** — no error banner, just dead UI elements. Always use the safe alternatives:

| Blocked pattern | Safe alternative |
|-----------------|-----------------|
| `<button onclick="fn()">` | `el.addEventListener('click', fn)` |
| `<div onmouseover="fn()">` | `el.addEventListener('mouseover', fn)` |
| Any `on*="..."` HTML attribute | `el.addEventListener(event, fn)` |
| `innerHTML` with `onclick` | `document.createElement()` + `addEventListener` |
| `eval("code")` | Direct function calls |
| `new Function("code")` | Named function declarations |
| `setTimeout("code string", ms)` | `setTimeout(fn, ms)` (function reference) |
| `<a href="javascript:...">` | `<a href="#" data-action="...">` + `addEventListener` |

**Quick self-check** — run before serving to catch violations:

```python
import subprocess
result = subprocess.run(
    ["grep", "-rnE", r'on(click|input|change|focus|blur|submit|load|error|mouse|key)\s*=',
     "work/dashboard/"],
    capture_output=True, text=True
)
if result.stdout.strip():
    raise RuntimeError(f"CSP-unsafe inline handlers found:\n{result.stdout}")
```

**Template literal hygiene** — when building HTML strings in JS template literals, CSS semicolons inside `${}` expressions cause silent parse failures:

```javascript
// BAD — semicolon inside ${} terminates the expression early
const el = `<div style="color:${positive ? 'green' : 'red'; font-weight:600}">`;

// GOOD — close the expression first, then continue the attribute string
const el = `<div style="color:${positive ? 'green' : 'red'};font-weight:600">`;
```

Rule: **never put a CSS semicolon inside `${}`** — always close `}` before the semicolon.

### How Preview Serving Works

`GetPreviewUrl` is a **platform-level tool** available only to the main agent runtime. It is NOT a Python function — do not `import` it or call it from `execute_code`. The agent invokes it as a tool call.

When you call `GetPreviewUrl(port, command, title)`:

1. The **command is persisted to the database** automatically
2. The platform starts the command in a dedicated sandbox session for that port
3. It polls until the port is listening, then generates a signed URL
4. If the port is **already reachable**, the command start is skipped entirely

**Sub-agent fallback:** Sub-agents cannot call `GetPreviewUrl`. Instead, build the dashboard files, start the server for verification, then return the serve details so the orchestrating agent can call `GetPreviewUrl`.

**All tiers** — use the Bash tool with `run_in_background=true` to start the server:

```bash
# Simple tier — Bash tool with run_in_background=true
cd work/<task> && python -m http.server 8050 --bind 0.0.0.0

# Docker tiers — Bash tool with run_in_background=true
cd work/<task> && bash start.sh
```

Then verify it's up in a separate (foreground) Bash call:

```bash
for i in $(seq 1 15); do curl -sf http://127.0.0.1:8050/ > /dev/null && echo "Server ready" && exit 0 || sleep 1; done; echo "FAIL"; exit 1
```

Then return **all three fields** to the orchestrating agent (it needs the command for DB persistence / restart recovery):

```
port: 8050
command: "cd work/<task> && python -m http.server 8050 --bind 0.0.0.0"  # or "bash work/<task>/start.sh"
title: "AAPL Stock Dashboard"
```

The orchestrating agent calls `GetPreviewUrl(port=8050, command="...", title="...")` which persists the command.

**On workspace restart** (user closes and reopens later):
- The sandbox filesystem persists (files, installed packages, Docker image cache all survive)
- Only processes die — the platform looks up the saved command and re-executes it
- The preview URL auto-recovers

**Implication:** Write commands that are **idempotent** — they must work whether run for the first time or re-run after a restart. The platform handles the rest. Docker image cache survives restart so rebuilds are fast (~2-5s with warm cache).

### Sandbox Capabilities

All pre-installed in the Daytona sandbox snapshot — no `pip install` or `apt-get` needed:

- **Python 3.12** + pandas, numpy, plotly, matplotlib, requests, httpx, yfinance
- **FastAPI + uvicorn** (available via `fastmcp` transitive dependency)
- **Node.js 24 + npm** (host sandbox) / **Node.js 20** (Docker `apt install nodejs`) — scaffold Vite/React projects with `npm create vite@latest`
- **Docker Engine** — for complex tier containerized dashboards (backend + frontend in one image)
- **Playwright + Chromium** — available for verification testing

## Workflow

### Step 1: Clarify Scope

Before writing any code:
- What data? (specific tickers, sector, portfolio, screener results)
- What visualizations? (price chart, comparison table, heatmap, etc.)
- Static snapshot or live refresh?
- How complex? (determines simple vs complex tier)

### Step 2: Fetch Data

Use **YF MCP servers** as the default financial data source (no API keys needed):

```python
from tools.yf_price import get_stock_history, get_multiple_stocks_history
from tools.yf_fundamentals import get_company_info, compare_valuations
from tools.yf_analysis import get_analyst_price_targets, get_news
from tools.yf_market import get_sector_info, screen_stocks
```

Always fetch and validate data **before** writing any HTML/React code. Check for empty responses.

### Step 3: Process Data

Use pandas to clean, aggregate, and compute derived metrics:

```python
import pandas as pd
import json

# Fetch
history = get_stock_history("AAPL", period="1y", interval="1d")
info = get_company_info("AAPL")

# Process
df = pd.DataFrame(history)
df['change_pct'] = df['close'].pct_change() * 100

# Prepare for frontend
chart_data = json.dumps({
    "dates": df['date'].tolist(),
    "prices": df['close'].tolist(),
    "volumes": df['volume'].tolist(),
})
```

### Step 4: Build Dashboard

**Simple tier** — write a self-contained HTML file:

```python
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AAPL Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
    <style>
        /* See references/ui-components.md for the theme foundation CSS */
    </style>
</head>
<body>
    <script>const DATA = {chart_data};</script>
    <script>
        /* Chart rendering code */
    </script>
</body>
</html>"""

with open("work/dashboard/index.html", "w") as f:
    f.write(html)
```

**FastAPI + HTML tier** — write a FastAPI server with API routes + `StaticFiles` mount, and a single `static/index.html` with `fetch()` polling (see FastAPI + HTML Tier section below).

**Complex tier** — scaffold a FastAPI + Vite/React project (see Complex Tier section below).

### Step 5: Verify Before Serving

**Tier 1 — Syntax check (required, < 1 second)**

Extract `<script>` blocks from the HTML and check with `node --check`:

```python
import re, subprocess, tempfile, os

with open("work/dashboard/index.html") as f:
    html = f.read()

scripts = re.findall(r'<script(?![^>]*src)[^>]*>(.*?)</script>', html, re.DOTALL)
for i, src in enumerate(scripts):
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as tmp:
        tmp.write(src)
        tmp_path = tmp.name
    result = subprocess.run(["node", "--check", tmp_path], capture_output=True, text=True)
    os.unlink(tmp_path)
    if result.returncode != 0:
        raise RuntimeError(f"JS syntax error in script block {i+1}:\n{result.stderr}")
print("Syntax check passed")
```

Also run the CSP self-check grep from the CSP section above.

**Tier 2 — Browser verification (recommended for interactive dashboards)**

Run when the dashboard has buttons, filters, or tabs. Skip for static data displays.

After `GetPreviewUrl` starts the server, run a Playwright check to catch runtime errors:

```python
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    js_errors = []
    page.on("pageerror", lambda exc: js_errors.append(str(exc)))
    page.goto("http://127.0.0.1:8050/", wait_until="networkidle", timeout=20000)
    assert not js_errors, f"JS runtime errors: {js_errors}"
    assert len(page.locator("body").inner_text().strip()) > 20, "Page appears blank"
    page.screenshot(path="work/dashboard/verify-screenshot.png", full_page=True)
    browser.close()
print("Browser verification passed")
```

See [references/verification.md](references/verification.md) for extended templates with button-click testing and API response validation.

### Step 6: Serve & Expose

```python
# Simple tier
GetPreviewUrl(port=8050, command="cd work/dashboard && python -m http.server 8050 --bind 0.0.0.0", title="AAPL Dashboard")

# FastAPI + HTML tier / Complex tier
GetPreviewUrl(port=8050, command="bash work/dashboard/start.sh", title="Stock Dashboard")
```

**Local verification before `GetPreviewUrl`** — if you need the server running for Playwright verification, use the Bash tool with `run_in_background=true` (see sub-agent fallback above for the pattern). Do NOT use `subprocess.Popen` from `execute_code` — the process becomes a zombie when the tool-call shell exits.

### Step 7: Iterate

After the user sees the preview, adjust layout, data, or charts based on feedback.

## Data Integration — YF MCP Servers

Default data sources for common dashboard needs:

| Need | MCP Server | Function | Key params |
|------|------------|----------|------------|
| Price history | `yf_price` | `get_stock_history` | `ticker, period="1y", interval="1d"` |
| Multi-stock prices | `yf_price` | `get_multiple_stocks_history` | `tickers=["AAPL","MSFT"]` |
| Dividends & splits | `yf_price` | `get_dividends_and_splits` | `ticker` |
| Company profile | `yf_fundamentals` | `get_company_info` | `ticker` |
| Income statement | `yf_fundamentals` | `get_income_statement` | `ticker, quarterly=True` |
| Balance sheet | `yf_fundamentals` | `get_balance_sheet` | `ticker, quarterly=True` |
| Cash flow | `yf_fundamentals` | `get_cash_flow` | `ticker, quarterly=True` |
| Valuation comps | `yf_fundamentals` | `compare_valuations` | `tickers=["AAPL","MSFT","GOOGL"]` |
| Financial comps | `yf_fundamentals` | `compare_financials` | `tickers, statement_type="income"` |
| Earnings data | `yf_fundamentals` | `get_earnings_data` | `ticker` |
| Analyst targets | `yf_analysis` | `get_analyst_price_targets` | `ticker` |
| Recommendations | `yf_analysis` | `get_analyst_recommendations` | `ticker` |
| Upgrades/downgrades | `yf_analysis` | `get_upgrades_downgrades` | `ticker` |
| Earnings estimates | `yf_analysis` | `get_earnings_estimates` | `ticker` |
| Revenue estimates | `yf_analysis` | `get_revenue_estimates` | `ticker` |
| Growth estimates | `yf_analysis` | `get_growth_estimates` | `ticker` |
| Institutional holders | `yf_analysis` | `get_institutional_holders` | `ticker` |
| Insider transactions | `yf_analysis` | `get_insider_transactions` | `ticker` |
| ESG data | `yf_analysis` | `get_sustainability_data` | `ticker` |
| News | `yf_analysis` | `get_news` | `ticker, count=10` |
| Ticker search | `yf_market` | `search_tickers` | `query, max_results=8` |
| Market status | `yf_market` | `get_market_status` | `market="US"` |
| Stock screener | `yf_market` | `screen_stocks` | `filters, sort_field, count` |
| Predefined screens | `yf_market` | `get_predefined_screen` | `screen_name` (day_gainers, most_actives, etc.) |
| Earnings calendar | `yf_market` | `get_earnings_calendar` | `start, end` (YYYY-MM-DD) |
| Sector info | `yf_market` | `get_sector_info` | `sector_key` (technology, healthcare, etc.) |
| Industry info | `yf_market` | `get_industry_info` | `industry_key` |

### Yahoo Finance Field Conventions

Many fields are already in display units — do NOT multiply by 100:

| Field | Unit | Example | Note |
|-------|------|---------|------|
| `regularMarketChangePercent` | % (not decimal) | `0.389` = +0.39% | Do NOT multiply by 100 |
| `dividendYield` | % (not decimal) | `0.41` = 0.41% | Same convention |
| `marketCap` | Absolute USD | `3.71e12` | Divide by 1e9 for $B display |
| `trailingPE` | Ratio | `31.98` | Display directly |

### `get_predefined_screen` Response Structure

Quotes are nested — not at the top level:

```python
result = get_predefined_screen("day_gainers")
quotes = result["data"]["quotes"]  # nested at result["data"]["quotes"], NOT result["quotes"]
# Each quote: symbol, regularMarketPrice, regularMarketChangePercent, marketCap, ...
```

### Direct yfinance Usage (Docker / without MCP)

Inside Docker containers, MCP tool modules are unavailable. Use `yfinance` directly:

```python
import yfinance as yf

# Stock screener (yfinance 1.2.0+)
result = yf.screen("day_gainers", count=5)
quotes = result.get("quotes", [])

# Stock data
ticker = yf.Ticker("AAPL")
info = ticker.info
hist = ticker.history(period="1y")
```

## UI Design Rules

Read `.agents/skills/ui-design/SKILL.md` for design quality (typography, color, avoiding generic AI aesthetics).

### Theming

A dashboard must **follow the theme it is viewed in** — never ship a fixed palette:

- **In the app**, the viewer injects theme tokens (`--color-bg-page`, `--color-text-primary`, …) that flip with the user's light/dark setting.
- **Standalone** (downloaded and opened directly) no tokens exist, so the dashboard follows the OS via `prefers-color-scheme`.

The **Theme Foundation block** in [references/ui-components.md](references/ui-components.md) §1 implements both: each bridge variable reads the app token first and falls back to an OS-adaptive value. Include that block **verbatim** in every dashboard, then style only with the bridge variables. Canvas/SVG chart code can't read `var()` — read the resolved value with the `pick()` helper (see [references/chart-patterns.md](references/chart-patterns.md)). **Never hardcode a palette hex in styles.**

| Bridge variable | App token | Role |
|---|---|---|
| `--bg-page` | `--color-bg-page` | Page background |
| `--bg-card` | `--color-bg-card` | Card / panel background |
| `--bg-elevated` | `--color-bg-elevated` | Raised surface (menus, popovers) |
| `--bg-subtle` | `--color-bg-subtle` | Zebra rows, quiet fills |
| `--bg-hover` | `--color-bg-hover` | Hover highlight, table headers |
| `--text-primary` | `--color-text-primary` | Primary text |
| `--text-secondary` | `--color-text-secondary` | Labels, captions, axis ticks |
| `--text-tertiary` | `--color-text-tertiary` | De-emphasized text |
| `--border` | `--color-border-muted` | Borders, grid lines |
| `--accent` | `--color-accent-primary` | Links, primary series, active tab |
| `--positive` | `--color-profit` | Gains |
| `--negative` | `--color-loss` | Losses |
| `--warning` | `--color-warning` | Cautions |

Light and dark literal values live **only** in the foundation block — don't restate them elsewhere.

### Layout

- KPI cards in a row at top (price, change, volume, market cap)
- Charts in a responsive 2-column grid below
- Tables full-width at bottom
- No horizontal scroll — everything fits the iframe width
- Use CSS Grid with `auto-fit` and `minmax()` for responsive columns

### Typography

```css
font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
```

| Element | Size |
|---------|------|
| Page title (h1) | `1.5rem` |
| Section title (h2) | `1.125rem` |
| Body text | `0.875rem` |
| Labels / captions | `0.75rem` |
| KPI value | `1.75rem` (bold) |

### Financial Data Formatting

- **Prices**: 2 decimal places with `$` prefix (`$182.52`)
- **Percentages**: 2 decimal places with `%` suffix, color-coded green/red (`+2.34%` / `-1.56%`)
- **Large numbers**: Abbreviated with suffix (`$2.87T`, `$142.5B`, `$3.2M`)
- **Volumes**: Comma-separated (`12,345,678`) or abbreviated (`12.3M`)
- **Dates**: `MMM DD, YYYY` format (`Mar 15, 2026`)

See [references/ui-components.md](references/ui-components.md) for complete CSS and component code.

## FastAPI + HTML Tier — Project Structure

For live-data dashboards without React. FastAPI serves API endpoints and static HTML directly — no npm, no build step. Use the **copy-ready template files** with `.fastapi-html` suffix in `references/`:

```
work/<task>/
├── Dockerfile           # cp references/Dockerfile.fastapi-html Dockerfile
├── start.sh             # cp references/start.sh start.sh
├── server/
│   ├── main.py          # cp references/server-main.fastapi-html.py server/main.py
│   └── requirements.txt # cp references/requirements.txt server/requirements.txt
└── static/
    └── index.html       # Single HTML file with fetch() polling
```

### Setup Workflow

1. **Copy template files** — all four `cp` commands above, then add your API routes to `server/main.py`
2. **Add your Python deps** to `server/requirements.txt` (append pandas, yfinance, etc.)
3. **Write `static/index.html`** with `fetch()` calls to your API routes for live data
4. **Serve**: `GetPreviewUrl(port=8050, command="bash work/<task>/start.sh", title="Dashboard")`

### Template Files

**`Dockerfile`** ([references/Dockerfile.fastapi-html](references/Dockerfile.fastapi-html)) — Python 3.12-slim only (no Node/npm). Uses `uv pip install` for fast deps.

**`server/main.py`** ([references/server-main.fastapi-html.py](references/server-main.fastapi-html.py)) — FastAPI skeleton with CORS, `HEAD /`, `/healthz`, and `StaticFiles` mount for `static/` directory. `StaticFiles` is the last mount (catches all unmatched routes).

**`start.sh`** — same `references/start.sh` template (works unchanged for all Docker tiers).

### Key Differences from Complex Tier

- **No `frontend/` directory** — HTML lives in `static/`
- **No npm/Node** — Dockerfile uses `python:3.12-slim` only
- **`StaticFiles` mount** — serves `static/` directory with `html=True` (auto-serves `index.html`)
- **`fetch()` for live data** — HTML uses `setInterval(() => fetch('/api/data').then(...), 30000)` for auto-refresh
- **No SPA routing** — single `index.html`, no client-side router needed

## Complex Tier — Project Structure

When using FastAPI + Vite/React, scaffold this structure using the **copy-ready template files** in `references/`:

```
work/<task>/
├── Dockerfile           # Copy from references/Dockerfile
├── start.sh             # Copy from references/start.sh
├── server/
│   ├── main.py          # Copy from references/server-main.py, add your API routes
│   ├── requirements.txt # Copy from references/requirements.txt, add your deps
│   ├── routes/          # API route modules (stocks.py, sectors.py)
│   └── models.py        # Pydantic response models
├── frontend/
│   ├── package.json     # Vite + React + chart libraries
│   ├── vite.config.js   # Copy from references/vite.config.js
│   ├── index.html
│   └── src/
│       ├── App.jsx      # Main app with routing/tabs
│       ├── components/  # Chart, KPI, Table components
│       ├── hooks/       # useStockData, useSectorData, etc.
│       └── utils/       # formatters, color helpers
└── verify.py            # Copy from references/verification.md (optional)
```

### Setup Workflow

1. **Copy template files** from `references/` into `work/<task>/` — they work with zero modifications for port 8050
2. **Add your API routes** to `server/main.py` (the template includes CORS, `HEAD /`, `/healthz`, and static file serving)
3. **Add your Python deps** to `server/requirements.txt` (template includes fastapi + uvicorn)
4. **Write frontend code** in `frontend/src/` (vite.config.js template proxies `/api` to backend on port 8051)
5. **Serve**: `GetPreviewUrl(port=8050, command="bash work/<task>/start.sh", title="Dashboard")`

### Template Files

**`Dockerfile`** ([references/Dockerfile](references/Dockerfile)) — Python 3.12 + Node + uv + tzdata. Builds frontend at image time, serves static files from FastAPI. Uses `uv pip install` for fast dependency installation.

**`start.sh`** ([references/start.sh](references/start.sh)) — Cold-boot safe Docker wrapper. Starts dockerd if needed, builds image (uses layer cache on re-runs), removes old container, starts new one, health-checks with log dump on failure. Env var overrides: `PORT` (default 8050), `NAME` (default "dashboard").

**`server/main.py`** ([references/server-main.py](references/server-main.py)) — FastAPI skeleton with CORS, `HEAD /` (liveness for platform proxy), `/healthz`, SPA catch-all route for client-side routing (`/tab/news`, `/stocks/AAPL` → `index.html`), and a 503 fallback if the frontend build is missing.

**`server/requirements.txt`** ([references/requirements.txt](references/requirements.txt)) — Minimal: fastapi + uvicorn. Append project-specific packages (pandas, yfinance, etc.).

**`frontend/vite.config.js`** ([references/vite.config.js](references/vite.config.js)) — Vite + React with `/api` proxy to backend on port 8051.

> **Critical: Vite proxy is dev-only.** The `proxy` setting in `vite.config.js` only applies during `npm run dev`. The production build outputs plain static files with no proxy. In the Docker image, FastAPI serves both the built SPA from `frontend/dist/` and all `/api/*` routes from the **same port**. The reference `server-main.py` template already does this correctly — do NOT use a two-process architecture with separate static file server and API server.

### Docker Gotchas

- **MCP tools are host-only**: The `tools/` modules (e.g., `from tools.yf_price import ...`) exist only in the host workspace Python environment — they are NOT copied into the Docker image. FastAPI server code inside Docker must call `yfinance` directly. Add `yfinance` to `server/requirements.txt`
- **`--network host`**: Required for the container to reach external APIs (yfinance, MCP servers). Already set in `start.sh` template
- **`tzdata`**: Required for yfinance timezone handling. Already in `Dockerfile` template
- **Image cache**: Persists across workspace restarts. First build: 30-60s. Subsequent builds: ~2-5s (cached layers)
- **Logs**: Use `docker logs dashboard` to debug startup failures
- **Fallback without Docker**: If Docker is unavailable, build the frontend and run FastAPI directly:
  ```bash
  fuser -k 8050/tcp 2>/dev/null || true
  cd frontend && npm install --prefer-offline && npm run build && cd ..
  cd server && uvicorn main:app --host 0.0.0.0 --port 8050
  ```

## Chart Libraries

### Simple Tier (CDN-loaded, no install)

| Library | CDN URL | Best for |
|---------|---------|----------|
| **Chart.js** | `https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js` | Line, bar, pie, doughnut, area |
| **Plotly.js** | `https://cdn.plot.ly/plotly-2.35.2.min.js` | Candlestick, heatmap, treemap |
| **Lightweight Charts** | `https://unpkg.com/lightweight-charts@4/dist/lightweight-charts.standalone.production.js` | TradingView-style candlestick |

Default to **Chart.js**. Use Plotly for candlesticks/heatmaps. Lightweight Charts only for TradingView-style.

### Complex Tier (npm packages)

| Library | Package | Best for |
|---------|---------|----------|
| **Recharts** | `recharts` | Composable React charts — line, bar, area, pie |
| **Plotly React** | `react-plotly.js plotly.js` | Candlestick, heatmap, treemap |
| **Lightweight Charts** | `lightweight-charts` | TradingView-style financial charts |

Default to **Recharts**. Use Plotly for advanced financial charts.

See [references/chart-patterns.md](references/chart-patterns.md) for ready-to-use code snippets.

## Common Dashboard Patterns

### 1. Single Stock Dashboard

**Data**: `get_stock_history`, `get_company_info`, `get_analyst_price_targets`, `get_news`

Layout:
- KPI row: current price, day change %, 52-week range, market cap, P/E
- Price chart (line/candlestick) with volume bars
- Analyst price target range (horizontal bar)
- Recent news list

### 2. Multi-Stock Comparison

**Data**: `get_multiple_stocks_history`, `compare_valuations`, `compare_financials`

Layout:
- Normalized price overlay chart (base 100)
- Performance bar chart (YTD, 1Y, 3Y returns)
- Valuation comparison table (P/E, EV/EBITDA, P/B, etc.)
- Revenue/earnings growth comparison

### 3. Sector Heatmap

**Data**: `get_sector_info`, `screen_stocks` with sector filters, `get_predefined_screen`

Layout:
- Treemap colored by daily/weekly performance
- Sector summary cards (top movers, average P/E)
- Top gainers/losers table
- Sector rotation chart

### 4. Earnings Tracker

**Data**: `get_earnings_calendar`, `get_earnings_data`, `get_earnings_estimates`

Layout:
- Calendar view with upcoming earnings dates
- Beat/miss history chart (bar chart with surprise %)
- EPS estimate vs actual trend line
- Revenue estimate revision chart

### 5. Portfolio Monitor

**Data**: `get_multiple_stocks_history`, `compare_valuations`, `get_company_info` for each holding

Layout:
- Holdings table (ticker, shares, price, value, weight, day P&L)
- Allocation pie chart (by sector/stock)
- Total portfolio value line chart
- Sector exposure bar chart

## Best Practices

### General

- **Data-first**: Fetch and validate ALL data before writing any HTML/React code
- **Fail gracefully**: If a ticker is invalid or API returns empty, show "No data available" — don't crash
- **No console errors**: Verify chart rendering works before calling `GetPreviewUrl`
- **Responsive**: CSS Grid `auto-fit` for layouts. No horizontal scroll at any width
- **Performance**: Resample data if > 1000 rows. Don't load unused chart libraries

### Simple Tier

- **Embed data as JSON**: `<script>const DATA = ${json.dumps(data)}</script>` — never inline raw Python dicts
- **Escape properly**: Always use `json.dumps()` with `ensure_ascii=False` for safe JSON embedding
- **Self-contained**: All CSS in `<style>`, all JS in `<script>`, libraries via CDN `<script src="...">`
- **One HTML file**: Keep everything in a single `index.html` — eliminates path bugs

### Complex Tier

- **Separation of concerns**: FastAPI = data API, Vite/React = UI rendering
- **Pydantic models**: Define response schemas for type safety
- **Component per widget**: One React component per chart/card/table
- **Shared hooks**: `useStockData(ticker)`, `useSectorData(key)` for data fetching
- **Error boundaries**: Wrap chart components so one failure doesn't crash the whole page
- **Single-port production**: Vite proxy is dev-only. In production Docker builds, FastAPI serves both `/api/*` and the SPA from one port
- **`host: '0.0.0.0'`**: Both FastAPI and Vite must bind to `0.0.0.0`, not `127.0.0.1` or `localhost`

## Error Handling & Debugging

| Problem | Solution |
|---------|----------|
| `GetPreviewUrl` returns error | Port already in use — try a different port (8051, 8052, ...) |
| Page is blank | Check for JS errors — ensure all `getElementById` targets exist |
| Data is empty | Validate MCP tool response before embedding — check for `None` or empty lists |
| Buttons/inputs do nothing | CSP blocking inline handlers — replace `onclick=` etc. with `addEventListener`. Run CSP self-check |
| FastAPI won't start | Ensure `host='0.0.0.0'` in `uvicorn.run()` |
| Vite won't start | Ensure `--host 0.0.0.0` flag and check if port is free |
| CORS errors | Add `CORSMiddleware` to FastAPI or use Vite proxy |
| Charts don't render | CDN scripts must load before chart initialization — use `DOMContentLoaded` event |
| Iframe shows "refused to connect" | Server not ready yet — add a small delay or retry logic |
| HEAD / returns 404 or 405 | Add `@app.head("/")` as its own function — don't stack with `/healthz` (use `server-main.py` template) |
| SPA deep route returns 404 | Add catch-all `@app.get("/{full_path:path}")` that serves `index.html` for non-file paths (use `server-main.py` template) |
| `start.sh` fails on restart | Ensure idempotent: dockerd startup check, `docker rm -f` before `docker run` (use `start.sh` template) |
| Docker: yfinance timezone error | Add `tzdata` package to Dockerfile (included in template) |
| Docker: can't reach external APIs | Use `--network host` flag (included in `start.sh` template) |
| `GetPreviewUrl` not found / `NameError` | Tool only available to main agent runtime — sub-agents use Bash tool with `run_in_background=true` to start the server, then report port/command/title back |
| Playwright `ERR_CONNECTION_REFUSED` | Use `127.0.0.1:PORT` not `localhost` — sandbox resolves `localhost` to IPv6 (`::1`) first |
| Background server died / `<defunct>` | Use Bash tool with `run_in_background=true` — do NOT use `subprocess.Popen` from `execute_code` (process becomes zombie when tool-call shell exits) |
| `ModuleNotFoundError: tools.*` in Docker | MCP tools are host-only — use `yfinance` directly inside Docker containers |

## Quality Checklist

Before calling `GetPreviewUrl`:

**Data & Code**
- [ ] All data fetched and validated (no empty dataframes or None values)
- [ ] Files written to `work/<task>/` directory
- [ ] JSON data properly escaped with `json.dumps()`
- [ ] All chart containers exist in HTML before JS tries to reference them

**CSP Safety**
- [ ] No inline event handlers (`onclick`, `oninput`, `onchange`, etc.) — all events via `addEventListener`
- [ ] No `eval()`, `new Function()`, or string-based `setTimeout()`
- [ ] No `javascript:` URLs
- [ ] CSP self-check grep passes (no matches)

**Verification**
- [ ] Tier 1: JS syntax check passed (`node --check` on extracted script blocks)
- [ ] Tier 2: Playwright verification passed (for interactive dashboards with buttons/filters/tabs)

**Serving**
- [ ] Server binds to `0.0.0.0` (not `127.0.0.1` or `localhost`)
- [ ] Correct port used (default 8050)
- [ ] Command passed to `GetPreviewUrl` is idempotent (works on re-run after restart)
- [ ] Complex tier: `start.sh` and `Dockerfile` copied from templates
- [ ] Complex tier: FastAPI includes `HEAD /` endpoint (use `server-main.py` template)

**UI Quality**
- [ ] Theme foundation block included verbatim; only bridge variables (and `pick()` for canvas colors) used — no palette hexes in styles
- [ ] Renders correctly in **both** light and dark
- [ ] Responsive layout — no horizontal scroll
- [ ] Financial numbers properly formatted (currency, %, abbreviations)
- [ ] Title passed to `GetPreviewUrl` is descriptive (e.g., "AAPL Stock Dashboard", not "Preview")
