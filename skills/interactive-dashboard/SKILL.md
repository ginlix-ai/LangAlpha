---
name: interactive-dashboard
description: "Interactive web dashboards: stock trackers, sector heatmaps, portfolio monitors — served via preview URL"
---

# Interactive Dashboard

Build interactive web dashboards inside the sandbox and expose them to the user via `GetPreviewUrl`. Use this skill for any request involving dashboards, trackers, monitors, live visualizations, or interactive web apps.

## When to Use

- User asks for a **dashboard**, **tracker**, **monitor**, or **interactive chart**
- User wants a **web app** or **live visualization** rather than a static image
- User requests something that benefits from interactivity: filtering, drill-downs, hover tooltips, tab switching
- User explicitly says "preview", "web view", or "interactive"

**Do NOT use if:** User wants a static chart image (use matplotlib/plotly `savefig` instead) or a document/report (use docx/pptx skills).

## Architecture

Choose the tier based on complexity:

| Tier | When | Stack | Serve command |
|------|------|-------|---------------|
| **Simple** | Static snapshot, few charts, no server-side logic | Self-contained HTML + CDN libs | `python -m http.server 8050` |
| **Complex** | Live refresh, filtering, multi-page, heavy interactivity | FastAPI backend + Vite/React frontend | `bash start.sh` |

**Decision rule:** Start with simple. Escalate to complex only when user needs: live data refresh, server-side filtering/pagination, multi-route navigation, or React-level component interactivity.

**Port convention:** Use port **8050** (default). Range 8050-8059 for dashboards.

### Sandbox Capabilities

All pre-installed in the Daytona sandbox snapshot — no `pip install` or `apt-get` needed:

- **Python 3.12** + pandas, numpy, plotly, matplotlib, requests, httpx, yfinance
- **FastAPI + uvicorn** (available via `fastmcp` transitive dependency)
- **Node.js 20 + npm** — scaffold Vite/React projects with `npm create vite@latest`
- **Playwright + Chromium** — available for advanced rendering

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
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        /* See references/ui-components.md for dark theme CSS */
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

**Complex tier** — scaffold a FastAPI + Vite/React project (see Project Structure section below).

### Step 5: Serve & Expose

```python
# Simple tier
GetPreviewUrl(port=8050, command="cd work/dashboard && python -m http.server 8050", title="AAPL Dashboard")

# Complex tier
GetPreviewUrl(port=8050, command="bash work/dashboard/start.sh", title="Stock Dashboard")
```

### Step 6: Iterate

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

## UI Design Rules

### Dark Theme (Default)

Match the Ginlix platform aesthetic:

| Element | Color |
|---------|-------|
| Page background | `#0f1117` |
| Card background | `#1a1d27` |
| Primary text | `#e5e7eb` |
| Secondary text | `#9ca3af` |
| Accent / links | `#3b82f6` |
| Positive / gain | `#10b981` |
| Negative / loss | `#ef4444` |
| Border | `#2d3748` |
| Hover highlight | `#252a36` |

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

## Complex Tier — Project Structure

When using FastAPI + Vite/React, scaffold this structure:

```
work/<task>/
├── server/
│   ├── main.py          # FastAPI app with CORS, /api routes
│   ├── routes/          # API route modules (stocks.py, sectors.py)
│   └── models.py        # Pydantic response models
├── frontend/
│   ├── package.json     # Vite + React + chart libraries
│   ├── vite.config.js   # Proxy /api to FastAPI, host 0.0.0.0
│   ├── index.html
│   └── src/
│       ├── App.jsx      # Main app with routing/tabs
│       ├── components/  # Chart, KPI, Table components
│       ├── hooks/       # useStockData, useSectorData, etc.
│       └── utils/       # formatters, color helpers
└── start.sh             # Starts both servers
```

### FastAPI Backend (`server/main.py`)

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/stock/{ticker}")
async def get_stock(ticker: str, period: str = "1y"):
    from tools.yf_price import get_stock_history
    from tools.yf_fundamentals import get_company_info
    return {"history": get_stock_history(ticker, period=period), "info": get_company_info(ticker)}
```

### Vite Config (`frontend/vite.config.js`)

```javascript
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 8050,
    proxy: { '/api': 'http://localhost:8051' }
  }
});
```

### Startup Script (`start.sh`)

```bash
#!/bin/bash
cd "$(dirname "$0")"
cd server && uvicorn main:app --host 0.0.0.0 --port 8051 &
cd frontend && npm install --prefer-offline && npx vite --host 0.0.0.0 --port 8050 &
wait
```

Call `GetPreviewUrl(port=8050, command="bash work/<task>/start.sh", title="Dashboard")`.

## Chart Libraries

### Simple Tier (CDN-loaded, no install)

| Library | CDN URL | Best for |
|---------|---------|----------|
| **Chart.js** | `https://cdn.jsdelivr.net/npm/chart.js` | Line, bar, pie, doughnut, area |
| **Plotly.js** | `https://cdn.plot.ly/plotly-2.35.2.min.js` | Candlestick, heatmap, treemap |
| **Lightweight Charts** | `https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js` | TradingView-style candlestick |

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
- **Proxy, not CORS**: Prefer Vite proxy config over CORS middleware when possible
- **`host: '0.0.0.0'`**: Both FastAPI and Vite must bind to `0.0.0.0`, not `127.0.0.1`

## Error Handling & Debugging

| Problem | Solution |
|---------|----------|
| `GetPreviewUrl` returns error | Port already in use — try a different port (8051, 8052, ...) |
| Page is blank | Check for JS errors — ensure all `getElementById` targets exist |
| Data is empty | Validate MCP tool response before embedding — check for `None` or empty lists |
| FastAPI won't start | Ensure `host='0.0.0.0'` in `uvicorn.run()` |
| Vite won't start | Ensure `--host 0.0.0.0` flag and check if port is free |
| CORS errors | Add `CORSMiddleware` to FastAPI or use Vite proxy |
| Charts don't render | CDN scripts must load before chart initialization — use `DOMContentLoaded` event |
| Iframe shows "refused to connect" | Server not ready yet — add a small delay or retry logic |

## Quality Checklist

Before calling `GetPreviewUrl`:

- [ ] All data fetched and validated (no empty dataframes or None values)
- [ ] Files written to `work/<task>/` directory
- [ ] JSON data properly escaped with `json.dumps()`
- [ ] All chart containers exist in HTML before JS tries to reference them
- [ ] Server binds to `0.0.0.0` (not `127.0.0.1` or `localhost`)
- [ ] Correct port used (default 8050)
- [ ] Dark theme applied consistently (see color table above)
- [ ] Responsive layout — no horizontal scroll
- [ ] Financial numbers properly formatted (currency, %, abbreviations)
- [ ] Title passed to `GetPreviewUrl` is descriptive (e.g., "AAPL Stock Dashboard", not "Preview")
