# UI Component Patterns

Reusable CSS and component patterns for financial dashboards. Copy-paste ready.

---

## Simple Tier (Vanilla HTML / CSS / JS)

### 1. Base Dark Theme

Complete CSS foundation. Include this in every dashboard `<style>` block.

```css
/* === Dark Theme Foundation === */
:root {
  --bg-page: #0f1117;
  --bg-card: #1a1d27;
  --bg-hover: #252a36;
  --text-primary: #e5e7eb;
  --text-secondary: #9ca3af;
  --accent: #3b82f6;
  --positive: #10b981;
  --negative: #ef4444;
  --border: #2d3748;
}

/* Reset */
*, *::before, *::after {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg-page);
  color: var(--text-primary);
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

a {
  color: var(--accent);
  text-decoration: none;
}

a:hover {
  text-decoration: underline;
}

/* Container utility */
.container {
  max-width: 1400px;
  margin: 0 auto;
  padding: 1.5rem;
}

/* Page header */
.page-header {
  margin-bottom: 1.5rem;
}

.page-header h1 {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--text-primary);
}

.page-header .subtitle {
  font-size: 0.875rem;
  color: var(--text-secondary);
  margin-top: 0.25rem;
}
```

---

### 2. KPI Card Row

Responsive row of metric cards for key figures (price, change, volume, market cap, etc.).

```html
<div class="kpi-row">
  <div class="kpi-card">
    <span class="kpi-label">Current Price</span>
    <span class="kpi-value">$182.52</span>
    <span class="kpi-change positive">+2.34%</span>
  </div>
  <div class="kpi-card">
    <span class="kpi-label">Market Cap</span>
    <span class="kpi-value">$2.87T</span>
    <span class="kpi-change neutral">-</span>
  </div>
  <div class="kpi-card">
    <span class="kpi-label">P/E Ratio</span>
    <span class="kpi-value">28.4x</span>
    <span class="kpi-change negative">-1.2x</span>
  </div>
  <div class="kpi-card">
    <span class="kpi-label">Volume</span>
    <span class="kpi-value">54.3M</span>
    <span class="kpi-change positive">+12.5%</span>
  </div>
</div>
```

```css
/* === KPI Card Row === */
.kpi-row {
  display: flex;
  gap: 1rem;
  margin-bottom: 1.5rem;
  flex-wrap: wrap;
}

.kpi-card {
  flex: 1 1 180px;
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1rem 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  transition: background 0.15s ease;
}

.kpi-card:hover {
  background: var(--bg-hover);
}

.kpi-label {
  font-size: 0.75rem;
  color: var(--text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.kpi-value {
  font-size: 1.75rem;
  font-weight: 700;
  color: var(--text-primary);
  line-height: 1.2;
}

.kpi-change {
  font-size: 0.8125rem;
  font-weight: 600;
}

.kpi-change.positive { color: var(--positive); }
.kpi-change.negative { color: var(--negative); }
.kpi-change.neutral  { color: var(--text-secondary); }
```

---

### 3. Responsive Chart Grid

CSS Grid layout for chart cards. Automatically adjusts columns based on available width.

```html
<div class="chart-grid">
  <div class="chart-card">
    <h2 class="chart-title">Price History (1Y)</h2>
    <div class="chart-container" id="priceChart"></div>
    <p class="chart-caption">Source: Yahoo Finance</p>
  </div>
  <div class="chart-card">
    <h2 class="chart-title">Revenue by Segment</h2>
    <div class="chart-container" id="revenueChart"></div>
    <p class="chart-caption">Source: Company filings (FY2025)</p>
  </div>
  <div class="chart-card full-width">
    <h2 class="chart-title">Earnings History vs. Estimates</h2>
    <div class="chart-container" id="earningsChart"></div>
  </div>
</div>
```

```css
/* === Responsive Chart Grid === */
.chart-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.chart-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1.25rem;
}

/* Span full width when needed */
.chart-card.full-width {
  grid-column: 1 / -1;
}

.chart-title {
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 1rem;
}

.chart-container {
  width: 100%;
  height: 300px;
  position: relative;
}

.chart-caption {
  font-size: 0.75rem;
  color: var(--text-secondary);
  margin-top: 0.75rem;
  text-align: right;
}

/* On narrow screens, allow chart cards to go full width */
@media (max-width: 480px) {
  .chart-grid {
    grid-template-columns: 1fr;
  }
  .chart-container {
    height: 250px;
  }
}
```

---

### 4. Styled Financial Table

Data table with sticky header, alternating rows, right-aligned numbers, and positive/negative coloring.

```html
<div class="table-wrapper">
  <table class="fin-table">
    <thead>
      <tr>
        <th class="text-left">Ticker</th>
        <th class="text-left">Company</th>
        <th class="text-right">Price</th>
        <th class="text-right">Change</th>
        <th class="text-right">% Change</th>
        <th class="text-right">Market Cap</th>
        <th class="text-right">P/E</th>
        <th class="text-right">Volume</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td class="text-left ticker">AAPL</td>
        <td class="text-left">Apple Inc.</td>
        <td class="text-right">$182.52</td>
        <td class="text-right positive">+$4.18</td>
        <td class="text-right positive">+2.34%</td>
        <td class="text-right">$2.87T</td>
        <td class="text-right">28.4x</td>
        <td class="text-right">54.3M</td>
      </tr>
      <tr>
        <td class="text-left ticker">MSFT</td>
        <td class="text-left">Microsoft Corp.</td>
        <td class="text-right">$415.30</td>
        <td class="text-right negative">-$2.70</td>
        <td class="text-right negative">-0.65%</td>
        <td class="text-right">$3.09T</td>
        <td class="text-right">35.1x</td>
        <td class="text-right">22.1M</td>
      </tr>
    </tbody>
  </table>
</div>
```

```css
/* === Financial Table === */
.table-wrapper {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  overflow-x: auto;
  margin-bottom: 1.5rem;
}

.fin-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
}

.fin-table thead {
  position: sticky;
  top: 0;
  z-index: 1;
}

.fin-table th {
  background: var(--bg-hover);
  color: var(--text-secondary);
  font-weight: 600;
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}

.fin-table td {
  padding: 0.625rem 1rem;
  border-bottom: 1px solid var(--border);
  color: var(--text-primary);
  white-space: nowrap;
}

/* Alternating row backgrounds */
.fin-table tbody tr:nth-child(even) {
  background: rgba(255, 255, 255, 0.02);
}

/* Hover highlight */
.fin-table tbody tr:hover {
  background: var(--bg-hover);
}

/* Alignment helpers */
.text-left  { text-align: left; }
.text-right { text-align: right; }

/* Ticker column styling */
.fin-table .ticker {
  font-weight: 600;
  color: var(--accent);
}

/* Positive/negative cell coloring */
.fin-table .positive { color: var(--positive); }
.fin-table .negative { color: var(--negative); }
```

---

### 5. Tab Switcher

Pure CSS/JS tab component. Minimal JavaScript -- just toggles an `active` class.

```html
<div class="tabs">
  <div class="tab-bar">
    <button class="tab-btn active" data-tab="overview">Overview</button>
    <button class="tab-btn" data-tab="financials">Financials</button>
    <button class="tab-btn" data-tab="analysis">Analysis</button>
    <button class="tab-btn" data-tab="news">News</button>
  </div>
  <div class="tab-panel active" id="tab-overview">
    <!-- Overview content -->
  </div>
  <div class="tab-panel" id="tab-financials">
    <!-- Financials content -->
  </div>
  <div class="tab-panel" id="tab-analysis">
    <!-- Analysis content -->
  </div>
  <div class="tab-panel" id="tab-news">
    <!-- News content -->
  </div>
</div>
```

```css
/* === Tab Switcher === */
.tab-bar {
  display: flex;
  gap: 0;
  border-bottom: 1px solid var(--border);
  margin-bottom: 1.5rem;
  overflow-x: auto;
}

.tab-btn {
  background: none;
  border: none;
  color: var(--text-secondary);
  font-family: inherit;
  font-size: 0.875rem;
  font-weight: 500;
  padding: 0.75rem 1.25rem;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  transition: color 0.15s ease, border-color 0.15s ease;
  white-space: nowrap;
}

.tab-btn:hover {
  color: var(--text-primary);
}

.tab-btn.active {
  color: var(--accent);
  border-bottom-color: var(--accent);
}

.tab-panel {
  display: none;
}

.tab-panel.active {
  display: block;
}
```

```javascript
/* === Tab Switcher JS === */
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tabId = btn.dataset.tab;

    // Update buttons
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    // Update panels
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.getElementById('tab-' + tabId).classList.add('active');
  });
});
```

---

### 6. Loading Spinner

CSS-only spinner with semi-transparent backdrop. Show while data loads, hide when ready.

```html
<div class="loading-overlay" id="loadingOverlay">
  <div class="spinner"></div>
  <p class="loading-text">Loading data...</p>
</div>
```

```css
/* === Loading Spinner === */
.loading-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(15, 17, 23, 0.85);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  transition: opacity 0.3s ease;
}

.loading-overlay.hidden {
  opacity: 0;
  pointer-events: none;
}

.spinner {
  width: 40px;
  height: 40px;
  border: 3px solid var(--border);
  border-top-color: var(--accent);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.loading-text {
  margin-top: 1rem;
  font-size: 0.875rem;
  color: var(--text-secondary);
}
```

```javascript
/* === Loading Overlay Helpers === */
function showLoading(message) {
  const overlay = document.getElementById('loadingOverlay');
  if (message) overlay.querySelector('.loading-text').textContent = message;
  overlay.classList.remove('hidden');
}

function hideLoading() {
  document.getElementById('loadingOverlay').classList.add('hidden');
}

// Usage: call hideLoading() after all charts are rendered
// document.addEventListener('DOMContentLoaded', () => { ... hideLoading(); });
```

---

### 7. Number Formatting Utilities

JavaScript helper functions for financial number display.

```javascript
/* === Number Formatting Utilities === */

/**
 * Format as currency: $1,234.56
 * @param {number} value
 * @param {number} [decimals=2]
 * @returns {string}
 */
function formatCurrency(value, decimals = 2) {
  if (value == null || isNaN(value)) return '-';
  return '$' + Math.abs(value).toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

/**
 * Format as percentage with sign: +2.34% or -1.56%
 * @param {number} value - Already in percent (e.g., 2.34 not 0.0234)
 * @param {number} [decimals=2]
 * @returns {string}
 */
function formatPercent(value, decimals = 2) {
  if (value == null || isNaN(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return sign + value.toFixed(decimals) + '%';
}

/**
 * Format large numbers with suffix: $2.87T, $142.5B, $3.2M, $45.0K
 * @param {number} value
 * @param {number} [decimals=1]
 * @returns {string}
 */
function formatLargeNumber(value, decimals = 1) {
  if (value == null || isNaN(value)) return '-';
  const abs = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  if (abs >= 1e12) return sign + '$' + (abs / 1e12).toFixed(decimals) + 'T';
  if (abs >= 1e9)  return sign + '$' + (abs / 1e9).toFixed(decimals) + 'B';
  if (abs >= 1e6)  return sign + '$' + (abs / 1e6).toFixed(decimals) + 'M';
  if (abs >= 1e3)  return sign + '$' + (abs / 1e3).toFixed(decimals) + 'K';
  return sign + '$' + abs.toFixed(decimals);
}

/**
 * Format volume: 12.3M, 542.1K, 890
 * @param {number} value
 * @returns {string}
 */
function formatVolume(value) {
  if (value == null || isNaN(value)) return '-';
  if (value >= 1e9) return (value / 1e9).toFixed(1) + 'B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + 'M';
  if (value >= 1e3) return (value / 1e3).toFixed(1) + 'K';
  return value.toLocaleString('en-US');
}

/**
 * Return CSS class name for positive/negative/neutral values.
 * @param {number} value
 * @returns {string} 'positive' | 'negative' | 'neutral'
 */
function colorForChange(value) {
  if (value == null || isNaN(value) || value === 0) return 'neutral';
  return value > 0 ? 'positive' : 'negative';
}

/**
 * Format a number as a multiple: 28.4x
 * @param {number} value
 * @param {number} [decimals=1]
 * @returns {string}
 */
function formatMultiple(value, decimals = 1) {
  if (value == null || isNaN(value)) return '-';
  return value.toFixed(decimals) + 'x';
}
```

---

## Complex Tier (React + Tailwind CSS)

### 1. Tailwind Config

Extend the default Tailwind config with financial dashboard colors. Dark theme is the default.

```javascript
// tailwind.config.js
/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx,ts,tsx}'],
  theme: {
    extend: {
      colors: {
        page:      '#0f1117',
        card:      '#1a1d27',
        hover:     '#252a36',
        border:    '#2d3748',
        accent:    '#3b82f6',
        positive:  '#10b981',
        negative:  '#ef4444',
        'text-primary':   '#e5e7eb',
        'text-secondary': '#9ca3af',
      },
      fontFamily: {
        sans: [
          '-apple-system', 'BlinkMacSystemFont', "'Segoe UI'",
          'Roboto', 'sans-serif',
        ],
      },
    },
  },
  plugins: [],
};
```

Base styles in `src/index.css`:

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

body {
  @apply bg-page text-text-primary antialiased;
}
```

---

### 2. DashboardLayout

Responsive layout wrapper with header, optional controls slot, and a CSS Grid content area.

```jsx
// src/components/DashboardLayout.jsx

export default function DashboardLayout({ title, subtitle, controls, children }) {
  return (
    <div className="max-w-[1400px] mx-auto px-4 py-6 sm:px-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-6 gap-3">
        <div>
          <h1 className="text-2xl font-bold text-text-primary">{title}</h1>
          {subtitle && (
            <p className="text-sm text-text-secondary mt-1">{subtitle}</p>
          )}
        </div>
        {controls && <div className="flex items-center gap-2">{controls}</div>}
      </div>

      {/* Content grid — single column on mobile, multi-column on desktop */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {children}
      </div>
    </div>
  );
}

/**
 * Wrapper for items that should span the full grid width.
 * Usage: <FullWidth><MyChart /></FullWidth>
 */
export function FullWidth({ children }) {
  return <div className="lg:col-span-2">{children}</div>;
}
```

Usage:

```jsx
<DashboardLayout
  title="AAPL Stock Dashboard"
  subtitle="Apple Inc. — Last updated Mar 20, 2026"
  controls={<PeriodSelector />}
>
  <FullWidth>
    <KPIRow kpis={kpis} />
  </FullWidth>
  <ChartCard title="Price History"><PriceChart data={priceData} /></ChartCard>
  <ChartCard title="Revenue by Segment"><RevenueChart data={revData} /></ChartCard>
  <FullWidth>
    <FinancialTable columns={cols} data={rows} />
  </FullWidth>
</DashboardLayout>
```

---

### 3. KPICard

Individual metric card with optional loading skeleton and change indicator.

```jsx
// src/components/KPICard.jsx

/**
 * KPICard — a single key performance indicator.
 *
 * Props:
 *   label    {string}  — metric label (e.g., "Market Cap")
 *   value    {string}  — formatted display value (e.g., "$2.87T")
 *   change   {string}  — formatted change text (e.g., "+2.34%"), optional
 *   prefix   {string}  — prepend to value (e.g., "$"), optional
 *   suffix   {string}  — append to value (e.g., "x"), optional
 *   loading  {boolean} — show skeleton placeholder, optional
 *   changeDirection {'positive'|'negative'|'neutral'} — override color, optional
 */
export default function KPICard({
  label,
  value,
  change,
  prefix = '',
  suffix = '',
  loading = false,
  changeDirection,
}) {
  // Auto-detect direction from change string if not explicitly provided
  const direction =
    changeDirection ??
    (change
      ? change.startsWith('+') ? 'positive'
      : change.startsWith('-') ? 'negative'
      : 'neutral'
    : 'neutral');

  const changeColor = {
    positive: 'text-positive',
    negative: 'text-negative',
    neutral:  'text-text-secondary',
  }[direction];

  if (loading) {
    return (
      <div className="flex-1 min-w-[180px] bg-card border border-border rounded-lg p-4 animate-pulse">
        <div className="h-3 w-20 bg-border rounded mb-3" />
        <div className="h-7 w-28 bg-border rounded mb-2" />
        <div className="h-3 w-16 bg-border rounded" />
      </div>
    );
  }

  return (
    <div className="flex-1 min-w-[180px] bg-card border border-border rounded-lg p-4 hover:bg-hover transition-colors">
      <span className="block text-xs text-text-secondary uppercase tracking-wider">
        {label}
      </span>
      <span className="block text-[1.75rem] font-bold text-text-primary leading-tight mt-1">
        {prefix}{value}{suffix}
      </span>
      {change && (
        <span className={`block text-sm font-semibold mt-0.5 ${changeColor}`}>
          {change}
        </span>
      )}
    </div>
  );
}

/**
 * KPIRow — horizontal row of KPI cards. Wraps on narrow screens.
 *
 * Props:
 *   kpis     {Array<KPICardProps>} — array of props to pass to each KPICard
 *   loading  {boolean}             — pass loading state to all cards
 */
export function KPIRow({ kpis, loading = false }) {
  return (
    <div className="flex flex-wrap gap-4 mb-6">
      {kpis.map((kpi, i) => (
        <KPICard key={kpi.label || i} {...kpi} loading={loading} />
      ))}
    </div>
  );
}
```

Usage:

```jsx
const kpis = [
  { label: 'Current Price', value: '182.52', prefix: '$', change: '+2.34%' },
  { label: 'Market Cap', value: '2.87T', prefix: '$' },
  { label: 'P/E Ratio', value: '28.4', suffix: 'x', change: '-1.2x', changeDirection: 'negative' },
  { label: 'Volume', value: '54.3M', change: '+12.5%' },
];

<KPIRow kpis={kpis} loading={isLoading} />
```

---

### 4. FinancialTable

Sortable, paginated data table for financial data.

```jsx
// src/components/FinancialTable.jsx
import { useState, useMemo } from 'react';

/**
 * Column definition:
 *   key      {string}                  — data field key
 *   label    {string}                  — display header text
 *   align    {'left'|'right'}          — text alignment (default: 'left')
 *   format   {(value, row) => string}  — custom formatter, optional
 *   sortable {boolean}                 — enable sorting on this column, optional
 */

const PAGE_SIZE_OPTIONS = [10, 25, 50];

export default function FinancialTable({
  columns,
  data,
  sortable = true,
  paginated = false,
  pageSize: initialPageSize = 10,
}) {
  const [sortKey, setSortKey] = useState(null);
  const [sortDir, setSortDir] = useState('asc');
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(initialPageSize);

  // Sorting
  const sortedData = useMemo(() => {
    if (!sortKey) return data;
    return [...data].sort((a, b) => {
      const aVal = a[sortKey];
      const bVal = b[sortKey];
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      const cmp = typeof aVal === 'number' ? aVal - bVal : String(aVal).localeCompare(String(bVal));
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [data, sortKey, sortDir]);

  // Pagination
  const totalPages = paginated ? Math.ceil(sortedData.length / pageSize) : 1;
  const pageData = paginated
    ? sortedData.slice(page * pageSize, (page + 1) * pageSize)
    : sortedData;

  function handleSort(key) {
    if (!sortable) return;
    const col = columns.find(c => c.key === key);
    if (col && col.sortable === false) return;
    if (sortKey === key) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  }

  function renderSortArrow(key) {
    if (sortKey !== key) return <span className="ml-1 text-border">&#8597;</span>;
    return (
      <span className="ml-1 text-accent">
        {sortDir === 'asc' ? '\u25B2' : '\u25BC'}
      </span>
    );
  }

  function renderCell(row, col) {
    const raw = row[col.key];
    const display = col.format ? col.format(raw, row) : raw;
    // Apply positive/negative coloring for numeric values that look like changes
    const isChangeCol = typeof raw === 'number' && col.colorize;
    const colorClass = isChangeCol
      ? raw > 0 ? 'text-positive' : raw < 0 ? 'text-negative' : ''
      : '';
    return (
      <td
        key={col.key}
        className={`px-4 py-2.5 whitespace-nowrap border-b border-border ${
          col.align === 'right' ? 'text-right' : 'text-left'
        } ${colorClass}`}
      >
        {display ?? '-'}
      </td>
    );
  }

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr>
              {columns.map(col => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={`px-4 py-3 bg-hover text-text-secondary text-xs font-semibold uppercase tracking-wider border-b border-border whitespace-nowrap ${
                    col.align === 'right' ? 'text-right' : 'text-left'
                  } ${sortable && col.sortable !== false ? 'cursor-pointer select-none hover:text-text-primary' : ''}`}
                >
                  {col.label}
                  {sortable && col.sortable !== false && renderSortArrow(col.key)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageData.map((row, i) => (
              <tr
                key={i}
                className={`hover:bg-hover transition-colors ${
                  i % 2 === 1 ? 'bg-white/[0.02]' : ''
                }`}
              >
                {columns.map(col => renderCell(row, col))}
              </tr>
            ))}
            {pageData.length === 0 && (
              <tr>
                <td
                  colSpan={columns.length}
                  className="px-4 py-8 text-center text-text-secondary"
                >
                  No data available
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination controls */}
      {paginated && totalPages > 1 && (
        <div className="flex items-center justify-between px-4 py-3 border-t border-border text-sm text-text-secondary">
          <div className="flex items-center gap-2">
            <span>Rows per page:</span>
            <select
              value={pageSize}
              onChange={e => { setPageSize(Number(e.target.value)); setPage(0); }}
              className="bg-hover border border-border rounded px-2 py-1 text-text-primary text-sm"
            >
              {PAGE_SIZE_OPTIONS.map(n => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </div>
          <div className="flex items-center gap-3">
            <span>
              {page * pageSize + 1}-{Math.min((page + 1) * pageSize, sortedData.length)} of {sortedData.length}
            </span>
            <button
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0}
              className="px-2 py-1 rounded border border-border hover:bg-hover disabled:opacity-30 disabled:cursor-not-allowed"
            >
              Prev
            </button>
            <button
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              className="px-2 py-1 rounded border border-border hover:bg-hover disabled:opacity-30 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
```

Usage:

```jsx
const columns = [
  { key: 'ticker', label: 'Ticker', align: 'left' },
  { key: 'name', label: 'Company', align: 'left' },
  { key: 'price', label: 'Price', align: 'right', format: v => `$${v.toFixed(2)}` },
  { key: 'changePct', label: '% Change', align: 'right', colorize: true,
    format: v => (v > 0 ? '+' : '') + v.toFixed(2) + '%' },
  { key: 'marketCap', label: 'Mkt Cap', align: 'right',
    format: v => v >= 1e12 ? `$${(v/1e12).toFixed(1)}T` : `$${(v/1e9).toFixed(1)}B` },
  { key: 'pe', label: 'P/E', align: 'right', format: v => v ? v.toFixed(1) + 'x' : '-' },
];

<FinancialTable columns={columns} data={stocks} sortable paginated pageSize={25} />
```

---

### 5. TabNav

Tab navigation component with underline active indicator.

```jsx
// src/components/TabNav.jsx

/**
 * TabNav — horizontal tab navigation with underline indicator.
 *
 * Props:
 *   tabs       {Array<{key: string, label: string}>}
 *   activeTab  {string}     — currently active tab key
 *   onChange   {(key) => void}
 */
export default function TabNav({ tabs, activeTab, onChange }) {
  return (
    <div className="flex border-b border-border mb-6 overflow-x-auto">
      {tabs.map(tab => (
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          className={`px-5 py-3 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${
            activeTab === tab.key
              ? 'text-accent border-accent'
              : 'text-text-secondary border-transparent hover:text-text-primary'
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
```

Usage with local state:

```jsx
import { useState } from 'react';
import TabNav from './components/TabNav';

function App() {
  const [activeTab, setActiveTab] = useState('overview');

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'financials', label: 'Financials' },
    { key: 'analysis', label: 'Analysis' },
    { key: 'news', label: 'News' },
  ];

  return (
    <div>
      <TabNav tabs={tabs} activeTab={activeTab} onChange={setActiveTab} />
      {activeTab === 'overview' && <OverviewPanel />}
      {activeTab === 'financials' && <FinancialsPanel />}
      {activeTab === 'analysis' && <AnalysisPanel />}
      {activeTab === 'news' && <NewsPanel />}
    </div>
  );
}
```

Usage with URL search params (optional):

```jsx
import { useSearchParams } from 'react-router-dom';
import TabNav from './components/TabNav';

function App() {
  const [params, setParams] = useSearchParams();
  const activeTab = params.get('tab') || 'overview';

  return (
    <TabNav
      tabs={tabs}
      activeTab={activeTab}
      onChange={key => setParams({ tab: key })}
    />
  );
}
```

---

### Shared React Utility: ChartCard

Reusable wrapper for chart components in the React tier.

```jsx
// src/components/ChartCard.jsx

export default function ChartCard({ title, caption, fullWidth = false, children }) {
  return (
    <div
      className={`bg-card border border-border rounded-lg p-5 ${
        fullWidth ? 'lg:col-span-2' : ''
      }`}
    >
      {title && (
        <h2 className="text-lg font-semibold text-text-primary mb-4">{title}</h2>
      )}
      <div className="w-full h-[300px] relative">
        {children}
      </div>
      {caption && (
        <p className="text-xs text-text-secondary mt-3 text-right">{caption}</p>
      )}
    </div>
  );
}
```

### Shared React Utility: Number Formatters

TypeScript-compatible utility module for the React tier.

```javascript
// src/utils/formatters.js

export function formatCurrency(value, decimals = 2) {
  if (value == null || isNaN(value)) return '-';
  return '$' + Math.abs(value).toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export function formatPercent(value, decimals = 2) {
  if (value == null || isNaN(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return sign + value.toFixed(decimals) + '%';
}

export function formatLargeNumber(value, decimals = 1) {
  if (value == null || isNaN(value)) return '-';
  const abs = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  if (abs >= 1e12) return sign + '$' + (abs / 1e12).toFixed(decimals) + 'T';
  if (abs >= 1e9)  return sign + '$' + (abs / 1e9).toFixed(decimals) + 'B';
  if (abs >= 1e6)  return sign + '$' + (abs / 1e6).toFixed(decimals) + 'M';
  if (abs >= 1e3)  return sign + '$' + (abs / 1e3).toFixed(decimals) + 'K';
  return sign + '$' + abs.toFixed(decimals);
}

export function formatVolume(value) {
  if (value == null || isNaN(value)) return '-';
  if (value >= 1e9) return (value / 1e9).toFixed(1) + 'B';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + 'M';
  if (value >= 1e3) return (value / 1e3).toFixed(1) + 'K';
  return value.toLocaleString('en-US');
}

export function formatMultiple(value, decimals = 1) {
  if (value == null || isNaN(value)) return '-';
  return value.toFixed(decimals) + 'x';
}

export function colorForChange(value) {
  if (value == null || isNaN(value) || value === 0) return 'neutral';
  return value > 0 ? 'positive' : 'negative';
}

/**
 * Map colorForChange output to Tailwind classes.
 * Usage: <span className={changeClass(value)}>{formatPercent(value)}</span>
 */
export function changeClass(value) {
  const dir = colorForChange(value);
  return {
    positive: 'text-positive',
    negative: 'text-negative',
    neutral:  'text-text-secondary',
  }[dir];
}
```
