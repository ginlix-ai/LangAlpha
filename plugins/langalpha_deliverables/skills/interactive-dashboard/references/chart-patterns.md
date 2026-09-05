# Chart Patterns Reference

Ready-to-use code snippets for financial dashboard charts. Copy, paste, and replace the `DATA` placeholder with real data from MCP tools.

## Theming

All snippets pull their colors from the bridge variables — `--bg-page`, `--bg-card`,
`--bg-subtle`, `--bg-hover`, `--text-primary`, `--text-secondary`, `--border`, `--accent`,
`--positive`, `--negative`. Paste the Theme Foundation block
([ui-components.md §1](ui-components.md)) into the page's `<style>`; it wires those names to
the app's injected theme tokens, and to an OS-adaptive fallback when the page is opened
standalone. Never hardcode a palette hex.

Canvas and SVG chart code can't read `var()`, so read the resolved values once per page:

```js
var cs = getComputedStyle(document.documentElement);
function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }
```

Every snippet below includes this helper. The literal in each `pick()` call is the
no-CSS fallback — a chart rendered without the foundation block stays dark.

---

## Simple Tier (Self-Contained HTML + CDN)

Each snippet is a working HTML block — add the Theme Foundation block to its `<style>`.
Embed data via a `DATA` variable, then render.

---

### 1. Line Chart (Chart.js) -- Stock Price Over Time

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Price Chart</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>Stock Price</h2>
    <canvas id="priceChart"></canvas>
  </div>

  <script>
    // Replace with real data: { dates: ["2025-01-02", ...], prices: [182.5, ...] }
    const DATA = {
      dates: [],
      prices: []
    };

    // Canvas can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    document.addEventListener('DOMContentLoaded', () => {
      const ctx = document.getElementById('priceChart').getContext('2d');

      // Gradient fill under the line (translucent accent — kept literal so the
      // canvas gradient stays valid whatever color format the token resolves to)
      const gradient = ctx.createLinearGradient(0, 0, 0, ctx.canvas.clientHeight);
      gradient.addColorStop(0, 'rgba(59, 130, 246, 0.3)');
      gradient.addColorStop(1, 'rgba(59, 130, 246, 0.0)');

      new Chart(ctx, {
        type: 'line',
        data: {
          labels: DATA.dates,
          datasets: [{
            label: 'Close Price',
            data: DATA.prices,
            borderColor: pick('--accent', '#3b82f6'),
            backgroundColor: gradient,
            borderWidth: 2,
            pointRadius: 0,           // Hide individual points for cleaner look
            pointHitRadius: 10,       // Still allow hover detection
            fill: true,
            tension: 0.1              // Slight curve for smoother line
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          interaction: { intersect: false, mode: 'index' },
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: pick('--bg-card', '#1a1d27'),
              titleColor: pick('--text-secondary', '#9ca3af'),
              bodyColor: pick('--text-primary', '#e5e7eb'),
              borderColor: pick('--border', '#2d3748'),
              borderWidth: 1,
              padding: 12,
              displayColors: false,
              callbacks: {
                title: (items) => items[0].label,
                label: (item) => `$${item.raw.toFixed(2)}`
              }
            }
          },
          scales: {
            x: {
              ticks: { color: pick('--text-secondary', '#9ca3af'), maxTicksLimit: 8, font: { size: 11 } },
              grid: { color: pick('--border', '#2d3748') }
            },
            y: {
              ticks: {
                color: pick('--text-secondary', '#9ca3af'),
                font: { size: 11 },
                callback: (v) => '$' + v.toFixed(0)
              },
              grid: { color: pick('--border', '#2d3748') }
            }
          }
        }
      });
    });
  </script>
</body>
</html>
```

---

### 2. Multi-Line Chart (Chart.js) -- Normalized Comparison

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Stock Comparison</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>Normalized Performance (Base 100)</h2>
    <canvas id="compChart"></canvas>
  </div>

  <script>
    // Replace with real data. Each ticker has a dates array and prices array.
    // Normalization is done in JS below so you can pass raw close prices.
    const DATA = {
      dates: [],   // Shared date labels: ["2025-01-02", ...]
      tickers: [
        { name: "AAPL", prices: [] },
        { name: "MSFT", prices: [] },
        { name: "GOOGL", prices: [] }
      ]
    };

    // Canvas can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    // Palette for up to 8 tickers — first three follow the theme, rest are categorical
    const COLORS = [
      pick('--accent', '#3b82f6'), pick('--positive', '#10b981'), pick('--negative', '#ef4444'),
      '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'
    ];

    document.addEventListener('DOMContentLoaded', () => {
      const ctx = document.getElementById('compChart').getContext('2d');

      // Normalize each series to base 100 from the first data point
      const datasets = DATA.tickers.map((t, i) => {
        const base = t.prices[0] || 1;
        return {
          label: t.name,
          data: t.prices.map(p => (p / base) * 100),
          borderColor: COLORS[i % COLORS.length],
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          pointHitRadius: 10,
          tension: 0.1
        };
      });

      new Chart(ctx, {
        type: 'line',
        data: { labels: DATA.dates, datasets },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          interaction: { intersect: false, mode: 'index' },
          plugins: {
            legend: {
              labels: { color: pick('--text-primary', '#e5e7eb'), font: { size: 12 }, usePointStyle: true, pointStyle: 'line' }
            },
            tooltip: {
              backgroundColor: pick('--bg-card', '#1a1d27'),
              titleColor: pick('--text-secondary', '#9ca3af'),
              bodyColor: pick('--text-primary', '#e5e7eb'),
              borderColor: pick('--border', '#2d3748'),
              borderWidth: 1,
              padding: 12,
              callbacks: {
                label: (item) => `${item.dataset.label}: ${item.raw.toFixed(1)}`
              }
            }
          },
          scales: {
            x: {
              ticks: { color: pick('--text-secondary', '#9ca3af'), maxTicksLimit: 8, font: { size: 11 } },
              grid: { color: pick('--border', '#2d3748') }
            },
            y: {
              ticks: {
                color: pick('--text-secondary', '#9ca3af'),
                font: { size: 11 },
                callback: (v) => v.toFixed(0)
              },
              grid: { color: pick('--border', '#2d3748') }
            }
          }
        }
      });
    });
  </script>
</body>
</html>
```

---

### 3. Bar Chart (Chart.js) -- Quarterly Revenue/Earnings

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Revenue & Earnings</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>Quarterly Revenue & Net Income</h2>
    <canvas id="barChart"></canvas>
  </div>

  <script>
    // Replace with real data. Values in millions.
    const DATA = {
      quarters: ["Q1 2025", "Q2 2025", "Q3 2025", "Q4 2025"],
      revenue:  [94800, 85780, 89500, 119600],   // in millions
      earnings: [23640, 21450, 22960, 33920]      // in millions
    };

    // Format large numbers as abbreviated currency ($94.8B, $23.6B, etc.)
    function formatCurrency(value) {
      const abs = Math.abs(value);
      if (abs >= 1e6) return '$' + (value / 1e6).toFixed(1) + 'T';
      if (abs >= 1e3) return '$' + (value / 1e3).toFixed(1) + 'B';
      return '$' + value.toFixed(1) + 'M';
    }

    // Canvas can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    document.addEventListener('DOMContentLoaded', () => {
      const ctx = document.getElementById('barChart').getContext('2d');

      new Chart(ctx, {
        type: 'bar',
        data: {
          labels: DATA.quarters,
          datasets: [
            {
              label: 'Revenue',
              data: DATA.revenue,
              backgroundColor: pick('--accent', '#3b82f6'),
              borderRadius: 4,
              barPercentage: 0.7,
              categoryPercentage: 0.8
            },
            {
              label: 'Net Income',
              data: DATA.earnings,
              backgroundColor: pick('--positive', '#10b981'),
              borderRadius: 4,
              barPercentage: 0.7,
              categoryPercentage: 0.8
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          plugins: {
            legend: {
              labels: { color: pick('--text-primary', '#e5e7eb'), font: { size: 12 }, usePointStyle: true, pointStyle: 'rect' }
            },
            tooltip: {
              backgroundColor: pick('--bg-card', '#1a1d27'),
              titleColor: pick('--text-secondary', '#9ca3af'),
              bodyColor: pick('--text-primary', '#e5e7eb'),
              borderColor: pick('--border', '#2d3748'),
              borderWidth: 1,
              padding: 12,
              callbacks: {
                label: (item) => `${item.dataset.label}: ${formatCurrency(item.raw)}`
              }
            }
          },
          scales: {
            x: {
              ticks: { color: pick('--text-secondary', '#9ca3af'), font: { size: 11 } },
              grid: { display: false }
            },
            y: {
              ticks: {
                color: pick('--text-secondary', '#9ca3af'),
                font: { size: 11 },
                callback: (v) => formatCurrency(v)
              },
              grid: { color: pick('--border', '#2d3748') }
            }
          }
        }
      });
    });
  </script>
</body>
</html>
```

---

### 4. Candlestick Chart (Plotly.js) -- OHLCV with Volume Subplot

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Candlestick Chart</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
    #candlestickChart { width: 100%; height: 500px; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>OHLCV Chart</h2>
    <div id="candlestickChart"></div>
  </div>

  <script>
    // Replace with real OHLCV data arrays
    const DATA = {
      dates:  [],   // ["2025-01-02", ...]
      open:   [],   // [180.5, ...]
      high:   [],   // [183.2, ...]
      low:    [],   // [179.1, ...]
      close:  [],   // [182.7, ...]
      volume: []    // [45000000, ...]
    };

    // SVG can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    document.addEventListener('DOMContentLoaded', () => {
      // Color volume bars by price direction (translucent, so kept as literals)
      const volumeColors = DATA.close.map((c, i) =>
        c >= DATA.open[i] ? 'rgba(16, 185, 129, 0.5)' : 'rgba(239, 68, 68, 0.5)'
      );

      const candlestick = {
        x: DATA.dates,
        open: DATA.open,
        high: DATA.high,
        low: DATA.low,
        close: DATA.close,
        type: 'candlestick',
        xaxis: 'x',
        yaxis: 'y',
        increasing: { line: { color: pick('--positive', '#10b981') }, fillcolor: pick('--positive', '#10b981') },
        decreasing: { line: { color: pick('--negative', '#ef4444') }, fillcolor: pick('--negative', '#ef4444') },
        name: 'Price'
      };

      const volume = {
        x: DATA.dates,
        y: DATA.volume,
        type: 'bar',
        xaxis: 'x',
        yaxis: 'y2',
        marker: { color: volumeColors },
        name: 'Volume',
        showlegend: false
      };

      const layout = {
        paper_bgcolor: pick('--bg-card', '#1a1d27'),
        plot_bgcolor: pick('--bg-card', '#1a1d27'),
        font: { color: pick('--text-primary', '#e5e7eb'), family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif" },
        margin: { l: 60, r: 20, t: 10, b: 40 },
        showlegend: false,
        // Two vertically stacked subplots: candlestick (top 70%) and volume (bottom 30%)
        grid: { rows: 2, columns: 1, subplots: [['xy'], ['xy2']], roworder: 'top to bottom' },
        xaxis: {
          gridcolor: pick('--border', '#2d3748'),
          rangeslider: { visible: false },
          // Range selector buttons for common time windows
          rangeselector: {
            buttons: [
              { count: 1, label: '1M', step: 'month', stepmode: 'backward' },
              { count: 3, label: '3M', step: 'month', stepmode: 'backward' },
              { count: 6, label: '6M', step: 'month', stepmode: 'backward' },
              { step: 'ytd', label: 'YTD' },
              { count: 1, label: '1Y', step: 'year', stepmode: 'backward' }
            ],
            font: { color: pick('--text-primary', '#e5e7eb') },
            bgcolor: pick('--bg-hover', '#2d3748'),
            activecolor: pick('--accent', '#3b82f6')
          }
        },
        yaxis: {
          domain: [0.3, 1],       // Top 70% of chart area
          gridcolor: pick('--border', '#2d3748'),
          tickformat: '$.0f'
        },
        yaxis2: {
          domain: [0, 0.25],      // Bottom 25% of chart area
          gridcolor: pick('--border', '#2d3748'),
          tickformat: '.2s'       // SI abbreviation (45M, 120M)
        }
      };

      Plotly.newPlot('candlestickChart', [candlestick, volume], layout, {
        responsive: true,
        displayModeBar: false     // Cleaner look without toolbar
      });
    });
  </script>
</body>
</html>
```

---

### 5. Candlestick Chart (Lightweight Charts) -- TradingView Style

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TradingView Chart</title>
  <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
    #tvChart { width: 100%; height: 450px; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>Price Chart</h2>
    <div id="tvChart"></div>
  </div>

  <script>
    // Replace with real data. time must be 'YYYY-MM-DD' strings or Unix timestamps.
    const DATA = {
      candles: [
        // { time: '2025-01-02', open: 180.5, high: 183.2, low: 179.1, close: 182.7 }
      ],
      volumes: [
        // { time: '2025-01-02', value: 45000000, color: 'rgba(16, 185, 129, 0.4)' }
      ]
    };

    // Canvas can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    document.addEventListener('DOMContentLoaded', () => {
      const container = document.getElementById('tvChart');

      const chart = LightweightCharts.createChart(container, {
        layout: {
          background: { type: 'solid', color: pick('--bg-card', '#1a1d27') },
          textColor: pick('--text-secondary', '#9ca3af')
        },
        grid: {
          vertLines: { color: pick('--border', '#2d3748') },
          horzLines: { color: pick('--border', '#2d3748') }
        },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        rightPriceScale: { borderColor: pick('--border', '#2d3748') },
        timeScale: {
          borderColor: pick('--border', '#2d3748'),
          timeVisible: false         // Show only dates, not intraday times
        }
      });

      // Candlestick series
      const up = pick('--positive', '#10b981'), down = pick('--negative', '#ef4444');
      const candleSeries = chart.addCandlestickSeries({
        upColor: up,
        downColor: down,
        borderUpColor: up,
        borderDownColor: down,
        wickUpColor: up,
        wickDownColor: down
      });
      candleSeries.setData(DATA.candles);

      // Volume histogram overlay at bottom of chart
      const volumeSeries = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: '',            // Overlay on same pane
        scaleMargins: {
          top: 0.8,                  // Volume bars fill only bottom 20%
          bottom: 0
        }
      });

      // Color volume bars by candle direction
      const volumeData = DATA.volumes.length > 0
        ? DATA.volumes
        : DATA.candles.map(c => ({
            time: c.time,
            value: 0,
            color: c.close >= c.open ? 'rgba(16, 185, 129, 0.4)' : 'rgba(239, 68, 68, 0.4)'
          }));
      volumeSeries.setData(volumeData);

      // Fit chart content and auto-resize on window resize
      chart.timeScale().fitContent();
      const resizeObserver = new ResizeObserver(() => {
        chart.applyOptions({ width: container.clientWidth });
      });
      resizeObserver.observe(container);
    });
  </script>
</body>
</html>
```

---

### 6. Treemap (Plotly.js) -- Sector/Industry Heatmap

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sector Heatmap</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
    #treemap { width: 100%; height: 500px; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>Market Heatmap</h2>
    <div id="treemap"></div>
  </div>

  <script>
    // Replace with real data. Each stock needs sector, name, ticker, market_cap, and performance.
    const DATA = {
      stocks: [
        // { sector: "Technology", name: "Apple Inc.", ticker: "AAPL", market_cap: 2870, performance: 1.25 }
      ]
    };

    // SVG can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    document.addEventListener('DOMContentLoaded', () => {
      // Build the hierarchical treemap structure: root -> sectors -> stocks
      const labels   = ['Market'];
      const parents  = [''];
      const values   = [0];
      const colors   = [0];
      const textArr  = [''];

      // Collect unique sectors
      const sectors = [...new Set(DATA.stocks.map(s => s.sector))];
      sectors.forEach(sector => {
        labels.push(sector);
        parents.push('Market');
        values.push(0);           // Plotly auto-sums children
        colors.push(0);
        textArr.push('');
      });

      // Add individual stocks under their sector
      DATA.stocks.forEach(s => {
        labels.push(s.ticker);
        parents.push(s.sector);
        values.push(s.market_cap);
        colors.push(s.performance);
        const sign = s.performance >= 0 ? '+' : '';
        textArr.push(`${s.name}<br>${s.ticker}<br>${sign}${s.performance.toFixed(2)}%`);
      });

      const trace = {
        type: 'treemap',
        labels: labels,
        parents: parents,
        values: values,
        text: textArr,
        textinfo: 'label+text',
        hovertemplate: '%{text}<extra></extra>',
        marker: {
          colors: colors,
          colorscale: [
            [0,   pick('--negative', '#ef4444')],   // Most negative = loss
            [0.5, pick('--bg-card', '#1a1d27')],    // Neutral = card background
            [1,   pick('--positive', '#10b981')]    // Most positive = profit
          ],
          cmid: 0,                // Center color scale at zero
          line: { width: 1, color: pick('--bg-page', '#0f1117') }
        },
        branchvalues: 'total',
        pathbar: { visible: false }
      };

      const layout = {
        paper_bgcolor: pick('--bg-card', '#1a1d27'),
        font: { color: pick('--text-primary', '#e5e7eb'), family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif", size: 11 },
        margin: { l: 5, r: 5, t: 5, b: 5 }
      };

      Plotly.newPlot('treemap', [trace], layout, {
        responsive: true,
        displayModeBar: false
      });
    });
  </script>
</body>
</html>
```

---

### 7. Pie/Doughnut Chart (Chart.js) -- Portfolio Allocation

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Portfolio Allocation</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .chart-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }
    .chart-wrapper { position: relative; max-width: 400px; margin: 0 auto; }
    /* Center label rendered over the doughnut hole */
    .center-label {
      position: absolute; top: 50%; left: 50%;
      transform: translate(-50%, -50%);
      text-align: center; pointer-events: none;
    }
    .center-label .amount { color: var(--text-primary); font-size: 1.5rem; font-weight: 700; }
    .center-label .sub { color: var(--text-secondary); font-size: 0.75rem; }
  </style>
</head>
<body>
  <div class="chart-container">
    <h2>Portfolio Allocation</h2>
    <div class="chart-wrapper">
      <canvas id="doughnutChart"></canvas>
      <div class="center-label">
        <div class="amount" id="totalValue"></div>
        <div class="sub">Total Value</div>
      </div>
    </div>
  </div>

  <script>
    // Replace with real portfolio data
    const DATA = {
      holdings: [
        // { label: "AAPL", value: 45200, pct: 32.1 }
      ]
    };

    // Canvas can't read var() — resolve theme colors once, dark literal as fallback
    var cs = getComputedStyle(document.documentElement);
    function pick(name, fallback) { return cs.getPropertyValue(name).trim() || fallback; }

    const COLORS = [
      pick('--accent', '#3b82f6'), pick('--positive', '#10b981'), '#f59e0b',
      pick('--negative', '#ef4444'), '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'
    ];

    function formatUSD(n) {
      if (n >= 1e9) return '$' + (n / 1e9).toFixed(2) + 'B';
      if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
      if (n >= 1e3) return '$' + (n / 1e3).toFixed(1) + 'K';
      return '$' + n.toFixed(2);
    }

    document.addEventListener('DOMContentLoaded', () => {
      const total = DATA.holdings.reduce((s, h) => s + h.value, 0);
      document.getElementById('totalValue').textContent = formatUSD(total);

      const ctx = document.getElementById('doughnutChart').getContext('2d');
      new Chart(ctx, {
        type: 'doughnut',
        data: {
          labels: DATA.holdings.map(h => h.label),
          datasets: [{
            data: DATA.holdings.map(h => h.value),
            backgroundColor: COLORS.slice(0, DATA.holdings.length),
            borderColor: pick('--bg-card', '#1a1d27'),  // Matches card bg for clean segment gaps
            borderWidth: 2
          }]
        },
        options: {
          responsive: true,
          cutout: '65%',             // Size of the doughnut hole
          plugins: {
            legend: {
              position: 'bottom',
              labels: {
                color: pick('--text-primary', '#e5e7eb'),
                font: { size: 12 },
                usePointStyle: true,
                pointStyle: 'circle',
                padding: 16
              }
            },
            tooltip: {
              backgroundColor: pick('--bg-card', '#1a1d27'),
              titleColor: pick('--text-secondary', '#9ca3af'),
              bodyColor: pick('--text-primary', '#e5e7eb'),
              borderColor: pick('--border', '#2d3748'),
              borderWidth: 1,
              padding: 12,
              callbacks: {
                label: (item) => {
                  const h = DATA.holdings[item.dataIndex];
                  return `${h.label}: ${formatUSD(h.value)} (${h.pct.toFixed(1)}%)`;
                }
              }
            }
          }
        }
      });
    });
  </script>
</body>
</html>
```

---

### 8. KPI Cards -- HTML/CSS Key Metrics Row

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KPI Cards</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }

    .kpi-row {
      display: grid;
      /* Responsive: auto-fit cards with min 160px width */
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      padding: 20px;
    }

    .kpi-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px;
    }

    .kpi-label {
      font-size: 0.75rem;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 6px;
    }

    .kpi-value {
      font-size: 1.75rem;
      font-weight: 700;
      color: var(--text-primary);
      line-height: 1.2;
    }

    .kpi-change {
      font-size: 0.8rem;
      font-weight: 500;
      margin-top: 4px;
    }

    .kpi-change.positive { color: var(--positive); }
    .kpi-change.negative { color: var(--negative); }
    .kpi-change.neutral  { color: var(--text-secondary); }
  </style>
</head>
<body>
  <div class="kpi-row" id="kpiRow"></div>

  <script>
    // Replace with real KPI data
    const DATA = {
      cards: [
        { label: "Price",       value: "$182.52", change: "+1.34%",  direction: "positive" },
        { label: "Market Cap",  value: "$2.87T",  change: "+1.34%",  direction: "positive" },
        { label: "Volume",      value: "48.2M",   change: "-12.5%",  direction: "negative" },
        { label: "P/E Ratio",   value: "28.4x",   change: null,      direction: "neutral"  },
        { label: "52W High",    value: "$199.62",  change: "-8.57%",  direction: "negative" }
      ]
    };

    document.addEventListener('DOMContentLoaded', () => {
      const row = document.getElementById('kpiRow');
      DATA.cards.forEach(card => {
        const changeHTML = card.change
          ? `<div class="kpi-change ${card.direction}">${card.change}</div>`
          : '';
        row.innerHTML += `
          <div class="kpi-card">
            <div class="kpi-label">${card.label}</div>
            <div class="kpi-value">${card.value}</div>
            ${changeHTML}
          </div>
        `;
      });
    });
  </script>
</body>
</html>
```

---

### 9. Data Table -- Sortable, Styled Financial Table

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Financial Table</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    /* + Theme Foundation :root block — see ui-components.md §1 */
    body { background: var(--bg-page); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .table-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin: 20px; overflow-x: auto; }
    h2 { color: var(--text-primary); font-size: 1.125rem; margin-bottom: 16px; }

    table { width: 100%; border-collapse: collapse; }

    th {
      text-align: left;
      padding: 10px 12px;
      font-size: 0.75rem;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }

    /* Sort indicator arrow */
    th .sort-arrow { font-size: 0.65rem; margin-left: 4px; opacity: 0.5; }
    th.sorted-asc .sort-arrow::after { content: ' \u25B2'; opacity: 1; }
    th.sorted-desc .sort-arrow::after { content: ' \u25BC'; opacity: 1; }

    /* Right-align numeric columns */
    th.num, td.num { text-align: right; }

    td {
      padding: 10px 12px;
      font-size: 0.875rem;
      color: var(--text-primary);
      border-bottom: 1px solid var(--border);
    }

    /* Alternating row backgrounds */
    tbody tr:nth-child(even) { background: var(--bg-subtle); }
    tbody tr:hover { background: var(--bg-hover); }

    .positive { color: var(--positive); }
    .negative { color: var(--negative); }
  </style>
</head>
<body>
  <div class="table-container">
    <h2>Stock Comparison</h2>
    <table id="dataTable">
      <thead><tr id="headerRow"></tr></thead>
      <tbody id="tableBody"></tbody>
    </table>
  </div>

  <script>
    // Replace with real data. 'type' controls formatting/alignment: "string", "number", "currency", "percent".
    const DATA = {
      columns: [
        { key: "ticker",   label: "Ticker",    type: "string"  },
        { key: "name",     label: "Company",   type: "string"  },
        { key: "price",    label: "Price",     type: "currency" },
        { key: "change",   label: "Change %",  type: "percent" },
        { key: "mktcap",   label: "Mkt Cap",   type: "currency" },
        { key: "pe",       label: "P/E",       type: "number"  },
        { key: "volume",   label: "Volume",    type: "number"  }
      ],
      rows: [
        // { ticker: "AAPL", name: "Apple Inc.", price: 182.52, change: 1.34, mktcap: 2870000, pe: 28.4, volume: 48200000 }
      ]
    };

    let currentSort = { key: null, asc: true };

    function formatCell(value, type) {
      if (value == null) return '-';
      switch (type) {
        case 'currency': {
          const abs = Math.abs(value);
          if (abs >= 1e9)  return '$' + (value / 1e9).toFixed(2) + 'B';
          if (abs >= 1e6)  return '$' + (value / 1e6).toFixed(2) + 'M';
          if (abs >= 1e3)  return '$' + (value / 1e3).toFixed(1) + 'K';
          return '$' + value.toFixed(2);
        }
        case 'percent': {
          const sign = value >= 0 ? '+' : '';
          const cls = value >= 0 ? 'positive' : 'negative';
          return `<span class="${cls}">${sign}${value.toFixed(2)}%</span>`;
        }
        case 'number':
          return typeof value === 'number' ? value.toLocaleString('en-US', { maximumFractionDigits: 2 }) : value;
        default:
          return String(value);
      }
    }

    function render() {
      // Build header
      const headerRow = document.getElementById('headerRow');
      headerRow.innerHTML = DATA.columns.map(col => {
        const isNum = col.type !== 'string';
        const sortClass = currentSort.key === col.key
          ? (currentSort.asc ? 'sorted-asc' : 'sorted-desc')
          : '';
        return `<th class="${isNum ? 'num' : ''} ${sortClass}" data-key="${col.key}">
          ${col.label}<span class="sort-arrow"></span>
        </th>`;
      }).join('');

      // Sort rows
      let rows = [...DATA.rows];
      if (currentSort.key) {
        rows.sort((a, b) => {
          const av = a[currentSort.key], bv = b[currentSort.key];
          if (av == null) return 1;
          if (bv == null) return -1;
          const cmp = typeof av === 'string' ? av.localeCompare(bv) : av - bv;
          return currentSort.asc ? cmp : -cmp;
        });
      }

      // Build body
      const tbody = document.getElementById('tableBody');
      tbody.innerHTML = rows.map(row =>
        '<tr>' + DATA.columns.map(col => {
          const isNum = col.type !== 'string';
          return `<td class="${isNum ? 'num' : ''}">${formatCell(row[col.key], col.type)}</td>`;
        }).join('') + '</tr>'
      ).join('');
    }

    document.addEventListener('DOMContentLoaded', () => {
      render();

      // Click-to-sort handler
      document.getElementById('headerRow').addEventListener('click', (e) => {
        const th = e.target.closest('th');
        if (!th) return;
        const key = th.dataset.key;
        if (currentSort.key === key) {
          currentSort.asc = !currentSort.asc;
        } else {
          currentSort = { key, asc: true };
        }
        render();
      });
    });
  </script>
</body>
</html>
```

---

## Complex Tier (React Components)

Each snippet is a standalone React component (JSX). Place in `frontend/src/components/` and import into `App.jsx`. All assume Vite + React project scaffolded per SKILL.md, with the Theme Foundation block in `src/index.css` — DOM styles then use `var(--…)` directly, while SVG/canvas chart props go through `pick()`.

Install chart dependencies in `package.json`:

```json
{
  "dependencies": {
    "react": "^19.0.0",
    "react-dom": "^19.0.0",
    "recharts": "^2.15.0",
    "lightweight-charts": "^4.2.0",
    "react-plotly.js": "^2.6.0",
    "plotly.js-dist-min": "^2.35.0"
  }
}
```

---

### 1. StockChart (Recharts) -- Responsive Line/Area Chart

```jsx
// components/StockChart.jsx
import { useMemo } from 'react';
import {
  ResponsiveContainer, AreaChart, Area, XAxis, YAxis,
  CartesianGrid, Tooltip
} from 'recharts';

// SVG props can't take var() — resolve theme colors once, dark literal as fallback
const cs = getComputedStyle(document.documentElement);
const pick = (name, fallback) => cs.getPropertyValue(name).trim() || fallback;

const COLORS = {
  bg: pick('--bg-card', '#1a1d27'),
  text: pick('--text-primary', '#e5e7eb'),
  textSec: pick('--text-secondary', '#9ca3af'),
  blue: pick('--accent', '#3b82f6'),
  grid: pick('--border', '#2d3748'),
  border: pick('--border', '#2d3748')
};

// Custom tooltip themed from the same tokens
function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: COLORS.bg, border: `1px solid ${COLORS.border}`,
      borderRadius: 6, padding: '10px 14px'
    }}>
      <div style={{ color: COLORS.textSec, fontSize: '0.75rem', marginBottom: 4 }}>{label}</div>
      <div style={{ color: COLORS.text, fontSize: '0.9rem', fontWeight: 600 }}>
        ${payload[0].value.toFixed(2)}
      </div>
    </div>
  );
}

/**
 * StockChart - Responsive area chart for stock price history.
 *
 * @param {Object[]} data - Array of { date: string, close: number }
 * @param {string} [color] - Line/fill color (default: blue)
 * @param {number} [height] - Chart height in px (default: 300)
 */
export default function StockChart({ data, color = COLORS.blue, height = 300 }) {
  // Memoize gradient ID so multiple instances don't collide
  const gradientId = useMemo(() => `gradient-${Math.random().toString(36).slice(2, 8)}`, []);

  if (!data?.length) {
    return <div style={{ color: COLORS.textSec, padding: 20 }}>No price data available</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.3} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke={COLORS.grid} />
        <XAxis
          dataKey="date"
          tick={{ fill: COLORS.textSec, fontSize: 11 }}
          tickLine={false}
          axisLine={{ stroke: COLORS.grid }}
          interval="preserveStartEnd"       /* Show first/last labels, auto-space middle */
          minTickGap={40}
        />
        <YAxis
          tick={{ fill: COLORS.textSec, fontSize: 11 }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v) => `$${v.toFixed(0)}`}
          domain={['auto', 'auto']}
          width={55}
        />
        <Tooltip content={<CustomTooltip />} />
        <Area
          type="monotone"
          dataKey="close"
          stroke={color}
          strokeWidth={2}
          fill={`url(#${gradientId})`}
          dot={false}                        /* Hide data point dots for clean look */
          activeDot={{ r: 4, fill: color }}  /* Show dot only on hover */
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
```

---

### 2. CandlestickChart (Lightweight Charts) -- React Wrapper

```jsx
// components/CandlestickChart.jsx
import { useEffect, useRef } from 'react';
import { createChart, CrosshairMode } from 'lightweight-charts';

// Canvas can't read var() — resolve theme colors once, dark literal as fallback
const cs = getComputedStyle(document.documentElement);
const pick = (name, fallback) => cs.getPropertyValue(name).trim() || fallback;

const THEME = {
  bg: pick('--bg-card', '#1a1d27'),
  text: pick('--text-secondary', '#9ca3af'),
  border: pick('--border', '#2d3748'),
  grid: pick('--border', '#2d3748'),
  up: pick('--positive', '#10b981'),
  down: pick('--negative', '#ef4444')
};

/**
 * CandlestickChart - TradingView-style OHLCV chart with React lifecycle management.
 *
 * @param {Object[]} candles - Array of { time: 'YYYY-MM-DD', open, high, low, close }
 * @param {Object[]} volumes - Array of { time: 'YYYY-MM-DD', value: number }. If omitted, volume bars are hidden.
 * @param {number} [height] - Chart height in px (default: 400)
 */
export default function CandlestickChart({ candles, volumes, height = 400 }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);

  useEffect(() => {
    if (!containerRef.current || !candles?.length) return;

    // Create chart instance
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height,
      layout: {
        background: { type: 'solid', color: THEME.bg },
        textColor: THEME.text
      },
      grid: {
        vertLines: { color: THEME.grid },
        horzLines: { color: THEME.grid }
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: THEME.border },
      timeScale: { borderColor: THEME.border, timeVisible: false }
    });
    chartRef.current = chart;

    // Candlestick series
    const candleSeries = chart.addCandlestickSeries({
      upColor: THEME.up, downColor: THEME.down,
      borderUpColor: THEME.up, borderDownColor: THEME.down,
      wickUpColor: THEME.up, wickDownColor: THEME.down
    });
    candleSeries.setData(candles);

    // Optional volume histogram
    if (volumes?.length) {
      const volSeries = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: '',
        scaleMargins: { top: 0.8, bottom: 0 }
      });
      const volData = volumes.map((v, i) => ({
        ...v,
        color: candles[i] && candles[i].close >= candles[i].open
          ? 'rgba(16, 185, 129, 0.4)'
          : 'rgba(239, 68, 68, 0.4)'
      }));
      volSeries.setData(volData);
    }

    chart.timeScale().fitContent();

    // Auto-resize when container width changes
    const observer = new ResizeObserver(entries => {
      for (const entry of entries) {
        chart.applyOptions({ width: entry.contentRect.width });
      }
    });
    observer.observe(containerRef.current);

    // Cleanup on unmount
    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, [candles, volumes, height]);

  if (!candles?.length) {
    return <div style={{ color: 'var(--text-secondary)', padding: 20 }}>No candlestick data available</div>;
  }

  return <div ref={containerRef} style={{ width: '100%', height }} />;
}
```

---

### 3. SectorHeatmap (Plotly React) -- Treemap

```jsx
// components/SectorHeatmap.jsx
import { useMemo } from 'react';
import Plot from 'react-plotly.js';

// SVG can't read var() — resolve theme colors once, dark literal as fallback
const cs = getComputedStyle(document.documentElement);
const pick = (name, fallback) => cs.getPropertyValue(name).trim() || fallback;

/**
 * SectorHeatmap - Treemap colored by performance, sized by market cap.
 *
 * @param {Object[]} stocks - Array of { sector, name, ticker, market_cap: number, performance: number }
 * @param {number} [height] - Chart height in px (default: 500)
 */
export default function SectorHeatmap({ stocks, height = 500 }) {
  const { data, layout } = useMemo(() => {
    if (!stocks?.length) return { data: [], layout: {} };

    const labels  = ['Market'];
    const parents = [''];
    const values  = [0];
    const colors  = [0];
    const textArr = [''];

    // Add sector-level nodes
    const sectors = [...new Set(stocks.map(s => s.sector))];
    sectors.forEach(sector => {
      labels.push(sector);
      parents.push('Market');
      values.push(0);
      colors.push(0);
      textArr.push('');
    });

    // Add stock-level nodes under their sector
    stocks.forEach(s => {
      labels.push(s.ticker);
      parents.push(s.sector);
      values.push(s.market_cap);
      colors.push(s.performance);
      const sign = s.performance >= 0 ? '+' : '';
      textArr.push(`${s.name}<br>${s.ticker}<br>${sign}${s.performance.toFixed(2)}%`);
    });

    return {
      data: [{
        type: 'treemap',
        labels, parents, values, text: textArr,
        textinfo: 'label+text',
        hovertemplate: '%{text}<extra></extra>',
        marker: {
          colors,
          colorscale: [
            [0, pick('--negative', '#ef4444')],
            [0.5, pick('--bg-card', '#1a1d27')],
            [1, pick('--positive', '#10b981')]
          ],
          cmid: 0,
          line: { width: 1, color: pick('--bg-page', '#0f1117') }
        },
        branchvalues: 'total',
        pathbar: { visible: false }
      }],
      layout: {
        paper_bgcolor: pick('--bg-card', '#1a1d27'),
        font: {
          color: pick('--text-primary', '#e5e7eb'),
          family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
          size: 11
        },
        margin: { l: 5, r: 5, t: 5, b: 5 },
        height
      }
    };
  }, [stocks, height]);

  if (!stocks?.length) {
    return <div style={{ color: 'var(--text-secondary)', padding: 20 }}>No sector data available</div>;
  }

  return (
    <Plot
      data={data}
      layout={layout}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  );
}
```

---

### 4. DataTable -- React Table with Sorting

```jsx
// components/DataTable.jsx
import { useState, useMemo } from 'react';

const STYLES = {
  container: {
    background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8,
    padding: 20, overflowX: 'auto'
  },
  table: { width: '100%', borderCollapse: 'collapse' },
  th: {
    textAlign: 'left', padding: '10px 12px', fontSize: '0.75rem', color: 'var(--text-secondary)',
    textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid var(--border)',
    cursor: 'pointer', userSelect: 'none', whiteSpace: 'nowrap'
  },
  td: {
    padding: '10px 12px', fontSize: '0.875rem', color: 'var(--text-primary)',
    borderBottom: '1px solid var(--border)'
  },
  evenRow: { background: 'var(--bg-subtle)' }
};

/**
 * Format a cell value based on its column type.
 */
function formatCell(value, type) {
  if (value == null) return '-';
  switch (type) {
    case 'currency': {
      const abs = Math.abs(value);
      if (abs >= 1e9)  return '$' + (value / 1e9).toFixed(2) + 'B';
      if (abs >= 1e6)  return '$' + (value / 1e6).toFixed(2) + 'M';
      if (abs >= 1e3)  return '$' + (value / 1e3).toFixed(1) + 'K';
      return '$' + value.toFixed(2);
    }
    case 'percent': {
      const sign = value >= 0 ? '+' : '';
      return `${sign}${value.toFixed(2)}%`;
    }
    case 'number':
      return typeof value === 'number'
        ? value.toLocaleString('en-US', { maximumFractionDigits: 2 })
        : value;
    default:
      return String(value);
  }
}

/**
 * DataTable - Sortable financial data table.
 *
 * @param {Object[]} columns - Array of { key: string, label: string, type: "string"|"number"|"currency"|"percent" }
 * @param {Object[]} rows - Array of row objects keyed by column.key
 * @param {number} [pageSize] - Rows per page. Set to 0 to disable pagination (default: 0).
 */
export default function DataTable({ columns, rows, pageSize = 0 }) {
  const [sort, setSort] = useState({ key: null, asc: true });
  const [page, setPage] = useState(0);

  const sortedRows = useMemo(() => {
    if (!sort.key) return rows;
    return [...rows].sort((a, b) => {
      const av = a[sort.key], bv = b[sort.key];
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === 'string' ? av.localeCompare(bv) : av - bv;
      return sort.asc ? cmp : -cmp;
    });
  }, [rows, sort]);

  const pagedRows = useMemo(() => {
    if (!pageSize) return sortedRows;
    return sortedRows.slice(page * pageSize, (page + 1) * pageSize);
  }, [sortedRows, page, pageSize]);

  const totalPages = pageSize ? Math.ceil(rows.length / pageSize) : 1;

  function handleSort(key) {
    setSort(prev => prev.key === key ? { key, asc: !prev.asc } : { key, asc: true });
    setPage(0);
  }

  if (!rows?.length) {
    return <div style={{ color: 'var(--text-secondary)', padding: 20 }}>No data available</div>;
  }

  return (
    <div style={STYLES.container}>
      <table style={STYLES.table}>
        <thead>
          <tr>
            {columns.map(col => {
              const isNum = col.type !== 'string';
              const arrow = sort.key === col.key ? (sort.asc ? ' \u25B2' : ' \u25BC') : '';
              return (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  style={{ ...STYLES.th, textAlign: isNum ? 'right' : 'left' }}
                >
                  {col.label}{arrow}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {pagedRows.map((row, i) => (
            <tr key={i} style={i % 2 === 1 ? STYLES.evenRow : {}}>
              {columns.map(col => {
                const isNum = col.type !== 'string';
                const val = formatCell(row[col.key], col.type);
                const color = col.type === 'percent' && row[col.key] != null
                  ? (row[col.key] >= 0 ? 'var(--positive)' : 'var(--negative)')
                  : 'var(--text-primary)';
                return (
                  <td key={col.key} style={{ ...STYLES.td, textAlign: isNum ? 'right' : 'left', color }}>
                    {val}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>

      {/* Pagination controls (only shown when pageSize > 0) */}
      {pageSize > 0 && totalPages > 1 && (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 12, fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
          <span>Page {page + 1} of {totalPages}</span>
          <div style={{ display: 'flex', gap: 8 }}>
            <button
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0}
              style={{ background: 'var(--bg-hover)', color: 'var(--text-primary)', border: 'none', borderRadius: 4, padding: '4px 12px', cursor: page === 0 ? 'not-allowed' : 'pointer', opacity: page === 0 ? 0.5 : 1 }}
            >
              Prev
            </button>
            <button
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              style={{ background: 'var(--bg-hover)', color: 'var(--text-primary)', border: 'none', borderRadius: 4, padding: '4px 12px', cursor: page >= totalPages - 1 ? 'not-allowed' : 'pointer', opacity: page >= totalPages - 1 ? 0.5 : 1 }}
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

---

### 5. KPICard -- React Component

```jsx
// components/KPICard.jsx

/**
 * KPICard - Single metric card for dashboard header rows.
 *
 * @param {string} label - Metric name (e.g., "Market Cap")
 * @param {string|number} value - Formatted display value (e.g., "$2.87T")
 * @param {number|null} [change] - Percentage change. Positive = green, negative = red.
 * @param {string} [prefix] - Optional prefix for the value (e.g., "$")
 */
export default function KPICard({ label, value, change = null, prefix = '' }) {
  const changeColor = change == null ? 'var(--text-secondary)'
    : change >= 0 ? 'var(--positive)' : 'var(--negative)';
  const changeText = change != null
    ? `${change >= 0 ? '+' : ''}${change.toFixed(2)}%`
    : null;

  return (
    <div style={{
      background: 'var(--bg-card)',
      border: '1px solid var(--border)',
      borderRadius: 8,
      padding: 16,
      minWidth: 140
    }}>
      <div style={{
        fontSize: '0.75rem', color: 'var(--text-secondary)',
        textTransform: 'uppercase', letterSpacing: '0.05em',
        marginBottom: 6
      }}>
        {label}
      </div>
      <div style={{
        fontSize: '1.75rem', fontWeight: 700,
        color: 'var(--text-primary)', lineHeight: 1.2
      }}>
        {prefix}{value}
      </div>
      {changeText && (
        <div style={{ fontSize: '0.8rem', fontWeight: 500, marginTop: 4, color: changeColor }}>
          {changeText}
        </div>
      )}
    </div>
  );
}
```

Usage in a dashboard layout:

```jsx
// Example: row of KPI cards
<div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 12 }}>
  <KPICard label="Price" value="182.52" prefix="$" change={1.34} />
  <KPICard label="Market Cap" value="2.87T" prefix="$" change={1.34} />
  <KPICard label="Volume" value="48.2M" change={-12.5} />
  <KPICard label="P/E Ratio" value="28.4x" />
</div>
```

---

### 6. useStockData Hook -- Data Fetching

```jsx
// hooks/useStockData.js
import { useState, useEffect } from 'react';

/**
 * useStockData - Fetches stock data from the FastAPI backend.
 *
 * The backend endpoint should return:
 *   { history: [{ date, open, high, low, close, volume }], info: { ... } }
 *
 * @param {string} ticker - Stock ticker symbol (e.g., "AAPL")
 * @param {string} [period] - History period (default: "1y")
 * @returns {{ data: Object|null, loading: boolean, error: string|null, refetch: Function }}
 */
export default function useStockData(ticker, period = '1y') {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  async function fetchData() {
    if (!ticker) {
      setData(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`/api/stock/${encodeURIComponent(ticker)}?period=${period}`);
      if (!res.ok) {
        throw new Error(`Failed to fetch ${ticker}: ${res.status} ${res.statusText}`);
      }
      const json = await res.json();

      // Validate response has expected shape
      if (!json.history || !Array.isArray(json.history)) {
        throw new Error(`Invalid response for ${ticker}: missing history array`);
      }

      setData(json);
    } catch (err) {
      setError(err.message);
      setData(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchData();
  }, [ticker, period]);

  return { data, loading, error, refetch: fetchData };
}
```

Usage in a page component:

```jsx
import useStockData from '../hooks/useStockData';
import StockChart from '../components/StockChart';
import KPICard from '../components/KPICard';

export default function StockPage({ ticker }) {
  const { data, loading, error } = useStockData(ticker, '1y');

  if (loading) return <div style={{ color: 'var(--text-secondary)', padding: 40 }}>Loading...</div>;
  if (error)   return <div style={{ color: 'var(--negative)', padding: 40 }}>Error: {error}</div>;

  const prices = data.history.map(d => ({ date: d.date, close: d.close }));
  const latest = data.history[data.history.length - 1];
  const first  = data.history[0];
  const changePct = ((latest.close - first.close) / first.close) * 100;

  return (
    <div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 12, marginBottom: 20 }}>
        <KPICard label="Price" value={latest.close.toFixed(2)} prefix="$" change={changePct} />
        <KPICard label="Volume" value={(latest.volume / 1e6).toFixed(1) + 'M'} />
      </div>
      <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8, padding: 20 }}>
        <StockChart data={prices} />
      </div>
    </div>
  );
}
```
