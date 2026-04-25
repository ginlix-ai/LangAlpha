/**
 * Shared TradingView embed plumbing — iframe-script variant.
 *
 * Strategy: every TV widget injects an iframe-script tag with a JSON config
 * blob. We dedupe loader fetches per scriptSrc so a dashboard with multiple
 * TV widgets of the same kind only hits the network once. Theme + config
 * changes rebuild the iframe (TV's embed scripts don't expose attribute
 * APIs for the legacy widget catalog).
 *
 * Web-components alternative: `framework/TradingViewWebComponent.tsx` hosts
 * the newer `<tv-*>` catalog from `widgets.tradingview-widget.com`. Web
 * components support `symbol-url` redirection (per-symbol click → our
 * `/market` route) and reactive attribute updates without iframe rebuild.
 *
 * Catalog status (probed 2026-04-24 — re-check periodically as TV expands):
 *   Available as WC (5 total): tv-ticker-tape, tv-ticker-tag, tv-mini-chart,
 *     tv-market-summary, tv-economic-map.
 *   NOT yet WC (403 at widgets.tradingview-widget.com/w/en/): tv-symbol-
 *     profile, tv-financials, tv-forex-cross-rates, tv-hotlists, tv-events,
 *     tv-crypto-coins-heatmap, tv-technical-analysis, tv-stock-heatmap,
 *     tv-symbol-overview, tv-advanced-chart.
 *   In use: only `tv-economic-map` (EconomicMapWidget). Our other 9 TV
 *     widgets stay on iframes until TV ships their WC equivalents. Symbol
 *     clicks inside iframes bounce to tradingview.com — only WCs honor
 *     `<tv-custom-settings symbol-url>` redirection.
 *
 * Iframe-only by choice (do NOT migrate to WC even when available):
 *   - `ticker-tape`: the WC `<tv-ticker-tape>` renders a static bordered
 *     *grid* (verified across `default`, `horizontal`, `horizontal_no_chart`
 *     presets in TV's gallery). The iframe `embed-widget-ticker-tape.js` is
 *     the only variant that produces the classic continuous scrolling
 *     marquee that users expect. Trade-off: symbol clicks bounce to
 *     tradingview.com instead of our `/market` route.
 */

const TV_BASE = 'https://s3.tradingview.com/external-embedding/';

/**
 * Resolves the canonical embed-script URL given a short widget key.
 * Pass either a full URL or one of the well-known short keys below.
 */
const SCRIPT_KEYS: Record<string, string> = {
  'ticker-tape': 'embed-widget-ticker-tape.js',
  'stock-heatmap': 'embed-widget-stock-heatmap.js',
  'crypto-coins-heatmap': 'embed-widget-crypto-coins-heatmap.js',
  'forex-cross-rates': 'embed-widget-forex-cross-rates.js',
  'events': 'embed-widget-events.js',
  'technical-analysis': 'embed-widget-technical-analysis.js',
  'hotlists': 'embed-widget-hotlists.js',
  'symbol-overview': 'embed-widget-symbol-overview.js',
  'symbol-profile': 'embed-widget-symbol-profile.js',
  'financials': 'embed-widget-financials.js',
  'advanced-chart': 'embed-widget-advanced-chart.js',
  'mini-symbol-overview': 'embed-widget-mini-symbol-overview.js',
  'screener': 'embed-widget-screener.js',
  'etf-heatmap': 'embed-widget-etf-heatmap.js',
  'timeline': 'embed-widget-timeline.js',
  'single-quote': 'embed-widget-single-quote.js',
  'symbol-info': 'embed-widget-symbol-info.js',
};

export function resolveScriptSrc(key: string): string {
  if (key.startsWith('https://') || key.startsWith('http://')) return key;
  const file = SCRIPT_KEYS[key];
  if (!file) throw new Error(`[tvConfig] unknown script key: ${key}`);
  return `${TV_BASE}${file}`;
}

/** Public site we send "view full chart" links to. */
export function largeChartUrl(): string {
  if (typeof window === 'undefined') return '/market';
  return `${window.location.origin}/market`;
}

/** Per-card config defaults baked into every TV embed payload.
 *
 * `largeChartUrl` overrides the "view full chart" button destination for the
 * subset of iframe widgets that honor it (symbol-overview, mini-chart,
 * ticker-tape, advanced-chart) — clicks land on our `/market` route instead
 * of tradingview.com. Heatmaps / hotlists / events ignore this key; symbol
 * clicks inside those widgets still bounce to tradingview.com. The full
 * symbol-link redirection (`symbol-url={tvsymbol}`) would require the
 * `<tv-*>` web-component variant — currently unreachable, see header. */
export const TV_COMMON_CONFIG = {
  autosize: true,
  width: '100%',
  height: '100%',
  isTransparent: true,
  backgroundColor: 'rgba(0,0,0,0)',
  locale: 'en',
  support_host: 'https://www.tradingview.com',
  largeChartUrl: largeChartUrl(),
} as const;
