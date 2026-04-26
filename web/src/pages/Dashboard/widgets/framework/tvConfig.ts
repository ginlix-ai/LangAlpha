/**
 * Shared TradingView embed plumbing — iframe-script variant.
 *
 * Strategy: every TV widget injects an iframe-script tag with a JSON config
 * blob. We dedupe loader fetches per scriptSrc so a dashboard with multiple
 * TV widgets of the same kind only hits the network once. Theme + config
 * changes rebuild the iframe (TV's embed scripts don't expose attribute
 * APIs for the legacy widget catalog).
 *
 * Locale: `getTVCommonConfig()` reads `i18n.language` at call time and maps
 * it into TV's locale grammar via `mapLocaleForTV()`. Embed effects must
 * include `i18n.language` in their dep array so the iframe rebuilds when
 * the user switches locale.
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

import i18n from '@/i18n';

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

/**
 * BCP-47 → TV locale string. TV uses underscored variants (`zh_CN`, `pt_BR`)
 * and falls back to `en` for unsupported locales. Caller maps once per
 * iframe rebuild.
 */
const TV_LOCALE_MAP: Record<string, string> = {
  'en-US': 'en',
  'en-GB': 'en',
  'zh-CN': 'zh_CN',
  'zh-TW': 'zh_TW',
  'ja-JP': 'ja',
  'ko-KR': 'ko',
  'es-ES': 'es',
  'fr-FR': 'fr',
  'de-DE': 'de',
  'pt-BR': 'pt',
  'ru-RU': 'ru',
  // Prefix-only fallbacks for unknown regions (e.g. `en-AU`, `zh-HK`).
  // Defaults pick the most common region per language.
  en: 'en',
  zh: 'zh_CN',
  ja: 'ja',
  ko: 'ko',
  es: 'es',
  fr: 'fr',
  de: 'de',
  pt: 'pt',
  ru: 'ru',
};

export function mapLocaleForTV(bcp47: string): string {
  if (TV_LOCALE_MAP[bcp47]) return TV_LOCALE_MAP[bcp47];
  const prefix = bcp47.split('-')[0];
  if (TV_LOCALE_MAP[prefix]) return TV_LOCALE_MAP[prefix];
  return 'en';
}

/** Per-card config defaults baked into every TV embed payload.
 *
 * Function (not `as const`) so `locale` reflects the current `i18n.language`
 * each time an embed effect rebuilds. Consumers MUST include `i18n.language`
 * in their effect deps; the function reads it at call time, but React only
 * re-runs the effect when a tracked dep changes.
 *
 * `largeChartUrl` overrides the "view full chart" button destination for the
 * subset of iframe widgets that honor it (symbol-overview, mini-chart,
 * ticker-tape, advanced-chart) — clicks land on our `/market` route instead
 * of tradingview.com. Heatmaps / hotlists / events ignore this key; symbol
 * clicks inside those widgets still bounce to tradingview.com. The full
 * symbol-link redirection (`symbol-url={tvsymbol}`) would require the
 * `<tv-*>` web-component variant — currently unreachable, see header. */
export function getTVCommonConfig(): Record<string, unknown> {
  return {
    autosize: true,
    width: '100%',
    height: '100%',
    isTransparent: true,
    backgroundColor: 'rgba(0,0,0,0)',
    locale: mapLocaleForTV(i18n.language),
    support_host: 'https://www.tradingview.com',
    largeChartUrl: largeChartUrl(),
  };
}
