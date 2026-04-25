import { newWidgetId } from './framework/ids';
import type { DashboardPrefs, RGLItem, WidgetInstance } from './types';
import { DASHBOARD_PREFS_VERSION } from './types';
import { DEFAULT_BLUE_CHIPS } from './framework/defaults';

export type PresetId =
  | 'morning-brief'
  | 'researcher'
  | 'trader'
  | 'trader-tv'
  | 'portfolio-steward'
  | 'agent-desk';

export interface PresetMeta {
  id: PresetId;
  name: string;
  tagline: string;
  tag: string;
  description: string;
  bestFor: string;
  pills: string[];
  popular?: boolean;
}

const newId = newWidgetId;

function makePrefs(
  widgets: WidgetInstance[],
  layoutsLg: RGLItem[]
): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  return {
    version: DASHBOARD_PREFS_VERSION,
    widgets,
    layouts: { lg: layoutsLg, md: layoutsLg },
  };
}

/**
 * MORNING BRIEF — Default.
 * AI Daily Brief is the hero; markets overview anchors the top. The combined
 * portfolio + watchlist rail on the right gives a personal lens at a glance,
 * while news and earnings fill the canvas below for the full morning scan.
 * Agent input lives as the floating pill at the bottom; not a grid cell here.
 */
export function morningBriefPreset(): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  const markets = { id: newId('mo'), type: 'markets.overview', config: {} };
  const brief = { id: newId('br'), type: 'insight.brief', config: { variant: 'latest' } };
  const holdings = { id: newId('pw'), type: 'personal.portfolioWatchlist', config: { defaultTab: 'watchlist', valuesHidden: false } };
  const news = { id: newId('nw'), type: 'news.feed', config: { source: 'market' } };
  const earnings = { id: newId('er'), type: 'calendar.earnings', config: {} };

  const widgets = [markets, brief, holdings, news, earnings];
  const layouts: RGLItem[] = [
    { i: markets.id, x: 0, y: 0, w: 12, h: 11 },
    { i: brief.id, x: 0, y: 11, w: 8, h: 22 },
    { i: holdings.id, x: 8, y: 11, w: 4, h: 30 },
    { i: news.id, x: 0, y: 33, w: 8, h: 38 },
    { i: earnings.id, x: 8, y: 41, w: 4, h: 29 },
  ];
  return makePrefs(widgets, layouts);
}

/**
 * RESEARCHER — Sources-first deep work.
 * The ask console leads the canvas; a tall market news rail on the right keeps
 * sources in view throughout the session. Brief, combined holdings + watchlist,
 * and a 2-week earnings calendar round out the working surface below.
 */
export function researcherPreset(): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  const markets = { id: newId('mo'), type: 'markets.overview', config: {} };
  const agent = { id: newId('ag'), type: 'agent.conversation', config: {} };
  const news = { id: newId('nw'), type: 'news.feed', config: { source: 'market' } };
  const brief = { id: newId('br'), type: 'insight.brief', config: { variant: 'latest' } };
  const holdings = { id: newId('pw'), type: 'personal.portfolioWatchlist', config: { defaultTab: 'watchlist', valuesHidden: false } };
  const earnings = { id: newId('er'), type: 'calendar.earnings', config: { window: '2w', tickers: 'all' } };

  const widgets = [markets, agent, news, brief, holdings, earnings];
  const layouts: RGLItem[] = [
    { i: markets.id, x: 0, y: 0, w: 12, h: 11 },
    { i: agent.id, x: 0, y: 11, w: 8, h: 13 },
    { i: news.id, x: 8, y: 11, w: 4, h: 42 },
    { i: brief.id, x: 0, y: 24, w: 8, h: 18 },
    { i: holdings.id, x: 0, y: 42, w: 8, h: 28 },
    { i: earnings.id, x: 8, y: 53, w: 4, h: 20 },
  ];
  return makePrefs(widgets, layouts);
}

/**
 * TRADER — Multi-timeframe workstation.
 * A 2×2 chart grid stacks short-horizon candles (5m intraday, 30m over a
 * week) over longer area views (daily NVDA + daily SPY benchmark) so the
 * same symbol reads at four zoom levels at once. The combined portfolio +
 * watchlist and a wide news tape close the canvas. Floating agent input
 * covers quick queries.
 */
export function traderPreset(): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  const markets = { id: newId('mo'), type: 'markets.overview', config: {} };
  const nvdaDay = { id: newId('ch'), type: 'chart.symbol', config: { symbol: 'NVDA', interval: '5min', chartType: 'candle' } };
  const nvdaWeek = { id: newId('ch'), type: 'chart.symbol', config: { symbol: 'NVDA', interval: '30min', chartType: 'candle' } };
  const nvdaQuarter = { id: newId('ch'), type: 'chart.symbol', config: { symbol: 'NVDA', interval: '1day', chartType: 'area' } };
  const spyQuarter = { id: newId('ch'), type: 'chart.symbol', config: { symbol: 'SPY', interval: '1day', chartType: 'area' } };
  const holdings = { id: newId('pw'), type: 'personal.portfolioWatchlist', config: { defaultTab: 'watchlist', valuesHidden: false } };
  const news = { id: newId('nw'), type: 'news.feed', config: { source: 'market' } };

  const widgets = [markets, nvdaDay, nvdaWeek, nvdaQuarter, spyQuarter, holdings, news];
  const layouts: RGLItem[] = [
    { i: markets.id, x: 0, y: 0, w: 12, h: 11 },
    { i: nvdaDay.id, x: 0, y: 11, w: 6, h: 22 },
    { i: nvdaWeek.id, x: 6, y: 11, w: 6, h: 22 },
    { i: nvdaQuarter.id, x: 0, y: 33, w: 6, h: 22 },
    { i: spyQuarter.id, x: 6, y: 33, w: 6, h: 22 },
    { i: holdings.id, x: 0, y: 55, w: 5, h: 26 },
    { i: news.id, x: 5, y: 55, w: 7, h: 26 },
  ];
  return makePrefs(widgets, layouts);
}

/**
 * TRADER (TV) — TradingView workstation.
 * All-TV layout: ticker tape anchor, stock heatmap hero, symbol spotlight +
 * movers side-by-side, economic events + technicals below, native mini-chart
 * grid closes the canvas. Existing `trader` preset is preserved unchanged —
 * users can pick either from the Presets dialog.
 */
export function traderTvPreset(): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  // Empty `symbols` lets TickerTapeWidget compute its live seed at render
  // time (defaults + user's watchlist + portfolio, deduped). Hardcoding
  // here would bypass the watchlist/portfolio merge.
  const tape = { id: newId('tt'), type: 'tv.ticker-tape', config: { symbols: [], displayMode: 'adaptive' } };
  const heatmap = { id: newId('hm'), type: 'tv.stock-heatmap', config: { dataSource: 'SPX500', blockSize: 'market_cap_basic', blockColor: 'change' } };
  const spotlight = { id: newId('ss'), type: 'tv.symbol-spotlight', config: { symbol: 'NASDAQ:NVDA', range: '12M' } };
  const movers = { id: newId('mv'), type: 'tv.movers', config: { exchange: 'US', dataSource: 'AllUSA' } };
  const events = { id: newId('ev'), type: 'tv.economic-events', config: { importanceFilter: '-1,0,1', countryFilter: 'us,eu,jp,gb,cn' } };
  const technicals = { id: newId('ta'), type: 'tv.technicals', config: { symbol: 'NASDAQ:NVDA', interval: '1D' } };
  const miniGrid = { id: newId('mg'), type: 'markets.miniChartGrid', config: { symbols: DEFAULT_BLUE_CHIPS } };

  const widgets = [tape, heatmap, spotlight, movers, events, technicals, miniGrid];
  const layouts: RGLItem[] = [
    { i: tape.id, x: 0, y: 0, w: 12, h: 4 },
    { i: heatmap.id, x: 0, y: 4, w: 12, h: 20 },
    { i: spotlight.id, x: 0, y: 24, w: 6, h: 22 },
    { i: movers.id, x: 6, y: 24, w: 6, h: 22 },
    { i: events.id, x: 0, y: 46, w: 6, h: 24 },
    { i: technicals.id, x: 6, y: 46, w: 6, h: 24 },
    { i: miniGrid.id, x: 0, y: 70, w: 12, h: 16 },
  ];
  return makePrefs(widgets, layouts);
}

/**
 * PORTFOLIO STEWARD — Wealth-first, actively tended.
 * Your top holding's daily chart anchors the left canvas; combined portfolio +
 * watchlist sit alongside; a portfolio-filtered news feed runs as a tall right
 * rail. The ask console and recent threads sit below for when you need to
 * dig into a position — so the book is surrounded by thinking surface.
 */
export function portfolioStewardPreset(): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  const markets = { id: newId('mo'), type: 'markets.overview', config: {} };
  const topHolding = { id: newId('ch'), type: 'chart.symbol', config: { symbol: 'NVDA', interval: '1day', chartType: 'candle' } };
  const holdings = { id: newId('pw'), type: 'personal.portfolioWatchlist', config: { defaultTab: 'watchlist', valuesHidden: false } };
  const agent = { id: newId('ag'), type: 'agent.conversation', config: {} };
  const news = { id: newId('nw'), type: 'news.feed', config: { source: 'portfolio' } };
  const threads = { id: newId('th'), type: 'threads.recent', config: { workspaceId: 'all', limit: 15 } };

  const widgets = [markets, topHolding, holdings, agent, news, threads];
  const layouts: RGLItem[] = [
    { i: markets.id, x: 0, y: 0, w: 12, h: 11 },
    { i: topHolding.id, x: 0, y: 11, w: 8, h: 30 },
    { i: holdings.id, x: 8, y: 11, w: 4, h: 30 },
    { i: agent.id, x: 0, y: 41, w: 8, h: 13 },
    { i: news.id, x: 8, y: 41, w: 4, h: 31 },
    { i: threads.id, x: 0, y: 54, w: 8, h: 15 },
  ];
  return makePrefs(widgets, layouts);
}

/**
 * AGENT DESK — The agent's working surface.
 * Today's AI brief anchors the top as a wide reading surface; a full-width
 * ask console sits directly beneath — read, then ask. Workspaces + recent
 * threads form the navigation row; automations and the combined portfolio +
 * watchlist close the canvas as the personal working surface. No markets
 * overview — this preset is intentionally context-first, not tape-first.
 */
export function agentDeskPreset(): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  const brief = { id: newId('br'), type: 'insight.brief', config: { variant: 'latest' } };
  const convo = { id: newId('cv'), type: 'agent.conversation', config: {} };
  const picker = { id: newId('ws'), type: 'workspace.picker', config: { limit: 12 } };
  const threads = { id: newId('th'), type: 'threads.recent', config: { limit: 10 } };
  const automations = { id: newId('au'), type: 'automations.list', config: { limit: 8 } };
  const holdings = { id: newId('pw'), type: 'personal.portfolioWatchlist', config: { defaultTab: 'watchlist', valuesHidden: false } };

  const widgets = [brief, convo, picker, threads, automations, holdings];
  const layouts: RGLItem[] = [
    { i: brief.id, x: 0, y: 0, w: 12, h: 20 },
    { i: convo.id, x: 0, y: 20, w: 12, h: 13 },
    { i: picker.id, x: 0, y: 33, w: 8, h: 22 },
    { i: threads.id, x: 8, y: 33, w: 4, h: 22 },
    { i: automations.id, x: 0, y: 55, w: 6, h: 18 },
    { i: holdings.id, x: 6, y: 55, w: 6, h: 18 },
  ];
  return makePrefs(widgets, layouts);
}

export const PRESETS_META: readonly PresetMeta[] = [
  {
    id: 'morning-brief',
    name: 'Morning Brief',
    tagline: 'balanced',
    tag: 'balanced',
    description: 'The first five minutes of your day. AI Daily Brief anchors the canvas; market news, earnings, and your portfolio + watchlist sit one glance away.',
    bestFor: 'Opening the app once a day and wanting the full picture.',
    pills: ['AI daily brief', 'Portfolio + Watchlist', 'News', 'Earnings', 'Markets overview'],
    popular: true,
  },
  {
    id: 'agent-desk',
    name: 'Agent Desk',
    tagline: 'agent-led',
    tag: 'agent-led',
    description: "The agent's working surface. Start with today's AI brief, then ask — a full-width ask console sits beneath. Workspaces, recent threads, automations, and your book round out the desk.",
    bestFor: 'Living next to the agent — reading, asking, running automations, jumping across workspaces.',
    pills: ['AI brief', 'Ask console', 'Workspaces', 'Recent threads', 'Automations', 'Holdings + Watchlist'],
  },
  {
    id: 'researcher',
    name: 'Researcher',
    tagline: 'sources-first',
    tag: 'sources-first',
    description: 'Deep qualitative work. The ask console leads; a tall market news rail keeps sources in view. AI brief, holdings + watchlist, and a 2-week earnings calendar round out the canvas.',
    bestFor: 'Writing memos, running deep-dives, chasing citations.',
    pills: ['Ask console', 'Market news rail', 'AI brief', 'Portfolio + Watchlist', 'Earnings (2w)', 'Markets overview'],
  },
  {
    id: 'trader',
    name: 'Trader',
    tagline: 'multi-timeframe',
    tag: 'multi-timeframe',
    description: 'Multi-timeframe workstation. A 2×2 chart grid reads the same symbol at four zoom levels — short-horizon candles over long-horizon area views and a benchmark. Portfolio + watchlist and a wide news tape close the canvas.',
    bestFor: 'Reading one symbol deeply across horizons with context in view.',
    pills: ['Chart × 4', 'Multi-timeframe', 'Portfolio + Watchlist', 'News feed', 'Markets overview'],
  },
  {
    id: 'trader-tv',
    name: 'Trader (TV)',
    tagline: 'tradingview-first',
    tag: 'tradingview-first',
    description: 'A TradingView-driven trading desk. Ticker tape anchors the top, stock heatmap as hero, symbol spotlight with market movers alongside, economic events + a technical analysis gauge below, and a native mini-chart grid closes the canvas.',
    bestFor: 'Tape-to-sector-to-symbol scan, with TV-native interactivity inside each card.',
    pills: ['Ticker tape', 'Stock heatmap', 'Symbol spotlight', 'Market movers', 'Economic events', 'Technicals', 'Mini chart grid'],
  },
  {
    id: 'portfolio-steward',
    name: 'Portfolio Steward',
    tagline: 'wealth-first',
    tag: 'wealth-first',
    description: "Your book, actively tended. Your top holding's chart anchors the canvas alongside the combined portfolio + watchlist; a portfolio-filtered news rail runs tall on the right. Ask console and recent threads sit below for when you need to dig in.",
    bestFor: 'Tending a concentrated book and asking questions about positions.',
    pills: ['Top holding chart', 'Portfolio + Watchlist', 'News (portfolio)', 'Ask console', 'Recent threads', 'Markets'],
  },
];

const PRESET_FACTORIES: Record<PresetId, () => Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'>> = {
  'morning-brief': morningBriefPreset,
  researcher: researcherPreset,
  trader: traderPreset,
  'trader-tv': traderTvPreset,
  'portfolio-steward': portfolioStewardPreset,
  'agent-desk': agentDeskPreset,
};

export function getPreset(id: PresetId): Pick<DashboardPrefs, 'widgets' | 'layouts' | 'version'> {
  return PRESET_FACTORIES[id]();
}
