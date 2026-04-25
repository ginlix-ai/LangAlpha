import { z } from 'zod';

/**
 * Per-widget Zod schemas applied at the prefs-load boundary
 * (`migrations.ts → sanitizeConfig`). Per-field `.catch()` recovers
 * individual bad values; whole-config failure falls back to `defaultConfig`.
 *
 * Centralizing schemas in one file keeps the contract visible: schema
 * changes here are intentional, not buried in 30 widget files.
 *
 * Enum tuples are exported `as const` so settings dialogs can derive their
 * dropdown options from the same source of truth and never drift from the
 * schema (Zod v4's `.options` is hidden after `.catch()` is applied — keep
 * the const around).
 */

// TV symbol grammar: prefix:base, dots in tickers (BRK.B), futures (`!`),
// forex slash (`/`), TVC indices (`^`), &, underscores in real exchange
// prefixes (CME_MINI:ES1!, FX_IDC:EURUSD, KRX:HMM_T), plus uppercase +
// digits + dash. Deliberately permissive — TV's wider grammar would
// otherwise drop valid configs the user typed in good faith.
const TV_SYMBOL_RE = /^[A-Z0-9._\-:!/^&]+$/i;

const tvSymbol = (def: string) => z.string().min(1).regex(TV_SYMBOL_RE).catch(def);
const intInRange = (def: number, min: number, max: number) =>
  z.number().int().min(min).max(max).catch(def);
const looseString = (def: string) => z.string().min(1).catch(def);

// =============================================================================
// TV widgets (17)
// =============================================================================

export const TICKER_TAPE_DISPLAY_MODES = ['adaptive', 'regular', 'compact'] as const;
export const TickerTapeConfigSchema = z.object({
  // Drop invalid entries (and dedupe) instead of per-element .catch(default) —
  // a corrupted blob with [bad, bad, NVDA] becomes [NVDA] rather than
  // [SPY, SPY, NVDA] (silent duplication). User keeps every valid symbol.
  symbols: z
    .array(z.unknown())
    .catch([])
    .transform((arr) => {
      const seen = new Set<string>();
      const out: string[] = [];
      for (const v of arr) {
        if (typeof v !== 'string') continue;
        if (!TV_SYMBOL_RE.test(v) || v.length === 0) continue;
        if (seen.has(v)) continue;
        seen.add(v);
        out.push(v);
      }
      return out;
    }),
  displayMode: z.enum(TICKER_TAPE_DISPLAY_MODES).catch('adaptive'),
});

export const STOCK_HEATMAP_DATA_SOURCES = [
  'SPX500', 'NASDAQ100', 'DOW30', 'AllUSA', 'Asia', 'Europe', 'crypto',
] as const;
export const STOCK_HEATMAP_BLOCK_SIZES = ['market_cap_basic', 'volume', 'number_of_employees'] as const;
export const STOCK_HEATMAP_BLOCK_COLORS = ['change', 'Perf.W', 'Perf.1M', 'Perf.YTD', 'Perf.Y'] as const;
export const StockHeatmapConfigSchema = z.object({
  dataSource: z.string().min(1).catch('SPX500'),
  blockSize: z.string().min(1).catch('market_cap_basic'),
  blockColor: z.string().min(1).catch('change'),
});

export const CryptoHeatmapConfigSchema = z.object({
  dataSource: z.string().min(1).catch('Crypto'),
  blockSize: z.string().min(1).catch('market_cap_calc'),
  blockColor: z.string().min(1).catch('24h_close_change|5'),
});

export const ETFHeatmapConfigSchema = z.object({
  dataSource: z.string().min(1).catch('AllUSEtf'),
  blockSize: z.string().min(1).catch('aum'),
  blockColor: z.string().min(1).catch('change'),
  grouping: z.string().min(1).catch('asset_class'),
});

// Mirror of DEFAULT_CURRENCIES in ForexHeatmapWidget — kept here so the
// schema's whole-array fallback matches the widget's defaultConfig (a
// single-currency catch produces a useless cross-rate widget).
export const FOREX_DEFAULT_CURRENCIES = [
  'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'CNY',
] as const;
export const ForexHeatmapConfigSchema = z.object({
  // Drop bad currency codes (and dedupe) rather than per-element .catch('USD')
  // which would turn [JPY, garbage, EUR] into [USD, USD, EUR]. If the whole
  // value is malformed (not an array), fall back to the full default set so
  // the cross-rate widget still renders something useful.
  currencies: z
    .array(z.unknown())
    .catch([...FOREX_DEFAULT_CURRENCIES])
    .transform((arr) => {
      const seen = new Set<string>();
      const out: string[] = [];
      for (const v of arr) {
        if (typeof v !== 'string') continue;
        if (!/^[A-Z]{3}$/.test(v)) continue;
        if (seen.has(v)) continue;
        seen.add(v);
        out.push(v);
      }
      // Empty after filtering = useless; restore the rich default.
      return out.length === 0 ? [...FOREX_DEFAULT_CURRENCIES] : out;
    }),
});

export const EconomicEventsConfigSchema = z.object({
  // .min(1) guards against an empty string sneaking past validation — TV
  // would render an empty calendar instead of falling back to the catch.
  importanceFilter: z.string().min(1).catch('-1,0,1'),
  countryFilter: z.string().min(1).catch('us,eu,jp,gb,cn'),
});

export const ECONOMIC_MAP_REGIONS = [
  'global', 'africa', 'asia', 'europe', 'north-america', 'oceania', 'south-america',
] as const;
export const ECONOMIC_MAP_METRICS = ['gdp', 'ur', 'gdg', 'intr', 'iryy'] as const;
export const EconomicMapConfigSchema = z.object({
  region: z.enum(ECONOMIC_MAP_REGIONS).catch('global'),
  metric: z.enum(ECONOMIC_MAP_METRICS).catch('gdp'),
  hideLegend: z.boolean().catch(false),
});

export const TechnicalsConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
  // TV interval grammar is wide ("1m", "5m", "1h", "1D", "1W"). Loose check.
  interval: z.string().min(1).catch('1D'),
});

export const MoversConfigSchema = z.object({
  exchange: z.string().min(1).catch('US'),
  dataSource: z.string().min(1).catch('AllUSA'),
});

export const SymbolSpotlightConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
  range: z.string().min(1).catch('12M'),
});

export const CompanyProfileConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
});

export const COMPANY_FIN_DISPLAY_MODES = ['regular', 'compact', 'adaptive'] as const;
export const CompanyFinancialsConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
  displayMode: z.enum(COMPANY_FIN_DISPLAY_MODES).catch('regular'),
});

export const TOP_STORIES_FEED_MODES = ['all_symbols', 'market', 'symbol'] as const;
export const TOP_STORIES_MARKETS = [
  'stock', 'crypto', 'forex', 'index', 'futures', 'bond', 'economic',
] as const;
export const TOP_STORIES_DISPLAY_MODES = ['regular', 'compact'] as const;
export const TopStoriesConfigSchema = z.object({
  feedMode: z.enum(TOP_STORIES_FEED_MODES).catch('market'),
  market: z.enum(TOP_STORIES_MARKETS).catch('stock'),
  symbol: tvSymbol('NASDAQ:NVDA'),
  displayMode: z.enum(TOP_STORIES_DISPLAY_MODES).catch('regular'),
});

export const SingleTickerConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
});

export const SymbolInfoConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
});

export const StockScreenerConfigSchema = z.object({
  market: z.string().min(1).catch('america'),
  defaultColumn: z.string().min(1).catch('overview'),
  defaultScreen: z.string().min(1).catch('general'),
});

export const CryptoScreenerConfigSchema = z.object({
  defaultColumn: z.string().min(1).catch('overview'),
  defaultScreen: z.string().min(1).catch('general'),
});

// =============================================================================
// Native widgets (13)
// =============================================================================

export const CHART_INTERVALS = ['1min', '5min', '15min', '30min', '1hour', '1day'] as const;
export const CHART_TYPES = ['candle', 'area', 'line'] as const;
export const ChartConfigSchema = z.object({
  symbol: looseString('NVDA'),
  interval: z.enum(CHART_INTERVALS).catch('1day'),
  chartType: z.enum(CHART_TYPES).catch('candle'),
});

export const MiniChartGridConfigSchema = z.object({
  // No regex — accepts plain symbols ("NVDA") and TV-style ("NASDAQ:NVDA").
  // Filter non-strings + empties, dedupe; return [] if all entries are bad
  // (the widget falls back to watchlist/blue-chips at render time).
  symbols: z
    .array(z.unknown())
    .catch([])
    .transform((arr) => {
      const seen = new Set<string>();
      const out: string[] = [];
      for (const v of arr) {
        if (typeof v !== 'string' || v.length === 0) continue;
        if (seen.has(v)) continue;
        seen.add(v);
        out.push(v);
      }
      return out;
    }),
});

export const PortfolioConfigSchema = z.object({
  valuesHidden: z.boolean().optional().catch(false),
});

export const AutomationsConfigSchema = z.object({
  limit: intInRange(8, 1, 100).optional().catch(8),
});

export const PW_TAB_KEYS = ['watchlist', 'portfolio'] as const;
export const PortfolioWatchlistConfigSchema = z.object({
  defaultTab: z.enum(PW_TAB_KEYS).optional().catch('watchlist'),
  valuesHidden: z.boolean().optional().catch(false),
});

export const WatchlistConfigSchema = z.object({}).catch({});

export const WorkspacePickerConfigSchema = z.object({
  limit: intInRange(12, 1, 100).optional().catch(12),
});

export const RecentThreadsConfigSchema = z.object({
  // 'all' | 'current' | <workspace UUID> — accept any non-empty string.
  workspaceId: z.string().min(1).optional().catch('all'),
  limit: intInRange(15, 1, 100).optional().catch(15),
});

export const EARNINGS_WINDOWS = ['1w', '2w', '1m'] as const;
export const EARNINGS_TICKERS = ['all', 'portfolio'] as const;
export const EarningsConfigSchema = z.object({
  window: z.enum(EARNINGS_WINDOWS).optional().catch('2w'),
  tickers: z.enum(EARNINGS_TICKERS).optional().catch('all'),
});

export const INSIGHT_BRIEF_VARIANTS = ['latest', 'personalized'] as const;
export const InsightBriefConfigSchema = z.object({
  variant: z.enum(INSIGHT_BRIEF_VARIANTS).optional().catch('latest'),
});

export const ConversationConfigSchema = z.object({}).catch({});

export const MarketsOverviewConfigSchema = z.object({
  indices: z.array(z.string().min(1)).optional().catch([]),
});

export const NEWS_FEED_SOURCES = ['market', 'portfolio', 'watchlist'] as const;
export const NewsFeedConfigSchema = z.object({
  source: z.enum(NEWS_FEED_SOURCES).optional().catch('market'),
  limit: intInRange(50, 1, 200).optional().catch(50),
});
