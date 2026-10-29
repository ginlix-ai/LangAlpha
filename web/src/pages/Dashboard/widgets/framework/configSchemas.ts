import * as z from 'zod/mini';

/**
 * Per-widget Zod schemas applied at the prefs-load boundary
 * (`migrations.ts → sanitizeConfig`). Per-field `catch` recovers
 * individual bad values; whole-config failure falls back to `defaultConfig`.
 *
 * Centralizing schemas in one file keeps the contract visible: schema
 * changes here are intentional, not buried in 30 widget files.
 *
 * Enum tuples are exported `as const` so settings dialogs can derive their
 * dropdown options from the same source of truth and never drift from the
 * schema (Zod v4's `.options` is hidden after `catch` is applied, so keep
 * the const around).
 */

// TV symbol grammar: prefix:base, dots in tickers (BRK.B), futures (`!`),
// forex slash (`/`), TVC indices (`^`), &, underscores in real exchange
// prefixes (CME_MINI:ES1!, FX_IDC:EURUSD, KRX:HMM_T), plus uppercase +
// digits + dash. Deliberately permissive — TV's wider grammar would
// otherwise drop valid configs the user typed in good faith.
const TV_SYMBOL_RE = /^[A-Z0-9._\-:!/^&]+$/i;

const nonEmpty = () => z.string().check(z.minLength(1));
const tvSymbol = (def: string) =>
  z.catch(z.string().check(z.minLength(1), z.regex(TV_SYMBOL_RE)), def);
const intInRange = (def: number, min: number, max: number) =>
  z.catch(z.int().check(z.gte(min), z.lte(max)), def);
const looseString = (def: string) => z.catch(nonEmpty(), def);
// `catch` stays outside `optional`: it is what yields the default for undefined.
const optCatch = <T extends z.ZodMiniType>(schema: T, def: z.output<T>) =>
  z.catch(z.optional(schema), def);
// Drop invalid entries and dedupe. A list that ends up empty (or was not an
// array) falls back to `fallback`; every caller wants that, and for [] it is a no-op.
const dedupedList = (isValid: (v: string) => boolean, fallback: readonly string[]) =>
  z.pipe(
    z.catch(z.array(z.unknown()), [...fallback]),
    z.transform((arr) => {
      const out = [...new Set(arr.filter((v): v is string => typeof v === 'string' && isValid(v)))];
      return out.length === 0 ? [...fallback] : out;
    }),
  );

// =============================================================================
// TV widgets (17)
// =============================================================================

export const TICKER_TAPE_DISPLAY_MODES = ['adaptive', 'regular', 'compact'] as const;
export const TickerTapeConfigSchema = z.object({
  // Drop invalid entries (and dedupe) instead of per-element catch(default):
  // a corrupted blob with [bad, bad, NVDA] becomes [NVDA] rather than
  // [SPY, SPY, NVDA] (silent duplication). User keeps every valid symbol.
  symbols: dedupedList((v) => TV_SYMBOL_RE.test(v), []),
  displayMode: z.catch(z.enum(TICKER_TAPE_DISPLAY_MODES), 'adaptive'),
});

export const STOCK_HEATMAP_DATA_SOURCES = [
  'SPX500', 'NASDAQ100', 'DOW30', 'AllUSA', 'Asia', 'Europe', 'crypto',
] as const;
export const STOCK_HEATMAP_BLOCK_SIZES = ['market_cap_basic', 'volume', 'number_of_employees'] as const;
export const STOCK_HEATMAP_BLOCK_COLORS = ['change', 'Perf.W', 'Perf.1M', 'Perf.YTD', 'Perf.Y'] as const;
export const StockHeatmapConfigSchema = z.object({
  dataSource: looseString('SPX500'),
  blockSize: looseString('market_cap_basic'),
  blockColor: looseString('change'),
});

export const CryptoHeatmapConfigSchema = z.object({
  dataSource: looseString('Crypto'),
  blockSize: looseString('market_cap_calc'),
  blockColor: looseString('24h_close_change|5'),
});

export const ETFHeatmapConfigSchema = z.object({
  dataSource: looseString('AllUSEtf'),
  blockSize: looseString('aum'),
  blockColor: looseString('change'),
  grouping: looseString('asset_class'),
});

// Mirror of DEFAULT_CURRENCIES in ForexHeatmapWidget — kept here so the
// schema's whole-array fallback matches the widget's defaultConfig (a
// single-currency catch produces a useless cross-rate widget).
export const FOREX_DEFAULT_CURRENCIES = [
  'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'CNY',
] as const;
export const ForexHeatmapConfigSchema = z.object({
  // Drop bad currency codes (and dedupe) rather than per-element catch('USD')
  // which would turn [JPY, garbage, EUR] into [USD, USD, EUR]. If the whole
  // value is malformed (not an array), fall back to the full default set so
  // the cross-rate widget still renders something useful.
  currencies: dedupedList((v) => /^[A-Z]{3}$/.test(v), FOREX_DEFAULT_CURRENCIES),
});

export const EconomicEventsConfigSchema = z.object({
  // minLength(1) guards against an empty string sneaking past validation: TV
  // would render an empty calendar instead of falling back to the catch.
  importanceFilter: looseString('-1,0,1'),
  countryFilter: looseString('us,eu,jp,gb,cn'),
});

export const ECONOMIC_MAP_REGIONS = [
  'global', 'africa', 'asia', 'europe', 'north-america', 'oceania', 'south-america',
] as const;
export const ECONOMIC_MAP_METRICS = ['gdp', 'ur', 'gdg', 'intr', 'iryy'] as const;
export const EconomicMapConfigSchema = z.object({
  region: z.catch(z.enum(ECONOMIC_MAP_REGIONS), 'global'),
  metric: z.catch(z.enum(ECONOMIC_MAP_METRICS), 'gdp'),
  hideLegend: z.catch(z.boolean(), false),
});

export const TechnicalsConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
  // TV interval grammar is wide ("1m", "5m", "1h", "1D", "1W"). Loose check.
  interval: looseString('1D'),
});

export const MoversConfigSchema = z.object({
  exchange: looseString('US'),
  dataSource: looseString('AllUSA'),
});

export const SymbolSpotlightConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
  range: looseString('12M'),
});

export const CompanyProfileConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
});

export const COMPANY_FIN_DISPLAY_MODES = ['regular', 'compact', 'adaptive'] as const;
export const CompanyFinancialsConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
  displayMode: z.catch(z.enum(COMPANY_FIN_DISPLAY_MODES), 'regular'),
});

export const TOP_STORIES_FEED_MODES = ['all_symbols', 'market', 'symbol'] as const;
export const TOP_STORIES_MARKETS = [
  'stock', 'crypto', 'forex', 'index', 'futures', 'bond', 'economic',
] as const;
export const TOP_STORIES_DISPLAY_MODES = ['regular', 'compact'] as const;
export const TopStoriesConfigSchema = z.object({
  feedMode: z.catch(z.enum(TOP_STORIES_FEED_MODES), 'market'),
  market: z.catch(z.enum(TOP_STORIES_MARKETS), 'stock'),
  symbol: tvSymbol('NASDAQ:NVDA'),
  displayMode: z.catch(z.enum(TOP_STORIES_DISPLAY_MODES), 'regular'),
});

export const SingleTickerConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
});

export const SymbolInfoConfigSchema = z.object({
  symbol: tvSymbol('NASDAQ:NVDA'),
});

export const StockScreenerConfigSchema = z.object({
  market: looseString('america'),
  defaultColumn: looseString('overview'),
  defaultScreen: looseString('general'),
});

export const CryptoScreenerConfigSchema = z.object({
  defaultColumn: looseString('overview'),
  defaultScreen: looseString('general'),
});

// =============================================================================
// Native widgets (13)
// =============================================================================

export const CHART_INTERVALS = ['1min', '5min', '15min', '30min', '1hour', '1day'] as const;
export const CHART_TYPES = ['candle', 'area', 'line'] as const;
export const ChartConfigSchema = z.object({
  symbol: looseString('NVDA'),
  interval: z.catch(z.enum(CHART_INTERVALS), '1day'),
  chartType: z.catch(z.enum(CHART_TYPES), 'candle'),
});

export const MiniChartGridConfigSchema = z.object({
  // No regex — accepts plain symbols ("NVDA") and TV-style ("NASDAQ:NVDA").
  // Filter non-strings + empties, dedupe; return [] if all entries are bad
  // (the widget falls back to watchlist/blue-chips at render time).
  symbols: dedupedList((v) => v.length > 0, []),
});

export const PortfolioConfigSchema = z.object({
  valuesHidden: optCatch(z.boolean(), false),
});

export const AutomationsConfigSchema = z.object({
  limit: optCatch(intInRange(8, 1, 100), 8),
});

export const PW_TAB_KEYS = ['watchlist', 'portfolio'] as const;
export const PortfolioWatchlistConfigSchema = z.object({
  defaultTab: optCatch(z.enum(PW_TAB_KEYS), 'watchlist'),
  valuesHidden: optCatch(z.boolean(), false),
});

export const WatchlistConfigSchema = z.catch(z.object({}), {});

export const WorkspacePickerConfigSchema = z.object({
  limit: optCatch(intInRange(12, 1, 100), 12),
});

export const RecentThreadsConfigSchema = z.object({
  // 'all' | 'current' | <workspace UUID> — accept any non-empty string.
  workspaceId: optCatch(nonEmpty(), 'all'),
  limit: optCatch(intInRange(15, 1, 100), 15),
});

export const EARNINGS_WINDOWS = ['1w', '2w', '1m'] as const;
export const EARNINGS_TICKERS = ['all', 'portfolio'] as const;
export const EarningsConfigSchema = z.object({
  window: optCatch(z.enum(EARNINGS_WINDOWS), '2w'),
  tickers: optCatch(z.enum(EARNINGS_TICKERS), 'all'),
});

export const INSIGHT_BRIEF_VARIANTS = ['latest', 'personalized'] as const;
export const InsightBriefConfigSchema = z.object({
  variant: optCatch(z.enum(INSIGHT_BRIEF_VARIANTS), 'latest'),
});

export const ConversationConfigSchema = z.catch(z.object({}), {});

export const MarketsOverviewConfigSchema = z.object({
  indices: optCatch(z.array(nonEmpty()), []),
});

export const NEWS_FEED_SOURCES = ['top', 'market', 'portfolio', 'watchlist'] as const;
export const NewsFeedConfigSchema = z.object({
  source: optCatch(z.enum(NEWS_FEED_SOURCES), 'market'),
  limit: optCatch(intInRange(50, 1, 200), 50),
});
