/**
 * CMDP progressive bars protocol — frontend client, types, and currency-aware
 * price formatting. See ./marketProtocol for the wire contract.
 */
export {
  INTERVAL_TO_SCHEMA,
  SCHEMA_IDS,
  intervalToSchema,
} from './marketProtocol';
export type {
  BarsCache,
  BarsPage,
  BarsResponse,
  BarsSeries,
  ChartBar,
  LoaderMeta,
  SchemaId,
  SeriesHeader,
  SeriesRecord,
} from './marketProtocol';

export {
  BarsNotAvailableError,
  coerceWatermark,
  fetchBarsSeries,
  headerToMeta,
  rowsToChartBars,
  toChartBars,
} from './barsClient';
export type { FetchBarsOptions, RawBarRow } from './barsClient';

export { fetchStockData } from './legacyBars';
export type { StockDataResult } from './legacyBars';

export {
  currencyForSymbol,
  currencySymbol,
  formatPrice,
  resolveDisplayCurrency,
} from './currencyDisplay';

export {
  FOREIGN_EXCHANGES,
  US_MARKET_TZ,
  isUSEquity,
  timezoneForSymbol,
} from './exchanges';
export type { ExchangeInfo } from './exchanges';

export { RANGE_PRESETS, rangeStartChartSec } from './rangePresets';
export type { RangePreset } from './rangePresets';

export {
  advanceWatermark,
  centerLatestBarView,
  computeInitialLoadRange,
  dedupeMergeByTime,
  etDateStr,
  fetchBarsDelta,
  rangeBeforeOldest,
  shouldSkipPollWhileWsHealthy,
} from './chartDataLoaders';
export type { BarsDeltaResult, TimedBar } from './chartDataLoaders';

export {
  AUTO_FIT_BARS,
  BARS_PER_DAY,
  DELTA_POLL_CADENCE_MS,
  INITIAL_LOAD_DAYS,
  INTERVAL_LABEL,
  INTERVAL_SECONDS,
  INTERVALS,
  STAGE1_LOAD_DAYS,
  WS_FOLD_INTERVALS,
  WS_RECONCILE_POLL_MS,
  WS_STALE_WINDOW_MS,
} from './chartConstants';
export type { IntervalConfig } from './chartConstants';

export {
  applyQuoteToDailyBar,
  foldMinuteBar,
  isSettledDailyHead,
  isSettledDailyHeadTime,
} from './formingBar';
export type { QuoteLike } from './formingBar';

export {
  EXTENDED_HOURS_INTERVALS,
  deriveMarketSession,
  getExtendedHoursType,
} from './marketSession';
export type {
  ExtendedHoursType,
  MarketSession,
  MarketSessionInputs,
  PriceMark,
  VenueBadge,
} from './marketSession';

export { useCurrencyDisplay } from './useCurrencyDisplay';
export type {
  CurrencyMeta,
  DisplayCurrency,
  UseCurrencyDisplay,
} from './useCurrencyDisplay';

export { useLiveBars } from './useLiveBars';
export type {
  LiveBarsController,
  LiveBarsMeta,
  UseLiveBarsOptions,
} from './useLiveBars';
