/**
 * Types for the CMDP progressive bars protocol endpoint:
 *
 *   GET /api/v1/market-data/bars/{instrument}?schema=ohlcv-1m[&after=<watermark_ms>]
 *     → { series: { header, records }, page, cache }
 *
 * Every interface carries a `[key: string]: unknown` escape hatch so the client
 * survives additive backend fields without a frontend redeploy — we only pin
 * the fields the chart layer actually reads.
 */

/** Canonical schema ids the protocol endpoint accepts. */
export type SchemaId =
  | 'ohlcv-1s'
  | 'ohlcv-1m'
  | 'ohlcv-5m'
  | 'ohlcv-15m'
  | 'ohlcv-30m'
  | 'ohlcv-1h'
  | 'ohlcv-4h'
  | 'ohlcv-1d';

export const SCHEMA_IDS: readonly SchemaId[] = [
  'ohlcv-1s',
  'ohlcv-1m',
  'ohlcv-5m',
  'ohlcv-15m',
  'ohlcv-30m',
  'ohlcv-1h',
  'ohlcv-4h',
  'ohlcv-1d',
];

/**
 * Legacy interval strings ("1min", "1hour", ...) → protocol schema ids. The
 * mapping is 1:1 with the vocabulary the chart toolbar already emits.
 * (`ohlcv-1s` stays in {@link SCHEMA_IDS} — it's still on the wire as the WS
 * forming-bar record schema — but no chart interval maps to it.)
 */
export const INTERVAL_TO_SCHEMA: Record<string, SchemaId> = {
  '1min': 'ohlcv-1m',
  '5min': 'ohlcv-5m',
  '15min': 'ohlcv-15m',
  '30min': 'ohlcv-30m',
  '1hour': 'ohlcv-1h',
  '4hour': 'ohlcv-4h',
  '1day': 'ohlcv-1d',
};

/** Map a legacy interval string to a schema id (defaults to 1m). */
export function intervalToSchema(interval: string): SchemaId {
  return INTERVAL_TO_SCHEMA[interval] ?? 'ohlcv-1m';
}

/** Series envelope header — instrument + presentation metadata. */
export interface SeriesHeader {
  instrument_key?: string;
  schema?: string;
  publisher?: string;
  price_treatment?: string;
  tier?: string | number;
  price_currency?: string;
  display_unit?: string;
  display_decimals?: number;
  ts_unit?: string;
  latest_trading_date?: string;
  revision?: number;
  watermark?: number | string | null;
  market_phase?: string;
  [key: string]: unknown;
}

/** One OHLCV bar record. Timestamps are epoch-ms (`header.ts_unit === "ms"`). */
export interface SeriesRecord {
  time?: number;
  ts_event?: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  is_final?: boolean;
  [key: string]: unknown;
}

export interface BarsSeries {
  header: SeriesHeader;
  records: SeriesRecord[];
}

export interface BarsPage {
  next_cursor: string | null;
  has_more: boolean;
  [key: string]: unknown;
}

export interface BarsCache {
  cached: boolean;
  cache_key: string | null;
  /** Venue market phase (`pre|open|post|closed`), calendar-derived server-side. */
  market_phase?: string | null;
  /** Epoch ms of the phase's next calendar boundary; null for 24/7 venues. */
  next_change_at?: number | null;
  [key: string]: unknown;
}

export interface BarsResponse {
  series: BarsSeries;
  page: BarsPage;
  cache: BarsCache;
  [key: string]: unknown;
}

/**
 * The chart's native bar shape (lightweight-charts). `time` is venue
 * wall-clock seconds (`utcMsToChartSec` with the symbol's market timezone),
 * NOT a raw epoch — so it can be dedupe-merged with bars from the legacy
 * loader.
 */
export interface ChartBar {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

/**
 * Metadata the loaders surface to chart consumers. Sourced from either the
 * protocol series header or the legacy REST envelope, normalized to one shape.
 * `watermark` is the backend's epoch-ms high-water mark used to drive `after=`
 * delta polls; `currency`/`displayDecimals` are only present when the protocol
 * endpoint served the data.
 */
export interface LoaderMeta {
  watermark: number | null;
  complete: boolean;
  marketPhase: string | null;
  /** Epoch ms of the phase's next calendar boundary (protocol endpoint only). */
  nextChangeAt?: number | null;
  truncated?: boolean;
  cached?: boolean;
  currency?: string;
  displayDecimals?: number;
  revision?: number;
}
