/** Market data types — indices, snapshots, stock quotes, chart data */

// --- Index Data ---

export interface SparklinePoint {
  time: string;
  val: number;
}

export interface IndexData {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
  sparklineData: SparklinePoint[];
  quoteAvailable?: boolean;
  /** ET date (YYYY-MM-DD) of the trading session the price/sparkline reflect. */
  asOfDate?: string;
  previousClose?: number | null;
}

export interface IndicesResponse {
  indices: IndexData[];
  failedCount: number;
}

// --- Freshness ---

/**
 * Freshness measured at the REST boundary against the venue clock. The
 * declared tier says what a provider sells; this says what actually arrived,
 * so `measured: false` (nothing carried a timestamp) means the label is only
 * the declaration repeated back.
 */
export interface Freshness {
  /** Newest bar/print that should exist now (Unix ms). */
  expected_latest?: number | null;
  /** Newest bar/print actually served (Unix ms). */
  actual_latest?: number | null;
  /** Seconds behind; on a closed venue, the shortfall against the final bar. */
  lag_s?: number | null;
  label: 'live' | 'delayed' | 'stale' | 'incomplete' | 'unknown';
  measured: boolean;
  /** Provider that filled the measured series or quote. */
  source?: string | null;
  /** Bar interval the measurement is against; null for quotes. */
  interval?: string | null;
  /** Venue fully closed when measured: a live row is the session's final
   *  print, not a moving price. Null when no venue resolved. */
  closed?: boolean | null;
}

/** Freshness a provider declares it sells for a quote. */
export type QuoteTier = 'realtime' | 'delayed_15m' | 'eod';

// --- Stock Snapshot ---

/**
 * One snapshot row as the snapshot endpoints send it (batch and single-symbol
 * alike). The one wire type: the quote layer's `QuoteRow` and the dashboard's
 * `SnapshotEntry` are this row, so a field the backend adds is declared once.
 * An absent value arrives as null.
 */
export interface SnapshotData {
  symbol: string;
  /** The request's own spellings this row answers (`600519.SS` asked, `600519.SH` shown). */
  requested?: string[] | null;
  price?: number | null;
  /** ISO 4217 code the prices are quoted in, resolved per symbol by the API. */
  currency?: string | null;
  /** `index` quotes a level in points, not a price. */
  asset_class?: string | null;
  change?: number | null;
  change_percent?: number | null;
  previous_close?: number | null;
  name?: string | null;
  name_local?: string | null;
  name_en?: string | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  volume?: number | null;
  last_minute_close?: number | null;
  regular_close?: number | null;
  regular_trading_change?: number | null;
  early_trading_change?: number | null;
  early_trading_change_percent?: number | null;
  late_trading_change?: number | null;
  late_trading_change_percent?: number | null;
  source?: string | null;
  /** Freshness the filling provider declares; the quote batcher narrows it with `asQuoteTier`. */
  tier?: QuoteTier | null;
  /** Time of the quoted print (Unix ms) when the provider reports one. */
  as_of?: number | null;
  /** Freshness measured for this quote at response time. */
  freshness?: Freshness | null;
  [key: string]: unknown;
}

export interface SnapshotBatchResponse {
  snapshots?: SnapshotData[];
  results?: SnapshotData[];
  data?: SnapshotData[];
  count?: number;
}

// --- Stock Price ---

export interface StockPrice {
  symbol: string;
  /** ISO 4217 code from the snapshot; absent on older payloads. */
  currency?: string | null;
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
  quoteAvailable?: boolean;
  previousClose?: number | null;
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
}

// --- Chart Data ---

export interface ChartDataPoint {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface FetchStockDataResult {
  data: ChartDataPoint[];
  error?: string;
  fiftyTwoWeekHigh?: number;
  fiftyTwoWeekLow?: number;
}

// --- Stock Quote ---

export interface StockInfo {
  Symbol: string;
  /** Null when no source names the listing; the reader decides what stands in. */
  Name: string | null;
  NameLocal?: string | null;
  NameEn?: string | null;
  Exchange: string;
  Price: number;
  Open: number;
  High: number;
  Low: number;
  Volume?: number;
  '52WeekHigh': number | null;
  '52WeekLow': number | null;
  AverageVolume: number | null;
  SharesOutstanding: number | null;
  MarketCapitalization: number | null;
  DividendYield: number | null;
}

export interface RealTimePrice {
  symbol: string;
  price: number;
  open: number;
  high: number;
  low: number;
  change: number;
  changePercent: number;
  volume: number;
  previousClose: number;
}

export interface StockQuoteResult {
  stockInfo: StockInfo;
  realTimePrice: RealTimePrice | null;
  snapshot: SnapshotData | null;
}

// --- Market Status ---

export interface MarketStatus {
  market: string;
  serverTime: string;
  exchanges: Record<string, unknown>;
  currencies: Record<string, unknown>;
  [key: string]: unknown;
}

// --- WebSocket ---

export type MarketType = 'stock' | 'index' | 'crypto' | 'forex';
export type WSInterval = 'second' | 'minute';
