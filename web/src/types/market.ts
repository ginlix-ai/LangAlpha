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
  previousClose?: number | null;
}

export interface IndicesResponse {
  indices: IndexData[];
  failedCount: number;
}

// --- Stock Snapshot ---

export interface SnapshotData {
  symbol: string;
  price: number | null;
  change?: number;
  change_percent?: number;
  previous_close?: number;
  name?: string;
  open?: number;
  high?: number;
  low?: number;
  volume?: number;
  early_trading_change_percent?: number | null;
  late_trading_change_percent?: number | null;
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
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
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
  Name: string;
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
