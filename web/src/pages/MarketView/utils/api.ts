/**
 * MarketView API utilities
 * All backend endpoints used by the MarketView page
 */
import { api } from '@/api/client';
import { normalizeIndexKey } from '@/lib/marketUtils';
import { isIndexFamilySpelling } from '@/lib/bars/exchanges';
import type { RealTimePrice, SnapshotData, StockInfo, StockQuoteResult } from '@/types/market';
import { quoteDecimals, roundQuote } from '@/lib/quotes/quoteAdapters';
import type { CompanyOverviewArtifact } from '@/lib/quotes/overview';

// Legacy full-window bar loader now lives in lib/bars (so lib/ never imports a
// page); re-exported for page-internal callers that still import it from here.
export { fetchStockData } from '@/lib/bars/legacyBars';
export type { StockDataResult } from '@/lib/bars/legacyBars';

const baseURL = api.defaults.baseURL;

/**
 * Build the WebSocket URL for the market data aggregate stream.
 * Converts the HTTP baseURL (e.g. http://localhost:8000) to ws:// scheme.
 * @param {string} [market='stock'] - Market type (stock, index, crypto, forex)
 * @param {string} [interval='second'] - Aggregate interval (second, minute)
 * @returns {string} Full WS URL with path
 */
export function getMarketDataWSUrl(market: string = 'stock', interval: string = 'second'): string {
  const wsBase = baseURL
    ? (baseURL as string).replace(/^http/, 'ws')
    : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}`;
  return `${wsBase}/ws/v1/market-data/aggregates/${market}?interval=${interval}`;
}

/**
 * GET /api/v1/market-data/snapshots/stocks/{symbol} — single stock snapshot
 * Returns snapshot data with name, price, change, previous_close, open, high, low, volume, etc.
 */
export async function fetchSnapshot(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<SnapshotData | null> {
  if (!symbol || !symbol.trim()) throw new Error('Symbol is required');
  const symbolUpper = symbol.trim().toUpperCase();
  const isIndex = isIndexFamilySpelling(symbolUpper);
  const norm = normalizeIndexKey(symbolUpper);
  const endpoint = isIndex
    ? `/api/v1/market-data/snapshots/indexes?symbols=${encodeURIComponent(norm)}`
    : `/api/v1/market-data/snapshots/stocks/${encodeURIComponent(symbolUpper)}`;
  try {
    const { data } = await api.get(endpoint, { signal });
    // Single stock endpoint returns SnapshotData directly;
    // index batch returns { snapshots: [...], count } — extract first match
    if (isIndex) {
      const results: SnapshotData[] = data?.snapshots || data?.results || [];
      if (Array.isArray(results)) return results.find((s: SnapshotData) => normalizeIndexKey(s.symbol) === norm) || results[0] || null;
      return null;
    }
    return data || null;
  } catch (error: unknown) {
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) throw error;
    console.error('Error fetching snapshot:', error);
    return null;
  }
}

/**
 * Pure transform: raw snapshot row → { stockInfo, realTimePrice, snapshot }.
 * A null/price-less snapshot yields the fallback shape (no realTimePrice). Kept
 * side-effect-free so both `fetchStockQuote` and the quote-layer-driven
 * `useStockData` derive an identical result from the same snapshot.
 */
export function mapSnapshotToStockQuote(symbol: string, snap: SnapshotData | null): StockQuoteResult {
  const symbolUpper = symbol.trim().toUpperCase();
  const fallbackInfo: StockInfo = {
    Symbol: symbolUpper,
    // No name and no exchange yet: invent neither a company name nor a venue
    // (a CN or HK listing is not on NASDAQ).
    Name: null,
    Exchange: '',
    Price: 0,
    Open: 0,
    High: 0,
    Low: 0,
    '52WeekHigh': null,
    '52WeekLow': null,
    AverageVolume: null,
    SharesOutstanding: null,
    MarketCapitalization: null,
    DividendYield: null,
  };

  if (!snap || snap.price == null) {
    return { stockInfo: fallbackInfo, realTimePrice: null, snapshot: null };
  }

  const price = snap.price;
  const previousClose = snap.previous_close ?? 0;
  const change = snap.change ?? (price - previousClose);
  const changePct = snap.change_percent != null
    ? parseFloat(snap.change_percent.toFixed(2))
    : (previousClose ? parseFloat(((change / previousClose) * 100).toFixed(2)) : 0);

  const stockInfo: StockInfo = {
    Symbol: symbolUpper,
    Name: snap.name || null,
    NameLocal: snap.name_local,
    NameEn: snap.name_en,
    Exchange: '',
    Price: price,
    Open: snap.open ?? 0,
    High: snap.high ?? 0,
    Low: snap.low ?? 0,
    Volume: snap.volume ?? 0,
    '52WeekHigh': null,
    '52WeekLow': null,
    AverageVolume: null,
    SharesOutstanding: null,
    MarketCapitalization: null,
    DividendYield: null,
  };

  const dp = quoteDecimals(snap);
  const realTimePrice: RealTimePrice = {
    symbol: symbolUpper,
    price: roundQuote(price, dp),
    open: roundQuote(snap.open ?? 0, dp),
    high: roundQuote(snap.high ?? 0, dp),
    low: roundQuote(snap.low ?? 0, dp),
    change: roundQuote(change, dp),
    changePercent: changePct,
    volume: snap.volume ?? 0,
    previousClose: roundQuote(previousClose, dp),
  };

  return { stockInfo, realTimePrice, snapshot: snap };
}

/**
 * Consolidated stock quote — uses snapshot endpoint for accurate price/change data.
 * Returns { stockInfo, realTimePrice, snapshot } where snapshot is the raw snapshot data.
 */
export async function fetchStockQuote(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<StockQuoteResult> {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  try {
    const snap = await fetchSnapshot(symbol, { signal });
    return mapSnapshotToStockQuote(symbol, snap);
  } catch (error: unknown) {
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) {
      throw error;
    }
    console.error('Error fetching stock quote:', error);
    return mapSnapshotToStockQuote(symbol, null);
  }
}


/**
 * Fetch company overview data (fundamentals, analyst ratings, earnings, revenue breakdown).
 * Uses backend API endpoint: GET /api/v1/market-data/stocks/{symbol}/overview
 *
 * @param {string} symbol - Stock symbol
 * @param {Object} [options] - Additional options
 * @param {AbortSignal} [options.signal] - AbortController signal for cancellation
 * @returns {Promise<Object>} Company overview data
 */
export async function fetchCompanyOverview(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<CompanyOverviewArtifact> {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }
  const { data } = await api.get(
    `/api/v1/market-data/stocks/${encodeURIComponent(symbol.trim().toUpperCase())}/overview`,
    { signal }
  );
  return data;
}

/**
 * Fetch analyst data (price targets + grades) for a stock symbol.
 * Uses backend API endpoint: GET /api/v1/market-data/stocks/{symbol}/analyst-data
 *
 * @param {string} symbol - Stock symbol
 * @param {Object} [options] - Additional options
 * @param {AbortSignal} [options.signal] - AbortController signal for cancellation
 * @returns {Promise<Object>} Analyst data with priceTargets and grades
 */
export async function fetchAnalystData(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<unknown> {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }
  try {
    const { data } = await api.get(
      `/api/v1/market-data/stocks/${encodeURIComponent(symbol.trim().toUpperCase())}/analyst-data`,
      { signal }
    );
    return data;
  } catch (error: unknown) {
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) {
      throw error;
    }
    console.error('Error fetching analyst data:', error);
    return null;
  }
}

/**
 * Delete a thread
 * @param {string} threadId - Thread ID to delete
 * @returns {Promise<void>}
 */
export async function deleteMarketThread(threadId: string): Promise<void> {
  if (!threadId || threadId === '__default__') {
    return; // Don't delete default placeholder
  }
  try {
    await api.delete(`/api/v1/threads/${threadId}`);
  } catch (error: unknown) {
    // Silently fail - thread might already be deleted
    console.warn('[MarketView] Failed to delete thread:', threadId, error);
  }
}
