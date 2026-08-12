/**
 * Dashboard API utilities
 * All backend endpoints used by the Dashboard page
 */
import { api } from '@/api/client';
import { utcMsToETDate, utcMsToETTime } from '@/lib/utils';
import { normalizeIndexKey } from '@/lib/marketUtils';
import { snapshotToStockPrice } from '@/lib/quotes/quoteAdapters';
import { getSnapshotIndexes, getSnapshotStocks } from '@/lib/quotes/snapshotApi';
import type { SnapshotEntry, SnapshotResponse } from '@/lib/quotes/snapshotApi';
import type { IndexData, SparklinePoint } from '@/types/market';
import * as portfolioApi from './portfolio';
import * as watchlistApi from './watchlist';
import * as watchlistItemsApi from './watchlistItems';

// Snapshot batch primitives moved to lib/quotes (breaking the lib→page cycle);
// re-exported here for back-compat with existing Dashboard callers/tests.
export { getSnapshotIndexes, getSnapshotStocks };
export type { SnapshotEntry, SnapshotResponse };

// --- Interfaces ---

interface IntradayPoint {
  time: number;
  open: number;
  close: number;
  high?: number;
  low?: number;
  volume?: number;
}

interface StockPrice {
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
  quoteAvailable?: boolean;
  previousClose?: number | null;
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
}

interface IndicesResult {
  indices: IndexData[];
  failedCount: number;
}

interface NewsParams {
  tickers?: string[];
  limit?: number;
  cursor?: string;
  provider?: string;
}

interface NewsResponse {
  results: Record<string, unknown>[];
  count: number;
  next_cursor: string | null;
}

interface EarningsParams {
  from?: string;
  to?: string;
}

interface EarningsResponse {
  data: Record<string, unknown>[];
  count: number;
}

// --- Market data (see docs/ptc-agent-api/market data) ---

/** Index symbols: normalized (GSPC, IXIC, DJI, RUT). Index.yml / Index Batch.yml use these. */
const INDEX_SYMBOLS: string[] = ['GSPC', 'IXIC', 'DJI', 'RUT', 'VIX'];
const INDEX_NAMES: Record<string, string> = { GSPC: 'S&P 500', IXIC: 'NASDAQ', DJI: 'Dow Jones', RUT: 'Russell 2000', VIX: 'VIX' };

// Trim + strip a leading '^' + uppercase — the shared symbol-key normalizer.
const normalizeIndexSymbol = normalizeIndexKey;

function fallbackIndex(norm: string): IndexData {
  return {
    symbol: norm,
    name: INDEX_NAMES[norm] ?? norm,
    price: 0,
    change: 0,
    changePercent: 0,
    isPositive: true,
    sparklineData: [],
    quoteAvailable: false,
  };
}

/**
 * GET /api/v1/market-data/intraday/indexes/:symbol (Index.yml)
 * Path uses normalized symbol (e.g. GSPC). Query: interval, from, to optional.
 * Returns the most recent data point for the index.
 */
export async function getIndex(symbol: string, _opts: Record<string, unknown> = {}): Promise<IndexData> {
  const norm = normalizeIndexSymbol(String(symbol).trim());
  try {
    const { data } = await api.get(`/api/v1/market-data/intraday/indexes/${encodeURIComponent(norm)}`);

    const pts: IntradayPoint[] = data?.data ?? [];

    if (!Array.isArray(pts) || !pts.length) {
      throw new Error(`No intraday data for ${norm}`);
    }

    // Sort ascending by time (Unix ms)
    const sorted = [...pts].sort((a: IntradayPoint, b: IntradayPoint) => a.time - b.time);

    // Isolate regular-hours points (9:30–16:00 ET) first, then take the most
    // recent date that actually has them. Some indices (e.g. VIX) carry
    // overnight/pre-market bars, so the chronologically-last bar's date can have
    // zero regular-hours points on a pre-open day — which would blank the
    // sparkline. `sorted` is ascending, so the regular-hours slice is too.
    const regularHours = sorted.filter((p: IntradayPoint) => {
      const t = utcMsToETTime(p.time);
      return t >= '09:30' && t <= '16:00';
    });
    const latestDate = regularHours.length
      ? utcMsToETDate(regularHours[regularHours.length - 1].time)
      : utcMsToETDate(sorted[sorted.length - 1].time);
    const todayPoints = regularHours.length
      ? regularHours.filter((p: IntradayPoint) => utcMsToETDate(p.time) === latestDate)
      : sorted.filter((p: IntradayPoint) => utcMsToETDate(p.time) === latestDate);

    const oldest = todayPoints[0];
    const mostRecent = todayPoints[todayPoints.length - 1];

    const open = Number(oldest?.open ?? 0);
    const close = Number(mostRecent?.close ?? 0);
    const change = close - open;
    const changePercent = open ? (change / open) * 100 : 0;

    const result: IndexData = {
      symbol: norm,
      name: INDEX_NAMES[norm] ?? norm,
      price: Math.round(close * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent: Math.round(changePercent * 100) / 100,
      isPositive: change >= 0,
      // Direct getIndex consumers get the same zero/invalid-close masking the
      // dashboard applies via the snapshot: a non-positive close isn't a quote.
      quoteAvailable: close > 0,
      asOfDate: latestDate,
      sparklineData: todayPoints
        .filter((p: IntradayPoint) => Number(p.close) > 0)
        .map((p: IntradayPoint) => ({ time: utcMsToETTime(p.time), val: Number(p.close) })),
    };

    return result;
  } catch (e: unknown) {
    const err = e as { response?: { status?: number; data?: { detail?: unknown } }; message?: string };
    console.error(`[API] getIndex - ${norm}: Error:`, err?.message);
    const msg = err.response?.data?.detail ?? err.message;
    throw new Error(typeof msg === 'string' ? msg : String(msg));
  }
}

/** Minimal snapshot shape buildIndexData reads — satisfied by both the local
 *  SnapshotEntry and the quote layer's QuoteRow (whose price is nullable). */
interface IndexSnapshotLike {
  symbol?: string;
  name?: string;
  price?: number | null;
  change?: number | null;
  change_percent?: number | null;
  previous_close?: number | null;
}

/**
 * Build one IndexData card from a raw index snapshot + its sparkline.
 * Index prices are never legitimately 0; a zero/negative price means a partial
 * provider snapshot, so it's treated as no quote (renders N/A, not a fake 0.00).
 * Shared by getIndices and the quote-layer-driven useDashboardData.
 */
export function buildIndexData(
  norm: string,
  snap: IndexSnapshotLike | undefined,
  sparklineData: SparklinePoint[] = [],
  asOfDate?: string,
): IndexData {
  if (snap && snap.price != null && snap.price > 0) {
    const change = snap.change ?? 0;
    const changePct = snap.change_percent ?? (snap.previous_close ? ((change / snap.previous_close) * 100) : 0);
    return {
      symbol: norm,
      name: INDEX_NAMES[norm] ?? snap.name ?? norm,
      price: Math.round(snap.price * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent: Math.round(changePct * 100) / 100,
      isPositive: change >= 0,
      previousClose: snap.previous_close ?? null,
      sparklineData,
      quoteAvailable: true,
      asOfDate,
    };
  }
  return { ...fallbackIndex(norm), sparklineData, asOfDate };
}

/**
 * Fetches indices data: snapshot batch for price/change, intraday for sparklines.
 * Returns { indices, failedCount }.
 */
export async function getIndices(symbols: string[] = INDEX_SYMBOLS, _opts: Record<string, unknown> = {}): Promise<IndicesResult> {
  const list = symbols.map((s: string) => normalizeIndexSymbol(String(s).trim()));

  // Fetch snapshot (price/change) and intraday (sparklines) in parallel
  const [snapshots, sparklineResults] = await Promise.all([
    getSnapshotIndexes(list),
    Promise.all(list.map(async (norm: string) => {
      try {
        const result = await getIndex(norm);
        return { symbol: norm, sparklineData: result.sparklineData, asOfDate: result.asOfDate };
      } catch {
        return { symbol: norm, sparklineData: [] as SparklinePoint[], asOfDate: undefined };
      }
    })),
  ]);

  const sparklineMap: Record<string, SparklinePoint[]> = Object.fromEntries(sparklineResults.map((r) => [r.symbol, r.sparklineData]));
  const asOfMap: Record<string, string | undefined> = Object.fromEntries(sparklineResults.map((r) => [r.symbol, r.asOfDate]));
  const snapshotList: SnapshotEntry[] = snapshots?.snapshots || snapshots?.results || snapshots?.data || [];
  const snapshotMap: Record<string, SnapshotEntry> = Array.isArray(snapshotList)
    ? Object.fromEntries(snapshotList.map((s: SnapshotEntry) => [normalizeIndexSymbol(s.symbol), s]))
    : {};

  let failedCount = 0;
  const indices: IndexData[] = list.map((norm: string) => {
    const idx = buildIndexData(norm, snapshotMap[norm], sparklineMap[norm] || [], asOfMap[norm]);
    if (!idx.quoteAvailable) failedCount++;
    return idx;
  });

  return { indices, failedCount };
}

export { INDEX_NAMES, INDEX_SYMBOLS, fallbackIndex, normalizeIndexSymbol };

// --- Hello ---

export async function fetchHello(): Promise<string> {
  const { data } = await api.get('/hello', { responseType: 'text' });
  return data;
}

// --- Users ---

export async function createUser(userData: Record<string, unknown>): Promise<Record<string, unknown>> {
  const { data } = await api.post('/api/v1/users', userData);
  return data;
}

export async function getCurrentUser(params?: { refresh_tier?: boolean }): Promise<Record<string, unknown>> {
  const { data } = await api.get('/api/v1/users/me', { params });
  return data;
}

export async function getPreferences(): Promise<Record<string, unknown> | null> {
  try {
    const { data } = await api.get('/api/v1/users/me/preferences');
    return data;
  } catch (e: unknown) {
    const err = e as { response?: { status?: number; data?: unknown }; message?: string };
    if (err.response?.status === 404) return null;
    throw e;
  }
}

export async function updateCurrentUser(userData: Record<string, unknown>): Promise<Record<string, unknown>> {
  const { data } = await api.put('/api/v1/users/me', userData);
  return data;
}

export async function updatePreferences(preferences: Record<string, unknown>): Promise<Record<string, unknown>> {
  const { data } = await api.put('/api/v1/users/me/preferences', preferences);
    return data;
}

export async function clearPreferences(): Promise<Record<string, unknown>> {
  const { data } = await api.delete('/api/v1/users/me/preferences');
  return data;
}

export async function uploadAvatar(file: File): Promise<{ avatar_url: string }> {
  const formData = new FormData();
  formData.append('file', file);

  const { data } = await api.post('/api/v1/users/me/avatar', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return data; // { avatar_url: "https://..." }
}

// --- Watchlist & Watchlist Items (CRUD) ---

/**
 * List all watchlists for a user
 * GET /api/v1/users/me/watchlists
 * Returns: { watchlists: [...], total: number }
 */
export const listWatchlists = watchlistApi.listWatchlists;

export const createWatchlist = watchlistApi.createWatchlist;
export const updateWatchlist = watchlistApi.updateWatchlist;
export const deleteWatchlist = watchlistApi.deleteWatchlist;

/**
 * List items in a specific watchlist
 * GET /api/v1/users/me/watchlists/:watchlist_id/items
 * @param {string} watchlistId - The watchlist ID (UUID or 'default')
 * @returns {Promise<Object>} { items: [...], total: number }
 */
export const listWatchlistItems = watchlistItemsApi.listWatchlistItems;

export const updateWatchlistItem = watchlistItemsApi.updateWatchlistItem;

/**
 * @deprecated Use listWatchlists() and listWatchlistItems() instead
 * This function is kept for backward compatibility but should not be used
 */
export async function getWatchlistItems(): Promise<unknown> {
  return watchlistItemsApi.listWatchlistItems('default');
}

/**
 * Adds a stock to a watchlist with full details
 * @param {Object} itemData - Stock item data: { symbol, instrument_type, exchange, name, notes, alert_settings }
 * @param {string} watchlistId - The watchlist ID (UUID or 'default')
 * @returns {Promise<Object>} Created watchlist item
 */
export async function addWatchlistItem(itemData: Record<string, unknown>, watchlistId: string = 'default'): Promise<unknown> {
  return watchlistItemsApi.addWatchlistItem(watchlistId, itemData as unknown as watchlistItemsApi.AddWatchlistItemPayload);
}

/**
 * Deletes a watchlist item by ID
 * @param {string} itemId - The item ID to delete
 * @param {string} watchlistId - The watchlist ID (UUID or 'default')
 */
export async function deleteWatchlistItem(itemId: string, watchlistId: string = 'default'): Promise<void> {
  return watchlistItemsApi.deleteWatchlistItem(watchlistId, itemId);
}

// --- Stock prices (batch, for watchlist) ---

const DEFAULT_WATCHLIST_SYMBOLS: string[] = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'TSLA'];
const DEFAULT_WATCHLIST_NAMES: Record<string, string> = { AAPL: 'Apple', MSFT: 'Microsoft', NVDA: 'NVIDIA', AMZN: 'Amazon', TSLA: 'Tesla' };

export { DEFAULT_WATCHLIST_SYMBOLS, DEFAULT_WATCHLIST_NAMES };

/**
 * Get company names for a list of stock symbols (FMP profile companyName).
 * @param {string[]} symbols - e.g. ['AAPL', 'MSFT']
 * @returns {Promise<Record<string, string>>} symbol -> company name
 */
export async function getStockCompanyNames(symbols: string[]): Promise<Record<string, string>> {
  const list = [...(symbols || [])].map((s: string) => String(s).trim().toUpperCase()).filter(Boolean);
  if (!list.length) return {};
  try {
    const { data } = await api.post('/api/v1/market-data/stocks/names', { symbols: list });
    return data?.names ?? {};
  } catch {
    return {};
  }
}

export async function getStockPrices(symbols: string[]): Promise<StockPrice[]> {
  const list = [...(symbols || [])].map((s: string) => String(s).trim().toUpperCase()).filter(Boolean);
  if (!list.length) return [];
  try {
    const snapshots = await getSnapshotStocks(list);
    const snapList: SnapshotEntry[] = snapshots?.snapshots || snapshots?.results || snapshots?.data || [];
    const snapMap: Record<string, SnapshotEntry> = Array.isArray(snapList)
      ? Object.fromEntries(snapList.map((s: SnapshotEntry) => [String(s.symbol).toUpperCase(), s]))
      : {};

    // snapshotToStockPrice is the byte-for-byte equivalent of the old inline
    // transform — the one source for raw-snapshot → StockPrice.
    return list.map((sym: string) => snapshotToStockPrice(sym, snapMap[sym]));
  } catch (e: unknown) {
    console.error('[API] getStockPrices failed:', e instanceof Error ? e.message : String(e));
    return list.map((sym: string) => snapshotToStockPrice(sym, null));
  }
}

// --- Portfolio (use CRUD module) ---

export const listPortfolio = portfolioApi.listPortfolio;
export const updatePortfolioHolding = portfolioApi.updatePortfolioHolding;
export const deletePortfolioHolding = portfolioApi.deletePortfolioHolding;

export const getPortfolio = portfolioApi.listPortfolio;

/** Add portfolio holding. Payload: symbol, instrument_type, quantity, average_cost?, ... */
export const addPortfolioHolding = portfolioApi.addPortfolioHolding;

// --- Models & BYOK API Keys (moved to shared api/model.ts) ---

export { getAvailableModels, getUserApiKeys, updateUserApiKeys, deleteUserApiKey } from '@/api/model';

// --- OAuth (Connected Accounts) ---

/**
 * Start Codex device code flow — returns { user_code, verification_url, interval }.
 * POST /api/v1/oauth/codex/device/initiate
 */
export async function initiateCodexDevice(): Promise<Record<string, unknown>> {
  const { data } = await api.post('/api/v1/oauth/codex/device/initiate');
  return data;
}

/**
 * Poll for device authorization approval.
 * POST /api/v1/oauth/codex/device/poll
 * @returns {Promise<Object>} { pending: true } or { success: true, email, plan_type, account_id }
 */
export async function pollCodexDevice(): Promise<Record<string, unknown>> {
  const { data } = await api.post('/api/v1/oauth/codex/device/poll');
  return data;
}

/**
 * Check Codex OAuth connection status.
 * GET /api/v1/oauth/codex/status
 * Returns { connected, account_id, email, plan_type }
 */
export async function getCodexOAuthStatus(): Promise<{ connected: boolean; account_id: string | null; email: string | null; plan_type: string | null }> {
  try {
    const { data } = await api.get('/api/v1/oauth/codex/status');
    return data;
  } catch {
    return { connected: false, account_id: null, email: null, plan_type: null };
  }
}

/**
 * Disconnect Codex OAuth — delete stored tokens.
 * DELETE /api/v1/oauth/codex
 */
export async function disconnectCodexOAuth(): Promise<Record<string, unknown>> {
  const { data } = await api.delete('/api/v1/oauth/codex');
  return data;
}

// --- Claude OAuth (PKCE Authorization Code Flow) ---

/**
 * Initiate Claude OAuth — returns { authorize_url }.
 * POST /api/v1/oauth/claude/initiate
 */
export async function initiateClaudeOAuth(): Promise<Record<string, unknown>> {
  const { data } = await api.post('/api/v1/oauth/claude/initiate');
  return data;
}

/**
 * Submit Claude OAuth callback — exchange code#state for tokens.
 * POST /api/v1/oauth/claude/callback
 * @param {string} callbackInput - Full URL, code#state, or code=X&state=Y
 * @returns {Promise<Object>} { success: true }
 */
export async function submitClaudeCallback(callbackInput: string): Promise<Record<string, unknown>> {
  const { data } = await api.post('/api/v1/oauth/claude/callback', { callback_input: callbackInput });
  return data;
}

/**
 * Check Claude OAuth connection status.
 * GET /api/v1/oauth/claude/status
 * Returns { connected, account_id, email, plan_type }
 */
export async function getClaudeOAuthStatus(): Promise<{ connected: boolean; account_id: string | null; email: string | null; plan_type: string | null }> {
  try {
    const { data } = await api.get('/api/v1/oauth/claude/status');
    return data;
  } catch {
    return { connected: false, account_id: null, email: null, plan_type: null };
  }
}

/**
 * Disconnect Claude OAuth — delete stored tokens.
 * DELETE /api/v1/oauth/claude
 */
export async function disconnectClaudeOAuth(): Promise<Record<string, unknown>> {
  const { data } = await api.delete('/api/v1/oauth/claude');
  return data;
}

// --- News feed ---

/**
 * Fetch news articles from the native news endpoint.
 * GET /api/v1/news?tickers=...&limit=...&cursor=...&provider=...
 * @param {{ tickers?: string[], limit?: number, cursor?: string, provider?: string }} opts
 * @returns {Promise<{ results: Array, count: number, next_cursor: string|null }>}
 */
export async function getNews({ tickers, limit = 20, cursor, provider }: NewsParams = {}): Promise<NewsResponse> {
  try {
    const params: Record<string, string | number> = {};
    if (tickers && tickers.length) params.tickers = tickers.join(',');
    if (limit) params.limit = limit;
    if (cursor) params.cursor = cursor;
    if (provider) params.provider = provider;
    const { data } = await api.get('/api/v1/news', { params });
    return data || { results: [], count: 0, next_cursor: null };
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getNews failed:', err?.message);
    // Re-throw so the React Query callers retry (retry:1) and KEEP the last
    // good list instead of overwriting a live feed with [] on a transient blip.
    throw e;
  }
}

/**
 * Fetch a single news article by ID (full detail).
 * GET /api/v1/news/:articleId
 */
export async function getNewsArticle(articleId: string): Promise<Record<string, unknown>> {
  const { data } = await api.get(`/api/v1/news/${encodeURIComponent(articleId)}`);
  return data;
}

// --- AI Insights ---

export async function getTodayInsights(): Promise<Record<string, unknown>[]> {
  try {
    const { data } = await api.get('/api/v1/insights/today');
    return data?.insights || [];
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getTodayInsights failed:', err?.message);
    return [];
  }
}

export async function getInsightDetail(marketInsightId: string): Promise<Record<string, unknown>> {
  const { data } = await api.get(`/api/v1/insights/${encodeURIComponent(marketInsightId)}`);
  return data;
}

export async function generatePersonalizedInsight(): Promise<Record<string, unknown>> {
  const { data } = await api.post('/api/v1/insights/generate');
  return data;
}

// --- Earnings Calendar ---

/**
 * GET /api/v1/calendar/earnings?from=YYYY-MM-DD&to=YYYY-MM-DD
 * Returns { data: [{ symbol, date, epsEstimated, revenueEstimated, ... }], count }
 */
export async function getEarningsCalendar({ from, to }: EarningsParams = {}): Promise<EarningsResponse> {
  try {
    const params: Record<string, string> = {};
    if (from) params.from = from;
    if (to) params.to = to;
    const { data } = await api.get('/api/v1/calendar/earnings', { params });
    return data || { data: [], count: 0 };
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getEarningsCalendar failed:', err?.message);
    return { data: [], count: 0 };
  }
}
