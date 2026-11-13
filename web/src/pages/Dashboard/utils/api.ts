/**
 * Dashboard API utilities
 * All backend endpoints used by the Dashboard page
 */
import { api } from '@/api/client';
import i18n from '@/i18n';
import type { Profile } from '@/lib/appBridge/protocol';
import { INDEX_SYMBOLS as US_INDEX_SYMBOLS } from '@/lib/indices';
import { normalizeIndexKey } from '@/lib/marketUtils';
import { snapshotRowSpellings, snapshotToStockPrice } from '@/lib/quotes/quoteAdapters';
import { getSnapshotIndexes, getSnapshotStocks } from '@/lib/quotes/snapshotApi';
import type { SnapshotEntry, SnapshotResponse } from '@/lib/quotes/snapshotApi';
import type { IndexData, SparklinePoint, StockPrice } from '@/types/market';
import * as portfolioApi from './portfolio';
import * as watchlistApi from './watchlist';
import * as watchlistItemsApi from './watchlistItems';

// Snapshot batch primitives moved to lib/quotes (breaking the lib→page cycle);
// re-exported here for back-compat with existing Dashboard callers/tests.
export { getSnapshotIndexes, getSnapshotStocks };
export type { SnapshotEntry, SnapshotResponse };

// --- Interfaces ---



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

/** Index symbols, normalized (GSPC, not ^GSPC), as the index endpoints take them. */
const INDEX_SYMBOLS: string[] = [...US_INDEX_SYMBOLS];
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
 * Build one IndexData card from a raw index snapshot + its sparkline.
 * Index prices are never legitimately 0; a zero/negative price means a partial
 * provider snapshot, so it's treated as no quote (renders N/A, not a fake 0.00).
 */
export function buildIndexData(
  norm: string,
  snap: SnapshotEntry | undefined,
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

  const { data } = await api.post('/api/v1/users/me/avatar', formData);
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

/** One listing's profile as `POST /market-data/profiles` sends it: the shape a framed app reads, field for field. */
export type CompanyProfileRow = Profile;

export interface CompanyProfilesResponse {
  /** Keyed by the spelling as sent; a symbol with no profile is in neither map. */
  results: Record<string, CompanyProfileRow>;
  errors: Record<string, string>;
}

/** Profiles for up to 50 listings; a failed request rejects, unlike the name lookup above. */
export async function getCompanyProfiles(symbols: string[]): Promise<CompanyProfilesResponse> {
  const { data } = await api.post('/api/v1/market-data/profiles', { symbols });
  return { results: data?.results ?? {}, errors: data?.errors ?? {} };
}

export async function getStockPrices(symbols: string[]): Promise<StockPrice[]> {
  const list = [...(symbols || [])].map((s: string) => String(s).trim().toUpperCase()).filter(Boolean);
  if (!list.length) return [];
  try {
    const snapshots = await getSnapshotStocks(list);
    const snapList: SnapshotEntry[] = snapshots?.snapshots || snapshots?.results || snapshots?.data || [];
    const snapMap: Record<string, SnapshotEntry> = Array.isArray(snapList)
      ? Object.fromEntries(snapList.flatMap((s: SnapshotEntry) =>
          snapshotRowSpellings(s).map((k) => [String(k).toUpperCase(), s])))
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

// --- News feed ---

/**
 * The server gates the CN feed on `users.locale`, else Accept-Language, and the
 * client decides eligibility on `users.locale`, else the active UI language,
 * which a cookie can set apart from the browser's. Sending that language keeps
 * the two answers the same, for the feed and for an article link from it.
 */
const newsHeaders = () => ({ 'Accept-Language': i18n.language });

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
    const { data } = await api.get('/api/v1/news', { params, headers: newsHeaders() });
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
  const { data } = await api.get(`/api/v1/news/${encodeURIComponent(articleId)}`, {
    headers: newsHeaders(),
  });
  return data;
}

// --- AI Insights ---

/** Rejects on failure, for a reader that must not take a failed read for a day with no insights. */
export async function fetchTodayInsights(): Promise<Record<string, unknown>[]> {
  const { data } = await api.get('/api/v1/insights/today');
  return data?.insights || [];
}

export async function getTodayInsights(): Promise<Record<string, unknown>[]> {
  try {
    return await fetchTodayInsights();
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
export async function fetchEarningsCalendar({ from, to }: EarningsParams = {}): Promise<EarningsResponse> {
  const params: Record<string, string> = {};
  if (from) params.from = from;
  if (to) params.to = to;
  const { data } = await api.get('/api/v1/calendar/earnings', { params });
  return data || { data: [], count: 0 };
}

/** As `fetchEarningsCalendar`, but a failure reads as an empty calendar. */
export async function getEarningsCalendar(params: EarningsParams = {}): Promise<EarningsResponse> {
  try {
    return await fetchEarningsCalendar(params);
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getEarningsCalendar failed:', err?.message);
    return { data: [], count: 0 };
  }
}
