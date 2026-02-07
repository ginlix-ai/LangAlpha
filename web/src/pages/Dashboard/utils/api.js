/**
 * Dashboard API utilities
 * All backend endpoints used by the Dashboard page
 */
import { api, headers, DEFAULT_USER_ID } from '@/api/client';
import * as portfolioApi from './portfolio';
import * as watchlistApi from './watchlist';
import * as watchlistItemsApi from './watchlistItems';

export { DEFAULT_USER_ID };

const baseURL = api.defaults.baseURL;

// --- Market data (see docs/ptc-agent-api/market data) ---

/** Index symbols: normalized (GSPC, IXIC, DJI, RUT). Index.yml / Index Batch.yml use these. */
const INDEX_SYMBOLS = ['GSPC', 'IXIC', 'DJI', 'RUT'];
const INDEX_NAMES = { GSPC: 'S&P 500', IXIC: 'NASDAQ 100', DJI: 'Dow Jones', RUT: 'Russell 2000' };

function normalizeIndexSymbol(s) {
  return String(s).replace(/^\^/, '').toUpperCase();
}

function parseDataPoints(pts, norm) {
  if (!Array.isArray(pts) || !pts.length) {
    // console.log(`[API] parseDataPoints - ${norm}: No data points`, {
    //   isArray: Array.isArray(pts),
    //   length: pts?.length,
    // });
    return null;
  }
  const first = pts[0];
  const last = pts[pts.length - 1];
  const open = Number(first?.open ?? 0);
  const close = Number(last?.close ?? 0);
  const change = close - open;
  const changePercent = open ? (change / open) * 100 : 0;
  
  const result = {
    symbol: norm,
    name: INDEX_NAMES[norm] ?? norm,
    price: Math.round(close * 100) / 100,
    change: Math.round(change * 100) / 100,
    changePercent: Math.round(changePercent * 100) / 100,
    isPositive: change >= 0,
  };
  
  // console.log(`[API] parseDataPoints - ${norm}:`, {
  //   firstPoint: first,
  //   lastPoint: last,
  //   open,
  //   close,
  //   change,
  //   changePercent,
  //   result,
  // });
  
  return result;
}

function fallbackIndex(norm) {
  return {
    symbol: norm,
    name: INDEX_NAMES[norm] ?? norm,
    price: 0,
    change: 0,
    changePercent: 0,
    isPositive: true,
  };
}

/**
 * GET /api/v1/market-data/intraday/indexes/:symbol (Index.yml)
 * Path uses normalized symbol (e.g. GSPC). Query: interval, from, to optional.
 * Returns the most recent data point for the index.
 */
export async function getIndex(symbol, opts = {}) {
  const norm = normalizeIndexSymbol(String(symbol).trim());
  try {
    const { data } = await api.get(`/api/v1/market-data/intraday/indexes/${encodeURIComponent(norm)}`);
    
    const pts = data?.data ?? [];
    
    // Use the most recent data point (first item in array, as backend returns newest first)
    if (!Array.isArray(pts) || !pts.length) {
      throw new Error(`No intraday data for ${norm}`);
    }
    
    // Most recent data point is the first one
    const mostRecent = pts[0];
    const oldest = pts[pts.length - 1];
    
    // Calculate change from oldest to newest (most recent)
    const open = Number(oldest?.open ?? 0);
    const close = Number(mostRecent?.close ?? 0);
    const change = close - open;
    const changePercent = open ? (change / open) * 100 : 0;
    
    const result = {
      symbol: norm,
      name: INDEX_NAMES[norm] ?? norm,
      price: Math.round(close * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent: Math.round(changePercent * 100) / 100,
      isPositive: change >= 0,
    };
    
    return result;
  } catch (e) {
    console.error(`[API] getIndex - ${norm}: Error:`, e?.message);
    const msg = e.response?.data?.detail ?? e.message;
    throw new Error(typeof msg === 'string' ? msg : String(msg));
  }
}

/**
 * Fetches indices data by making individual GET calls for each symbol.
 * Uses GET /api/v1/market-data/intraday/indexes/:symbol endpoint.
 * Returns { indices, failedCount }.
 */
export async function getIndices(symbols = INDEX_SYMBOLS, opts = {}) {
  const list = symbols.map((s) => normalizeIndexSymbol(String(s).trim()));
  
  // Make individual GET requests for each symbol (no query params per API docs)
  const promises = list.map(async (norm) => {
    try {
      const result = await getIndex(norm);
      return { success: true, symbol: norm, data: result };
    } catch (error) {
      console.error(`[API] getIndices - Failed to fetch ${norm}:`, error?.message);
      return { success: false, symbol: norm, error };
    }
  });
  
  const results = await Promise.all(promises);
  
  const indices = results.map((result) => {
    if (result.success) {
      return result.data;
    } else {
      return fallbackIndex(result.symbol);
    }
  });
  
  const failed = results.filter((r) => !r.success).length;
  
  return { indices, failedCount: failed };
}

export { INDEX_NAMES, INDEX_SYMBOLS, fallbackIndex, normalizeIndexSymbol };

// --- Hello ---

export async function fetchHello() {
  const { data } = await api.get('/hello', { responseType: 'text' });
  return data;
}

// --- Users ---

export async function createUser(userData, userId = DEFAULT_USER_ID) {
  const { data } = await api.post('/api/v1/users', userData, { headers: headers(userId) });
  return data;
}

export async function getCurrentUser(userId = DEFAULT_USER_ID) {
  const { data } = await api.get('/api/v1/users/me', { headers: headers(userId) });
  return data;
}

export async function getPreferences(userId = DEFAULT_USER_ID) {
  const { data } = await api.get('/api/v1/users/me/preferences', { headers: headers(userId) });
  return data;
}

export async function updateCurrentUser(userData, userId = DEFAULT_USER_ID) {
  const { data } = await api.put('/api/v1/users/me', userData, { headers: headers(userId) });
  return data;
}

export async function updatePreferences(preferences, userId = DEFAULT_USER_ID) {
  const { data } = await api.put('/api/v1/users/me/preferences', preferences, { headers: headers(userId) });
    return data;
}

export async function uploadAvatar(file, userId = DEFAULT_USER_ID) {
  const formData = new FormData();
  formData.append('file', file);
  
  const { data } = await api.post('/api/v1/users/me/avatar', formData, {
    headers: {
      ...headers(userId),
      'Content-Type': 'multipart/form-data',
    },
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
 * @param {string} userId - The user ID
 * @returns {Promise<Object>} { items: [...], total: number }
 */
export const listWatchlistItems = watchlistItemsApi.listWatchlistItems;

export const updateWatchlistItem = watchlistItemsApi.updateWatchlistItem;

/**
 * @deprecated Use listWatchlists() and listWatchlistItems() instead
 * This function is kept for backward compatibility but should not be used
 */
export async function getWatchlistItems(userId = DEFAULT_USER_ID) {
  return watchlistItemsApi.listWatchlistItems('default', userId);
}

/**
 * Adds a stock to a watchlist with full details
 * @param {Object} itemData - Stock item data: { symbol, instrument_type, exchange, name, notes, alert_settings }
 * @param {string} watchlistId - The watchlist ID (UUID or 'default')
 * @param {string} userId - The user ID
 * @returns {Promise<Object>} Created watchlist item
 */
export async function addWatchlistItem(itemData, watchlistId = 'default', userId = DEFAULT_USER_ID) {
  return watchlistItemsApi.addWatchlistItem(watchlistId, itemData, userId);
}

/**
 * Deletes a watchlist item by ID
 * @param {string} itemId - The item ID to delete
 * @param {string} watchlistId - The watchlist ID (UUID or 'default')
 * @param {string} userId - The user ID
 */
export async function deleteWatchlistItem(itemId, watchlistId = 'default', userId = DEFAULT_USER_ID) {
  return watchlistItemsApi.deleteWatchlistItem(watchlistId, itemId, userId);
}

// --- Stock prices (batch, for watchlist) ---

const DEFAULT_WATCHLIST_SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'TSLA'];
const DEFAULT_WATCHLIST_NAMES = { AAPL: 'Apple', MSFT: 'Microsoft', NVDA: 'NVIDIA', AMZN: 'Amazon', TSLA: 'Tesla' };

export { DEFAULT_WATCHLIST_SYMBOLS, DEFAULT_WATCHLIST_NAMES };

/**
 * Search for stocks by keyword (symbol or company name).
 * GET /api/v1/market-data/search/stocks
 * @param {string} query - Search keyword (e.g., "AAPL", "Apple", "Micro")
 * @param {number} limit - Maximum number of results (default: 50, max: 100)
 * @returns {Promise<Object>} { query: string, results: Array, count: number }
 */
export async function searchStocks(query, limit = 50) {
  if (!query || !query.trim()) {
    return { query: '', results: [], count: 0 };
  }
  try {
    const { data } = await api.get('/api/v1/market-data/search/stocks', {
      params: {
        query: query.trim(),
        limit: Math.min(Math.max(1, limit), 100), // Clamp between 1 and 100
      },
    });
    return data || { query: query.trim(), results: [], count: 0 };
  } catch (e) {
    console.error('Search stocks failed:', e?.response?.status, e?.response?.data, e?.message);
    return { query: query.trim(), results: [], count: 0 };
  }
}

/**
 * Get company names for a list of stock symbols (FMP profile companyName).
 * @param {string[]} symbols - e.g. ['AAPL', 'MSFT']
 * @returns {Promise<Record<string, string>>} symbol -> company name
 */
export async function getStockCompanyNames(symbols) {
  const list = [...(symbols || [])].map((s) => String(s).trim().toUpperCase()).filter(Boolean);
  if (!list.length) return {};
  try {
    const { data } = await api.post('/api/v1/market-data/stocks/names', { symbols: list });
    return data?.names ?? {};
  } catch {
    return {};
  }
}

export async function getStockPrices(symbols) {
  const list = [...(symbols || [])].map((s) => String(s).trim().toUpperCase()).filter(Boolean);
  if (!list.length) return [];
  try {
    const { data } = await api.post('/api/v1/market-data/intraday/stocks', { symbols: list, interval: '1min' });
    const results = data?.results ?? {};
    return list.map((sym) => {
      const pts = results[sym];
      if (!Array.isArray(pts) || !pts.length) {
        return { symbol: sym, price: 0, change: 0, changePercent: 0, isPositive: true };
      }
      
      // Backend returns data with most recent first (like indices endpoint)
      // Most recent data point is the first one (pts[0])
      // Oldest data point is the last one (pts[pts.length - 1])
      const mostRecent = pts[0];
      const oldest = pts[pts.length - 1];
      
      // Use most recent close price as the current price
      const close = Number(mostRecent?.close ?? 0);
      // Calculate change from oldest open to most recent close
      const open = Number(oldest?.open ?? 0);
      const change = close - open;
      const pct = open ? (change / open) * 100 : 0;
      
      return {
        symbol: sym,
        price: Math.round(close * 100) / 100,
        change: Math.round(change * 100) / 100,
        changePercent: Math.round(pct * 100) / 100,
        isPositive: change >= 0,
      };
    });
  } catch {
    return list.map((sym) => ({ symbol: sym, price: 0, change: 0, changePercent: 0, isPositive: true }));
  }
}

// --- Portfolio (use CRUD module) ---

export const listPortfolio = portfolioApi.listPortfolio;
export const updatePortfolioHolding = portfolioApi.updatePortfolioHolding;
export const deletePortfolioHolding = portfolioApi.deletePortfolioHolding;

export const getPortfolio = portfolioApi.listPortfolio;

/** Add portfolio holding. Payload: symbol, instrument_type, quantity, average_cost?, ... */
export const addPortfolioHolding = portfolioApi.addPortfolioHolding;

// --- InfoFlow (content feed) ---

/**
 * Fetch InfoFlow results filtered by category.
 * GET /api/v1/infoflow/results?category={cat}&limit={limit}&offset={offset}
 */
export async function getInfoFlowResults(category, limit = 10, offset = 0) {
  try {
    const params = { limit, offset };
    if (category) params.category = category;
    const { data } = await api.get('/api/v1/infoflow/results', { params });
    return data || { results: [], total: 0, limit, offset, has_more: false };
  } catch (e) {
    console.error('[API] getInfoFlowResults failed:', e?.message);
    return { results: [], total: 0, limit, offset, has_more: false };
  }
}

/**
 * Fetch InfoFlow result detail by indexNumber.
 * GET /api/v1/infoflow/results/{indexNumber}
 */
export async function getInfoFlowDetail(indexNumber) {
  try {
    const { data } = await api.get(`/api/v1/infoflow/results/${encodeURIComponent(indexNumber)}`);
    return data;
  } catch (e) {
    console.error('[API] getInfoFlowDetail failed:', e?.message);
    return null;
  }
}
