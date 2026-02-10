/**
 * TradingCenter API utilities
 * All backend endpoints used by the TradingCenter page
 */
import { api } from '@/api/client';
import { supabase } from '@/lib/supabase';

const baseURL = api.defaults.baseURL;

/** Get Bearer auth headers for raw fetch() calls (SSE streams). */
async function getAuthHeaders() {
  if (!supabase) return {};
  const { data } = await supabase.auth.getSession();
  const token = data.session?.access_token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Search for stocks by keyword (symbol or company name).
 * Same API as Dashboard Add Watchlist: GET /api/v1/market-data/search/stocks
 * @param {string} query - Search keyword (e.g., "AAPL", "Apple", "Micro")
 * @param {number} limit - Maximum number of results (default: 50, max: 100)
 * @returns {Promise<{query: string, results: Array, count: number}>}
 */
export async function searchStocks(query, limit = 50) {
  if (!query || !query.trim()) {
    return { query: '', results: [], count: 0 };
  }
  try {
    const params = new URLSearchParams();
    params.append('query', query.trim());
    params.append('limit', String(Math.min(Math.max(1, limit), 100)));
    ['NASDAQ', 'NYSE', 'OTC'].forEach((ex) => params.append('exchange', ex)); // US exchanges only
    const { data } = await api.get('/api/v1/market-data/search/stocks', { params });
    return data || { query: query.trim(), results: [], count: 0 };
  } catch (e) {
    console.error('Search stocks failed:', e?.response?.status, e?.response?.data, e?.message);
    return { query: query.trim(), results: [], count: 0 };
  }
}

/**
 * Fetch stock historical data for charting
 * Uses backend API endpoint: GET /api/v1/market-data/intraday/stocks/{symbol}
 *
 * @param {string} symbol - Stock symbol (e.g., 'AAPL', 'MSFT')
 * @param {string} interval - Data interval (default: '1hour' for daily-like view, supports: 1min, 5min, 15min, 30min, 1hour, 4hour)
 * @param {string} [fromDate] - Start date in YYYY-MM-DD format
 * @param {string} [toDate] - End date in YYYY-MM-DD format
 * @param {Object} [options] - Additional options
 * @param {AbortSignal} [options.signal] - AbortController signal for cancellation
 * @returns {Promise<{data: Array, error?: string, fiftyTwoWeekHigh?: number, fiftyTwoWeekLow?: number}>} Chart data in lightweight-charts format
 */
export async function fetchStockData(symbol, interval = '1hour', fromDate, toDate, { signal } = {}) {
  if (!symbol || !symbol.trim()) {
    return { data: [], error: 'Symbol is required' };
  }

  const symbolUpper = symbol.trim().toUpperCase();

  try {
    // Use daily endpoint for 1day interval, intraday endpoint for everything else
    const isDaily = interval === '1day';
    const url = isDaily
      ? `/api/v1/market-data/daily/stocks/${encodeURIComponent(symbolUpper)}`
      : `/api/v1/market-data/intraday/stocks/${encodeURIComponent(symbolUpper)}`;
    const params = isDaily ? {} : { interval };

    if (fromDate) params.from = fromDate;
    if (toDate) params.to = toDate;

    const { data } = await api.get(url, { params, signal });

    const dataPoints = data?.data || [];

    if (!Array.isArray(dataPoints) || dataPoints.length === 0) {
      return { data: [], error: 'No data available' };
    }

    // Convert backend format to lightweight-charts format
    // Backend returns: { date: "YYYY-MM-DD HH:MM:SS", open, high, low, close, volume }
    const chartData = dataPoints.map((point) => {
      const date = new Date(point.date);
      return {
        time: Math.floor(date.getTime() / 1000),
        open: parseFloat(point.open) || 0,
        high: parseFloat(point.high) || 0,
        low: parseFloat(point.low) || 0,
        close: parseFloat(point.close) || 0,
        volume: parseFloat(point.volume) || 0,
      };
    }).filter(item =>
      !isNaN(item.open) && !isNaN(item.high) && !isNaN(item.low) && !isNaN(item.close) && item.time > 0
    ).sort((a, b) => a.time - b.time);

    if (chartData.length === 0) {
      return { data: [], error: 'Data conversion failed' };
    }

    // Derive 52-week high/low from series for header display
    let fiftyTwoWeekHigh = null;
    let fiftyTwoWeekLow = null;
    if (chartData.length > 0) {
      const highs = chartData.map((d) => d.high);
      const lows = chartData.map((d) => d.low);
      fiftyTwoWeekHigh = Math.max(...highs);
      fiftyTwoWeekLow = Math.min(...lows);
    }

    return {
      data: chartData,
      fiftyTwoWeekHigh,
      fiftyTwoWeekLow,
    };
  } catch (error) {
    // Don't treat abort as an error
    if (error?.name === 'CanceledError' || error?.name === 'AbortError') {
      return { data: [], error: 'Request cancelled' };
    }
    console.error('Error fetching stock data from backend:', error);
    const errorMsg = error?.response?.data?.detail || error?.message || 'Failed to fetch stock data';
    return { data: [], error: errorMsg };
  }
}

/**
 * Fetch real-time stock price and quote information
 * Uses backend API endpoint: POST /api/v1/market-data/intraday/stocks (batch endpoint)
 *
 * @param {string} symbol - Stock symbol
 * @param {Object} [options] - Additional options
 * @param {AbortSignal} [options.signal] - AbortController signal for cancellation
 * @returns {Promise<{price: number, change: number, changePercent: string, open: number, high: number, low: number}>}
 */
export async function fetchRealTimePrice(symbol, { signal } = {}) {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  const symbolUpper = symbol.trim().toUpperCase();

  try {
    // Use batch endpoint to get latest price
    const { data } = await api.post('/api/v1/market-data/intraday/stocks', {
      symbols: [symbolUpper],
      interval: '1min',
    }, { signal });

    const results = data?.results || {};
    const points = results[symbolUpper];

    if (!Array.isArray(points) || points.length === 0) {
      throw new Error('No price data available');
    }

    // Get first and last data points to calculate change
    const first = points[0];
    const last = points[points.length - 1];
    const open = parseFloat(first?.open || 0);
    const close = parseFloat(last?.close || 0);
    const high = parseFloat(last?.high || close);
    const low = parseFloat(last?.low || close);
    const change = close - open;
    const changePercent = open ? ((change / open) * 100).toFixed(2) + '%' : '0.00%';

    return {
      symbol: symbolUpper,
      price: Math.round(close * 100) / 100,
      open: Math.round(open * 100) / 100,
      high: Math.round(high * 100) / 100,
      low: Math.round(low * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent,
    };
  } catch (error) {
    if (error?.name === 'CanceledError' || error?.name === 'AbortError') {
      throw error; // Let caller handle abort
    }
    console.error('Error fetching real-time price:', error);
    throw error;
  }
}

/**
 * Fetch stock profile/company information
 * Note: This endpoint may need to be implemented in the backend
 * For now, returns basic info from quote data
 *
 * @param {string} symbol - Stock symbol
 * @param {Object} [options] - Additional options
 * @param {AbortSignal} [options.signal] - AbortController signal for cancellation
 * @returns {Promise<Object>} Stock profile information
 */
export async function fetchStockInfo(symbol, { signal } = {}) {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  const symbolUpper = symbol.trim().toUpperCase();

  try {
    // Use intraday endpoint to get basic info
    // In a full implementation, this would call a dedicated profile endpoint
    const { data } = await api.post('/api/v1/market-data/intraday/stocks', {
      symbols: [symbolUpper],
      interval: '1min',
    }, { signal });

    const results = data?.results || {};
    const points = results[symbolUpper];

    if (!Array.isArray(points) || points.length === 0) {
      return {
        Symbol: symbolUpper,
        Name: `${symbolUpper} Corp`,
        Exchange: 'NASDAQ',
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
    }

    const last = points[points.length - 1];
    const first = points[0];
    const totalVolume = points.reduce((sum, p) => sum + (Number(p.volume) || 0), 0);
    const avgVolume = points.length > 0 ? Math.round(totalVolume / points.length) : null;

    return {
      Symbol: symbolUpper,
      Name: `${symbolUpper} Corp`,
      Exchange: 'NASDAQ',
      Price: parseFloat(last?.close || 0),
      Open: parseFloat(first?.open || 0),
      High: parseFloat(Math.max(...points.map((p) => Number(p.high) || 0)) || 0),
      Low: parseFloat(Math.min(...points.map((p) => Number(p.low) || 0)) || 0),
      '52WeekHigh': null,
      '52WeekLow': null,
      AverageVolume: avgVolume,
      SharesOutstanding: null,
      MarketCapitalization: null,
      DividendYield: null,
    };
  } catch (error) {
    if (error?.name === 'CanceledError' || error?.name === 'AbortError') {
      throw error; // Let caller handle abort
    }
    console.error('Error fetching stock info:', error);
    return {
      Symbol: symbolUpper,
      Name: `${symbolUpper} Corp`,
      Exchange: 'NASDAQ',
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
  }
}

/**
 * Consolidated stock quote — fetches both stockInfo and realTimePrice
 * from a single API call to the batch intraday endpoint.
 * Replaces separate fetchStockInfo + fetchRealTimePrice calls.
 *
 * @param {string} symbol - Stock symbol
 * @param {Object} [options] - Additional options
 * @param {AbortSignal} [options.signal] - AbortController signal for cancellation
 * @returns {Promise<{stockInfo: Object, realTimePrice: Object|null}>}
 */
export async function fetchStockQuote(symbol, { signal } = {}) {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  const symbolUpper = symbol.trim().toUpperCase();
  const fallbackInfo = {
    Symbol: symbolUpper,
    Name: `${symbolUpper} Corp`,
    Exchange: 'NASDAQ',
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

  try {
    const { data } = await api.post('/api/v1/market-data/intraday/stocks', {
      symbols: [symbolUpper],
      interval: '1min',
    }, { signal });

    const results = data?.results || {};
    const points = results[symbolUpper];

    if (!Array.isArray(points) || points.length === 0) {
      return { stockInfo: fallbackInfo, realTimePrice: null };
    }

    const first = points[0];
    const last = points[points.length - 1];
    const open = parseFloat(first?.open || 0);
    const close = parseFloat(last?.close || 0);
    const high = parseFloat(last?.high || close);
    const low = parseFloat(last?.low || close);
    const change = close - open;
    const changePercent = open ? ((change / open) * 100).toFixed(2) + '%' : '0.00%';
    const totalVolume = points.reduce((sum, p) => sum + (Number(p.volume) || 0), 0);
    const avgVolume = points.length > 0 ? Math.round(totalVolume / points.length) : null;

    const stockInfo = {
      Symbol: symbolUpper,
      Name: `${symbolUpper} Corp`,
      Exchange: 'NASDAQ',
      Price: close,
      Open: open,
      High: parseFloat(Math.max(...points.map((p) => Number(p.high) || 0)) || 0),
      Low: parseFloat(Math.min(...points.map((p) => Number(p.low) || 0)) || 0),
      '52WeekHigh': null,
      '52WeekLow': null,
      AverageVolume: avgVolume,
      SharesOutstanding: null,
      MarketCapitalization: null,
      DividendYield: null,
    };

    const realTimePrice = {
      symbol: symbolUpper,
      price: Math.round(close * 100) / 100,
      open: Math.round(open * 100) / 100,
      high: Math.round(high * 100) / 100,
      low: Math.round(low * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent,
    };

    return { stockInfo, realTimePrice };
  } catch (error) {
    if (error?.name === 'CanceledError' || error?.name === 'AbortError') {
      throw error;
    }
    console.error('Error fetching stock quote:', error);
    return { stockInfo: fallbackInfo, realTimePrice: null };
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
export async function fetchCompanyOverview(symbol, { signal } = {}) {
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
export async function fetchAnalystData(symbol, { signal } = {}) {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }
  try {
    const { data } = await api.get(
      `/api/v1/market-data/stocks/${encodeURIComponent(symbol.trim().toUpperCase())}/analyst-data`,
      { signal }
    );
    return data;
  } catch (error) {
    if (error?.name === 'CanceledError' || error?.name === 'AbortError') {
      throw error;
    }
    console.error('Error fetching analyst data:', error);
    return null;
  }
}

// --- Flash Mode Chat Streaming ---

/**
 * Stream fetch helper for SSE (Server-Sent Events)
 * @param {string} url - API endpoint
 * @param {Object} opts - Fetch options
 * @param {Function} onEvent - Event handler callback
 */
async function streamFetch(url, opts, onEvent) {
  if (process.env.NODE_ENV === 'development') {
    console.log('[TradingCenter API] Starting stream fetch:', url);
  }

  const res = await fetch(`${baseURL}${url}`, opts);

  if (process.env.NODE_ENV === 'development') {
    console.log('[TradingCenter API] Response status:', res.status, 'Content-Type:', res.headers.get('content-type'));
  }

  if (!res.ok) {
    const errorText = await res.text().catch(() => 'Unknown error');
    throw new Error(`HTTP error! status: ${res.status}, message: ${errorText}`);
  }

  if (!res.body) {
    throw new Error('Response body is null - cannot read stream');
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let ev = {};
  let hasReceivedData = false;

  const processLine = (line) => {
    if (line.startsWith('id: ')) ev.id = line.slice(4).trim();
    else if (line.startsWith('event: ')) ev.event = line.slice(7).trim();
    else if (line.startsWith('data: ')) {
      hasReceivedData = true;
      try {
        const d = JSON.parse(line.slice(6));
        if (ev.event) d.event = ev.event;
        onEvent(d);
      } catch (e) {
        console.warn('[TradingCenter API] SSE parse error', e, line);
      }
      ev = {};
    } else if (line.trim() === '') ev = {};
  };

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Stream ended normally - decode any remaining buffer
        if (process.env.NODE_ENV === 'development') {
          console.log('[TradingCenter API] Stream ended normally, hasReceivedData:', hasReceivedData);
        }
        if (buffer) {
          buffer += decoder.decode(new Uint8Array(), { stream: false });
          const lines = buffer.split('\n');
          lines.forEach(processLine);
        }
        break;
      }

      // Handle case where value might be null
      if (value) {
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        lines.forEach(processLine);
      }
    }
  } catch (error) {
    // Handle incomplete chunked encoding or other stream errors
    // Only log as warning if we've received some data (partial success)
    // Otherwise, it's a real error
    const isNetworkError = error.name === 'TypeError' &&
      (error.message.includes('network') || error.message.includes('chunked') || error.message.includes('aborted'));

    if (isNetworkError) {
      // Process any remaining buffer before exiting
      if (buffer) {
        try {
          buffer += decoder.decode(new Uint8Array(), { stream: false });
          const lines = buffer.split('\n');
          lines.forEach(processLine);
        } catch (e) {
          // Ignore errors when processing final buffer
        }
      }

      // Only warn if we received some data (partial stream is better than nothing)
      if (hasReceivedData) {
        console.warn('[TradingCenter API] Stream interrupted after receiving data:', error.message);
        // Don't throw - we got partial data which is better than nothing
      } else {
        // No data received - this is a real error
        console.error('[TradingCenter API] Stream failed before receiving data:', error.message);
        throw error;
      }
    } else {
      // Re-throw unexpected errors
      throw error;
    }
  } finally {
    // Ensure reader is released
    try {
      reader.releaseLock();
    } catch (e) {
      // Reader might already be released
    }
  }
}

/**
 * Send chat message in flash mode (fast response without sandbox)
 * @param {string} message - User message content
 * @param {string} threadId - Thread ID (use '__default__' for new thread)
 * @param {Function} onEvent - Event handler callback
 * @param {string} locale - Locale (defaults to 'en-US')
 * @param {string} timezone - Timezone (defaults to 'America/New_York')
 * @returns {Promise<void>}
 */
export async function sendFlashChatMessage(
  message,
  threadId = '__default__',
  onEvent = () => {},
  locale = 'en-US',
  timezone = 'America/New_York',
  additionalContext = null
) {
  const body = {
    thread_id: threadId,
    agent_mode: 'flash',
    messages: [
      { role: 'user', content: message }
    ],
    locale,
    timezone,
  };
  if (additionalContext) {
    body.additional_context = additionalContext;
  }

  if (process.env.NODE_ENV === 'development') {
    console.log('[TradingCenter API] Sending flash chat message:', {
      threadId,
      agentMode: 'flash',
      messageLength: message.length,
    });
  }

  const authHeaders = await getAuthHeaders();

  try {
    await streamFetch(
      '/api/v1/chat/stream',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'text/event-stream',
          ...authHeaders,
        },
        body: JSON.stringify(body),
      },
      onEvent
    );
  } catch (error) {
    console.error('[TradingCenter API] Error in sendFlashChatMessage:', error);
    throw error;
  }
}

/**
 * Delete a thread
 * @param {string} threadId - Thread ID to delete
 * @returns {Promise<void>}
 */
export async function deleteTradingThread(threadId) {
  if (!threadId || threadId === '__default__') {
    return; // Don't delete default placeholder
  }
  try {
    await api.delete(`/api/v1/threads/${threadId}`);
  } catch (error) {
    // Silently fail - thread might already be deleted
    console.warn('[TradingCenter] Failed to delete thread:', threadId, error);
  }
}

/**
 * List all workspaces for the user
 * @returns {Promise<Array>} Array of workspace objects
 */
export async function listWorkspaces() {
  try {
    const { data } = await api.get('/api/v1/workspaces');
    return data?.workspaces || [];
  } catch (error) {
    console.warn('[TradingCenter] Failed to list workspaces:', error);
    return [];
  }
}

/**
 * Delete a workspace
 * @param {string} workspaceId - Workspace ID to delete
 * @returns {Promise<void>}
 */
export async function deleteWorkspace(workspaceId) {
  if (!workspaceId) {
    return;
  }
  try {
    await api.delete(`/api/v1/workspaces/${workspaceId}`);
    if (process.env.NODE_ENV === 'development') {
      console.log('[TradingCenter] Deleted workspace:', workspaceId);
    }
  } catch (error) {
    // Silently fail - workspace might already be deleted
    console.warn('[TradingCenter] Failed to delete workspace:', workspaceId, error);
  }
}

/**
 * Delete all workspaces named "__flash__"
 * @returns {Promise<void>}
 */
export async function deleteFlashWorkspaces() {
  try {
    const workspaces = await listWorkspaces();
    const flashWorkspaces = workspaces.filter((ws) => ws.name === '__flash__');

    if (flashWorkspaces.length === 0) {
      if (process.env.NODE_ENV === 'development') {
        console.log('[TradingCenter] No flash workspaces to delete');
      }
      return;
    }

    if (process.env.NODE_ENV === 'development') {
      console.log(`[TradingCenter] Found ${flashWorkspaces.length} flash workspace(s) to delete`);
    }

    // Delete all flash workspaces in parallel
    await Promise.all(
      flashWorkspaces.map((ws) => deleteWorkspace(ws.workspace_id))
    );

    if (process.env.NODE_ENV === 'development') {
      console.log('[TradingCenter] Deleted all flash workspaces');
    }
  } catch (error) {
    console.warn('[TradingCenter] Error deleting flash workspaces:', error);
  }
}
