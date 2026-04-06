/**
 * MarketView API utilities
 * All backend endpoints used by the MarketView page
 */
import { api } from '@/api/client';
import { supabase } from '@/lib/supabase';
import { utcMsToChartSec } from '@/lib/utils';

const baseURL = api.defaults.baseURL;

/** Strip leading ^ from index symbols to match backend batch response keys. */
const normalizeSymbolKey = (sym: string): string => sym.replace(/^\^/, '');

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
 * Get the current Supabase access token for WS auth.
 * Returns null when auth is disabled (local dev).
 * @returns {Promise<string|null>}
 */
export async function getWSAuthToken(): Promise<string | null> {
  if (!supabase) return null;
  try {
    const { data } = await supabase.auth.getSession();
    return data.session?.access_token || null;
  } catch {
    return null;
  }
}

/** Get Bearer auth headers for raw fetch() calls (SSE streams). */
async function getAuthHeaders(): Promise<Record<string, string>> {
  if (!supabase) return {};
  const { data } = await supabase.auth.getSession();
  const token = data.session?.access_token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

interface ChartDataPoint {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface StockDataResult {
  data: ChartDataPoint[];
  error?: string;
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
export async function fetchStockData(
  symbol: string,
  interval: string = '1hour',
  fromDate: string | undefined,
  toDate: string | undefined,
  { signal }: { signal?: AbortSignal } = {}
): Promise<StockDataResult> {
  if (!symbol || !symbol.trim()) {
    return { data: [], error: 'Symbol is required' };
  }

  const symbolUpper = symbol.trim().toUpperCase();
  const isIndex = symbolUpper.startsWith('^');

  try {
    // Use daily endpoint for 1day interval, intraday endpoint for everything else
    const isDaily = interval === '1day';
    const market = isIndex ? 'indexes' : 'stocks';
    const url = isDaily
      ? `/api/v1/market-data/daily/${market}/${encodeURIComponent(symbolUpper)}`
      : `/api/v1/market-data/intraday/${market}/${encodeURIComponent(symbolUpper)}`;
    const params: Record<string, string> = isDaily ? {} : { interval };

    if (fromDate) params.from = fromDate;
    if (toDate) params.to = toDate;

    const { data } = await api.get(url, { params, signal });

    const dataPoints = data?.data || [];

    if (!Array.isArray(dataPoints) || dataPoints.length === 0) {
      return { data: [], error: 'No data available' };
    }

    // Convert backend format to lightweight-charts format
    // Backend returns: { time: <unix_ms>, open, high, low, close, volume }
    const chartData = dataPoints.map((point: Record<string, unknown>) => ({
      time: utcMsToChartSec(point.time as number),
      open: parseFloat(point.open as string) || 0,
      high: parseFloat(point.high as string) || 0,
      low: parseFloat(point.low as string) || 0,
      close: parseFloat(point.close as string) || 0,
      volume: parseFloat(point.volume as string) || 0,
    })).filter((item: ChartDataPoint) =>
      !isNaN(item.open) && !isNaN(item.high) && !isNaN(item.low) && !isNaN(item.close) && item.time > 0
    ).sort((a: ChartDataPoint, b: ChartDataPoint) => a.time - b.time)
    .filter((item: ChartDataPoint, i: number, arr: ChartDataPoint[]) => i === 0 || item.time !== arr[i - 1].time);

    if (chartData.length === 0) {
      return { data: [], error: 'Data conversion failed' };
    }

    return {
      data: chartData,
    };
  } catch (error: unknown) {
    // Don't treat abort as an error
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) {
      return { data: [], error: 'Request cancelled' };
    }
    console.error('Error fetching stock data from backend:', error);
    const axiosError = error as { response?: { data?: { detail?: string } }; message?: string };
    const errorMsg = axiosError?.response?.data?.detail || axiosError?.message || 'Failed to fetch stock data';
    return { data: [], error: errorMsg };
  }
}

interface SnapshotData {
  symbol: string;
  name?: string;
  price: number;
  previous_close?: number;
  change?: number;
  change_percent?: number;
  open?: number;
  high?: number;
  low?: number;
  volume?: number;
  [key: string]: unknown;
}

/**
 * GET /api/v1/market-data/snapshots/stocks/{symbol} — single stock snapshot
 * Returns snapshot data with name, price, change, previous_close, open, high, low, volume, etc.
 */
export async function fetchSnapshot(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<SnapshotData | null> {
  if (!symbol || !symbol.trim()) throw new Error('Symbol is required');
  const symbolUpper = symbol.trim().toUpperCase();
  const isIndex = symbolUpper.startsWith('^');
  const norm = normalizeSymbolKey(symbolUpper);
  const endpoint = isIndex
    ? `/api/v1/market-data/snapshots/indexes?symbols=${encodeURIComponent(norm)}`
    : `/api/v1/market-data/snapshots/stocks/${encodeURIComponent(symbolUpper)}`;
  try {
    const { data } = await api.get(endpoint, { signal });
    // Single stock endpoint returns SnapshotData directly;
    // index batch returns { snapshots: [...], count } — extract first match
    if (isIndex) {
      const results: SnapshotData[] = data?.snapshots || data?.results || [];
      if (Array.isArray(results)) return results.find((s: SnapshotData) => normalizeSymbolKey(s.symbol) === norm) || results[0] || null;
      return null;
    }
    return data || null;
  } catch (error: unknown) {
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) throw error;
    console.error('Error fetching snapshot:', error);
    return null;
  }
}

interface StockInfo {
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

interface RealTimePrice {
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

interface StockQuoteResult {
  stockInfo: StockInfo;
  realTimePrice: RealTimePrice | null;
  snapshot: SnapshotData | null;
}

/**
 * Consolidated stock quote — uses snapshot endpoint for accurate price/change data.
 * Returns { stockInfo, realTimePrice, snapshot } where snapshot is the raw snapshot data.
 */
export async function fetchStockQuote(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<StockQuoteResult> {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  const symbolUpper = symbol.trim().toUpperCase();
  const isIndex = symbolUpper.startsWith('^');
  const fallbackInfo: StockInfo = {
    Symbol: symbolUpper,
    Name: `${symbolUpper} Corp`,
    Exchange: isIndex ? '' : 'NASDAQ',
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
    const snap = await fetchSnapshot(symbol, { signal });

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
      Name: snap.name || `${symbolUpper} Corp`,
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

    const realTimePrice: RealTimePrice = {
      symbol: symbolUpper,
      price: Math.round(price * 100) / 100,
      open: Math.round((snap.open ?? 0) * 100) / 100,
      high: Math.round((snap.high ?? 0) * 100) / 100,
      low: Math.round((snap.low ?? 0) * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent: changePct,
      volume: snap.volume ?? 0,
      previousClose: Math.round(previousClose * 100) / 100,
    };

    return { stockInfo, realTimePrice, snapshot: snap };
  } catch (error: unknown) {
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) {
      throw error;
    }
    console.error('Error fetching stock quote:', error);
    return { stockInfo: fallbackInfo, realTimePrice: null, snapshot: null };
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
export async function fetchCompanyOverview(symbol: string, { signal }: { signal?: AbortSignal } = {}): Promise<unknown> {
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

// --- Flash Mode Chat Streaming ---

interface StreamError extends Error {
  status?: number;
  rateLimitInfo?: Record<string, unknown>;
}

/**
 * Stream fetch helper for SSE (Server-Sent Events)
 * @param {string} url - API endpoint
 * @param {Object} opts - Fetch options
 * @param {Function} onEvent - Event handler callback
 */
async function streamFetch(
  url: string,
  opts: RequestInit,
  onEvent: (event: Record<string, unknown>) => void
): Promise<void> {
  if (process.env.NODE_ENV === 'development') {
    console.log('[MarketView API] Starting stream fetch:', url);
  }

  const res = await fetch(`${baseURL}${url}`, opts);

  if (process.env.NODE_ENV === 'development') {
    console.log('[MarketView API] Response status:', res.status, 'Content-Type:', res.headers.get('content-type'));
  }

  if (!res.ok) {
    // Handle 429 (rate limit) with structured detail
    if (res.status === 429) {
      let detail: Record<string, unknown> = {};
      try { detail = await res.json(); } catch { /* ignore */ }
      const err: StreamError = new Error((detail?.detail as Record<string, unknown>)?.message as string || 'Rate limit exceeded');
      err.status = 429;
      err.rateLimitInfo = (detail?.detail as Record<string, unknown>) || {};
      throw err;
    }
    let detail = '';
    let errorInfo: Record<string, unknown> | null = null;
    const text = await res.text().catch(() => '');
    try {
      const body = JSON.parse(text);
      if (body?.detail && typeof body.detail === 'object' && 'message' in body.detail) {
        errorInfo = body.detail as Record<string, unknown>;
        detail = (errorInfo.message as string) || '';
      } else {
        detail = typeof body?.detail === 'string' ? body.detail : JSON.stringify(body?.detail || body);
      }
    } catch {
      detail = text || 'Unknown error';
    }
    const err: Error & { status?: number; errorInfo?: Record<string, unknown> } =
      new Error(detail || `HTTP error! status: ${res.status}`);
    err.status = res.status;
    if (errorInfo) err.errorInfo = errorInfo;
    throw err;
  }

  if (!res.body) {
    throw new Error('Response body is null - cannot read stream');
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let ev: { id?: string; event?: string } = {};
  let hasReceivedData = false;

  const processLine = (line: string): void => {
    if (line.startsWith('id: ')) ev.id = line.slice(4).trim();
    else if (line.startsWith('event: ')) ev.event = line.slice(7).trim();
    else if (line.startsWith('data: ')) {
      hasReceivedData = true;
      try {
        const d: Record<string, unknown> = JSON.parse(line.slice(6));
        if (ev.event) d.event = ev.event;
        onEvent(d);
      } catch (e: unknown) {
        console.warn('[MarketView API] SSE parse error', e, line);
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
          console.log('[MarketView API] Stream ended normally, hasReceivedData:', hasReceivedData);
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
  } catch (error: unknown) {
    // Handle incomplete chunked encoding or other stream errors
    // Only log as warning if we've received some data (partial success)
    // Otherwise, it's a real error
    const isNetworkError = error instanceof Error &&
      error.name === 'TypeError' &&
      (error.message.includes('network') || error.message.includes('chunked') || error.message.includes('aborted'));

    if (isNetworkError) {
      // Process any remaining buffer before exiting
      if (buffer) {
        try {
          buffer += decoder.decode(new Uint8Array(), { stream: false });
          const lines = buffer.split('\n');
          lines.forEach(processLine);
        } catch {
          // Ignore errors when processing final buffer
        }
      }

      // Only warn if we received some data (partial stream is better than nothing)
      if (hasReceivedData) {
        console.warn('[MarketView API] Stream interrupted after receiving data:', (error as Error).message);
        // Don't throw - we got partial data which is better than nothing
      } else {
        // No data received - this is a real error
        console.error('[MarketView API] Stream failed before receiving data:', (error as Error).message);
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
    } catch {
      // Reader might already be released
    }
  }
}

/**
 * Send chat message in flash mode (fast response without sandbox)
 * @param {string} message - User message content
 * @param {string|null} threadId - Thread ID (null or '__default__' for new thread)
 * @param {Function} onEvent - Event handler callback
 * @param {string} locale - Locale (defaults to 'en-US')
 * @param {string} timezone - Timezone (defaults to 'America/New_York')
 * @returns {Promise<void>}
 */
export async function sendFlashChatMessage(
  message: string,
  threadId: string | null = null,
  onEvent: (event: Record<string, unknown>) => void = () => {},
  locale: string = 'en-US',
  timezone: string = 'America/New_York',
  additionalContext: unknown = null,
  model: string | null = null
): Promise<void> {
  const body: Record<string, unknown> = {
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
  if (model) {
    body.llm_model = model;
  }

  // Use /threads/{id}/messages for existing thread, /threads/messages for new
  const isNewThread = !threadId || threadId === '__default__';
  const url = isNewThread
    ? '/api/v1/threads/messages'
    : `/api/v1/threads/${threadId}/messages`;

  if (process.env.NODE_ENV === 'development') {
    console.log('[MarketView API] Sending flash chat message:', {
      threadId,
      agentMode: 'flash',
      messageLength: message.length,
    });
  }

  const authHeaders = await getAuthHeaders();

  try {
    await streamFetch(
      url,
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
  } catch (error: unknown) {
    console.error('[MarketView API] Error in sendFlashChatMessage:', error);
    throw error;
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

interface Workspace {
  workspace_id: string;
  name: string;
  [key: string]: unknown;
}

/**
 * List all workspaces for the user
 * @returns {Promise<Array>} Array of workspace objects
 */
export async function listWorkspaces(): Promise<Workspace[]> {
  try {
    const { data } = await api.get('/api/v1/workspaces');
    return data?.workspaces || [];
  } catch (error: unknown) {
    console.warn('[MarketView] Failed to list workspaces:', error);
    return [];
  }
}

/**
 * Delete a workspace
 * @param {string} workspaceId - Workspace ID to delete
 * @returns {Promise<void>}
 */
export async function deleteWorkspace(workspaceId: string): Promise<void> {
  if (!workspaceId) {
    return;
  }
  try {
    await api.delete(`/api/v1/workspaces/${workspaceId}`);
    if (process.env.NODE_ENV === 'development') {
      console.log('[MarketView] Deleted workspace:', workspaceId);
    }
  } catch (error: unknown) {
    // Silently fail - workspace might already be deleted
    console.warn('[MarketView] Failed to delete workspace:', workspaceId, error);
  }
}

/**
 * Delete all workspaces named "__flash__"
 * @returns {Promise<void>}
 */
export async function deleteFlashWorkspaces(): Promise<void> {
  try {
    const workspaces = await listWorkspaces();
    const flashWorkspaces = workspaces.filter((ws: Workspace) => ws.name === '__flash__');

    if (flashWorkspaces.length === 0) {
      if (process.env.NODE_ENV === 'development') {
        console.log('[MarketView] No flash workspaces to delete');
      }
      return;
    }

    if (process.env.NODE_ENV === 'development') {
      console.log(`[MarketView] Found ${flashWorkspaces.length} flash workspace(s) to delete`);
    }

    // Delete all flash workspaces in parallel
    await Promise.all(
      flashWorkspaces.map((ws: Workspace) => deleteWorkspace(ws.workspace_id))
    );

    if (process.env.NODE_ENV === 'development') {
      console.log('[MarketView] Deleted all flash workspaces');
    }
  } catch (error: unknown) {
    console.warn('[MarketView] Error deleting flash workspaces:', error);
  }
}
