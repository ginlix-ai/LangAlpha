/**
 * Shared market utilities used across Dashboard and MarketView.
 */
import { api } from '@/api/client';

interface MarketStatusData {
  market?: string;
  afterHours?: boolean;
  earlyHours?: boolean;
  [key: string]: unknown;
}

interface ExtendedHoursRow {
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
  early_trading_change_percent?: number | null;
  late_trading_change_percent?: number | null;
  lastMinuteClose?: number | null;
  last_minute_close?: number | null;
  earlyTradingChange?: number | null;
  early_trading_change?: number | null;
  lateTradingChange?: number | null;
  late_trading_change?: number | null;
  regularClose?: number | null;
  regular_close?: number | null;
  regularTradingChange?: number | null;
  regular_trading_change?: number | null;
  previousClose?: number | null;
  previous_close?: number | null;
  [key: string]: unknown;
}

interface ExtendedHoursInfo {
  extPct: number | null;
  extLabel: string | null;
  extType: 'pre' | 'post' | null;
  extPrice: number | null;
  extChange: number | null;
  /** The close the extended move is measured against: regular close for post, prev close for pre. */
  extAnchor: number | null;
  prevClose: number | null;
  /** Official regular-session close (prevClose + regular_trading_change); null when the row lacks it. */
  regularClose: number | null;
}

interface StockSearchResult {
  query: string;
  results: unknown[];
  count: number;
}

/**
 * Normalize a symbol to its cache/lookup key spelling: trim, strip a leading
 * `^` index caret, uppercase. The single source for the caret-stripping
 * normalizer that MarketView/Dashboard/quote-layer all need.
 */
export function normalizeIndexKey(symbol: string): string {
  return String(symbol ?? '').trim().replace(/^\^/, '').toUpperCase();
}

/**
 * Compute extended-hours display info from market status and a data row.
 * Accepts both camelCase (snapshot-enriched rows) and snake_case (raw snapshot) field names.
 */
export function getExtendedHoursInfo(
  marketStatus: MarketStatusData | null,
  data: ExtendedHoursRow | null,
  { shortLabels = false } = {},
): ExtendedHoursInfo {
  const isRegularOpen = marketStatus?.market === 'open' && !marketStatus?.afterHours && !marketStatus?.earlyHours;
  const isPreMarket = marketStatus?.earlyHours === true;

  const earlyPct = data?.earlyTradingChangePercent ?? data?.early_trading_change_percent ?? null;
  const latePct = data?.lateTradingChangePercent ?? data?.late_trading_change_percent ?? null;

  const extPct = isPreMarket && earlyPct != null
    ? earlyPct
    : !isRegularOpen && latePct != null
      ? latePct
      : null;

  const extLabel = isPreMarket && earlyPct != null
    ? (shortLabels ? 'PM' : 'Pre-Market')
    : !isRegularOpen && latePct != null
      ? (shortLabels ? 'AH' : 'After-Hours')
      : null;

  const extType: 'pre' | 'post' | null = extLabel ? (isPreMarket && earlyPct != null ? 'pre' : 'post') : null;

  const prevClose = data?.previousClose ?? data?.previous_close ?? null;
  // Prefer the provider-exact close; the change fields are served at reduced
  // precision (1dp), so deriving the close from them is off by cents.
  const regularChange = data?.regularTradingChange ?? data?.regular_trading_change ?? null;
  const regularClose = data?.regularClose ?? data?.regular_close
    ?? (prevClose != null && regularChange != null
      ? Math.round((prevClose + regularChange) * 100) / 100
      : null);

  // The extended moves are declared against different closes: early
  // (pre-market) vs the previous daily close, late (after-hours) vs today's
  // regular close. Anchoring both on prevClose produced nonsense post prices.
  const extAnchor = extType === 'post' ? (regularClose ?? prevClose) : prevClose;

  // Extended price precedence: minute-aggregate close (the consolidated last
  // sale — what the chart's bars show; the provider's last_trade and the
  // change fields derived from it can track an odd-lot print that doesn't
  // update the official last) → exact dollar change → rounded percent. When
  // the aggregate close is used, the whole triple is re-derived from it so
  // price/change/percent stay one coherent statement.
  const lastMinuteClose = data?.lastMinuteClose ?? data?.last_minute_close ?? null;
  const extDollar = extType === 'post'
    ? (data?.lateTradingChange ?? data?.late_trading_change ?? null)
    : extType === 'pre'
      ? (data?.earlyTradingChange ?? data?.early_trading_change ?? null)
      : null;
  let extPrice: number | null;
  let extChange: number | null;
  let displayPct = extPct;
  if (extType && lastMinuteClose != null && extAnchor != null) {
    extPrice = lastMinuteClose;
    extChange = Math.round((lastMinuteClose - extAnchor) * 100) / 100;
    displayPct = Math.round((extChange / extAnchor) * 10000) / 100;
  } else {
    extChange = extDollar ?? (extPct != null && extAnchor != null
      ? Math.round(extAnchor * (extPct / 100) * 100) / 100
      : null);
    extPrice = extAnchor != null && extChange != null
      ? Math.round((extAnchor + extChange) * 100) / 100
      : null;
  }

  return { extPct: displayPct, extLabel, extType, extPrice, extChange, extAnchor, prevClose, regularClose };
}

/**
 * Search for stocks by keyword (symbol or company name).
 * GET /api/v1/market-data/search/stocks
 */
export async function searchStocks(query: string, limit = 50): Promise<StockSearchResult> {
  if (!query || !query.trim()) {
    return { query: '', results: [], count: 0 };
  }
  try {
    const params = new URLSearchParams();
    params.append('query', query.trim());
    params.append('limit', String(Math.min(Math.max(1, limit), 100)));
    const { data } = await api.get('/api/v1/market-data/search/stocks', { params });
    return data || { query: query.trim(), results: [], count: 0 };
  } catch (e: unknown) {
    const err = e as { response?: { status?: number; data?: unknown }; message?: string };
    console.error('Search stocks failed:', err?.response?.status, err?.response?.data, err?.message);
    return { query: query.trim(), results: [], count: 0 };
  }
}

/**
 * GET /api/v1/market-data/market-status
 * Returns { market, afterHours, earlyHours, serverTime, exchanges }
 */
export async function fetchMarketStatus({ signal }: { signal?: AbortSignal } = {}): Promise<MarketStatusData> {
  try {
    const { data } = await api.get('/api/v1/market-data/market-status', { signal });
    return data || {};
  } catch (e: unknown) {
    const err = e as { name?: string; message?: string };
    if (err?.name === 'CanceledError' || err?.name === 'AbortError') throw e;
    console.error('[API] fetchMarketStatus failed:', err?.message);
    return {};
  }
}
