/**
 * Shared market utilities used across Dashboard and MarketView.
 */
import { api } from '@/api/client';
import { displaySpelling, venueLabelForSymbol } from '@/lib/bars/exchanges';

export interface MarketStatusData {
  market?: string;
  afterHours?: boolean;
  earlyHours?: boolean;
  [key: string]: unknown;
}

/** The extended-session fields, in either spelling: a display row's camelCase or the raw snapshot's snake_case. */
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
}

/** A quote row as {@link sessionQuote} reads it: its own figures, and its extended-session fields beside them. */
export interface SessionRow {
  price?: number | null;
  change?: number | null;
  changePercent?: number | null;
  session?: ExtendedHoursRow | null;
}

/** One row of `GET /api/v1/market-data/search/stocks`. */
export interface StockSearchHit {
  symbol: string;
  name?: string;
  // CN/HK listings carry both spellings; `name` stays the single-name fallback
  // so callers that do not resolve a dual name are unaffected.
  nameLocal?: string;
  nameEn?: string;
  exchangeShortName?: string;
  stockExchange?: string;
  currency?: string;
  /** How directly the row answers the query, as the server's ranking read it. */
  match?: 'exact' | 'symbol_prefix' | 'name_word' | 'other';
}

interface StockSearchResult {
  query: string;
  results: StockSearchHit[];
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

export interface SessionQuote {
  /**
   * The row's big number and its change. In an extended session it is the
   * last official close (market convention): today's regular close after
   * hours with its change against the previous close, the previous close
   * before the open with no change yet. Otherwise it is the row's own price.
   */
  headline: { price: number | null; change: number | null; pct: number | null };
  /** The extended-hours print against its own anchor, when the venue is in one and the row carries it. */
  ext: {
    type: 'pre' | 'post';
    label: string;
    price: number;
    change: number | null;
    pct: number;
  } | null;
}

/**
 * Split a quote into its official close and its extended-hours move, the one
 * rule the stock header and every watchlist or portfolio row follow, so the
 * extended move is never counted inside the headline change as well. The
 * headline defaults to the row's own figures. A live tick (`livePrice`)
 * stands in for the row's extended price.
 */
export function sessionQuote(
  marketStatus: MarketStatusData | null,
  row: SessionRow | null,
  {
    previousClose,
    livePrice = null,
    shortLabels = false,
  }: {
    previousClose?: number | null;
    livePrice?: number | null;
    shortLabels?: boolean;
  } = {},
): SessionQuote {
  const asIs: SessionQuote = { headline: { price: row?.price ?? null, change: row?.change ?? null, pct: row?.changePercent ?? null }, ext: null };
  const data = row?.session ?? null;
  const isRegularOpen = marketStatus?.market === 'open' && !marketStatus?.afterHours && !marketStatus?.earlyHours;
  const isPreMarket = marketStatus?.earlyHours === true;
  const earlyPct = data?.earlyTradingChangePercent ?? data?.early_trading_change_percent ?? null;
  const latePct = data?.lateTradingChangePercent ?? data?.late_trading_change_percent ?? null;
  // An unknown session splits nothing, and a pre-market session without an
  // early move never falls back to the previous evening's late move.
  const type: 'pre' | 'post' | null = isPreMarket
    ? (earlyPct != null ? 'pre' : null)
    : marketStatus != null && !isRegularOpen && latePct != null ? 'post' : null;
  if (!type) return asIs;
  const label = type === 'pre' ? (shortLabels ? 'PM' : 'Pre-Market') : (shortLabels ? 'AH' : 'After-Hours');
  const rawPct = (type === 'pre' ? earlyPct : latePct) as number;

  const rowPrev = data?.previousClose ?? data?.previous_close ?? null;
  // Prefer the provider-exact close; the change fields are served at reduced
  // precision (1dp), so deriving the close from them is off by cents.
  const regularChange = data?.regularTradingChange ?? data?.regular_trading_change ?? null;
  const regularClose = data?.regularClose ?? data?.regular_close
    ?? (rowPrev != null && regularChange != null ? rowPrev + regularChange : null);
  const prev = previousClose ?? rowPrev;
  const settledClose = type === 'post' ? regularClose : prev;
  if (settledClose == null) return asIs;

  // The extended moves are declared against different closes: early
  // (pre-market) vs the previous daily close, late (after-hours) vs today's
  // regular close. Anchoring both on prevClose produced nonsense post prices.
  const anchor = type === 'post' ? regularClose : rowPrev;

  // Extended price precedence: the live tick, then the minute-aggregate close
  // (the consolidated last sale — what the chart's bars show; the provider's
  // last_trade and the change fields derived from it can track an odd-lot
  // print that doesn't update the official last) → exact dollar change →
  // rounded percent. When a print is used, the whole triple is re-derived
  // from it so price/change/percent stay one coherent statement. Nothing is
  // rounded here: a sub-dollar move rounded to the cent reads 0.00% on a +2%
  // print.
  const printed = livePrice ?? data?.lastMinuteClose ?? data?.last_minute_close ?? null;
  const extDollar = type === 'post'
    ? (data?.lateTradingChange ?? data?.late_trading_change ?? null)
    : (data?.earlyTradingChange ?? data?.early_trading_change ?? null);
  let extPrice: number | null;
  let extChange: number | null;
  let extPct = rawPct;
  if (printed != null && anchor != null) {
    extPrice = printed;
    extChange = printed - anchor;
    if (anchor) extPct = (extChange / anchor) * 100;
  } else {
    extChange = extDollar ?? (anchor != null ? anchor * (rawPct / 100) : null);
    extPrice = livePrice ?? (anchor != null && extChange != null ? anchor + extChange : null);
  }
  if (extPrice == null) return asIs;

  const settledChange = type === 'post' && prev != null ? settledClose - prev : null;
  const settledPct = settledChange != null && prev ? (settledChange / prev) * 100 : null;
  return {
    headline: { price: settledClose, change: settledChange, pct: settledPct },
    ext: { type, label, price: extPrice, change: extChange, pct: extPct },
  };
}

/**
 * Search for stocks by keyword (symbol or company name).
 * GET /api/v1/market-data/search/stocks
 */
/**
 * A ticker typed straight in, when the search has nothing better to offer.
 * Wide enough for the instrument forms the data providers take besides
 * equities: `EURUSD=X`, `ES=F`, `X:BTCUSD`, `I:SPX`.
 */
const TICKER_RE = /^[A-Za-z0-9.^=:-]{1,16}$/;

/** Every symbol field hands over the same shape: the display spelling
 *  (trimmed, uppercase, `.SH` for Shanghai), never blank. */
export function normalizeSymbolInput(raw: string): string | null {
  const sym = displaySpelling(raw);
  return sym ? sym : null;
}

/** The typed text as a ticker in its display spelling, or null when it reads as a company name.
 *  Tested after the fold, so a code typed through a Chinese IME (`６００５１９。ＳＨ`) reads as one. */
export function readTypedTicker(raw: string): string | null {
  const sym = displaySpelling(raw);
  return TICKER_RE.test(sym) ? sym : null;
}

/** Bare exchange codes and the providers' instrument forms, none of which a word takes.
 *  Four digits only with a leading zero (`0700`): any other four could be a year. */
const MARKED_TICKER_RE = /^(?:\d{5,6}|0\d{3}|[A-Z0-9]{1,10}=[XF]|\^[A-Z0-9]{1,10}|[A-Z]:[A-Z0-9]{2,14})$/;

/**
 * A typed ticker in a form no word takes, so it is one even when no search
 * confirms it: a known venue suffix (`600519.SH`, `0700.HK`), a bare exchange
 * code (`600519`), or an instrument form (`EURUSD=X`, `ES=F`, `^GSPC`,
 * `X:BTCUSD`). A plain one (`NVDA`, `DARK`) could as well be a word, and only
 * a search hit says which.
 */
export function readMarkedTicker(raw: string): string | null {
  const sym = readTypedTicker(raw);
  return sym && (venueLabelForSymbol(sym) || MARKED_TICKER_RE.test(sym)) ? sym : null;
}

export async function searchStocks(
  query: string,
  limit = 50,
  { signal }: { signal?: AbortSignal } = {},
): Promise<StockSearchResult> {
  if (!query || !query.trim()) {
    return { query: '', results: [], count: 0 };
  }
  try {
    const params = new URLSearchParams();
    params.append('query', query.trim());
    params.append('limit', String(Math.min(Math.max(1, limit), 100)));
    const { data } = await api.get('/api/v1/market-data/search/stocks', { params, signal });
    return data || { query: query.trim(), results: [], count: 0 };
  } catch (e: unknown) {
    const err = e as { name?: string; response?: { status?: number; data?: unknown }; message?: string };
    // An aborted search is the caller moving on, not a failure to report.
    if (err?.name === 'CanceledError' || err?.name === 'AbortError') throw e;
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
