/**
 * One exchange-suffix table for the chart layer — the single source for
 * "what currency does this ticker list in", "is this a US equity", and
 * "what timezone does its venue trade in".
 *
 * Keyed by the ticker's exchange suffix (the part after the last dot,
 * uppercased). `foreign` is true for any non-US listing; `currency` is its
 * listing currency, used only as the legacy fallback when the backend didn't
 * send a `price_currency` on the wire; `tz` is the venue's IANA timezone,
 * used to render chart times in market-local wall clock.
 *
 * Mirrors the backend's suffix registry (`src/market_protocol/symbology.py`
 * `_MICS`) — keep the two in sync when adding venues.
 */

export interface ExchangeInfo {
  /** ISO listing currency. */
  currency: string;
  /** True for non-US exchanges (drives `isUSEquity`). */
  foreign: boolean;
  /** IANA timezone of the listing venue (drives market-local chart display). */
  tz: string;
}

/** Venue timezone for US listings and the fallback for anything unknown. */
export const US_MARKET_TZ = 'America/New_York';

const EXCHANGE_SUFFIXES: Record<string, ExchangeInfo> = {
  L: { currency: 'GBP', foreign: true, tz: 'Europe/London' }, // London
  HK: { currency: 'HKD', foreign: true, tz: 'Asia/Hong_Kong' }, // Hong Kong
  T: { currency: 'JPY', foreign: true, tz: 'Asia/Tokyo' }, // Tokyo
  TO: { currency: 'CAD', foreign: true, tz: 'America/Toronto' }, // Toronto
  PA: { currency: 'EUR', foreign: true, tz: 'Europe/Paris' }, // Paris
  DE: { currency: 'EUR', foreign: true, tz: 'Europe/Berlin' }, // Xetra / Frankfurt
  AS: { currency: 'EUR', foreign: true, tz: 'Europe/Amsterdam' }, // Amsterdam
  MI: { currency: 'EUR', foreign: true, tz: 'Europe/Rome' }, // Milan
  MC: { currency: 'EUR', foreign: true, tz: 'Europe/Madrid' }, // Madrid
  SW: { currency: 'CHF', foreign: true, tz: 'Europe/Zurich' }, // Switzerland (SIX)
  SS: { currency: 'CNY', foreign: true, tz: 'Asia/Shanghai' }, // Shanghai
  SZ: { currency: 'CNY', foreign: true, tz: 'Asia/Shanghai' }, // Shenzhen
  KS: { currency: 'KRW', foreign: true, tz: 'Asia/Seoul' }, // Korea (KOSPI)
  KQ: { currency: 'KRW', foreign: true, tz: 'Asia/Seoul' }, // Korea (KOSDAQ)
  TW: { currency: 'TWD', foreign: true, tz: 'Asia/Taipei' }, // Taiwan
  SI: { currency: 'SGD', foreign: true, tz: 'Asia/Singapore' }, // Singapore
  BO: { currency: 'INR', foreign: true, tz: 'Asia/Kolkata' }, // Bombay (BSE)
  NS: { currency: 'INR', foreign: true, tz: 'Asia/Kolkata' }, // India (NSE)
  AX: { currency: 'AUD', foreign: true, tz: 'Australia/Sydney' }, // Australia (ASX)
};

/** Look up the exchange info for a ticker's suffix, or null when it has none. */
function exchangeInfoForSymbol(symbol: string): ExchangeInfo | null {
  const s = symbol.toUpperCase();
  const dotIdx = s.lastIndexOf('.');
  if (dotIdx === -1) return null;
  return EXCHANGE_SUFFIXES[s.slice(dotIdx + 1)] ?? null;
}

/** Infer listing currency from a ticker's exchange suffix; defaults to USD. */
export function currencyForSymbol(symbol?: string | null): string {
  if (!symbol) return 'USD';
  return exchangeInfoForSymbol(symbol)?.currency ?? 'USD';
}

/**
 * Venue IANA timezone for a ticker, inferred from its exchange suffix.
 * US listings, indexes, and anything unrecognized fall back to ET
 * ({@link US_MARKET_TZ}). Every epoch→chart-time conversion for a symbol MUST
 * go through this so all data paths (initial load, delta poll, WS ticks)
 * agree on the encoding — mixed timezones break merge-by-time silently.
 */
export function timezoneForSymbol(symbol?: string | null): string {
  if (!symbol) return US_MARKET_TZ;
  return exchangeInfoForSymbol(symbol)?.tz ?? US_MARKET_TZ;
}

/** Returns true for US-listed equities (not indexes, not foreign stocks). */
export function isUSEquity(sym: string | null | undefined): boolean {
  if (!sym) return true;
  if (sym.startsWith('^')) return false; // index
  const info = exchangeInfoForSymbol(sym);
  // No suffix, or a suffix we don't recognize as foreign → treat as US.
  return info ? !info.foreign : true;
}

/** Exchange suffixes that denote a foreign (non-US) listing — derived from the
 *  table above. Kept for back-compat with callers that want the raw set. */
export const FOREIGN_EXCHANGES: ReadonlySet<string> = new Set(
  Object.entries(EXCHANGE_SUFFIXES)
    .filter(([, info]) => info.foreign)
    .map(([suffix]) => suffix),
);
