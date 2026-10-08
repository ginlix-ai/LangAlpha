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
 * Mirrors the backend's suffix registry (`libs/market-protocol/market_protocol/symbology.py`
 * `_MICS`) — keep the two in sync when adding venues.
 */

export interface ExchangeInfo {
  /** ISO listing currency. */
  currency: string;
  /** True for non-US exchanges (drives `isUSEquity`). */
  foreign: boolean;
  /** IANA timezone of the listing venue (drives market-local chart display). */
  tz: string;
  /** Short venue name prefixed to a status badge (`HK Closed`); index
   *  families carry none. */
  label?: string;
  /** The suffix this one is shown as. Shanghai has two spellings and both are
   *  accepted, but `.SH` is the one stored, compared and shown (the backend's
   *  `display_spelling`). */
  displayAs?: string;
  /** Code prefix that marks an index on this venue. CN code ranges are
   *  partitioned by instrument type, so the code alone names the class
   *  (the backend's `_CN_INDEX_PREFIXES`). */
  indexCodePrefix?: string;
  /** Digits a numeric code is written at. HKEX codes are typed with and
   *  without leading zeros (700, 00700) and all name one listing, so they
   *  collapse to this width (the backend's `_venue_code`). */
  codeWidth?: number;
}

/** Venue timezone for US listings and the fallback for anything unknown. */
export const US_MARKET_TZ = 'America/New_York';

const EXCHANGE_SUFFIXES: Record<string, ExchangeInfo> = {
  L: { currency: 'GBP', foreign: true, tz: 'Europe/London', label: 'LON' }, // London
  HK: { currency: 'HKD', foreign: true, tz: 'Asia/Hong_Kong', label: 'HK', codeWidth: 4 }, // Hong Kong
  T: { currency: 'JPY', foreign: true, tz: 'Asia/Tokyo', label: 'TYO' }, // Tokyo
  TO: { currency: 'CAD', foreign: true, tz: 'America/Toronto', label: 'TSX' }, // Toronto
  PA: { currency: 'EUR', foreign: true, tz: 'Europe/Paris', label: 'PAR' }, // Paris
  DE: { currency: 'EUR', foreign: true, tz: 'Europe/Berlin', label: 'XETRA' }, // Xetra / Frankfurt
  AS: { currency: 'EUR', foreign: true, tz: 'Europe/Amsterdam', label: 'AMS' }, // Amsterdam
  MI: { currency: 'EUR', foreign: true, tz: 'Europe/Rome', label: 'MIL' }, // Milan
  MC: { currency: 'EUR', foreign: true, tz: 'Europe/Madrid', label: 'BME' }, // Madrid
  SW: { currency: 'CHF', foreign: true, tz: 'Europe/Zurich', label: 'SIX' }, // Switzerland (SIX)
  SS: { currency: 'CNY', foreign: true, tz: 'Asia/Shanghai', label: 'SH', displayAs: 'SH', indexCodePrefix: '000' }, // Shanghai (vendor spelling, still accepted)
  SH: { currency: 'CNY', foreign: true, tz: 'Asia/Shanghai', label: 'SH', indexCodePrefix: '000' }, // Shanghai
  SZ: { currency: 'CNY', foreign: true, tz: 'Asia/Shanghai', label: 'SZ', indexCodePrefix: '399' }, // Shenzhen
  BJ: { currency: 'CNY', foreign: true, tz: 'Asia/Shanghai', label: 'BJ', indexCodePrefix: '899' }, // Beijing (BSE)
  KS: { currency: 'KRW', foreign: true, tz: 'Asia/Seoul', label: 'KRX' }, // Korea (KOSPI)
  KQ: { currency: 'KRW', foreign: true, tz: 'Asia/Seoul', label: 'KOSDAQ' }, // Korea (KOSDAQ)
  TW: { currency: 'TWD', foreign: true, tz: 'Asia/Taipei', label: 'TWSE' }, // Taiwan
  SI: { currency: 'SGD', foreign: true, tz: 'Asia/Singapore', label: 'SGX' }, // Singapore
  BO: { currency: 'INR', foreign: true, tz: 'Asia/Kolkata', label: 'BSE' }, // Bombay (BSE)
  NS: { currency: 'INR', foreign: true, tz: 'Asia/Kolkata', label: 'NSE' }, // India (NSE)
  AX: { currency: 'AUD', foreign: true, tz: 'Australia/Sydney', label: 'ASX' }, // Australia (ASX)
};

/**
 * Caret-spelled index families that do not trade in New York. Mirrors the
 * backend's `_INDEX_FAMILIES` home venues (`symbology.py`): the family's home venue
 * decides its clock and its quote currency. Venue-listed indices
 * (`000001.SH`) resolve through the suffix table like any other listing.
 */
const INDEX_FAMILIES: Record<string, ExchangeInfo> = {
  HSI: { currency: 'HKD', foreign: true, tz: 'Asia/Hong_Kong' },
  HSCE: { currency: 'HKD', foreign: true, tz: 'Asia/Hong_Kong' },
  N225: { currency: 'JPY', foreign: true, tz: 'Asia/Tokyo' },
  FTSE: { currency: 'GBP', foreign: true, tz: 'Europe/London' },
  GDAXI: { currency: 'EUR', foreign: true, tz: 'Europe/Berlin' },
  STOXX50E: { currency: 'EUR', foreign: true, tz: 'Europe/Berlin' },
  FCHI: { currency: 'EUR', foreign: true, tz: 'Europe/Paris' },
};

/**
 * A venue-less index family spelled with a caret (`^GSPC`). This is a spelling
 * question, not an asset-class one: it picks the index endpoints, which read
 * the bare family name, while a venue-listed index (`000300.SH`) is served by
 * the stock endpoints like any listing, since the backend routes it by the
 * instrument it resolves to. "Is this an index" is {@link isIndexListing}.
 */
export function isIndexFamilySpelling(symbol: string | null | undefined): boolean {
  return !!symbol && symbol.trim().startsWith('^');
}

/** Split a ticker on its venue suffix, or null when it has no known one. */
function splitVenueSuffix(symbol: string): { stem: string; info: ExchangeInfo } | null {
  const s = symbol.trim().toUpperCase();
  const dotIdx = s.lastIndexOf('.');
  if (dotIdx === -1) return null;
  const info = EXCHANGE_SUFFIXES[s.slice(dotIdx + 1)];
  return info ? { stem: s.slice(0, dotIdx), info } : null;
}

/** Look up the exchange info for a ticker's suffix, or null when it has none. */
function exchangeInfoForSymbol(symbol: string): ExchangeInfo | null {
  if (isIndexFamilySpelling(symbol)) return INDEX_FAMILIES[symbol.trim().slice(1).toUpperCase()] ?? null;
  return splitVenueSuffix(symbol)?.info ?? null;
}

/**
 * The spelling a symbol is stored, compared and shown in: trimmed, uppercased,
 * and with its venue suffix and code in our form (`600519.ss` becomes
 * `600519.SH`, `700.HK` becomes `0700.HK`). Mirrors the backend's
 * `display_spelling`, so applying it where a symbol enters the page means
 * every key built from it matches the server's. A Chinese IME's full-width
 * forms fold first, and its ideographic full stop reads as a dot.
 */
export function displaySpelling(symbol: string): string {
  const s = symbol.normalize('NFKC').replace(/。/g, '.').trim().toUpperCase();
  const venue = splitVenueSuffix(s);
  if (!venue) return s;
  const { stem, info } = venue;
  const code =
    info.codeWidth && /^\d+$/.test(stem)
      ? (stem.replace(/^0+/, '') || '0').padStart(info.codeWidth, '0')
      : stem;
  return `${code}.${info.displayAs ?? s.slice(s.lastIndexOf('.') + 1)}`;
}

/**
 * Whether a listing quotes a level in points rather than a price. The wire's
 * asset class decides when the row carries one; otherwise a caret family is an
 * index, and so is a CN code in its venue's index range (`000300.SH`,
 * `399001.SZ`), which the code alone settles.
 */
export function isIndexListing(symbol: string | null | undefined, assetClass?: string | null): boolean {
  if (assetClass) return assetClass === 'index';
  if (!symbol) return false;
  if (isIndexFamilySpelling(symbol)) return true;
  const venue = splitVenueSuffix(symbol);
  const prefix = venue?.info.indexCodePrefix;
  return !!venue && !!prefix && /^\d+$/.test(venue.stem) && venue.stem.startsWith(prefix);
}

/** Infer listing currency from a ticker's exchange suffix; defaults to USD. */
export function currencyForSymbol(symbol?: string | null): string {
  if (!symbol) return 'USD';
  return exchangeInfoForSymbol(symbol)?.currency ?? 'USD';
}

/** Short venue name for a ticker's listing (`HK`, `SH`, `LON`), or null for
 *  US listings, index families and unknown suffixes. */
export function venueLabelForSymbol(symbol?: string | null): string | null {
  if (!symbol) return null;
  return exchangeInfoForSymbol(symbol)?.label ?? null;
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

/** The currency a row is quoted in: its own code when the wire carried one,
 *  else the listing currency its symbol's venue implies. */
export function resolveCurrency(code: string | null | undefined, symbol: string | null | undefined): string {
  return code || currencyForSymbol(symbol);
}

/** The currency to print a row's figures in, or null for an index, whose level
 *  is points and prints with no currency symbol. */
export function quoteCurrency(
  code: string | null | undefined,
  symbol: string | null | undefined,
  assetClass?: string | null,
): string | null {
  return isIndexListing(symbol, assetClass) ? null : resolveCurrency(code, symbol);
}

/** Returns true for US-listed equities (not indexes, not foreign stocks). */
export function isUSEquity(sym: string | null | undefined): boolean {
  if (!sym) return true;
  if (isIndexListing(sym)) return false;
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
