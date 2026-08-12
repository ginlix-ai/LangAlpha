/**
 * Currency-aware price labels for the chart layer.
 *
 * When the protocol endpoint serves the data, use the series header's
 * `price_currency` / `display_decimals`. When only legacy data exists (no
 * currency on the wire), fall back to the exchange-suffix heuristic in
 * ./exchanges — the single place that heuristic lives.
 */
import { currencyForSymbol } from './exchanges';

export { currencyForSymbol };

const CURRENCY_SYMBOLS: Record<string, string> = {
  USD: '$',
  GBP: '£',
  HKD: 'HK$',
  EUR: '€',
  JPY: '¥',
  CNY: 'CN¥',
};

/**
 * Symbol/prefix for an ISO currency code. Unknown codes fall back to
 * `"<ISO> "` (e.g. `"AUD "`); a missing code defaults to `"$"` (USD).
 */
export function currencySymbol(code?: string | null): string {
  if (!code) return '$';
  const c = code.toUpperCase();
  return CURRENCY_SYMBOLS[c] ?? `${c} `;
}

/**
 * Format a price with its currency symbol and a fixed number of decimals.
 * Uses `toFixed` (no locale grouping) so it stays deterministic and matches the
 * chart crosshair/axis style; widget headers keep their own grouped formatters
 * and just prepend {@link currencySymbol}.
 */
export function formatPrice(value: number, code?: string | null, decimals = 2): string {
  const d = Number.isFinite(decimals) && decimals >= 0 ? Math.floor(decimals) : 2;
  const n = Number.isFinite(value) ? value : 0;
  return `${currencySymbol(code)}${n.toFixed(d)}`;
}

/**
 * Resolve the display currency + decimals for a symbol, preferring protocol
 * metadata (when present) over the legacy suffix heuristic.
 */
export function resolveDisplayCurrency(
  symbol: string,
  meta?: { currency?: string; displayDecimals?: number } | null,
): { code: string; decimals: number } {
  return {
    code: meta?.currency || currencyForSymbol(symbol),
    decimals: typeof meta?.displayDecimals === 'number' ? meta.displayDecimals : 2,
  };
}
