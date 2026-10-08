/**
 * Currency-aware price labels for the chart layer.
 *
 * When the protocol endpoint serves the data, use the series header's
 * `price_currency` / `display_decimals`. When only legacy data exists (no
 * currency on the wire), fall back to the exchange-suffix heuristic in
 * ./exchanges — the single place that heuristic lives.
 */
import type { PriceFormatCustom } from 'lightweight-charts';
import { createFormatter } from '@/lib/format';
import { currencyForSymbol, quoteCurrency, resolveCurrency } from './exchanges';

export { currencyForSymbol, resolveCurrency };

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
 * `"<ISO> "` (e.g. `"AUD "`). A missing code is an index level, which is points
 * rather than money, so it gets no prefix, as in {@link formatMoney}.
 */
export function currencySymbol(code?: string | null): string {
  if (!code) return '';
  const c = code.toUpperCase();
  return CURRENCY_SYMBOLS[c] ?? `${c} `;
}

/**
 * The chart price axis label: the currency symbol and a fixed number of
 * decimals. `toFixed` with no locale grouping keeps tick labels narrow and
 * matches the crosshair readout; every other amount prints through
 * {@link formatMoney}.
 */
export function formatPrice(value: number, code?: string | null, decimals = 2): string {
  const n = Number.isFinite(value) ? value : 0;
  return `${currencySymbol(code)}${n.toFixed(chartPlaces(decimals))}`;
}

function chartPlaces(decimals: number): number {
  return Number.isFinite(decimals) && decimals >= 0 ? Math.floor(decimals) : 2;
}

/**
 * A chart price series' format: labels from {@link formatPrice}, stepping one
 * unit in the last displayed place, since a cent step leaves a 4-place FX rate
 * or a pence series served in pounds with ticks coarser than its labels. The
 * labels read `display` live, but the step is fixed when this is called, so a
 * series re-applies it when the decimals change.
 */
export function chartPriceFormat(display: {
  readonly current: { code: string | null; decimals: number };
}): PriceFormatCustom {
  return {
    type: 'custom',
    minMove: 10 ** -chartPlaces(display.current.decimals),
    formatter: (price: number) => formatPrice(price, display.current.code, display.current.decimals),
  };
}

/**
 * Resolve the display currency + decimals for a symbol, preferring protocol
 * metadata (when present) over the legacy suffix heuristic. An index resolves
 * to no code whatever the header says, so its axis and crosshair print bare.
 */
export function resolveDisplayCurrency(
  symbol: string,
  meta?: { currency?: string; displayDecimals?: number } | null,
): { code: string | null; decimals: number } {
  return {
    code: quoteCurrency(meta?.currency, symbol),
    decimals: typeof meta?.displayDecimals === 'number' ? meta.displayDecimals : 2,
  };
}

export interface FormatMoneyOptions {
  /** Prefix `+` on a gain as well as `-` on a loss, ahead of the currency symbol. */
  signed?: boolean;
  /** `CN¥1.23B` (`¥12.30亿` in zh-CN) instead of the full grouped figure. */
  compact?: boolean;
  /** Fraction digits, minimum and maximum both. */
  decimals?: number;
}

type NumberFormatter = (n: number, locale: string) => string;
const moneyFormatters = new Map<string, NumberFormatter>();

function moneyFormatter(code: string | null, signed: boolean, compact: boolean, decimals: number): NumberFormatter {
  const key = `${code ?? ''}|${signed}|${compact}|${decimals}`;
  let format = moneyFormatters.get(key);
  if (!format) {
    format = createFormatter({
      ...(code ? { style: 'currency', currency: code } : {}),
      ...(compact ? { notation: 'compact' } : {}),
      // `negative` drops the sign of a value that rounds to zero, as
      // `exceptZero` does for a signed one: a flat move never reads `-$0.00`.
      signDisplay: signed ? 'exceptZero' : 'negative',
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
    moneyFormatters.set(key, format);
  }
  return format;
}

// What Intl accepts as a currency code; anything else makes it throw.
const ISO_CODE = /^[A-Z]{3}$/;

/**
 * The one way to print an amount of money, in the locale's own currency style
 * (`-CN¥1.23` in en-US, `¥1.23` and `US$1.23` in zh-CN). The sign always leads
 * the currency symbol, and a value that rounds to zero carries none. A `null`
 * code is an index level, which is points rather than money and prints as the
 * bare grouped figure. A missing value renders `N/A`. The locale is an argument
 * (`useLocale()`), as it is for every formatter in lib/format.
 */
export function formatMoney(
  value: number | null | undefined,
  code: string | null,
  locale: string,
  { signed = false, compact = false, decimals = 2 }: FormatMoneyOptions = {},
): string {
  if (value == null || !Number.isFinite(value)) return 'N/A';
  const d = Number.isFinite(decimals) && decimals >= 0 ? Math.min(20, Math.floor(decimals)) : 2;
  const iso = code ? code.trim().toUpperCase() : null;
  if (!iso || ISO_CODE.test(iso)) return moneyFormatter(iso, signed, compact, d)(value, locale);
  // A malformed code still names itself, after the sign like a symbol would.
  const figure = moneyFormatter(null, signed, compact, d)(value, locale);
  const sign = /^[+\-\u2212]/.test(figure) ? figure[0] : '';
  return `${sign}${iso} ${figure.slice(sign.length)}`;
}
