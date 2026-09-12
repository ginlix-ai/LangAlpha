import { createDateFormatter } from '@/lib/format';
import type { OrderSummary } from '@/pages/ChatAgent/utils/api';

/** What a hidden amount reads as. Same glyphs the account mask uses. */
export const HIDDEN = '••••';

// Intl draws at most 100 fraction digits, so a longer fraction is passed
// through as text rather than cut.
const PLAIN_DECIMAL = /^-?\d+(?:\.(\d{1,100}))?$/;
const CURRENCY_CODE = /^[A-Za-z]{3}$/;

const moneyFormats = new Map<string, Intl.NumberFormat>();

function moneyFormat(
  locale: string | undefined,
  currency: string | null,
  digits: number,
): Intl.NumberFormat {
  const key = `${locale ?? ''}|${currency ?? ''}|${digits}`;
  let format = moneyFormats.get(key);
  if (!format) {
    const opts: Intl.NumberFormatOptions = {
      ...(currency ? { style: 'currency', currency } : {}),
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    };
    // `i18n.language` can be briefly invalid, and a row must not throw on it.
    try {
      format = new Intl.NumberFormat(locale || undefined, opts);
    } catch {
      format = new Intl.NumberFormat(undefined, opts);
    }
    moneyFormats.set(key, format);
  }
  return format;
}

/**
 * An amount as the tool wrote it. A price that came with no currency is drawn
 * bare rather than as dollars, and the text itself goes to Intl rather than a
 * float, so every fraction digit it carries survives.
 */
function money(
  amount: string | null | undefined,
  currency: unknown,
  locale: string | undefined,
): string {
  const text = amount ?? '';
  const match = PLAIN_DECIMAL.exec(text);
  if (!match) return text;
  const value = text as Intl.StringNumericLiteral;
  const digits = Math.max(2, match[1]?.length ?? 0);
  const code = typeof currency === 'string' ? currency.trim() : '';
  if (CURRENCY_CODE.test(code)) {
    return moneyFormat(locale, code.toUpperCase(), digits).format(value);
  }
  const figure = moneyFormat(locale, null, digits).format(value);
  return code ? `${figure} ${code}` : figure;
}

/**
 * The size of an order: a share count, or the dollar amount for a broker that
 * takes one instead. Exactly one of the two is ever set.
 */
export function orderSize(
  order: OrderSummary | null | undefined,
  opts: { hidden: boolean; locale?: string },
): string {
  if (!order) return '';
  if (opts.hidden && (order.qty != null || order.notional != null)) return HIDDEN;
  if (order.qty != null) return order.qty;
  if (order.notional?.amount != null) {
    return money(order.notional.amount, order.notional.currency, opts.locale);
  }
  return '';
}

/**
 * An amount the vendor reported back, in whatever currency it named. Empty
 * when there is none, so the caller decides what an absence looks like, the
 * same contract `orderSize` and the two price readers keep.
 */
export function orderAmount(
  amount: string | null | undefined,
  currency: string | null | undefined,
  opts: { hidden: boolean; locale?: string },
): string {
  if (amount == null || amount === '') return '';
  if (opts.hidden) return HIDDEN;
  return money(amount, currency, opts.locale);
}

/** A dollar-amount order names its currency on the amount, not beside it. */
function orderCurrency(order: OrderSummary): string | null | undefined {
  return order.currency ?? order.notional?.currency;
}

/**
 * The limit and the stop are read apart because a stop-limit order carries
 * both and they answer different questions: the stop is the price that
 * triggers it, the limit the worst it may then fill at.
 */
export function orderLimitPrice(
  order: OrderSummary | null | undefined,
  opts: { hidden: boolean; locale?: string },
): string {
  if (!order) return '';
  return orderAmount(order.limit_price, orderCurrency(order), opts);
}

export function orderStopPrice(
  order: OrderSummary | null | undefined,
  opts: { hidden: boolean; locale?: string },
): string {
  if (!order) return '';
  return orderAmount(order.stop_price, orderCurrency(order), opts);
}

const orderDate = createDateFormatter({
  month: 'short',
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
});

/** The moment in the viewer's locale, or the empty string when there is none. */
export function formatOrderTime(value: string | null | undefined): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return orderDate(date);
}
