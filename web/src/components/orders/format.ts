import type { OrderSummary } from '@/types/orders';

/**
 * An order's amounts as text, for every surface that draws one. The Orders
 * row and the chat card are the same order seen twice, so a price written
 * with one more digit, or one mask, on one of them than on the other reads as
 * two different trades.
 */

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
    // The locale can be briefly invalid, and a row must not throw on it.
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
export function money(
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
 * An amount on an order or reported back for it, in whatever currency it
 * named. Empty when there is none, so the caller decides what an absence looks
 * like.
 */
export function orderAmount(
  amount: string | null | undefined,
  currency: string | null | undefined,
  opts: { hidden: boolean; locale: string },
): string {
  if (amount == null || amount === '') return '';
  if (opts.hidden) return HIDDEN;
  return money(amount, currency, opts.locale);
}

/** A dollar-amount order names its currency on the amount, not beside it. */
export function orderCurrency(order: OrderSummary): string | null | undefined {
  return order.currency ?? order.notional?.currency;
}
