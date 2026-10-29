import { HIDDEN, money, orderAmount, orderCurrency } from '@/components/orders/format';
import { createDateFormatter } from '@/lib/format';
import type { OrderSummary } from '@/pages/ChatAgent/utils/api';

/**
 * The size of an order: a share count, or the dollar amount for a broker that
 * takes one instead. Exactly one of the two is ever set.
 */
export function orderSize(
  order: OrderSummary | null | undefined,
  opts: { hidden: boolean; locale: string },
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
 * The limit and the stop are read apart because a stop-limit order carries
 * both and they answer different questions: the stop is the price that
 * triggers it, the limit the worst it may then fill at. Empty when the order
 * carries none, the same contract `orderSize` and `orderAmount` keep.
 */
export function orderLimitPrice(
  order: OrderSummary | null | undefined,
  opts: { hidden: boolean; locale: string },
): string {
  if (!order) return '';
  return orderAmount(order.limit_price, orderCurrency(order), opts);
}

export function orderStopPrice(
  order: OrderSummary | null | undefined,
  opts: { hidden: boolean; locale: string },
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
export function formatOrderTime(value: string | null | undefined, locale: string): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return orderDate(date, locale);
}
