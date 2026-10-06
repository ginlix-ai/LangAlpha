import { isOrderOpen } from '@/pages/ChatAgent/utils/api/orders';
import type { OrderStatus, OrderSummary, OrderType } from '@/types/orders';

/**
 * What an order comes to in money: the figure a person checks before
 * approving and the one a receipt settles on.
 *
 * Worked out on the decimal strings as integers, never as floats. A price
 * reaches the card as text precisely so nothing on the way rounds it, and a
 * float product of two such strings is already wrong in the last place
 * (0.1 x 3 is 0.30000000000000004) before it is rounded for display.
 */

/** `estimated` is what the order was for; `filled` and `partial` are what it
 *  has cost so far, settled or still moving. */
export type OrderTotalKind = 'estimated' | 'filled' | 'partial';

export interface OrderTotal {
  kind: OrderTotalKind;
  /** A plain decimal with two fraction digits, for the money formatter. */
  amount: string;
  currency: string | null;
}

/** The part of an outcome a total reads: how much filled, at what price, and
 *  whether the order can still move. */
export interface OrderTotalFill {
  status: OrderStatus;
  filled_qty?: string | null;
  avg_fill_price?: string | null;
}

interface Decimal {
  units: bigint;
  scale: number;
}

// The server writes a decimal below a millionth in exponent form ("1.2E-7"),
// which is a real crypto price, so that form is read as well.
const DECIMAL = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d{1,3}))?$/;

function decimal(value: string | null | undefined): Decimal | null {
  const match = DECIMAL.exec(typeof value === 'string' ? value.trim() : '');
  if (!match) return null;
  const [, sign, whole, fraction = '', exponent = '0'] = match;
  let units = BigInt(whole + fraction);
  let scale = fraction.length - Number(exponent);
  if (scale < 0) {
    units *= 10n ** BigInt(-scale);
    scale = 0;
  }
  return { units: sign ? -units : units, scale };
}

/** The types whose limit is the worst price they can trade at, so the size at
 *  the limit is what the order is for. Any other type either trades at a price
 *  nobody has set yet or ignores a price it carries (a market order at moomoo
 *  still sends one). */
export const LIMIT_BOUND_TYPES: ReadonlySet<OrderType> = new Set([
  'limit',
  'stop_limit',
  'limit_if_touched',
  'auction_limit',
]);

function text(value: string | null | undefined): string | null {
  return typeof value === 'string' ? value.trim() || null : null;
}

/**
 * What one unit of size costs in units of the quoted price, or null when that
 * is not known.
 *
 * An option is quoted per share and sold per contract, so a $3.40 premium is
 * $340, and guessing 1 there would understate the order a hundredfold on the
 * card asking someone to approve it. No total beats a wrong one, which is also
 * why a future, a combo and anything unclassified get none: their price is in
 * points, net of legs, or in units this card cannot know.
 */
function multiplier(order: OrderSummary): bigint | null {
  switch (order.asset_class) {
    case 'equity':
    case 'crypto':
    case 'warrant':
      return 1n;
    case 'option': {
      const value = order.instrument?.kind === 'option' ? order.instrument.multiplier : null;
      return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
        ? BigInt(value)
        : null;
    }
    default:
      return null;
  }
}

/** a x b x m, rounded half up to two places. */
function product(a: Decimal, b: Decimal, m: bigint): string {
  const units = a.units * b.units * m;
  const scale = a.scale + b.scale;
  const negative = units < 0n;
  let cents = negative ? -units : units;
  if (scale > 2) {
    const divisor = 10n ** BigInt(scale - 2);
    const rest = cents % divisor;
    cents = cents / divisor + (rest * 2n >= divisor ? 1n : 0n);
  } else {
    cents *= 10n ** BigInt(2 - scale);
  }
  const digits = cents.toString().padStart(3, '0');
  const sign = negative && cents > 0n ? '-' : '';
  return `${sign}${digits.slice(0, -2)}.${digits.slice(-2)}`;
}

/**
 * The order's total, or null when it has none worth stating.
 *
 * Once something has filled, the total is what it filled at. Before that it is
 * the size at the limit, the price the order was willing to trade at, which is
 * still what it was for after a rejection or a cancel. A market order, a stop
 * without a limit and an amount-sized order have no such figure until they
 * fill, and an estimate from a price the order does not trade at would be a
 * number the broker never saw.
 */
export function orderTotal(
  order: OrderSummary | null | undefined,
  fill?: OrderTotalFill | null,
): OrderTotal | null {
  if (!order) return null;
  const m = multiplier(order);
  if (m == null) return null;
  const currency = text(order.currency) ?? text(order.notional?.currency);
  const filled = decimal(fill?.filled_qty);
  const avg = decimal(fill?.avg_fill_price);
  // A vendor reports 0 and 0 for an order nothing has happened to yet, which
  // is not a fill at no cost.
  if (fill && filled && filled.units > 0n) {
    // Something filled at a price nobody reported (a Robinhood option answer
    // carries none): the estimate would sit beside the fill as if it were it.
    if (!avg || avg.units <= 0n) return null;
    return {
      kind: isOrderOpen(fill.status) ? 'partial' : 'filled',
      amount: product(filled, avg, m),
      currency,
    };
  }
  if (!order.order_type || !LIMIT_BOUND_TYPES.has(order.order_type)) return null;
  // An order sent by amount is for that amount, whatever its size works out to.
  if (text(order.notional?.amount)) return null;
  const qty = decimal(order.qty);
  const limit = decimal(order.limit_price);
  if (qty && limit && limit.units > 0n) {
    return { kind: 'estimated', amount: product(qty, limit, m), currency };
  }
  return null;
}
