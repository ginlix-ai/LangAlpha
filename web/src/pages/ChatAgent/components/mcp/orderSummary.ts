import { HIDDEN, orderAmount, orderCurrency } from '@/components/orders/format';
import { instrumentLabel } from '@/components/orders/instrument';
import {
  ORDER_ASSET_CLASS_KEY,
  ORDER_SESSION_KEY,
  ORDER_SIDE_KEY,
  ORDER_TIME_IN_FORCE_KEY,
  ORDER_TYPE_KEY,
  orderWordKey,
} from '@/components/orders/labels';
import {
  LIMIT_BOUND_TYPES,
  orderTotal,
  type OrderTotalFill,
  type OrderTotalKind,
} from '@/components/orders/total';
import type { OrderType } from '@/types/orders';
import type { OrderAction, OrderInstrument, OrderProposal } from '@/types/sse';

/**
 * What a call would do at the broker, read off the server's normalized summary
 * rather than off the raw arguments.
 *
 * This is the question a person actually has in front of a live order (which
 * way, on what, how much, at what price), so the card leads with those and
 * says the rest in a quieter line under them. The arguments stay reachable
 * under the card, because they are the exact frame the vendor will see.
 */

/** Written out rather than built from the action, so the locale sweep and a
 *  reader can both see every verdict these cards can name. */
export const ORDER_ACTION_KEY: Record<OrderAction, string> = {
  place: 'toolArtifact.directTool.orderAction.place',
  replace: 'toolArtifact.directTool.orderAction.replace',
  cancel: 'toolArtifact.directTool.orderAction.cancel',
  confirm: 'toolArtifact.directTool.orderAction.confirm',
  stage: 'toolArtifact.directTool.orderAction.stage',
  unstage: 'toolArtifact.directTool.orderAction.unstage',
  exercise: 'toolArtifact.directTool.orderAction.exercise',
  cancel_exercise: 'toolArtifact.directTool.orderAction.cancelExercise',
};

const FIELD_KEY = {
  quantity: 'toolArtifact.directTool.orderField.quantity',
  notional: 'toolArtifact.directTool.orderField.notional',
  stopPrice: 'toolArtifact.directTool.orderField.stopPrice',
  limitPrice: 'toolArtifact.directTool.orderField.limitPrice',
  price: 'toolArtifact.directTool.orderField.price',
  orderType: 'toolArtifact.directTool.orderField.orderType',
  note: 'toolArtifact.directTool.orderField.note',
};

/** The total column is named for what it adds up: an estimate says so, and a
 *  fill still moving does not read as the final cost. */
export const ORDER_TOTAL_KEY: Record<OrderTotalKind, string> = {
  estimated: 'toolArtifact.directTool.orderTotal.estimated',
  filled: 'toolArtifact.directTool.orderTotal.filled',
  partial: 'toolArtifact.directTool.orderTotal.partial',
};

/** The word in front of the vendor id an amend acts on. */
export const ORDER_REF_KEY = 'toolArtifact.directTool.orderTicket.order';

/** Every locale key the ticket can ask for through a map, so one test can hold
 *  them all against both catalogs. The order's own words (side, type and the
 *  rest) are bare `orders.*` keys, which the tree-wide sweep already reads. */
export const ORDER_SUMMARY_KEYS: readonly string[] = [
  ...Object.values(ORDER_ACTION_KEY),
  ...Object.values(FIELD_KEY),
  ...Object.values(ORDER_TOTAL_KEY),
  ORDER_REF_KEY,
];

/**
 * One labelled piece of an order as drawn: a figure column, an item on the
 * line under the headline, a part of the fill line.
 */
export interface OrderPart {
  /** Which fact this is. Unique within its list, so it is also the React key. */
  field: string;
  labelKey?: string;
  /** The plain reading: the formatted figure, or the raw value when the
   *  catalog has no word for it. */
  value: string;
  /** The catalog's word or sentence for the value, interpolated with
   *  `valueParams` where it is drawn. Anything that rewrites `value` has to
   *  clear these two with it. */
  valueKey?: string;
  valueParams?: Record<string, string>;
}

/** What the card draws for one order, with no wire shapes left in it. */
export interface OrderTicketView {
  side: OrderPart | null;
  /** Kept whole rather than flattened to a label, so the headline can set an
   *  option's contract one step lighter than its underlying. */
  instrument: OrderInstrument | null;
  /** The vendor id an amend acts on, set here only when there is no
   *  instrument to name: then it is the subject of the headline. Beside an
   *  instrument it leads the meta line instead. */
  targetRef: string | null;
  meta: OrderPart[];
  figures: OrderPart[];
}

function text(value: string | null | undefined): string | null {
  return typeof value === 'string' ? value.trim() || null : null;
}

function word(
  field: string,
  map: Readonly<Record<string, string>>,
  value: string,
): OrderPart {
  return { field, value, valueKey: orderWordKey(map, value) ?? undefined };
}

/**
 * Whether the price columns already say what kind of order this is: a limit
 * price alone is a limit order, a stop price alone a stop, both a stop-limit.
 * Anything else has its type said beside them. A market order can carry a
 * price too (moomoo takes one on every order and ignores it on a market
 * order), and drawn without its type that price reads as a limit.
 */
function typeShownByPrices(type: string, prices: { stop: boolean; limit: boolean }): boolean {
  switch (type) {
    case 'limit':
      return prices.limit && !prices.stop;
    case 'stop':
      return prices.stop && !prices.limit;
    case 'stop_limit':
      return prices.stop && prices.limit;
    default:
      return false;
  }
}

/**
 * The ticket to draw for one order. A field the adapter could not fill is not
 * drawn at all rather than drawn empty: on this surface a blank price reads as
 * a market order and a placeholder dash reads as zero.
 *
 * `fill` is what the order has done so far, for a receipt; the approval card
 * has none, so its total is the estimate. `hidden` masks the amounts and
 * nothing else: the side, the instrument and the words are the point of the
 * card, and they are not what a shoulder should not read.
 */
export function orderTicket(
  order: OrderProposal,
  opts: { fill?: OrderTotalFill | null; hidden: boolean; locale: string },
): OrderTicketView {
  const shown = { hidden: opts.hidden, locale: opts.locale };
  const instrument = instrumentLabel(order.instrument) ? (order.instrument ?? null) : null;
  const ref = text(order.target_ref);
  const type = order.order_type ?? null;

  const figures: OrderPart[] = [];
  const qty = text(order.qty);
  const notional = orderAmount(
    text(order.notional?.amount),
    order.notional?.currency ?? order.currency,
    shown,
  );
  // Both when both were sent: which one the vendor goes by is its rule, and
  // a card that drew one would be asking about an order of the other size.
  if (qty) figures.push({ field: 'qty', labelKey: FIELD_KEY.quantity, value: opts.hidden ? HIDDEN : qty });
  if (notional) figures.push({ field: 'notional', labelKey: FIELD_KEY.notional, value: notional });
  const stop = orderAmount(text(order.stop_price), orderCurrency(order), shown);
  const limit = orderAmount(text(order.limit_price), orderCurrency(order), shown);
  // The type sits ahead of the prices unless they already say it. With no
  // price to read it is the price: "Market" where the limit column would be,
  // rather than a blank a reader takes for a market order.
  if (type && !typeShownByPrices(type, { stop: !!stop, limit: !!limit })) {
    figures.push({ ...word('order_type', ORDER_TYPE_KEY, type), labelKey: FIELD_KEY.orderType });
  }
  if (stop) figures.push({ field: 'stop_price', labelKey: FIELD_KEY.stopPrice, value: stop });
  if (limit) {
    // A price on a type that takes no limit is not one: moomoo wants a price
    // on a market order and ignores it.
    const limitPriced = !type || LIMIT_BOUND_TYPES.has(type);
    figures.push({
      field: 'limit_price',
      labelKey: limitPriced ? FIELD_KEY.limitPrice : FIELD_KEY.price,
      value: limit,
    });
  }
  const total = orderTotal(order, opts.fill);
  if (total) {
    figures.push({
      field: 'total',
      labelKey: ORDER_TOTAL_KEY[total.kind],
      value: orderAmount(total.amount, total.currency, shown),
    });
  }

  const meta: OrderPart[] = [];
  if (instrument && ref) meta.push({ field: 'target_ref', labelKey: ORDER_REF_KEY, value: ref });
  // The server defaults the class to "other" on an order that names no
  // instrument, and on a cancel that word describes nothing.
  if (instrument && order.asset_class) {
    meta.push(word('asset_class', ORDER_ASSET_CLASS_KEY, order.asset_class));
  }
  const venue =
    instrument?.kind === 'equity' || instrument?.kind === 'future' ? text(instrument.venue) : null;
  if (venue) meta.push({ field: 'venue', value: venue });
  if (order.time_in_force) {
    meta.push(word('time_in_force', ORDER_TIME_IN_FORCE_KEY, order.time_in_force));
  }
  if (order.session) meta.push(word('session', ORDER_SESSION_KEY, order.session));
  const note = text(order.note);
  if (note) meta.push({ field: 'note', labelKey: FIELD_KEY.note, value: note });

  return {
    side: order.side ? word('side', ORDER_SIDE_KEY, order.side) : null,
    instrument,
    targetRef: instrument ? null : ref,
    meta,
    figures,
  };
}

/**
 * Whether the adapter read what a person needs in order to know what they are
 * answering. When a vendor renames a field, the value lands in `extras`, and
 * the ticket comes out shorter while still looking whole: a buy with no size
 * reads as a complete order. So the card has to be told what is missing rather
 * than just drawing less.
 */
export function orderReadFully(order: OrderProposal): boolean {
  const named = instrumentNamed(order.instrument);
  // An option known only by the vendor's id is exact to the vendor and says
  // nothing a person can check: no underlying, expiry, strike or right. It is
  // enough to address a cancel, not to approve a position on.
  const checkable =
    named && !(order.asset_class === 'option' && order.instrument?.kind === 'opaque');
  const target = !!text(order.target_ref);
  const limit = !!text(order.limit_price);
  const stop = !!text(order.stop_price);
  switch (order.action) {
    case 'place':
    case 'stage':
      return (
        checkable &&
        // A spread states its side on each leg, so it has no one side to read.
        (!!order.side || order.instrument?.kind === 'combo') &&
        !!(text(order.qty) || text(order.notional?.amount)) &&
        // A limit order with no limit drawn reads as one with no ceiling.
        (!order.order_type || !LIMIT_BOUND_TYPES.has(order.order_type) || limit) &&
        (!order.order_type || !STOP_PRICED_TYPES.has(order.order_type) || stop)
      );
    case 'exercise':
      return checkable && !!text(order.qty);
    // An exercise is addressed by its contract rather than by an order id.
    case 'cancel_exercise':
      return named || target;
    // A replace that draws nothing new reads as one that changes nothing.
    case 'replace':
      return target && !!(text(order.qty) || text(order.notional?.amount) || limit || stop);
    default:
      return target;
  }
}

const STOP_PRICED_TYPES: ReadonlySet<OrderType> = new Set([
  'stop',
  'stop_limit',
  'market_if_touched',
  'limit_if_touched',
]);

/** Whether the instrument says what is traded, not just where: a venue alone
 *  names no stock, and an option short of its expiry, strike or right is a
 *  different contract from the one being approved. */
function instrumentNamed(instrument: OrderInstrument | null | undefined): boolean {
  if (!instrument) return false;
  switch (instrument.kind) {
    case 'equity':
    case 'future':
      return !!text(instrument.symbol);
    case 'option':
      return !!(
        text(instrument.underlying) &&
        text(instrument.expiration) &&
        text(instrument.strike) &&
        (instrument.right === 'C' || instrument.right === 'P')
      );
    default:
      return !!instrumentLabel(instrument);
  }
}
