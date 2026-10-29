import type {
  OptionRight,
  OrderAssetClass,
  OrderSession,
  OrderSide,
  OrderTimeInForce,
  OrderType,
} from '@/types/orders';

/**
 * The words an order is made of, spelled out as locale keys, one entry per
 * value.
 *
 * Shared because the Orders row and the chat card draw the same order, and a
 * side or a duration worded one way on one of them and another way on the
 * other reads as two different trades. Written out rather than built from the
 * value so a term added to the ledger is a compile error here, and so the
 * locale sweep can see every key.
 */

export const ORDER_ASSET_CLASS_KEY: Record<OrderAssetClass, string> = {
  equity: 'orders.assetClass.equity',
  option: 'orders.assetClass.option',
  option_combo: 'orders.assetClass.option_combo',
  future: 'orders.assetClass.future',
  crypto: 'orders.assetClass.crypto',
  warrant: 'orders.assetClass.warrant',
  other: 'orders.assetClass.other',
};

export const ORDER_SIDE_KEY: Record<OrderSide, string> = {
  buy: 'orders.side.buy',
  sell: 'orders.side.sell',
  sell_short: 'orders.side.sell_short',
  buy_to_cover: 'orders.side.buy_to_cover',
};

export const ORDER_TYPE_KEY: Record<OrderType, string> = {
  market: 'orders.orderType.market',
  limit: 'orders.orderType.limit',
  stop: 'orders.orderType.stop',
  stop_limit: 'orders.orderType.stop_limit',
  market_if_touched: 'orders.orderType.market_if_touched',
  limit_if_touched: 'orders.orderType.limit_if_touched',
  auction: 'orders.orderType.auction',
  auction_limit: 'orders.orderType.auction_limit',
};

/**
 * How long the order stands, in words. The wire values read as vendor jargon
 * on a row a person scans for what they told the broker to do, and the three
 * beyond `day` and `gtc` say nothing at all until they are spelled out.
 */
export const ORDER_TIME_IN_FORCE_KEY: Record<OrderTimeInForce, string> = {
  day: 'orders.timeInForce.day',
  gtc: 'orders.timeInForce.gtc',
  overnight: 'orders.timeInForce.overnight',
  overnight_next_day: 'orders.timeInForce.overnight_next_day',
  at_the_open: 'orders.timeInForce.at_the_open',
};

export const ORDER_SESSION_KEY: Record<OrderSession, string> = {
  rth: 'orders.session.rth',
  rth_plus_ext: 'orders.session.rth_plus_ext',
  overnight: 'orders.session.overnight',
  all_day: 'orders.session.all_day',
};

/** A call or a put in words: the letter is the vendor's shorthand, and on a
 *  card asking someone to buy one it is the part least worth guessing at. */
export const OPTION_RIGHT_KEY: Record<OptionRight, string> = {
  C: 'orders.optionRight.C',
  P: 'orders.optionRight.P',
};

/**
 * The key for a wire value, or null when the vocabulary has no word for it.
 *
 * The caller then draws the raw value: a row written before the server learned
 * a term, or a value it never typed, still says something true, where a
 * missing word would leave a blank that reads as nothing having been set.
 */
export function orderWordKey(
  map: Readonly<Record<string, string>>,
  value: string | null | undefined,
): string | null {
  return value && Object.hasOwn(map, value) ? map[value] : null;
}
