import type {
  OrderAction,
  OrderAssetClass,
  OrderSide,
  OrderStatusGroup,
  OrderTimeInForce,
  OrderType,
} from '@/pages/ChatAgent/utils/api';

/**
 * The page's own vocabulary, spelled out as locale keys, one entry per value.
 *
 * Written out rather than built from the value so a term added to the ledger
 * is a compile error here, and so the locale sweep can see all of them: a
 * template-literal key is invisible to it, and these are the words on a row
 * about money that already moved. The status words and the mode words are not
 * here: both are shared with the chat card in `@/components/orders`.
 */

export const ORDER_STATUS_GROUP_KEY: Record<OrderStatusGroup, string> = {
  awaiting: 'orders.statusGroup.awaiting',
  open: 'orders.statusGroup.open',
  filled: 'orders.statusGroup.filled',
  cancelled: 'orders.statusGroup.cancelled',
  rejected: 'orders.statusGroup.rejected',
  failed: 'orders.statusGroup.failed',
};

export const ORDER_ASSET_CLASS_KEY: Record<OrderAssetClass, string> = {
  equity: 'orders.assetClass.equity',
  option: 'orders.assetClass.option',
  option_combo: 'orders.assetClass.option_combo',
  future: 'orders.assetClass.future',
  crypto: 'orders.assetClass.crypto',
  warrant: 'orders.assetClass.warrant',
  other: 'orders.assetClass.other',
};

export const ORDER_ACTION_KEY: Record<OrderAction, string> = {
  place: 'orders.action.place',
  replace: 'orders.action.replace',
  cancel: 'orders.action.cancel',
  confirm: 'orders.action.confirm',
  stage: 'orders.action.stage',
  unstage: 'orders.action.unstage',
  exercise: 'orders.action.exercise',
  cancel_exercise: 'orders.action.cancel_exercise',
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
