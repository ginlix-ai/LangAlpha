import type { OrderAction, OrderStatusGroup } from '@/pages/ChatAgent/utils/api';

/**
 * The page's own vocabulary, spelled out as locale keys, one entry per value.
 *
 * Written out rather than built from the value so a term added to the ledger
 * is a compile error here, and so the locale sweep can see all of them: a
 * template-literal key is invisible to it, and these are the words on a row
 * about money that already moved. Only the words no other surface draws are
 * here. The words an order is made of (side, type, duration, session, asset
 * class), the status words and the mode words are shared with the chat card
 * in `@/components/orders`.
 */

export const ORDER_STATUS_GROUP_KEY: Record<OrderStatusGroup, string> = {
  awaiting: 'orders.statusGroup.awaiting',
  open: 'orders.statusGroup.open',
  filled: 'orders.statusGroup.filled',
  cancelled: 'orders.statusGroup.cancelled',
  rejected: 'orders.statusGroup.rejected',
  failed: 'orders.statusGroup.failed',
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
