import type { OrderMode } from '@/types/orders';

/**
 * A word per kind of order, written out rather than built from the mode, so the
 * tree-wide locale sweep can see every key and a mode added later cannot ship
 * as a raw key on a card about to spend real money.
 *
 * One map, because the Plugins row where the gate is configured, the chat card
 * where a stopped order is answered and the Orders filter all have to name a
 * mode the same way, or the switch a user set reads as being about something
 * else than the order it produced.
 */
export const ORDER_MODE_KEY: Record<OrderMode, string> = {
  live: 'plugins.detail.orderModeLive',
  paper: 'plugins.detail.orderModePaper',
  staged: 'plugins.detail.orderModeStaged',
};

export const ORDER_MODES: readonly OrderMode[] = ['live', 'paper', 'staged'] as const;
