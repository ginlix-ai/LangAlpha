export type WakeupKind = 'at' | 'cron' | 'price' | 'order';
export type WakeupState =
  | 'armed' | 'held' | 'held_usage' | 'fired' | 'finished' | 'lapsed' | 'expired' | 'cancelled';
export type OrderOutcome =
  | 'filled'
  | 'partial'
  | 'partial_cancelled'
  | 'cancelled'
  | 'rejected'
  | 'failed'
  | 'partial_expired'
  | 'expired'
  | 'confirmed'
  | 'handed_off';

export interface WakeupPrice {
  symbol: string;
  kind: 'above' | 'below' | 'pct_up' | 'pct_down';
  value: number;
  pct_from: 'previous_close' | 'now' | null;
  armed_price: number | null;
  fired_price: number | null;
  expires_at?: string | null;
}

export interface WakeupOrder {
  vendor_order_id: string;
  summary: string;
  outcome?: OrderOutcome | null;
  filled_qty?: number | null;
  qty?: number | null;
  avg_fill_price?: number | null;
  currency?: string | null;
  reason?: string | null;
}

/** One wakeup of a thread, as the listing reads it. A legacy row is a price
 *  target set before wakeups: `wakeup_id` is its target id and `short_id` null. */
export interface Wakeup {
  wakeup_id: string;
  short_id: string | null;
  label: string | null;
  kind: WakeupKind;
  state: WakeupState;
  timezone: string | null;
  cron: string | null;
  at: string | null;
  until: string | null;
  next_fire_at: string | null;
  fires_done: number;
  fires_total: number;
  ends_at: string | null;
  expires_at: string | null;
  fired_at: string | null;
  price: WakeupPrice | null;
  order: WakeupOrder | null;
  cancellable: boolean;
  legacy: boolean;
  created_at: string;
}

export interface WakeupsResponse {
  thread_id: string;
  user_timezone: string | null;
  wakeups: Wakeup[];
}

export type WakeupCancelOutcome = 'cancelled' | 'cancelled_in_flight' | 'too_late' | 'not_pending';

export interface WakeupCancelResponse {
  results: { wakeup_id: string; outcome: WakeupCancelOutcome }[];
  /** Null when the cancel committed but the listing after it could not be read. */
  wakeups: Wakeup[] | null;
}

/** One notice that fired, as a wake turn's metadata and a delivered event carry it. */
export interface WakeNotice {
  wakeup_id: string;
  short_id: string;
  label: string | null;
  kind: WakeupKind;
  notice: 'fire' | 'lapse';
  seq: number;
  total: number;
  last: boolean;
  skipped: number;
  fired_at: string;
  timezone?: string | null;
  cron?: string | null;
  price?: WakeupPrice | null;
  order?: WakeupOrder | null;
}

export interface WakeupMeta {
  v: 1;
  notices: WakeNotice[];
}

/** A thread's wakeups with the zone the user's own times read in. */
export interface WakeupList {
  items: Wakeup[];
  userTimezone: string | null;
}
