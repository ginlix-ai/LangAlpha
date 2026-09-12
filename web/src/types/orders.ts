/**
 * The order domain, as the server writes it.
 *
 * One declaration per thing, shared by the two envelopes that carry an order:
 * the SSE interrupt and receipt (`types/sse`) and the REST attempt ledger
 * (`utils/api/orders`). Those two disagree about their outer shape and about
 * nothing else, which is what these types are for.
 *
 * Mirrors `src/server/services/brokerage_orders/models.py`. Decimals arrive as
 * strings: a price is not a float on the way to a broker.
 */

/** What an order tool does. Confirm and stage are separate acts, not a place
 *  with a flag: one sends an order the vendor is already holding, the other
 *  writes one into the account for the user to send themselves. */
export type OrderAction =
  | 'place'
  | 'replace'
  | 'cancel'
  | 'confirm'
  | 'stage'
  | 'unstage'
  | 'exercise'
  | 'cancel_exercise';

/** Which money an order tool moves. `staged` moves none and still writes into
 *  the real account, which is why it is its own answer and not a kind of live. */
export type OrderMode = 'live' | 'paper' | 'staged';

/**
 * Where an order attempt stands, from proposal to a settled end state. Mirrors
 * `AttemptStatus` on the server; every surface that draws one names all
 * fourteen, so a state added there without a verdict here is a compile error.
 */
export type OrderStatus =
  | 'proposed'
  | 'approved'
  | 'rejected_by_user'
  | 'refused'
  | 'submitting'
  | 'submitted'
  | 'pending_confirm'
  | 'working'
  | 'partially_filled'
  | 'filled'
  | 'cancelled'
  | 'rejected_by_vendor'
  | 'failed'
  | 'unknown';

export const ORDER_STATUSES: readonly OrderStatus[] = [
  'proposed',
  'approved',
  'rejected_by_user',
  'refused',
  'submitting',
  'submitted',
  'pending_confirm',
  'working',
  'partially_filled',
  'filled',
  'cancelled',
  'rejected_by_vendor',
  'failed',
  'unknown',
] as const;

export type OrderAssetClass =
  | 'equity'
  | 'option'
  | 'option_combo'
  | 'future'
  | 'crypto'
  | 'warrant'
  | 'other';

export const ORDER_ASSET_CLASSES: readonly OrderAssetClass[] = [
  'equity',
  'option',
  'option_combo',
  'future',
  'crypto',
  'warrant',
  'other',
] as const;

export type OrderSide = 'buy' | 'sell' | 'sell_short' | 'buy_to_cover';

/**
 * How long an order stands. The three beyond `day` and `gtc` are durations a
 * vendor sells as their own order kind rather than as a session flag on a day
 * order, which is why they are values here and not something `session` carries.
 */
export type OrderTimeInForce =
  | 'day'
  | 'gtc'
  | 'overnight'
  | 'overnight_next_day'
  | 'at_the_open';

export type OrderType =
  | 'market'
  | 'limit'
  | 'stop'
  | 'stop_limit'
  | 'market_if_touched'
  | 'limit_if_touched'
  | 'auction'
  | 'auction_limit';

export interface OrderMoney {
  amount?: string | null;
  currency?: string | null;
}

/** Why an attempt ended anywhere but at a fill. `kind` says whose fault it
 *  was (vendor, policy, transport); `code` is the vendor's own, when it sent one. */
export interface OrderFailure {
  kind: string;
  code?: string | null;
  message?: string | null;
}

export interface EquityInstrument {
  kind: 'equity';
  symbol: string;
  venue?: string | null;
}

export interface OptionInstrument {
  kind: 'option';
  underlying: string;
  expiration?: string | null;
  strike?: string | null;
  right?: 'C' | 'P' | null;
  multiplier?: number | null;
  vendor_instrument_id?: string | null;
}

/** Both carriers, because moomoo sends legs and Robinhood sends a name. */
export interface ComboInstrument {
  kind: 'combo';
  legs?: Record<string, unknown>[] | null;
  strategy?: string | null;
}

export interface FutureInstrument {
  kind: 'future';
  symbol: string;
  venue?: string | null;
  contract_month?: string | null;
}

export interface CryptoInstrument {
  kind: 'crypto';
  pair: string;
}

/** An instrument addressed only by a vendor string, such as an HK warrant. */
export interface OpaqueInstrument {
  kind: 'opaque';
  raw_code: string;
}

/**
 * What is being traded, told apart by `kind`. Each variant carries only the
 * fields its own asset class has: an equity has a symbol, crypto a pair, a
 * warrant only the vendor's raw code, an option nothing shorter than its parts.
 */
export type OrderInstrument =
  | EquityInstrument
  | OptionInstrument
  | ComboInstrument
  | FutureInstrument
  | CryptoInstrument
  | OpaqueInstrument;

/**
 * The normalized order: what a call would do at the broker, in vendor-neutral
 * terms. Everything past the instrument is optional because a cancel, a
 * replace and a confirm are orders too, and each knows only a slice of this.
 */
export interface OrderSummary {
  asset_class?: OrderAssetClass | null;
  /** The vendor order id, instruction id or confirm id a cancel, replace,
   *  confirm or unstage acts on, and the only thing identifying those rows. */
  target_ref?: string | null;
  instrument?: OrderInstrument | null;
  side?: OrderSide | null;
  qty?: string | null;
  notional?: OrderMoney | null;
  currency?: string | null;
  order_type?: OrderType | null;
  limit_price?: string | null;
  stop_price?: string | null;
  time_in_force?: OrderTimeInForce | null;
  session?: string | null;
  note?: string | null;
}
