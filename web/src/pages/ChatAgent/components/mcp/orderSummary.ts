import { instrumentLabel } from '@/components/orders/instrument';
import { maskAccountId } from '@/pages/ChatAgent/utils/directTools';
import type { OrderAction, OrderMoney, OrderProposal } from '@/types/sse';

/**
 * What a call would do at the broker, read off the server's normalized summary
 * rather than off the raw arguments and drawn as one line per field.
 *
 * This is the question a person actually has in front of a live order (which
 * account, which instrument, which side, how much), with the account masked
 * because it is the one field here worth nothing to the reader and everything
 * to anyone else looking at the screen. The arguments stay reachable elsewhere,
 * unmasked, because that is the exact frame the vendor will see.
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

function text(value: string | null | undefined): string | null {
  return typeof value === 'string' ? value.trim() || null : null;
}

/** An amount and the currency it is in, which the order may carry either on
 *  the money itself or beside it. */
function money(
  amount: OrderMoney | null | undefined,
  fallbackCurrency: string | null | undefined,
): string | null {
  const value = text(amount?.amount);
  if (!value) return null;
  const currency = text(amount?.currency) ?? text(fallbackCurrency);
  return currency ? `${value} ${currency}` : value;
}

export const ACCOUNT_KEY = 'toolArtifact.directTool.orderField.account';

/**
 * The fields drawn, in the order a person reads them: whose money, on what,
 * which way, how much, and only then how the order is priced and timed. Each
 * reads itself off the order, because the account is masked, the instrument is
 * a whole algorithm and money carries its own currency.
 */
const FIELDS: ReadonlyArray<{
  field: string;
  labelKey: string;
  read: (order: OrderProposal, instrument: string) => string | null;
}> = [
  {
    field: 'account_ref',
    labelKey: ACCOUNT_KEY,
    read: (o) => {
      const value = text(o.account_ref);
      return value && maskAccountId(value);
    },
  },
  {
    field: 'instrument',
    labelKey: 'toolArtifact.directTool.orderField.instrument',
    read: (_, instrument) => instrument || null,
  },
  {
    field: 'asset_class',
    labelKey: 'toolArtifact.directTool.orderField.assetClass',
    read: (o) => text(o.asset_class),
  },
  { field: 'side', labelKey: 'toolArtifact.directTool.orderField.side', read: (o) => text(o.side) },
  { field: 'qty', labelKey: 'toolArtifact.directTool.orderField.quantity', read: (o) => text(o.qty) },
  {
    field: 'notional',
    labelKey: 'toolArtifact.directTool.orderField.notional',
    read: (o) => money(o.notional, o.currency),
  },
  {
    field: 'order_type',
    labelKey: 'toolArtifact.directTool.orderField.orderType',
    read: (o) => text(o.order_type),
  },
  {
    field: 'limit_price',
    labelKey: 'toolArtifact.directTool.orderField.limitPrice',
    read: (o) => text(o.limit_price),
  },
  {
    field: 'stop_price',
    labelKey: 'toolArtifact.directTool.orderField.stopPrice',
    read: (o) => text(o.stop_price),
  },
  {
    field: 'time_in_force',
    labelKey: 'toolArtifact.directTool.orderField.timeInForce',
    read: (o) => text(o.time_in_force),
  },
  {
    field: 'session',
    labelKey: 'toolArtifact.directTool.orderField.session',
    read: (o) => text(o.session),
  },
  { field: 'note', labelKey: 'toolArtifact.directTool.orderField.note', read: (o) => text(o.note) },
];

/** Every locale key these rows can ask for, so one test can hold them all
 *  against both catalogs. */
export const ORDER_SUMMARY_KEYS: readonly string[] = [
  ...Object.values(ORDER_ACTION_KEY),
  ...FIELDS.map((f) => f.labelKey),
];

export interface OrderSummaryRow {
  field: string;
  labelKey: string;
  value: string;
  /** For a value that is a sentence rather than a figure ("Filled 4 of 10"):
   *  the locale key and what to put in it, interpolated where the row is
   *  drawn. `value` stays the plain reading, and anything that rewrites the
   *  value has to clear these two with it. */
  valueKey?: string;
  valueParams?: Record<string, string>;
}

/**
 * The rows to draw for one order. A field the adapter could not fill is not
 * drawn at all rather than drawn empty: on this surface a blank price reads as
 * a market order and a placeholder dash reads as zero.
 *
 * `instrument` is passed in by a caller that knows a better name for the same
 * instrument than the order itself carries, which is what a receipt following
 * the ledger has once reconciliation reads the vendor's own listing.
 */
export function orderSummaryRows(
  order: OrderProposal,
  instrument?: string | null,
): OrderSummaryRow[] {
  const named = instrument || instrumentLabel(order.instrument);
  const rows: OrderSummaryRow[] = [];
  for (const { field, labelKey, read } of FIELDS) {
    const value = read(order, named);
    if (value) rows.push({ field, labelKey, value });
  }
  return rows;
}
