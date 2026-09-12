import { instrumentLabel } from '@/components/orders/instrument';
import { ORDER_STATUS_KEY, ORDER_STATUS_STAGED_KEYS } from '@/components/orders/status';
import { readValuesHidden } from '@/components/orders/valuesHidden';
import { useOrder } from '@/hooks/useOrders';
import { maskAccountId } from '@/pages/ChatAgent/utils/directTools';
import { fillFigure, isOrderOpen, type OrderAttempt } from '@/pages/ChatAgent/utils/api';
import type {
  OrderAction,
  OrderMode,
  OrderOutcome,
  OrderProposal,
  OrderReceipt,
  OrderStatus,
} from '@/types/sse';
import { ACCOUNT_KEY, orderSummaryRows, type OrderSummaryRow } from './orderSummary';
import { useDirectToolVendorLabel } from './useDirectToolVendor';

/**
 * One order's receipt, resolved: the artifact the tool answered with, the
 * ledger row it has moved to since, and the fields worth drawing from both.
 *
 * The artifact was written the moment the tool answered, so a fill, a cancel
 * or a broker rejection that reconciliation records afterwards never reaches
 * the thread on its own. The card asks the ledger for its row and lays the
 * fields the ledger owns over the artifact; everything else stays the
 * artifact's, because the artifact is the record of what was actually sent and
 * the row is a summary of it.
 */

const OUTCOME_LABEL = {
  orderId: 'toolArtifact.directTool.orderOutcome.orderId',
  filled: 'toolArtifact.directTool.orderOutcome.filled',
  avgFillPrice: 'toolArtifact.directTool.orderOutcome.avgFillPrice',
  fees: 'toolArtifact.directTool.orderOutcome.fees',
  failure: 'toolArtifact.directTool.orderOutcome.failure',
  decisionMessage: 'toolArtifact.directTool.orderOutcome.decisionMessage',
  /** The label over a fill written as a sentence rather than as a figure. */
  fill: 'toolArtifact.directTool.orderOutcome.fill',
  filledOf: 'toolArtifact.directTool.orderOutcome.filledOf',
  filledAt: 'toolArtifact.directTool.orderOutcome.filledAt',
};

/** What the order's own note row is labelled. Reached here only to tell it
 *  apart from the reason a person rejected with, when an order carries both. */
const NOTE_LABEL = 'toolArtifact.directTool.orderField.note';

/** Every locale key this card can ask for. The status verdicts and the outcome
 *  labels reach `t()` through a map, so the tree-wide sweep cannot see them;
 *  this is what one test holds against both catalogs. */
export const ORDER_RECEIPT_KEYS: readonly string[] = [
  ...Object.values(ORDER_STATUS_KEY),
  ...ORDER_STATUS_STAGED_KEYS,
  ...Object.values(OUTCOME_LABEL),
  NOTE_LABEL,
];

/** What a shoulder should not read: sizes, prices and fees. Not the side, the
 *  instrument or the verdict, which are the point of the card. */
const VALUE_FIELDS = new Set([
  'qty',
  'notional',
  'limit_price',
  'stop_price',
  'filled_qty',
  'avg_fill_price',
  'fees',
]);

const HIDDEN = '******';

function hide(rows: OrderSummaryRow[], hidden: boolean): OrderSummaryRow[] {
  if (!hidden) return rows;
  return rows.map((row) =>
    VALUE_FIELDS.has(row.field)
      // The sentence goes with the figures: "Filled 4 of 10" is the number the
      // toggle was asked to hide, spelled out.
      ? { ...row, value: HIDDEN, valueKey: undefined, valueParams: undefined }
      : row,
  );
}

function text(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === 'string') return value.trim() || null;
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : null;
  return null;
}

function failureText(outcome: OrderOutcome): string | null {
  const failure = outcome.failure;
  if (!failure) return null;
  const parts = [text(failure.code), text(failure.message) || text(failure.kind)];
  const joined = parts.filter(Boolean).join(' ');
  return joined || null;
}

/**
 * The rows the vendor's answer adds under the order: its id for the order, what
 * filled, what it cost, and why it did not. Each is drawn only when there is
 * one, for the same reason the order fields are.
 *
 * A fill is the one answer that is a fact about progress rather than a figure,
 * so where the order says how much was asked for it is written as a sentence:
 * "Filled 4 of 10" answers the question a partial fill actually raises, which
 * a bare 4 under "Filled quantity" does not. When the pieces for a sentence
 * are not all there, the figures go back to a row each.
 */
export function orderOutcomeRows(
  outcome: OrderOutcome,
  order?: OrderProposal | null,
): OrderSummaryRow[] {
  const rows: OrderSummaryRow[] = [];
  const orderId = text(outcome.vendor_order_id);
  if (orderId) {
    rows.push({ field: 'vendor_order_id', labelKey: OUTCOME_LABEL.orderId, value: orderId });
  }
  const filled = fillFigure(outcome.filled_qty);
  const avg = fillFigure(outcome.avg_fill_price);
  const qty = text(order?.qty);
  const partial =
    filled && qty && outcome.status === 'partially_filled'
      ? {
          field: 'filled_qty',
          labelKey: OUTCOME_LABEL.fill,
          value: `${filled} / ${qty}`,
          valueKey: OUTCOME_LABEL.filledOf,
          valueParams: { filled, qty },
        }
      : null;
  const whole =
    filled && avg && outcome.status === 'filled'
      ? {
          field: 'filled_qty',
          labelKey: OUTCOME_LABEL.fill,
          value: `${filled} @ ${avg}`,
          valueKey: OUTCOME_LABEL.filledAt,
          valueParams: { filled, price: avg },
        }
      : null;
  if (whole) {
    rows.push(whole);
  } else {
    if (partial) rows.push(partial);
    else if (filled) {
      rows.push({ field: 'filled_qty', labelKey: OUTCOME_LABEL.filled, value: filled });
    }
    if (avg) {
      rows.push({ field: 'avg_fill_price', labelKey: OUTCOME_LABEL.avgFillPrice, value: avg });
    }
  }
  const fees = text(outcome.fees?.amount);
  if (fees) {
    const currency = text(outcome.fees?.currency);
    rows.push({
      field: 'fees',
      labelKey: OUTCOME_LABEL.fees,
      value: currency ? `${fees} ${currency}` : fees,
    });
  }
  const failure = failureText(outcome);
  if (failure) {
    rows.push({ field: 'failure', labelKey: OUTCOME_LABEL.failure, value: failure });
  }
  return rows;
}

/**
 * The reason a person gave when they rejected, as the last field row.
 *
 * It sits with the order rather than with the outcome because it is the answer
 * to what the card asked, not something the brokerage said. An order that
 * already carries its own note gets the reason under a label of its own, so
 * two different sentences never appear as two rows both called Note.
 */
function decisionRow(outcome: OrderOutcome, orderRows: OrderSummaryRow[]): OrderSummaryRow[] {
  const message = text(outcome.decision_message);
  if (!message) return [];
  const collides = orderRows.some((row) => row.field === 'note');
  return [
    {
      field: 'decision_message',
      labelKey: collides ? OUTCOME_LABEL.decisionMessage : NOTE_LABEL,
      value: message,
    },
  ];
}

/**
 * The receipt on a tool result's artifact, or null when it carries none.
 *
 * The one place the wire fragment becomes a typed receipt. The status is the
 * field the card cannot do without, so an artifact that arrived without one
 * reads as `unknown` rather than as no receipt: the attempt is real either way
 * and saying nothing about an order is the worse answer.
 */
export function orderReceiptOf(artifact: unknown): OrderReceipt | null {
  const fragment = (artifact as { order_receipt?: unknown } | null | undefined)?.order_receipt;
  if (!fragment || typeof fragment !== 'object') return null;
  const receipt = fragment as OrderReceipt;
  const outcome = (receipt.outcome ?? {}) as OrderOutcome;
  const status = typeof outcome.status === 'string' ? outcome.status : 'unknown';
  return { ...receipt, outcome: { ...outcome, status: status as OrderStatus } };
}

/**
 * The outcome the ledger row reports, laid over the one the artifact froze.
 * Only the fields the ledger owns are taken.
 */
export function overlayLedgerRow(
  receipt: OrderReceipt | null,
  row: OrderAttempt | undefined,
): OrderReceipt | null {
  if (!receipt) return null;
  if (!row || row.attempt_id !== receipt.attempt_id) return receipt;
  const outcome = receipt.outcome;
  // An open row against a settled receipt is the older of the two, always: the
  // ledger's rank table lets nothing overtake a terminal state, so the row did
  // not regress, it was read before the receipt was. It reaches here when
  // another surface cached it and this receipt was served without refetching.
  // Nothing on it is worth laying over a final answer, so none of it is.
  if (!isOrderOpen(outcome.status) && isOrderOpen(row.status)) return receipt;
  return {
    ...receipt,
    outcome: {
      ...outcome,
      status: row.status,
      vendor_order_id: row.vendor_order_id ?? outcome.vendor_order_id ?? null,
      filled_qty: row.filled_qty ?? outcome.filled_qty ?? null,
      avg_fill_price: row.avg_fill_price ?? outcome.avg_fill_price ?? null,
      fees: row.fees ?? outcome.fees ?? null,
      action_url: row.action_url ?? outcome.action_url ?? null,
      // The ledger's alone: it clears a lost answer's failure once the vendor
      // lists the order, and the artifact froze that failure before it did.
      failure: row.failure ?? null,
      completed_at: row.completed_at ?? outcome.completed_at ?? null,
    },
  };
}

/** What the card draws: no wire shapes left, no decisions left to make. */
export interface OrderReceiptView {
  attemptId: string;
  vendor: string;
  vendorLabel: string;
  action: OrderAction;
  mode: OrderMode | null;
  targetRef: string | null;
  status: OrderStatus;
  rows: OrderSummaryRow[];
  outcomeRows: OrderSummaryRow[];
  actionUrl: string | null;
}

export function useOrderReceipt(artifact: Record<string, unknown>): OrderReceiptView | null {
  const frozen = orderReceiptOf(artifact);
  const vendorLabel = useDirectToolVendorLabel(frozen?.vendor || '');
  // A public thread never gets here: the share route strips the receipt
  // fragment, `orderReceiptOf` then returns null, and the query stays disabled.
  // A receipt the artifact already froze at a terminal verdict still reads the
  // row once, because reconciliation may have named the instrument or retracted
  // a failure since, but it has no reason to ask a second time.
  const { data: row } = useOrder(frozen?.attempt_id ?? null, {
    poll: true,
    settled: !!frozen && !isOrderOpen(frozen.outcome.status),
  });
  const receipt = overlayLedgerRow(frozen, row);
  if (!receipt) return null;

  const order = receipt.order ?? null;
  const hidden = readValuesHidden();
  const account = receipt.account_ref ?? order?.account_ref ?? null;
  // The instrument is the one part of the order itself that moves: a vendor
  // addressed by an opaque contract id is all the adapter could name at the
  // time, and reconciliation reads the vendor's own listing later.
  const instrument =
    (row?.attempt_id === receipt.attempt_id ? instrumentLabel(row?.order?.instrument) : '') ||
    instrumentLabel(order?.instrument);
  // An action with no order to normalize (an exercise, say) still has an
  // account, and that is the row worth keeping.
  const orderRows = order
    ? orderSummaryRows(order, instrument)
    : account
      ? [{ field: 'account_ref', labelKey: ACCOUNT_KEY, value: maskAccountId(account) }]
      : [];

  return {
    attemptId: receipt.attempt_id,
    vendor: receipt.vendor || '',
    vendorLabel,
    action: receipt.action ?? order?.action ?? 'place',
    mode: receipt.mode ?? order?.mode ?? null,
    targetRef: order?.target_ref ?? null,
    status: receipt.outcome.status,
    rows: [...hide(orderRows, hidden), ...decisionRow(receipt.outcome, orderRows)],
    outcomeRows: hide(orderOutcomeRows(receipt.outcome, order), hidden),
    actionUrl: receipt.outcome.action_url ?? null,
  };
}
