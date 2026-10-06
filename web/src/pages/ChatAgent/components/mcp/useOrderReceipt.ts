import { HIDDEN, orderAmount, orderCurrency } from '@/components/orders/format';
import { instrumentLabel } from '@/components/orders/instrument';
import { ORDER_STATUS_KEY, ORDER_STATUS_STAGED_KEYS } from '@/components/orders/status';
import { readValuesHidden } from '@/components/orders/valuesHidden';
import { useLocale } from '@/hooks/useLocale';
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
import { orderTicket, type OrderPart, type OrderTicketView } from './orderSummary';
import { useDirectToolVendorLabel } from './useDirectToolVendor';

/**
 * One order's receipt, resolved: the artifact the tool answered with, the
 * ledger row it has moved to since, and what is worth drawing from both.
 *
 * The artifact was written the moment the tool answered, so a fill, a cancel
 * or a broker rejection that reconciliation records afterwards never reaches
 * the thread on its own. The card asks the ledger for its row and lays the
 * fields the ledger owns over the artifact; everything else stays the
 * artifact's, because the artifact is the record of what was actually sent and
 * the row is a summary of it.
 */

const FILL_LABEL = {
  filled: 'toolArtifact.directTool.orderOutcome.filled',
  avgFillPrice: 'toolArtifact.directTool.orderOutcome.avgFillPrice',
  fees: 'toolArtifact.directTool.orderOutcome.fees',
  /** The label a fill sentence keeps when values are hidden and the sentence,
   *  which is all figures, goes. */
  fill: 'toolArtifact.directTool.orderOutcome.fill',
  filledOf: 'toolArtifact.directTool.orderOutcome.filledOf',
  filledAt: 'toolArtifact.directTool.orderOutcome.filledAt',
};

/** Every locale key this card can ask for through a map. The status verdicts
 *  and the fill labels reach `t()` through one, so the tree-wide sweep cannot
 *  see them; this is what one test holds against both catalogs. */
export const ORDER_RECEIPT_KEYS: readonly string[] = [
  ...Object.values(ORDER_STATUS_KEY),
  ...ORDER_STATUS_STAGED_KEYS,
  ...Object.values(FILL_LABEL),
];

function text(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === 'string') return value.trim() || null;
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : null;
  return null;
}

/** What the brokerage answered and what the person said, ready to draw. */
export interface OrderOutcomeView {
  /** The reason a person typed when they rejected. Always under its own label:
   *  it answers what the card asked, and it is not the order's note. */
  decisionMessage: string | null;
  /** The vendor's sentence (or, without one, the kind of failure) and its code. */
  failure: { message: string | null; code: string | null } | null;
  fill: OrderPart[];
  orderId: string | null;
}

/**
 * The part of the receipt that is the answer rather than the order: why it did
 * not go through, what filled and what it cost, and the vendor's id for it.
 * Each is drawn only when there is one, for the same reason the order's own
 * fields are.
 *
 * A fill is the one answer that is a fact about progress rather than a figure,
 * so where the order says how much was asked for it is written as a sentence:
 * "Filled 4 of 10" answers the question a partial fill actually raises, which
 * a bare 4 under "Filled quantity" does not. When the pieces for a sentence
 * are not all there, the figures go back to a part each.
 */
export function orderOutcomeView(
  outcome: OrderOutcome,
  order: OrderProposal | null | undefined,
  opts: { hidden: boolean; locale: string },
): OrderOutcomeView {
  const { hidden } = opts;
  const currency = order ? orderCurrency(order) : null;
  const filled = fillFigure(outcome.filled_qty);
  const avg = fillFigure(outcome.avg_fill_price);
  const qty = text(order?.qty);
  // The sentence goes with the figures when they are hidden: "Filled 4 of 10"
  // is the number the toggle was asked to hide, spelled out.
  const masked: OrderPart = { field: 'fill', labelKey: FILL_LABEL.fill, value: HIDDEN };
  const fill: OrderPart[] = [];
  if (filled && avg && outcome.status === 'filled') {
    const price = orderAmount(avg, currency, opts);
    fill.push(
      hidden
        ? masked
        : {
            field: 'fill',
            value: `${filled} @ ${price}`,
            valueKey: FILL_LABEL.filledAt,
            valueParams: { filled, price },
          },
    );
  } else {
    if (filled && qty && outcome.status === 'partially_filled') {
      fill.push(
        hidden
          ? masked
          : {
              field: 'fill',
              value: `${filled} / ${qty}`,
              valueKey: FILL_LABEL.filledOf,
              valueParams: { filled, qty },
            },
      );
    } else if (filled) {
      fill.push({ field: 'filled_qty', labelKey: FILL_LABEL.filled, value: hidden ? HIDDEN : filled });
    }
    if (avg) {
      fill.push({
        field: 'avg_fill_price',
        labelKey: FILL_LABEL.avgFillPrice,
        value: orderAmount(avg, currency, opts),
      });
    }
  }
  // In the currency the vendor charged it in, which is not always the order's.
  const fees = orderAmount(text(outcome.fees?.amount), text(outcome.fees?.currency), opts);
  if (fees) fill.push({ field: 'fees', labelKey: FILL_LABEL.fees, value: fees });

  const message = text(outcome.failure?.message) ?? text(outcome.failure?.kind);
  const code = text(outcome.failure?.code);
  const orderId = text(outcome.vendor_order_id);
  return {
    decisionMessage: text(outcome.decision_message),
    failure: message || code ? { message, code } : null,
    fill,
    // A cancel answers with the id of the order it cancelled, which the ticket
    // already names; saying it a second time reads as a second order.
    orderId: orderId && orderId !== text(order?.target_ref) ? orderId : null,
  };
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
  /** Already masked. */
  account: string | null;
  status: OrderStatus;
  ticket: OrderTicketView | null;
  outcome: OrderOutcomeView;
  actionUrl: string | null;
}

export function useOrderReceipt(artifact: Record<string, unknown>): OrderReceiptView | null {
  const frozen = orderReceiptOf(artifact);
  const vendorLabel = useDirectToolVendorLabel(frozen?.vendor || '');
  const locale = useLocale();
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
  const shown = { hidden: readValuesHidden(), locale };
  const account = text(receipt.account_ref) ?? text(order?.account_ref);
  // The instrument is the one part of the order itself that moves: a vendor
  // addressed by an opaque contract id is all the adapter could name at the
  // time, and reconciliation reads the vendor's own listing later.
  const reconciled = row?.attempt_id === receipt.attempt_id ? row.order?.instrument : null;
  const instrument = instrumentLabel(reconciled) ? reconciled : order?.instrument;

  return {
    attemptId: receipt.attempt_id,
    vendor: receipt.vendor || '',
    vendorLabel,
    action: receipt.action ?? order?.action ?? 'place',
    mode: receipt.mode ?? order?.mode ?? null,
    account: account && maskAccountId(account),
    status: receipt.outcome.status,
    ticket: order ? orderTicket({ ...order, instrument }, { ...shown, fill: receipt.outcome }) : null,
    outcome: orderOutcomeView(receipt.outcome, order, shown),
    actionUrl: receipt.outcome.action_url ?? null,
  };
}
