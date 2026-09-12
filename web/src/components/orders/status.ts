import type { OrderMode, OrderStatus } from '@/types/orders';

/**
 * The status vocabulary. The ledger spells its values with underscores and the
 * catalog with camelCase, which is the whole reason this map exists; it is
 * written out rather than built so the locale sweep can see every key.
 */
export const ORDER_STATUS_KEY: Record<OrderStatus, string> = {
  proposed: 'toolArtifact.directTool.orderStatus.proposed',
  approved: 'toolArtifact.directTool.orderStatus.approved',
  rejected_by_user: 'toolArtifact.directTool.orderStatus.rejectedByUser',
  refused: 'toolArtifact.directTool.orderStatus.refused',
  submitting: 'toolArtifact.directTool.orderStatus.submitting',
  submitted: 'toolArtifact.directTool.orderStatus.submitted',
  pending_confirm: 'toolArtifact.directTool.orderStatus.pendingConfirm',
  working: 'toolArtifact.directTool.orderStatus.working',
  partially_filled: 'toolArtifact.directTool.orderStatus.partiallyFilled',
  filled: 'toolArtifact.directTool.orderStatus.filled',
  cancelled: 'toolArtifact.directTool.orderStatus.cancelled',
  rejected_by_vendor: 'toolArtifact.directTool.orderStatus.rejectedByVendor',
  failed: 'toolArtifact.directTool.orderStatus.failed',
  unknown: 'toolArtifact.directTool.orderStatus.unknown',
};

/**
 * The one-word verdict a row wears. The ledger is scanned, not read, so the
 * table shows this and keeps the full sentence for the tooltip and the drawer.
 */
export const ORDER_STATUS_SHORT_KEY: Record<OrderStatus, string> = {
  proposed: 'toolArtifact.directTool.orderStatusShort.proposed',
  approved: 'toolArtifact.directTool.orderStatusShort.approved',
  rejected_by_user: 'toolArtifact.directTool.orderStatusShort.rejectedByUser',
  refused: 'toolArtifact.directTool.orderStatusShort.refused',
  submitting: 'toolArtifact.directTool.orderStatusShort.submitting',
  submitted: 'toolArtifact.directTool.orderStatusShort.submitted',
  pending_confirm: 'toolArtifact.directTool.orderStatusShort.pendingConfirm',
  working: 'toolArtifact.directTool.orderStatusShort.working',
  partially_filled: 'toolArtifact.directTool.orderStatusShort.partiallyFilled',
  filled: 'toolArtifact.directTool.orderStatusShort.filled',
  cancelled: 'toolArtifact.directTool.orderStatusShort.cancelled',
  rejected_by_vendor: 'toolArtifact.directTool.orderStatusShort.rejectedByVendor',
  failed: 'toolArtifact.directTool.orderStatusShort.failed',
  unknown: 'toolArtifact.directTool.orderStatusShort.unknown',
};

const STAGED_STATUS_SHORT_KEY: Partial<Record<OrderStatus, string>> = {
  submitted: 'toolArtifact.directTool.orderStatusShort.submittedStaged',
};

/**
 * What a staged order that reached the vendor is actually waiting on.
 *
 * Nothing was sent to a market: the instruction sits in the broker's own
 * client until the user confirms it there, and expires in seven days if they
 * do not. "Sent to the brokerage" reads as an order working, which is the one
 * thing this state is not, so `submitted` gets its own sentence in staged mode
 * and every other mode keeps the word it had.
 */
const STAGED_STATUS_KEY: Partial<Record<OrderStatus, string>> = {
  submitted: 'toolArtifact.directTool.orderStatus.submittedStaged',
};

/** The same sentence for a surface that cannot name the broker. */
const STAGED_STATUS_KEY_UNNAMED: Partial<Record<OrderStatus, string>> = {
  submitted: 'toolArtifact.directTool.orderStatus.submittedStagedUnnamed',
};

/** Every staged sentence, so the tests that hold keys against both catalogs
 *  can see the ones a plain map lookup would never reach. */
export const ORDER_STATUS_STAGED_KEYS: readonly string[] = [
  ...Object.values(STAGED_STATUS_KEY),
  ...Object.values(STAGED_STATUS_KEY_UNNAMED),
  ...Object.values(STAGED_STATUS_SHORT_KEY),
];

/** The one-word verdict for a row. */
export function orderStatusShortLabel(status: OrderStatus, mode?: OrderMode | null): string {
  if (mode === 'staged' && STAGED_STATUS_SHORT_KEY[status]) {
    return STAGED_STATUS_SHORT_KEY[status] as string;
  }
  return ORDER_STATUS_SHORT_KEY[status] ?? ORDER_STATUS_SHORT_KEY.unknown;
}

/** What to call a status on a card, and what to interpolate into it. */
export function orderStatusLabel(
  status: OrderStatus,
  mode?: OrderMode | null,
  vendorLabel?: string | null,
): { key: string; params?: { vendor: string } } {
  if (mode === 'staged') {
    const vendor = vendorLabel?.trim();
    if (vendor && STAGED_STATUS_KEY[status]) {
      return { key: STAGED_STATUS_KEY[status] as string, params: { vendor } };
    }
    const unnamed = STAGED_STATUS_KEY_UNNAMED[status];
    if (unnamed) return { key: unnamed };
  }
  return { key: ORDER_STATUS_KEY[status] ?? ORDER_STATUS_KEY.unknown };
}
