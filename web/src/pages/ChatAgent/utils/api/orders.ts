import { api } from '@/api/client';
import type {
  OrderAction,
  OrderAssetClass,
  OrderFailure,
  OrderMode,
  OrderMoney,
  OrderStatus,
  OrderSummary,
} from '@/types/orders';

/**
 * The order attempt ledger, read-only. One row is the authorization state of
 * one order call: what the model asked for, what the user decided, and what
 * the broker answered.
 *
 * The order vocabulary itself lives in `@/types/orders`, shared with the SSE
 * interrupt and receipt: the row a person reads on the Orders page and the
 * card that stopped the call are the same order seen twice.
 */

export type {
  OrderAction,
  OrderAssetClass,
  OrderFailure,
  OrderInstrument,
  OrderMoney,
  OrderSide,
  OrderStatus,
  OrderSummary,
  OrderTimeInForce,
  OrderType,
} from '@/types/orders';
export { ORDER_ASSET_CLASSES, ORDER_STATUSES } from '@/types/orders';

/**
 * The status filter's vocabulary: six ways an order can stand, each covering
 * the ledger statuses a person means by it. The row still says which of them
 * an order is in; the filter asks the coarser question.
 */
export type OrderStatusGroup =
  | 'awaiting'
  | 'open'
  | 'filled'
  | 'cancelled'
  | 'rejected'
  | 'failed';

export const ORDER_STATUS_GROUPS: readonly OrderStatusGroup[] = [
  'awaiting',
  'open',
  'filled',
  'cancelled',
  'rejected',
  'failed',
] as const;

export const ORDER_STATUS_GROUP_STATUSES: Record<OrderStatusGroup, readonly OrderStatus[]> = {
  awaiting: ['proposed'],
  open: [
    'approved',
    'submitting',
    'submitted',
    'pending_confirm',
    'working',
    'partially_filled',
    // The server still reconciles an unknown row against the vendor's list,
    // so a surface watching one keeps polling until it settles.
    'unknown',
  ],
  filled: ['filled'],
  cancelled: ['cancelled'],
  rejected: ['rejected_by_user', 'refused', 'rejected_by_vendor'],
  failed: ['failed'],
};

export function isOrderStatusGroup(value: string | null | undefined): value is OrderStatusGroup {
  return !!value && (ORDER_STATUS_GROUPS as readonly string[]).includes(value);
}

/**
 * Whether the ledger can still move this row: the broker is holding the order,
 * or nobody has answered the proposal yet. Everything else is where the order
 * ended, so a surface watching one has nothing left to ask for.
 */
export function isOrderOpen(status: OrderStatus | null | undefined): boolean {
  if (!status) return false;
  return (
    ORDER_STATUS_GROUP_STATUSES.open.includes(status) ||
    ORDER_STATUS_GROUP_STATUSES.awaiting.includes(status)
  );
}

/**
 * A fill figure the vendor reports, or null when it is reporting none.
 *
 * An order still working comes back with a filled quantity of 0 and an average
 * price of 0, which is the vendor saying nothing has happened yet rather than
 * an answer: "Average fill price 0" is not a price, and the status already
 * says the order is working. So a zero here reads as absent, the same way a
 * missing field does.
 */
export function fillFigure(value: string | number | null | undefined): string | null {
  if (value == null) return null;
  const written = String(value).trim();
  if (!written) return null;
  const parsed = Number(written);
  return Number.isFinite(parsed) && parsed === 0 ? null : written;
}

export interface OrderAttempt {
  attempt_id: string;
  thread_id: string | null;
  workspace_id: string | null;
  conversation_response_id: string | null;
  vendor: string;
  server: string | null;
  tool: string | null;
  action: OrderAction | null;
  mode: OrderMode | null;
  account_ref: string | null;
  asset_class: OrderAssetClass | null;
  order: OrderSummary | null;
  status: OrderStatus;
  approval_required: boolean;
  decided_at: string | null;
  decision_message: string | null;
  executed_at: string | null;
  completed_at: string | null;
  vendor_order_id: string | null;
  /** What the vendor has filled, as reconciliation last read it back. Absent
   *  until the broker reports a fill, and a decimal string for the same reason
   *  every price here is one. */
  filled_qty: string | null;
  avg_fill_price: string | null;
  fees: OrderMoney | null;
  /** The vendor deep link that finishes a staged instruction, when there is
   *  one: the order does not exist until the user opens it there. */
  action_url: string | null;
  route: Record<string, string> | null;
  failure: OrderFailure | null;
  parent_attempt_id: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface OrderPage {
  items: OrderAttempt[];
  next_cursor: string | null;
}

export interface OrderFilters {
  vendor?: string | null;
  mode?: OrderMode | null;
  status?: OrderStatusGroup | null;
  asset_class?: OrderAssetClass | null;
}

/**
 * The filters as the server reads them. An unset facet is left off entirely
 * rather than sent empty, so it is also what the list's cache key is keyed on:
 * two filter sets that ask the same question share one cached list.
 */
export function ordersQueryParams(
  filters: OrderFilters,
): Record<string, string | string[]> {
  const params: Record<string, string | string[]> = {};
  if (filters.vendor) params.vendor = filters.vendor;
  if (filters.mode) params.mode = filters.mode;
  if (filters.status) params.status = [...ORDER_STATUS_GROUP_STATUSES[filters.status]];
  if (filters.asset_class) params.asset_class = filters.asset_class;
  return params;
}

/** The server reads a repeated `status`, which axios would bracket; spell it out. */
function ordersSearchParams(params: Record<string, string | string[] | number>): URLSearchParams {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) value.forEach((v) => search.append(key, v));
    else search.append(key, String(value));
  }
  return search;
}

export async function getOrders(
  filters: OrderFilters = {},
  cursor?: string | null,
  limit = 50,
): Promise<OrderPage> {
  const params: Record<string, string | string[] | number> = {
    ...ordersQueryParams(filters),
    limit,
  };
  if (cursor) params.cursor = cursor;
  const { data } = await api.get<OrderPage>('/api/v1/orders', {
    params: ordersSearchParams(params),
  });
  return { items: data.items ?? [], next_cursor: data.next_cursor ?? null };
}

export async function getOrder(attemptId: string): Promise<OrderAttempt> {
  const { data } = await api.get<OrderAttempt>(
    `/api/v1/orders/${encodeURIComponent(attemptId)}`,
  );
  return data;
}
