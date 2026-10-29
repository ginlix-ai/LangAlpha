import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  getOrder,
  getOrders,
  isOrderOpen,
  ordersQueryParams,
  type OrderFilters,
  type OrderPage,
} from '@/pages/ChatAgent/utils/api';
import { useNoteOrderExists } from './useOrdersVisible';

const ORDERS_PAGE_SIZE = 50;

/**
 * The user's order attempts, newest first, paged on the server's keyset cursor.
 *
 * A filter is part of the key rather than a client-side predicate: the page is
 * a slice of a ledger that keeps growing, so filtering the loaded pages would
 * answer from whatever happened to be fetched.
 */
export function useOrders(filters: OrderFilters = {}) {
  return useInfiniteQuery({
    queryKey: queryKeys.orders.list(ordersQueryParams(filters)),
    queryFn: ({ pageParam }) => getOrders(filters, pageParam, ORDERS_PAGE_SIZE),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage: OrderPage) => lastPage.next_cursor ?? undefined,
    staleTime: 15_000,
    // A page of settled orders has nothing left to learn and makes no
    // requests; one holding an order the broker still has asks again at the
    // receipt's cadence, so the table moves without a reload.
    refetchInterval: (query) =>
      pagesHoldOpenOrder(query.state.data?.pages) ? ORDER_POLL_MS : false,
  });
}

/** How often a surface watching an order still at the broker asks again. Long
 *  enough that a thread full of receipts is quiet, short enough that a fill
 *  lands while the reader is still looking at the card. The ledger itself moves
 *  on the reconciler's minute, so asking faster would only repeat an answer. */
const ORDER_POLL_MS = 30_000;

export function pagesHoldOpenOrder(pages: OrderPage[] | undefined): boolean {
  return !!pages?.some((page) => page.items.some((row) => isOrderOpen(row.status)));
}

/**
 * A refusal this row will keep giving, however often it is asked. 404 is no
 * such attempt, 403 is somebody else's, and 410 is one that is gone. Anything
 * else is the backend or the link having a bad moment, which a later ask can
 * still survive, so the retry and the poll both read this rather than each
 * deciding for themselves and disagreeing.
 */
export function isDefinitiveOrderError(error: unknown): boolean {
  const status = (error as { response?: { status?: number } } | null)?.response
    ?.status;
  return status === 404 || status === 403 || status === 410;
}

/**
 * One attempt, for the detail overlay. Fetched by id rather than read out of
 * the list: the overlay is deep-linkable, so the row may not be on any page
 * that has loaded.
 *
 * `poll` is for a surface that outlives the fetch, such as a receipt sitting in
 * a thread while the order is still working. It stops on its own at a terminal
 * status, because the ledger cannot move that row again.
 *
 * `settled` says the caller already holds a terminal verdict. The row is still
 * worth one read, for the fields reconciliation may have filled in since, but
 * not worth asking for again.
 */
export function useOrder(
  attemptId: string | null,
  { poll = false, settled = false }: { poll?: boolean; settled?: boolean } = {},
) {
  const query = useQuery({
    queryKey: queryKeys.orders.detail(attemptId ?? ''),
    queryFn: () => getOrder(attemptId as string),
    enabled: !!attemptId,
    // Without the settled case, every receipt in a thread asks again on each
    // tab return, for rows the ledger can no longer move.
    staleTime: settled ? Infinity : 15_000,
    refetchOnWindowFocus: true,
    refetchInterval: poll
      ? (query) => {
          const row = query.state.data;
          // No row at all means the fetch failed, not that the order settled.
          // Reading it as settled is what stranded a card on its frozen status
          // for as long as the tab stayed focused.
          if (!row) {
            return isDefinitiveOrderError(query.state.error)
              ? false
              : ORDER_POLL_MS;
          }
          return isOrderOpen(row.status) ? ORDER_POLL_MS : false;
        }
      : false,
    retry: (failureCount, error) =>
      isDefinitiveOrderError(error) ? false : failureCount < 2,
  });
  useNoteOrderExists(!!query.data);
  return query;
}
