import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useMemo } from 'react';
import { queryKeys } from '@/lib/queryKeys';
import type { OrderPage } from '@/pages/ChatAgent/utils/api';
import { useHasConnectedBrokerage } from './useConnectedBrokerage';

/**
 * Whether the user has any order attempt at all, connected broker or not.
 *
 * One row is enough to answer, so this asks for one. A ledger row outlives
 * the connection that produced it, which is why this is its own question and
 * not a corollary of "is a broker connected".
 */
export function useHasAnyOrder(): boolean | undefined {
  const { data } = useQuery({
    queryKey: queryKeys.orders.any(),
    // Imported when the query runs, for the reason useHasConnectedBrokerage
    // gives: this hook loads with the app shell and the orders client need not.
    queryFn: () =>
      import('@/pages/ChatAgent/utils/api/orders').then((api) => api.getOrders({}, undefined, 1)),
    staleTime: 60_000,
  });
  return data ? data.items.length > 0 : undefined;
}

/**
 * Re-asks `useHasAnyOrder` once a surface holds proof of a ledger row: an
 * attempt it fetched, or an approval card keyed to one. The nav asks that once
 * and nothing else re-asks it, so a first order this session would leave "none"
 * cached, and disconnecting the broker would then hide Orders.
 */
export function useNoteOrderExists(known: boolean): void {
  const queryClient = useQueryClient();
  useEffect(() => {
    if (!known) return;
    // Every loaded list is now short a row, whatever the nav's own answer
    // holds, and `any` is not a prefix of `list`: they part at the second
    // segment. A list of settled rows polls at no cadence, so nothing else
    // would ask again while the page stays mounted.
    void queryClient.invalidateQueries({ queryKey: queryKeys.orders.lists() });
    const any = queryClient.getQueryData<OrderPage>(queryKeys.orders.any());
    if (any?.items.length === 0) {
      void queryClient.invalidateQueries({ queryKey: queryKeys.orders.any() });
    }
  }, [known, queryClient]);
}

/**
 * Whether the Orders page is worth offering: a broker is connected, or the
 * ledger already holds something to show. `undefined` only while neither half
 * has answered yes and at least one is still in flight, so a settled yes on
 * either side shows the entry without waiting on the other.
 */
export function useOrdersVisible(): boolean | undefined {
  const hasBrokerage = useHasConnectedBrokerage();
  const hasOrder = useHasAnyOrder();
  return useMemo(() => {
    if (hasBrokerage === true || hasOrder === true) return true;
    if (hasBrokerage === false && hasOrder === false) return false;
    return undefined;
  }, [hasBrokerage, hasOrder]);
}
