import { describe, expect, it, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { OrderAttempt, OrderPage } from '@/pages/ChatAgent/utils/api';
import { renderHookWithProviders } from '@/test/utils';
import { useOrder } from '../useOrders';

const getOrder = vi.fn();

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getOrder: (...args: unknown[]) => getOrder(...args),
}));

const ROW = { attempt_id: 'a-1', status: 'filled' } as OrderAttempt;

/** A client holding the nav's "any order" answer from earlier in the session. */
function clientRemembering(rows: OrderAttempt[]) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  client.setQueryData<OrderPage>(queryKeys.orders.any(), { items: rows, next_cursor: null });
  return client;
}

beforeEach(() => {
  getOrder.mockReset();
  getOrder.mockResolvedValue(ROW);
});

describe('useOrder and the nav gate', () => {
  it('re-asks whether any order exists once it finds one the cache says is not there', async () => {
    const queryClient = clientRemembering([]);
    const { result } = renderHookWithProviders(() => useOrder('a-1'), { queryClient });

    await waitFor(() => expect(result.current.data).toEqual(ROW));
    await waitFor(() =>
      expect(queryClient.getQueryState(queryKeys.orders.any())?.isInvalidated).toBe(true),
    );
  });

  // `any` and `list` part at the second key segment, so invalidating the first
  // reaches none of the loaded pages. A page of settled rows polls at no
  // cadence, so nothing else would ask again while it stays mounted.
  it('marks every loaded order list stale, not only the nav answer', async () => {
    const queryClient = clientRemembering([ROW]);
    const listKey = queryKeys.orders.list({ status: 'filled' });
    queryClient.setQueryData<OrderPage>(listKey, { items: [], next_cursor: null });

    const { result } = renderHookWithProviders(() => useOrder('a-1'), { queryClient });

    await waitFor(() => expect(result.current.data).toEqual(ROW));
    await waitFor(() =>
      expect(queryClient.getQueryState(listKey)?.isInvalidated).toBe(true),
    );
    // The nav's own answer already holds a row, so that one is left alone.
    expect(queryClient.getQueryState(queryKeys.orders.any())?.isInvalidated).toBe(false);
  });

  it('leaves an answer that already has a row alone', async () => {
    const queryClient = clientRemembering([ROW]);
    const { result } = renderHookWithProviders(() => useOrder('a-1'), { queryClient });

    await waitFor(() => expect(result.current.data).toEqual(ROW));
    expect(queryClient.getQueryState(queryKeys.orders.any())?.isInvalidated).toBe(false);
  });
});
