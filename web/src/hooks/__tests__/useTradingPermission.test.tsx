/**
 * What a trading-permission write rereads decides what the agreement dialog
 * shows next. A success is the server's own answer, so it is stored without
 * fetching the same row again; a failure rereads the level, because a 422 can
 * mean the server moved to an agreement this bundle does not show, and the
 * dialog only learns that from a fresh read.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { TradingPermission } from '@/types/api';

vi.mock('@/api/tradingPermission', () => ({
  getTradingPermission: vi.fn(),
  updateTradingPermission: vi.fn(),
}));

import { getTradingPermission, updateTradingPermission } from '@/api/tradingPermission';
import { useTradingPermission, useUpdateTradingPermission } from '../useTradingPermission';

const mockGet = vi.mocked(getTradingPermission);
const mockUpdate = vi.mocked(updateTradingPermission);

const row = (over: Partial<TradingPermission> = {}): TradingPermission => ({
  level: 'approve_each',
  agreement_version: 1,
  agreed_at: null,
  updated_at: null,
  ...over,
});

/** The catalog is seeded without an observer, so its invalidation shows as a
 *  flag rather than a fetch; it has to outlive that lack of observers. */
function client() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity },
      mutations: { retry: false },
    },
  });
  queryClient.setQueryData(queryKeys.mcp.catalog(), { servers: [] });
  return queryClient;
}

/** The read is mounted beside the write, so an invalidation of the level
 *  shows up as a second GET. */
function mount() {
  const queryClient = client();
  const hook = renderHookWithProviders(
    () => ({ read: useTradingPermission(), write: useUpdateTradingPermission() }),
    { queryClient },
  );
  return { ...hook, queryClient };
}

describe('useUpdateTradingPermission', () => {
  beforeEach(() => {
    mockGet.mockReset();
    mockUpdate.mockReset();
  });

  it('stores a success as answered, without rereading it, and marks the catalog stale', async () => {
    mockGet.mockResolvedValue(row());
    const saved = row({ level: 'plan_first', agreed_at: '2026-10-06T00:00:00Z' });
    mockUpdate.mockResolvedValue(saved);
    const { result, queryClient } = mount();
    await waitFor(() => expect(result.current.read.data).toEqual(row()));

    await act(async () => {
      await result.current.write.mutateAsync({ level: 'plan_first', agreement_version: 1 });
    });

    await waitFor(() => expect(result.current.read.data).toEqual(saved));
    // Long enough for a reread to have started and answered with the old row.
    await act(() => new Promise((resolve) => setTimeout(resolve, 20)));
    expect(result.current.read.data).toEqual(saved);
    expect(queryClient.getQueryState(queryKeys.user.tradingPermission())?.isInvalidated).toBe(false);
    expect(mockGet).toHaveBeenCalledTimes(1);
    expect(queryClient.getQueryState(queryKeys.mcp.catalog())?.isInvalidated).toBe(true);
  });

  // A focus refetch that left before the write and lands after it carries the
  // level from before, and would show a user who just opted in as still asking.
  it('keeps the saved level over a read that was already on its way', async () => {
    mockGet.mockResolvedValueOnce(row());
    const saved = row({ level: 'autonomous', agreed_at: '2026-10-06T00:00:00Z' });
    mockUpdate.mockResolvedValue(saved);
    const { result, queryClient } = mount();
    await waitFor(() => expect(result.current.read.data).toEqual(row()));

    let answerStaleRead: (value: TradingPermission) => void = () => {};
    mockGet.mockImplementationOnce(
      () => new Promise<TradingPermission>((resolve) => (answerStaleRead = resolve)),
    );
    void queryClient.refetchQueries({ queryKey: queryKeys.user.tradingPermission() });
    await act(async () => {
      await result.current.write.mutateAsync({ level: 'autonomous', agreement_version: 1 });
    });
    await act(async () => answerStaleRead(row()));

    // Long enough for the stale answer to have been stored, had it counted.
    await act(() => new Promise((resolve) => setTimeout(resolve, 20)));
    expect(result.current.read.data).toEqual(saved);
  });

  it('rereads the level after a refusal, so a newer agreement version reaches the page', async () => {
    mockGet.mockResolvedValueOnce(row()).mockResolvedValue(row({ agreement_version: 2 }));
    mockUpdate.mockRejectedValue(new Error('Request failed with status code 422'));
    const { result, queryClient } = mount();
    await waitFor(() => expect(result.current.read.data).toEqual(row()));

    await act(async () => {
      await result.current.write
        .mutateAsync({ level: 'plan_first', agreement_version: 1 })
        .catch(() => {});
    });

    await waitFor(() => expect(result.current.read.data?.agreement_version).toBe(2));
    expect(mockGet).toHaveBeenCalledTimes(2);
    // Nothing optimistic to undo: the level is what the server last said.
    expect(result.current.read.data?.level).toBe('approve_each');
    // A write whose answer was lost may still have landed.
    expect(queryClient.getQueryState(queryKeys.mcp.catalog())?.isInvalidated).toBe(true);
  });
});
