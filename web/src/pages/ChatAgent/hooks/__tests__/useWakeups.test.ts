/**
 * The thread's wakeup list as a query: read while the chat is in front, held
 * while it is not, settled by a cancel's answer, re-read on an invalidation,
 * and lapsing in place.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { Wakeup } from '@/types/wakeups';

let flag = true;
let watchFlag = true;
vi.mock('@/hooks/useFeatures', () => ({
  useFeatureEnabled: (key: string) => (key === 'market_watch' ? watchFlag : flag),
}));
vi.mock('../../utils/api', () => ({ fetchWakeups: vi.fn(), cancelWakeups: vi.fn() }));

import { cancelWakeups, fetchWakeups } from '../../utils/api';
import { invalidateThreadWakeups, useCancelWakeups, useThreadWakeups } from '../useWakeups';

const mockFetch = fetchWakeups as Mock;
const mockCancel = cancelWakeups as Mock;

const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';

// Neutral placeholder ticker and a made-up level only.
function wakeup(over: Partial<Wakeup>): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: '2099-01-02T00:00:00Z', fired_at: null,
    price: { symbol: 'NVDA', kind: 'above', value: 205, pct_from: null, armed_price: null, fired_price: null },
    order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
    ...over,
  };
}

const ARMED = wakeup({});
const answer = (wakeups: Wakeup[]) => ({ thread_id: THREAD, user_timezone: 'UTC', wakeups });

/** A client that keeps an entry no hook observes, as the app's does. */
function keepingClient(): QueryClient {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

describe('useThreadWakeups', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    flag = true;
    watchFlag = true;
  });

  it('reads the list and the user zone from the GET endpoint', async () => {
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD));
    await waitFor(() => expect(result.current).toEqual({ items: [ARMED], userTimezone: 'UTC' }));
    expect(mockFetch).toHaveBeenCalledWith(THREAD);
  });

  it('keeps showing the legacy price targets with wakeups off', async () => {
    flag = false;
    const legacy = wakeup({ wakeup_id: 'legacy-1', short_id: null, legacy: true });
    mockFetch.mockResolvedValue(answer([ARMED, legacy]));
    const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD));
    await waitFor(() => expect(result.current?.items).toEqual([legacy]));
  });

  it('shows nothing with wakeups off once no legacy target is left', async () => {
    flag = false;
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD));
    await waitFor(() => expect(mockFetch).toHaveBeenCalled());
    await act(async () => {});
    expect(result.current).toBeNull();
  });

  it('reads nothing while both features are off', async () => {
    flag = false;
    watchFlag = false;
    const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD));
    await act(async () => {});
    expect(mockFetch).not.toHaveBeenCalled();
    expect(result.current).toBeNull();
  });

  it('reads nothing for an id that is not a thread yet', async () => {
    const { result } = renderHookWithProviders(() => useThreadWakeups('__default__'));
    await act(async () => {});
    expect(mockFetch).not.toHaveBeenCalled();
    expect(result.current).toBeNull();
  });

  // A chat kept warm behind another one: the Status tab's follow compares
  // what it saw before against what it sees on return, so the list holds.
  it('holds what it read while inactive, without reading', async () => {
    const queryClient = keepingClient();
    queryClient.setQueryData(queryKeys.threads.wakeups(THREAD), answer([ARMED]));
    const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD, false), { queryClient });
    await act(async () => {});
    expect(mockFetch).not.toHaveBeenCalled();
    expect(result.current?.items).toEqual([ARMED]);
  });

  it('reads again on an invalidation', async () => {
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result, queryClient } = renderHookWithProviders(() => useThreadWakeups(THREAD));
    await waitFor(() => expect(result.current?.items).toEqual([ARMED]));
    const fired = { ...ARMED, state: 'fired' as const, cancellable: false };
    mockFetch.mockResolvedValue(answer([fired]));
    act(() => invalidateThreadWakeups(queryClient, THREAD));
    await waitFor(() => expect(result.current?.items).toEqual([fired]));
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  // The server settles it on its next sweep, and nothing re-reads the list
  // between turns, so the view must not go on offering a Cancel for it.
  it('reads an armed price wakeup lapsed once its window passes, with no re-read', async () => {
    vi.useFakeTimers({ now: Date.parse('2030-01-01T23:59:00Z'), toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    try {
      const soon = wakeup({ expires_at: '2030-01-02T00:00:00Z' });
      mockFetch.mockResolvedValue(answer([soon]));
      const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD));
      // The cache tells its readers on a timer, which is faked here.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(0);
      });
      expect(result.current?.items[0].state).toBe('armed');
      await act(async () => {
        vi.advanceTimersByTime(60_000);
      });
      expect(result.current?.items[0]).toMatchObject({ state: 'lapsed', cancellable: false });
      expect(mockFetch).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  // A fire held for usage starts no turn, so no event tells the view.
  it('reads again after a due fire settles, twice at most', async () => {
    vi.useFakeTimers({ now: Date.parse('2030-01-01T13:59:00Z'), toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    try {
      const due = wakeup({ kind: 'at', price: null, expires_at: null, next_fire_at: '2030-01-01T14:00:00Z' });
      mockFetch.mockResolvedValue(answer([due]));
      const { result } = renderHookWithProviders(() => useThreadWakeups(THREAD));
      await act(async () => {
        await vi.advanceTimersByTimeAsync(0);
      });
      expect(mockFetch).toHaveBeenCalledTimes(1);
      // Due at 14:00, read at 14:00:45 and, still armed then, at 14:03.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(105_000);
      });
      expect(mockFetch).toHaveBeenCalledTimes(2);
      mockFetch.mockResolvedValue(answer([{ ...due, state: 'held_usage' }]));
      await act(async () => {
        await vi.advanceTimersByTimeAsync(136_000);
      });
      expect(mockFetch).toHaveBeenCalledTimes(3);
      expect(result.current?.items[0].state).toBe('held_usage');
      await act(async () => {
        await vi.advanceTimersByTimeAsync(3_600_000);
      });
      expect(mockFetch).toHaveBeenCalledTimes(3);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('useCancelWakeups', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    flag = true;
    watchFlag = true;
  });

  function renderBoth() {
    return renderHookWithProviders(() => ({ list: useThreadWakeups(THREAD), cancel: useCancelWakeups(THREAD) }));
  }

  it('shows the listing a cancel answers with, keeping the user zone, and returns its outcomes', async () => {
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result } = renderBoth();
    await waitFor(() => expect(result.current.list?.items).toEqual([ARMED]));

    const cancelled = { ...ARMED, state: 'cancelled' as const, cancellable: false };
    const results = [{ wakeup_id: 'w-1', outcome: 'cancelled' }];
    mockCancel.mockResolvedValue({ results, wakeups: [cancelled] });
    let outcome: unknown;
    await act(async () => {
      outcome = await result.current.cancel.mutateAsync(['w-1']);
    });
    expect(mockCancel).toHaveBeenCalledWith(THREAD, ['w-1']);
    expect(outcome).toEqual({ results, wakeups: [cancelled] });
    await waitFor(() => expect(result.current.list).toEqual({ items: [cancelled], userTimezone: 'UTC' }));
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('drops a read in flight when the cancel lands, so it cannot show the wakeup armed again', async () => {
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result, queryClient } = renderBoth();
    await waitFor(() => expect(result.current.list?.items).toEqual([ARMED]));

    let answerRead: (v: unknown) => void = () => {};
    mockFetch.mockImplementation(() => new Promise((r) => { answerRead = r; }));
    act(() => invalidateThreadWakeups(queryClient, THREAD));
    await waitFor(() => expect(mockFetch).toHaveBeenCalledTimes(2));

    const cancelled = { ...ARMED, state: 'cancelled' as const, cancellable: false };
    mockCancel.mockResolvedValue({ results: [{ wakeup_id: 'w-1', outcome: 'cancelled' }], wakeups: [cancelled] });
    await act(async () => {
      await result.current.cancel.mutateAsync(['w-1']);
    });
    await waitFor(() => expect(result.current.list?.items).toEqual([cancelled]));
    await act(async () => {
      answerRead(answer([ARMED]));
    });
    // Long enough for a late answer to have reached the cache had it been kept.
    await new Promise((r) => setTimeout(r, 20));
    expect(result.current.list?.items).toEqual([cancelled]);
  });

  it('reads the listing again when a cancel answers without one', async () => {
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result } = renderBoth();
    await waitFor(() => expect(result.current.list?.items).toEqual([ARMED]));

    const cancelled = { ...ARMED, state: 'cancelled' as const, cancellable: false };
    mockCancel.mockResolvedValue({ results: [{ wakeup_id: 'w-1', outcome: 'cancelled' }], wakeups: null });
    mockFetch.mockResolvedValue(answer([cancelled]));
    await act(async () => {
      await result.current.cancel.mutateAsync(['w-1']);
    });
    expect(mockFetch).toHaveBeenCalledTimes(2);
    await waitFor(() => expect(result.current.list?.items).toEqual([cancelled]));
  });

  it('rejects when the cancel request fails, leaving the list as it was', async () => {
    mockFetch.mockResolvedValue(answer([ARMED]));
    const { result } = renderBoth();
    await waitFor(() => expect(result.current.list?.items).toEqual([ARMED]));
    mockCancel.mockRejectedValue(new Error('Network Error'));
    await act(async () => {
      await expect(result.current.cancel.mutateAsync('all')).rejects.toThrow('Network Error');
    });
    expect(result.current.list?.items).toEqual([ARMED]);
  });
});
