import { describe, it, expect, vi, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';
import { act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '../../../../../test/utils';
import { queryKeys } from '@/lib/queryKeys';

const prefsState: { current: { other_preference?: Record<string, unknown> } | null } = {
  current: { other_preference: { theme: 'dark' } },
};
const loadingState = { isLoading: false };
const mockMutate = vi.fn();
const mockToast = vi.fn();
const mutationState = { isPending: false };

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: prefsState.current, isLoading: loadingState.isLoading }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: mockMutate, isPending: mutationState.isPending }),
}));
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: mockToast, dismiss: vi.fn(), toasts: [] }),
}));

import { useDashboardPrefs } from '../useDashboardPrefs';

/**
 * Build a queryClient pre-seeded with the same prefs the mocked usePreferences
 * returns. The replay-aware flush reads from this cache, not from the hook
 * snapshot, so we have to keep them in sync for "preserves sibling keys"
 * assertions to mean what they say.
 */
function makePrimedClient(): QueryClient {
  // gcTime: Infinity so seeded data survives vi.advanceTimersByTime() ticks.
  // The default test client's gcTime: 0 collects unobserved queries the
  // moment a fake-timer tick advances past 0ms, which would silently empty
  // the cache before our debounced flush reads it.
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
  qc.setQueryData(queryKeys.user.preferences(), prefsState.current);
  return qc;
}

describe('useDashboardPrefs', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockMutate.mockReset();
    mockToast.mockReset();
    mutationState.isPending = false;
    loadingState.isLoading = false;
    prefsState.current = { other_preference: { theme: 'dark' } };
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('cold-cache gate: update() is a no-op while preferences are still loading', () => {
    // Regression: without the gate, a fast click before the GET resolves
    // would PUT { other_preference: { dashboard: {...} } } and clobber
    // sibling server-side keys (theme, locale).
    loadingState.isLoading = true;
    prefsState.current = null;
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ mode: 'custom' });
    });
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('debounces non-immediate writes by 800ms', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ mode: 'custom' });
    });
    expect(mockMutate).not.toHaveBeenCalled();
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('flushes immediately when {immediate:true}', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.setMode('custom');
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('preserves sibling other_preference keys (theme) when flushing', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.setMode('custom');
    });
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { theme?: string; dashboard?: { mode: string } };
    };
    expect(payload.other_preference.theme).toBe('dark');
    expect(payload.other_preference.dashboard?.mode).toBe('custom');
  });

  it('shows a destructive toast when the server rejects the write', () => {
    mockMutate.mockImplementation((_payload, opts: { onError: () => void }) => {
      opts.onError();
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.setMode('custom');
    });
    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast.mock.calls[0][0]).toMatchObject({ variant: 'destructive' });
  });

  it('caps history at 3 prior layouts on repeated applyPreset', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.applyPreset('morning-brief');
    });
    act(() => {
      result.current.applyPreset('trader');
    });
    act(() => {
      result.current.applyPreset('researcher');
    });
    act(() => {
      result.current.applyPreset('agent-desk');
    });
    expect(result.current.prefs.history?.length ?? 0).toBeLessThanOrEqual(3);
  });

  it('replay-aware flush reads the freshest queryClient snapshot, not the queue-time ref', () => {
    // Tab A queues an edit. While the debounce is pending, Tab B writes and
    // we receive the broadcast → cache gets a NEW theme value. Tab A's flush
    // must use the new theme, not the snapshot it captured at queue time.
    const queryClient = makePrimedClient();
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.update({ mode: 'custom' });
    });
    // Simulate cross-tab landing in the cache before our debounce fires.
    act(() => {
      queryClient.setQueryData(queryKeys.user.preferences(), {
        other_preference: { theme: 'light', remoteAddedKey: 'remote' },
      });
    });
    // Sanity: confirm cache has the cross-tab value before flush fires.
    const cachedNow = queryClient.getQueryData(queryKeys.user.preferences()) as { other_preference: Record<string, unknown> };
    expect(cachedNow.other_preference.theme).toBe('light');
    act(() => {
      vi.advanceTimersByTime(800);
    });
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { theme?: string; remoteAddedKey?: string; dashboard?: { mode: string } };
    };
    expect(payload.other_preference.theme).toBe('light');
    expect(payload.other_preference.remoteAddedKey).toBe('remote');
    expect(payload.other_preference.dashboard?.mode).toBe('custom');
  });

  it('resets pendingTimer to null after the debounce fires (gate not stuck)', () => {
    // Without the reset, the BroadcastChannel onmessage handler reads a
    // truthy timer ID and skips invalidation forever after the first edit.
    const queryClient = makePrimedClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.update({ mode: 'custom' });
    });
    act(() => {
      vi.advanceTimersByTime(800);
    });
    // After flush, broadcast a remote edit — the gate should be open.
    const channels = (globalThis as unknown as { __testChannels?: Array<{ onmessage?: (e: MessageEvent) => void }> }).__testChannels;
    const chan = channels?.[channels.length - 1];
    chan?.onmessage?.({ data: { type: 'updated' } } as MessageEvent);
    // invalidate should have run once for the cross-tab signal (mutation isPending=false in mock).
    expect(invalidateSpy).toHaveBeenCalled();
  });
});

describe('useDashboardPrefs — BroadcastChannel', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockMutate.mockReset();
    mockToast.mockReset();
    mutationState.isPending = false;
    prefsState.current = { other_preference: { theme: 'dark' } };
    // Reset captured channels between tests.
    (globalThis as unknown as { __testChannels: unknown[] }).__testChannels = [];
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  // Each useDashboardPrefs mount creates TWO channels: a writer (from
  // useDashboardPrefsWriter) and a listener (from useDashboardPrefs itself).
  // Helpers find them by role rather than index so refactors of either side
  // don't ripple into test brittleness.
  const getListenerChannel = () => {
    const channels = (globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels;
    return channels.find((c) => typeof c.onmessage === 'function');
  };
  const getWriterChannel = () => {
    const channels = (globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels;
    return channels.find((c) => c.posted.length > 0)
      // Fall back to the first non-listener channel if nothing has posted yet.
      ?? channels.find((c) => typeof c.onmessage !== 'function');
  };

  it('opens a writer + listener channel on mount and closes them on unmount', () => {
    const { unmount } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    const channels = (globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels;
    expect(channels.length).toBe(2);
    expect(channels.every((c) => !c.closed)).toBe(true);
    unmount();
    expect(channels.every((c) => c.closed)).toBe(true);
  });

  it('postMessages "updated" on flush success', () => {
    mockMutate.mockImplementation((_payload, opts: { onSuccess: () => void }) => {
      opts.onSuccess();
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.setMode('custom');
    });
    expect(getWriterChannel()?.posted).toEqual([{ type: 'updated' }]);
  });

  it('onmessage invalidates the prefs query when no edit is in flight', () => {
    const queryClient = makePrimedClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    getListenerChannel()?.onmessage?.({ data: { type: 'updated' } } as MessageEvent);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: queryKeys.user.preferences() });
  });

  it('onmessage defers invalidate (replay) when a debounce is pending', () => {
    const queryClient = makePrimedClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.update({ mode: 'custom' });
    });
    invalidateSpy.mockClear();
    // Broadcast lands while debounce is pending — should NOT invalidate yet.
    getListenerChannel()?.onmessage?.({ data: { type: 'updated' } } as MessageEvent);
    expect(invalidateSpy).not.toHaveBeenCalled();
    // Drain the debounce → flush succeeds → replay should drain the deferred
    // invalidate. mockMutate doesn't auto-call onSuccess; do so manually.
    mockMutate.mockImplementationOnce((_p, opts: { onSuccess: () => void }) => opts.onSuccess());
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: queryKeys.user.preferences() });
  });

  it('onmessage defers invalidate (replay) when the mutation is in flight', () => {
    mutationState.isPending = true;
    const queryClient = makePrimedClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { rerender } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    getListenerChannel()?.onmessage?.({ data: { type: 'updated' } } as MessageEvent);
    expect(invalidateSpy).not.toHaveBeenCalled();
    // Mutation completes → re-render flips isPending → replay drains.
    mutationState.isPending = false;
    rerender();
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: queryKeys.user.preferences() });
  });

  it('ignores messages with the wrong type', () => {
    const queryClient = makePrimedClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    const listener = getListenerChannel();
    listener?.onmessage?.({ data: { type: 'something-else' } } as MessageEvent);
    listener?.onmessage?.({ data: null } as MessageEvent);
    expect(invalidateSpy).not.toHaveBeenCalled();
  });
});

describe('useDashboardPrefs — BroadcastChannel unsupported', () => {
  let originalBC: typeof BroadcastChannel | undefined;
  beforeEach(() => {
    vi.useFakeTimers();
    mockMutate.mockReset();
    mutationState.isPending = false;
    prefsState.current = { other_preference: { theme: 'dark' } };
    originalBC = (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel;
    delete (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel;
  });
  afterEach(() => {
    vi.useRealTimers();
    if (originalBC) (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel = originalBC;
  });

  it('does not crash and skips broadcast when BroadcastChannel is undefined', () => {
    mockMutate.mockImplementation((_payload, opts: { onSuccess: () => void }) => {
      opts.onSuccess();
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    expect(() => {
      act(() => {
        result.current.setMode('custom');
      });
    }).not.toThrow();
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });
});

// --- jsdom BroadcastChannel polyfill --------------------------------------
// jsdom doesn't ship BroadcastChannel; install a minimal recording stand-in
// at module load. Tests pull the channel list from globalThis.__testChannels
// to assert posts and invoke onmessage manually.
type TestChannel = {
  name: string;
  posted: unknown[];
  closed: boolean;
  onmessage?: (e: MessageEvent) => void;
  postMessage: (m: unknown) => void;
  close: () => void;
};
// Scoped install: beforeAll → install fake, afterAll → restore (or delete if
// the worker had no native BroadcastChannel). Without the restore, other
// test files running in the same vitest worker would see a fake
// BroadcastChannel and miss real-undefined fallback behavior.
class FakeBroadcastChannel implements TestChannel {
  name: string;
  posted: unknown[] = [];
  closed = false;
  onmessage?: (e: MessageEvent) => void;
  constructor(name: string) {
    this.name = name;
    ((globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels).push(this);
  }
  postMessage(m: unknown) { this.posted.push(m); }
  close() { this.closed = true; }
}

let originalBroadcastChannel: typeof BroadcastChannel | undefined;
beforeAll(() => {
  originalBroadcastChannel = (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel;
  (globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels = [];
  (globalThis as unknown as { BroadcastChannel: typeof FakeBroadcastChannel }).BroadcastChannel =
    FakeBroadcastChannel;
});
afterAll(() => {
  if (originalBroadcastChannel) {
    (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel = originalBroadcastChannel;
  } else {
    delete (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel;
  }
  delete (globalThis as { __testChannels?: unknown }).__testChannels;
});
