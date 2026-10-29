import { describe, it, expect, vi, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';
import { act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '../../../../../test/utils';
import { queryKeys } from '@/lib/queryKeys';

const prefsState: { current: { other_preference?: Record<string, unknown> | null } | null } = {
  current: { other_preference: { theme: 'dark' } },
};
const loadingState = { isLoading: false, failed: false };
const mockMutate = vi.fn();
const mockToast = vi.fn();
const mutationState = { isPending: false };

type WriteCallbacks = { onSuccess?: () => void; onError?: (err: unknown) => void };
/** A `mutateAsync` whose promise settles when a test calls the write's
 *  callbacks, as the real one settles alongside them. */
function settledBy(record: (payload: unknown, opts: WriteCallbacks) => void) {
  return (payload: unknown, opts?: WriteCallbacks) =>
    new Promise<void>((resolve, reject) => {
      record(payload, {
        onSuccess: () => {
          opts?.onSuccess?.();
          resolve();
        },
        onError: (err) => {
          opts?.onError?.(err);
          reject(err);
        },
      });
    });
}

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({
    preferences: prefsState.current,
    isLoading: loadingState.isLoading,
    isLoaded: !loadingState.isLoading && !loadingState.failed,
  }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  PREFERENCE_MUTATION_KEY: ['user-preferences'],
  useUpdatePreferences: () => ({ mutateAsync: settledBy(mockMutate), isPending: mutationState.isPending }),
}));
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: mockToast, dismiss: vi.fn(), toasts: [] }),
}));

import { useDashboardPrefs } from '../useDashboardPrefs';
import { defaultLayout, getPreset } from '../../presets';
import { DASHBOARD_PREFS_VERSION } from '../../types';

/**
 * Build a queryClient pre-seeded with the same prefs the mocked usePreferences
 * returns. The writer's cold-cache check reads this cache, not the hook
 * snapshot, so they have to stay in sync for writes to be accepted.
 */
/** A preferences write in the mutation cache that never settles. */
function startWrite(queryClient: QueryClient, variables: unknown): void {
  void queryClient
    .getMutationCache()
    .build(queryClient, { mutationKey: ['user-preferences'], mutationFn: () => new Promise(() => {}) })
    .execute(variables);
}

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
    loadingState.failed = false;
    prefsState.current = { other_preference: { theme: 'dark' } };
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('shows the default layout, with the same fixed ids on every mount, when nothing is saved', () => {
    // The grid remembers a widget's last loaded shape by its id, and the
    // default can be drawn many times before an edit saves it.
    const first = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    const firstIds = first.result.current.prefs.widgets.map((w) => w.id);
    first.unmount();
    const second = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    const secondIds = second.result.current.prefs.widgets.map((w) => w.id);

    expect(firstIds.length).toBeGreaterThan(0);
    expect(firstIds).toEqual(first.result.current.prefs.widgets.map((w) => `default-${w.type}`));
    expect(secondIds).toEqual(firstIds);
    expect(second.result.current.prefs).toEqual(defaultLayout());
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('shows the default layout for a v1 Classic record', () => {
    prefsState.current = {
      other_preference: {
        dashboard: { version: 1, mode: 'classic', widgets: [{ id: 'old', type: 'news.feed', config: {} }], layouts: {} },
      },
    };
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    expect(result.current.prefs.widgets.map((w) => w.id)).toEqual(defaultLayout().widgets.map((w) => w.id));
  });

  it('shows the saved layout when there is one', () => {
    const widgets = [{ id: 'saved-1', type: 'news.feed', config: {} }];
    prefsState.current = {
      other_preference: { dashboard: { version: DASHBOARD_PREFS_VERSION, widgets, layouts: {} } },
    };
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    expect(result.current.prefs.widgets.map((w) => w.id)).toEqual(['saved-1']);
  });

  it('resetToDefault writes Morning brief at once and keeps the prior layout in history', () => {
    const widgets = [{ id: 'saved-1', type: 'news.feed', config: {} }];
    prefsState.current = {
      other_preference: { dashboard: { version: DASHBOARD_PREFS_VERSION, widgets, layouts: {} } },
    };
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.resetToDefault();
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const types = (prefs: { widgets: Array<{ type: string }> }) => prefs.widgets.map((w) => w.type);
    expect(types(result.current.prefs)).toEqual(types(getPreset('morning-brief')));
    expect(result.current.prefs.history?.[0]?.widgets.map((w) => w.id)).toEqual(['saved-1']);
  });

  it('new user, warm cache: a first edit is written with only the dashboard key', () => {
    // Regression: the writer used to refuse when the user had no
    // other_preference at all, so a new user's first edit reverted on the
    // next refetch.
    prefsState.current = { other_preference: null };
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { dashboard?: { widgets?: unknown[] } };
    };
    expect(Object.keys(payload.other_preference)).toEqual(['dashboard']);
    expect(payload.other_preference.dashboard?.widgets?.length).toBe(defaultLayout().widgets.length);
  });

  it('cold-cache gate: update() is a no-op while preferences are still loading', () => {
    // Regression: without the gate, a fast click before the GET resolves
    // would PUT a dashboard built from `{}` and wipe the user's saved layout
    // (the server replaces the dashboard key wholesale).
    loadingState.isLoading = true;
    prefsState.current = null;
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('after a failed read, keeps changes on the page and saves none, which would replace the saved layout', () => {
    loadingState.failed = true;
    prefsState.current = null;
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    expect(result.current.editable).toBe(false);
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(result.current.prefs.lastBreakpoint).toBe('md');
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('keeps an adjustment to the unsaved default on the page, and saves it with the first edit', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { adjust: true });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(result.current.prefs.layouts.md).toEqual([]);
    expect(mockMutate).not.toHaveBeenCalled();

    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { dashboard: { layouts: Record<string, unknown>; lastBreakpoint?: string; mode?: string } };
    };
    expect(payload.other_preference.dashboard.layouts.md).toEqual([]);
    expect(payload.other_preference.dashboard.lastBreakpoint).toBe('md');
    // For a tab still on a build from before v2, which reads a record
    // without it as Classic.
    expect(payload.other_preference.dashboard.mode).toBe('custom');
  });

  it('saves an adjustment to the unsaved default with an edit already waiting to save', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { adjust: true });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { dashboard: { layouts: Record<string, unknown>; lastBreakpoint?: string } };
    };
    expect(payload.other_preference.dashboard.lastBreakpoint).toBe('md');
    expect(payload.other_preference.dashboard.layouts.md).toEqual([]);
  });

  it('saves an adjustment made while an edit is saving once that edit lands', () => {
    let land = () => {};
    mockMutate.mockImplementation((_payload, opts: { onSuccess: () => void }) => {
      mutationState.isPending = true;
      land = () => {
        mutationState.isPending = false;
        opts.onSuccess();
      };
    });
    const { result, rerender } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    rerender();
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { adjust: true });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    // Nothing is sent under a write still saving.
    expect(mockMutate).toHaveBeenCalledTimes(1);
    act(() => land());
    rerender();
    expect(mockMutate).toHaveBeenCalledTimes(2);
    const payload = mockMutate.mock.calls[1][0] as {
      other_preference: { dashboard: { layouts: Record<string, unknown>; lastBreakpoint?: string } };
    };
    expect(payload.other_preference.dashboard.lastBreakpoint).toBe('md');
    expect(payload.other_preference.dashboard.layouts.md).toEqual([]);
  });

  it('holds an adjustment made before the page sees its write start', () => {
    // The write is in the mutation cache at once, but the observer's
    // `isPending` has not reached a render yet.
    prefsState.current = {
      other_preference: {
        dashboard: { version: DASHBOARD_PREFS_VERSION, widgets: [{ id: 'saved-1', type: 'news.feed', config: {} }], layouts: {} },
      },
    };
    const queryClient = makePrimedClient();
    mockMutate.mockImplementation((payload) => {
      startWrite(queryClient, payload);
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.applyPreset('trader');
    });
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { adjust: true });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('saves an adjustment to a saved layout while another preference is saving', () => {
    prefsState.current = {
      other_preference: {
        dashboard: { version: DASHBOARD_PREFS_VERSION, widgets: [{ id: 'saved-1', type: 'news.feed', config: {} }], layouts: {} },
      },
    };
    const queryClient = makePrimedClient();
    startWrite(queryClient, { other_preference: { theme: 'light' } });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { adjust: true });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('drops an adjustment made while an edit is saving when that edit is rejected', () => {
    const widgets = [{ id: 'saved-1', type: 'news.feed', config: {} }];
    prefsState.current = {
      other_preference: { dashboard: { version: DASHBOARD_PREFS_VERSION, widgets, layouts: {} } },
    };
    let reject = () => {};
    mockMutate.mockImplementation((_payload, opts: { onError: () => void }) => {
      mutationState.isPending = true;
      reject = () => {
        mutationState.isPending = false;
        opts.onError();
      };
    });
    const { result, rerender } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.applyPreset('trader');
    });
    rerender();
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { adjust: true });
    });
    act(() => reject());
    rerender();
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    // The rejected preset is not sent again under the adjustment.
    expect(mockMutate).toHaveBeenCalledTimes(1);
    expect(result.current.prefs.widgets.map((w) => w.id)).toEqual(['saved-1']);
  });

  it('saves an adjustment to a saved layout', () => {
    prefsState.current = {
      other_preference: {
        dashboard: { version: DASHBOARD_PREFS_VERSION, widgets: [{ id: 'saved-1', type: 'news.feed', config: {} }], layouts: {} },
      },
    };
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { adjust: true });
    });
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('debounces non-immediate writes by 800ms', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    expect(mockMutate).not.toHaveBeenCalled();
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('saves an edit still waiting on its debounce when the page is left', () => {
    const { result, unmount } = renderHookWithProviders(() => useDashboardPrefs(), {
      queryClient: makePrimedClient(),
    });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    unmount();
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const payload = mockMutate.mock.calls[0][0] as { other_preference: { dashboard: { lastBreakpoint?: string } } };
    expect(payload.other_preference.dashboard.lastBreakpoint).toBe('md');
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('flushes immediately when {immediate:true}', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  // The backend shallow-merges top-level other_preference keys (JSONB ||) —
  // re-sending cached siblings would replay a stale value over a newer write
  // from another tab. The payload must carry ONLY the dashboard key.
  it('sends only the dashboard key — siblings are preserved by the server merge', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { theme?: string; dashboard?: { lastBreakpoint?: string } };
    };
    expect(Object.keys(payload.other_preference)).toEqual(['dashboard']);
    expect(payload.other_preference.dashboard?.lastBreakpoint).toBe('md');
  });

  it('shows a destructive toast when the server rejects the write', () => {
    mockMutate.mockImplementation((_payload, opts: { onError: () => void }) => {
      opts.onError();
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast.mock.calls[0][0]).toMatchObject({ variant: 'destructive' });
  });

  it('takes the saved layout back when a write is rejected', () => {
    // The rollback lands while the write still counts as pending, and the
    // refetch after it holds the same copy: only the write settling says so.
    const widgets = [{ id: 'saved-1', type: 'news.feed', config: {} }];
    prefsState.current = {
      other_preference: { dashboard: { version: DASHBOARD_PREFS_VERSION, widgets, layouts: {} } },
    };
    let reject = () => {};
    mockMutate.mockImplementation((_payload, opts: { onError: () => void }) => {
      mutationState.isPending = true;
      reject = () => {
        mutationState.isPending = false;
        opts.onError();
      };
    });
    const { result, rerender } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    act(() => {
      result.current.update({ widgets: [] }, { immediate: true });
    });
    rerender();
    expect(result.current.prefs.widgets).toEqual([]);
    act(() => reject());
    rerender();
    expect(result.current.prefs.widgets.map((w) => w.id)).toEqual(['saved-1']);
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

  it('a cross-tab cache update mid-debounce never leaks siblings into the flush payload', () => {
    // Tab A queues an edit. While the debounce is pending, Tab B's write lands
    // in the cache (new theme + a key this tab has never seen). Tab A's flush
    // must still PUT only the dashboard key: Tab B's values are preserved by
    // the server-side merge, not replayed from this tab's cache — replaying
    // them is how a stale cache used to revert another tab's settings.
    const queryClient = makePrimedClient();
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    // Simulate cross-tab landing in the cache before our debounce fires.
    act(() => {
      queryClient.setQueryData(queryKeys.user.preferences(), {
        other_preference: { theme: 'light', remoteAddedKey: 'remote' },
      });
    });
    act(() => {
      vi.advanceTimersByTime(800);
    });
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { theme?: string; remoteAddedKey?: string; dashboard?: { lastBreakpoint?: string } };
    };
    expect(Object.keys(payload.other_preference)).toEqual(['dashboard']);
    expect(payload.other_preference.dashboard?.lastBreakpoint).toBe('md');
  });

  it('resets pendingTimer to null after the debounce fires (gate not stuck)', () => {
    // Without the reset, the BroadcastChannel onmessage handler reads a
    // truthy timer ID and skips invalidation forever after the first edit.
    const queryClient = makePrimedClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
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

  it('postMessages "updated" on flush success', async () => {
    mockMutate.mockImplementation((_payload, opts: { onSuccess: () => void }) => {
      opts.onSuccess();
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs(), { queryClient: makePrimedClient() });
    await act(async () => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    expect(getWriterChannel()?.posted).toEqual([{ type: 'updated' }]);
  });

  it('tells other tabs about an edit saved as the page is left', async () => {
    let succeed: () => void = () => {};
    mockMutate.mockImplementation((_payload, opts: { onSuccess: () => void }) => {
      succeed = opts.onSuccess;
    });
    const { result, unmount } = renderHookWithProviders(() => useDashboardPrefs(), {
      queryClient: makePrimedClient(),
    });
    act(() => {
      result.current.update({ lastBreakpoint: 'md' });
    });
    unmount();
    const before = (globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels.length;
    await act(async () => {
      succeed();
    });
    const channels = (globalThis as unknown as { __testChannels: TestChannel[] }).__testChannels;
    const opened = channels.slice(before);
    expect(opened).toHaveLength(1);
    expect(opened[0].posted).toEqual([{ type: 'updated' }]);
    expect(opened[0].closed).toBe(true);
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
      result.current.update({ lastBreakpoint: 'md' });
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
        result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
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
