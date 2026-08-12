import { describe, it, expect, vi, beforeEach, beforeAll, afterAll } from 'vitest';
import { act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

const mockMutate = vi.fn();
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: mockMutate, isPending: false }),
}));

import { useOnboardingPrefsWriter } from '../onboardingPrefsWriter';
import { emptyOnboardingPrefs } from '../onboardingPrefsSchema';
import { readMirror } from '../mirror';

// jsdom has no BroadcastChannel; a same-name in-process bus is enough to test
// both halves of the cross-tab sync (a channel never receives its own posts).
class MockBroadcastChannel {
  static registry = new Map<string, Set<MockBroadcastChannel>>();
  onmessage: ((e: MessageEvent) => void) | null = null;
  constructor(public name: string) {
    const set = MockBroadcastChannel.registry.get(name) ?? new Set();
    set.add(this);
    MockBroadcastChannel.registry.set(name, set);
  }
  postMessage(data: unknown) {
    for (const chan of MockBroadcastChannel.registry.get(this.name) ?? []) {
      if (chan !== this) chan.onmessage?.({ data } as MessageEvent);
    }
  }
  close() {
    MockBroadcastChannel.registry.get(this.name)?.delete(this);
  }
}

const originalBC = globalThis.BroadcastChannel;
beforeAll(() => {
  (globalThis as Record<string, unknown>).BroadcastChannel = MockBroadcastChannel;
});
afterAll(() => {
  (globalThis as Record<string, unknown>).BroadcastChannel = originalBC;
});

const USER = 'u1';

function makeClient(seed?: unknown): QueryClient {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
  if (seed !== undefined) qc.setQueryData(queryKeys.user.preferences(), seed);
  qc.setQueryData(queryKeys.user.me(), { user_id: USER });
  return qc;
}

describe('useOnboardingPrefsWriter', () => {
  beforeEach(() => {
    mockMutate.mockReset();
    localStorage.clear();
    MockBroadcastChannel.registry.clear();
  });

  it('refuses to write on a cold cache with no fallback (unknown-state guard)', () => {
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient(undefined),
    });
    let accepted: boolean | undefined;
    act(() => {
      accepted = result.current.writeOnboardingPrefs(emptyOnboardingPrefs());
    });
    expect(accepted).toBe(false);
    expect(mockMutate).not.toHaveBeenCalled();
  });

  // The backend shallow-merges top-level other_preference keys (JSONB ||) —
  // re-sending cached siblings would replay a stale value over a newer write
  // from another tab. The payload must carry ONLY the onboarding key.
  it('sends only the onboarding key — siblings are preserved by the server merge', () => {
    const seed = { other_preference: { dashboard: { mode: 'custom' }, theme: 'dark' } };
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient(seed),
    });
    const next = { ...emptyOnboardingPrefs(), firstRunAt: 1 };
    act(() => {
      result.current.writeOnboardingPrefs(next, { fallbackOther: null });
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const [payload] = mockMutate.mock.calls[0];
    expect(payload).toEqual({ other_preference: { onboarding: next } });
  });

  it('writes the localStorage mirror on success', () => {
    mockMutate.mockImplementation((_vars, opts?: { onSuccess?: () => void }) => opts?.onSuccess?.());
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    const next = { ...emptyOnboardingPrefs(), firstRunAt: 7, lastSeenReleaseVersion: '2026.5' };
    act(() => {
      result.current.writeOnboardingPrefs(next, { fallbackOther: null });
    });
    expect(readMirror(USER)).toEqual({ pageIntrosSeen: {}, lastSeenReleaseVersion: '2026.5', firstRunAt: 7 });
  });

  it('functional updater builds on the cached onboarding state', () => {
    const stored = { ...emptyOnboardingPrefs(), firstRunAt: 5, lastSeenReleaseVersion: '2026.4' };
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: { onboarding: stored } }),
    });
    act(() => {
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, pageIntrosSeen: { ...cur.pageIntrosSeen, chat: 99 } }),
        { fallbackOther: null }
      );
    });
    const [payload] = mockMutate.mock.calls[0];
    expect(payload.other_preference.onboarding).toEqual({
      ...stored,
      pageIntrosSeen: { chat: 99 },
    });
  });

  it('a write mid-flight coalesces into one trailing PUT carrying both changes (in order)', () => {
    // mutate stays in flight until we settle it — as with the real PUT
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    act(() => {
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, firstRunAt: 5, lastSeenReleaseVersion: '2026.5' }),
        { fallbackOther: null }
      );
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, pageIntrosSeen: { ...cur.pageIntrosSeen, chat: 9 } }),
        { fallbackOther: null }
      );
    });
    // serialized: the second write must NOT race the first as a parallel PUT —
    // overlapping full-object writes could commit out of order server-side
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const [, firstOpts] = mockMutate.mock.calls[0];
    act(() => firstOpts.onSuccess());
    expect(mockMutate).toHaveBeenCalledTimes(2);
    const [secondPayload] = mockMutate.mock.calls[1];
    expect(secondPayload.other_preference.onboarding).toEqual({
      ...emptyOnboardingPrefs(),
      firstRunAt: 5,
      lastSeenReleaseVersion: '2026.5',
      pageIntrosSeen: { chat: 9 },
    });
  });

  it('after settling, the next write builds from the cache — an external reset is respected', () => {
    mockMutate.mockImplementation((_vars, opts?: { onSuccess?: () => void }) => opts?.onSuccess?.());
    const qc = makeClient({ other_preference: {} });
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: qc,
    });
    act(() => {
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, pageIntrosSeen: { chat: 1 } }),
        { fallbackOther: null }
      );
    });
    // External replacement of the prefs (Settings "Reset Preferences" DELETE,
    // or another tab's write landing via invalidate+refetch).
    qc.setQueryData(queryKeys.user.preferences(), { other_preference: {} });
    act(() => {
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, gettingStartedDismissedAt: 2 }),
        { fallbackOther: null }
      );
    });
    const [payload] = mockMutate.mock.calls[1];
    // must NOT resurrect the pre-reset pageIntrosSeen from the stale local copy
    expect(payload.other_preference.onboarding).toEqual({
      ...emptyOnboardingPrefs(),
      gettingStartedDismissedAt: 2,
    });
  });

  it('a failed write is dropped — a later write does not silently re-send it', () => {
    mockMutate.mockImplementationOnce((_vars, opts?: { onError?: (e: unknown) => void }) =>
      opts?.onError?.(new Error('rejected'))
    );
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    act(() => {
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, pageIntrosSeen: { chat: 1 } }),
        { fallbackOther: null }
      );
    });
    act(() => {
      result.current.writeOnboardingPrefs(
        (cur) => ({ ...cur, gettingStartedDismissedAt: 2 }),
        { fallbackOther: null }
      );
    });
    const [payload] = mockMutate.mock.calls[1];
    expect(payload.other_preference.onboarding).toEqual({
      ...emptyOnboardingPrefs(),
      gettingStartedDismissedAt: 2,
    });
  });

  it('clearMirror opt removes the per-user mirror before writing', () => {
    localStorage.setItem(
      `langalpha-onboarding-v1:${USER}`,
      JSON.stringify({ pageIntrosSeen: { chat: 5 }, lastSeenReleaseVersion: null, firstRunAt: 1 })
    );
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    act(() => {
      result.current.writeOnboardingPrefs(emptyOnboardingPrefs(), {
        fallbackOther: null,
        clearMirror: true,
      });
    });
    expect(readMirror(USER)).toBeNull();
  });

  it('updater returning null aborts the write', () => {
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    let accepted: boolean | undefined;
    act(() => {
      accepted = result.current.writeOnboardingPrefs(() => null, { fallbackOther: null });
    });
    expect(accepted).toBe(false);
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('optimisticMirror writes the mirror before the server confirms', () => {
    // mutate never resolves — without the flag the mirror would stay empty
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    act(() => {
      result.current.writeOnboardingPrefs(
        { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 42 } },
        { fallbackOther: null, optimisticMirror: true }
      );
    });
    expect(readMirror(USER)?.pageIntrosSeen).toEqual({ chat: 42 });
  });

  it('on error: forwards onError, writes no mirror, broadcasts nothing', () => {
    const boom = new Error('put failed');
    mockMutate.mockImplementation((_vars, opts?: { onError?: (e: unknown) => void }) =>
      opts?.onError?.(boom)
    );
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    const peer = new MockBroadcastChannel('onboarding-prefs');
    const received = vi.fn();
    peer.onmessage = received;
    const onError = vi.fn();
    act(() => {
      result.current.writeOnboardingPrefs(
        { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } },
        { fallbackOther: null, onError }
      );
    });
    expect(onError).toHaveBeenCalledWith(boom);
    expect(readMirror(USER)).toBeNull();
    expect(received).not.toHaveBeenCalled();
  });

  it('broadcasts {type: updated} to other tabs on success', () => {
    mockMutate.mockImplementation((_vars, opts?: { onSuccess?: () => void }) => opts?.onSuccess?.());
    const { result } = renderHookWithProviders(() => useOnboardingPrefsWriter(), {
      queryClient: makeClient({ other_preference: {} }),
    });
    const peer = new MockBroadcastChannel('onboarding-prefs');
    const received = vi.fn();
    peer.onmessage = received;
    act(() => {
      result.current.writeOnboardingPrefs(emptyOnboardingPrefs(), { fallbackOther: null });
    });
    expect(received).toHaveBeenCalledTimes(1);
    expect(received.mock.calls[0][0].data).toEqual({ type: 'updated' });
  });

  it('invalidates the prefs query on a cross-tab update, ignoring other message types', () => {
    const qc = makeClient({ other_preference: {} });
    const invalidate = vi.spyOn(qc, 'invalidateQueries');
    renderHookWithProviders(() => useOnboardingPrefsWriter(), { queryClient: qc });
    const peer = new MockBroadcastChannel('onboarding-prefs');
    act(() => {
      peer.postMessage({ type: 'something-else' });
    });
    expect(invalidate).not.toHaveBeenCalled();
    act(() => {
      peer.postMessage({ type: 'updated' });
    });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.user.preferences() });
  });
});
