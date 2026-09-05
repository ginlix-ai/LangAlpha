import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';

const mockWrite = vi.fn().mockReturnValue(true);
const mockRemoteUpdateGen = { current: 0 };
vi.mock('../onboardingPrefsWriter', () => ({
  useOnboardingPrefsWriter: () => ({ writeOnboardingPrefs: mockWrite, remoteUpdateGen: mockRemoteUpdateGen }),
}));

let mockPreferences: unknown = { other_preference: {} };
let mockLoading = false;
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: mockPreferences, isLoading: mockLoading }),
}));

import { useOnboardingPrefs } from '../useOnboardingPrefs';
import { emptyOnboardingPrefs } from '../onboardingPrefsSchema';
import type { OnboardingPrefs } from '../types';

/** Run the functional updater the hook handed to the writer against `cur`. */
function applyUpdater(callIndex: number, cur: OnboardingPrefs): OnboardingPrefs | null {
  const arg = mockWrite.mock.calls[callIndex][0];
  expect(typeof arg).toBe('function');
  return (arg as (c: OnboardingPrefs) => OnboardingPrefs | null)(cur);
}

describe('useOnboardingPrefs', () => {
  beforeEach(() => {
    mockWrite.mockClear();
    mockRemoteUpdateGen.current = 0;
    mockPreferences = { other_preference: {} };
    mockLoading = false;
    localStorage.clear();
  });

  it('every mutator no-ops while prefs are loading (cold-cache guard)', () => {
    mockLoading = true;
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => {
      result.current.markPageIntroSeen('chat');
      result.current.markTaskDone('firstChat');
      result.current.dismissGettingStarted();
      result.current.replayGuides();
      result.current.setLastSeenReleaseVersion('2026.6');
      result.current.ensureFirstRun('2026.6');
      result.current.resetAll();
    });
    expect(mockWrite).not.toHaveBeenCalled();
  });

  it('markPageIntroSeen stamps once per intro and is idempotent', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.markPageIntroSeen('chat'));
    expect(mockWrite).toHaveBeenCalledTimes(1);
    // requests local suppression even if the server write later fails
    expect(mockWrite.mock.calls[0][1]).toMatchObject({ optimisticMirror: true });

    const fresh = applyUpdater(0, emptyOnboardingPrefs());
    expect(fresh?.pageIntrosSeen.chat).toEqual(expect.any(Number));
    // another intro's stamp survives the merge
    const merged = applyUpdater(0, {
      ...emptyOnboardingPrefs(),
      pageIntrosSeen: { dashboard: 4 },
    });
    expect(merged?.pageIntrosSeen).toMatchObject({ dashboard: 4, chat: expect.any(Number) });
    // already-seen state → updater aborts the write instead of re-stamping
    expect(
      applyUpdater(0, { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 5 } })
    ).toBeNull();
  });

  it('markTaskDone attempts each task once per session, even when the write never lands', () => {
    // A rejected PUT rolls the cache back and then refetches: two new prefs
    // identities, both still unstamped. Callers stamp from effects keyed on
    // those prefs (route visits, the stocks and workspace probes), so the
    // guard has to live here or the same write is re-sent in a tight loop.
    // The writer accepts the attempt (true); the PUT fails later, async.
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => {
      result.current.markTaskDone('dashboard');
      result.current.markTaskDone('dashboard');
    });
    expect(mockWrite).toHaveBeenCalledTimes(1);

    // Another task is unaffected.
    act(() => result.current.markTaskDone('stocks'));
    expect(mockWrite).toHaveBeenCalledTimes(2);

    // A reset un-stamps every task, so its attempt starts over.
    act(() => {
      result.current.resetAll();
      result.current.markTaskDone('dashboard');
    });
    expect(mockWrite).toHaveBeenCalledTimes(4);
  });

  it('markTaskDone tries again after another tab lands a write, never on its own', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.markTaskDone('dashboard'));
    act(() => result.current.markTaskDone('dashboard'));
    expect(mockWrite).toHaveBeenCalledTimes(1);

    // Another tab reset the guides: its write bumps the remote count, and the
    // task this tab already gave up on is unstamped again on the server.
    mockRemoteUpdateGen.current += 1;
    act(() => result.current.markTaskDone('dashboard'));
    expect(mockWrite).toHaveBeenCalledTimes(2);
    // Still one attempt per remote update, not a loop.
    act(() => result.current.markTaskDone('dashboard'));
    expect(mockWrite).toHaveBeenCalledTimes(2);
  });

  it('markTaskDone does not spend its attempt on a cold-cache refusal', () => {
    // The prefs query failed once: preferences is null with isLoading false,
    // so the guard is reached but the writer refuses (false). A later refetch
    // fills the cache; the visit effect runs again and must still stamp.
    mockWrite.mockReturnValueOnce(false);
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.markTaskDone('dashboard'));
    act(() => result.current.markTaskDone('dashboard'));
    expect(mockWrite).toHaveBeenCalledTimes(2);
    // Accepted the second time: from here it is once per session.
    act(() => result.current.markTaskDone('dashboard'));
    expect(mockWrite).toHaveBeenCalledTimes(2);
  });

  it('markTaskDone stamps once and aborts when already done', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.markTaskDone('firstChat'));
    const fresh = applyUpdater(0, emptyOnboardingPrefs());
    expect(fresh?.gettingStartedDoneAt.firstChat).toEqual(expect.any(Number));
    expect(
      applyUpdater(0, { ...emptyOnboardingPrefs(), gettingStartedDoneAt: { firstChat: 5 } })
    ).toBeNull();
  });

  it('dismissGettingStarted stamps once and aborts when already dismissed', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.dismissGettingStarted());
    const fresh = applyUpdater(0, emptyOnboardingPrefs());
    expect(fresh?.gettingStartedDismissedAt).toEqual(expect.any(Number));
    expect(
      applyUpdater(0, { ...emptyOnboardingPrefs(), gettingStartedDismissedAt: 1 })
    ).toBeNull();
  });

  it('replayGuides clears the mirror, seen intros, and the card dismissal — keeps the rest', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.replayGuides());
    // the writer owns the per-user mirror; replay asks it to clear first
    expect(mockWrite.mock.calls[0][1]).toMatchObject({ clearMirror: true });
    const next = applyUpdater(0, {
      ...emptyOnboardingPrefs(),
      pageIntrosSeen: { chat: 1, dashboard: 2 },
      gettingStartedDismissedAt: 3,
      gettingStartedDoneAt: { firstChat: 4 },
      firstRunAt: 5,
      lastSeenReleaseVersion: '2026.5',
    });
    expect(next).toEqual({
      ...emptyOnboardingPrefs(),
      gettingStartedDoneAt: { firstChat: 4 }, // progress preserved
      firstRunAt: 5,
      lastSeenReleaseVersion: '2026.5',
    });
  });

  it('ensureFirstRun stamps caught-up version only when lastSeen is null', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.ensureFirstRun('2026.6'));

    const fromNew = applyUpdater(0, emptyOnboardingPrefs());
    expect(fromNew?.firstRunAt).toEqual(expect.any(Number));
    expect(fromNew?.lastSeenReleaseVersion).toBe('2026.6');

    const fromReturning = applyUpdater(0, {
      ...emptyOnboardingPrefs(),
      lastSeenReleaseVersion: '2026.2',
    });
    expect(fromReturning?.lastSeenReleaseVersion).toBe('2026.2'); // preserved, not bumped

    // already stamped → abort
    expect(applyUpdater(0, { ...emptyOnboardingPrefs(), firstRunAt: 1 })).toBeNull();
  });

  it('setLastSeenReleaseVersion overwrites onto the freshest state', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.setLastSeenReleaseVersion('2026.7'));
    const next = applyUpdater(0, { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 3 } });
    expect(next).toEqual({
      ...emptyOnboardingPrefs(),
      pageIntrosSeen: { chat: 3 },
      lastSeenReleaseVersion: '2026.7',
    });
  });

  it('resetAll clears the mirror and writes empty prefs', () => {
    const { result } = renderHook(() => useOnboardingPrefs());
    act(() => result.current.resetAll());
    expect(mockWrite).toHaveBeenCalledWith(
      emptyOnboardingPrefs(),
      expect.objectContaining({ clearMirror: true })
    );
  });

  it('migrates whatever is stored into a complete prefs object', () => {
    mockPreferences = {
      other_preference: { onboarding: { pageIntrosSeen: { chat: 7 }, junk: true } },
    };
    const { result } = renderHook(() => useOnboardingPrefs());
    expect(result.current.prefs).toEqual({
      ...emptyOnboardingPrefs(),
      pageIntrosSeen: { chat: 7 },
    });
  });
});
