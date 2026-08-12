import { useCallback, useMemo } from 'react';
import { usePreferences } from '@/hooks/usePreferences';
import { migrateOnboardingPrefs, emptyOnboardingPrefs } from './onboardingPrefsSchema';
import { useOnboardingPrefsWriter } from './onboardingPrefsWriter';
import type { OnboardingPrefs } from './types';

function readOnboarding(preferences: unknown): unknown {
  const prefs = preferences as { other_preference?: { onboarding?: unknown } } | null;
  return prefs?.other_preference?.onboarding;
}

/**
 * Reads + migrates `other_preference.onboarding` and exposes the mutators the
 * engine needs. Every mutator is gated on `isLoading` (cold-cache guard, same
 * as `useDashboardPrefs.update`) and routed through the guarded writer.
 */
export function useOnboardingPrefs() {
  const { preferences, isLoading } = usePreferences();
  const { writeOnboardingPrefs } = useOnboardingPrefsWriter();

  const prefs = useMemo<OnboardingPrefs>(
    () => migrateOnboardingPrefs(readOnboarding(preferences)),
    [preferences]
  );

  const fallbackOther =
    preferences === null
      ? undefined
      : ((preferences as { other_preference?: Record<string, unknown> }).other_preference ?? null);

  // Mutators use FUNCTIONAL updates: the writer applies them to the freshest
  // onboarding state it knows (last in-flight write, else the cache), so two
  // quick writes (ensureFirstRun then an immediate markPageIntroSeen)
  // field-merge instead of the second clobbering the first from a stale render.

  /** Mark a page intro as seen so it shows once per page. */
  const markPageIntroSeen = useCallback(
    (id: string) => {
      if (isLoading) return;
      writeOnboardingPrefs(
        (cur) =>
          cur.pageIntrosSeen[id] != null
            ? null
            : { ...cur, pageIntrosSeen: { ...cur.pageIntrosSeen, [id]: Date.now() } },
        // Suppress locally even if the PUT fails — otherwise a flaky save makes
        // the intro re-nag every session until a write finally lands.
        { fallbackOther, optimisticMirror: true }
      );
    },
    [writeOnboardingPrefs, fallbackOther, isLoading]
  );

  /** Stamp a getting-started task complete. */
  const markTaskDone = useCallback(
    (id: string) => {
      if (isLoading) return;
      writeOnboardingPrefs(
        (cur) =>
          cur.gettingStartedDoneAt[id] != null
            ? null
            : { ...cur, gettingStartedDoneAt: { ...cur.gettingStartedDoneAt, [id]: Date.now() } },
        { fallbackOther }
      );
    },
    [writeOnboardingPrefs, fallbackOther, isLoading]
  );

  /** Hide the getting-started card permanently. */
  const dismissGettingStarted = useCallback(() => {
    if (isLoading) return;
    writeOnboardingPrefs(
      (cur) =>
        cur.gettingStartedDismissedAt !== null
          ? null
          : { ...cur, gettingStartedDismissedAt: Date.now() },
      { fallbackOther }
    );
  }, [writeOnboardingPrefs, fallbackOther, isLoading]);

  /**
   * Re-show all page tips and the getting-started card (Settings affordance).
   * Returns false if the write was refused (cold cache) so the caller can skip
   * a "done" toast it can't honor — the server state would be unchanged.
   */
  const replayGuides = useCallback((): boolean => {
    if (isLoading) return false;
    return writeOnboardingPrefs(
      (cur) => ({ ...cur, pageIntrosSeen: {}, gettingStartedDismissedAt: null }),
      { fallbackOther, clearMirror: true }
    );
  }, [writeOnboardingPrefs, fallbackOther, isLoading]);

  const setLastSeenReleaseVersion = useCallback(
    (version: string) => {
      if (isLoading) return;
      writeOnboardingPrefs((cur) => ({ ...cur, lastSeenReleaseVersion: version }), {
        fallbackOther,
      });
    },
    [writeOnboardingPrefs, fallbackOther, isLoading]
  );

  /**
   * Set `firstRunAt` once. For a brand-new user (no `lastSeenReleaseVersion`)
   * we also stamp the current release so they start "caught up" and get the
   * page intros instead of a What's-New backlog.
   */
  const ensureFirstRun = useCallback(
    (caughtUpVersion: string | null, opts?: { onError?: (err: unknown) => void }): boolean => {
      if (isLoading) return false;
      return writeOnboardingPrefs(
        (cur) =>
          cur.firstRunAt !== null
            ? null
            : {
                ...cur,
                firstRunAt: Date.now(),
                lastSeenReleaseVersion: cur.lastSeenReleaseVersion ?? caughtUpVersion,
              },
        { fallbackOther, onError: opts?.onError }
      );
    },
    [writeOnboardingPrefs, fallbackOther, isLoading]
  );

  /** Clear all onboarding state ("reset onboarding"). Returns false if the
   * write was refused (cold cache), so the caller can skip an empty toast. */
  const resetAll = useCallback((): boolean => {
    if (isLoading) return false;
    return writeOnboardingPrefs(emptyOnboardingPrefs(), { fallbackOther, clearMirror: true });
  }, [writeOnboardingPrefs, fallbackOther, isLoading]);

  return {
    prefs,
    isLoading,
    markPageIntroSeen,
    markTaskDone,
    dismissGettingStarted,
    replayGuides,
    setLastSeenReleaseVersion,
    ensureFirstRun,
    resetAll,
  };
}
