import { useCallback, useMemo, useRef } from 'react';
import { usePreferences } from '@/hooks/usePreferences';
import { authSessionCheck } from '@/lib/authResets';
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
  const { preferences, isLoading, isLoaded } = usePreferences();
  const { writeOnboardingPrefs, remoteUpdateGen } = useOnboardingPrefsWriter();

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

  // One write attempt per task per session. A refused PUT moves the prefs
  // entry twice (optimistic rollback, then the settle refetch), and callers
  // that stamp from an effect keyed on those prefs would re-send the same
  // write for as long as the server kept refusing it. Only an accepted write
  // counts: a cold-cache refusal (the writer returns false) leaves the task
  // for the next visit once the cache fills. The attempts also restart when
  // another tab's write lands (it may have been a reset that un-stamped a
  // task this tab already tried); this tab's own failed PUTs never move that
  // count, so the loop the guard exists for stays closed.
  const taskAttemptsRef = useRef({ gen: remoteUpdateGen.current, ids: new Map<string, Promise<void>>() });
  // The chat apps prompt's pin, once per session the same way: a refused PUT
  // rolls it back, and the card's effect would send it again.
  const pinTriedRef = useRef(false);
  // Both are this account's: a sign-out or an account switch, which leaves
  // the provider mounted, starts them over for the next one.
  const sessionRef = useRef(authSessionCheck());
  const forgetLastAccount = useCallback(() => {
    if (sessionRef.current()) return;
    sessionRef.current = authSessionCheck();
    taskAttemptsRef.current.ids.clear();
    pinTriedRef.current = false;
  }, []);

  /**
   * Stamp a getting-started task complete. Resolves once the write lands or
   * fails, or at once when there is nothing to send, for a caller about to
   * leave the page that would otherwise cut the write off; a task asked again
   * while its write is out resolves with that write. Never rejects.
   */
  const markTaskDone = useCallback(
    (id: string): Promise<void> => {
      if (isLoading) return Promise.resolve();
      forgetLastAccount();
      const attempts = taskAttemptsRef.current;
      if (attempts.gen !== remoteUpdateGen.current) {
        attempts.gen = remoteUpdateGen.current;
        attempts.ids.clear();
      }
      const tried = attempts.ids.get(id);
      if (tried) return tried;
      let settle!: () => void;
      const landed = new Promise<void>((resolve) => (settle = resolve));
      const accepted = writeOnboardingPrefs(
        (cur) =>
          cur.gettingStartedDoneAt[id] != null
            ? null
            : { ...cur, gettingStartedDoneAt: { ...cur.gettingStartedDoneAt, [id]: Date.now() } },
        { fallbackOther, onSuccess: () => settle(), onError: () => settle() }
      );
      if (!accepted) return Promise.resolve();
      attempts.ids.set(id, landed);
      return landed;
    },
    [writeOnboardingPrefs, remoteUpdateGen, fallbackOther, isLoading, forgetLastAccount]
  );

  /** Hide the getting-started list permanently. */
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
   * Re-show all page tips and the getting-started list (Settings affordance).
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

  /** Record that the user passed on the welcome flow, once. */
  const markWelcomeSkipped = useCallback(() => {
    if (isLoading) return;
    writeOnboardingPrefs(
      (cur) => (cur.welcomeSkippedAt !== null ? null : { ...cur, welcomeSkippedAt: Date.now() }),
      { fallbackOther }
    );
  }, [writeOnboardingPrefs, fallbackOther, isLoading]);

  const dismissChatAppsPrompt = useCallback(() => {
    if (isLoading) return;
    writeOnboardingPrefs(
      (cur) => (cur.chatAppsPromptDismissedAt !== null ? null : { ...cur, chatAppsPromptDismissedAt: Date.now() }),
      { fallbackOther }
    );
  }, [writeOnboardingPrefs, fallbackOther, isLoading]);

  /** Pins the chat apps prompt to the thread it first showed in. */
  const pinChatAppsPrompt = useCallback((threadId: string) => {
    if (isLoading) return;
    forgetLastAccount();
    if (pinTriedRef.current) return;
    pinTriedRef.current = writeOnboardingPrefs(
      (cur) => (cur.chatAppsPromptThreadId !== null ? null : { ...cur, chatAppsPromptThreadId: threadId }),
      { fallbackOther }
    );
  }, [writeOnboardingPrefs, fallbackOther, isLoading, forgetLastAccount]);

  /** Clear all onboarding state ("reset onboarding"). Returns false if the
   * write was refused (cold cache), so the caller can skip an empty toast. */
  const resetAll = useCallback((): boolean => {
    if (isLoading) return false;
    // A reset un-stamps every task, so the session's attempts start over too.
    taskAttemptsRef.current.ids.clear();
    pinTriedRef.current = false;
    return writeOnboardingPrefs(emptyOnboardingPrefs(), { fallbackOther, clearMirror: true });
  }, [writeOnboardingPrefs, fallbackOther, isLoading]);

  return {
    prefs,
    isLoading,
    /** The preferences were read; a failed read leaves `prefs` empty and refuses writes. */
    isLoaded,
    markPageIntroSeen,
    markTaskDone,
    dismissGettingStarted,
    replayGuides,
    setLastSeenReleaseVersion,
    ensureFirstRun,
    markWelcomeSkipped,
    dismissChatAppsPrompt,
    pinChatAppsPrompt,
    resetAll,
  };
}
