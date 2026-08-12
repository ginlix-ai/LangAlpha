import { useCallback, useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import type { UserPreferences } from '@/types/api';
import type { OnboardingPrefs } from './types';
import { migrateOnboardingPrefs } from './onboardingPrefsSchema';
import { writeMirror, clearMirror } from './mirror';

const ONBOARDING_BROADCAST_CHANNEL = 'onboarding-prefs';

interface PendingBatch {
  onSuccess: Array<() => void>;
  onError: Array<(err: unknown) => void>;
}

/**
 * Single writer for `other_preference.onboarding`. The PUT carries ONLY the
 * onboarding key: the backend shallow-merges top-level `other_preference`
 * keys (JSONB `||`), so sibling keys (dashboard, theme, locale) are preserved
 * server-side — re-sending them from this tab's cache would replay a stale
 * sibling over a newer write from another tab.
 *
 * The cold-cache refusal protects the onboarding key itself: the server
 * replaces `onboarding` wholesale, and a functional update applied to an
 * unknown current state would erase previously-seen intros/tasks.
 *
 * One PUT in flight at a time: writes that arrive mid-flight coalesce into a
 * single trailing PUT carrying the accumulated state. Full-object writes
 * therefore commit strictly in order — a slow early response can't erase a
 * later write's fields server-side.
 */
export function useOnboardingPrefsWriter() {
  // Destructure `mutate` (stable across renders) rather than depending on the
  // whole mutation result object (new identity every render) — otherwise the
  // writer callback churns and destabilizes every downstream callback.
  const { mutate } = useUpdatePreferences();
  const queryClient = useQueryClient();

  // Accumulated onboarding state of the in-flight PUT (plus writes coalesced
  // behind it). Trusted ONLY while a write is in flight: useUpdatePreferences
  // syncs the cache to server truth on settle (setQueryData on success,
  // invalidate on error), so once idle the cache is authoritative again and
  // external changes — another tab's write, a Settings preferences reset —
  // are picked up instead of being clobbered by a stale local copy.
  const lastWrittenRef = useRef<OnboardingPrefs | null>(null);
  const inFlightRef = useRef(false);
  const pendingRef = useRef<PendingBatch | null>(null);

  const bcRef = useRef<BroadcastChannel | null>(null);
  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return;
    const chan = new BroadcastChannel(ONBOARDING_BROADCAST_CHANNEL);
    bcRef.current = chan;
    // Listen on the same channel we post to (a channel never receives its own
    // messages, so this only fires for OTHER tabs): invalidate prefs so a tab
    // that didn't make the write pulls the fresh onboarding state instead of
    // re-opening a popup another tab already dismissed. Mirrors useDashboardPrefs.
    chan.onmessage = (e: MessageEvent) => {
      if ((e.data as { type?: string } | null)?.type !== 'updated') return;
      queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
    };
    return () => {
      chan.close();
      bcRef.current = null;
    };
  }, [queryClient]);

  // The mirror is per-user; the user is resolved long before any onboarding
  // write can happen, so a write-time cache read is sufficient.
  const currentUserId = useCallback(
    () =>
      queryClient.getQueryData<{ user_id?: string }>(queryKeys.user.me())?.user_id ?? null,
    [queryClient]
  );

  const send = useCallback(
    function send() {
      const batch = pendingRef.current;
      const resolved = lastWrittenRef.current;
      if (!batch || resolved === null) return;
      pendingRef.current = null;
      inFlightRef.current = true;
      const settle = () => {
        inFlightRef.current = false;
        if (pendingRef.current) send(); // writes coalesced mid-flight → trailing PUT
        else lastWrittenRef.current = null; // idle: the cache is authoritative again
      };
      mutate(
        { other_preference: { onboarding: resolved } },
        {
          onSuccess: () => {
            writeMirror(currentUserId(), resolved);
            bcRef.current?.postMessage({ type: 'updated' });
            // settle() in finally: a throwing callback must not strand the
            // queue with inFlightRef stuck true (no further write would send).
            try {
              for (const cb of batch.onSuccess) cb();
            } finally {
              settle();
            }
          },
          onError: (err) => {
            // No mirror write, no broadcast — and `settle` drops the rejected
            // payload once idle, so a failed write can't be silently re-sent
            // on top of fresh server state by a later unrelated write.
            try {
              for (const cb of batch.onError) cb(err);
            } finally {
              settle();
            }
          },
        }
      );
    },
    [mutate, currentUserId]
  );

  const writeOnboardingPrefs = useCallback(
    (
      next: OnboardingPrefs | ((current: OnboardingPrefs) => OnboardingPrefs | null),
      opts?: {
        /**
         * Base for reading the CURRENT onboarding state when the prefs cache
         * is cold. `null` = treat as a new user (empty state). `undefined` =
         * cold → refuse the write (an updater applied to unknown state would
         * clobber the server's onboarding key).
         */
        fallbackOther?: Record<string, unknown> | null;
        /**
         * Write the localStorage mirror BEFORE the server confirms. The mirror
         * is suppress-only by design, so this is safe — it just means a failed
         * PUT can't make the page intro re-nag locally every session.
         */
        optimisticMirror?: boolean;
        /** Remove the local mirror before writing (replay/reset affordances). */
        clearMirror?: boolean;
        onSuccess?: () => void;
        onError?: (err: unknown) => void;
      }
    ): boolean => {
      const fresh = queryClient.getQueryData<UserPreferences>(queryKeys.user.preferences());
      if (fresh === undefined && opts?.fallbackOther === undefined) return false;
      const freshOther = (fresh?.other_preference as Record<string, unknown> | undefined) ?? null;
      const baseOther = freshOther ?? opts?.fallbackOther ?? {};
      const current =
        (inFlightRef.current ? lastWrittenRef.current : null) ??
        migrateOnboardingPrefs(baseOther['onboarding']);
      const resolved = typeof next === 'function' ? next(current) : next;
      if (resolved === null) return false; // updater decided it's a no-op
      if (opts?.clearMirror) clearMirror(currentUserId());
      lastWrittenRef.current = resolved;
      if (opts?.optimisticMirror) writeMirror(currentUserId(), resolved);
      const batch: PendingBatch = pendingRef.current ?? { onSuccess: [], onError: [] };
      if (opts?.onSuccess) batch.onSuccess.push(opts.onSuccess);
      if (opts?.onError) batch.onError.push(opts.onError);
      pendingRef.current = batch;
      if (!inFlightRef.current) send();
      return true;
    },
    [queryClient, send, currentUserId]
  );

  return { writeOnboardingPrefs };
}
