import { useCallback, useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import type { UserPreferences } from '@/types/api';
import type { DashboardPrefs } from '../types';

export const BROADCAST_CHANNEL = 'dashboard-prefs';

/**
 * Single writer for `other_preference.dashboard`. Centralizes three concerns
 * that every dashboard prefs mutation needs to do correctly:
 *
 * 1. **Minimal payload.** The PUT carries ONLY the dashboard key: the backend
 *    shallow-merges top-level `other_preference` keys (JSONB `||`), so
 *    sibling keys (theme, locale, onboarding, providers) are preserved
 *    server-side. Re-sending them from this tab's cache would replay a stale
 *    sibling over a newer write from another tab or an in-flight settings
 *    change.
 *
 * 2. **Cross-tab broadcast.** Post `{type:'updated'}` to the dashboard-prefs
 *    BroadcastChannel on success so other tabs invalidate their cache and
 *    refetch — covers cross-tab consistency without relying on alt-tab focus.
 *
 * 3. **Cold-cache safety.** Refuse the write when the cache is cold AND no
 *    fallback snapshot was supplied. The server replaces the `dashboard` key
 *    wholesale, and a `next` computed without ever seeing the real prefs
 *    would erase the user's saved layout (irreversible — no prefs undo).
 *
 * Used by `useDashboardPrefs.flush()` (debounced widget edits).
 */
export function useDashboardPrefsWriter() {
  const updatePrefs = useUpdatePreferences();
  const queryClient = useQueryClient();

  // One channel per hook instance so postMessage doesn't pay the
  // construction cost on every write. Reset on unmount.
  const bcRef = useRef<BroadcastChannel | null>(null);
  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return;
    const chan = new BroadcastChannel(BROADCAST_CHANNEL);
    bcRef.current = chan;
    return () => {
      chan.close();
      bcRef.current = null;
    };
  }, []);

  const writeDashboardPrefs = useCallback(
    (
      next: DashboardPrefs,
      opts?: {
        /** Cold-cache sentinel (content unused — the payload is minimal).
         *  `null` = caller vouches the user has no saved prefs (new users).
         *  `undefined` = no info — writer refuses the write. */
        fallbackOther?: Record<string, unknown> | null;
        onSuccess?: () => void;
        onError?: (err: unknown) => void;
      }
    ): boolean => {
      const fresh = queryClient.getQueryData<UserPreferences>(queryKeys.user.preferences());
      if (fresh === undefined && opts?.fallbackOther === undefined) return false;
      // A user with no preferences row reads as null, and a write's optimistic
      // patch only lands on a row. Start them on the empty row this write
      // creates, so the page shows the write as it saves and a rejected write
      // rolls back to it, not past a write before it that landed.
      if (fresh === null) {
        queryClient.setQueryData<Partial<UserPreferences>>(queryKeys.user.preferences(), {});
      }
      updatePrefs
        .mutateAsync(
          {
            // `mode` is not read since v2. It is written for a tab still on a
            // build from before it, which reads any record without
            // `mode: 'custom'` as Classic and would show (and save) that page.
            other_preference: { dashboard: { ...next, mode: 'custom' } },
          },
          {
            onSuccess: () => opts?.onSuccess?.(),
            onError: (err) => opts?.onError?.(err),
          }
        )
        // Announced from the write's own promise, not a per-call callback:
        // those stop once the page unmounts, and the page saves an edit still
        // waiting as it is left. Another tab not told would keep the older
        // layout and save it over this one at its next edit.
        .then(() => {
          if (bcRef.current) {
            bcRef.current.postMessage({ type: 'updated' });
          } else if (typeof BroadcastChannel !== 'undefined') {
            const chan = new BroadcastChannel(BROADCAST_CHANNEL);
            chan.postMessage({ type: 'updated' });
            chan.close();
          }
        }, () => {});
      return true;
    },
    [updatePrefs, queryClient]
  );

  return { writeDashboardPrefs, isPending: updatePrefs.isPending };
}
