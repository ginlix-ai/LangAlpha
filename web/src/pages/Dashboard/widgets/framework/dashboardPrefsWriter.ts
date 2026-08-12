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
 * Used by `useDashboardPrefs.flush()` (debounced widget edits) and
 * `DashboardRouter.onModeChange()` (mode toggle).
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
      updatePrefs.mutate(
        {
          other_preference: { dashboard: next },
        },
        {
          onSuccess: () => {
            bcRef.current?.postMessage({ type: 'updated' });
            opts?.onSuccess?.();
          },
          onError: (err) => opts?.onError?.(err),
        }
      );
      return true;
    },
    [updatePrefs, queryClient]
  );

  return { writeDashboardPrefs, isPending: updatePrefs.isPending };
}
