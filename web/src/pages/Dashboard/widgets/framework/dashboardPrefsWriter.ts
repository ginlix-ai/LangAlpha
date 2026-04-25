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
 * 1. **Replay-aware sibling preservation.** Read the freshest cache snapshot
 *    at write time (not at queue/render time) so a cross-tab update or a
 *    concurrent write to a sibling key (theme, locale, provider) isn't
 *    clobbered by spreading a stale `other_preference` blob.
 *
 * 2. **Cross-tab broadcast.** Post `{type:'updated'}` to the dashboard-prefs
 *    BroadcastChannel on success so other tabs invalidate their cache and
 *    refetch — covers cross-tab consistency without relying on alt-tab focus.
 *
 * 3. **Cold-cache safety.** Refuse the write when the cache is cold AND no
 *    fallback dashboard snapshot was supplied. Without this, a fast click on
 *    cold load PUTs `{ other_preference: { dashboard: {...} } }` and wipes
 *    every sibling key on the server (irreversible — no prefs undo).
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
        /** `null` = warm prefs with empty other_preference (new users).
         *  `undefined` = no info — writer refuses the write. */
        fallbackOther?: Record<string, unknown> | null;
        onSuccess?: () => void;
        onError?: (err: unknown) => void;
      }
    ): boolean => {
      const fresh = queryClient.getQueryData<UserPreferences>(queryKeys.user.preferences());
      if (fresh === undefined && opts?.fallbackOther === undefined) return false;
      const freshOther = (fresh?.other_preference as Record<string, unknown> | undefined) ?? null;
      const baseOther = freshOther ?? opts?.fallbackOther ?? {};
      updatePrefs.mutate(
        {
          other_preference: { ...baseOther, dashboard: next },
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
