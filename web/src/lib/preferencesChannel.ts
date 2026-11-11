import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { PREFERENCE_MUTATION_KEY } from '@/hooks/useUpdatePreferences';
import { queryKeys } from './queryKeys';

/**
 * Where a tab says its preferences row moved. Named for the dashboard, its
 * first user, and kept so: a tab still on an older build listens here.
 */
export const PREFERENCES_CHANNEL = 'dashboard-prefs';

/**
 * Tells this browser's other tabs to read the row again. Every other channel
 * object hears it, this tab's own listeners included, since a channel skips
 * only the object that posted.
 */
export function announcePreferencesChanged(): void {
  if (typeof BroadcastChannel === 'undefined') return;
  const chan = new BroadcastChannel(PREFERENCES_CHANNEL);
  chan.postMessage({ type: 'updated' });
  chan.close();
}

/** Reads the row again when another tab says it moved. Mounted once, by the app shell. */
export function usePreferencesSync(): void {
  const qc = useQueryClient();
  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return;
    const chan = new BroadcastChannel(PREFERENCES_CHANNEL);
    chan.onmessage = (e: MessageEvent) => {
      if ((e.data as { type?: string } | null)?.type !== 'updated') return;
      // A write of this tab in flight reads the row again as it settles, and
      // a read now could land over its optimistic value.
      if (qc.isMutating({ mutationKey: PREFERENCE_MUTATION_KEY }) > 0) return;
      void qc.invalidateQueries({ queryKey: queryKeys.user.preferences() }, { cancelRefetch: false });
    };
    return () => chan.close();
  }, [qc]);
}
