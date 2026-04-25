import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getPreferences } from '../pages/Dashboard/utils/api';
import type { UserPreferences } from '../types/api';

// staleTime split by capability: browsers with BroadcastChannel get cross-tab
// sync via the dashboard prefs channel, so 60s is enough; Safari < 15.4 has
// no channel and depends on focus refetch, so it needs 0.
const PREFS_STALE_TIME_MS =
  typeof BroadcastChannel === 'undefined' ? 0 : 60_000;

export function usePreferences() {
  const { data, ...rest } = useQuery({
    queryKey: queryKeys.user.preferences(),
    queryFn: getPreferences as () => Promise<UserPreferences>,
    staleTime: PREFS_STALE_TIME_MS,
    retry: false,
  });
  return { preferences: data ?? null, ...rest };
}
