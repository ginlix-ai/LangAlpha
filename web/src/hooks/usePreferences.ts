import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getPreferences } from '../pages/Dashboard/utils/api';
import type { UserPreferences } from '../types/api';

/**
 * Shared hook for user preferences.
 * Replaces manual useEffect+useState fetching of /api/v1/users/me/preferences.
 * All consumers share a single cached entry — updates propagate automatically.
 *
 * staleTime: 0 + global refetchOnWindowFocus (set in main.tsx) means a tab
 * that returns to focus pulls fresh prefs. This pairs with the dashboard
 * prefs BroadcastChannel (useDashboardPrefs) so cross-tab edits land without
 * the user having to refresh.
 */
export function usePreferences() {
  const { data, ...rest } = useQuery({
    queryKey: queryKeys.user.preferences(),
    queryFn: getPreferences as () => Promise<UserPreferences>,
    staleTime: 0,
    retry: false,
  });
  return { preferences: data ?? null, ...rest };
}
