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
  // Named fields, never a spread of the result: react-query re-renders a reader
  // only when a field it read changes, and a spread reads all of them, so every
  // background refetch re-rendered every reader, each one paying for the spread.
  const { data, isLoading } = useQuery({
    queryKey: queryKeys.user.preferences(),
    queryFn: getPreferences as () => Promise<UserPreferences>,
    staleTime: PREFS_STALE_TIME_MS,
    retry: false,
  });
  // `preferences` is null both for a user with no row yet and for a read that
  // failed; `isLoaded` tells them apart, for a writer that replaces a value whole.
  return { preferences: data ?? null, isLoading, isLoaded: data !== undefined };
}
