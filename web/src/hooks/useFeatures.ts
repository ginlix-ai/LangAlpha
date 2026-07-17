import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getFeatures, setFeatureOverride } from '../api/features';
import type { FeatureState } from '../types/api';

// Flags change rarely; a 5-minute window avoids re-fetching on every mount
// while still picking up server-side changes within a session.
const FEATURES_STALE_TIME_MS = 5 * 60_000;

export function useFeatures() {
  return useQuery({
    queryKey: queryKeys.features.list(),
    queryFn: getFeatures,
    staleTime: FEATURES_STALE_TIME_MS,
    retry: false,
  });
}

/**
 * Effective enabled state for one flag. Features fail CLOSED in the UI: this
 * returns false while the query is loading or errored, so a gated surface never
 * flashes on before the real value lands.
 */
export function useFeatureEnabled(key: string): boolean {
  const { data } = useFeatures();
  return data?.find((f) => f.key === key)?.enabled ?? false;
}

/** Mutation for a user override; setQueryData propagates the returned list instantly. */
export function useSetFeatureOverride() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ key, enabled }: { key: string; enabled: boolean | null }) =>
      setFeatureOverride(key, enabled),
    onSuccess: (features: FeatureState[]) => {
      queryClient.setQueryData(queryKeys.features.list(), features);
    },
    onError: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.features.list() });
    },
  });
}
