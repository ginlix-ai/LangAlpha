import { useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getPreferences } from '../pages/Dashboard/utils/api';
import type { UserPreferences } from '../types/api';

/**
 * Reads the stored preferences now, past the cache. The server replaces an
 * array whole, so a writer that rebuilds one (`custom_models`, the starred
 * list) builds it from what is stored when it saves, not from what it
 * rendered: a change another tab saved since then is kept.
 */
export function useReadStoredPreferences(): () => Promise<UserPreferences | null> {
  const queryClient = useQueryClient();
  return useCallback(
    async () => (await queryClient.fetchQuery({
      queryKey: queryKeys.user.preferences(),
      queryFn: getPreferences as () => Promise<UserPreferences>,
      staleTime: 0,
    })) ?? null,
    [queryClient],
  );
}
