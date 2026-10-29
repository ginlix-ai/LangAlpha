/**
 * Per-tier count quotas for the tier pickers.
 *
 * Both pickers read the workspace quota endpoint, including the one that
 * creates a computer: the backend still meters elevated tiers per workspace
 * (`check_quota="workspace"`), so this endpoint is the count that decides a
 * machine's tier too, and both surfaces have to spend from the same figure.
 */
import { type QueryClient, queryOptions, useQuery } from '@tanstack/react-query';

import { isPlatformMode } from '@/config/hostMode';
import { queryKeys } from '@/lib/queryKeys';

import { getWorkspaceQuota } from '../utils/api';

const quotaQuery = queryOptions({
  queryKey: queryKeys.workspaces.quota(),
  queryFn: getWorkspaceQuota,
  staleTime: 60_000,
});

/** `enabled` is the caller's own gate: fetch when a picker is actually open. */
export function useTierQuota(options: { enabled: boolean }) {
  return useQuery({ ...quotaQuery, enabled: isPlatformMode && options.enabled });
}

/** Read the quota ahead of a picker the user is about to open. */
export function prefetchTierQuota(queryClient: QueryClient) {
  return isPlatformMode ? queryClient.prefetchQuery(quotaQuery) : Promise.resolve();
}
