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
import { readUserAfterPlanChange } from '@/hooks/useUser';
import { queryKeys } from '@/lib/queryKeys';

import { getWorkspaceQuota } from '../utils/api';

// Set by `readPlanAfterChange` and spent by the next read that
// succeeds, so a retry after a failed one asks again.
let planChanged = false;

// The fetch is wrapped so the api module is read at call time, not at import.
const quotaQuery = queryOptions({
  queryKey: queryKeys.workspaces.quota(),
  queryFn: async () => {
    const fresh = planChanged;
    const quota = await getWorkspaceQuota(fresh);
    if (fresh) planChanged = false;
    return quota;
  },
  staleTime: 60_000,
});

/** `enabled` is the caller's own gate: fetch when a picker is actually open. */
export function useTierQuota(options: { enabled: boolean }) {
  return useQuery({ ...quotaQuery, enabled: isPlatformMode && options.enabled });
}

/** The user is back from the plan page: the next quota read, and only that
 *  one, asks the server to read the plan again, and the account is read past
 *  the server's cached plan, so a size the new plan opens shows open, and the
 *  plan shows by its new name, however quickly the user came back. */
export function readPlanAfterChange(queryClient: QueryClient) {
  planChanged = true;
  return Promise.all([
    queryClient.invalidateQueries({ queryKey: quotaQuery.queryKey }),
    readUserAfterPlanChange(queryClient),
  ]);
}

/** Read the quota ahead of a picker the user is about to open. */
export function prefetchTierQuota(queryClient: QueryClient) {
  return isPlatformMode ? queryClient.prefetchQuery(quotaQuery) : Promise.resolve();
}
