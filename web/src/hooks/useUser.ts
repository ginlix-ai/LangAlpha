import { type QueryClient, queryOptions, useQuery } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getCurrentUser } from '../pages/Dashboard/utils/api';
import { apiErrorStatus } from '@/api/errors';
import type { User } from '../types/api';

/** A 503 says the plan cannot be read right now (the platform is restarting),
 *  not that the user has none, so it is asked again after the wait the server
 *  names. Anything else fails at once, as it always has. */
const UNAVAILABLE_RETRIES = 3;

function retryAfterMs(error: unknown): number {
  const header = (error as { response?: { headers?: Record<string, string> } })
    ?.response?.headers?.['retry-after'];
  const seconds = Number.parseInt(header ?? '', 10);
  return (seconds > 0 ? Math.min(seconds, 30) : 5) * 1000;
}

/** The account's `/users/me`, read the one way every reader shares. */
export const userQueryOptions = queryOptions({
  queryKey: queryKeys.user.me(),
  queryFn: async (): Promise<User> => {
    const res = await getCurrentUser() as Record<string, unknown> & { user?: User };
    return (res.user ?? res) as User;
  },
  staleTime: 5 * 60_000,
  retry: (failureCount, error) =>
    apiErrorStatus(error) === 503 && failureCount < UNAVAILABLE_RETRIES,
  retryDelay: (_failureCount, error) => retryAfterMs(error),
});

/** Reads the account past the server's cached plan, then has every reader
 *  read it again, so a plan just changed shows by its new name. A read that
 *  fails leaves the account as it was. */
export function readUserAfterPlanChange(queryClient: QueryClient) {
  return getCurrentUser({ refresh_tier: true }).then(
    () => queryClient.invalidateQueries({ queryKey: queryKeys.user.me() }),
    () => undefined,
  );
}

/**
 * Shared hook for current user profile data.
 * Replaces manual useEffect+useState fetching of /api/v1/users/me.
 * All consumers share a single cached entry — updates propagate automatically.
 * With `enabled: false` it only reads what the app already holds, for a
 * surface a signed-out reader can open, where asking would only be refused.
 */
export function useUser({ enabled = true }: { enabled?: boolean } = {}) {
  // Named fields, not a spread: see usePreferences.
  const { data, isLoading, isError, refetch } = useQuery({ ...userQueryOptions, enabled });
  return { user: data ?? null, isLoading, isError, refetch };
}
