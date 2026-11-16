import { useQuery } from '@tanstack/react-query';
import { apiErrorStatus } from '@/api/errors';
import { getPendingVendorTerms, type PendingVendorTerms } from '@/api/vendorTerms';
import { queryKeys } from '@/lib/queryKeys';

/** The feed's own reconcile pace, as for the agent's command requests. */
const FALLBACK_REFETCH_MS = 30_000;

const NONE: PendingVendorTerms[] = [];

/** The vendors whose terms wait on the user; empty until read. */
export function usePendingVendorTerms(): PendingVendorTerms[] {
  const { data } = useQuery({
    queryKey: queryKeys.vendorTerms.pending(),
    queryFn: getPendingVendorTerms,
    // The user feed invalidates this whenever the pending set changes; the
    // interval only covers a feed that is down.
    staleTime: 0,
    // A server without the route answers 404 every time, so it is not asked again.
    refetchInterval: (query) => (apiErrorStatus(query.state.error) === 404 ? false : FALLBACK_REFETCH_MS),
    refetchOnWindowFocus: 'always',
    retry: (count, err) => count < 1 && apiErrorStatus(err) === null,
  });
  return data ?? NONE;
}
