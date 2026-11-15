import { useQuery } from '@tanstack/react-query';
import { apiErrorStatus } from '@/api/errors';
import { getPendingCliApprovals, type CliApproval } from '@/api/cliApprovals';
import { queryKeys } from '@/lib/queryKeys';

/** The feed's own reconcile pace: a request it missed shows within one beat. */
const FALLBACK_REFETCH_MS = 30_000;

const NONE: CliApproval[] = [];

/**
 * The requests waiting on the user, oldest first (empty until read), and
 * whether the list has been read at all, answered or failed.
 */
export function usePendingCliApprovalsQuery(): { approvals: CliApproval[]; read: boolean } {
  const { data, isFetched } = useQuery({
    queryKey: queryKeys.cliApprovals.pending(),
    queryFn: getPendingCliApprovals,
    // The user feed invalidates this as requests arrive and are answered; the
    // interval only covers a feed that is down.
    staleTime: 0,
    // A server without the route answers 404 every time, so it is not asked again.
    refetchInterval: (query) => (apiErrorStatus(query.state.error) === 404 ? false : FALLBACK_REFETCH_MS),
    refetchOnWindowFocus: 'always',
    retry: (count, err) => count < 1 && apiErrorStatus(err) === null,
  });
  return { approvals: data ?? NONE, read: isFetched };
}

/** The requests waiting on the user, oldest first; empty until read. */
export function usePendingCliApprovals(): CliApproval[] {
  return usePendingCliApprovalsQuery().approvals;
}
