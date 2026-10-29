import { useQuery, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { GRANT_RENEW_MARGIN_MS, MIN_RENEW_INTERVAL_MS, msUntilRenewal } from '@/lib/expiry';
import { isClientError, retryUnlessClientError } from '@/api/errors';
import { createFileGrant } from '@/pages/ChatAgent/utils/api/shareLinks';
import type { FileGrant } from '@/types/api';

/** A grant lasts 12h; keep the entry that long so a re-opened panel does not mint again. */
const GRANT_GC_MS = 12 * 60 * 60_000;

/**
 * The signed prefix the owner's report iframes load workspace files under.
 *
 * The grant token names its workspace, but the id alone no longer opens
 * anything: a file is served only under the signature, and the signature
 * expires. One entry per workspace: the grant covers the whole workspace, and
 * every viewer on it shares the same renewal clock.
 */
export function useWorkspaceFileGrant(
  workspaceId: string | null | undefined,
): UseQueryResult<FileGrant, Error> {
  return useQuery<FileGrant, Error>({
    queryKey: queryKeys.fileGrants.workspace(workspaceId ?? ''),
    queryFn: () => createFileGrant(workspaceId!),
    enabled: !!workspaceId,
    // React Query counts staleTime from the fetch, so it is the span from that
    // fetch to the renewal deadline. Measured from now, it would shrink as it is
    // read, and a focus halfway through the grant would re-mint it.
    staleTime: ({ state }) =>
      msUntilRenewal(state.data?.expires_in, state.dataUpdatedAt, GRANT_RENEW_MARGIN_MS, state.dataUpdatedAt),
    // The interval is not a retry: after a 4xx it stops, and only the viewer's
    // own Retry asks again.
    refetchInterval: ({ state }) =>
      isClientError(state.error)
        ? false
        : Math.max(msUntilRenewal(state.data?.expires_in, state.dataUpdatedAt, GRANT_RENEW_MARGIN_MS), MIN_RENEW_INTERVAL_MS),
    gcTime: GRANT_GC_MS,
    retry: retryUnlessClientError,
  });
}
