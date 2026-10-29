import { useEffect } from 'react';
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { authSessionCheck } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import { patchThreadShared } from '@/lib/threadListCache';
import type { ThreadShareStatus } from '@/types/api';
import { updateThreadSharing } from '../utils/api';
import { threadShareQuery } from '../utils/threadQueries';

/**
 * A thread's public-link status, the one entry the chat header and every share
 * panel read, so a change from either shows on both. The cached thread lists
 * follow it, for the gallery's public-link badge.
 */
export function useThreadShareStatus(threadId: string, { refetchOnMount }: { refetchOnMount?: boolean | 'always' } = {}) {
  const queryClient = useQueryClient();
  const query = useQuery({ ...threadShareQuery(threadId), enabled: !!threadId, refetchOnMount });
  const isShared = query.data?.is_shared;
  useEffect(() => {
    if (isShared !== undefined) patchThreadShared(queryClient, threadId, isShared === true);
  }, [queryClient, threadId, isShared]);
  return query;
}

/**
 * Change a thread's sharing and make the answer the cached status. A read
 * still in flight started before the change, so it is cancelled rather than
 * let land over it; an answer that arrives after a sign-out is dropped.
 */
export async function saveThreadSharing(
  queryClient: QueryClient,
  threadId: string,
  body: Pick<ThreadShareStatus, 'is_shared' | 'permissions'>,
): Promise<void> {
  const sameSession = authSessionCheck();
  const result: ThreadShareStatus = await updateThreadSharing(threadId, body);
  if (!sameSession()) return;
  const queryKey = queryKeys.threads.share(threadId);
  await queryClient.cancelQueries({ queryKey });
  queryClient.setQueryData(queryKey, result);
}
