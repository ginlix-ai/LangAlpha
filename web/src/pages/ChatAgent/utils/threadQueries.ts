/**
 * Thread queries read from more than one surface, each defined once so every
 * reader of a key fetches it the same way and agrees on how fresh it is.
 */
import { queryOptions } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { ThreadsResponse } from '@/types/api';
import { getThread, getThreadShareStatus, getWorkspaceThreads } from './api';

/**
 * One thread, read to learn its workspace on a direct link. No retry: the chat
 * route shows its access-denied state from this query's error, which a retry
 * would hold back.
 */
export function threadDetailQuery(threadId: string) {
  return queryOptions({
    queryKey: queryKeys.threads.detail(threadId),
    queryFn: () => getThread(threadId),
    retry: false,
  });
}

/** A workspace's newest `limit` threads, the list the sidebar and dashboard read. */
export function workspaceThreadsQuery(workspaceId: string, limit: number) {
  return queryOptions({
    queryKey: queryKeys.threads.page(workspaceId, limit),
    queryFn: () => getWorkspaceThreads(workspaceId, limit, 0) as Promise<ThreadsResponse>,
    staleTime: 30_000,
  });
}

/**
 * A thread's public-link status. Fresh for 30s, so each view's header reads it
 * once; a share panel reads it again as it opens.
 */
export function threadShareQuery(threadId: string) {
  return queryOptions({
    queryKey: queryKeys.threads.share(threadId),
    queryFn: () => getThreadShareStatus(threadId),
    staleTime: 30_000,
  });
}
