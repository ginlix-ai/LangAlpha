import type { QueryClient } from '@tanstack/react-query';
import { markThreadDeleted } from '@/lib/deletedThreads';
import { queryKeys } from '@/lib/queryKeys';
import { scrollMemory } from '@/lib/scrollMemory';
import { patchThreadRows } from '@/lib/threadRowActions';
import { deleteThread } from '../utils/api';
import { getStoredThreadId, removeStoredThreadId } from './utils/threadStorage';

/**
 * Delete a thread and forget it everywhere the client kept it: the scroll
 * position, the workspace's remembered thread, every cached thread list (the
 * gallery's pages, the sidebar's, the dashboard's recent threads), and its
 * cached chat view and history entries (lib/deletedThreads). Throws
 * when the server refuses, leaving all of it in place.
 */
export async function deleteThreadRow(
  queryClient: QueryClient,
  workspaceId: string,
  threadId: string,
): Promise<void> {
  await deleteThread(threadId);
  markThreadDeleted(threadId);
  scrollMemory.forget(`thread:${threadId}`);
  if (workspaceId && getStoredThreadId(workspaceId) === threadId) removeStoredThreadId(workspaceId);
  patchThreadRows(queryClient, queryKeys.threads.all, (rows) =>
    rows.some((th) => th.thread_id === threadId) ? rows.filter((th) => th.thread_id !== threadId) : rows,
  );
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.recentAll() });
}
