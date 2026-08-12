/**
 * Canonical workspace-row cache patches and mutations, shared by the nav tree
 * and the gallery.
 *
 * Pin used to exist twice — the gallery's copy only invalidated, so unpinning
 * there left the sidebar frozen on the pinned-era order until a reload. One
 * implementation now owns the refetch-then-unfreeze sequence; surfaces attach
 * their own presentation side effects through `onAfterPin`.
 */
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { updateWorkspace } from '../utils/api';
import { resetWorkspaceOrderFreeze } from './useNavigationData';

// Shape of a cached workspace-list query entry (queryKeys.workspaces.lists()).
interface CachedWorkspaceList {
  workspaces: Array<Record<string, unknown> & { workspace_id: string }>;
  [key: string]: unknown;
}

export type WorkspaceQueriesSnapshot = ReturnType<QueryClient['getQueriesData']>;

/**
 * Optimistically patch one workspace across every cached list. Returns the
 * snapshot so the caller can roll back on error.
 */
export function patchCachedWorkspace(
  queryClient: QueryClient,
  wsId: string,
  patch: Record<string, unknown>,
): WorkspaceQueriesSnapshot {
  const previous = queryClient.getQueriesData({ queryKey: queryKeys.workspaces.lists() });
  previous.forEach(([key, data]) => {
    const d = data as CachedWorkspaceList | undefined;
    if (!d?.workspaces) return;
    queryClient.setQueryData(key as readonly unknown[], {
      ...d,
      workspaces: d.workspaces.map((ws) => (ws.workspace_id === wsId ? { ...ws, ...patch } : ws)),
    });
  });
  return previous;
}

/** Restore a snapshot captured by patchCachedWorkspace. */
export function rollbackCachedWorkspaces(queryClient: QueryClient, previous: WorkspaceQueriesSnapshot): void {
  previous.forEach(([key, data]) => queryClient.setQueryData(key as readonly unknown[], data));
}

/**
 * Optimistically patch one workspace, persist it, then invalidate so the
 * server's re-sort lands. Rolls the caches back on failure.
 */
export async function patchWorkspaceRow(
  queryClient: QueryClient,
  wsId: string,
  patch: Record<string, unknown>,
): Promise<boolean> {
  const previous = patchCachedWorkspace(queryClient, wsId, patch);
  try {
    await updateWorkspace(wsId, patch);
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    // The detail cache (useWorkspace → FilePanel header) carries the name, so
    // a rename must refresh it too; harmless for other patches.
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.detail(wsId) });
    return true;
  } catch (e) {
    rollbackCachedWorkspaces(queryClient, previous);
    console.warn('[workspaceRowActions] Failed to update workspace:', e);
    return false;
  }
}

/**
 * Pin/unpin a workspace. The list must be refetched BEFORE the nav's session
 * freeze is released: the optimistic patch itself trips the freeze's
 * arrangement-change detection, so releasing it while the cache is merely
 * stale re-snapshots the PRE-sort order and an unpinned row keeps squatting at
 * the top for the session. refetchQueries (not invalidate) so cached lists
 * without an observer really refetch.
 */
export async function pinWorkspaceRow(
  queryClient: QueryClient,
  wsId: string,
  pinned: boolean,
  { onAfterPin }: { onAfterPin?: () => void } = {},
): Promise<void> {
  const previous = patchCachedWorkspace(queryClient, wsId, { is_pinned: pinned });
  try {
    await updateWorkspace(wsId, { is_pinned: pinned });
    await queryClient.refetchQueries({ queryKey: queryKeys.workspaces.lists() });
    resetWorkspaceOrderFreeze();
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.detail(wsId) });
    onAfterPin?.();
  } catch (e) {
    rollbackCachedWorkspaces(queryClient, previous);
    console.warn('[workspaceRowActions] Failed to toggle workspace pin:', e);
  }
}

/** Rename a workspace. A blank/unchanged name is the caller's guard. */
export function renameWorkspaceRow(queryClient: QueryClient, wsId: string, name: string): Promise<boolean> {
  return patchWorkspaceRow(queryClient, wsId, { name });
}
