/**
 * Canonical workspace-row cache patches and mutations, shared by the nav tree
 * and the gallery.
 *
 * Pin used to exist twice — the gallery's copy only invalidated, so unpinning
 * there left the sidebar frozen on the pinned-era order until a reload. One
 * implementation now owns the refetch-then-unfreeze sequence; surfaces attach
 * their own presentation side effects through `onAfterPin`.
 */
import type { QueryClient, QueryKey } from '@tanstack/react-query';
import { forgetSharedWorkspaceThreads } from '@/lib/navThreadsStore';
import { queryKeys } from '@/lib/queryKeys';
import { scrollMemory } from '@/lib/scrollMemory';
import { clearAllMarketThreadsForWorkspace } from '../../MarketView/utils/threadPersistence';
import { forgetNavPanelExpansion } from '../components/navExpansionStore';
import { updateWorkspace } from '../utils/api';
import { forgetStableNavOrder, resetWorkspaceOrderFreeze } from './useNavigationData';
import { removeStoredThreadId } from './utils/threadStorage';

// Shape of a cached workspace-list query entry (queryKeys.workspaces.lists()).
interface CachedWorkspaceList {
  workspaces: Array<Record<string, unknown> & { workspace_id: string }>;
  [key: string]: unknown;
}

export type WorkspaceQueriesSnapshot = ReturnType<QueryClient['getQueriesData']>;

/**
 * Refresh both sides of the workspace-to-computer membership projection. The
 * computer side is the list only: the storage breakdown shares the prefix and
 * costs a `du` on the machine, which membership does not change.
 */
export function invalidateWorkspaceMembership(queryClient: QueryClient): void {
  void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
  void queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() });
}

/**
 * Refresh after a workspace is created or duplicated. Besides membership, the
 * new workspace starts with every account server new workspaces start without
 * switched off, and a duplicate also copies what its source switched off,
 * built-ins included. The Plugins scope badges read those switches off both
 * the catalog and the built-in list.
 */
export function invalidateNewWorkspace(queryClient: QueryClient): void {
  invalidateWorkspaceMembership(queryClient);
  void queryClient.invalidateQueries({ queryKey: queryKeys.mcp.catalog() });
  void queryClient.invalidateQueries({ queryKey: queryKeys.mcp.builtins() });
}

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

/**
 * Take a deleted workspace out of every cached list, totals included, so no
 * surface (gallery, sidebar, drawer) offers it while the refetch is in
 * flight: a list read page by page can take a while to land.
 */
export function removeCachedWorkspace(queryClient: QueryClient, wsId: string): void {
  queryClient.setQueriesData<CachedWorkspaceList | undefined>(
    { queryKey: queryKeys.workspaces.lists() },
    (prev) => {
      if (!prev?.workspaces?.some((ws) => ws.workspace_id === wsId)) return prev;
      return {
        ...prev,
        workspaces: prev.workspaces.filter((ws) => ws.workspace_id !== wsId),
        ...(typeof prev.total === 'number' ? { total: Math.max(0, prev.total - 1) } : {}),
      };
    },
  );
}

/**
 * Everything this tab keeps about a workspace that is now deleted: stored
 * thread pointers (chat and market), remembered tree expansion, frozen nav
 * orders, the shared thread lists, scroll positions, and its row in every
 * cached list, so nothing re-expands or 404s a dead workspace while the
 * refetch the caller starts is in flight.
 */
export function forgetDeletedWorkspace(queryClient: QueryClient, wsId: string): void {
  removeStoredThreadId(wsId);
  clearAllMarketThreadsForWorkspace(wsId);
  forgetNavPanelExpansion(wsId);
  forgetStableNavOrder(wsId);
  forgetSharedWorkspaceThreads(wsId);
  scrollMemory.forget(`threads:${wsId}:active`);
  scrollMemory.forget(`threads:${wsId}:archived`);
  removeCachedWorkspace(queryClient, wsId);
}

/** A cached list read in the custom order (the gallery's under Custom, the sidebar's). */
const isCustomOrderList = (key: QueryKey) => {
  const params = key[2];
  return !!params && typeof params === 'object' && (params as { sortBy?: unknown }).sortBy === 'custom';
};

/** `rows` put in `rank`'s order, each carrying its new slot. Rows `rank` lacks keep their place at the end. */
function applyRank<T extends { workspace_id: string }>(rows: readonly T[], rank: Map<string, number>): T[] {
  const at = (ws: T) => rank.get(ws.workspace_id) ?? Number.MAX_SAFE_INTEGER;
  return rows
    .map((ws, i) => ({ ws, i }))
    .sort((a, b) => at(a.ws) - at(b.ws) || a.i - b.i)
    .map(({ ws }) => (rank.has(ws.workspace_id) ? { ...ws, sort_order: rank.get(ws.workspace_id) } : ws));
}

/**
 * Show a new custom order at once in every cached list in that order, the
 * gallery's and the sidebar's both, each row carrying its new `sort_order`,
 * so a reorder made in either view reads the same in the other before the
 * save lands. A read of those lists in flight is cancelled first, since it
 * would land the old order over the new one (a cancel puts back the list it
 * started from). Returns the snapshot `unrankCachedWorkspaces` takes back.
 */
export function rankCachedWorkspaces(
  queryClient: QueryClient,
  items: ReadonlyArray<{ workspace_id: string; sort_order: number }>,
): WorkspaceQueriesSnapshot {
  const rank = new Map(items.map((it) => [it.workspace_id, it.sort_order]));
  const custom = { queryKey: queryKeys.workspaces.lists(), predicate: (q: { queryKey: QueryKey }) => isCustomOrderList(q.queryKey) };
  void queryClient.cancelQueries(custom);
  const previous = queryClient.getQueriesData(custom);
  for (const [key, data] of previous) {
    const d = data as CachedWorkspaceList | undefined;
    if (!d?.workspaces) continue;
    queryClient.setQueryData(key as readonly unknown[], { ...d, workspaces: applyRank(d.workspaces, rank) });
  }
  return previous;
}

/**
 * Take back a rankCachedWorkspaces: each list's rows return to the order and
 * `sort_order`s the snapshot holds, on the rows the list holds now. Restoring
 * the snapshot whole would also undo a delete, create or rename that landed
 * while the save was in flight. Rows the snapshot lacks keep their place at
 * the end.
 */
export function unrankCachedWorkspaces(queryClient: QueryClient, previous: WorkspaceQueriesSnapshot): void {
  for (const [key, data] of previous) {
    const before = (data as CachedWorkspaceList | undefined)?.workspaces;
    if (!before) continue;
    const slot = new Map(before.map((ws, i) => [ws.workspace_id, { i, sort_order: ws.sort_order }]));
    const at = (ws: { workspace_id: string }) => slot.get(ws.workspace_id)?.i ?? Number.MAX_SAFE_INTEGER;
    queryClient.setQueryData<CachedWorkspaceList | undefined>(key as readonly unknown[], (now) => {
      if (!now?.workspaces) return now;
      const workspaces = now.workspaces
        .map((ws, i) => ({ ws, i }))
        .sort((a, b) => at(a.ws) - at(b.ws) || a.i - b.i)
        .map(({ ws }) => {
          const was = slot.get(ws.workspace_id);
          return was ? { ...ws, sort_order: was.sort_order } : ws;
        });
      return { ...now, workspaces };
    });
  }
}

/** Restore a snapshot captured by patchCachedWorkspace. */
export function rollbackCachedWorkspaces(queryClient: QueryClient, previous: WorkspaceQueriesSnapshot): void {
  previous.forEach(([key, data]) => queryClient.setQueryData(key as readonly unknown[], data));
}

/**
 * Optimistically patch one workspace, persist it, then invalidate so the
 * server's re-sort lands. Rolls the caches back on failure and hands the error
 * to `onError`, which is how a surface tells the user why.
 */
export async function patchWorkspaceRow(
  queryClient: QueryClient,
  wsId: string,
  patch: Record<string, unknown>,
  { onError }: { onError?: (err: unknown) => void } = {},
): Promise<boolean> {
  const previous = patchCachedWorkspace(queryClient, wsId, patch);
  try {
    await updateWorkspace(wsId, patch);
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    // The detail cache (useWorkspace → FilePanel header) carries the name, so
    // a rename must refresh it too; harmless for other patches.
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.detail(wsId) });
    // Hand-off cards head themselves with the workspace's name from their
    // liveness read, which stops polling once the run settles.
    if ('name' in patch) {
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.dispatchLivenessAll() });
    }
    return true;
  } catch (e) {
    rollbackCachedWorkspaces(queryClient, previous);
    console.warn('[workspaceRowActions] Failed to update workspace:', e);
    onError?.(e);
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
export function renameWorkspaceRow(
  queryClient: QueryClient,
  wsId: string,
  name: string,
  options: { onError?: (err: unknown) => void } = {},
): Promise<boolean> {
  return patchWorkspaceRow(queryClient, wsId, { name }, options);
}
