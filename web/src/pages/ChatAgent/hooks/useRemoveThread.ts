import { useCallback, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';
import { navThreadRows } from './useNavigationData';

/** The workspace's row in whichever cached list holds it, for the name and status a route carries. */
function cachedWorkspace(queryClient: QueryClient, wsId: string): Workspace | undefined {
  for (const [, data] of queryClient.getQueriesData<{ workspaces?: Workspace[] }>({ queryKey: queryKeys.workspaces.lists() })) {
    const ws = data?.workspaces?.find((row) => row.workspace_id === wsId);
    if (ws) return ws;
  }
  return undefined;
}

/**
 * Opening a thread, and removing one (archive, delete) from a surface that
 * may be showing it, the same from the sidebar and the command palette.
 * Removing the thread on screen must not strand ChatView on a row that just
 * left the lists: it opens the thread after it in the nav tree's order, else
 * the one before, or the workspace home when it was the workspace's last.
 * The target is read BEFORE the removal (which patches the row out), and the
 * move waits for the server: a failed removal leaves the user where they are,
 * as does having opened another thread meanwhile. Read at call time, so the
 * callbacks keep one identity while the thread data changes.
 */
export function useRemoveThread(currentThreadId: string | null | undefined, fallbackWorkspaceName = '') {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const label = useWorkspaceLabel();
  // Read once the server answers, when the reader may have moved on.
  const currentRef = useRef(currentThreadId);
  useEffect(() => {
    currentRef.current = currentThreadId;
  }, [currentThreadId]);

  // Route-state contract: the workspace name/status ride along so ChatAgent
  // skips the workspace refetch.
  const routeState = useCallback((wsId: string) => {
    const ws = cachedWorkspace(queryClient, wsId);
    return { workspaceName: label(ws) || fallbackWorkspaceName, workspaceStatus: ws?.status || null };
  }, [queryClient, label, fallbackWorkspaceName]);

  const openThread = useCallback((wsId: string, threadId: string, { replace = false } = {}) => {
    // A second history entry for the thread on screen would make Back look dead.
    if (threadId === currentRef.current) return;
    navigate(`/chat/t/${threadId}`, { replace, state: { workspaceId: wsId, ...routeState(wsId) } });
  }, [navigate, routeState]);

  /**
   * `remove` resolves whether the server took it (or rejects, which reaches
   * the caller). A deleted thread's entry is replaced in the history
   * (`replace`), so Back never returns to it: its view stays cached and would
   * show it as if it still existed.
   */
  const removeThread = useCallback(async (
    wsId: string,
    threadId: string,
    remove: () => Promise<boolean>,
    { replace = false }: { replace?: boolean } = {},
  ) => {
    const isCurrent = threadId === currentRef.current;
    let next;
    if (isCurrent) {
      const rows = navThreadRows(queryClient, wsId);
      const idx = rows.findIndex((th) => th.thread_id === threadId);
      next = idx >= 0 ? rows[idx + 1] ?? rows[idx - 1] : undefined;
    }
    const ok = await remove();
    if (!ok || !isCurrent || currentRef.current !== threadId) return;
    if (next) openThread(wsId, next.thread_id, { replace });
    else navigate(`/chat/${wsId}`, { replace, state: routeState(wsId) });
  }, [queryClient, openThread, navigate, routeState]);

  return { openThread, removeThread };
}
