/**
 * The NavigationPanel prop bundle, assembled once for both hosts (the desktop
 * AppSidebar and ChatView's mobile drawer). The two used to wire ~18 props each
 * by hand off the same hook, so a prop added on one side silently stopped
 * existing on the other; only the genuinely per-surface pieces stay arguments.
 */
import { useCallback, useMemo, useRef } from 'react';
import { useNavigate } from 'react-router';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { useNavigationData } from './useNavigationData';
import type { NavWorkspace } from './useNavigationData';
import type { SidebarAgentRow } from '../session/subagents/subagentStatus';

/** Subagent tree slice — ChatView holds it locally, AppSidebar reads it off sidebarAgentsBridge. */
export interface NavTreeAgents {
  agents?: SidebarAgentRow[];
  activeAgentId?: string | null;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent?: (agentId: string) => void;
}

export interface UseNavTreePropsOptions {
  currentWorkspaceId?: string | null;
  currentThreadId?: string | null;
  /** Null when no ChatView is publishing a slice for this thread — the tree then renders no agent rows. */
  agents?: NavTreeAgents | null;
  /**
   * REQUIRED per surface: the hosts deliberately diverge (desktop opens the
   * workspace home, mobile opens a fresh default thread), so neither inherits
   * a default that would silently drift from the other. `ws` is the tree's
   * row for the workspace, undefined while the list does not have it.
   */
  onNewThread: (wsId: string, ws: NavWorkspace | undefined) => void;
  /** Route-state name to fall back on before the tree has loaded the workspace row. */
  fallbackWorkspaceName?: string;
}

// Stable fallback while no ChatView is publishing (agents undefined → no tree).
const noopSelectAgent = () => {};

export function useNavTreeProps({
  currentWorkspaceId,
  currentThreadId,
  agents,
  onNewThread: openNewThread,
  fallbackWorkspaceName,
}: UseNavTreePropsOptions) {
  const navigate = useNavigate();
  const {
    workspaces,
    workspaceThreads,
    expandWorkspace,
    hasMore,
    loadAll,
    loadMoreThreads,
    reorderWorkspace,
    canReorderWorkspaces,
    pinWorkspace,
    renameWorkspace,
    pinThread,
    archiveThread,
    renameThread,
    deleteThread,
  } = useNavigationData(currentWorkspaceId || '');

  const findWorkspace = useCallback(
    (wsId: string): NavWorkspace | undefined => workspaces.find((ws) => ws.workspace_id === wsId),
    [workspaces],
  );
  const label = useWorkspaceLabel();

  const onNewThread = useCallback(
    (wsId: string) => openNewThread(wsId, findWorkspace(wsId)),
    [openNewThread, findWorkspace],
  );

  // Route-state contract: the workspace name/status ride along so ChatAgent
  // skips the workspace refetch.
  const onNavigateThread = useCallback((wsId: string, threadId: string, { replace = false } = {}) => {
    const ws = findWorkspace(wsId);
    navigate(`/chat/t/${threadId}`, {
      replace,
      state: {
        workspaceId: wsId,
        workspaceName: label(ws) || fallbackWorkspaceName || '',
        workspaceStatus: ws?.status || null,
      },
    });
  }, [findWorkspace, navigate, fallbackWorkspaceName, label]);

  // Archiving or deleting the thread being viewed must not strand ChatView on
  // a row that just left the nav: mount the adjacent thread in the tree's
  // visible order (the row after, else the one before), or the workspace home
  // when it was the workspace's last one. The target is computed BEFORE the
  // await (the removal patches the row out) and navigation waits for the
  // server: a failed removal must leave the user where they are.
  // Read at call time: these handlers reach every memoized sidebar row, and
  // an identity that followed the thread data re-rendered them all on each
  // refetch. A deleted thread's entry is replaced in the history, so Back
  // never returns to it: its view stays cached and would show it as if it
  // still existed. A user who opened another thread while the removal was
  // pending stays on it.
  const workspaceThreadsRef = useRef(workspaceThreads);
  workspaceThreadsRef.current = workspaceThreads;
  const currentThreadIdRef = useRef(currentThreadId);
  currentThreadIdRef.current = currentThreadId;
  const removeThread = useCallback(async (
    wsId: string,
    threadId: string,
    remove: () => Promise<boolean>,
    { replace = false }: { replace?: boolean } = {},
  ) => {
    const isCurrent = threadId === currentThreadIdRef.current;
    let next;
    if (isCurrent) {
      const rows = workspaceThreadsRef.current[wsId]?.threads || [];
      const idx = rows.findIndex((th) => th.thread_id === threadId);
      next = idx >= 0 ? rows[idx + 1] ?? rows[idx - 1] : undefined;
    }
    const ok = await remove();
    if (!ok || !isCurrent || currentThreadIdRef.current !== threadId) return;
    if (next) {
      onNavigateThread(wsId, next.thread_id, { replace });
    } else {
      const ws = findWorkspace(wsId);
      navigate(`/chat/${wsId}`, {
        replace,
        state: {
          workspaceName: label(ws) || fallbackWorkspaceName || '',
          workspaceStatus: ws?.status || null,
        },
      });
    }
  }, [onNavigateThread, findWorkspace, navigate, fallbackWorkspaceName, label]);

  const onArchiveThread = useCallback(
    (wsId: string, threadId: string) => removeThread(wsId, threadId, () => archiveThread(wsId, threadId)),
    [removeThread, archiveThread],
  );

  // Rejects when the server refuses, for the confirm dialog to show.
  const onDeleteThread = useCallback(
    (wsId: string, threadId: string) => removeThread(wsId, threadId, async () => {
      await deleteThread(wsId, threadId);
      return true;
    }, { replace: true }),
    [removeThread, deleteThread],
  );

  return useMemo(() => ({
    workspaces,
    workspaceThreads,
    currentWorkspaceId,
    currentThreadId,
    agents: agents?.agents,
    activeAgentId: agents?.activeAgentId,
    onSelectAgent: agents?.onSelectAgent ?? noopSelectAgent,
    onRemoveAgent: agents?.onRemoveAgent,
    expandWorkspace,
    onNavigateThread,
    hasMore,
    onLoadMore: loadAll,
    onLoadMoreThreads: loadMoreThreads,
    // Withheld under activity/name order: a drop would persist a sort_order the
    // view doesn't reflect, so the affordance disables itself.
    onReorderWorkspace: canReorderWorkspaces ? reorderWorkspace : undefined,
    onPinWorkspace: pinWorkspace,
    onRenameWorkspace: renameWorkspace,
    onNewThread,
    onPinThread: pinThread,
    onArchiveThread,
    onRenameThread: renameThread,
    onDeleteThread,
  }), [
    workspaces, workspaceThreads, currentWorkspaceId, currentThreadId, agents,
    expandWorkspace, onNavigateThread, hasMore, loadAll, loadMoreThreads,
    canReorderWorkspaces, reorderWorkspace, pinWorkspace, renameWorkspace,
    onNewThread, pinThread, onArchiveThread, renameThread, onDeleteThread,
  ]);
}
