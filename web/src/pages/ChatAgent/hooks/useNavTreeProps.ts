/**
 * The NavigationPanel prop bundle, assembled once for both hosts (the desktop
 * AppSidebar and ChatView's mobile drawer). The two used to wire ~18 props each
 * by hand off the same hook, so a prop added on one side silently stopped
 * existing on the other; only the genuinely per-surface pieces stay arguments.
 */
import { useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
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
   * a default that would silently drift from the other.
   */
  onNewThread: (wsId: string) => void;
  /** False parks the whole data layer for a drawer that is mounted but never shown (desktop ChatViews). */
  enabled?: boolean;
  /** Route-state name to fall back on before the tree has loaded the workspace row. */
  fallbackWorkspaceName?: string;
}

// Stable fallback while no ChatView is publishing (agents undefined → no tree).
const noopSelectAgent = () => {};

export function useNavTreeProps({
  currentWorkspaceId,
  currentThreadId,
  agents,
  onNewThread,
  enabled = true,
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
  } = useNavigationData(currentWorkspaceId || '', { enabled });

  const findWorkspace = useCallback(
    (wsId: string): NavWorkspace | undefined => workspaces.find((ws) => ws.workspace_id === wsId),
    [workspaces],
  );

  // Route-state contract: the workspace name/status ride along so ChatAgent
  // skips the workspace refetch, and flash workspaces carry their agent mode.
  const onNavigateThread = useCallback((wsId: string, threadId: string) => {
    const ws = findWorkspace(wsId);
    navigate(`/chat/t/${threadId}`, {
      state: {
        workspaceId: wsId,
        workspaceName: ws?.name || fallbackWorkspaceName || '',
        workspaceStatus: ws?.status || null,
        ...(ws?.status === 'flash' ? { agentMode: 'flash' } : {}),
      },
    });
  }, [findWorkspace, navigate, fallbackWorkspaceName]);

  // Archiving the thread being viewed must not strand ChatView on a row that
  // just left the nav: mount the adjacent thread in the tree's visible order
  // (the row after, else the one before), or the workspace home when it was
  // the workspace's last one. The target is computed BEFORE the await (the
  // optimistic patch removes the row) and navigation waits for the server —
  // a failed archive rolls back and must leave the user where they are.
  const onArchiveThread = useCallback(async (wsId: string, threadId: string) => {
    const isCurrent = threadId === currentThreadId;
    let next;
    if (isCurrent) {
      const rows = workspaceThreads[wsId]?.threads || [];
      const idx = rows.findIndex((th) => th.thread_id === threadId);
      next = idx >= 0 ? rows[idx + 1] ?? rows[idx - 1] : undefined;
    }
    const ok = await archiveThread(wsId, threadId);
    if (!ok || !isCurrent) return;
    if (next) {
      onNavigateThread(wsId, next.thread_id);
    } else {
      const ws = findWorkspace(wsId);
      navigate(`/chat/${wsId}`, {
        state: {
          workspaceName: ws?.name || fallbackWorkspaceName || '',
          workspaceStatus: ws?.status || null,
        },
      });
    }
  }, [currentThreadId, workspaceThreads, onNavigateThread, findWorkspace, navigate, fallbackWorkspaceName, archiveThread]);

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
  }), [
    workspaces, workspaceThreads, currentWorkspaceId, currentThreadId, agents,
    expandWorkspace, onNavigateThread, hasMore, loadAll, loadMoreThreads,
    canReorderWorkspaces, reorderWorkspace, pinWorkspace, renameWorkspace,
    onNewThread, pinThread, onArchiveThread,
  ]);
}
