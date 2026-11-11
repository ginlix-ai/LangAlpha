/**
 * The NavigationPanel prop bundle, assembled once for both hosts (the desktop
 * AppSidebar and ChatView's mobile drawer). The two used to wire ~18 props each
 * by hand off the same hook, so a prop added on one side silently stopped
 * existing on the other; only the genuinely per-surface pieces stay arguments.
 */
import { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { deleteThreadRow } from './deleteThreadRow';
import { archiveThreadRow, pinThreadRow, renameThreadRow } from './threadRowMutations';
import { useNavigationData } from './useNavigationData';
import type { NavWorkspace } from './useNavigationData';
import { useRemoveThread } from './useRemoveThread';
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
  const queryClient = useQueryClient();
  const { t } = useTranslation();
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
  } = useNavigationData(currentWorkspaceId || '');
  const { openThread: onNavigateThread, removeThread } = useRemoveThread(currentThreadId, fallbackWorkspaceName);

  const findWorkspace = useCallback(
    (wsId: string): NavWorkspace | undefined => workspaces.find((ws) => ws.workspace_id === wsId),
    [workspaces],
  );

  const onNewThread = useCallback(
    (wsId: string) => openNewThread(wsId, findWorkspace(wsId)),
    [openNewThread, findWorkspace],
  );

  // The pinned-first partition repositions the row from the optimistic
  // is_pinned patch alone.
  const onPinThread = useCallback(
    (wsId: string, threadId: string, pinned: boolean) => pinThreadRow(queryClient, wsId, threadId, pinned),
    [queryClient],
  );

  const onArchiveThread = useCallback(
    (wsId: string, threadId: string) => removeThread(wsId, threadId, () => archiveThreadRow(queryClient, wsId, threadId)),
    [removeThread, queryClient],
  );

  const onRenameThread = useCallback(
    (wsId: string, threadId: string, title: string, previousTitle: string) =>
      renameThreadRow(queryClient, wsId, threadId, title, previousTitle, t),
    [queryClient, t],
  );

  // Rejects when the server refuses, for the confirm dialog to show.
  const onDeleteThread = useCallback(
    (wsId: string, threadId: string) => removeThread(wsId, threadId, async () => {
      await deleteThreadRow(queryClient, wsId, threadId);
      return true;
    }, { replace: true }),
    [removeThread, queryClient],
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
    onPinThread,
    onArchiveThread,
    onRenameThread,
    onDeleteThread,
  }), [
    workspaces, workspaceThreads, currentWorkspaceId, currentThreadId, agents,
    expandWorkspace, onNavigateThread, hasMore, loadAll, loadMoreThreads,
    canReorderWorkspaces, reorderWorkspace, pinWorkspace, renameWorkspace,
    onNewThread, onPinThread, onArchiveThread, onRenameThread, onDeleteThread,
  ]);
}
