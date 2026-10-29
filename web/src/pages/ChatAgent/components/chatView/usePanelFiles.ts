import { useState } from 'react';
import { useWorkspaceFiles } from '../../hooks/useWorkspaceFiles';

/**
 * Which workspace's files the chat's file panel shows, and what it may do
 * with them. A view shows its own unless a link named a sibling's, which it
 * shows read-only: each agent works only in its own folder.
 *
 * `refreshOwn` is for after the agent writes, and lists the view's own files
 * even while a sibling's are on screen: the turn wrote its own, and listing
 * the sibling's would start its computer.
 */
export function usePanelFiles({ isHome, workspaceId, workspaceName = null, includeSystem }: {
  isHome: boolean;
  workspaceId: string;
  /** The view's own workspace, which the way back from a sibling is labelled with; Home's says Home. */
  workspaceName?: string | null;
  includeSystem: boolean;
}) {
  const [override, setOverride] = useState<string | null>(null);
  const shownId = override ?? workspaceId;
  const showsSibling = !!override && override !== workspaceId;
  const shown = useWorkspaceFiles(shownId, { includeSystem });
  // An @-mention is a bare path the agent reads in its own folder, so the
  // composer offers the view's own files even while the panel shows a sibling's.
  const own = useWorkspaceFiles(showsSibling ? workspaceId : null, { includeSystem });

  return {
    override,
    setOverride,
    /** The workspace the panel lists. */
    shownWorkspaceId: shownId,
    files: shown.files,
    loading: shown.loading,
    error: shown.error,
    refresh: shown.refresh,
    mentionFiles: showsSibling ? own.files : shown.files,
    refreshOwn: showsSibling ? own.refresh : shown.refresh,
    /** FilePanel's access props, spread onto every mount. */
    panelAccess: showsSibling
      ? {
          readOnly: true, singleFileMode: true, canShare: false, isHome: false,
          onReturnHome: () => setOverride(null), returnLabel: isHome ? null : workspaceName,
        }
      : {
          readOnly: false, singleFileMode: false, canShare: true, isHome,
          onReturnHome: null, returnLabel: null,
        },
  };
}
