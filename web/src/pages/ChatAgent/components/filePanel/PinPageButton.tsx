import React from 'react';
import { usePinPageAsDashboard } from '@/lib/dashboards';
import { useWorkspaceFolders } from '../../contexts/WorkspaceContext';
import { PinDashboardButton } from './PinDashboardButton';

interface PinPageButtonProps {
  workspaceId: string;
  path: string;
  /** The thread beside the panel, where a dashboard set from here is edited. */
  threadId: string | null | undefined;
}

/**
 * Set the HTML file in front as the dashboard, beside its comment tools. The
 * page is kept on the server, so the dashboard opens with this computer off.
 */
export function PinPageButton({ workspaceId, path, threadId }: PinPageButtonProps): React.ReactElement {
  const root = useWorkspaceFolders()?.rootDir;
  const { pinned, pending, pinIt } = usePinPageAsDashboard(path, { workspaceId, threadId }, root);
  return <PinDashboardButton pinned={pinned} pending={pending} onClick={pinIt} />;
}
