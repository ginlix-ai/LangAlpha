/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useMemo, type ReactNode } from 'react';
import type { ComputerFolders } from '../utils/agentPaths';

interface WorkspaceContextValue {
  workspaceId: string | null;
  downloadFile: ((path: string) => void) | null;
  /** The workspace's folders and its siblings', for links that name a sibling by folder. */
  folders: ComputerFolders | null;
}

const WorkspaceContext = createContext<WorkspaceContextValue>({ workspaceId: null, downloadFile: null, folders: null });

interface WorkspaceProviderProps {
  workspaceId: string | null;
  downloadFile: ((path: string) => void) | null;
  folders?: ComputerFolders | null;
  children: ReactNode;
}

export const WorkspaceProvider = ({ workspaceId, downloadFile, folders = null, children }: WorkspaceProviderProps) => {
  const value = useMemo(() => ({ workspaceId, downloadFile, folders }), [workspaceId, downloadFile, folders]);
  return <WorkspaceContext value={value}>{children}</WorkspaceContext>;
};

export const useWorkspaceId = () => useContext(WorkspaceContext).workspaceId;
export const useWorkspaceDownloadFile = () => useContext(WorkspaceContext).downloadFile;
export const useWorkspaceFolders = () => useContext(WorkspaceContext).folders;
