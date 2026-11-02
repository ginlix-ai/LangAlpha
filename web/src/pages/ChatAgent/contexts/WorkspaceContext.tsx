/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useMemo, type ReactNode } from 'react';
import type { ComputerFolders } from '../utils/agentPaths';

interface WorkspaceContextValue {
  workspaceId: string | null;
  downloadFile: ((path: string) => void) | null;
  /** The workspace's folders and its siblings', for links that name a sibling by folder. */
  folders: ComputerFolders | null;
  /** Whose images these are on a page with no workspace, where the same path is another image under another link or version. */
  imageScope: string | null;
}

const WorkspaceContext = createContext<WorkspaceContextValue>({ workspaceId: null, downloadFile: null, folders: null, imageScope: null });

interface WorkspaceProviderProps {
  workspaceId: string | null;
  downloadFile: ((path: string) => void) | null;
  folders?: ComputerFolders | null;
  imageScope?: string | null;
  children: ReactNode;
}

export const WorkspaceProvider = ({ workspaceId, downloadFile, folders = null, imageScope = null, children }: WorkspaceProviderProps) => {
  const value = useMemo(() => ({ workspaceId, downloadFile, folders, imageScope }), [workspaceId, downloadFile, folders, imageScope]);
  return <WorkspaceContext value={value}>{children}</WorkspaceContext>;
};

export const useWorkspaceId = () => useContext(WorkspaceContext).workspaceId;
export const useWorkspaceDownloadFile = () => useContext(WorkspaceContext).downloadFile;
export const useWorkspaceFolders = () => useContext(WorkspaceContext).folders;
export const useWorkspaceImageScope = () => useContext(WorkspaceContext).imageScope;
