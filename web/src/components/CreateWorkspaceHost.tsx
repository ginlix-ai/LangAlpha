import { Suspense, createContext, lazy, useCallback, useContext, useState, type ReactNode } from 'react';
import { useNavigate } from 'react-router';
import { useCreateWorkspace } from '@/pages/ChatAgent/hooks/useCreateWorkspace';
import type { Workspace } from '@/types/api';

const CreateWorkspaceModal = lazy(() => import('@/pages/ChatAgent/components/CreateWorkspaceModal'));

const OpenCreateWorkspace = createContext<(() => void) | null>(null);

/**
 * The app shell's "New workspace" dialog, which opens the workspace once made.
 * One for every surface that offers it (the sidebar, the command palette), and
 * held above them, since the palette closes as the dialog opens.
 */
export function CreateWorkspaceHost({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  const createWorkspace = useCreateWorkspace();
  const [open, setOpen] = useState(false);
  const openDialog = useCallback(() => setOpen(true), []);
  const openCreated = useCallback(
    (ws: Workspace) => navigate(`/chat/${ws.workspace_id}`, { state: { workspaceName: ws.name, workspaceStatus: null } }),
    [navigate],
  );

  return (
    <OpenCreateWorkspace.Provider value={openDialog}>
      {children}
      {open && (
        <Suspense fallback={null}>
          <CreateWorkspaceModal isOpen onClose={() => setOpen(false)} onCreate={createWorkspace} onComplete={openCreated} />
        </Suspense>
      )}
    </OpenCreateWorkspace.Provider>
  );
}

/** Opens the "New workspace" dialog; null outside the app shell. */
// eslint-disable-next-line react-refresh/only-export-components
export function useOpenCreateWorkspace(): (() => void) | null {
  return useContext(OpenCreateWorkspace);
}
