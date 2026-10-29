import { useWorkspaces } from '@/hooks/useWorkspaces';

export interface WorkspaceOption {
  workspace_id: string;
  name: string;
}

const EMPTY: WorkspaceOption[] = [];

/** The workspaces an automation can run in, from the one cached list the
 *  form and the inspector both read, so a name never differs between them. */
export function useWorkspaceOptions(): WorkspaceOption[] {
  const { data } = useWorkspaces({ limit: 100 });
  return data?.workspaces ?? EMPTY;
}

export function workspaceNameOf(workspaces: WorkspaceOption[], id: string | null | undefined): string | undefined {
  return id ? workspaces.find((w) => w.workspace_id === id)?.name : undefined;
}
