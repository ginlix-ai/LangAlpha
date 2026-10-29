import { useMemo } from 'react';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import { useWorkspace } from '@/hooks/useWorkspace';
import { computerFolders, type ComputerFolders } from '../utils/agentPaths';
import { useComputerRoots } from './useComputers';

/**
 * The workspace's own folders and every other workspace's, for reading a path
 * that names one by its folder (`computerFolders`).
 *
 * Every workspace, not one page: a sibling missing from a truncated list would
 * leave its links dead without looking any different. Home comes along,
 * since once bound to the computer an analyst reaches it as `../Home/`. So do
 * the workspaces on the user's other computers, which the Chief of Staff
 * links at their whole path on that machine.
 *
 * The computer's root comes along too: the agent writes absolute paths under
 * it, and a computer whose user cannot create the stock root has its own. The
 * row carries it, so it is known as soon as the folders are; the computer list
 * stands in for a row read without it. So do the roots the workspace's folder
 * may have sat under before it moved: the user's other computers', and the
 * ones the row recorded leaving, whose computers may since be gone.
 */
export function useComputerFolders(workspaceId: string | null | undefined): ComputerFolders | null {
  const { data: viewed } = useWorkspace(workspaceId);
  const { data: all } = useAllWorkspaces({ includeHome: true, enabled: !!viewed?.computer_id });
  const computers = useComputerRoots({ enabled: !!viewed });
  // Keyed on the folders alone: a row refetched for its activity time or
  // status is a new object, and every rendered Markdown block rebuilds when
  // this value changes identity.
  const key = JSON.stringify(computerFolders(viewed, all?.workspaces, computers));
  return useMemo(() => JSON.parse(key) as ComputerFolders | null, [key]);
}
