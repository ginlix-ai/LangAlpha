import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getWorkspace } from '../pages/ChatAgent/utils/api';
import type { Workspace, WorkspacesResponse } from '../types/api';

/**
 * Shared hook for a single workspace's details.
 * Derives initialData from cached workspace lists to avoid redundant fetches.
 */
export function useWorkspace(workspaceId: string | null | undefined) {
  const queryClient = useQueryClient();

  return useQuery({
    queryKey: queryKeys.workspaces.detail(workspaceId ?? ''),
    queryFn: () => getWorkspace(workspaceId!),
    enabled: !!workspaceId,
    staleTime: 5 * 60_000,
    retry: (failureCount, error) => {
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status === 403 || status === 404) return false;
      return failureCount < 3;
    },
    initialData: (): Workspace | undefined => {
      // Try to derive from any cached workspace list
      const queries = queryClient.getQueriesData<WorkspacesResponse>({ queryKey: queryKeys.workspaces.lists() });
      for (const [, data] of queries) {
        const ws = data?.workspaces?.find((w) => w.workspace_id === workspaceId);
        if (ws) return ws;
      }
      return undefined;
    },
  });
}
