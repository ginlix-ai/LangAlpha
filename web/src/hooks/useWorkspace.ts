import { useQuery, useQueryClient, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { cachedHomeRow, workspaceDetailQuery } from '../pages/ChatAgent/utils/workspaceQueries';
import type { Workspace, WorkspacesResponse } from '../types/api';

/**
 * Shared hook for a single workspace's details.
 * Derives initialData from cached workspace lists to avoid redundant fetches.
 *
 * The return type is written out because react-query infers the wrong one: any
 * `initialData` function at all selects its "defined" overload, which promises
 * `data` is never undefined. Ours returns undefined whenever no cached list
 * holds this workspace, and the query is disabled without an id, so callers
 * genuinely have to null-check.
 */
export function useWorkspace(
  workspaceId: string | null | undefined,
): UseQueryResult<Workspace, Error> {
  const queryClient = useQueryClient();

  return useQuery({
    ...workspaceDetailQuery(workspaceId ?? ''),
    enabled: !!workspaceId,
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
    // The surfaces that open Home without a list that holds it resolve it
    // through Home's own query first, so that row names it while the detail
    // loads. Only as a placeholder: it is as of Home's first read, before a
    // start gave it a computer, a folder and a current status.
    placeholderData: () => cachedHomeRow(queryClient, workspaceId),
  });
}
