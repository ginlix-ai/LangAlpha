/**
 * The workspace detail query, defined once so the hook that renders a
 * workspace and the warm path that writes its status agree on the fetch and
 * the type of what the cache holds.
 */
import { queryOptions, type QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';
import { getWorkspace } from './api';

export function workspaceDetailQuery(workspaceId: string) {
  return queryOptions({
    queryKey: queryKeys.workspaces.detail(workspaceId),
    queryFn: () => getWorkspace(workspaceId),
    staleTime: 5 * 60_000,
  });
}

/** Home's row as Home's own query holds it, when `workspaceId` is Home. Its
 *  id never changes, so this answers whose row it is; read the rest from the
 *  detail. */
export function cachedHomeRow(queryClient: QueryClient, workspaceId: string | null | undefined): Workspace | undefined {
  const home = queryClient.getQueryData<Workspace>(queryKeys.workspaces.home());
  return workspaceId && home?.workspace_id === workspaceId ? home : undefined;
}
