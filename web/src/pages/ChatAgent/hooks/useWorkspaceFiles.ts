import { useCallback, useMemo } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import { listWorkspaceFiles } from '../utils/api';

interface UseWorkspaceFilesOptions {
  includeSystem?: boolean;
}

interface UseWorkspaceFilesResult {
  files: string[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useWorkspaceFiles(
  workspaceId: string | null,
  { includeSystem = false }: UseWorkspaceFilesOptions = {},
): UseWorkspaceFilesResult {
  const queryClient = useQueryClient();
  const opts = useMemo(() => ({ includeSystem }), [includeSystem]);

  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.workspaceFiles.byWs(workspaceId!, opts),
    queryFn: () => listWorkspaceFiles(workspaceId!, '.', { autoStart: false, includeSystem }),
    enabled: !!workspaceId,
    retry: (count, err: { response?: { status?: number } }) =>
      count < 3 && [500, 503].includes(err?.response?.status ?? 0),
    retryDelay: (attempt: number) => (attempt + 1) * 1000,
    staleTime: 30_000,
  });

  const refresh = useCallback(async () => {
    if (!workspaceId) return;
    try {
      const data = await listWorkspaceFiles(workspaceId, '.', { autoStart: true, includeSystem });
      queryClient.setQueryData(queryKeys.workspaceFiles.byWs(workspaceId, { includeSystem }), data);
    } catch {
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaceFiles.byWs(workspaceId, { includeSystem }) });
    }
  }, [queryClient, workspaceId, includeSystem]);

  return {
    files: data?.files || [],
    loading: isLoading,
    error: error
      ? ((error as { response?: { status?: number } }).response?.status === 503
        ? 'Sandbox not available'
        : 'Failed to load files')
      : null,
    refresh,
  };
}
