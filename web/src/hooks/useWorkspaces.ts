import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getWorkspaces } from '../pages/ChatAgent/utils/api';

interface UseWorkspacesOptions {
  limit?: number;
  offset?: number;
  sortBy?: string;
  includeFlash?: boolean;
  enabled?: boolean;
  /** 'always' forces a fetch on mount even inside the staleTime window, for a
   *  consumer that has to decide something against the current list. */
  refetchOnMount?: boolean | 'always';
}

/**
 * Shared hook for workspace list queries.
 * Uses keepPreviousData for smooth pagination transitions.
 * All consumers with the same params share one cached entry.
 */
export function useWorkspaces({ limit = 20, offset = 0, sortBy = 'custom', includeFlash = false, enabled = true, refetchOnMount }: UseWorkspacesOptions = {}) {
  const params = { limit, offset, sortBy, includeFlash };
  return useQuery({
    queryKey: queryKeys.workspaces.list(params),
    queryFn: () => getWorkspaces(limit, offset, sortBy, includeFlash),
    enabled,
    staleTime: 30_000,
    refetchOnMount,
    placeholderData: keepPreviousData,
  });
}
