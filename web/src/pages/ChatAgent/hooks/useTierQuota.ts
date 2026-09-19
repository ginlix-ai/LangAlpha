/**
 * Per-tier count quotas for the tier pickers.
 *
 * Both pickers read the workspace quota endpoint, including the one that
 * creates a computer: the backend still meters elevated tiers per workspace
 * (`check_quota="workspace"`), so this endpoint is the count that decides a
 * machine's tier too, and both surfaces have to spend from the same figure.
 */
import { useQuery } from '@tanstack/react-query';

import { isPlatformMode } from '@/config/hostMode';
import { queryKeys } from '@/lib/queryKeys';

import { getWorkspaceQuota } from '../utils/api';

/** `enabled` is the caller's own gate: fetch when a picker is actually open. */
export function useTierQuota(options: { enabled: boolean }) {
  return useQuery({
    queryKey: queryKeys.workspaces.quota(),
    queryFn: getWorkspaceQuota,
    enabled: isPlatformMode && options.enabled,
    staleTime: 60_000,
  });
}
