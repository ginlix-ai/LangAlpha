import { useCallback, useMemo, useRef } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import { listWorkspaceFiles } from '../utils/api';
import { apiErrorDetail, apiErrorStatus } from '@/api/errors';

interface UseWorkspaceFilesOptions {
  includeSystem?: boolean;
}

interface UseWorkspaceFilesResult {
  files: string[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

interface RefreshSlot {
  again: boolean;
  done: Promise<void>;
}

// Stable fallback — a fresh [] per render would churn the identity of every
// consumer's `files` prop while the query has no data yet.
const NO_FILES: string[] = [];

function filesErrorText(error: unknown): string {
  const status = apiErrorStatus(error);
  if (status === 503) return 'Sandbox not available';
  // An owner's SSH machine that answered and refused: asking again gets the
  // same answer, and the server's sentence says what to fix on the machine.
  const detail = apiErrorDetail(error);
  if (status === 422 && typeof detail === 'string' && detail) return detail;
  return 'Failed to load files';
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

  // File pings arrive in bursts (attaching to a live subagent replays one per
  // write in a single tick), so a refresh asked for mid-fetch joins one
  // trailing fetch instead of starting its own; that fetch begins after the
  // last request, so it still sees the last write.
  const inFlight = useRef(new Map<string, RefreshSlot>());

  const refresh = useCallback((): Promise<void> => {
    if (!workspaceId) return Promise.resolve();
    const key = JSON.stringify([workspaceId, includeSystem]);
    const running = inFlight.current.get(key);
    if (running) {
      running.again = true;
      return running.done;
    }
    const slot: RefreshSlot = { again: false, done: Promise.resolve() };
    inFlight.current.set(key, slot);
    slot.done = (async () => {
      try {
        do {
          slot.again = false;
          try {
            const data = await listWorkspaceFiles(workspaceId, '.', { autoStart: true, includeSystem });
            queryClient.setQueryData(queryKeys.workspaceFiles.byWs(workspaceId, { includeSystem }), data);
          } catch {
            // Awaited so the refetch this starts cannot land after, and
            // overwrite, the trailing fetch's newer list.
            await queryClient.invalidateQueries({ queryKey: queryKeys.workspaceFiles.byWs(workspaceId, { includeSystem }) });
          }
        } while (slot.again);
      } finally {
        inFlight.current.delete(key);
      }
    })();
    return slot.done;
  }, [queryClient, workspaceId, includeSystem]);

  return {
    files: data?.files || NO_FILES,
    loading: isLoading,
    error: error ? filesErrorText(error) : null,
    refresh,
  };
}
