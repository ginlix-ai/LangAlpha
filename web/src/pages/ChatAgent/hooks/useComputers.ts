/**
 * Computer list query and the status fan-out.
 *
 * A machine is what actually starts and stops; its workspaces are folders on
 * it. So a surface showing many workspaces subscribes once per machine and
 * fans the transition out, instead of opening one connection per project to
 * hear the same news several times.
 */
import { useCallback, useEffect, useMemo, useSyncExternalStore } from 'react';

import { QueryClient, useQuery, useQueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { ComputersResponse } from '@/types/api';

import {
  getComputers,
  streamComputerEvents,
  streamWorkspaceEvents,
} from '../utils/api';
import {
  cachedWorkspaceLists,
  patchWorkspaceStatusInCaches,
} from '../utils/warmWorkspace';
import {
  isComputerStatusTerminal,
  isComputerStatusTransitional,
} from '../components/computerStatusUi';

/**
 * Reconnect pacing for a status stream that closed mid-transition. The first
 * retry is quick because the common cause is the server's own stream cap; the
 * cap keeps a flapping link from hammering the origin, and the count bounds a
 * machine that never leaves 'starting' to a few minutes of listening.
 */
export const STATUS_RECONNECT = { baseMs: 500, capMs: 15_000, maxAttempts: 20 };

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      signal.removeEventListener('abort', done);
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener('abort', done, { once: true });
  });
}

/** The machine's status as the list cache holds it right now. */
function cachedComputerStatus(queryClient: QueryClient, computerId: string): string | undefined {
  for (const [, data] of queryClient.getQueriesData<ComputersResponse | undefined>({
    queryKey: queryKeys.computers.lists(),
  })) {
    const row = data?.computers?.find((c) => c.computer_id === computerId);
    if (row) return row.status;
  }
  return undefined;
}

/** Shared computer list. Every consumer with this key reads one cached entry. */
export function useComputers(options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: queryKeys.computers.lists(),
    queryFn: getComputers,
    enabled: options.enabled ?? true,
    staleTime: 30_000,
  });
}

/**
 * Write `status` into every cached computer list.
 * Exported because the action handlers reflect their own 202 through it, which
 * is also what arms the status stream for the transition that follows.
 */
export function patchComputerStatusInCaches(
  queryClient: QueryClient,
  computerId: string,
  status: string,
): void {
  queryClient.setQueriesData<ComputersResponse | undefined>(
    { queryKey: queryKeys.computers.lists() },
    (prev) => {
      if (!prev?.computers) return prev;
      return {
        ...prev,
        computers: prev.computers.map((c) =>
          c.computer_id === computerId ? { ...c, status } : c,
        ),
      };
    },
  );
}

/**
 * Subscribe to every machine in flight and mirror each transition onto the
 * machine row and onto every workspace that lives on it.
 *
 * Mount this once above every surface that can start or stop a machine, not on
 * the surface that happens to hold a workspace list: arming the stream is a
 * cache write any of them can make, so a page where the write has no watcher
 * leaves a row saying "Starting" forever. It takes no arguments for the same
 * reason - the watch set is read from the caches, so it is the same set
 * wherever it mounts.
 *
 * Only transitional machines get a stream. A machine at rest changes state only
 * because someone acts, and the action writes 'starting' into the cache itself,
 * which is what brings it into this set, so waiting on a resting machine would
 * hold a connection open for news that cannot arrive. That matters more than it
 * sounds: the dev proxy speaks HTTP/1.1, whose six connections per origin a
 * gallery of resting machines would spend entirely.
 *
 * A machine with no workspaces yet is watched too. It has nothing to fan out
 * to, but it is exactly the row a user just pressed Start on in the computers
 * list, and that row is the one that has to stop saying "Starting".
 *
 * Workspaces with no `computer_id` (flash, and rows that predate the split)
 * keep the per-workspace channel as their fallback, since no machine will
 * speak for them.
 */
export function useComputerStatusFanout(): void {
  const queryClient = useQueryClient();
  const { data: computerData } = useComputers();

  // Only the ids, joined, so the effect re-runs when the *set* of watched
  // machines changes rather than on every list refetch that returns equal rows.
  const watchedComputerKey = useMemo(
    () =>
      (computerData?.computers ?? [])
        .filter((c) => isComputerStatusTransitional(c.status))
        .map((c) => c.computer_id)
        .sort()
        .join(','),
    [computerData],
  );

  const watchedWorkspaceKey = useUnboundTransitionalWorkspaceKey();

  useEffect(() => {
    if (!watchedComputerKey) return;
    const controller = new AbortController();

    for (const computerId of watchedComputerKey.split(',')) {
      void (async () => {
        // A stream that closes mid-transition is reopened, because the watch
        // key only changes when the *set* of moving machines does: a refetch
        // that still says 'starting' leaves this effect exactly where it was,
        // so nothing else would ever listen again.
        for (let attempt = 0; !controller.signal.aborted; attempt++) {
          // The last status this stream delivered. The close below has to tell
          // a transition that ended on the wire from one that ended without us,
          // and the stream is the only witness: nothing caches a machine on its own.
          let lastStatus: string | undefined;
          await streamComputerEvents(
            computerId,
            (status) => {
              lastStatus = status;
              patchComputerStatusInCaches(queryClient, computerId, status);
              // Read the grouping from the live cache, not the closure: a
              // workspace created while the machine was booting has to inherit
              // the transition too.
              const onThisMachine = new Set(
                cachedWorkspaceLists(queryClient)
                  .filter((ws) => ws.computer_id === computerId && ws.workspace_id)
                  .map((ws) => ws.workspace_id),
              );
              for (const workspaceId of onThisMachine) {
                patchWorkspaceStatusInCaches(queryClient, workspaceId, status);
              }
            },
            controller.signal,
          );
          if (controller.signal.aborted) return;
          // On a terminal status the stream said so. Otherwise it hit the
          // server's 600 s cap or a dropped link, and the cache may now be
          // holding a transition that finished without us: reconcile, and if
          // the machine is still moving, listen again.
          if (isComputerStatusTerminal(lastStatus)) return;
          await Promise.all([
            queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() }),
            queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() }),
          ]);
          if (controller.signal.aborted) return;
          if (!isComputerStatusTransitional(cachedComputerStatus(queryClient, computerId))) return;
          if (attempt >= STATUS_RECONNECT.maxAttempts) return;
          await sleep(
            Math.min(STATUS_RECONNECT.baseMs * 2 ** attempt, STATUS_RECONNECT.capMs),
            controller.signal,
          );
        }
      })();
    }

    return () => controller.abort();
  }, [watchedComputerKey, queryClient]);

  useEffect(() => {
    if (!watchedWorkspaceKey) return;
    const controller = new AbortController();
    for (const workspaceId of watchedWorkspaceKey.split(',')) {
      void streamWorkspaceEvents(
        workspaceId,
        (status) => patchWorkspaceStatusInCaches(queryClient, workspaceId, status),
        controller.signal,
      );
    }
    return () => controller.abort();
  }, [watchedWorkspaceKey, queryClient]);
}

/** Root of the workspace key family: the cache events this watch cares about. */
const WORKSPACE_KEY_ROOT = queryKeys.workspaces.all[0];

/**
 * The in-flight workspaces no machine speaks for, as a sorted joined key.
 *
 * Read from the cache rather than taken as a prop so the fan-out mounts once,
 * and subscribed rather than snapshotted because the arming write is a cache
 * write: `warmWorkspace` putting the 202's 'starting' on a row is what brings
 * it into this set.
 */
function useUnboundTransitionalWorkspaceKey(): string {
  const queryClient = useQueryClient();

  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      queryClient.getQueryCache().subscribe((event) => {
        const [root] = event.query.queryKey;
        if (typeof root === 'string' && root === WORKSPACE_KEY_ROOT) onStoreChange();
      }),
    [queryClient],
  );

  const getKey = useCallback(() => {
    const ids = new Set(
      cachedWorkspaceLists(queryClient)
        .filter((ws) => !ws.computer_id && isComputerStatusTransitional(ws.status))
        .map((ws) => ws.workspace_id),
    );
    return [...ids].sort().join(',');
  }, [queryClient]);

  return useSyncExternalStore(subscribe, getKey);
}
