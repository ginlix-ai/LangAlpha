/**
 * warmWorkspace — proactive sandbox warming primitive.
 *
 * Shared between gallery click handlers and mount-time hooks so both call
 * sites go through the same dedupe Map. The backend's /start?lazy=true
 * returns 202 immediately and continues the restart in a background task.
 *
 * Best-effort: 4xx, 404, and network errors are swallowed silently. The
 * chat-time get_session_for_workspace path will surface real errors with
 * proper UX when the user actually sends a message.
 */
import { QueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { Workspace, WorkspacesResponse } from '@/types/api';

import { getWorkspace, startWorkspace } from './api';

const inFlight = new Map<string, Promise<void>>();

/**
 * Every workspace the list caches currently hold, across pages and sort orders.
 *
 * The one place that knows the shape those entries have, so a reader asking
 * "what does the app believe about the workspaces right now" does not sniff a
 * cache value or cast it.
 */
export function cachedWorkspaceLists(queryClient: QueryClient): Workspace[] {
  const rows: Workspace[] = [];
  const entries = queryClient.getQueriesData<WorkspacesResponse>({
    queryKey: queryKeys.workspaces.lists(),
  });
  for (const [, data] of entries) {
    if (data?.workspaces) rows.push(...data.workspaces);
  }
  return rows;
}

/**
 * Write `status` into both the workspace detail cache and any active
 * workspace-list caches. Shared between `warmWorkspace` (writes the
 * 202 /start response) and `useWarmWorkspaceSandbox` (writes each
 * SSE-pushed transition) so a single status change visibly updates
 * every gallery + detail consumer without a network round-trip.
 */
export function patchWorkspaceStatusInCaches(
  queryClient: QueryClient,
  workspaceId: string,
  status: string,
): void {
  queryClient.setQueryData<Workspace | undefined>(
    queryKeys.workspaces.detail(workspaceId),
    (prev) => (prev ? { ...prev, status } : prev),
  );
  queryClient.setQueriesData<WorkspacesResponse | undefined>(
    { queryKey: queryKeys.workspaces.lists() },
    (prev) => {
      if (!prev?.workspaces) return prev;
      return {
        ...prev,
        workspaces: prev.workspaces.map((w) =>
          w.workspace_id === workspaceId ? { ...w, status } : w,
        ),
      };
    },
  );
}

/** Two-level warming state the chat spinner renders: not warming, a generic
 * start, or a slow restore from cold storage. */
export type WarmingDisplay = false | 'starting' | 'archived';

/**
 * Merge the chat-path start signal (`workspaceStarting`) with the entry-time
 * warm signal (`warmingState`) into the single state the spinner shows.
 * 'archived' from EITHER source wins so a slow cold-storage restore always
 * gets the longer-wait copy even when only one source observed the refinement;
 * otherwise the first truthy signal shows.
 */
export function mergeWarmingDisplay(
  workspaceStarting: WarmingDisplay,
  warmingState: WarmingDisplay,
): WarmingDisplay {
  if (workspaceStarting === 'archived' || warmingState === 'archived') {
    return 'archived';
  }
  return workspaceStarting || warmingState || false;
}

export function warmWorkspace(
  workspaceId: string,
  queryClient: QueryClient,
): Promise<void> {
  if (!workspaceId) return Promise.resolve();

  const existing = inFlight.get(workspaceId);
  if (existing) return existing;

  const cached = queryClient.getQueryData<Workspace>(
    queryKeys.workspaces.detail(workspaceId),
  );
  if (cached && cached.status && cached.status !== 'stopped') {
    return Promise.resolve();
  }

  const p = (async () => {
    try {
      const detail =
        cached ??
        (await queryClient.fetchQuery({
          queryKey: queryKeys.workspaces.detail(workspaceId),
          queryFn: () => getWorkspace(workspaceId),
        }));
      if (!detail || detail.status !== 'stopped') return;

      const resp = await startWorkspace(workspaceId, { lazy: true });
      // Only reflect the 202 'starting' if nothing has advanced the cache past
      // 'stopped' meanwhile. The SSE stream (useWarmWorkspaceSandbox) can push
      // a fast 'running' (or 'error') before this slower patch lands; without
      // the guard, 'starting' would clobber it and wedge the UI on 'starting'
      // until the next refetch.
      const current = queryClient.getQueryData<Workspace>(
        queryKeys.workspaces.detail(workspaceId),
      );
      if (!current?.status || current.status === 'stopped') {
        patchWorkspaceStatusInCaches(queryClient, workspaceId, resp.status);
      }
    } catch (err) {
      // Best-effort warming — chat-time start path surfaces real errors with
      // proper UX. Log in dev so programmer mistakes (URL typos, response
      // shape changes) don't disappear silently.
      if (import.meta.env?.DEV) {
        console.warn('[warmWorkspace] failed', workspaceId, err);
      }
    } finally {
      inFlight.delete(workspaceId);
    }
  })();

  inFlight.set(workspaceId, p);
  return p;
}

export function __resetWarmStateForTests(): void {
  inFlight.clear();
}
