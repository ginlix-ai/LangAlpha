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

import { getWorkspace, startWorkspace } from './api';

interface WorkspaceLike {
  workspace_id?: string;
  id?: string;
  status?: string;
  [key: string]: unknown;
}

const inFlight = new Map<string, Promise<void>>();

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
  queryClient.setQueryData<WorkspaceLike | undefined>(
    queryKeys.workspaces.detail(workspaceId),
    (prev) => (prev ? { ...prev, status } : prev),
  );
  const patchOne = (w: WorkspaceLike): WorkspaceLike =>
    (w.workspace_id ?? w.id) === workspaceId ? { ...w, status } : w;
  queryClient.setQueriesData<unknown>(
    { queryKey: queryKeys.workspaces.lists() },
    (prev: unknown) => {
      if (!prev) return prev;
      if (Array.isArray(prev)) {
        return (prev as WorkspaceLike[]).map(patchOne);
      }
      if (typeof prev === 'object' && prev !== null) {
        const obj = prev as { workspaces?: WorkspaceLike[] };
        if (Array.isArray(obj.workspaces)) {
          return { ...obj, workspaces: obj.workspaces.map(patchOne) };
        }
      }
      return prev;
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

  const cached = queryClient.getQueryData<WorkspaceLike>(
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
      const current = queryClient.getQueryData<WorkspaceLike>(
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
