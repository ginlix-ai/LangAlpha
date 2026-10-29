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

import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import type { ComputersResponse, Workspace, WorkspacesResponse } from '@/types/api';

import { startWorkspace } from './api';
import { cachedHomeRow, workspaceDetailQuery } from './workspaceQueries';

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
 * `row` with `status` written in, and the computer when the writer names one.
 * The root the row was read with is its computer's, so a writer that names
 * another computer clears it, and the computer list's stands in until the row
 * is read again.
 */
function withStatus<T extends Workspace>(row: T, status: string, computerId?: string): T {
  const moved = !!computerId && computerId !== row.computer_id;
  return {
    ...row,
    status,
    ...(computerId ? { computer_id: computerId } : {}),
    ...(moved && row.root_dir ? { root_dir: null } : {}),
  };
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
  computerId?: string,
): void {
  queryClient.setQueryData(
    workspaceDetailQuery(workspaceId).queryKey,
    (prev) => prev ? withStatus(prev, status, computerId) : prev,
  );
  queryClient.setQueriesData<WorkspacesResponse | undefined>(
    { queryKey: queryKeys.workspaces.lists() },
    (prev) => {
      if (!prev?.workspaces) return prev;
      return {
        ...prev,
        workspaces: prev.workspaces.map((w) =>
          w.workspace_id === workspaceId ? withStatus(w, status, computerId) : w,
        ),
      };
    },
  );
}

/** Update one machine and every cached workspace bound to it. */
export function patchComputerStatusInCaches(
  queryClient: QueryClient,
  computerId: string,
  status: string,
): void {
  queryClient.setQueriesData<ComputersResponse | undefined>(
    { queryKey: queryKeys.computers.lists() },
    (prev) => !prev?.computers ? prev : {
      ...prev,
      computers: prev.computers.map((computer) =>
        computer.computer_id === computerId ? { ...computer, status } : computer,
      ),
    },
  );
  queryClient.setQueriesData<Workspace | undefined>(
    { queryKey: queryKeys.workspaces.details() },
    (prev) => prev?.computer_id === computerId && prev.status !== 'deleted'
      ? withStatus(prev, status)
      : prev,
  );
  const workspaceIds = new Set(
    cachedWorkspaceLists(queryClient)
      .filter((workspace) =>
        workspace.computer_id === computerId && workspace.status !== 'deleted')
      .map((workspace) => workspace.workspace_id),
  );
  for (const id of workspaceIds) patchWorkspaceStatusInCaches(queryClient, id, status);
}

/** The computer Home is bound to, called once a start has bound it. A row
 * cached before Home's first start names no computer, and the start's reply
 * does not either, so the detail is read again then; that fresh read is also
 * what puts Home's folder in front of every reader of the detail. */
async function homeComputerId(queryClient: QueryClient, workspaceId: string): Promise<string | null> {
  const cached = [
    queryClient.getQueryData(workspaceDetailQuery(workspaceId).queryKey),
    ...cachedWorkspaceLists(queryClient),
  ].find((w) => w?.workspace_id === workspaceId && w.computer_id)?.computer_id;
  if (cached) return cached;
  const row = await queryClient.fetchQuery({ ...workspaceDetailQuery(workspaceId), staleTime: 0 });
  return row?.computer_id ?? null;
}

/** What the caches believe a machine is doing: its own row, else a workspace
 * bound to it. */
function cachedComputerStatus(queryClient: QueryClient, computerId: string): string | undefined {
  for (const [, data] of queryClient.getQueriesData<ComputersResponse>({ queryKey: queryKeys.computers.lists() })) {
    const computer = data?.computers?.find((c) => c.computer_id === computerId);
    if (computer) return computer.status;
  }
  return cachedWorkspaceLists(queryClient)
    .find((w) => w.computer_id === computerId && w.status !== 'deleted')
    ?.status;
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

/** Whether the caches know `workspaceId` as Home. Every warm follows a read
 * of the row: Home's own query, a list holding it, or its detail. */
function isCachedHome(queryClient: QueryClient, workspaceId: string): boolean {
  if (cachedHomeRow(queryClient, workspaceId)) return true;
  return isHomeWorkspace(
    queryClient.getQueryData(workspaceDetailQuery(workspaceId).queryKey)
      ?? cachedWorkspaceLists(queryClient).find((w) => w.workspace_id === workspaceId),
  );
}

export function warmWorkspace(workspaceId: string, queryClient: QueryClient): Promise<void> {
  if (!workspaceId) return Promise.resolve();

  const existing = inFlight.get(workspaceId);
  if (existing) return existing;

  // Before its first computer Home's row reads 'unbound', which the gate
  // below skips; the start itself binds Home to the computer and answers
  // from the computer's state.
  if (isCachedHome(queryClient, workspaceId)) {
    // The start moves Home's computer, which the workspaces beside it on that
    // machine show too, so patch the machine.
    const p = startWorkspace(workspaceId, { lazy: true })
      .then(async (resp) => {
        const computerId = await homeComputerId(queryClient, workspaceId);
        if (computerId && cachedComputerStatus(queryClient, computerId) === 'stopped') {
          patchComputerStatusInCaches(queryClient, computerId, resp.status);
        }
      })
      .catch((err: unknown) => {
        if (import.meta.env?.DEV) {
          console.warn('[warmWorkspace] failed', workspaceId, err);
        }
      })
      .finally(() => inFlight.delete(workspaceId));
    inFlight.set(workspaceId, p);
    return p;
  }

  const cached = queryClient.getQueryData(workspaceDetailQuery(workspaceId).queryKey);
  if (cached && cached.status && !['stopped', 'running'].includes(cached.status)) {
    return Promise.resolve();
  }

  const p = (async () => {
    try {
      const detail = cached ?? (await queryClient.fetchQuery(workspaceDetailQuery(workspaceId)));
      if (!detail?.status || !['stopped', 'running'].includes(detail.status)) return;

      const resp = await startWorkspace(workspaceId, { lazy: true });
      // Only reflect the 202 'starting' if nothing has advanced the cache past
      // 'stopped' meanwhile. The SSE stream (useWarmWorkspaceSandbox) can push
      // a fast 'running' (or 'error') before this slower patch lands; without
      // the guard, 'starting' would clobber it and wedge the UI on 'starting'
      // until the next refetch.
      const current = queryClient.getQueryData(workspaceDetailQuery(workspaceId).queryKey);
      if (!current?.status || current.status === 'stopped') {
        if (detail.computer_id) {
          patchComputerStatusInCaches(queryClient, detail.computer_id, resp.status);
        } else {
          patchWorkspaceStatusInCaches(queryClient, workspaceId, resp.status);
        }
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
