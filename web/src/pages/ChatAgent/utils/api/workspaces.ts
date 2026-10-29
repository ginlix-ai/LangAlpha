/**
 * Workspace management endpoints.
 */
import { api } from '@/api/client';
import type {
  Workspace,
  WorkspaceCreate,
  WorkspaceMove,
  WorkspaceMoveError,
  WorkspaceQuota,
  WorkspacesResponse,
} from '@/types/api';
import { apiErrorObject, apiErrorStatus } from '@/api/errors';
import { streamStatusEvents } from './statusStream';

// The shared API client sets no global timeout. Workspace-management ops
// legitimately run tens of seconds (a spec change rebuilds the sandbox,
// duplicate provisions one), so these bounds are generous — they convert a
// network hang into a visible failure rather than race the server.
const WORKSPACE_MUTATION_TIMEOUT_MS = 120000;

const WORKSPACE_QUERY_TIMEOUT_MS = 15000;

// A restore copies a whole workspace back onto a machine over SSH, which can
// outlast any other workspace call.
const FILES_RESTORE_TIMEOUT_MS = 600000;

export async function getWorkspaces(limit: number = 20, offset: number = 0, sortBy: string = 'custom', includeHome: boolean = false): Promise<WorkspacesResponse> {
  const { data } = await api.get<WorkspacesResponse>('/api/v1/workspaces', {
    params: { limit, offset, sort_by: sortBy, ...(includeHome ? { include_home: true } : {}) },
  });
  return data;
}

/** `computerId` puts the folder on that computer; without it the server uses the primary. */
export async function createWorkspace(
  name: string,
  description: string = '',
  config: Record<string, unknown> = {},
  computerId?: string | null,
): Promise<Workspace> {
  const body: WorkspaceCreate = { name, description, config, ...(computerId ? { computer_id: computerId } : {}) };
  const { data } = await api.post<Workspace>('/api/v1/workspaces', body);
  return data;
}

/**
 * `deleteFiles` also deletes the folder from a machine the user brought, where
 * the server otherwise leaves it. Every other computer's folder goes regardless.
 */
export async function deleteWorkspace(workspaceId: string, { deleteFiles = false }: { deleteFiles?: boolean } = {}) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const id = String(workspaceId).trim();
  if (!id) throw new Error('Workspace ID cannot be empty');
  const url = `/api/v1/workspaces/${id}`;
  await (deleteFiles ? api.delete(url, { params: { delete_files: true } }) : api.delete(url));
}

export async function getWorkspace(workspaceId: string): Promise<Workspace> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get<Workspace>(`/api/v1/workspaces/${workspaceId}`);
  return data;
}

/**
 * Get or create the user's Home, the workspace the Chief of Staff runs in.
 * Idempotent, so safe to call on every app load.
 */
export async function getHomeWorkspace(): Promise<Workspace> {
  const { data } = await api.post<Workspace>('/api/v1/workspaces/home');
  return data;
}

export async function updateWorkspace(workspaceId: string, updates: Record<string, unknown>) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}`, updates);
  return data;
}

export async function reorderWorkspaces(items: Array<{ workspace_id: string; sort_order: number }>) {
  if (!items?.length) throw new Error('Reorder items are required');
  await api.post('/api/v1/workspaces/reorder', { items });
}

/**
 * Rename a workspace. Thin wrapper over the existing update endpoint
 * (PUT /api/v1/workspaces/{id} with { name }).
 */
export async function renameWorkspace(workspaceId: string, name: string) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}`, { name }, {
    timeout: WORKSPACE_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/**
 * Duplicate a workspace (copies persisted files; always-on is reset to off on
 * the copy so it re-checks entitlement). Returns the new workspace record.
 */
export async function duplicateWorkspace(workspaceId: string) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/duplicate`, null, {
    timeout: WORKSPACE_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/**
 * Move a workspace to another of the user's computers. The server answers 202
 * once the move is accepted and runs it afterwards, so its progress and
 * outcome are read back with `getWorkspaceMove`. A refusal is a 409 (a 404
 * when the workspace is gone) carrying a `WorkspaceMoveError`; see
 * `moveRefusal`. Home is refused here: it moves only with the primary.
 *
 * `fromBackup` restores the workspace onto the target from its backup instead
 * of copying it off its computer, which is never contacted: the way off a
 * machine the user brought that is gone for good. Refused with `no_backup`
 * when there is no backup to restore from.
 */
export async function moveWorkspace(
  workspaceId: string,
  computerId: string,
  options: { fromBackup?: boolean } = {},
): Promise<WorkspaceMove> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.post<WorkspaceMove>(
    `/api/v1/workspaces/${workspaceId}/move`,
    { computer_id: computerId, ...(options.fromBackup ? { from_backup: true } : {}) },
    { timeout: WORKSPACE_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/** The workspace's latest move, running or ended; null when it has never moved. */
export async function getWorkspaceMove(workspaceId: string): Promise<WorkspaceMove | null> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get<WorkspaceMove | null>(`/api/v1/workspaces/${workspaceId}/move`, {
    timeout: WORKSPACE_QUERY_TIMEOUT_MS,
  });
  return data ?? null;
}

/**
 * Cancel a move while it is still copying. Answers the move as `cancelled`, or
 * as it ended if it already had. Past copying it is refused with a 409
 * (`not_cancellable`): the move then finishes or puts the folder back on its own.
 */
export async function cancelWorkspaceMove(workspaceId: string): Promise<WorkspaceMove> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.post<WorkspaceMove>(`/api/v1/workspaces/${workspaceId}/move/cancel`, null, {
    timeout: WORKSPACE_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/**
 * The coded refusal a move request got (and make-primary, which answers in
 * the same shape), or null for any other failure. Its `message` is the
 * server's sentence for the user.
 */
export function moveRefusal(err: unknown): WorkspaceMoveError | null {
  const status = apiErrorStatus(err);
  if (status !== 409 && status !== 404) return null;
  const detail = apiErrorObject(err);
  return detail && typeof detail.code === 'string' && typeof detail.message === 'string' && detail.message
    ? (detail as unknown as WorkspaceMoveError)
    : null;
}

/**
 * Answer a restore offer (`files_restore_offered_at`): copy the backup back
 * into the folder, or keep the folder empty and let the backup follow it as it
 * is now. Restoring brings the machine up and copies every file back, so the
 * bound is the long one. Answers the workspace with the offer cleared; a 409
 * means no offer stands (already answered), and a machine that cannot be
 * reached is a 503 in the sandbox-failure shape.
 */
export async function chooseFilesRestore(workspaceId: string, restore: boolean): Promise<Workspace> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.post<Workspace>(
    `/api/v1/workspaces/${workspaceId}/files/restore-choice`,
    { restore },
    { timeout: FILES_RESTORE_TIMEOUT_MS },
  );
  return data;
}

/**
 * Fetch per-tier workspace count quotas. Platform mode only — every field is null
 * in OSS mode, so callers should treat null as "no limit to show".
 */
export async function getWorkspaceQuota(): Promise<WorkspaceQuota> {
  const { data } = await api.get('/api/v1/workspaces/quota', {
    timeout: WORKSPACE_QUERY_TIMEOUT_MS,
  });
  return data;
}

export interface WorkspaceActionResponse {
  workspace_id: string;
  status: string;
  message?: string;
}

/**
 * Start (or warm) a stopped workspace. When { lazy: true }, the backend
 * returns 202 immediately and continues the restart in a background task.
 */
export async function startWorkspace(
  workspaceId: string,
  opts: { lazy?: boolean } = {},
): Promise<WorkspaceActionResponse> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const params = opts.lazy ? '?lazy=true' : '';
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/start${params}`);
  return data;
}

/**
 * Subscribe to workspace lifecycle status via SSE. Invokes `onStatus`
 * for each status transition reported by the backend, passing an optional
 * `sandboxState` refinement (e.g. 'archived') when present so callers can
 * show a slow-restore spinner, and the `computerId` of the machine the
 * workspace runs on when the backend reports one. Resolves when the stream
 * closes (terminal status, server timeout, or aborted via the AbortController
 * signal). Best-effort: network errors resolve without throwing so callers
 * don't need defensive wrappers.
 *
 * This is the fallback channel once a workspace names a computer: the machine
 * is what actually moves, so `streamComputerEvents` is the subscription a
 * surface showing several workspaces should open.
 */
export async function streamWorkspaceEvents(
  workspaceId: string,
  onStatus: (
    status: string,
    sandboxState?: string,
    computerId?: string,
  ) => void,
  signal: AbortSignal,
): Promise<void> {
  if (!workspaceId) return;
  await streamStatusEvents(
    `/api/v1/workspaces/${workspaceId}/events`,
    (frame) => onStatus(frame.status, frame.sandbox_state, frame.computer_id),
    signal,
  );
}

// --- Threads ---
