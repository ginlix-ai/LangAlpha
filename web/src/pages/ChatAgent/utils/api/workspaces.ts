/**
 * Workspace management endpoints.
 */
import { api } from '@/api/client';
import type { ResourceTier, WorkspaceQuota } from '@/types/api';
import { baseURL, getAuthHeaders } from './transport';

// The shared axios instance sets no global timeout. Workspace-management ops
// legitimately run tens of seconds (a spec change rebuilds the sandbox,
// duplicate provisions one), so these bounds are generous — they convert a
// network hang into a visible failure rather than race the server.
const WORKSPACE_MUTATION_TIMEOUT_MS = 120000;

const WORKSPACE_QUERY_TIMEOUT_MS = 15000;

export async function getWorkspaces(limit: number = 20, offset: number = 0, sortBy: string = 'custom', includeFlash: boolean = false) {
  const { data } = await api.get('/api/v1/workspaces', {
    params: { limit, offset, sort_by: sortBy, ...(includeFlash ? { include_flash: true } : {}) },
  });
  return data;
}

export async function createWorkspace(name: string, description: string = '', config: Record<string, unknown> = {}) {
  const { data } = await api.post('/api/v1/workspaces', { name, description, config });
  return data;
}

export async function deleteWorkspace(workspaceId: string) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const id = String(workspaceId).trim();
  if (!id) throw new Error('Workspace ID cannot be empty');
  await api.delete(`/api/v1/workspaces/${id}`);
}

export async function getWorkspace(workspaceId: string) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}`);
  return data;
}

/**
 * Ensure the shared flash workspace exists for the current user.
 * Idempotent — safe to call on every app load.
 * @returns {Promise<Object>} Flash workspace record
 */
export async function getFlashWorkspace() {
  const { data } = await api.post('/api/v1/workspaces/flash');
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
 * Change a workspace's sandbox resource tier (standard / performance / max).
 * In platform mode the backend gates elevated tiers: 403 (not on plan) or
 * 429 (workspace count limit reached). OSS mode is ungated.
 */
export async function setWorkspaceSpec(workspaceId: string, tier: ResourceTier) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/spec`, { tier }, {
    timeout: WORKSPACE_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/**
 * Toggle always-on (keep the sandbox running, disable idle auto-stop).
 * In platform mode enabling is gated (403 not on plan / 429 limit reached);
 * disabling is always allowed. OSS mode is ungated.
 */
export async function setWorkspaceAlwaysOn(workspaceId: string, enabled: boolean) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/always-on`, { enabled }, {
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
 * show a slow-restore spinner. Resolves when the stream closes (terminal
 * status, server timeout, or aborted via the AbortController signal).
 * Best-effort: network errors resolve without throwing so callers don't
 * need defensive wrappers.
 */
export async function streamWorkspaceEvents(
  workspaceId: string,
  onStatus: (status: string, sandboxState?: string) => void,
  signal: AbortSignal,
): Promise<void> {
  if (!workspaceId) return;
  const authHeaders = await getAuthHeaders();
  let res: Response;
  try {
    res = await fetch(`${baseURL}/api/v1/workspaces/${workspaceId}/events`, {
      method: 'GET',
      headers: { ...authHeaders, Accept: 'text/event-stream' },
      signal,
    });
  } catch {
    return; // network error or aborted — caller wants best-effort
  }
  if (!res.ok || !res.body) return;

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      const chunks = buffer.split('\n\n');
      buffer = chunks.pop() ?? '';
      for (const chunk of chunks) {
        let eventType = '';
        const dataLines: string[] = [];
        for (const raw of chunk.split('\n')) {
          if (raw.startsWith('event:')) eventType = raw.slice(6).trim();
          else if (raw.startsWith('data:')) dataLines.push(raw.slice(5).trim());
          // Comments (lines starting with ':') and unknown fields ignored.
        }
        // Per the SSE spec, multiple data: lines join with a newline. The
        // backend emits single-line json.dumps payloads, so this is one line in
        // practice — but joining correctly keeps a multi-line payload parseable
        // instead of silently corrupting the JSON.
        const data = dataLines.join('\n');
        if (eventType === 'status' && data) {
          try {
            const parsed = JSON.parse(data) as {
              status?: string;
              sandbox_state?: string;
            };
            if (typeof parsed.status === 'string') {
              onStatus(
                parsed.status,
                typeof parsed.sandbox_state === 'string'
                  ? parsed.sandbox_state
                  : undefined,
              );
            }
          } catch { /* ignore malformed payload */ }
        } else if (eventType === 'timeout') {
          return;
        }
      }
    }
  } catch (err) {
    if ((err as { name?: string })?.name === 'AbortError') return;
    // Best-effort — drop everything else.
  } finally {
    // Deterministically release the stream so repeated workspace navigation
    // doesn't retain fetch/body resources until browser GC. cancel() also
    // releases the lock; both are no-ops if the stream already closed.
    try {
      await reader.cancel();
    } catch {
      /* already closed / aborted */
    }
  }
}

// --- Threads ---
