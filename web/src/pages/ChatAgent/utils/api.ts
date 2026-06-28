/**
 * ChatAgent API utilities
 * All backend endpoints used by the ChatAgent page
 */
import { api } from '@/api/client';
import { supabase } from '@/lib/supabase';

const baseURL = api.defaults.baseURL;

/** Get Bearer auth headers for raw fetch() calls (SSE streams). */
async function getAuthHeaders(): Promise<Record<string, string>> {
  if (!supabase) return {};
  const { data } = await supabase.auth.getSession();
  const session = data.session;
  let token = session?.access_token;
  // Supabase's auto-refresh timer is frozen while the tab is backgrounded, so on
  // resume the cached session may already be expired. If it's past (or within
  // ~60s of) expiry, force a refresh so SSE reconnects don't fire with a dead
  // token and 401. expires_at is a Unix timestamp in SECONDS. Never throw from
  // this helper: a failed refresh falls back to whatever token we already have.
  if (session && token && typeof session.expires_at === 'number') {
    const nowSec = Math.floor(Date.now() / 1000);
    if (session.expires_at - nowSec <= 60) {
      try {
        const { data: refreshed } = await supabase.auth.refreshSession();
        const newToken = refreshed.session?.access_token;
        if (newToken) token = newToken;
      } catch {
        /* refresh failed — keep the existing (possibly stale) token */
      }
    }
  }
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Normalize an axios/fetch error into a readable message.
 *
 * Reads `err.response.data.detail`. FastAPI emits a string for most errors,
 * but validation failures come back as a list of `{ loc, msg }` entries — those
 * are flattened to `loc.path: msg` joined with `'; '` so the UI never renders
 * `[object Object]`. Falls back to `err.message` then a generic label.
 */
export function formatApiErrorDetail(err: unknown): string {
  const detail = (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (Array.isArray(detail)) {
    const parts = detail
      .map((entry) => {
        const e = entry as { loc?: unknown[]; msg?: unknown };
        const loc = Array.isArray(e?.loc) ? e.loc.map(String).join('.') : '';
        const msg = typeof e?.msg === 'string' ? e.msg : JSON.stringify(entry);
        return loc ? `${loc}: ${msg}` : msg;
      })
      .filter(Boolean);
    if (parts.length > 0) return parts.join('; ');
  }
  if (typeof detail === 'string' && detail) return detail;
  const message = (err as { message?: unknown })?.message;
  return typeof message === 'string' && message ? message : 'Request failed';
}

// --- Workspaces ---

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

/**
 * Get a single thread by ID (used to resolve workspace_id on direct URL access)
 * @param {string} threadId - The thread ID
 * @returns {Promise<Object>} Thread object with workspace_id, thread_id, title, etc.
 */
export async function getThread(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}`);
  return data;
}

/**
 * Get all threads for a specific workspace
 * @param {string} workspaceId - The workspace ID
 * @param {number} limit - Maximum threads to return (default: 20)
 * @param {number} offset - Pagination offset (default: 0)
 * @returns {Promise<Object>} Response with threads array, total, limit, offset
 */
export async function getWorkspaceThreads(
  workspaceId: string,
  limit: number = 20,
  offset: number = 0,
  platformPrefix: string | null = null,
) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const params: Record<string, string | number> = { workspace_id: workspaceId, limit, offset };
  if (platformPrefix) params.platform_prefix = platformPrefix;
  const { data } = await api.get('/api/v1/threads', { params });
  return data;
}

/**
 * Get recent threads across all workspaces for the current user.
 * Uses the same /api/v1/threads endpoint but omits workspace_id so the server
 * returns threads across every workspace the user owns, sorted by updated_at.
 */
export async function getRecentThreads(limit: number = 20, offset: number = 0) {
  const { data } = await api.get('/api/v1/threads', {
    params: { limit, offset, sort_by: 'updated_at', sort_order: 'desc' },
  });
  return data;
}

/**
 * Delete a thread
 * @param {string} threadId - The thread ID to delete
 * @returns {Promise<Object>} Response with success, thread_id, and message
 */
export async function deleteThread(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.delete(`/api/v1/threads/${threadId}`);
  return data;
}

/**
 * Update a thread's title
 * @param {string} threadId - The thread ID to update
 * @param {string} title - New thread title (max 255 chars, can be null to clear)
 * @returns {Promise<Object>} Updated thread object
 */
export async function updateThreadTitle(threadId: string, title: string | null) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.patch(`/api/v1/threads/${threadId}`, { title });
  return data;
}

// --- Streaming (fetch + ReadableStream; axios not used) ---

/**
 * Parse a `run_id` query parameter out of a backend `Content-Location` header
 * value such as `/api/v1/threads/{tid}/messages/stream?run_id={uuid}`.
 * Returns `null` when the value is missing or has no `run_id` param.
 */
export function parseRunIdFromContentLocation(
  contentLocation: string | null | undefined,
): string | null {
  if (!contentLocation) return null;
  const qIdx = contentLocation.indexOf('?');
  if (qIdx === -1) return null;
  try {
    const params = new URLSearchParams(contentLocation.slice(qIdx + 1));
    const runId = params.get('run_id');
    return runId && runId.length > 0 ? runId : null;
  } catch {
    return null;
  }
}

/**
 * Parse the `{tid}` path segment out of a backend `Content-Location` header
 * value such as `/api/v1/threads/{tid}/messages/stream?run_id={uuid}`.
 * Lets a new-thread send latch the server-assigned thread id from the response
 * headers — before the first SSE event — so an early stop can still cancel it.
 * Returns `null` when the value is missing or doesn't match the expected shape.
 */
export function parseThreadIdFromContentLocation(
  contentLocation: string | null | undefined,
): string | null {
  if (!contentLocation) return null;
  const match = contentLocation.match(/\/threads\/([^/?]+)\//);
  const tid = match?.[1];
  if (!tid || tid.length === 0) return null;
  try {
    return decodeURIComponent(tid);
  } catch {
    // Malformed percent-encoding (e.g. "%ZZ") throws URIError. The contract is
    // non-throwing/return-null for unusable input — a bad id can't be latched.
    return null;
  }
}

async function streamFetch(
  url: string,
  opts: RequestInit,
  onEvent: (event: Record<string, unknown>) => void,
  onHeaders?: (contentLocation: string | null) => void,
): Promise<{ disconnected: boolean; aborted: boolean; contentLocation: string | null }> {
  let res: Response;
  try {
    res = await fetch(`${baseURL}${url}`, opts);
  } catch (error: unknown) {
    // An AbortController.abort() during the initial fetch (e.g. the user hit
    // stop before the response headers arrived) surfaces as AbortError (a
    // DOMException, which is not always an Error instance — match on name).
    // Treat it as an intentional stop rather than a network failure so callers
    // don't show an error toast or run double cleanup.
    if ((error as { name?: string })?.name === 'AbortError') {
      return { disconnected: false, aborted: true, contentLocation: null };
    }
    throw error;
  }
  // Snapshot Content-Location before body errors so callers can recover the
  // canonical reconnect URL (carries ?run_id=…) even when a 4xx aborts later.
  const contentLocation = res.headers.get('Content-Location');
  // Notify the caller of headers IMMEDIATELY — well before any SSE body byte —
  // so the run_id can be latched before the first `metadata` event arrives.
  // Closes the reconnect race window between "clear stale run_id" and "new
  // turn's first metadata frame" (see useChatMessages.resumeWithHitlResponse).
  if (onHeaders) {
    try {
      onHeaders(contentLocation);
    } catch (e) {
      console.warn('[api] onHeaders callback threw', e);
    }
  }
  if (!res.ok) {
    // Handle 429 (rate limit) with structured detail
    if (res.status === 429) {
      let detail: Record<string, unknown> = {};
      try { detail = await res.json(); } catch { /* ignore */ }
      const err: Error & { status?: number; rateLimitInfo?: Record<string, unknown>; retryAfter?: number | null } =
        new Error((detail?.detail as Record<string, unknown>)?.message as string || 'Rate limit exceeded');
      err.status = 429;
      err.rateLimitInfo = (detail?.detail as Record<string, unknown>) || {};
      err.retryAfter = parseInt(res.headers.get('Retry-After') as string, 10) || null;
      throw err;
    }
    // Handle 413 (payload too large) with user-friendly message
    if (res.status === 413) {
      const err: Error & { status?: number } = new Error('Files too large. Try smaller files or fewer attachments.');
      err.status = 413;
      throw err;
    }
    // Handle 404 specifically for history replay (expected for new threads)
    if (res.status === 404 && url.includes('/replay')) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }
    // Read response body for error detail
    let detail = '';
    let errorInfo: Record<string, unknown> | null = null;
    const text = await res.text().catch(() => '');
    try {
      const body = JSON.parse(text);
      if (body?.detail && typeof body.detail === 'object' && 'message' in body.detail) {
        // Structured error detail (e.g., { message, type, link })
        errorInfo = body.detail as Record<string, unknown>;
        detail = (errorInfo.message as string) || '';
      } else {
        detail = typeof body?.detail === 'string' ? body.detail : JSON.stringify(body?.detail || body);
      }
    } catch { /* ignore parse errors */ }
    console.error(`[api] ${opts.method || 'GET'} ${url} failed:`, res.status, detail);
    const err: Error & { status?: number; errorInfo?: Record<string, unknown> } =
      new Error(detail || `HTTP error! status: ${res.status}`);
    err.status = res.status;
    if (errorInfo) err.errorInfo = errorInfo;
    throw err;
  }

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let ev: { id?: string; event?: string } = {};
  const processLine = (line: string) => {
    if (line.startsWith('id: ')) ev.id = line.slice(4).trim();
    else if (line.startsWith('event: ')) ev.event = line.slice(7).trim();
    else if (line.startsWith('data: ')) {
      try {
        const d = JSON.parse(line.slice(6));
        if (ev.event) d.event = ev.event;
        if (ev.id != null) d._eventId = parseInt(ev.id, 10) || ev.id;
        onEvent(d);
      } catch (e: unknown) {
        console.warn('[api] SSE parse error', e, line);
      }
      ev = {};
    } else if (line.trim() === '') ev = {};
  };

  let disconnected = false;
  let aborted = false;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      lines.forEach(processLine);
    }
    // Process any remaining buffer
    buffer.split('\n').forEach(processLine);
  } catch (error: unknown) {
    // An AbortController.abort() on the reader (the user hit stop) surfaces as
    // AbortError (a DOMException — match on name, not instanceof Error). This
    // is an INTENTIONAL stop, not a failure — return an aborted marker so
    // callers skip reconnect/error-toast/double-cleanup.
    if ((error as { name?: string })?.name === 'AbortError') {
      aborted = true;
    } else if (error instanceof Error && error.name === 'TypeError') {
      // iOS Safari freezes a backgrounded tab and tears down its connection,
      // rejecting reader.read() with "Load failed" / "The network connection was
      // lost." — neither reliably contains "network", so the old substring guard
      // re-threw it and surfaced a dead-end error banner with no reconnect. Per
      // the Streams/Fetch spec, reader.read() only rejects with a TypeError on a
      // transport-level network error; the loop body (decode/split/processLine,
      // which guards its own JSON.parse) won't otherwise throw one. So treat any
      // TypeError here as a dropped stream and route it into the reconnect path.
      console.warn('[api] Stream interrupted (transport error):', error.message);
      disconnected = true;
    } else {
      throw error;
    }
  }
  return { disconnected, aborted, contentLocation };
}

export async function replayThreadHistory(threadId: string, onEvent: (event: Record<string, unknown>) => void = () => {}) {
  if (!threadId) throw new Error('Thread ID is required');
  const authHeaders = await getAuthHeaders();
  await streamFetch(`/api/v1/threads/${threadId}/messages/replay`, { method: 'GET', headers: { ...authHeaders } }, onEvent);
}

export async function sendChatMessageStream(
  message: string,
  workspaceId: string,
  threadId: string | null = null,
  messageHistory: Array<{ role: string; content: string }> = [],
  planMode: boolean = false,
  onEvent: (event: Record<string, unknown>) => void = () => {},
  additionalContext: Record<string, unknown>[] | string | null = null,
  agentMode: string = 'ptc',
  locale: string = 'en-US',
  timezone: string = 'America/New_York',
  checkpointId: string | null = null,
  forkFromTurn: number | null = null,
  llmModel: string | null = null,
  reasoningEffort: string | null = null,
  fastMode: boolean | null = null,
  platform: string | null = null,
  onRunIdResolved: ((runId: string, threadId: string | null) => void) | null = null,
  signal: AbortSignal | null = null,
) {
  // For checkpoint replay (regenerate/retry), send empty messages
  const messages = checkpointId && !message
    ? []
    : [...messageHistory, { role: 'user', content: message }];
  const body: Record<string, unknown> = {
    workspace_id: workspaceId,
    messages,
    agent_mode: agentMode,
    plan_mode: planMode,
    locale,
    timezone,
  };
  if (additionalContext) {
    body.additional_context = additionalContext;
  }
  if (checkpointId) {
    body.checkpoint_id = checkpointId;
  }
  if (forkFromTurn != null) {
    body.fork_from_turn = forkFromTurn;
  }
  if (llmModel) body.llm_model = llmModel;
  if (reasoningEffort) body.reasoning_effort = reasoningEffort;
  if (fastMode) body.fast_mode = true;
  if (platform) body.platform = platform;
  // Use /threads/{id}/messages for existing thread, /threads/messages for new
  const isNewThread = !threadId || threadId === '__default__';
  const url = isNewThread
    ? '/api/v1/threads/messages'
    : `/api/v1/threads/${threadId}/messages`;
  const authHeaders = await getAuthHeaders();
  return await streamFetch(
    url,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream',
        ...authHeaders,
      },
      body: JSON.stringify(body),
      ...(signal ? { signal } : {}),
    },
    onEvent,
    onRunIdResolved
      ? (contentLocation) => {
          const runId = parseRunIdFromContentLocation(contentLocation);
          if (runId) onRunIdResolved(runId, parseThreadIdFromContentLocation(contentLocation));
        }
      : undefined,
  );
}

/**
 * Hard-cancel the workflow for a thread (stops the main agent AND kills all
 * subagents immediately, flushing the checkpoint so the next message resumes
 * from the last committed step).
 *
 * Pass ``runId`` to target a specific run. Without it the backend cancels the
 * latest active run — which, if a slow/retried cancel lands after the stopped
 * turn already tore down and the user started a new one, would hard-cancel that
 * *new* turn. The stop flow captures the run id at stop entry to avoid this.
 *
 * @param {string} threadId - The thread ID to cancel
 * @param {string|null} runId - The specific run to cancel; null = latest active
 * @returns {Promise<Object>} Response data
 */
export async function cancelWorkflow(threadId: string, runId: string | null = null) {
  if (!threadId) throw new Error('Thread ID is required');
  // Bound the request: the shared axios instance sets no global timeout, so a
  // network-level hang (not a 4xx) would block each stopWorkflow retry until the
  // browser's ~60s default — delaying the "couldn't stop" toast by minutes. 5s
  // is ample for a cancel POST.
  const { data } = await api.post(`/api/v1/threads/${threadId}/cancel`, undefined, {
    timeout: 5000,
    params: runId ? { run_id: runId } : undefined,
  });
  return data;
}

/**
 * Get the current status of a workflow for a thread
 * @param {string} threadId - The thread ID to check
 * @returns {Promise<Object>} Workflow status with can_reconnect, status, etc.
 */
export async function getWorkflowStatus(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/status`);
  return data;
}

/**
 * Watch a thread for new workflow activity via SSE (Redis pub/sub backed).
 * Returns an AbortController so the caller can close the connection.
 * Calls onWorkflowStarted(payload) when the backend signals a new workflow;
 * the payload carries the started run_id (e.g. a flash report-back run) so the
 * caller can attach to that exact run directly.
 * @param {string} threadId - The thread ID to watch
 * @param {Function} onWorkflowStarted - Callback when new workflow is detected
 * @returns {{ abort: AbortController }} - Call abort.abort() to stop watching
 */
export function watchThread(
  threadId: string,
  onWorkflowStarted: (payload?: { run_id?: string | null }) => void | Promise<void>,
): { abort: AbortController } {
  const abort = new AbortController();
  const MAX_RETRIES = 2;

  (async () => {
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (abort.signal.aborted) return;
      try {
        const authHeaders = await getAuthHeaders();
        const res = await fetch(`${baseURL}/api/v1/threads/${threadId}/watch`, {
          method: 'GET',
          headers: { ...authHeaders },
          signal: abort.signal,
        });

        if (!res.ok || !res.body) return;

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          // Process only COMPLETE SSE frames (terminated by a blank line). A
          // single frame can arrive split across reads, so reacting on the first
          // sight of the event name would race a half-buffered `data:` line and
          // parse partial JSON — losing the run_id and forcing the caller down a
          // /status fallback that, for a fast report-back, has already been torn
          // down. Splitting on the frame terminator guarantees the data line is
          // whole before we read the run_id.
          let sep: number;
          while ((sep = buffer.indexOf('\n\n')) >= 0) {
            const frame = buffer.slice(0, sep);
            buffer = buffer.slice(sep + 2);
            // Skip keepalive pings / timeout frames — only the wake carries a run_id.
            if (!frame.includes('event: workflow_started')) continue;
            reader.cancel();
            // Pull the run_id out of the event's data line so the caller can
            // attach to that exact run without a /status round-trip. Per the SSE
            // spec, multiple data: lines join with a newline — collect them all
            // (mirroring streamFetch above) so a multi-line payload stays
            // parseable instead of truncating to the first line and corrupting
            // the JSON. The backend wake is single-line today; this is resilience.
            let payload: { run_id?: string | null } = {};
            const dataLines: string[] = [];
            for (const raw of frame.split('\n')) {
              if (raw.startsWith('data:')) dataLines.push(raw.slice(5).trim());
            }
            if (dataLines.length) {
              try {
                payload = JSON.parse(dataLines.join('\n'));
              } catch {
                /* payload-less / malformed wake — caller falls back to /status */
              }
            }
            await onWorkflowStarted({ run_id: payload.run_id ?? null });
            return;
          }
        }
        return; // Stream ended cleanly without event — no retry
      } catch (err: unknown) {
        if ((err as Error).name === 'AbortError') return;
        if (attempt < MAX_RETRIES) {
          await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
        }
      }
    }
  })();

  return { abort };
}

/**
 * Reconnect to an in-progress workflow stream (replays buffered events, then live stream).
 *
 * When ``runId`` is provided, the backend targets the exact per-run Redis
 * stream key (``workflow:stream:{tid}:{rid}``). When omitted, the backend
 * falls back to the latest run on the thread.
 *
 * @param {string} threadId - The thread ID to reconnect to
 * @param {string|null} runId - The specific run to target; null = latest
 * @param {number|null} lastEventId - Last received event ID for deduplication
 * @param {Function} onEvent - Callback for each SSE event
 * @param {AbortSignal|null} signal - Abort the reader on a user stop
 */
export async function reconnectToWorkflowStream(
  threadId: string,
  runId: string | null = null,
  lastEventId: number | null = null,
  onEvent: (event: Record<string, unknown>) => void = () => {},
  signal: AbortSignal | null = null
) {
  if (!threadId) throw new Error('Thread ID is required');
  const params = new URLSearchParams();
  if (runId) params.set('run_id', runId);
  if (lastEventId != null) params.set('last_event_id', String(lastEventId));
  const query = params.toString();
  const queryParam = query ? `?${query}` : '';
  const authHeaders = await getAuthHeaders();
  return await streamFetch(
    `/api/v1/threads/${threadId}/messages/stream${queryParam}`,
    { method: 'GET', headers: { ...authHeaders }, ...(signal ? { signal } : {}) },
    onEvent
  );
}

/**
 * Fetch turn-boundary checkpoint IDs for a thread.
 * Used lazily (on-demand) when user clicks Edit or Regenerate on a message.
 * @param {string} threadId - The thread ID
 * @returns {Promise<{thread_id: string, turns: Array<{turn_index: number, edit_checkpoint_id: string|null, regenerate_checkpoint_id: string}>, retry_checkpoint_id: string|null}>}
 */
export async function fetchThreadTurns(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/turns`);
  return data;
}

/**
 * Stream a single subagent's content events (message_chunk, tool_calls, etc.)
 * via a dedicated per-task SSE endpoint.
 * @param {string} threadId - The thread ID
 * @param {string} taskId - The 6-char subagent task ID (e.g., 'k7Xm2p')
 * @param {Function} onEvent - Callback for each SSE event
 * @param {AbortSignal} signal - AbortController signal for cancellation
 */
export async function streamSubagentTaskEvents(
  threadId: string,
  taskId: string,
  onEvent: (event: Record<string, unknown>) => void,
  signal: AbortSignal
) {
  if (!threadId) throw new Error('Thread ID is required');
  if (!taskId) throw new Error('Task ID is required');
  const authHeaders = await getAuthHeaders();
  await streamFetch(
    `/api/v1/threads/${threadId}/tasks/${taskId}`,
    { method: 'GET', headers: { ...authHeaders }, signal },
    onEvent
  );
}

/**
 * Send a message/instruction to a running background subagent.
 * @param {string} threadId - The thread ID
 * @param {string} taskId - The subagent task ID (e.g., 'k7Xm2p')
 * @param {string} content - The instruction to send
 * @returns {Promise<Object>} { success, tool_call_id, display_id, queue_position }
 */
export async function sendSubagentMessage(threadId: string, taskId: string, content: string) {
  if (!threadId) throw new Error('Thread ID is required');
  if (!taskId) throw new Error('Task ID is required');
  const { data } = await api.post(
    `/api/v1/threads/${threadId}/tasks/${taskId}/messages`,
    { content }
  );
  return data;
}

/**
 * List files in a workspace sandbox
 * @param {string} workspaceId
 * @param {string} dirPath - e.g. "results"
 */
export async function listWorkspaceFiles(
  workspaceId: string,
  dirPath: string = 'results',
  { autoStart = false, includeSystem = false }: { autoStart?: boolean; includeSystem?: boolean } = {}
) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files`, {
    params: { path: dirPath, include_system: includeSystem, auto_start: autoStart, wait_for_sandbox: autoStart },
  });
  return data; // { workspace_id, path, files: [...] }
}

/**
 * Read a text file from workspace sandbox
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.md"
 */
export async function readWorkspaceFile(workspaceId: string, filePath: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath },
  });
  return data; // { workspace_id, path, content, mime, truncated }
}

/**
 * Download a file from workspace sandbox (returns blob URL)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<string>} Blob URL for the file
 */
export async function downloadWorkspaceFile(workspaceId: string, filePath: string) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'blob',
  });
  return URL.createObjectURL(response.data as Blob);
}

/**
 * Download a file from workspace sandbox as ArrayBuffer (for client-side parsing)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function downloadWorkspaceFileAsArrayBuffer(workspaceId: string, filePath: string) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'arraybuffer',
  });
  return response.data as ArrayBuffer;
}

/**
 * Trigger file download in browser
 * @param {string} workspaceId
 * @param {string} filePath
 */
export async function triggerFileDownload(workspaceId: string, filePath: string) {
  const blobUrl = await downloadWorkspaceFile(workspaceId, filePath);
  const fileName = filePath.split('/').pop() || 'download';
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(blobUrl);
}

/**
 * Send an HITL (Human-in-the-Loop) resume response to continue an interrupted workflow.
 * Used after the agent triggers a plan-mode interrupt and the user approves or rejects.
 *
 * @param {string} workspaceId - The workspace ID
 * @param {string} threadId - The thread ID of the interrupted workflow
 * @param {Object} hitlResponse - The HITL response payload, e.g. { [interruptId]: { decisions: [{ type: "approve" }] } }
 * @param {Function} onEvent - Callback for each SSE event
 * @param {boolean} planMode - Whether plan mode is active (to preserve SubmitPlan tool)
 */
export async function sendHitlResponse(
  workspaceId: string,
  threadId: string,
  hitlResponse: Record<string, unknown>,
  onEvent: (event: Record<string, unknown>) => void = () => {},
  planMode: boolean = false,
  modelOptions: { model?: string; reasoningEffort?: string; fastMode?: boolean } = {},
  agentMode: string = 'ptc',
  onRunIdResolved: ((runId: string, threadId: string | null) => void) | null = null,
  signal: AbortSignal | null = null,
) {
  const body: Record<string, unknown> = {
    workspace_id: workspaceId,
    messages: [],
    hitl_response: hitlResponse,
    plan_mode: planMode,
    agent_mode: agentMode,
  };
  if (modelOptions?.model) body.llm_model = modelOptions.model;
  if (modelOptions?.reasoningEffort) body.reasoning_effort = modelOptions.reasoningEffort;
  if (modelOptions?.fastMode) body.fast_mode = true;
  const authHeaders = await getAuthHeaders();
  return await streamFetch(
    `/api/v1/threads/${threadId}/messages`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream',
        ...authHeaders,
      },
      body: JSON.stringify(body),
      ...(signal ? { signal } : {}),
    },
    onEvent,
    onRunIdResolved
      ? (contentLocation) => {
          const runId = parseRunIdFromContentLocation(contentLocation);
          if (runId) onRunIdResolved(runId, parseThreadIdFromContentLocation(contentLocation));
        }
      : undefined,
  );
}

/**
 * Backup workspace files from sandbox to DB for offline access
 * @param {string} workspaceId
 * @returns {Promise<Object>} { synced, skipped, deleted, errors, total_size }
 */
export async function backupWorkspaceFiles(workspaceId: string) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/files/backup`);
  return data;
}

/**
 * Get backup status: which files are saved in DB
 * @param {string} workspaceId
 * @returns {Promise<Object>} { persisted_files: {path: hash}, total_size }
 */
export async function getBackupStatus(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/backup-status`);
  return data;
}

/**
 * Write full file content to a sandbox file
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.py"
 * @param {string} content - File content to write
 * @returns {Promise<Object>} { workspace_id, path, size }
 */
export async function writeWorkspaceFile(workspaceId: string, filePath: string, content: string) {
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}/files/write`,
    { content },
    { params: { path: filePath } }
  );
  return data;
}

/**
 * Read a file without line-limit pagination (for edit mode)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<Object>} { workspace_id, path, content, mime }
 */
export async function readWorkspaceFileFull(workspaceId: string, filePath: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath, unlimited: true },
  });
  return data;
}

export async function deleteWorkspaceFiles(workspaceId: string, paths: string[]) {
  const { data } = await api.delete(`/api/v1/workspaces/${workspaceId}/files`, {
    data: { paths },
  });
  return data;
}

// --- Sandbox ---

export async function getSandboxStats(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/sandbox/stats`);
  return data;
}

export async function installSandboxPackages(workspaceId: string, packages: string[]) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/packages`, { packages });
  return data;
}

export async function refreshWorkspace(workspaceId: string) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/refresh`);
  return data;
}

export async function getPreviewUrl(workspaceId: string, port: number, command?: string, force?: boolean) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/preview-url`, {
    port,
    ...(command && { command }),
    ...(force && { force: true }),
  });
  return data;
}

export async function checkPreviewHealth(workspaceId: string, port: number) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/preview-health`, { port });
  return data as { reachable: boolean; checked_at: number };
}

// --- Thread Sharing ---

/**
 * Get current share status for a thread
 * @param {string} threadId
 * @returns {Promise<Object>} { is_shared, share_token, share_url, permissions }
 */
export async function getThreadShareStatus(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/share`);
  return data;
}

/**
 * Update sharing settings for a thread
 * @param {string} threadId
 * @param {Object} body - { is_shared: bool, permissions?: { allow_files?: bool, allow_download?: bool } }
 * @returns {Promise<Object>} { is_shared, share_token, share_url, permissions }
 */
export async function updateThreadSharing(threadId: string, body: Record<string, unknown>) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/threads/${threadId}/share`, body);
  return data;
}

// --- Compaction ---
// The endpoint path `/summarize` and the `summarizeThread` function name are
// preserved for REST contract compatibility.

export async function summarizeThread(threadId: string, keepMessages: number = 5) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/threads/${threadId}/summarize`, null, {
    params: { keep_messages: keepMessages },
  });
  return data;
}

export async function offloadThread(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/threads/${threadId}/offload`);
  return data;
}

// --- Skills ---

const _skillsPromises: Record<string, Promise<unknown[]>> = {};  // module-level cache keyed by mode

export async function getSkills(mode: string | null = null) {
  const key = mode || '_all';
  if (key in _skillsPromises) return _skillsPromises[key];
  _skillsPromises[key] = api.get('/api/v1/skills', { params: mode ? { mode } : {} })
    .then(({ data }) => data.skills || [])
    .catch(() => { delete _skillsPromises[key]; return []; });
  return _skillsPromises[key];
}

// --- Model Metadata (eager prefetch at import time — resolved before ChatInput mounts) ---

const _modelMetadataPromise: Promise<Record<string, unknown>> = api.get('/api/v1/models')
  .then(({ data }) => data.model_metadata || {})
  .catch(() => ({}));

export function getModelMetadata() {
  return _modelMetadataPromise;
}

// --- File Upload ---

// --- Feedback ---

export async function submitFeedback(
  threadId: string,
  turnIndex: number,
  rating: string,
  issueCategories: string[] | null = null,
  comment: string | null = null,
  consentHumanReview: boolean = false
) {
  const { data } = await api.post(`/api/v1/threads/${threadId}/feedback`, {
    turn_index: turnIndex,
    rating,
    issue_categories: issueCategories,
    comment: comment || null,
    consent_human_review: consentHumanReview,
  });
  return data;
}

export async function removeFeedback(threadId: string, turnIndex: number) {
  const { data } = await api.delete(`/api/v1/threads/${threadId}/feedback`, {
    params: { turn_index: turnIndex },
  });
  return data;
}

export async function getThreadFeedback(threadId: string) {
  const { data } = await api.get(`/api/v1/threads/${threadId}/feedback`);
  return data;
}

// --- File uploads ---

export async function uploadWorkspaceFile(
  workspaceId: string,
  file: File,
  destPath: string | null = null,
  onProgress: ((percent: number) => void) | null = null
) {
  const formData = new FormData();
  formData.append('file', file);
  const params = destPath ? { path: destPath } : {};
  const { data } = await api.post(
    `/api/v1/workspaces/${workspaceId}/files/upload`,
    formData,
    {
      params,
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress
        ? (e) => onProgress(Math.round((e.loaded * 100) / (e.total || 1)))
        : undefined,
    }
  );
  return data;
}

// --- Vault Secrets ---

export async function getVaultSecrets(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/vault/secrets`);
  return data.secrets;
}

export async function createVaultSecret(workspaceId: string, body: { name: string; value: string; description?: string }) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/vault/secrets`, body);
  return data;
}

export async function updateVaultSecret(workspaceId: string, name: string, body: { value?: string; description?: string }) {
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}/vault/secrets/${name}`, body);
  return data;
}

export async function revealVaultSecret(workspaceId: string, name: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/vault/secrets/${name}/reveal`);
  return data.value as string;
}

export async function deleteVaultSecret(workspaceId: string, name: string) {
  const { data } = await api.delete(`/api/v1/workspaces/${workspaceId}/vault/secrets/${name}`);
  return data;
}

// --- Vault Blueprints (credentials recommended but not yet set) ---

export interface VaultBlueprint {
  name: string;
  label: string;
  description: string;
  docs_url: string | null;
  regex: string | null;
  sources: string[];
}

export interface VaultBlueprintsResponse {
  blueprints: VaultBlueprint[];
  remaining_slots: number;
}

export async function getVaultBlueprints(workspaceId: string): Promise<VaultBlueprintsResponse> {
  const { data } = await api.get<VaultBlueprintsResponse>(
    `/api/v1/workspaces/${workspaceId}/vault/blueprints`,
  );
  return data;
}

// --- Memory (agent long-term memory in LangGraph store) ---

export interface MemoryEntry {
  key: string;
  size: number;
  created_at: string | null;
  modified_at: string | null;
}

export interface MemoryListResponse {
  tier: 'user' | 'workspace';
  entries: MemoryEntry[];
}

export interface MemoryReadResponse {
  tier: 'user' | 'workspace';
  key: string;
  content: string;
  encoding: string;
  created_at: string | null;
  modified_at: string | null;
}

export async function listUserMemory(): Promise<MemoryListResponse> {
  const { data } = await api.get<MemoryListResponse>('/api/v1/memory/user');
  return data;
}

export async function readUserMemory(key: string): Promise<MemoryReadResponse> {
  const { data } = await api.get<MemoryReadResponse>('/api/v1/memory/user/read', {
    params: { key },
  });
  return data;
}

export async function listWorkspaceMemory(workspaceId: string): Promise<MemoryListResponse> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get<MemoryListResponse>(
    `/api/v1/memory/workspaces/${workspaceId}`,
  );
  return data;
}

export async function readWorkspaceMemory(
  workspaceId: string,
  key: string,
): Promise<MemoryReadResponse> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get<MemoryReadResponse>(
    `/api/v1/memory/workspaces/${workspaceId}/read`,
    { params: { key } },
  );
  return data;
}

// --- Memo (user-managed document store) -----------------------------------

export type MemoMetadataStatus = 'pending' | 'ready' | 'failed';

export interface MemoEntry {
  key: string;
  original_filename: string | null;
  mime_type: string | null;
  size_bytes: number;
  description: string | null;
  metadata_status: MemoMetadataStatus | null;
  created_at: string | null;
  modified_at: string | null;
  source_kind: string | null;
  source_workspace_id: string | null;
  source_path: string | null;
  sha256: string | null;
}

export interface MemoListResponse {
  entries: MemoEntry[];
  truncated: boolean;
}

export interface MemoReadResponse {
  key: string;
  original_filename: string | null;
  mime_type: string | null;
  content: string;
  encoding: string;
  description: string | null;
  summary: string | null;
  metadata_status: MemoMetadataStatus | null;
  metadata_error: string | null;
  size_bytes: number;
  created_at: string | null;
  modified_at: string | null;
  source_kind: string | null;
  source_workspace_id: string | null;
  source_path: string | null;
}

export interface MemoUploadResponse {
  key: string;
  original_filename: string;
  metadata_status: MemoMetadataStatus;
  replaced?: boolean;
}

export interface MemoUploadSource {
  source_kind: 'sandbox' | 'upload';
  source_workspace_id?: string;
  source_path?: string;
}

export async function listUserMemos(): Promise<MemoListResponse> {
  const { data } = await api.get<MemoListResponse>('/api/v1/memo/user');
  return data;
}

export async function readUserMemo(key: string): Promise<MemoReadResponse> {
  const { data } = await api.get<MemoReadResponse>('/api/v1/memo/user/read', {
    params: { key },
  });
  return data;
}

export async function uploadUserMemo(
  file: File,
  onProgress: ((percent: number) => void) | null = null,
  source?: MemoUploadSource | null,
): Promise<MemoUploadResponse> {
  const formData = new FormData();
  formData.append('file', file);
  if (source?.source_kind) {
    formData.append('source_kind', source.source_kind);
  }
  if (source?.source_workspace_id) {
    formData.append('source_workspace_id', source.source_workspace_id);
  }
  if (source?.source_path) {
    formData.append('source_path', source.source_path);
  }
  const { data } = await api.post<MemoUploadResponse>(
    '/api/v1/memo/user/upload',
    formData,
    {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress
        ? (e) => onProgress(Math.round((e.loaded * 100) / (e.total || 1)))
        : undefined,
    },
  );
  return data;
}

export async function writeUserMemo(
  key: string,
  content: string,
): Promise<MemoUploadResponse> {
  const { data } = await api.put<MemoUploadResponse>('/api/v1/memo/user/write', {
    key,
    content,
  });
  return data;
}

export async function deleteUserMemo(key: string): Promise<void> {
  await api.delete('/api/v1/memo/user', { params: { key } });
}

export async function regenerateUserMemo(
  key: string,
): Promise<MemoUploadResponse> {
  const { data } = await api.post<MemoUploadResponse>(
    '/api/v1/memo/user/regenerate',
    undefined,
    { params: { key } },
  );
  return data;
}

/**
 * Fetch the original memo bytes via axios (bearer-token auth attached) and
 * return a blob URL suitable for `<object data=...>` or an `<a download>`
 * anchor. Callers are responsible for `URL.revokeObjectURL()` when done.
 *
 * The download endpoint requires the Authorization header, so a plain
 * `<a href="/api/v1/memo/user/download?key=...">` won't work — the same
 * reason `FilePanel` uses the blob-URL pattern.
 */
export async function downloadUserMemoBlobUrl(key: string): Promise<string> {
  const response = await api.get('/api/v1/memo/user/download', {
    params: { key },
    responseType: 'blob',
  });
  return URL.createObjectURL(response.data as Blob);
}

/**
 * Trigger a browser download of the original memo file.
 * Uses the same blob + anchor-click pattern as `triggerFileDownload`.
 */
export async function triggerUserMemoDownload(
  key: string,
  filename: string | null = null,
): Promise<void> {
  const blobUrl = await downloadUserMemoBlobUrl(key);
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = filename || key;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(blobUrl);
}

// --- MCP servers (per-workspace + user catalog) ---
//
// Per-workspace effective list mixes built-in servers with workspace-added
// ones; the catalog holds reusable user templates that get copied into a
// workspace via `from_template`. Env/header literal values are never echoed by
// the backend — only `${vault:NAME}` reference names surface (as `*_refs`).

/** A full MCP server definition payload (matches backend `McpServerInput`). */
export interface McpServerInput {
  name: string;
  transport: 'stdio' | 'sse' | 'http';
  command?: string | null;
  args?: string[];
  url?: string | null;
  env?: Record<string, string>;
  headers?: Record<string, string>;
  description?: string;
  instruction?: string;
  tool_exposure_mode?: 'summary' | 'detailed';
  discovery_uses_secrets?: boolean;
}

/** One discovered tool (sanitized snapshot from the discovery cache). */
export interface McpToolSummary {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
}

export type McpStatus =
  | 'connected'
  | 'error'
  | 'needs_secret'
  | 'disabled'
  | 'pending'
  | 'unknown';

/** One row in the effective per-workspace MCP list. */
export interface EffectiveServer {
  name: string;
  origin: 'builtin' | 'workspace';
  transport: string;
  enabled: boolean;
  editable: boolean;
  deletable: boolean;
  status: McpStatus;
  error: string;
  tool_count: number;
  tools: McpToolSummary[];
  missing_secrets: string[];
  env_refs: string[];
  header_refs: string[];
  /**
   * The stored env/header reference maps for workspace-origin servers — keys are
   * the real var/header names, values are the configured `${vault:NAME}` ref
   * strings or plain literals (never resolved secrets). Empty/absent for builtin
   * rows and on older backends that only return `env_refs`/`header_refs`.
   */
  env?: Record<string, string>;
  headers?: Record<string, string>;
  description: string;
  instruction: string;
  tool_exposure_mode: string | null;
  discovery_uses_secrets?: boolean;
  command: string | null;
  args: string[];
  url: string | null;
  config_version: number;
}

export interface EffectiveServerList {
  servers: EffectiveServer[];
  sandbox_running: boolean;
  max_servers: number;
  config_version: number;
  /**
   * The MCP config version the *running* session has actually applied (loaded
   * into the live agent), or null when no warm session exists. When this has
   * caught up to `config_version`, the latest config is live — the
   * version-accurate "synced" signal. Null/behind ⇒ "applying / will apply".
   */
  applied_config_version?: number | null;
  /**
   * True while the sandbox is transitioning *up* toward running (a proactive
   * MCP apply, or workspace entry, kicked a warm). Lets the UI keep polling and
   * show "Starting workspace…" through the stopped→running gap.
   */
  sandbox_warming?: boolean;
}

/** A user catalog template row (masked — only vault refs surfaced). */
export interface CatalogServer {
  name: string;
  transport: string;
  command: string | null;
  args: string[];
  url: string | null;
  env_refs: string[];
  header_refs: string[];
  description: string;
  instruction: string;
  tool_exposure_mode: string;
  discovery_uses_secrets?: boolean;
  created_at: string | null;
  updated_at: string | null;
}

/** Result of a discovery probe (POST /discover). */
export interface McpDiscoveryResult {
  server_name?: string;
  status: McpStatus;
  tools: McpToolSummary[];
  error: string;
  /** The per-server config fingerprint this snapshot was discovered under. */
  config_hash?: string;
  discovered_at?: string | null;
}

/** Response shape of GET /api/v1/mcp/servers (the user catalog list). */
export interface CatalogServerList {
  servers: CatalogServer[];
  max_servers: number;
}

// --- Per-workspace MCP ---

export async function getWorkspaceMcpServers(workspaceId: string): Promise<EffectiveServerList> {
  const { data } = await api.get<EffectiveServerList>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers`,
  );
  return data;
}

/** Add a server to a workspace — either a full def or `{ from_template }`. */
export async function addWorkspaceMcpServer(
  workspaceId: string,
  body: McpServerInput | { from_template: string },
) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/mcp/servers`, body);
  return data as { name: string; source: string; enabled: boolean };
}

export async function updateWorkspaceMcpServer(
  workspaceId: string,
  name: string,
  body: McpServerInput,
) {
  const { data } = await api.put(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}`,
    body,
  );
  return data as { name: string; source: string; enabled: boolean };
}

export async function setWorkspaceMcpServerEnabled(
  workspaceId: string,
  name: string,
  enabled: boolean,
) {
  const { data } = await api.patch(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/enabled`,
    { enabled },
  );
  return data as { name: string; enabled: boolean };
}

export async function deleteWorkspaceMcpServer(workspaceId: string, name: string) {
  const { data } = await api.delete(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}`,
  );
  return data as { ok: boolean };
}

export async function discoverWorkspaceMcpServer(
  workspaceId: string,
  name: string,
): Promise<McpDiscoveryResult> {
  const { data } = await api.post<{ server: McpDiscoveryResult }>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/discover`,
  );
  return data.server;
}

/** One per-server outcome from a bulk import. */
export interface McpImportResultRow {
  name: string;
  original_name: string;
  renamed: boolean;
  status: 'created' | 'exists' | 'skipped' | 'invalid' | 'error';
  reason?: string;
  error?: string;
}

export interface McpImportResult {
  results: McpImportResultRow[];
  created: number;
  /** Vault secret names auto-created from inline literal credentials. */
  secrets_created: string[];
  config_version: number;
}

/**
 * Bulk-import a standard `mcpServers` JSON blob. The backend coerces names,
 * maps transports, and auto-extracts inline literal secrets into the vault.
 * `payload` is the parsed JSON object (e.g. `{ mcpServers: { … } }`).
 */
export async function importWorkspaceMcpServers(
  workspaceId: string,
  payload: unknown,
): Promise<McpImportResult> {
  const { data } = await api.post<McpImportResult>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/import`,
    payload,
  );
  return data;
}

/**
 * Promote a workspace server UP into the user's reusable template catalog (the
 * inverse of `from_template`). Only `${vault:NAME}` reference names travel —
 * secret values are workspace-scoped, so the template surfaces `needs_secret`
 * when later added to another workspace. `overwrite` replaces an existing
 * same-named template; without it a clash is a 409.
 */
export async function promoteWorkspaceMcpServerToTemplate(
  workspaceId: string,
  name: string,
  overwrite = false,
): Promise<CatalogServer> {
  const { data } = await api.post<CatalogServer>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/promote`,
    { overwrite },
  );
  return data;
}

// --- User catalog (templates) ---

export async function getMcpCatalog(): Promise<CatalogServerList> {
  const { data } = await api.get<CatalogServerList>('/api/v1/mcp/servers');
  return { servers: data.servers ?? [], max_servers: data.max_servers ?? 20 };
}

export async function createMcpCatalogServer(body: McpServerInput): Promise<CatalogServer> {
  const { data } = await api.post<CatalogServer>('/api/v1/mcp/servers', body);
  return data;
}

export async function updateMcpCatalogServer(
  name: string,
  body: McpServerInput,
): Promise<CatalogServer> {
  const { data } = await api.put<CatalogServer>(`/api/v1/mcp/servers/${name}`, body);
  return data;
}

export async function deleteMcpCatalogServer(name: string) {
  const { data } = await api.delete(`/api/v1/mcp/servers/${name}`);
  return data as { ok: boolean };
}
