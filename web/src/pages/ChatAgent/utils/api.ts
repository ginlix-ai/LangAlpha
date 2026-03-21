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
  const token = data.session?.access_token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

// --- Workspaces ---

export async function getWorkspaces(limit: number = 20, offset: number = 0, sortBy: string = 'custom') {
  const { data } = await api.get('/api/v1/workspaces', {
    params: { limit, offset, sort_by: sortBy },
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

export async function reorderWorkspaces(items: Array<{ workspace_id: string; position: number }>) {
  if (!items?.length) throw new Error('Reorder items are required');
  await api.post('/api/v1/workspaces/reorder', { items });
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
export async function getWorkspaceThreads(workspaceId: string, limit: number = 20, offset: number = 0) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get('/api/v1/threads', {
    params: { workspace_id: workspaceId, limit, offset },
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

async function streamFetch(
  url: string,
  opts: RequestInit,
  onEvent: (event: Record<string, unknown>) => void
) {
  const res = await fetch(`${baseURL}${url}`, opts);
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
    // Handle 404 specifically for history replay (expected for new threads)
    if (res.status === 404 && url.includes('/replay')) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }
    throw new Error(`HTTP error! status: ${res.status}`);
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
    // Handle incomplete chunked encoding or other stream errors
    if (error instanceof Error && error.name === 'TypeError' && error.message.includes('network')) {
      console.warn('[api] Stream interrupted (network error):', error.message);
      disconnected = true;
    } else {
      throw error;
    }
  }
  return { disconnected };
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
  fastMode: boolean | null = null
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
    },
    onEvent
  );
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
 * Reconnect to an in-progress workflow stream (replays buffered events, then live stream)
 * @param {string} threadId - The thread ID to reconnect to
 * @param {number|null} lastEventId - Last received event ID for deduplication
 * @param {Function} onEvent - Callback for each SSE event
 */
export async function reconnectToWorkflowStream(
  threadId: string,
  lastEventId: number | null = null,
  onEvent: (event: Record<string, unknown>) => void = () => {}
) {
  if (!threadId) throw new Error('Thread ID is required');
  const queryParam = lastEventId != null ? `?last_event_id=${lastEventId}` : '';
  const authHeaders = await getAuthHeaders();
  return await streamFetch(
    `/api/v1/threads/${threadId}/messages/stream${queryParam}`,
    { method: 'GET', headers: { ...authHeaders } },
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
 * Soft-interrupt the workflow for a thread (pauses main agent, keeps subagents running)
 * @param {string} threadId - The thread ID to interrupt
 * @returns {Promise<Object>} Response data
 */
export async function softInterruptWorkflow(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/threads/${threadId}/interrupt`);
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
  agentMode: string = 'ptc'
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
    },
    onEvent
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

// --- Summarization ---

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
