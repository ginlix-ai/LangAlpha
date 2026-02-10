/**
 * ChatAgent API utilities
 * All backend endpoints used by the ChatAgent page
 */
import { api } from '@/api/client';
import { supabase } from '@/lib/supabase';

const baseURL = api.defaults.baseURL;

/** Get Bearer auth headers for raw fetch() calls (SSE streams). */
async function getAuthHeaders() {
  if (!supabase) return {};
  const { data } = await supabase.auth.getSession();
  const token = data.session?.access_token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

// --- Workspaces ---

export async function getWorkspaces(limit = 20, offset = 0) {
  const { data } = await api.get('/api/v1/workspaces', {
    params: { limit, offset },
  });
  return data;
}

export async function createWorkspace(name, description = '', config = {}) {
  const { data } = await api.post('/api/v1/workspaces', { name, description, config });
  return data;
}

export async function deleteWorkspace(workspaceId) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const id = String(workspaceId).trim();
  if (!id) throw new Error('Workspace ID cannot be empty');
  await api.delete(`/api/v1/workspaces/${id}`);
}

export async function getWorkspace(workspaceId) {
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

// --- Conversations ---

export async function getConversations(limit = 50, offset = 0) {
  const { data } = await api.get('/api/v1/conversations', {
    params: { limit, offset },
  });
  return data;
}

/**
 * Get all threads for a specific workspace
 * @param {string} workspaceId - The workspace ID
 * @param {number} limit - Maximum threads to return (default: 20)
 * @param {number} offset - Pagination offset (default: 0)
 * @returns {Promise<Object>} Response with threads array, total, limit, offset
 */
export async function getWorkspaceThreads(workspaceId, limit = 20, offset = 0) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/threads`, {
    params: { limit, offset },
  });
  return data;
}

/**
 * Delete a thread
 * @param {string} threadId - The thread ID to delete
 * @returns {Promise<Object>} Response with success, thread_id, and message
 */
export async function deleteThread(threadId) {
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
export async function updateThreadTitle(threadId, title) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.patch(`/api/v1/threads/${threadId}`, { title });
  return data;
}

// --- Streaming (fetch + ReadableStream; axios not used) ---

async function streamFetch(url, opts, onEvent) {
  const res = await fetch(`${baseURL}${url}`, opts);
  if (!res.ok) {
    // Handle 404 specifically for history replay (expected for new threads)
    if (res.status === 404 && url.includes('/replay')) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }
    throw new Error(`HTTP error! status: ${res.status}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let ev = {};
  const processLine = (line) => {
    if (line.startsWith('id: ')) ev.id = line.slice(4).trim();
    else if (line.startsWith('event: ')) ev.event = line.slice(7).trim();
    else if (line.startsWith('data: ')) {
      try {
        const d = JSON.parse(line.slice(6));
        if (ev.event) d.event = ev.event;
        if (ev.id != null) d._eventId = parseInt(ev.id, 10) || ev.id;
        onEvent(d);
      } catch (e) {
        console.warn('[api] SSE parse error', e, line);
      }
      ev = {};
    } else if (line.trim() === '') ev = {};
  };

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
  } catch (error) {
    // Handle incomplete chunked encoding or other stream errors
    if (error.name === 'TypeError' && error.message.includes('network')) {
      console.warn('[api] Stream interrupted (network error):', error.message);
      // Don't throw - allow the stream to complete gracefully
    } else {
      throw error;
    }
  }
}

export async function replayThreadHistory(threadId, onEvent = () => {}) {
  if (!threadId) throw new Error('Thread ID is required');
  await streamFetch(`/api/v1/threads/${threadId}/replay`, { method: 'GET' }, onEvent);
}

export async function sendChatMessageStream(
  message,
  workspaceId,
  threadId = '__default__',
  messageHistory = [],
  planMode = false,
  onEvent = () => {},
  additionalContext = null,
  agentMode = 'ptc',
  locale = 'en-US',
  timezone = 'America/New_York'
) {
  const messages = [...messageHistory, { role: 'user', content: message }];
  const body = {
    workspace_id: workspaceId,
    thread_id: threadId,
    messages,
    agent_mode: agentMode,
    plan_mode: planMode,
    locale,
    timezone,
  };
  if (additionalContext) {
    body.additional_context = additionalContext;
  }
  const authHeaders = await getAuthHeaders();
  await streamFetch(
    '/api/v1/chat/stream',
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
export async function getWorkflowStatus(threadId) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/workflow/${threadId}/status`);
  return data;
}

/**
 * Reconnect to an in-progress workflow stream (replays buffered events, then live stream)
 * @param {string} threadId - The thread ID to reconnect to
 * @param {number|null} lastEventId - Last received event ID for deduplication
 * @param {Function} onEvent - Callback for each SSE event
 */
export async function reconnectToWorkflowStream(threadId, lastEventId = null, onEvent = () => {}) {
  if (!threadId) throw new Error('Thread ID is required');
  const queryParam = lastEventId != null ? `?last_event_id=${lastEventId}` : '';
  const authHeaders = await getAuthHeaders();
  await streamFetch(
    `/api/v1/chat/stream/${threadId}/reconnect${queryParam}`,
    { method: 'GET', headers: { ...authHeaders } },
    onEvent
  );
}

/**
 * Soft-interrupt the workflow for a thread (pauses main agent, keeps subagents running)
 * @param {string} threadId - The thread ID to interrupt
 * @returns {Promise<Object>} Response data
 */
export async function softInterruptWorkflow(threadId) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/workflow/${threadId}/soft-interrupt`);
  return data;
}

/**
 * List files in a workspace sandbox
 * @param {string} workspaceId
 * @param {string} dirPath - e.g. "results"
 */
export async function listWorkspaceFiles(workspaceId, dirPath = 'results') {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files`, {
    params: { path: dirPath, include_system: false },
  });
  return data; // { workspace_id, path, files: [...] }
}

/**
 * Read a text file from workspace sandbox
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.md"
 */
export async function readWorkspaceFile(workspaceId, filePath) {
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
export async function downloadWorkspaceFile(workspaceId, filePath) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'blob',
  });
  return URL.createObjectURL(response.data);
}

/**
 * Download a file from workspace sandbox as ArrayBuffer (for client-side parsing)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function downloadWorkspaceFileAsArrayBuffer(workspaceId, filePath) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'arraybuffer',
  });
  return response.data;
}

/**
 * Trigger file download in browser
 * @param {string} workspaceId
 * @param {string} filePath
 */
export async function triggerFileDownload(workspaceId, filePath) {
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
export async function sendHitlResponse(workspaceId, threadId, hitlResponse, onEvent = () => {}, planMode = false) {
  const body = {
    workspace_id: workspaceId,
    thread_id: threadId,
    messages: [],
    hitl_response: hitlResponse,
    plan_mode: planMode,
  };
  const authHeaders = await getAuthHeaders();
  await streamFetch(
    '/api/v1/chat/stream',
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

export async function uploadWorkspaceFile(workspaceId, file, destPath = null, onProgress = null) {
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
