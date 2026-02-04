/**
 * ChatAgent API utilities
 * All backend endpoints used by the ChatAgent page
 */
import { api, headers, DEFAULT_USER_ID } from '@/api/client';

export { DEFAULT_USER_ID };

const baseURL = api.defaults.baseURL;

// --- Workspaces ---

export async function getWorkspaces(userId = DEFAULT_USER_ID, limit = 20, offset = 0) {
  const { data } = await api.get('/api/v1/workspaces', {
    params: { limit, offset },
    headers: headers(userId),
  });
  return data;
}

export async function createWorkspace(name, description = '', config = {}, userId = DEFAULT_USER_ID) {
  const { data } = await api.post('/api/v1/workspaces', { name, description, config }, { headers: headers(userId) });
  return data;
}

export async function deleteWorkspace(workspaceId) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const id = String(workspaceId).trim();
  if (!id) throw new Error('Workspace ID cannot be empty');
  await api.delete(`/api/v1/workspaces/${id}`);
}

// --- Conversations ---

export async function getConversations(userId = DEFAULT_USER_ID, limit = 50, offset = 0) {
  const { data } = await api.get('/api/v1/conversations', {
    params: { limit, offset },
    headers: headers(userId),
  });
  return data;
}

// --- Streaming (fetch + ReadableStream; axios not used) ---

async function streamFetch(url, opts, onEvent) {
  const res = await fetch(`${baseURL}${url}`, opts);
  if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
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
        onEvent(d);
      } catch (e) {
        console.warn('[api] SSE parse error', e, line);
      }
      ev = {};
    } else if (line.trim() === '') ev = {};
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    lines.forEach(processLine);
  }
  buffer.split('\n').forEach(processLine);
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
  userId = DEFAULT_USER_ID
) {
  const messages = [...messageHistory, { role: 'user', content: message }];
  await streamFetch(
    '/api/v1/chat/stream',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers(userId) },
      body: JSON.stringify({ workspace_id: workspaceId, thread_id: threadId, messages, plan_mode: planMode }),
    },
    onEvent
  );
}
