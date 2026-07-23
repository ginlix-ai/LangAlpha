/**
 * Thread CRUD, sharing and compaction endpoints.
 */
import { api } from '@/api/client';

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

export interface MarketWatchResponse {
  thread_id: string;
  symbols: string[];
}

/**
 * Fetch the thread's current market-watch list (the tickers the agent is
 * live-stamping). Returns an empty `symbols` array when watch is off. Used to
 * seed the persistent watch chip on thread load and to refetch after a turn.
 */
export async function fetchMarketWatch(threadId: string): Promise<MarketWatchResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get<MarketWatchResponse>(`/api/v1/threads/${threadId}/market-watch`);
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
