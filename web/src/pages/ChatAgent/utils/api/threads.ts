/**
 * Thread CRUD, sharing and compaction endpoints.
 */
import { api, type RequestConfig } from '@/api/client';
import { authSessionCheck } from '@/lib/authResets';
import type { Thread } from '@/types/api';
import type { WakeupCancelResponse, WakeupsResponse } from '@/types/wakeups';
import type { CommandStopResponse, CommandsResponse } from '@/types/commands';

export interface ThreadCreatedInfo {
  thread_id: string;
  workspace_id: string;
  thread_index: number;
  title: string;
  msg_type: string;
  /** The tuning overrides the create stored, echoed back; null follows Settings. */
  reasoning_effort?: string | null;
  fast_mode?: boolean | null;
}

/**
 * Pre-create a thread via POST /api/v1/threads — one round-trip, plain 201.
 *
 * Resolves with the created row so the caller can navigate and paint the
 * sidebar row immediately. The generated title is NOT awaited here: it lands
 * on the lifecycle feed's `thread_title` event (and on the next list refetch),
 * which is the single delivery path for it. A `title` the caller names is
 * stored as given and none is generated. Rejects on HTTP failure or timeout —
 * callers fall back to the legacy `__default__` send path.
 */
export async function createThreadWithTitle(opts: {
  workspaceId?: string | null;
  /** May be empty when `title` is given. */
  firstQuery: string;
  /** The thread's title, in place of one generated from `firstQuery`. */
  title?: string;
  platform?: string | null;
  /** Lets title generation resolve relative dates ("today") on the user's
   *  wall clock; the server reads UTC without it. */
  timezone?: string;
  /** Left out, the thread stores no value and follows the owner's default. */
  subagentsAllowed?: boolean;
  /** The thread's own effort and fast mode, picked before it existed. Left
   *  out, the thread follows Settings. */
  reasoningEffort?: string;
  fastMode?: boolean;
  timeoutMs?: number;
}): Promise<ThreadCreatedInfo> {
  const {
    workspaceId, firstQuery, title, platform, timezone, subagentsAllowed, reasoningEffort, fastMode, timeoutMs = 8000,
  } = opts;

  const body: Record<string, unknown> = { first_query: firstQuery };
  if (title) body.title = title;
  if (workspaceId) body.workspace_id = workspaceId;
  if (platform) body.platform = platform;
  if (timezone) body.timezone = timezone;
  if (subagentsAllowed !== undefined) body.subagents_allowed = subagentsAllowed;
  if (reasoningEffort !== undefined) body.reasoning_effort = reasoningEffort;
  if (fastMode !== undefined) body.fast_mode = fastMode;

  const { data } = await api.post<ThreadCreatedInfo>('/api/v1/threads', body, { timeout: timeoutMs });
  return data;
}

/**
 * Get a single thread by ID (used to resolve workspace_id on direct URL access)
 * @param {string} threadId - The thread ID
 * @returns {Promise<Object>} Thread object with workspace_id, thread_id, title, etc.
 */
export async function getThread(threadId: string): Promise<Thread> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get<Thread>(`/api/v1/threads/${threadId}`);
  return data;
}

export interface MarketWatchResponse {
  thread_id: string;
  symbols: string[];
}

/**
 * Fetch the thread's current market-watch list (the tickers the agent is
 * live-stamping). Returns an empty `symbols` array when watch is off. Used
 * to seed the persistent watch chip on thread load and to refetch after a
 * turn.
 */
export async function fetchMarketWatch(threadId: string): Promise<MarketWatchResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get<MarketWatchResponse>(`/api/v1/threads/${threadId}/market-watch`);
  return data;
}

/** The thread's wakeups: pending ones, those that ended in the last day, and
 *  price targets set before wakeups existed. */
export async function fetchWakeups(threadId: string): Promise<WakeupsResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get<WakeupsResponse>(`/api/v1/threads/${threadId}/wakeups`);
  return data;
}

/** Cancel the named wakeups, or every one with `'all'`. The answer carries each
 *  outcome and the listing as it stands, so a `too_late` one shows where it is;
 *  the listing is null when the server could not read it after the cancel. */
export async function cancelWakeups(
  threadId: string,
  which: readonly string[] | 'all',
): Promise<WakeupCancelResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const body = which === 'all' ? { all: true } : { wakeup_ids: which };
  const { data } = await api.post<WakeupCancelResponse>(`/api/v1/threads/${threadId}/wakeups/cancel`, body);
  return data;
}

/** The thread's background commands: those still running and those that
 *  ended, newest first. */
export async function fetchCommands(threadId: string): Promise<CommandsResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get<CommandsResponse>(`/api/v1/threads/${threadId}/commands`);
  return data;
}

/** Stop a background command. A stopped command does not report back. */
export async function stopCommand(threadId: string, commandId: string): Promise<CommandStopResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post<CommandStopResponse>(
    `/api/v1/threads/${threadId}/commands/${encodeURIComponent(commandId)}/stop`,
  );
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
  archived: boolean = false,
) {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const params: Record<string, string | number | boolean> = { workspace_id: workspaceId, limit, offset };
  if (platformPrefix) params.platform_prefix = platformPrefix;
  // Only sent when true — the hot path never carries archived rows.
  if (archived) params.archived = true;
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

export interface ThreadUpdates {
  title?: string | null;
  is_pinned?: boolean;
  archived?: boolean;
  /** `null` returns the thread to the account default. A name the server
   *  cannot resolve is refused with a 400. */
  llm_model?: string | null;
  /** Whether the agent may hand work to subagents on this thread. `null`
   *  returns it to following the user's default. */
  subagents_allowed?: boolean | null;
  /** The thread's own reasoning effort, stored as sent. `null` returns it to
   *  following Settings. */
  reasoning_effort?: string | null;
  /** The thread's own fast mode, stored as sent. `null` returns it to
   *  following Settings. */
  fast_mode?: boolean | null;
}

/**
 * Update user-editable thread fields (title, pin, archive, model, subagents,
 * effort, fast mode) via PATCH.
 * Only the keys present in `updates` are applied server-side.
 */
export async function updateThread(threadId: string, updates: ThreadUpdates, config?: RequestConfig): Promise<Thread> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.patch<Thread>(`/api/v1/threads/${threadId}`, updates, config);
  return data;
}

/**
 * A queue per thread: each write starts once the one before it settles, so a
 * thread's writes reach the server in the order made, from whichever surface
 * makes them, and an earlier one can never land last. A write still waiting
 * when the user signs out is dropped rather than sent under the next session.
 */
function perThreadQueue() {
  const tails = new Map<string, Promise<unknown>>();
  return <T>(threadId: string, run: () => Promise<T>): Promise<T> => {
    const sameSession = authSessionCheck();
    const write = (tails.get(threadId) ?? Promise.resolve()).then(() => {
      if (!sameSession()) throw new Error('Signed out before the write was sent');
      return run();
    });
    const settled = write.catch(() => undefined);
    tails.set(threadId, settled);
    void settled.then(() => {
      if (tails.get(threadId) === settled) tails.delete(threadId);
    });
    return write;
  };
}

const titleWrites = perThreadQueue();
const shareWrites = perThreadQueue();
// A queued write that never answers would hold back every write behind it, so
// each one gives up after this long and lets the next go.
const QUEUED_WRITE_TIMEOUT_MS = 30_000;

/**
 * Update a thread's title, after any title write before it (the sidebar's
 * field and the gallery's dialog both write it).
 * @param {string} threadId - The thread ID to update
 * @param {string} title - New thread title (max 255 chars, can be null to clear)
 * @returns {Promise<Object>} Updated thread object
 */
export function updateThreadTitle(threadId: string, title: string | null) {
  return titleWrites(threadId, () => updateThread(threadId, { title }, { timeout: QUEUED_WRITE_TIMEOUT_MS }));
}

// --- Streaming (fetch + ReadableStream; the API client does not stream) ---

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
 * Update sharing settings for a thread, after any sharing write before it
 * (the chat header's panel and the sidebar's dialog both write them).
 * @param {string} threadId
 * @param {Object} body - { is_shared: bool, permissions?: { allow_files?: bool, allow_download?: bool } }
 * @returns {Promise<Object>} { is_shared, share_token, share_url, permissions }
 */
export function updateThreadSharing(threadId: string, body: Record<string, unknown>) {
  if (!threadId) return Promise.reject(new Error('Thread ID is required'));
  return shareWrites(threadId, async () => {
    const { data } = await api.post(`/api/v1/threads/${threadId}/share`, body, { timeout: QUEUED_WRITE_TIMEOUT_MS });
    return data;
  });
}

// --- Compaction ---

// The endpoint path `/summarize` and the `summarizeThread` function name are
// preserved for REST contract compatibility.

export async function summarizeThread(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/threads/${threadId}/summarize`);
  return data;
}

export async function offloadThread(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.post(`/api/v1/threads/${threadId}/offload`);
  return data;
}

// --- Skills ---
