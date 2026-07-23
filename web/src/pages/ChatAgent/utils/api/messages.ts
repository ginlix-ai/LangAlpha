/**
 * Chat messaging: send/retry/HITL/cancel streams, workflow + report-back
 * status, thread watch, reconnect, turns, and the v2 thread mux.
 */
import { api } from '@/api/client';
import type { WorkflowRunStatus } from '@/types/api';
import { baseURL, getAuthHeaders, streamFetch, postSSEStream } from './transport';

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
  requestKey: string | null = null,
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
  if (requestKey) body.request_key = requestKey;
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
  return await postSSEStream(url, body, { onEvent, onRunIdResolved, signal });
}

/**
 * Retry the thread's latest failed run as a new attempt on the same turn
 * (v4 attempt chain). The backend validates the target is still the latest
 * attempt and resolves the retry checkpoint itself — no client-side
 * checkpoint fetch, no fork/truncation. Streams the same SSE contract as a
 * normal send.
 */
export async function sendRetryStream(
  workspaceId: string,
  threadId: string,
  onEvent: (event: Record<string, unknown>) => void = () => {},
  llmModel: string | null = null,
  reasoningEffort: string | null = null,
  fastMode: boolean | null = null,
  onRunIdResolved: ((runId: string, threadId: string | null) => void) | null = null,
  signal: AbortSignal | null = null,
  requestKey: string | null = null,
) {
  const body: Record<string, unknown> = { workspace_id: workspaceId };
  if (llmModel) body.llm_model = llmModel;
  if (reasoningEffort) body.reasoning_effort = reasoningEffort;
  if (fastMode) body.fast_mode = true;
  if (requestKey) body.request_key = requestKey;
  return await postSSEStream(`/api/v1/threads/${threadId}/retry`, body, { onEvent, onRunIdResolved, signal });
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
 * The report-back slice shared by {@link ReportBackStatusResponse} and
 * {@link WorkflowStatusResponse}. `pending_report_back` is TRI-STATE — decode it
 * with {@link decodeReportBackSignal}, never branch on the raw `boolean | null`.
 */
interface ThreadReportBackStatus {
  // true=pending, false=drained, null=the backend's own Redis read failed.
  pending_report_back: boolean | null;
  report_back_run_id: string | null;
  // Recently DRAINED report-back run ids, newest first (last ~10, 15-min TTL).
  // A drained turn's live pointer is deleted server-side, so this list is the
  // only way a client that missed the wake discovers the turn. Optional: older
  // backends omit it.
  recent_report_back_run_ids?: string[];
  // Live tail-subagent writers on a PTC thread (producer-undecided signal):
  // a task report-back only becomes `pending` once its subagent completes, so
  // while this list is non-empty an `idle` read must not be taken as drained.
  // Optional: older backends and the flash slice omit it.
  active_tasks?: string[];
}

// Decoding lives in a dependency-free module (usable where `./api` is mocked);
// re-exported here for the API boundary.
export { decodeReportBackSignal, shouldArmReportBack } from '../reportBackSignal';

export type { ReportBackSignal } from '../reportBackSignal';

/** Full workflow status for a thread (the `/status` response); the report-back
 *  fields are optional here (the full status may omit them). */
export type WorkflowStatusResponse = Partial<ThreadReportBackStatus> & {
  can_reconnect: boolean;
  // Backend's public run vocabulary, plus `'error'` — a client-only sentinel the
  // reconnect flow synthesizes when the `/status` fetch itself fails.
  status: WorkflowRunStatus | 'error';
  active_tasks?: string[];
  is_shared?: boolean;
  run_id?: string | null;
  // Highest persisted turn_index for the thread (terminal AND live threads).
  // null = no persisted turns (or the backend's DB read failed); absent on
  // older backends. The staleness signal for cached views whose missed run
  // already finished (can_reconnect=false carries no run_id to compare).
  latest_turn_index?: number | null;
  [key: string]: unknown;
};

/**
 * Get the current status of a workflow for a thread
 * @param {string} threadId - The thread ID to check
 * @returns {Promise<WorkflowStatusResponse>} Workflow status with can_reconnect, status, etc.
 */
export async function getWorkflowStatus(threadId: string): Promise<WorkflowStatusResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/status`);
  return data;
}

/** Cheap report-back-only slice of {@link getWorkflowStatus}. */
export interface ReportBackStatusResponse extends ThreadReportBackStatus {
  thread_id: string;
}

/**
 * Fetch only the report-back fields of a thread's status (`?fields=report_back`
 * skips the checkpoint / background-task / share reads the full status does).
 * This is the slice the report-back watch's reconcile loop polls.
 */
export async function getReportBackStatus(
  threadId: string,
): Promise<ReportBackStatusResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/status`, {
    params: { fields: 'report_back' },
  });
  return data;
}

/** One dispatched thread's liveness slice (the cheap, batchable status read). */
export interface DispatchLiveness {
  thread_id: string;
  status: string;
  run_id: string | null;
  can_reconnect: boolean;
}

/**
 * Batched dispatch liveness for a turn's PTC cards: one request resolves the
 * status + run_id of many dispatched threads at once, replacing N per-card
 * `/status` polls. Foreign/unknown/expired ids are omitted from the response.
 * Returns `[]` without a request for an empty id list.
 */
export async function getDispatchLiveness(
  threadIds: string[],
): Promise<DispatchLiveness[]> {
  if (!threadIds.length) return [];
  const { data } = await api.get('/api/v1/threads/dispatches/liveness', {
    params: { ids: threadIds.join(',') },
  });
  return (data?.liveness ?? []) as DispatchLiveness[];
}

// SSE event name of a report-back wake on GET /threads/{id}/watch. Contract:
// must match report_back.WAKE_EVENT (src/server/handlers/chat/report_back.py).
const REPORT_BACK_WAKE_EVENT = 'workflow_started';

// State-on-attach frame the backend emits once per /watch subscription (same
// JSON as /status?fields=report_back). Contract: report_back.SNAPSHOT_EVENT.
const WATCH_SNAPSHOT_EVENT = 'watch_snapshot';

/**
 * Watch a thread for new workflow activity via SSE (Redis pub/sub backed).
 * Returns an AbortController so the caller can close the connection.
 * Calls onWorkflowStarted(payload) when the backend signals a new workflow;
 * the payload carries the started run_id (e.g. a flash report-back run) so the
 * caller can attach to that exact run directly.
 * @param {string} threadId - The thread ID to watch
 * @param {Function} onWorkflowStarted - Callback when new workflow is detected
 * @param {Function} onClosed - Callback for a non-deliberate final close (backend
 *   timeout / drop / retries spent) — never after a caller-initiated abort
 * @param {Function} onResubscribed - Callback each time the IN-LOOP retry lands a
 *   fresh subscription after a transient error; wakes published during that gap
 *   are lost (pub/sub, no replay), so the caller should run a catch-up pull.
 *   Distinct from onClosed, which still fires exactly once at the final close.
 * @param {Function} onSnapshot - Callback for the state-on-attach frame the
 *   backend emits once per subscription; carries the report-back status slice,
 *   making every (re)subscribe gapless without a /status round-trip.
 * @returns {{ abort: AbortController }} - Call abort.abort() to stop watching
 */
export function watchThread(
  threadId: string,
  onWorkflowStarted: (
    payload?: { run_id?: string | null; needs_input?: string | null; cleared?: boolean },
  ) => void | Promise<void>,
  onClosed?: () => void,
  onResubscribed?: () => void,
  onSnapshot?: (status: ReportBackStatusResponse) => void | Promise<void>,
): { abort: AbortController } {
  const abort = new AbortController();
  const MAX_RETRIES = 2;

  (async () => {
    try {
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

          // A retry attempt (not the initial subscribe) just re-established the
          // stream: surface it so the caller can reconcile the gap. Fired AFTER
          // the response is known good, so a hard-failing endpoint (the `return`
          // above) never reports a phantom recovery.
          if (attempt > 0) onResubscribed?.();

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
            // Per the SSE spec, multiple data: lines join with a newline —
            // collect them all (mirroring streamFetch above) so a multi-line
            // payload stays parseable instead of truncating to the first line
            // and corrupting the JSON. Backend frames are single-line today;
            // this is resilience.
            const parseFrameData = (frame: string): unknown => {
              const dataLines: string[] = [];
              for (const raw of frame.split('\n')) {
                if (raw.startsWith('data:')) dataLines.push(raw.slice(5).trim());
              }
              if (!dataLines.length) return null;
              try {
                return JSON.parse(dataLines.join('\n'));
              } catch {
                return null; // payload-less / malformed — caller falls back to /status
              }
            };
            let sep: number;
            while ((sep = buffer.indexOf('\n\n')) >= 0) {
              const frame = buffer.slice(0, sep);
              buffer = buffer.slice(sep + 2);
              // State-on-attach snapshot: deliver the status slice directly.
              if (frame.includes(`event: ${WATCH_SNAPSHOT_EVENT}`)) {
                const snapshot = parseFrameData(frame);
                if (snapshot) await onSnapshot?.(snapshot as ReportBackStatusResponse);
                continue;
              }
              // Skip keepalive pings / timeout frames — only the wake carries a run_id.
              if (!frame.includes(`event: ${REPORT_BACK_WAKE_EVENT}`)) continue;
              // Pull the run_id out of the event's data line so the caller can
              // attach to that exact run without a /status round-trip.
              const payload = (parseFrameData(frame) ?? {}) as {
                run_id?: string | null;
                needs_input?: string | null;
                cleared?: boolean;
              };
              // PERSISTENT: do NOT cancel + return after the first wake — N
              // dispatched PTCs wake separately, and a re-subscribe would lose
              // wake #2+. Awaiting `onWorkflowStarted` (which blocks until that
              // run's stream finishes) naturally serializes the chain.
              await onWorkflowStarted({
                run_id: payload.run_id ?? null,
                needs_input: payload.needs_input ?? null,
                cleared: payload.cleared === true,
              });
            }
          }
          return; // Backend closed the stream (30-min cap / disconnect).
        } catch (err: unknown) {
          if ((err as Error).name === 'AbortError') return;
          if (attempt < MAX_RETRIES) {
            await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
          }
        }
      }
    } finally {
      // Signal a non-deliberate close (backend timeout / drop / retries spent) so
      // the caller can clear its abort ref and let a future re-arm re-subscribe.
      // A caller-initiated abort already tore everything down — skip it.
      if (!abort.signal.aborted) onClosed?.();
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
 * @returns {Promise<{thread_id: string, turns: Array<{turn_index: number, edit_checkpoint_id: string|null, regenerate_checkpoint_id: string}>}>}
 */
export async function fetchThreadTurns(threadId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/turns`);
  return data;
}

/**
 * Raw line-oriented reader for the multiplexed thread stream
 * (`GET /threads/{id}/stream?contract=v2`). The mux client parses SSE blocks
 * itself — it needs the `run:<run_id>#<entry_id>` cursor id line that
 * streamFetch's parser would mangle — so this helper only owns transport:
 * base URL, auth headers, abort, and line splitting.
 * Resolves on server close; throws on HTTP error or network failure.
 */
export async function openThreadMuxStream(
  threadId: string,
  cursors: string | null,
  onLine: (line: string) => void,
  signal: AbortSignal,
  sinceAgeS = 0,
): Promise<void> {
  if (!threadId) throw new Error('Thread ID is required');
  const authHeaders = await getAuthHeaders();
  let qs = cursors ? `&cursors=${encodeURIComponent(cursors)}` : '';
  // Knowledge-horizon age: how far the client's snapshot/last-frame lags
  // this connect. The server widens its settled-run catch-up window by it.
  if (sinceAgeS > 0) qs += `&since_age_s=${Math.ceil(sinceAgeS)}`;
  const res = await fetch(
    `${baseURL}/api/v1/threads/${threadId}/stream?contract=v2${qs}`,
    {
      method: 'GET',
      headers: { ...authHeaders },
      signal,
    },
  );
  if (!res.ok) {
    const err: Error & { status?: number } = new Error(
      `mux stream HTTP ${res.status}`,
    );
    err.status = res.status;
    throw err;
  }
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    for (const line of lines) onLine(line);
  }
  if (buffer) onLine(buffer);
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
 * Durable terminal state of a single subagent task from the run ledger.
 * Used to hydrate a stale detail view (a settled task whose live stream
 * drained while the tab was backgrounded) without a full thread reload.
 * @returns { task_id, status, error } — status/error null when no ledgered run.
 */
export async function getSubagentTaskStatus(
  threadId: string,
  taskId: string,
): Promise<{ task_id: string; status: string | null; error: string | null }> {
  const { data } = await api.get(
    `/api/v1/threads/${threadId}/tasks/${taskId}/status`,
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
  requestKey: string | null = null,
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
  if (requestKey) body.request_key = requestKey;
  return await postSSEStream(`/api/v1/threads/${threadId}/messages`, body, { onEvent, onRunIdResolved, signal });
}
