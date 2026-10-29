/**
 * Chat messaging: send/retry/HITL/cancel streams, workflow + report-back
 * status, thread watch, reconnect, turns, and the v2 thread mux.
 */
import { api } from '@/api/client';
import type { WorkflowRunStatus } from '@/types/api';
import type { CancelOutcome, HeldSteer } from '../cancelOutcome';
import { bearerTokenOf, refreshAccessToken } from '@/lib/authToken';
import { baseURL, getAuthHeaders, streamFetch, postSSEStream } from './transport';

/**
 * Which part of the history a replay covers. With neither bound the server
 * replays the whole thread; with either it opens on a `history_page` event
 * saying where the page starts and whether older turns remain.
 */
export interface ReplayPageRequest {
  /** The newest this many checkpointed turns (before `beforeTurn` if given). */
  limit?: number;
  /** Only turns older than this one: the previous page's first turn. */
  beforeTurn?: number;
  signal?: AbortSignal;
}

export async function replayThreadHistory(
  threadId: string,
  onEvent: (event: Record<string, unknown>) => void = () => {},
  { limit, beforeTurn, signal }: ReplayPageRequest = {},
) {
  if (!threadId) throw new Error('Thread ID is required');
  const params = new URLSearchParams();
  if (limit != null) params.set('limit', String(limit));
  if (beforeTurn != null) params.set('before_turn', String(beforeTurn));
  const query = params.toString();
  const authHeaders = await getAuthHeaders();
  return await streamFetch(
    `/api/v1/threads/${threadId}/messages/replay${query ? `?${query}` : ''}`,
    { method: 'GET', headers: { ...authHeaders }, signal },
    onEvent,
  );
}

/**
 * Thread settings stored on the thread a send creates. A send to an existing
 * thread leaves them alone: there they change only through PATCH /threads/{id}.
 */
export interface SendThreadSettings {
  subagentsAllowed?: boolean;
}

type OnStreamEvent = (event: Record<string, unknown>) => void;

/** What a send and a resume both carry besides their message. */
interface StreamTurnOptions {
  onEvent?: OnStreamEvent;
  llmModel?: string | null;
  reasoningEffort?: string | null;
  fastMode?: boolean | null;
  /** Called with the run id from the response headers, before the first
   *  event, so a disconnect that early still knows which run to rejoin. */
  onRunIdResolved?: ((runId: string, threadId: string | null) => void) | null;
  signal?: AbortSignal | null;
  requestKey?: string | null;
}

export interface SendChatMessageOptions extends StreamTurnOptions {
  additionalContext?: Record<string, unknown>[] | string | null;
  locale?: string;
  timezone?: string;
  checkpointId?: string | null;
  forkFromTurn?: number | null;
  /** On a fork, the thread's newest run as this view saw it: the server
   *  refuses the fork as stale when another run came since. */
  expectedLatestRunId?: string | null;
  platform?: string | null;
  threadSettings?: SendThreadSettings;
  /** Only steer a running turn: with none running the server refuses the
   *  send as `not_running` instead of starting one. */
  steerOnly?: boolean;
}

export type SendHitlResponseOptions = StreamTurnOptions;

// A steer's POST answers once admission decides: at once while a turn runs,
// but held while a compaction finishes (the server's
// `admission_compaction_wait_timeout`, 260s by default) or a stopping turn
// tears down. The bound sits past those so a held steer is not taken for a
// failed one, and a link that never answers fails it, through the failed
// path, rather than holding the refused steers queued behind it forever.
const STEER_ANSWER_TIMEOUT_MS = 300_000;

/** `signal` with a deadline. Rejects as a `TimeoutError`, which no caller reads as a stop. */
function withDeadline(signal: AbortSignal | null, ms: number): AbortSignal {
  const deadline = AbortSignal.timeout(ms);
  if (!signal) return deadline;
  const either = new AbortController();
  for (const s of [signal, deadline]) {
    if (s.aborted) either.abort(s.reason);
    else s.addEventListener('abort', () => either.abort(s.reason), { once: true });
  }
  return either.signal;
}

export async function sendChatMessageStream(
  message: string,
  workspaceId: string,
  threadId: string | null = null,
  {
    onEvent = () => {},
    additionalContext = null,
    locale = 'en-US',
    timezone = 'America/New_York',
    checkpointId = null,
    forkFromTurn = null,
    expectedLatestRunId = null,
    llmModel = null,
    reasoningEffort = null,
    fastMode = null,
    platform = null,
    onRunIdResolved = null,
    signal = null,
    requestKey = null,
    threadSettings = {},
    steerOnly = false,
  }: SendChatMessageOptions = {},
) {
  // For checkpoint replay (regenerate/retry), send empty messages
  const messages = checkpointId && !message ? [] : [{ role: 'user', content: message }];
  const body: Record<string, unknown> = {
    workspace_id: workspaceId,
    messages,
    locale,
    timezone,
  };
  if (requestKey) body.request_key = requestKey;
  if (steerOnly) body.steer_only = true;
  if (additionalContext) {
    body.additional_context = additionalContext;
  }
  if (checkpointId) {
    body.checkpoint_id = checkpointId;
  }
  if (forkFromTurn != null) {
    body.fork_from_turn = forkFromTurn;
    if (expectedLatestRunId) body.expected_latest_run_id = expectedLatestRunId;
  }
  if (llmModel) body.llm_model = llmModel;
  if (reasoningEffort) body.reasoning_effort = reasoningEffort;
  if (fastMode != null) body.fast_mode = fastMode;
  if (platform) body.platform = platform;
  if (threadSettings.subagentsAllowed !== undefined) body.subagents_allowed = threadSettings.subagentsAllowed;
  // Use /threads/{id}/messages for existing thread, /threads/messages for new
  const isNewThread = !threadId || threadId === '__default__';
  const url = isNewThread
    ? '/api/v1/threads/messages'
    : `/api/v1/threads/${threadId}/messages`;
  return await postSSEStream(url, body, {
    onEvent,
    onRunIdResolved,
    signal: steerOnly ? withDeadline(signal, STEER_ANSWER_TIMEOUT_MS) : signal,
  });
}

/**
 * Retry the thread's latest failed run as a new attempt on the same turn
 * (v4 attempt chain). The backend validates the target is still the latest
 * attempt and resolves the retry checkpoint itself — no client-side
 * checkpoint fetch, no fork/truncation. Streams the same SSE contract as a
 * normal send. `runId` names the failed run, so the server refuses it (409)
 * rather than retry a different turn's attempt. `additionalContext` is the
 * failed send's, which the server uses only when that message runs again.
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
  runId: string | null = null,
  additionalContext: Record<string, unknown>[] | null = null,
) {
  const body: Record<string, unknown> = { workspace_id: workspaceId };
  if (llmModel) body.llm_model = llmModel;
  if (reasoningEffort) body.reasoning_effort = reasoningEffort;
  if (fastMode != null) body.fast_mode = fastMode;
  if (requestKey) body.request_key = requestKey;
  if (runId) body.run_id = runId;
  if (additionalContext) body.additional_context = additionalContext;
  return await postSSEStream(`/api/v1/threads/${threadId}/retry`, body, { onEvent, onRunIdResolved, signal });
}

export { isNoOpCancellation } from '../cancelOutcome';
export type { CancelNoOpState, CancelOutcome, HeldSteer } from '../cancelOutcome';

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
 * Resolving is not proof of a stop — read {@link CancelOutcome.state} before
 * treating the turn as cancelled.
 *
 * @param {string} threadId - The thread ID to cancel
 * @param {string|null} runId - The specific run to cancel; null = latest active
 */
export async function cancelWorkflow(
  threadId: string,
  runId: string | null = null,
): Promise<CancelOutcome> {
  if (!threadId) throw new Error('Thread ID is required');
  // Bound the request: the shared API client sets no global timeout, so a
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
  // Optional: older backends and a dispatching thread's slice omit it.
  active_tasks?: string[];
  // The thread's background commands still running. Each one's end can start
  // a turn of its own, so they keep the watch armed, but owe nothing the tray
  // names: their rows are there. Optional: older backends omit it.
  active_commands?: string[];
  // The dispatched threads still owed, oldest first; null when the read
  // failed. [] can come with a pending report: a PTC thread's slice names the
  // analysts its hand-offs went to but not its own subagents, and a thread in
  // a deleted workspace or another owner's has no label. Optional: older
  // backends omit it.
  owed_threads?: ReportBackOwedThread[] | null;
  // Whose reports are owed, so the tray names what it waits on; null when the
  // read failed. Optional: older backends omit it.
  owed_report_kinds?: OwedReportKind[] | null;
}

/** Whose report a thread is owed: a subagent's, a background command's, or
 *  an Analyst's. */
export type OwedReportKind = 'task' | 'command' | 'analyst';

/** What a status read in hand says is owed, which an arm seeds the watch with. */
export type OwedReportsRead = Pick<Partial<ThreadReportBackStatus>, 'owed_threads' | 'owed_report_kinds'>;

/** A dispatched thread that has yet to report back to the thread that sent it. */
export interface ReportBackOwedThread {
  thread_id: string;
  workspace_id: string;
  workspace_name: string;
  title: string | null;
}

// Decoding lives in a dependency-free module (usable where `./api` is mocked);
// re-exported here for the API boundary.
export { decodeReportBackSignal, shouldArmReportBack } from '../reportBackSignal';

export type { ReportBackSignal } from '../reportBackSignal';

/** Tokens a thread has used, as the server counts them. */
export type ThreadTokenTotals = { input_tokens: number; output_tokens: number };

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
  // Tokens the thread has used, over every model call of every run except the
  // live one, whose calls the reconnect replays from its stream. null when the
  // backend's read failed.
  token_totals?: ThreadTokenTotals | null;
  // The user's steers an errored turn left queued, oldest first: the next
  // turn on the thread reads them. Absent on older backends.
  pending_steers?: HeldSteer[];
  [key: string]: unknown;
};

/**
 * Get the current status of a workflow for a thread
 * @param {string} threadId - The thread ID to check
 * @param {number} [options.timeout] - Bound the read; the shared API client sets none
 * @returns {Promise<WorkflowStatusResponse>} Workflow status with can_reconnect, status, etc.
 */
export async function getWorkflowStatus(
  threadId: string,
  options: { timeout?: number } = {},
): Promise<WorkflowStatusResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/status`, { timeout: options.timeout });
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
  /** ISO start of the thread's latest run, live or settled. */
  run_started_at?: string | null;
  /** ISO time that run settled; null while it is live. */
  run_finished_at?: string | null;
  /** ISO time the thread's latest turn started or settled, whoever sent it. */
  last_activity_at?: string | null;
  /** The thread's title; null until it is named. */
  title?: string | null;
  /** The current name of the thread's workspace. */
  workspace_name?: string | null;
}

/** A run read by its id beside the threads: one asked for, or the run that
 *  took in a steer asked for. */
export interface DispatchRunLiveness {
  run_id: string;
  thread_id: string;
  /** The same public vocabulary as a thread's row. */
  status: string;
  run_started_at?: string | null;
  /** Null while the run is live. */
  run_finished_at?: string | null;
}

/**
 * A hand-off's steer into a run already going, read by its id: `taken` by the
 * run it names, whose view is in `runs` whether it was asked for or not,
 * `queued` until a run takes it in, or `dropped` once it settled with none.
 */
export interface DispatchSteerLiveness {
  steer_id: string;
  thread_id: string;
  state: 'taken' | 'queued' | 'dropped';
  /** Set exactly when the steer was taken. */
  run_id: string | null;
}

/** One batched read: each thread's latest run, the runs asked for by id with
 *  the run of each steer taken, and the steers asked for. */
export interface HandoffLiveness {
  liveness: DispatchLiveness[];
  runs: DispatchRunLiveness[];
  steers: DispatchSteerLiveness[];
}

/** What one liveness read asks about, by id: threads for their latest run,
 *  runs on their own, and steers for the run that took each in. */
export interface LivenessAsk {
  threads: readonly string[];
  runs: readonly string[];
  steers: readonly string[];
}

/** The most ids of each kind the liveness route reads per request; it drops the rest. */
const LIVENESS_IDS_PER_REQUEST = 100;

/**
 * Batched liveness for a turn's hand-offs: one request resolves the latest run
 * of many dispatched threads, and any runs and steers named by id, at once,
 * replacing N per-card `/status` polls. Foreign/unknown/expired ids are
 * omitted from the response. Resolves empty without a request when nothing is
 * asked.
 */
export async function getHandoffLiveness({ threads, runs, steers }: LivenessAsk): Promise<HandoffLiveness> {
  const batches = Math.ceil(Math.max(threads.length, runs.length, steers.length) / LIVENESS_IDS_PER_REQUEST);
  const pages = await Promise.all(Array.from({ length: batches }, async (_, i) => {
    const start = i * LIVENESS_IDS_PER_REQUEST;
    const page = (all: readonly string[]) => all.slice(start, start + LIVENESS_IDS_PER_REQUEST).join(',');
    const [ids, runIds, steerIds] = [page(threads), page(runs), page(steers)];
    const { data } = await api.get('/api/v1/threads/dispatches/liveness', {
      params: {
        ...(ids ? { ids } : {}),
        ...(runIds ? { runs: runIds } : {}),
        ...(steerIds ? { steers: steerIds } : {}),
      },
    });
    return {
      liveness: (data?.liveness ?? []) as DispatchLiveness[],
      runs: (data?.runs ?? []) as DispatchRunLiveness[],
      steers: (data?.steers ?? []) as DispatchSteerLiveness[],
    };
  }));
  return {
    liveness: pages.flatMap((p) => p.liveness),
    runs: pages.flatMap((p) => p.runs),
    steers: pages.flatMap((p) => p.steers),
  };
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
 * the payload carries the started run_id (e.g. a Chief of Staff report-back run) so the
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
  // One rotation per subscription, so a token the server keeps refusing cannot
  // turn the retry budget into a burst of them.
  let authRetried = false;

  (async () => {
    try {
      // The auth rotation below rides this loop but must not be paid for out of
      // it: spending an iteration on it would leave a genuinely flaky network
      // one retry instead of `MAX_RETRIES`. `authRetried` caps the extension at
      // one, so the loop still terminates. Counted as an extra turn rather than
      // by winding `attempt` back, because `attempt` is also what tells a
      // re-subscribe from the first one for `onResubscribed`.
      const lastAttempt = () => MAX_RETRIES + (authRetried ? 1 : 0);
      for (let attempt = 0; attempt <= lastAttempt(); attempt++) {
        if (abort.signal.aborted) return;
        try {
          const authHeaders = await getAuthHeaders();
          const res = await fetch(`${baseURL}/api/v1/threads/${threadId}/watch`, {
            method: 'GET',
            headers: { ...authHeaders },
            signal: abort.signal,
          });

          // `fetch` does not throw on an HTTP status, so an auth failure never
          // reaches the catch below and never spends a retry -- the loop's
          // budget is only ever available to network errors. Rotate the token
          // this request actually carried and re-subscribe with whatever comes
          // back. When nothing better is available the fall-through below ends
          // the subscription, and the caller's own paced re-subscribe takes it
          // from there rather than this loop spinning on a refused token.
          if (res.status === 401 && !authRetried) {
            authRetried = true;
            if (await refreshAccessToken(bearerTokenOf(authHeaders.Authorization))) continue;
          }

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
          if (attempt < lastAttempt()) {
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
  openTurn = false,
): Promise<void> {
  if (!threadId) throw new Error('Thread ID is required');
  const authHeaders = await getAuthHeaders();
  let qs = cursors ? `&cursors=${encodeURIComponent(cursors)}` : '';
  // Knowledge-horizon age: how far the client's snapshot/last-frame lags
  // this connect. The server widens its settled-run catch-up window by it.
  if (sinceAgeS > 0) qs += `&since_age_s=${Math.ceil(sinceAgeS)}`;
  // Drain the executing turn's settled runs, which no history load holds.
  if (openTurn) qs += '&open_turn=true';
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
 * @param {string} inputId - The caller's id for it, echoed by the frame that delivers or returns it
 * @returns {Promise<Object>} { success, tool_call_id, display_id, queue_position, input_id }
 */
export async function sendSubagentMessage(threadId: string, taskId: string, content: string, inputId: string) {
  if (!threadId) throw new Error('Thread ID is required');
  if (!taskId) throw new Error('Task ID is required');
  const { data } = await api.post(
    `/api/v1/threads/${threadId}/tasks/${taskId}/messages`,
    { content, input_id: inputId }
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
): Promise<{
  task_id: string;
  status: string | null;
  error: string | null;
  last_activity_ms?: number | null;
  /** The latest run's id, which its facts are filed under beside the ones
   *  the stream and replay found. */
  run_id?: string | null;
  /** The latest run's ledger bounds (epoch ms); no finish while it runs. */
  started_ms?: number | null;
  finished_ms?: number | null;
  /** That run's model, present only when it was not its launcher's. */
  model?: { id: string; provider: string };
}> {
  const { data } = await api.get(
    `/api/v1/threads/${threadId}/tasks/${taskId}/status`,
  );
  return data;
}

/**
 * On-demand checkpoint transcript for one subagent task. Serves drill-in
 * views for tasks replay never projects as lanes (workflow children).
 * Items are the same SSE-shaped events replay emits.
 */
export async function getSubagentTaskHistory(
  threadId: string,
  taskId: string,
): Promise<{
  task_id: string;
  items: Array<{ event: string; data: Record<string, unknown> }>;
}> {
  const { data } = await api.get(
    `/api/v1/threads/${threadId}/tasks/${taskId}/history`,
  );
  return data;
}

/** Stop one background task (e.g. a workflow run) without cancelling the turn. */
export async function cancelSubagentTask(
  threadId: string,
  taskId: string,
): Promise<CancelOutcome> {
  const { data } = await api.post(
    `/api/v1/threads/${threadId}/tasks/${taskId}/cancel`,
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
 * Resume an interrupted workflow with the user's answers (a question, a
 * proposal, a tool approval, a credit pause), keyed by interrupt id, e.g.
 * `{ [interruptId]: { decisions: [{ type: 'approve' }] } }`.
 */
export async function sendHitlResponse(
  hitlResponse: Record<string, unknown>,
  workspaceId: string,
  threadId: string,
  {
    onEvent = () => {},
    llmModel = null,
    reasoningEffort = null,
    fastMode = null,
    onRunIdResolved = null,
    signal = null,
    requestKey = null,
  }: SendHitlResponseOptions = {},
) {
  const body: Record<string, unknown> = {
    workspace_id: workspaceId,
    messages: [],
    hitl_response: hitlResponse,
  };
  if (llmModel) body.llm_model = llmModel;
  if (reasoningEffort) body.reasoning_effort = reasoningEffort;
  if (fastMode != null) body.fast_mode = fastMode;
  if (requestKey) body.request_key = requestKey;
  return await postSSEStream(`/api/v1/threads/${threadId}/messages`, body, { onEvent, onRunIdResolved, signal });
}
