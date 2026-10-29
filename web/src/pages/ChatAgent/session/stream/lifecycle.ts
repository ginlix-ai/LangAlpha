/**
 * Recovery/ownership lifecycle (the last session carve): stream-ownership
 * contract, post-refresh reconnect, disconnect retry, and shared stream-end
 * cleanup. Consumes the RecoveryRuntime lane port; composition-level pieces
 * (the bound live-router factory, mux controller, report-back watch) arrive
 * via RecoveryDeps so this module never sees the full runtime.
 */

import { finalizeAssistantMessage } from './finalizeMessage';
import { holdsContent } from './continuedBubbles';
import type { StreamEventProcessor } from './processStreamEvent';
import { noteLiveRun } from '../latestRun';
import {
  reconnectToWorkflowStream, getWorkflowStatus, getReportBackStatus,
  type ReportBackStatusResponse,
} from '../../utils/api';
import { decodeReportBackSignal } from '../../utils/reportBackSignal';
import { ZERO_USAGE } from '../../utils/tokenUsage';
import { REPORT_BACK_IDLE_MAX_REARMS } from '../../hooks/useReportBackWatch';
import { createAssistantMessage, appendMessage, updateMessage } from '../../hooks/utils/messageHelpers';
import { turnOf } from '../../components/messageList/turnProjection';
import { finalizeTodoListProcessesInMessages } from '../../hooks/utils/messageFinalizers';
import { stripHistoryInterruptCards } from '../interrupts/buckets';
import { checkForNewBuild } from '@/lib/staleBuild';
import { isOnline, waitForOnline } from '@/lib/network';
import type { SSEEvent, StreamProcessorRefs } from '../types';
import type { RecoveryRuntime } from '../runtime';

/** Quiet gap after the last held event before a reconnect stream that never
 *  sends `caught_up` paints what it has. */
export const RECONNECT_BACKLOG_QUIET_MS = 1500;
/** Ceiling measured from the first held event, so a replay that keeps the
 *  quiet window alive still paints instead of being held indefinitely. */
export const RECONNECT_BACKLOG_CAP_MS = 6000;

/** The disconnect retry loop's allowance: attempts, and the first step of the
 *  doubling backoff between them (1s, 2s, 4s, 8s). */
const RETRY_ATTEMPTS = 5;
const RETRY_BASE_DELAY_MS = 1000;

/** The wait before the retry loop's next attempt, `failed` having failed in a
 *  row. A 429's Retry-After takes the backoff step's place, but never past its
 *  longest, so a server asking for minutes cannot park the turn longer than the
 *  loop itself would. */
export const retryDelayMs = (failed: number, retryAfterMs: number | null): number =>
  retryAfterMs !== null
    ? Math.min(retryAfterMs, RETRY_BASE_DELAY_MS * 2 ** (RETRY_ATTEMPTS - 2))
    : RETRY_BASE_DELAY_MS * 2 ** (failed - 1);

/** A 429's Retry-After in ms; null for any other failure, or one naming none. */
const retryAfterOf = (err: unknown): number | null => {
  const { status, retryAfter } = (err ?? {}) as { status?: number; retryAfter?: number | null };
  return status === 429 && retryAfter ? retryAfter * 1000 : null;
};

/** A steer a reload left on screen, still queued or failed: the replay has no
 *  copy of either. One still queued may be the status's, drawn as history. */
const isKeptSteer = (m: { role: string; isHistory?: boolean }): boolean => {
  const steer = m as { steering?: boolean; queueError?: unknown };
  return m.role === 'user' && (!!steer.steering || (!m.isHistory && !!steer.queueError));
};

export interface ReconnectOptions {
  activeTasks?: string[];
  runId?: string | null;
  resetCursor?: boolean;
  idleAbortMs?: number;
  snapshotAtMs?: number;
  resetSubagentProjection?: boolean;
  /** Set by the disconnect retry loop for its own attempts: a drop of this
   *  stream, or a failure to open it, is handed to it instead of ending the
   *  turn. */
  onDrop?: (drop: ReconnectDrop) => void;
  /** Set by the same loop: the bubbles its earlier attempts wrote to, which
   *  this one settles with its own when it ends the turn. */
  owned?: ReadonlySet<string>;
}

/** A retry loop's reconnect that dropped, or failed, before its run ended. */
export interface ReconnectDrop {
  /** Whether it moved the cursor first, which makes it a fresh drop. */
  progressed: boolean;
  /** Whether any event arrived, which means the run rendered something. */
  received: boolean;
  /** A tab resume cut it on purpose, which no link failure did. */
  resumed: boolean;
  /** The bubbles it wrote to (see ownBubbles), which a give-up settles. */
  bubbles: string[];
  /** The server has no run left to stream (404/410), which no retry changes. */
  gone: boolean;
  /** The wait a 429 asked for before the next attempt, in ms. */
  retryAfterMs: number | null;
}

/** What an attach that dropped hands the retry loop it starts. */
export interface RetryOptions {
  /** A report-back attach's idle watchdog, which each reconnect arms in turn. */
  idleAbortMs?: number;
  /** The bubbles the dropped stream wrote to besides the one it left. */
  owned?: Iterable<string>;
  /** Whether the dropped stream rendered anything. */
  rendered?: boolean;
}

export interface RecoveryDeps {
  /** Live-router factory with runtime, router deps, and task routing pre-bound
   * by the composition root. */
  createProcessor: (
    assistantMessageId: string,
    refs: StreamProcessorRefs,
    wasInterruptedRef: { current: boolean },
  ) => StreamEventProcessor;
  buildStreamRefs: (o?: {
    isNewConversation?: boolean;
    isReconnect?: boolean;
    withUnresolvedInterrupt?: boolean;
  }) => StreamProcessorRefs;
  clearSubagentCards: (() => void) | null;
  isSettledTask: (taskId: string) => boolean;
  attachSubagentMux: (tid: string, processEvent: (event: SSEEvent) => void, snapshotAtMs?: number) => void;
  muxOpenTaskIds: () => Set<string>;
  /** `keepQueued` leaves out steers still queued in a run that may go on. */
  markTranscriptPersisted: (opts?: { keepQueued?: boolean }) => void;
  clearModelStatus: () => void;
  finalizePendingTodos: (() => void) | null;
  /** Re-read the machine rows after a turn. */
  refreshComputerAfterTurn: () => void;
  /** Re-read the workspace row for its folder; null with no workspace. */
  refreshWorkspaceFolder: (() => void) | null;
  reportBackWatch: {
    onStreamEnd: () => void;
    arm: (
      dispatcherThreadId: string | null | undefined,
      reportBackRunId: string | null | undefined,
      pokeSource: string | null,
    ) => void;
  };
}

// (isStreamingRef, streamingThreadIdRef) are one invariant: a reconnect stream
// is owned by exactly one thread, or none. Mutate them only through these so
// the flag and the owner can never drift out of sync (the supersede logic and
// the thread-load guard both depend on them agreeing).
export const acquireStreamOwnership = (rt: RecoveryRuntime, tid: string | null) => {
  rt.isStreamingRef.current = true;
  rt.streamingThreadIdRef.current = tid;
};

export const releaseStreamOwnership = (rt: RecoveryRuntime) => {
  rt.isStreamingRef.current = false;
  rt.streamingThreadIdRef.current = null;
  if (rt.pendingMuxResyncRef.current) {
    rt.pendingMuxResyncRef.current = false;
    rt.setReloadTrigger((n) => n + 1);
  }
};

/**
 * Reconnects to an in-progress workflow stream after page refresh.
 * Creates an assistant message placeholder and processes live SSE events.
 */
export const reconnectToStream = async (
  rt: RecoveryRuntime,
  deps: RecoveryDeps,
  { activeTasks = [], runId, resetCursor = false, idleAbortMs, snapshotAtMs, resetSubagentProjection = true, onDrop, owned }: ReconnectOptions = {},
) => {
  // Reconnect targets the LATCHED thread, not the threadId prop. A brand-new
  // chat keeps the prop at '__default__' until the first SSE event updates the
  // route, but Content-Location already latched the real id into threadIdRef.
  // Keying off the prop would bail this whole first-answer window (the most
  // common "ask, background the tab, come back" moment). Snapshot once so the
  // id stays stable across this single reconnect attempt.
  const tid = rt.threadIdRef.current;
  if (!tid || tid === '__default__') return;

  // Callers that (re)attach to the thread's CURRENT active run — thread-load,
  // cross-thread navigation, post-HITL resume, report-back — pass that run's
  // id and rewind the cursor. Without this, currentRunIdRef/lastEventIdRef
  // still point at the PRIOR thread's stream, so we attach to a dead key
  // (zero live events → content only appears on a later refetch). The
  // mid-stream disconnect path omits both and keeps its in-progress cursor.
  if (runId !== undefined) rt.currentRunIdRef.current = runId;
  if (runId) noteLiveRun(rt, runId);
  if (resetCursor) rt.lastEventIdRef.current = null;
  // A held send's `released` frame rides only the stream that sent it, so a
  // send that dropped before it would keep the line up through the answer.
  // The reconnect's own status stands in for it from here.
  rt.setTurnHeld(null);

  console.log('[Reconnect] Starting reconnection for thread:', tid);

  // Clear subagent cards to prevent duplicate content from cache + Redis
  // overlap on a HISTORY-backed reconnect (thread-load, cross-thread nav,
  // post-HITL resume): those replay the whole run, so a stale card would
  // double up. A report-back attach carries NO subagent replay — it streams
  // only the synthetic notification turn — so clearing here is pure
  // collateral: it deletes a still-running sibling's detail card, and if that
  // sibling is quiet inside a long tool call nothing rebuilds it, leaving its
  // detail view stuck at "Initializing" while its inline chip reads "Running".
  // resetSubagentProjection=false (report-back) preserves the live projection.
  if (resetSubagentProjection) {
    if (deps.clearSubagentCards) {
      deps.clearSubagentCards();
    }
    // The observed-terminal map shadows the card projection: a history-backed
    // reset rebuilds status from ledger truth, so the live-closure evidence is
    // no longer needed (and would otherwise mask a legitimately resumed task).
    rt.terminalTaskOutcomesRef.current.clear();
  }

  rt.setIsLoading(true);
  rt.setIsReconnecting(true);
  acquireStreamOwnership(rt, tid);
  // Fresh stream: clear any stale stop flag from a PRIOR turn (e.g. user
  // hard-stopped thread A, then switched to live thread B). Without this the
  // stop-during-reconnect guards below (result.aborted || wasStoppedRef) would
  // bail this legitimate reconnect and leave isLoading stuck. Matches the reset
  // every other stream entry point does (handleSendMessage, resume, steering).
  rt.wasStoppedRef.current = false;
  rt.backgroundReconnectRef.current = false;

  // Create assistant message placeholder for reconnection
  const assistantMessageId = `assistant-reconnect-${Date.now()}`;
  rt.contentOrderCounterRef.current = 0;
  rt.currentReasoningIdRef.current = null;
  rt.currentToolCallIdRef.current = null;

  // Strip interrupt segments populated by history replay before the reconnect stream
  // re-delivers them; otherwise the same question/proposal renders twice (once on the
  // history bubble, once on the reconnect bubble). The reconnect stream is
  // authoritative for live interrupt state. Also redirect any unresolved-interrupt
  // refs to point at the new reconnect bubble so the tool_call_result history-resolver
  // writes resolution status to where the proposal actually lives now.
  const stripList = rt.unresolvedHistoryInterruptRef.current;
  if (stripList.length > 0) {
    // Release the stripped ids from the rendered-interrupt set, synchronously
    // before the stream opens: the reconnect stream re-delivers a still-pending
    // interrupt with the same interrupt_id, and after this strip that
    // re-delivery is the ONLY copy — a stale entry would suppress it and leave
    // the interrupt with no card anywhere, unanswerable until a full reload.
    for (const info of stripList) {
      if (info.interruptId) rt.renderedInterruptIdsRef.current.delete(info.interruptId);
    }
    rt.setMessages((prev) => stripHistoryInterruptCards(prev, stripList));
    // Redirect refs so the tool_call_result history-resolver targets the new bubble.
    for (const info of stripList) info.assistantMessageId = assistantMessageId;
  }

  // The bubble a dropped stream left: the same run continues on the new one,
  // and so does the model call the drop cut off, finish included.
  const continuedId = runId === undefined ? rt.currentMessageRef.current : null;
  // The new bubble names it, so what lands on the turn as a whole, its failure
  // above all, finds the bubble that holds the turn (continuedBubbles.ts).
  const reconnectBubble = (turnIndex: number | undefined) => ({
    ...createAssistantMessage(assistantMessageId, turnIndex),
    ...(continuedId ? { continues: continuedId } : {}),
  });
  {
    // Replace trailing empty history assistant message (created by history replay for the
    // in-progress pair) to avoid a duplicate bubble. If the last message is a non-empty
    // history assistant or something else, just append normally. A bubble carrying its
    // run's end (the replay's settle time, or a finished stream's) closed a settled run,
    // never the one attaching: it is a turn of its own, however empty it ended.
    rt.setMessages((current) => {
      // Nothing will finish its call here, so it stops streaming now, as a
      // steered bubble does when its turn moves on. The turn end is stamped
      // when the run ends.
      const prev = continuedId
        ? updateMessage(current, continuedId, (m) => (m.role === 'assistant' && m.isStreaming ? { ...m, isStreaming: false } : m))
        : current;
      // A steer the reload kept, still queued or with its answer lost, sits
      // below that bubble, as it did below the turn's writes before the
      // reload: the attach takes the bubble over all the same.
      let tail = prev.length - 1;
      while (tail >= 0 && isKeptSteer(prev[tail])) tail--;
      if (tail >= 0) {
        const lastMsg = prev[tail];
        if (
          lastMsg.role === 'assistant' &&
          lastMsg.isHistory &&
          (!lastMsg.contentSegments || lastMsg.contentSegments.length === 0) &&
          !lastMsg.content &&
          lastMsg.completedAt === undefined &&
          lastMsg.completionObservedAt === undefined
        ) {
          return [...prev.slice(0, tail), reconnectBubble(turnOf(prev, lastMsg.id)), ...prev.slice(tail + 1)];
        }
      }
      // A run attached fresh (a report-back) starts unstamped: it is a turn the
      // transcript has not shown, which the count after the last stamp names
      // until the run's metadata frame names it. So a bubble continuing one may
      // have no stamp to copy and takes its turn from the projection.
      const turnIndex = continuedId ? turnOf(prev, continuedId) : undefined;
      return appendMessage(prev, reconnectBubble(turnIndex));
    });
    rt.currentMessageRef.current = assistantMessageId;
  }

  // Prepare refs for event handlers — use persistent subagent state
  const refs = deps.buildStreamRefs({ isReconnect: true, withUnresolvedInterrupt: true });

  const wasInterruptedRef = { current: false };
  const baseProcessEvent = deps.createProcessor(assistantMessageId, refs, wasInterruptedRef);
  // The bubbles this reader wrote to, read when it ends: its own, the one it
  // continues, and the continuation a steer moved it onto, with those the
  // retry loop's earlier attempts wrote to. They are the ones it settles, and
  // no other stream's.
  const ownBubbles = () => {
    const ids = new Set([...(owned ?? []), assistantMessageId, baseProcessEvent.currentBubble()]);
    if (continuedId !== null) ids.add(continuedId);
    return ids;
  };

  // Register the reconnect reader on mainStreamAbortRef so a user stop aborts
  // it. Without this, stopWorkflow's abort is a no-op during a reconnect: the
  // reader keeps consuming SSE + mutating messages and the finally below
  // re-runs cleanup (re-toggling isLoading / re-opening report-back) after the
  // stop already tore everything down.
  const abortController = new AbortController();
  rt.mainStreamAbortRef.current = abortController;
  // This reconnect now owns the spinner set above (setIsReconnecting(true)).
  rt.isReconnectingOwnerRef.current = abortController;

  // Idle watchdog (report-back catch-up only — gated on idleAbortMs). The
  // per-run stream has no terminal sentinel (~8s handshake after the summary,
  // forever for a wedged run), so a reader that never resolves would strand
  // the spinner + isStreamingRef unrecoverably. A quiet window is NOT proof of
  // terminality, though: when it elapses, PROBE /status and finalize only if
  // the queue drained or a newer head run superseded us — a blind finalize
  // would dismiss a slow-but-live run #1 in favor of run #2. Otherwise re-arm,
  // bounded, so a genuinely wedged run still force-releases.
  let idleClosed = false;
  // Whether any content arrived. A latched run that never streamed must release
  // currentRunIdRef in the finally, or attach() dedups on the stale id and the
  // still-pending summary only surfaces on reload.
  let receivedEvent = false;
  // A drop handed back to the retry loop, which owns the turn from there.
  let dropped = false;
  // A drop of an attach outside the loop, which the loop takes over.
  let retryDrop = false;
  // Progress is a cursor that moved: a replay of frames the view already
  // holds is no sign the run or the link is getting anywhere.
  const cursorAtStart = rt.lastEventIdRef.current;
  // An attempt that delivered nothing leaves nothing: the finally drops its
  // empty bubble, as on any other end, and the next attempt continues the
  // bubble this one did. Nor the cursor its run's id frame moved, so the next
  // attempt reads that frame again and its bubble takes the turn it names.
  const handOff = (gone: boolean, retryAfterMs: number | null = null, resumed = false) => {
    dropped = true;
    if (!receivedEvent) rt.lastEventIdRef.current = cursorAtStart;
    if (!receivedEvent && rt.currentMessageRef.current === assistantMessageId) {
      rt.currentMessageRef.current = continuedId;
    }
    onDrop?.({
      progressed: rt.lastEventIdRef.current !== cursorAtStart,
      received: receivedEvent,
      resumed,
      bubbles: [...ownBubbles()],
      gone,
      retryAfterMs,
    });
  };
  let idleTimer: ReturnType<typeof setTimeout> | null = null;
  // The run this reconnect attached to, captured stably so the gate can tell
  // "still our run, just slow" from "superseded" via /status.report_back_run_id.
  const attachedRunId = runId ?? rt.currentRunIdRef.current;
  let idleRearms = 0;
  // Mark a clean idle-close and abort: the finally then finalizes the bubble
  // and runs teardown (release isStreamingRef, poke the watch) instead of bailing.
  const finalizeIdleClose = () => {
    idleClosed = true;
    abortController.abort();
  };
  const bumpIdle = idleAbortMs
    ? () => {
        if (abortController.signal.aborted) return;
        if (idleTimer) clearTimeout(idleTimer);
        idleTimer = setTimeout(() => {
          // Terminality gate: probe the run's real status before finalizing.
          void (async () => {
            let status: ReportBackStatusResponse | null = null;
            try {
              status = await getReportBackStatus(tid);
            } catch {
              status = null; // probe blip → unknown; never finalize on it
            }
            // Re-check ownership after the await: a newer reconnect or a real
            // finalize/stop may have taken over while the probe was in flight.
            if (rt.mainStreamAbortRef.current !== abortController || abortController.signal.aborted) return;

            const signal = status ? decodeReportBackSignal(status.pending_report_back) : 'unknown';
            const rearmOrRelease = () => {
              if (idleRearms++ < REPORT_BACK_IDLE_MAX_REARMS) bumpIdle?.();
              else finalizeIdleClose();
            };
            if (status === null || signal === 'unknown') {
              // The whole /status read is suspect — checked FIRST so a stale
              // report_back_run_id under a failed read can't trip the finalize.
              rearmOrRelease();
            } else if (
              (status.report_back_run_id && status.report_back_run_id !== attachedRunId) ||
              signal === 'idle'
            ) {
              // A newer head run is current, or the queue drained — genuinely done.
              finalizeIdleClose();
            } else {
              // Still our turn (pending/none, same-or-no run id) — slow, not terminal.
              rearmOrRelease();
            }
          })();
        }, idleAbortMs);
      }
    : null;
  // Clear the "Reconnecting…" spinner the moment content flows: a LIVE run's
  // reader stays open for the whole turn, so the spinner would otherwise
  // linger while tokens visibly arrive. isLoading (the stop button) stays on.
  // Ownership-guarded so a superseded stream can't clear a newer reconnect's
  // spinner; nulling the owner makes it idempotent.
  const markReconnected = () => {
    if (rt.isReconnectingOwnerRef.current === abortController) {
      rt.setIsReconnecting(false);
      rt.isReconnectingOwnerRef.current = null;
    }
  };
  // The per-run log replays its backlog in a burst before the live tail.
  // Applied event by event, that burst is presented as if it were live: the
  // bubble mounts empty, the typewriter re-types everything already written,
  // and the scroll follow chases it. So the backlog is held until the server's
  // `caught_up` marker and applied in one synchronous pass, which React
  // batches into a single render: the turn appears as it was, and only what
  // arrives afterwards streams. The timers are the fallback for a stream that
  // never sends the marker; they cost a delayed first paint, never lost events.
  let backlog: SSEEvent[] | null = [];
  let backlogTimer: ReturnType<typeof setTimeout> | null = null;
  let backlogCapAt = 0;
  // One processor for the whole stream: it owns closure state a steering
  // continuation rewrites (the bubble it appends to, pending task ids), so
  // the tail cannot be handed to a fresh one. Handlers read `isReconnect` off
  // the bag on arrival, so flipping it after the synchronous flush is what
  // turns the tail live. Only the stream that still owns the reader may paint
  // what it held: a superseded or stopped one discards its backlog, and the
  // cursor stays put, so the reconnect that replaced it replays the same
  // events from the log.
  const flushBacklog = () => {
    if (!backlog) return;
    const held = backlog;
    backlog = null;
    if (backlogTimer) clearTimeout(backlogTimer);
    backlogTimer = null;
    if (rt.wasStoppedRef.current || rt.mainStreamAbortRef.current !== abortController) return;
    for (const heldEvent of held) baseProcessEvent(heldEvent);
    refs.isReconnect = false;
    // Reaching this point is itself proof the stream is attached and current.
    markReconnected();
  };
  // Restart the quiet window on every held event so a slow replay is not cut
  // in half, but never hold past the cap.
  const armBacklogTimer = () => {
    const now = Date.now();
    if (!backlogCapAt) backlogCapAt = now + RECONNECT_BACKLOG_CAP_MS;
    if (backlogTimer) clearTimeout(backlogTimer);
    backlogTimer = setTimeout(
      flushBacklog,
      Math.max(0, Math.min(RECONNECT_BACKLOG_QUIET_MS, backlogCapAt - now)),
    );
  };
  const processEvent = (event: SSEEvent) => {
    // Progress resets the re-arm budget: a healthy run with an occasional
    // >idleAbortMs gap must never accrue toward the wedged-run cap.
    idleRearms = 0;
    bumpIdle?.();
    // The marker is transport, not content: a stream that sent only it has
    // rendered nothing, and must read as such when it closes.
    if (event.event === 'caught_up') {
      flushBacklog();
      return;
    }
    // Nor is the run's id frame: it is sent before the graph starts, so a
    // stream that went quiet after it rendered nothing either.
    if (event.event !== 'metadata') receivedEvent = true;
    if (backlog) {
      backlog.push(event);
      armBacklogTimer();
      return;
    }
    baseProcessEvent(event);
    markReconnected();
  };
  // Arm before the first read so an un-started run (no events at all) still
  // trips the watchdog instead of hanging forever.
  bumpIdle?.();
  // Bound the hold from the first read, not the first held event: a stream
  // that sends neither marker nor data (the head probe failed with the
  // cursor already at the head) would otherwise keep the spinner up until
  // the model speaks again.
  backlogCapAt = Date.now() + RECONNECT_BACKLOG_CAP_MS;
  backlogTimer = setTimeout(flushBacklog, RECONNECT_BACKLOG_CAP_MS);

  try {
    // Replay buffered events first — this processes artifact{task,spawned} events
    // which create subagent cards with the correct description/type. Per-task streams
    // are opened AFTER so they merge into existing cards instead of creating empty ones.
    const result = await reconnectToWorkflowStream(
      tid,
      rt.currentRunIdRef.current,
      rt.lastEventIdRef.current as number | null,
      processEvent,
      abortController.signal,
    );
    // A stream that ended (or dropped) before the marker still shows what it
    // delivered, and advances the cursor so a retry resumes after it. Not on
    // a user stop: stopWorkflow already finalized the bubble.
    flushBacklog();
    // User stop aborted the reader — stopWorkflow owns teardown; bail.
    // Exception: a foreground handler aborted this stream because the tab
    // resumed (background abort, not a user stop): the suspend likely killed
    // the reader, so it is a drop, and goes to the retry loop as one, which
    // continues the bubble this attempt did.
    // A report-back idle-close (idleClosed) is NOT a bail: fall through so the
    // summary bubble is finalized and the normal completion teardown runs.
    if ((result?.aborted && !idleClosed) || rt.wasStoppedRef.current) {
      if (rt.backgroundReconnectRef.current && !rt.wasStoppedRef.current) {
        rt.backgroundReconnectRef.current = false;
        handOff(false, null, true);
        retryDrop = !onDrop;
      }
      return;
    }
    if (result?.disconnected) {
      // The retry loop takes a drop of its own stream as it took the first:
      // the turn runs on, left for the attempt that continues it.
      if (onDrop) {
        handOff(false);
        return;
      }
      // A load's or a report-back's attach that drops is the same mid-run
      // drop, and goes to the loop once this reader lets go, with the idle
      // watchdog it had. Not one another stream has taken over from.
      if (rt.mainStreamAbortRef.current === abortController) {
        handOff(false);
        retryDrop = true;
        return;
      }
      throw new Error('Reconnection stream disconnected');
    }

    // Mark this reader's bubbles complete. A steer's continuation is the
    // turn's tail; failing that, a reconnect that brings no more text leaves
    // its own bubble empty, and once the cleanup below drops it, the bubble it
    // continues is.
    const end = wasInterruptedRef.current ? 'paused' : 'completed';
    const settled = ownBubbles();
    rt.setMessages((prev) =>
      prev.map((msg) => (settled.has(msg.id) ? finalizeAssistantMessage(msg, end) : msg)),
    );
    deps.markTranscriptPersisted();

    // Pre-seed subagent cards from history for tasks whose artifact events were
    // cleared from the Redis buffer after the spawning turn persisted to DB.
    // This mirrors the Scenario B pre-seed at lines 1611-1626.
    // A task the (older) /status snapshot calls active but that is already
    // settled — terminal in history, or seen closing live this session — must
    // not be re-activated: no closure would ever arrive to wedge it back down.
    const liveActiveTasks = activeTasks.filter((t) => !deps.isSettledTask(t));
    if (liveActiveTasks.length > 0 && rt.updateSubagentCard) {
      const historyEntries = rt.subagentHistory.get().entries;
      for (const taskId of liveActiveTasks) {
        const agentId = `task:${taskId}`;
        const historyData = historyEntries[agentId];
        if (historyData) {
          // Seed the live token-usage ref from history so subsequent live
          // deltas accumulate on top of the historical total instead of
          // starting from zero.
          rt.subagentTokenUsageRef.current[agentId] = historyData.tokenUsage ?? ZERO_USAGE;
          rt.updateSubagentCard(agentId, {
            agentId,
            displayId: `Task-${taskId}`,
            taskId: agentId,
            description: historyData.description || '',
            prompt: historyData.prompt || historyData.description || '',
            type: historyData.type || 'general-purpose',
            tokenUsage: historyData.tokenUsage ?? ZERO_USAGE,
            status: 'active',
            isActive: true,
            isReconnect: true,
          });
        }
      }
    }

    // Attach the thread mux for live subagent frames. The server seeds a
    // channel per open run and replays each immutable per-run stream from
    // 0, so no events are lost.
    if (liveActiveTasks.length > 0) {
      console.log('[Reconnect] Attaching thread mux for active tasks:', liveActiveTasks);
      deps.attachSubagentMux(tid, processEvent, snapshotAtMs);
    }
  } catch (err: unknown) {
    flushBacklog();
    // User stop aborted the reader — stopWorkflow owns teardown; bail before
    // surfacing this as a reconnect error.
    if ((err as Error)?.name === 'AbortError' || rt.wasStoppedRef.current) {
      return;
    }
    // 404/410: the server has no run left to stream here. The message is the
    // server's detail text, so the status comes off the error itself.
    const status = (err as { status?: number }).status ?? Number((err as Error).message?.match(/status:\s*(\d+)/)?.[1]);
    const gone = status === 404 || status === 410;
    // The retry loop counts a failed attempt, or gives up on a gone run, as
    // it does for a drop. Not a refused credential: no retry fixes that, so it
    // ends here with the banner, as it does outside the loop.
    if (onDrop && status !== 401 && status !== 403) {
      handOff(gone, retryAfterOf(err));
      return;
    }
    // The stream failed, which says nothing of the run: a failure the run
    // reported arrived as an `error` event and settled the bubble already. So
    // this reader's bubbles stop without a turn end, as a reconnect given up
    // on does, and the error, if any, is the banner's.
    const settled = ownBubbles();
    rt.setMessages((prev) =>
      prev.map((m) =>
        m.role === 'assistant' && m.isStreaming && settled.has(m.id)
          ? finalizeAssistantMessage(m, 'disconnected')
          : m,
      ),
    );
    if (gone) {
      console.log('[Reconnect] Workflow no longer available (', status, '), cleaning up');
    } else {
      console.error('[Reconnect] Error during reconnection:', err);
      // The error's own text is the transport's or a status line, which says
      // nothing the user can act on. A refused credential is the one failure
      // a sign-in fixes.
      const signedOut = status === 401 || status === 403;
      rt.setMessageError({ message: rt.t(signedOut ? 'chat.reconnectSignedOut' : 'chat.reconnectFailed'), kind: 'notice' });
    }
  } finally {
    // Stop the idle watchdog (no-op when it already fired or was never armed).
    if (idleTimer) clearTimeout(idleTimer);
    if (backlogTimer) clearTimeout(backlogTimer);
    // Clean up empty reconnect messages (no content segments = nothing was
    // streamed). Skip on a user stop: finalizeStreamingMessage just stamped
    // this bubble `stopped: true` (with the "⏹ Stopped" chip) but left it
    // content-empty, so removing it here would erase the stop marker. Nor on
    // a drop that delivered something: the next attempt continues this
    // bubble, stamp and all. Nor when it shows how the turn ended, a failure
    // or a stop from elsewhere, which is then its whole reply: the reconnect
    // heard nothing else of the run.
    rt.setMessages((prev) => {
      if (rt.wasStoppedRef.current || (dropped && receivedEvent)) return prev;
      const msg = prev.find((m) => m.id === assistantMessageId);
      if (msg && msg.role === 'assistant' && !msg.error && !msg.stopped && !holdsContent(msg)) {
        return prev.filter((m) => m.id !== assistantMessageId);
      }
      return prev;
    });

    // Only finalize if this reconnect is still the active stream. Navigation
    // can supersede it (the thread-load effect aborts a report-back stream on
    // the prior thread and starts a fresh stream for the new thread); running
    // cleanup here would then reset isStreamingRef / re-arm watches and clobber
    // the new thread's stream. The owner that superseded us manages that state.
    const stillActive = rt.mainStreamAbortRef.current === abortController;
    // ANY zero-content stream end (idle-close before the first event, a
    // 404/410 the catch discarded, a thrown fetch) releases the run-id latch —
    // otherwise the next reconcile's attach() dedups against the stale id and
    // the still-pending summary only appears on reload. The watch's attach()
    // reads this null as "never rendered" and un-records the run for a bounded
    // retry. Guarded on stillActive so a stream that superseded us (which
    // already latched its own run id) is never clobbered.
    if (!receivedEvent && stillActive && !dropped) {
      rt.currentRunIdRef.current = null;
    }
    // Skip cleanup on a user stop too — stopWorkflow already cleared isLoading /
    // hasActiveSubagents and ran finalize; re-running it here would re-toggle
    // loading and re-open the report-back watch after the stop. Also skip when
    // this reconnect's own stream was aborted (a foreground re-kick on tab
    // resume): the re-kicked reconnect, suspended at its getWorkflowStatus
    // await, still owns mainStreamAbortRef, so cleaning up here would null the
    // spinner mid-resume. EXCEPTION: a report-back idle-close aborts
    // deliberately and MUST run cleanup (release isStreamingRef, clear
    // isLoading, poke the watch) — stranding those is what the watchdog exists
    // to prevent.
    if (
      stillActive &&
      !dropped &&
      !wasInterruptedRef.current &&
      !rt.wasStoppedRef.current &&
      (!abortController.signal.aborted || idleClosed)
    ) {
      cleanupAfterStreamEnd(rt, deps, ownBubbles());
    } else if (stillActive && !dropped && wasInterruptedRef.current) {
      // A stream that ends on a question or an approval card skips the
      // cleanup, and the server sends no wake for an interrupted summary. The
      // attach dropped the tip, so re-read whether another report is owed.
      deps.reportBackWatch.onStreamEnd();
    }
    // Clear the spinner only if THIS reconnect still owns it. A newer reconnect
    // (cross-thread nav) took ownership and manages its own spinner — clobbering
    // it would hide the new thread's reconnect. Any other teardown that swapped
    // the stream out from under us (a user stop)
    // leaves ownership with us, so we still clear and never strand the spinner.
    if (rt.isReconnectingOwnerRef.current === abortController) {
      rt.setIsReconnecting(false);
      rt.isReconnectingOwnerRef.current = null;
    }
    // Null the abort ref only under a LIVE re-check, never the stillActive
    // snapshot above: cleanupAfterStreamEnd → onStreamEnd can SYNCHRONOUSLY
    // chain-attach the next queued report-back run, which registers a fresh
    // AbortController here before its first await — the stale snapshot would
    // null that NEW registration, orphaning the stream (un-stoppable, cleanup
    // skipped, isLoading + isStreamingRef wedged true forever).
    if (rt.mainStreamAbortRef.current === abortController) {
      rt.mainStreamAbortRef.current = null;
    }
    // Last, so the spinner the loop raises is not cleared above. Awaited, so
    // a caller that reads the run-id latch once this resolves (the report-back
    // watch) reads it after the whole chain, the loop's give-up included. The
    // loop takes every bubble this reader wrote to, a steer's continuation
    // and the bubble it left both, to settle when the turn ends.
    if (retryDrop) {
      await attemptReconnectAfterDisconnect(rt, deps, rt.currentMessageRef.current ?? assistantMessageId, {
        idleAbortMs, owned: ownBubbles(), rendered: receivedEvent,
      });
    }
  }
};

/**
 * Attempts to auto-reconnect after a mid-stream network disconnect.
 *
 * Two different failures, handled differently: a network that is present but
 * unhappy gets 5 attempts on exponential backoff (1s, 2s, 4s, 8s — ~15s of
 * delay), and a machine with no link at all is waited on rather than retried
 * at, up to OFFLINE_CEILING_MS. Falls back to cleanupAfterStreamEnd when the
 * workflow is no longer reconnectable or both budgets are spent. A reconnect
 * that drops too, or fails to open, comes back here as the next attempt.
 */
export const attemptReconnectAfterDisconnect = async (
  rt: RecoveryRuntime,
  deps: RecoveryDeps,
  assistantMessageId: string,
  { idleAbortMs, owned: droppedOwned = [], rendered: droppedRendered = true }: RetryOptions = {},
) => {
  // Long, because the cost of waiting is a spinner while the cost of giving up
  // is a truncated answer for a turn that is still running server-side.
  const OFFLINE_CEILING_MS = 5 * 60 * 1000;
  // The offline wait is sliced rather than taken in one go so a stop landing
  // mid-wait is noticed within a slice instead of at link-return. It cannot be
  // aborted instead: the prior stream's finally already nulled
  // mainStreamAbortRef, and parking our own controller there would read as a
  // live stream to the tab-resume handler, which would abort us expecting a
  // stream teardown to re-kick the reconnect.
  const OFFLINE_SLICE_MS = 5 * 1000;

  rt.setIsReconnecting(true);

  // Target the latched thread, not the threadId prop: a first-turn disconnect
  // (background during a brand-new chat's first answer) still has the prop at
  // '__default__' while the real id lives in threadIdRef. Snapshot once so the
  // retry loop stays pinned to the run we started reconnecting, matching the
  // prior closure-captured semantics.
  const tid = rt.threadIdRef.current;
  // The turn this retry belongs to. Every other candidate is reused by the next
  // turn — see the token's declaration in useChatMessages.
  const originSessionEpoch = rt.sessionEpochRef.current;
  // The run this retry resumes, and whether any stream of the chain has shown
  // it: a chain that showed nothing lets the run go when it gives up.
  const latchedRunId = rt.currentRunIdRef.current;
  let rendered = droppedRendered;

  // Every bubble the loop's reconnects wrote to, and the ones the stream that
  // dropped did, for the give-up to settle along with the one it left. A send
  // names the bubble it opened, but a steer the run delivered moved its writes
  // onto a continuation, which is the current bubble.
  const owned = new Set([assistantMessageId, ...droppedOwned]);
  if (rt.currentMessageRef.current) owned.add(rt.currentMessageRef.current);
  // They stop streaming without a turn end: nothing here saw the run end.
  const settleOwned = () => rt.setMessages((prev) =>
    prev.map((m) =>
      m.role === 'assistant' && owned.has(m.id) && m.isStreaming
        ? finalizeAssistantMessage(m, 'disconnected')
        : m,
    ),
  );

  // Three ways this reconnect stops being wanted, checked across every await.
  //
  // A user stop stays reachable for the whole function: the send path returns
  // before its own cleanup on a disconnect, so isLoading stays true and the
  // composer still offers Stop. And reconnectToStream clears wasStoppedRef on
  // entry, so without this we would resume streaming under a bubble already
  // stamped "Stopped".
  //
  // Navigation is the other, and it is invisible to every existing cancel: the
  // thread-load effect supersedes a cross-thread stream by aborting
  // mainStreamAbortRef, which is null for this entire wait (the disconnected
  // send nulled it and skipped the rest of its cleanup). Ungated, a link that
  // returns after the user opened another thread attaches THIS run's id and
  // cursor to THAT thread — reconnectToStream targets the live threadIdRef,
  // not our tid, so the status check and the stream would disagree about which
  // thread they are for.
  //
  // A newer turn on this same thread is the third, and it is why the stop flag
  // alone is not enough: a fresh send clears wasStoppedRef before opening its
  // own stream, so a stop followed by a send inside one slice leaves this retry
  // looking at its own thread with the flag already reset. It would then either
  // run the give-up cleanup against the live new turn, or reconnect and
  // overwrite that turn's mainStreamAbortRef, putting two readers on one run
  // and leaving Stop pointing at neither.
  //
  // Returning rather than breaking is deliberate in all three cases: the
  // give-up tail runs cleanupAfterStreamEnd, which a user stop already did
  // (stopWorkflow finalized the message and tore the session down) and which
  // would clobber whatever session has taken over since.
  const abandoned = () => {
    if (rt.wasStoppedRef.current) {
      console.log('[Reconnect] Turn was stopped; abandoning reconnect');
      rt.setIsReconnecting(false);
      return true;
    }
    if (rt.sessionEpochRef.current !== originSessionEpoch) {
      console.log('[Reconnect] A newer turn took the session; abandoning reconnect');
      // The turn moved on without this one ending, so nothing else settles
      // what its reconnects were writing. A stop already did, in stopWorkflow.
      settleOwned();
      // They stop where the link dropped, and the run went on server-side, so
      // the turn is reloaded whole once the newer turn's stream lets go, or at
      // once if it already has. A view that has left the thread has nothing
      // to reload.
      if (rt.threadIdRef.current === tid) {
        if (rt.isStreamingRef.current) rt.pendingMuxResyncRef.current = true;
        else rt.setReloadTrigger((n) => n + 1);
      }
      // Same spinner rule as below: the turn that superseded us owns its own.
      if (rt.isReconnectingOwnerRef.current === null) rt.setIsReconnecting(false);
      return true;
    }
    if (rt.threadIdRef.current !== tid) {
      console.log('[Reconnect] Thread changed under the reconnect; abandoning', tid);
      // Clear the spinner only if nobody newer owns it — the thread we moved to
      // may already be reconnecting on its own, and hiding its pill would read
      // as a turn that finished.
      if (rt.isReconnectingOwnerRef.current === null) rt.setIsReconnecting(false);
      return true;
    }
    return false;
  };

  // Wall-clock bound on waiting for a link, so a flapping connection cannot
  // keep the loop alive indefinitely.
  let offlineDeadline = Date.now() + OFFLINE_CEILING_MS;
  let attempt = 0;
  // The wait a 429 asked for, which the next attempt keeps to.
  let askedDelay: number | null = null;
  // A tab resume cut the last attempt's reader, so the next opens at once.
  let resumed = false;
  // Whether the run is known to be over. A loop that runs out of attempts or
  // of offline wait does not know it.
  let runOver = false;

  while (attempt < RETRY_ATTEMPTS) {
    if (!tid || tid === '__default__') break;
    if (abandoned()) return;

    // An offline machine cannot be retried into working. Spending the budget
    // against it is why this used to give up ~15s in, long before anyone has
    // noticed their wifi dropped: every attempt fails instantly, so the whole
    // allowance burns in setTimeout. Wait for the link instead, and do not
    // charge the wait against the retries — they exist for a network that is
    // present but unhappy, which is a different problem.
    if (!isOnline()) {
      const remaining = offlineDeadline - Date.now();
      if (remaining <= 0) {
        console.warn('[Reconnect] Still offline after the ceiling; giving up');
        break;
      }
      await waitForOnline(Math.min(remaining, OFFLINE_SLICE_MS));
      continue;
    }

    if (attempt > 0 && !resumed) {
      const delay = retryDelayMs(attempt, askedDelay);
      askedDelay = null;
      await new Promise((r) => setTimeout(r, delay));
      if (abandoned()) return;
      // The link can drop during the backoff. Charging an attempt to a request
      // that is guaranteed to fail instantly is the exact waste this function
      // exists to avoid, so re-gate on connectivity before spending one.
      if (!isOnline()) continue;
    }
    resumed = false;
    attempt += 1;

    try {
      const snapshotAtMs = Date.now();
      const status = await getWorkflowStatus(tid);
      if (!status.can_reconnect) {
        console.log('[Reconnect] Workflow no longer reconnectable, cleaning up');
        runOver = true;
        break;
      }

      // Five minutes offline is long enough for the run we lost to finish and a
      // NEW one to take the thread (another tab, an automation, a queued
      // report-back). The stream is keyed per run, and this path deliberately
      // keeps the disconnected run's id and cursor — so resuming here would
      // target a dead key and, with no idle watchdog armed outside a
      // report-back's retries, hang holding the spinner. Break instead: the
      // give-up tail settles this bubble and reloads, and the load path
      // attaches to status.run_id with a reset cursor, which is the only way
      // to pick up a run we never saw.
      if (
        status.run_id &&
        rt.currentRunIdRef.current &&
        status.run_id !== rt.currentRunIdRef.current
      ) {
        console.log('[Reconnect] Thread moved to run', status.run_id, '; reloading instead of resuming', rt.currentRunIdRef.current);
        runOver = true;
        break;
      }

      console.log('[Reconnect] Attempt', attempt, 'of', RETRY_ATTEMPTS);
      // Last gate before handing the turn back to a live stream: getWorkflowStatus
      // is a round trip, and a stop landing inside it would otherwise be undone
      // by reconnectToStream clearing wasStoppedRef.
      if (abandoned()) return;
      // Mid-stream disconnect resumes from the retained cursor (no resetCursor),
      // so it does NOT replay the subagent projection — wiping the cards here
      // would strand a task spawned earlier in this same (unpersisted) turn with
      // nothing to rebuild it. Preserve the live projection; the active_tasks
      // pre-seed below re-asserts anything still running.
      const dropped: { current: ReconnectDrop | null } = { current: null };
      await reconnectToStream(rt, deps, {
        activeTasks: status.active_tasks || [], snapshotAtMs, resetSubagentProjection: false, idleAbortMs,
        onDrop: (drop) => { dropped.current = drop; }, owned,
      });
      const drop = dropped.current;
      if (drop) {
        for (const id of drop.bubbles) owned.add(id);
        rendered ||= drop.received;
        // The run ended while the link was down: the give-up reloads it whole.
        if (drop.gone) {
          runOver = true;
          break;
        }
        // A turn that took over while this reader ran owns the spinner.
        if (abandoned()) return;
        askedDelay = drop.retryAfterMs;
        rt.setIsReconnecting(true);
        // One that moved the cursor first is a fresh drop with the first
        // one's allowance; one that moved nothing was this attempt failing.
        if (drop.progressed) {
          attempt = 0;
          offlineDeadline = Date.now() + OFFLINE_CEILING_MS;
        }
        // Unless a tab resume cut it: the link may be fine, and the reader
        // was let go on purpose, so that costs no attempt and no wait. A
        // quiet tool call can span any number of app switches.
        if (drop.resumed) {
          if (!drop.progressed) attempt -= 1;
          resumed = true;
          continue;
        }
        // The allowance comes back but not the pace: a link that drops after
        // every event would otherwise reopen the stream back to back.
        if (drop.progressed) await new Promise((r) => setTimeout(r, RETRY_BASE_DELAY_MS));
        continue;
      }

      rt.setIsReconnecting(false);
      return;
    } catch (err: unknown) {
      console.warn('[Reconnect] Attempt', attempt, 'failed:', (err as Error).message);
      askedDelay = retryAfterOf(err);
    }
  }

  // Most ways out of the loop come straight off an await, a status check or
  // a reconnect, where a stop or a newer turn can land unseen.
  if (abandoned()) return;
  rt.setIsReconnecting(false);

  // Settle the bubble we gave up on, and the ones the reconnects that dropped
  // too were writing. Nothing else does: cleanupAfterStreamEnd resets the
  // *session* (isLoading, subagents, todos) but never touches the
  // message, and the other stream-end paths each clear isStreaming themselves
  // on their way out. Left set, the bubble keeps its loading animation forever
  // while the composer has already flipped back to send — visibly alive, inert,
  // and with its hover actions suppressed so it cannot even be retried.
  // Deliberately narrow to this path rather than folded into
  // cleanupAfterStreamEnd, which every other stream-end route also calls.
  settleOwned();

  // A chain that showed nothing of the run lets it go, as a reconnect that
  // brought nothing does, and before the cleanup's report-back poke reads the
  // latch as a run on screen. The watch reads the release as a run it never
  // showed, so a later wake for it can still attach.
  if (!rendered && rt.currentRunIdRef.current === latchedRunId) rt.currentRunIdRef.current = null;

  cleanupAfterStreamEnd(rt, deps, owned);
  // The reload rebuilds the turn from the server, from its checkpoint once the
  // run is over and from the run's stream while it is not, so the bubbles on
  // screen would be a second copy. The replay clears only bubbles marked as
  // its own, which a stream that ends marks too. Not a steer still queued in a
  // run that may go on: no replay has it until the run delivers it or hands it
  // back, which the reload's attach shows.
  deps.markTranscriptPersisted({ keepQueued: !runOver });
  // Reload conversation to show complete response after failed reconnection
  rt.setReloadTrigger((n) => n + 1);
};

/**
 * Shared cleanup logic for all stream-end paths (send, reconnect, HITL resume).
 * Resets loading/streaming state, finalizes subagents, and auto-completes todos
 * on the bubbles the stream wrote to: a reconnect writes to several.
 */
export const cleanupAfterStreamEnd = (
  rt: RecoveryRuntime,
  deps: RecoveryDeps,
  bubbles: string | Iterable<string>,
) => {
  rt.setIsLoading(false);
  rt.setWorkspaceStarting(false);
  rt.setTurnHeld(null);
  rt.setIsCompacting(false);
  deps.clearModelStatus();
  // A settled turn is the cheapest moment to learn the edge has moved on: the
  // user is between actions, so a reload costs them nothing. Throttled, and
  // silent on every ambiguity.
  void checkForNewBuild();
  rt.currentMessageRef.current = null;
  releaseStreamOwnership(rt);

  // Deliberately no status sweep here: card/chip status moves only on
  // positive evidence (ledger stamp at load, per-task chan_close live).
  // Absence of an open mux channel is NOT terminality — during the
  // attach window the mux has no channels at all, and a sweep here would
  // permanently paint still-running tail tasks as completed.
  const hasOpenStreams = deps.muxOpenTaskIds().size > 0;
  rt.setHasActiveSubagents(hasOpenStreams);

  // Finalize pending todos as stale
  if (deps.finalizePendingTodos) deps.finalizePendingTodos();
  const ids = typeof bubbles === 'string' ? [bubbles] : [...bubbles];
  rt.setMessages((prev) => ids.reduce((next, id) => finalizeTodoListProcessesInMessages(next, id), prev));

  // The server measures the machine's disk when a turn ends, onto the
  // computer row; nothing else re-reads that row while the page is open.
  deps.refreshComputerAfterTurn();
  // The router re-reads the workspace row on the run's `metadata` event; a
  // stream attached past that event gets its re-read here.
  if (deps.refreshWorkspaceFolder) deps.refreshWorkspaceFolder();

  // Re-arm the keyed report-back watch and poke a catch-up reconcile (no-op
  // when not awaiting): this turn's stream just ended, and the next ordered
  // report-back may already be queued.
  deps.reportBackWatch.onStreamEnd();

  // Tail mode on a PTC thread: the main turn ended with subagent streams
  // still open. Each subagent that finishes unseen posts a notification
  // turn into THIS thread (task report-back); arm the watch NOW so its
  // dispatch wake attaches live. No poke: pendingness materializes only
  // when a subagent completes — /status is legitimately idle right now,
  // and the backend's `cleared` wake reconciles the chip if every tail
  // task turns out to have been delivered already.
  if (hasOpenStreams) {
    deps.reportBackWatch.arm(rt.threadIdRef.current, null, null);
  }
};
