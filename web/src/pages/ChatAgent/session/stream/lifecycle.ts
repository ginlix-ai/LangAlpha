/**
 * Recovery/ownership lifecycle (the last session carve): stream-ownership
 * contract, post-refresh reconnect, disconnect retry, and shared stream-end
 * cleanup. Consumes the RecoveryRuntime lane port; composition-level pieces
 * (the bound live-router factory, mux controller, report-back watch) arrive
 * via RecoveryDeps so this module never sees the full runtime.
 */

import {
  reconnectToWorkflowStream, getWorkflowStatus, getReportBackStatus,
  type ReportBackStatusResponse,
} from '../../utils/api';
import { decodeReportBackSignal } from '../../utils/reportBackSignal';
import { ZERO_USAGE } from '../../utils/tokenUsage';
import { REPORT_BACK_IDLE_MAX_REARMS } from '../../hooks/useReportBackWatch';
import { createAssistantMessage, appendMessage, updateMessage } from '../../hooks/utils/messageHelpers';
import { finalizeTodoListProcessesInMessages } from '../../hooks/utils/messageFinalizers';
import type { AssistantMessage } from '@/types/chat';
import type { SSEEvent, HistoryInterruptInfo, StreamProcessorRefs } from '../types';
import type { RecoveryRuntime } from '../runtime';

export interface ReconnectOptions {
  activeTasks?: string[];
  runId?: string | null;
  resetCursor?: boolean;
  idleAbortMs?: number;
  snapshotAtMs?: number;
  resetSubagentProjection?: boolean;
}

export interface RecoveryDeps {
  /** Live-router factory with runtime, router deps, and task routing pre-bound
   * by the composition root. */
  createProcessor: (
    assistantMessageId: string,
    refs: StreamProcessorRefs,
    wasInterruptedRef: { current: boolean },
  ) => (event: SSEEvent) => void;
  buildStreamRefs: (o?: {
    isNewConversation?: boolean;
    isReconnect?: boolean;
    withUnresolvedInterrupt?: boolean;
  }) => StreamProcessorRefs;
  clearSubagentCards: (() => void) | null;
  isSettledTask: (taskId: string) => boolean;
  attachSubagentMux: (tid: string, processEvent: (event: SSEEvent) => void, snapshotAtMs?: number) => void;
  muxOpenTaskIds: () => Set<string>;
  markTranscriptPersisted: () => void;
  clearModelStatus: () => void;
  finalizePendingTodos: (() => void) | null;
  reportBackWatch: {
    onStreamEnd: () => void;
    arm: (
      flashThreadId: string | null | undefined,
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
  { activeTasks = [], runId, resetCursor = false, idleAbortMs, snapshotAtMs, resetSubagentProjection = true }: ReconnectOptions = {},
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
  if (resetCursor) rt.lastEventIdRef.current = null;

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
    const stripsByMsgId = new Map<string, HistoryInterruptInfo[]>();
    for (const info of stripList) {
      const arr = stripsByMsgId.get(info.assistantMessageId) || [];
      arr.push(info);
      stripsByMsgId.set(info.assistantMessageId, arr);
    }
    rt.setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const strips = stripsByMsgId.get(m.id);
        if (!strips) return m;
        const msg = m as AssistantMessage;
        const stripQuestionIds = new Set(
          strips.filter((s) => s.type === 'ask_user_question' && s.questionId).map((s) => s.questionId!),
        );
        const stripProposalIds = new Set(strips.filter((s) => s.proposalId).map((s) => s.proposalId!));
        const stripPlanApprovalIds = new Set(
          strips.filter((s) => s.type === 'plan_approval' && s.planApprovalId).map((s) => s.planApprovalId!),
        );
        const newSegments = (msg.contentSegments || []).filter((seg) => {
          if (seg.type === 'user_question') return !stripQuestionIds.has(seg.questionId);
          if (
            seg.type === 'create_workspace' ||
            seg.type === 'start_question' ||
            seg.type === 'ptc_agent' ||
            seg.type === 'delete_workspace' ||
            seg.type === 'stop_workspace' ||
            seg.type === 'delete_thread'
          ) {
            return !stripProposalIds.has(seg.proposalId);
          }
          if (seg.type === 'plan_approval') return !stripPlanApprovalIds.has(seg.planApprovalId);
          return true;
        });
        const next: AssistantMessage = { ...msg, contentSegments: newSegments };
        if (stripQuestionIds.size > 0 && msg.userQuestions) {
          const map = { ...msg.userQuestions };
          for (const qid of stripQuestionIds) delete map[qid];
          next.userQuestions = map;
        }
        if (stripProposalIds.size > 0) {
          for (const key of ['workspaceProposals', 'questionProposals', 'ptcAgentProposals', 'secretaryActionProposals'] as const) {
            const bucket = msg[key];
            if (!bucket) continue;
            const map = { ...bucket };
            for (const pid of stripProposalIds) delete (map as Record<string, unknown>)[pid];
            (next as unknown as Record<string, unknown>)[key] = map;
          }
        }
        if (stripPlanApprovalIds.size > 0 && msg.planApprovals) {
          const map = { ...msg.planApprovals };
          for (const pid of stripPlanApprovalIds) delete map[pid];
          next.planApprovals = map;
        }
        return next;
      }),
    );
    // Redirect refs so the tool_call_result history-resolver targets the new bubble.
    for (const info of stripList) info.assistantMessageId = assistantMessageId;
  }

  {
    const assistantMessage = createAssistantMessage(assistantMessageId);
    // Replace trailing empty history assistant message (created by history replay for the
    // in-progress pair) to avoid a duplicate bubble. If the last message is a non-empty
    // history assistant or something else, just append normally.
    rt.setMessages((prev) => {
      if (prev.length > 0) {
        const lastMsg = prev[prev.length - 1];
        if (
          lastMsg.role === 'assistant' &&
          (lastMsg as AssistantMessage).isHistory &&
          (!(lastMsg as AssistantMessage).contentSegments || (lastMsg as AssistantMessage).contentSegments.length === 0) &&
          !lastMsg.content
        ) {
          return [...prev.slice(0, -1), assistantMessage];
        }
      }
      return appendMessage(prev,assistantMessage);
    });
    rt.currentMessageRef.current = assistantMessageId;
  }

  // Prepare refs for event handlers — use persistent subagent state
  const refs = deps.buildStreamRefs({ isReconnect: true, withUnresolvedInterrupt: true });

  const wasInterruptedRef = { current: false };
  const baseProcessEvent = deps.createProcessor(assistantMessageId, refs, wasInterruptedRef);

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
  // Whether ANY event arrived. A latched run that never streamed must release
  // currentRunIdRef in the finally, or attach() dedups on the stale id and the
  // still-pending summary only surfaces on reload.
  let receivedEvent = false;
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
  const processEvent = (event: SSEEvent) => {
    receivedEvent = true;
    // Progress resets the re-arm budget: a healthy run with an occasional
    // >idleAbortMs gap must never accrue toward the wedged-run cap.
    idleRearms = 0;
    baseProcessEvent(event);
    markReconnected();
    bumpIdle?.();
  };
  // Arm before the first read so an un-started run (no events at all) still
  // trips the watchdog instead of hanging forever.
  bumpIdle?.();

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
    // User stop aborted the reader — stopWorkflow owns teardown; bail.
    // Exception: a foreground handler aborted this stream because the tab
    // resumed (background abort, not a user stop) — re-kick the reconnect.
    // A report-back idle-close (idleClosed) is NOT a bail: fall through so the
    // summary bubble is finalized and the normal completion teardown runs.
    if ((result?.aborted && !idleClosed) || rt.wasStoppedRef.current) {
      if (rt.backgroundReconnectRef.current && !rt.wasStoppedRef.current) {
        rt.backgroundReconnectRef.current = false;
        attemptReconnectAfterDisconnect(rt, deps, rt.currentMessageRef.current || assistantMessageId);
      }
      return;
    }
    if (result?.disconnected) {
      throw new Error('Reconnection stream disconnected');
    }

    // Mark message as complete
    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (msg) => ({
        ...msg,
        isStreaming: false,
      }))
    );
    deps.markTranscriptPersisted();

    // Pre-seed subagent cards from history for tasks whose artifact events were
    // cleared from the Redis buffer after the spawning turn persisted to DB.
    // This mirrors the Scenario B pre-seed at lines 1611-1626.
    // A task the (older) /status snapshot calls active but that is already
    // settled — terminal in history, or seen closing live this session — must
    // not be re-activated: no closure would ever arrive to wedge it back down.
    const liveActiveTasks = activeTasks.filter((t) => !deps.isSettledTask(t));
    if (liveActiveTasks.length > 0 && rt.updateSubagentCard && rt.subagentHistoryRef.current) {
      for (const taskId of liveActiveTasks) {
        const agentId = `task:${taskId}`;
        const historyData = rt.subagentHistoryRef.current[agentId];
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
    // User stop aborted the reader — stopWorkflow owns teardown; bail before
    // surfacing this as a reconnect error.
    if ((err as Error)?.name === 'AbortError' || rt.wasStoppedRef.current) {
      return;
    }
    // 404/410 = workflow no longer available, not a real error
    const status = (err as Error).message?.match(/status:\s*(\d+)/)?.[1];
    if (status === '404' || status === '410') {
      console.log('[Reconnect] Workflow no longer available (', status, '), cleaning up');
    } else {
      console.error('[Reconnect] Error during reconnection:', err);
      rt.setMessageError((err as Error).message || 'Failed to reconnect to stream');
    }
  } finally {
    // Stop the idle watchdog (no-op when it already fired or was never armed).
    if (idleTimer) clearTimeout(idleTimer);
    // Clean up empty reconnect messages (no content segments = nothing was
    // streamed). Skip on a user stop: finalizeStreamingMessage just stamped
    // this bubble `stopped: true` (with the "⏹ Stopped" chip) but left it
    // content-empty, so removing it here would erase the stop marker.
    rt.setMessages((prev) => {
      if (rt.wasStoppedRef.current) return prev;
      const msg = prev.find((m) => m.id === assistantMessageId);
      if (msg && msg.role === 'assistant' && (!(msg as AssistantMessage).contentSegments || (msg as AssistantMessage).contentSegments.length === 0) && !msg.content) {
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
    if (!receivedEvent && stillActive) {
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
      !wasInterruptedRef.current &&
      !rt.wasStoppedRef.current &&
      (!abortController.signal.aborted || idleClosed)
    ) {
      cleanupAfterStreamEnd(rt, deps, assistantMessageId);
    }
    // Clear the spinner only if THIS reconnect still owns it. A newer reconnect
    // (cross-thread nav) took ownership and manages its own spinner — clobbering
    // it would hide the new thread's reconnect. Any other teardown that swapped
    // the stream out from under us (user stop, steering demoted to a new turn)
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
  }
};

/**
 * Attempts to auto-reconnect after a mid-stream network disconnect.
 * Uses exponential backoff (1s, 2s, 4s, 8s, 16s) with up to 5 retries.
 * Falls back to cleanupAfterStreamEnd if workflow completes or retries exhaust.
 */
export const attemptReconnectAfterDisconnect = async (
  rt: RecoveryRuntime,
  deps: RecoveryDeps,
  assistantMessageId: string,
) => {
  const MAX_RETRIES = 5;
  const BASE_DELAY = 1000;

  rt.setIsReconnecting(true);

  // Target the latched thread, not the threadId prop: a first-turn disconnect
  // (background during a brand-new chat's first answer) still has the prop at
  // '__default__' while the real id lives in threadIdRef. Snapshot once so the
  // ~31s retry loop stays pinned to the run we started reconnecting, matching
  // the prior closure-captured semantics.
  const tid = rt.threadIdRef.current;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    if (!tid || tid === '__default__') break;

    if (attempt > 0) {
      await new Promise((r) => setTimeout(r, BASE_DELAY * Math.pow(2, attempt - 1)));
    }

    try {
      const snapshotAtMs = Date.now();
      const status = await getWorkflowStatus(tid);
      if (!status.can_reconnect) {
        console.log('[Reconnect] Workflow no longer reconnectable, cleaning up');
        break;
      }

      console.log('[Reconnect] Attempt', attempt + 1, 'of', MAX_RETRIES);
      // Mid-stream disconnect resumes from the retained cursor (no resetCursor),
      // so it does NOT replay the subagent projection — wiping the cards here
      // would strand a task spawned earlier in this same (unpersisted) turn with
      // nothing to rebuild it. Preserve the live projection; the active_tasks
      // pre-seed below re-asserts anything still running.
      await reconnectToStream(rt, deps, { activeTasks: status.active_tasks || [], snapshotAtMs, resetSubagentProjection: false });

      rt.setIsReconnecting(false);
      return;
    } catch (err: unknown) {
      console.warn('[Reconnect] Attempt', attempt + 1, 'failed:', (err as Error).message);
    }
  }

  rt.setIsReconnecting(false);
  cleanupAfterStreamEnd(rt, deps, assistantMessageId);
  // Reload conversation to show complete response after failed reconnection
  rt.setReloadTrigger((n) => n + 1);
};

/**
 * Shared cleanup logic for all stream-end paths (send, reconnect, HITL resume).
 * Resets loading/streaming state, finalizes subagents, and auto-completes todos.
 */
export const cleanupAfterStreamEnd = (
  rt: RecoveryRuntime,
  deps: RecoveryDeps,
  assistantMessageId: string,
) => {
  rt.setIsLoading(false);
  rt.setWorkspaceStarting(false);
  rt.setIsCompacting(false);
  deps.clearModelStatus();
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
  rt.setMessages((prev) => finalizeTodoListProcessesInMessages(prev, assistantMessageId));

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
