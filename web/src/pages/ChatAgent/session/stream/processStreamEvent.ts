/**
 * Live SSE event router (carve C): interprets stream events and projects
 * them into UI state. Extracted from useChatMessages; consumes the
 * StreamRuntime lane port plus cross-lane orchestration callbacks.
 */

import { settleFailedBubble, settleStoppedBubble } from './stopBubble';
import { readStreamFailure } from './streamFailure';
import { continuationChain } from './continuedBubbles';
import { readRetryNotice } from './autoRetry';
import { noteLiveRun } from '../latestRun';
import type { StructuredError } from '@/utils/rateLimitError';
import { applyAnnotationArtifact } from '@/pages/MarketView/stores/chartAnnotationStore';
import type { AssistantMessage, ChatMessage } from '@/types/chat';
import { CREDIT_STOP_ERROR_TYPE, isDeliveredNotice, isDeliveredReport, isDeliveredSteer } from '@/types/sse';
import { setStoredThreadId } from '../../hooks/utils/threadStorage';
import { createAssistantMessage, updateMessage } from '../../hooks/utils/messageHelpers';
import { turnOf } from '../../components/messageList/turnProjection';
import { withLiveReportRow } from '../provenance';
import { stampRunTurn } from './runTurn';
import type { HtmlWidgetData } from '../../hooks/utils/types';
import {
  ZERO_USAGE, extractTokenUsageDelta, accumulateTokenUsage,
} from '../../utils/tokenUsage';
import { computeSteeringBoundary, keepSegmentsThrough, shouldSkipSteeringRollback } from './steeringRollback';
import { foldSteeringDelivery, steeringDeliveredEntries, withContinuation } from './steeringDelivery';
import {
  buildModelFallbackSegment, appendNotificationSegmentOnce,
  isWorkspaceChangingToolSuccess, mapToolCallIdToAgentId,
} from '../../hooks/utils/messageFinalizers';
import { handleContextWindowEvent, withServerTotals } from '../../hooks/utils/contextWindowEvent';
import { BUSY_REPLY_KEYS, isBusyCode } from '../../hooks/utils/forkRefusal';
import {
  handleReasoningSignal, handleReasoningContent, handleTextContent,
  handleToolCalls, handleToolCallResult, handleToolCallChunks,
  handleTodoUpdate, handleHtmlWidget, handleProvenance,
} from './mainEventHandlers';
import {
  isSubagentEvent, handleSubagentMessageChunk, handleSubagentToolCallChunks,
  handleSubagentToolCalls, handleSubagentToolCallResult, handleTaskSteeringAccepted,
  handleWorkflowLifecycle, pushTaskNotice, returnTaskInstruction, returnedTaskInstruction,
  type DeliveredInstruction,
} from '../subagents/liveEventHandlers';
import { getOrCreateTaskRefs, type UpdateSubagentCard } from '../streamRefs';
import { handleMarketWatchUpdate, type MarketWatchState } from '../marketWatchEvents';
import { SCHEDULE_WAKEUP, wakeTurnOpener, withLiveWakeRow } from '../wakeupEvents';
import type {
  SSEEvent, StreamProcessorRefs, ModelStatus,
} from '../types';
import { setCardFields, settleProposalFromResult } from '../interrupts/buckets';
import { projectLiveInterrupt } from '../interrupts/fromLiveEvent';
import type { StreamRuntime } from '../runtime';
import { preapprovedReportBack } from '../../utils/preapprovedCards';
import { dispatchResultFields } from '../../utils/dispatchResult';

type RefreshOnResult = 'refreshMarketWatch' | 'refreshWakeups' | 'refreshCommands';

/** The tools whose result is when the chip and the Status panel re-read what
 *  the call changed, rather than at turn end. */
const REFRESH_ON_RESULT = new Map<string, RefreshOnResult>([
  ['watch_market', 'refreshMarketWatch'],
  [SCHEDULE_WAKEUP, 'refreshWakeups'],
  // A status read can stop a command.
  ['ThreadStatus', 'refreshCommands'],
]);

export interface StreamRouterDeps {
  /** Fold a model-fallback event into the fallback banner state. */
  applyFallbackSuggestion: (event: Record<string, unknown>) => void;
  applyModelStatus: (status: ModelStatus) => void;
  clearModelStatus: () => void;
  insertNotification: (text: string, variant?: 'info' | 'success' | 'warning', detail?: string) => void;
  releaseStreamOwnership: () => void;
  attachSubagentMux: (tid: string, processEvent: (event: SSEEvent) => void, snapshotAtMs?: number) => void;
  setMarketWatch: React.Dispatch<React.SetStateAction<MarketWatchState | null>>;
  /** Re-read the thread's watched symbols, after the agent changed them. */
  refreshMarketWatch: () => void;
  /** Re-read the thread's wakeups, after one fired or the agent set or
   * cancelled some. */
  refreshWakeups: () => void;
  /** Re-read the thread's background commands, after one moved to the
   * background, was stopped, or reported its end. */
  refreshCommands: () => void;
  /** Re-read the workspace row, whose folder a run's sandbox acquisition may
   * have moved; null with no workspace. */
  refreshWorkspaceFolder: (() => void) | null;
  /** Watch this thread for a hand-off's report-back; `owed` as the watch's
   *  arm takes it (null arms without touching the tip). */
  armReportBack: (owed?: boolean | null) => void;
  /** Re-read the user's workspaces after an agent changed one. */
  refreshWorkspaces: () => void;
  /** Hears the turn the server admitted the run to, before the run's bubbles
   *  take it, while they still carry the turn the view counted. */
  onAdmittedTurn?: (bubbleId: string, turnIndex: number) => void;
  /** The bubble as the transcript holds it now, writes included. */
  readMessage?: (bubbleId: string) => ChatMessage | undefined;
}

/**
 * The handler for one turn's SSE events, shared by a live send and a
 * reconnect so both fold events into the transcript the same way.
 * `assistantMessageId` is only where the turn starts writing: a delivered
 * steer moves it to a new bubble. `wasInterruptedRef` is set when the turn
 * stops on an interrupt, so the caller finalizes the bubble as paused rather
 * than completed.
 */
/** Routes a stream's events, and names the bubble it writes to now: a steer
 * moves it onto a continuation, which the stream's owner must settle too. */
export type StreamEventProcessor = ((event: SSEEvent) => void) & { currentBubble: () => string };

export const createStreamEventProcessor = (rt: StreamRuntime, deps: StreamRouterDeps, assistantMessageId: string, refs: StreamProcessorRefs, getTaskIdFromEvent: (event: SSEEvent) => string | null, wasInterruptedRef: { current: boolean } | null = null): StreamEventProcessor => {
  const reconnectOrigin = refs.isReconnect === true;
  const setMessagesForHandlers = rt.setMessages as unknown as (
    updater: (prev: Record<string, unknown>[]) => Record<string, unknown>[]
  ) => void;
  const queueMessages = rt.queueMessages as unknown as typeof setMessagesForHandlers;
  // A streamed chunk waits for the next frame. A reconnect's backlog skips the
  // wait: it replays in one task, so the reconnected turn appears in a single
  // render, already typed. Read per call: the bag goes live once the backlog
  // is applied, and a mux frame can flip it for its own dispatch.
  const queueMessagesForHandlers: typeof setMessagesForHandlers = (update) =>
    refs.isReconnect ? setMessagesForHandlers(update) : queueMessages(update);
  const queueCardForHandlers: UpdateSubagentCard = (taskId, patch) =>
    rt.updateSubagentCard?.(taskId, patch, { nextFrame: !refs.isReconnect });
  // Snapshot of the old assistant message's content order at the time the user
  // sent a steering message.  Used to roll back any content that leaked into the
  // old bubble due to stream-mode multiplexing (custom events can arrive after
  // message chunks from the post-injection model call).
  let steeringAtOrder: number | null = null;
  // The boundary is spent by the delivery that takes the steer which set it.
  // The closure copy is typically null: steering_accepted only arrives on the
  // secondary POST stream, and handleSendSteering sets the shared ref there.
  const takeSteeringBoundary = (): number | null => {
    const boundary = steeringAtOrder ?? refs.steeringAtOrderRef?.current ?? null;
    steeringAtOrder = null;
    if (refs.steeringAtOrderRef) refs.steeringAtOrderRef.current = null;
    return boundary;
  };
  // Whether one of this stream's `error` frames showed the turn's failure. A
  // turn shows one failure, so a later internal frame for it shows nothing.
  let failureShown = false;
  // Whether this stream has carried the run's `metadata`, which the server
  // writes only once the run is with the background executor.
  let metadataSeen = false;
  // Whether the turn shows its failure already, which a later internal signal
  // for it (a second frame, the run's closing outcome) must not show again.
  // After a drop the failure sits on a bubble of the turn's chain
  // (continuedBubbles.ts), which is all a reconnect's processor, never having
  // seen the frame, can go by.
  const turnShowsFailure = (): boolean => {
    if (failureShown) return true;
    const read = deps.readMessage;
    if (!read) return false;
    return continuationChain((id) => read(id) as AssistantMessage | undefined, assistantMessageId).some((msg) => msg.error);
  };

  // FIFO queue for matching Task tool call IDs to artifact 'spawned' events.
  // Populated by the tool_calls handler, drained by the artifact/spawned handler.
  // This ensures the tool-call index is populated before tool_call_result.
  const pendingTaskToolCallIds: string[] = [];

  // Those calls still to answer, by call id.
  const pendingRefreshCalls = new Map<string, RefreshOnResult>();

  // Append a notification segment to a task card's latest assistant message
  // (transcript notice inside the subagent detail view). No-ops before the
  // task has an assistant message — status still surfaces via chan_close.
  const appendTaskNotification = (taskId: string, text: string, detail?: string): void => {
    if (!rt.updateSubagentCard) return;
    const taskRefs = getOrCreateTaskRefs(refs, taskId);
    const order = ++taskRefs.contentOrderCounterRef.current;
    const updatedMessages = [...taskRefs.messages] as Record<string, unknown>[];
    const msgIdx = updatedMessages.findLastIndex((m) => m.role === 'assistant');
    if (msgIdx !== -1) {
      const existingMsg = updatedMessages[msgIdx];
      const segs = (existingMsg.contentSegments || []) as Record<string, unknown>[];
      updatedMessages[msgIdx] = { ...existingMsg, contentSegments: [...segs, { type: 'notification', content: text, order, detail }] };
    }
    taskRefs.messages = updatedMessages;
    rt.updateSubagentCard(taskId, { messages: updatedMessages });
  };

  // Append a standalone assistant notice message to a task card (the run
  // error notice).
  //
  // `card` rides the same write rather than taking one of its own: by the time
  // a terminal notice arrives the card is usually already inactive, and a write
  // carrying no messages is dropped there as a stale pure-status update. The
  // messages make this one a content update, which always lands.
  const appendTaskNoticeMessage = (
    taskId: string,
    text: string,
    detail?: string,
    card?: Record<string, unknown>,
  ): void => {
    if (!rt.updateSubagentCard) return;
    const taskRefs = getOrCreateTaskRefs(refs, taskId);
    pushTaskNotice(taskRefs, text, detail);
    rt.updateSubagentCard(taskId, { ...card, messages: taskRefs.messages });
  };

  const dispatch = (event: SSEEvent): void => {
    const eventType = event.event || 'message_chunk';
    // Only token chunks wait for the frame. Anything else first applies the
    // chunks queued ahead of it, in this task, so a state change it makes
    // (loading, an interrupt, an error) never renders before the text.
    if (eventType !== 'message_chunk' && eventType !== 'tool_call_chunks') rt.flushMessages();

    // Check if this is a subagent event — filtered from the main chat view
    // and (critically) from the main reconnect cursor: task-lane frames
    // carry per-task seqs that must never clobber lastEventIdRef.
    const isSubagent = isSubagentEvent(event);

    // Track last event ID for reconnection (main lane only)
    if (event._eventId != null && !isSubagent) {
      rt.lastEventIdRef.current = event._eventId;
    }

    // The ``metadata`` event is the first event of every workflow stream
    // and carries the authoritative run_id for this turn. Latch it so
    // reconnect targets ``workflow:stream:{tid}:{rid}`` precisely.
    if (eventType === 'metadata') {
      metadataSeen = true;
      if (event.run_id) {
        rt.currentRunIdRef.current = event.run_id;
        noteLiveRun(rt, event.run_id);
        if (event.report_source) {
          const { run_id: runId, report_source: source, turn_index: turn } = event;
          const beforeId = rt.currentMessageRef.current;
          rt.setMessages((prev) => withLiveReportRow(prev, runId, source, beforeId, turn));
          // The command it reports on has ended, which its tool card shows.
          if (source.kind === 'command_report') deps.refreshCommands();
        } else {
          // A wake, attached live, draws its opener row from this frame,
          // keyed on the turn as replay keys it, so a later load replaces it
          // rather than drawing a second.
          const row = typeof event.turn_index === 'number' ? wakeTurnOpener(event.turn_index, event, new Date()) : null;
          if (row) {
            const beforeId = rt.currentMessageRef.current;
            rt.setMessages((prev) => withLiveWakeRow(prev, row, beforeId));
            // The wake moved its wakeup on the server, which the chip and the
            // Status panel would otherwise show armed.
            deps.refreshWakeups();
          }
        }
      }
      // The turn admission gave the run, which a view that missed turns added
      // elsewhere cannot count to. An older backend names none, and the count
      // the bubbles were created with stands.
      if (typeof event.turn_index === 'number') {
        const turn = event.turn_index;
        const runBubbleId = assistantMessageId;
        const runId = typeof event.run_id === 'string' ? event.run_id : undefined;
        deps.onAdmittedTurn?.(runBubbleId, turn);
        rt.setMessages((prev) => stampRunTurn(prev, runBubbleId, turn, runId));
      }
      // The server writes this only after the run has its sandbox, so any
      // folder move that acquisition made is already on the row.
      if (deps.refreshWorkspaceFolder) deps.refreshWorkspaceFolder();
      return;
    }

    // compaction_chunk is the side channel for LLM output from the
    // compaction middleware; swallow so it does not leak into the
    // assistant message stream. The context_window summarize
    // start/complete/error events already surface compaction state.
    if (eventType === 'compaction_chunk') {
      return;
    }

    // Update thread_id if provided in the event (ref = synchronous for closures)
    if (event.thread_id && event.thread_id !== '__default__') {
      rt.threadIdRef.current = event.thread_id;
      if (event.thread_id !== rt.threadId) {
        rt.setThreadId(event.thread_id);
        setStoredThreadId(rt.workspaceId, event.thread_id);
      }
    }

    // The run's closing frame counts what the thread has used once the run's
    // own usage committed, calls this stream never reported included, so the
    // count replaces the live sum.
    //
    // A cancel from anywhere but this tab (another tab, a channel, the API)
    // ends the run's stream with this frame alone, so the bubble settles as
    // stopped here, the way a reload shows it. This tab's Stop aborts the read
    // first and settles the bubble itself.
    if (eventType === 'run_end' && !isSubagent) {
      const counted = event.token_totals;
      if (counted) rt.setTokenUsage((prev) => withServerTotals(prev, counted));
      if (event.outcome === 'cancelled') {
        settleStoppedBubble({ assistantMessageId, refs, setMessages: setMessagesForHandlers });
      } else if (event.outcome === 'error' && !refs.retryNotice && !turnShowsFailure()) {
        // A run can fail where no `error` frame can be written for it: the
        // stream's own quota is spent, or its owner died and recovery closed
        // the row. The outcome is all this stream hears, so the turn settles
        // as an internal failure would, banner and all, over any notice.
        deps.clearModelStatus();
        rt.setFallbackSuggestion(null);
        rt.setMessageError({ message: '', kind: 'internal' });
        settleFailedBubble({ assistantMessageId, refs, setMessages: setMessagesForHandlers, internal: true });
      }
      return;
    }

    // Handle workspace_status events (workspace starting/ready).
    // An optional `sandbox_state: "archived"` refinement event follows the
    // generic `starting` on the slow cold-restore path — branch copy on it.
    if (eventType === 'workspace_status') {
      if (event.status === 'starting') {
        const state = event.sandbox_state === 'archived' ? 'archived' : 'starting';
        rt.setWorkspaceStarting(state);
      } else {
        rt.setWorkspaceStarting(false);
      }
      return;
    }

    // turn_held: this send waits on a thread mutation (compact, offload, a
    // model-switch handoff). `released` ends the wait; the stream's end clears
    // it too, so an error frame in between cannot leave the line up.
    if (eventType === 'turn_held') {
      rt.setTurnHeld(event.status === 'held' ? String(event.reason ?? '') : null);
      return;
    }

    // (b) Live market-watch stamp — keep the persistent watch chip current
    // mid-turn. Swallowed here so it never leaks into the message stream.
    if (eventType === 'market_watch_update') {
      handleMarketWatchUpdate({
        event,
        setMarketWatch: deps.setMarketWatch,
      });
      return;
    }

    // Handle steering_accepted events for the MAIN agent (user sent a message while agent streams).
    // Subagent steering_accepted events are handled below in the isSubagent block.
    if (eventType === 'steering_accepted' && !isSubagent) {
      // The steering_accepted event's own `_eventId` is the boundary: every
      // earlier event in the Redis stream has a smaller id, every later one
      // has a larger id. Use it directly so the rollback filter knows what
      // to keep vs. drop. Fall back to the local counter for tests/legacy
      // flows where SSE events carry no `_eventId`.
      const boundary = computeSteeringBoundary(event, refs.contentOrderCounterRef.current);
      steeringAtOrder = boundary;
      if (refs.steeringAtOrderRef) refs.steeringAtOrderRef.current = boundary;
      return;
    }

    // Handle steering_delivered custom events (middleware picked up the steering message).
    // Subagent steering_delivered events are handled in the isSubagent block below.
    if (eventType === 'steering_delivered' && !isSubagent) {
      const oldAssistantId = assistantMessageId;
      const entries = steeringDeliveredEntries(event);
      // A delivery of reports or price alerts alone leaves the user's own
      // steer, and the boundary its acceptance set, for the delivery that
      // takes it. Read here, not in the rollback updater, which React may run
      // after the take has nulled both.
      const effectiveSteeringAtOrder = entries.some(isDeliveredSteer) ? takeSteeringBoundary() : null;
      // A delivered alert or wakeup moved its wakeup on the server, which the
      // chip and the Status panel show.
      if (entries.some(isDeliveredNotice)) deps.refreshWakeups();
      // A command's report taken into the turn: the command has ended.
      if (entries.some((entry) => isDeliveredReport(entry) && entry.report_source.kind === 'command_report')) {
        deps.refreshCommands();
      }

      // 1. Roll back old assistant message to the snapshot taken at steering_accepted
      //    time, removing any content that leaked due to stream-mode multiplexing.
      //    Then finalize it (isStreaming: false).
      rt.setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== oldAssistantId) return msg;
          if (msg.role !== 'assistant') return msg;
          const aMsg = msg as AssistantMessage;

          // If no snapshot — or snapshot is non-positive / NaN (steering
          // arrived before any ordered content was emitted, or `_eventId`
          // was a non-numeric fallback) — skip the destructive filter and
          // just finalize. Real segment orders are always positive; any
          // other boundary would drop every segment, wiping the visible turn.
          if (shouldSkipSteeringRollback(effectiveSteeringAtOrder)) {
            const tp: typeof aMsg.toolCallProcesses = {};
            for (const [id, val] of Object.entries(aMsg.toolCallProcesses || {})) {
              tp[id] = val.isInProgress ? { ...val, isInProgress: false, isComplete: true } : val;
            }
            const rp: typeof aMsg.reasoningProcesses = {};
            for (const [id, val] of Object.entries(aMsg.reasoningProcesses || {})) {
              rp[id] = val.isReasoning ? { ...val, isReasoning: false, reasoningComplete: true } : val;
            }
            return { ...aMsg, isStreaming: false, toolCallProcesses: tp, reasoningProcesses: rp };
          }

          // Keep only segments at or before the steering point. Guard
          // already proved boundary is a positive finite number.
          const keptSegments = keepSegmentsThrough(aMsg.contentSegments || [], effectiveSteeringAtOrder as number);

          // Rebuild plain-text content from kept text segments
          const keptContent = keptSegments
            .filter((s): s is import('@/types/chat').TextSegment => s.type === 'text')
            .map((s) => s.content || '')
            .join('');

          // Collect IDs of kept processes so we can prune orphans
          const keptReasoningIds = new Set(
            keptSegments.filter((s): s is import('@/types/chat').ReasoningSegment => s.type === 'reasoning').map((s) => s.reasoningId)
          );
          const keptToolCallIds = new Set(
            keptSegments.filter((s): s is import('@/types/chat').ToolCallSegment => s.type === 'tool_call').map((s) => s.toolCallId)
          );
          const keptTodoListIds = new Set(
            keptSegments.filter((s): s is import('@/types/chat').TodoListSegment => s.type === 'todo_list').map((s) => s.todoListId)
          );
          const keptSubagentIds = new Set(
            keptSegments.filter((s): s is import('@/types/chat').SubagentTaskSegment => s.type === 'subagent_task').map((s) => s.subagentId)
          );

          const filterObj = <V>(obj: Record<string, V> | undefined, keepSet: Set<string>): Record<string, V> => {
            if (!obj) return {} as Record<string, V>;
            const out: Record<string, V> = {};
            for (const [id, val] of Object.entries(obj)) {
              if (keepSet.has(id)) out[id] = val;
            }
            return out;
          };

          // Finalize retained processes: mark in-progress as complete
          const keptToolCalls = filterObj(aMsg.toolCallProcesses, keptToolCallIds);
          for (const [id, val] of Object.entries(keptToolCalls)) {
            if (val.isInProgress) keptToolCalls[id] = { ...val, isInProgress: false, isComplete: true };
          }
          const keptReasoning = filterObj(aMsg.reasoningProcesses, keptReasoningIds);
          for (const [id, val] of Object.entries(keptReasoning)) {
            if (val.isReasoning) keptReasoning[id] = { ...val, isReasoning: false, reasoningComplete: true };
          }

          return {
            ...aMsg,
            contentSegments: keptSegments,
            content: keptContent,
            reasoningProcesses: keptReasoning,
            toolCallProcesses: keptToolCalls,
            todoListProcesses: filterObj(aMsg.todoListProcesses, keptTodoListIds),
            subagentTasks: filterObj(aMsg.subagentTasks, keptSubagentIds),
            isStreaming: false,
          };
        })
      );

      // The bubble being steered: every bubble this creates joins its turn,
      // read from the projection because a report-back's bubble has no stamp.
      const steeredId = assistantMessageId;
      // 2. Mark delivered the steers it names and draw the rest, reports and
      //    price alerts ahead of the user's words.
      let clearedErrors: unknown[] = [];
      let settledKeys: string[] = [];
      rt.setMessages((prev) => {
        const fold = foldSteeringDelivery(prev, entries, turnOf(prev, steeredId), (kind) =>
          `steering-${kind}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`);
        clearedErrors = fold.clearedErrors;
        settledKeys = fold.settledKeys;
        return fold.messages;
      });
      if (clearedErrors.length > 0) rt.setMessageError((prev) => (clearedErrors.includes(prev) ? null : prev));
      for (const key of settledKeys) rt.requestKeyRef.current.settle(key);

      // 3. Create new assistant message placeholder (steering continuation — not a new backend turn),
      //    above any steer still waiting. Random suffix: the turn's first bubble uses the same
      //    Date.now() scheme, and two ids minted in the same millisecond would collide.
      const newAssistantId = `assistant-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
      rt.setMessages((prev) => {
        const turn = turnOf(prev, steeredId);
        return withContinuation(prev, { ...createAssistantMessage(newAssistantId, turn), isSteering: true }, turn);
      });

      // 4. Switch closure & refs to new assistant message
      assistantMessageId = newAssistantId;
      rt.currentMessageRef.current = newAssistantId;

      // 5. Reset content counters
      refs.contentOrderCounterRef.current = 0;
      refs.currentReasoningIdRef.current = null;
      refs.currentToolCallIdRef.current = null;
      return;
    }

    // A main-thread steering_returned hands a chat app's missed steers back
    // to the gateway that resubmits them; a steer this view sent stays on the
    // server and its next turn reads it. Task-lane steering_returned is
    // handled in the isSubagent block (it must not mutate main-chat state).
    if (eventType === 'steering_returned' && !isSubagent) return;

    // Handle unified context_window events (token_usage, summarize, offload)
    if (eventType === 'context_window') {
      if (isSubagent) {
        // For subagent context_window events, embed notification as a content
        // segment inside the current assistant message (same as main chat) so
        // it appears at the correct chronological position.
        const taskId = getTaskIdFromEvent(event);
        if (taskId && event.action === 'token_usage') {
          // Sum per-call delta into the per-task running total, then push
          // the new total onto SubagentData so the AgentInfo projection
          // (and the inline subagent card) re-renders with it.
          const prev = rt.subagentTokenUsageRef.current[taskId] ?? ZERO_USAGE;
          const next = accumulateTokenUsage(prev, extractTokenUsageDelta(event));
          rt.subagentTokenUsageRef.current[taskId] = next;
          if (rt.updateSubagentCard) {
            rt.updateSubagentCard(taskId, { tokenUsage: next });
          }
          return;
        }
        // token_usage is handled and returned above; this branch is now
        // for summarize / offload / future actions only.
        if (taskId) {
          const action = event.action;
          let text;
          let detail: string | undefined;
          if (action === 'summarize' && event.signal === 'complete') {
            text = rt.t('chat.compactedNotification', { from: event.original_message_count });
            detail = (event.summary_text as string | undefined) || undefined;
          } else if (action === 'offload' && event.signal === 'complete') {
            const args = event.offloaded_args || 0;
            const reads = event.offloaded_reads || 0;
            if (args > 0 && reads > 0) text = rt.t('chat.offloadedNotification', { args, reads });
            else if (reads > 0) text = rt.t('chat.offloadedReadsNotification', { count: reads });
            else if (args > 0) text = rt.t('chat.offloadedArgsNotification', { count: args });
          }
          if (text) {
            appendTaskNotification(taskId, text, detail);
          }
        }
        return;
      }
      handleContextWindowEvent(event, {
        getMsgId: () => rt.currentMessageRef.current,
        nextOrder: () => {
          const eventId = event._eventId;
          return eventId != null ? Number(eventId) : ++refs.contentOrderCounterRef.current;
        },
        setMessages: rt.setMessages,
        setTokenUsage: rt.setTokenUsage,
        live: true,
        setIsCompacting: rt.setIsCompacting,
        setCompactionTokens: rt.setCompactionTokens,
        insertNotification: deps.insertNotification,
        t: rt.t,
        offloadBatch: rt.offloadBatchRef,
      });
      return;
    }

    // Surface model retry/fallback resilience to the user. Both carry their
    // own `task:` prefix guard: v1 ignores subagent-attributed events
    // (agent="task:...") entirely — the pill and transcript notification are
    // main-agent only.
    if (eventType === 'model_retry') {
      if (!isSubagent) {
        deps.applyModelStatus({
          kind: 'retrying',
          model: (event.model as string) || '',
          attempt: typeof event.attempt === 'number' ? event.attempt : 0,
          maxRetries: typeof event.max_retries === 'number' ? event.max_retries : 0,
        });
      }
      return;
    }

    if (eventType === 'model_fallback') {
      if (!isSubagent) {
        const fromModel = (event.from_model as string) || '';
        const toModel = (event.to_model as string) || '';
        const paidBy = typeof event.to_paid_by === 'string' ? event.to_paid_by : undefined;
        const provider = typeof event.to_provider === 'string' ? event.to_provider : undefined;
        deps.applyModelStatus({ kind: 'fallback', fromModel, toModel, paidBy, provider });
        deps.applyFallbackSuggestion(event);
        // Persistent transcript notification — survives reload via the
        // persisted model_fallback replay in loadConversationHistory.
        const order = event._eventId != null
          ? Number(event._eventId)
          : ++refs.contentOrderCounterRef.current;
        const segment = buildModelFallbackSegment(event, rt.t, order);
        rt.setMessages((prev) => updateMessage(prev, assistantMessageId, (msg) => {
          if (msg.role !== 'assistant') return msg;
          return appendNotificationSegmentOnce(msg as AssistantMessage, segment);
        }));
      }
      return;
    }

    // A `retry` ends the stream of a run that failed before it reached the
    // agent. After `metadata` the run lives on in the background executor, so
    // a `retry` there, which only an older server sends, means only this
    // reader broke, and the transport drop that follows reconnects to the run.
    if (eventType === 'retry') {
      // A run that failed after the server started it is the thread's newest.
      if (!isSubagent && typeof event.run_id === 'string') noteLiveRun(rt, event.run_id);
      if (!isSubagent && !metadataSeen) {
        deps.clearModelStatus();
        refs.retryNotice = readRetryNotice(event);
      }
      return;
    }

    // Handle provenance events BEFORE the isSubagent filter so subagent-emitted
    // accessed-data records still attach to the main turn's assistant message
    // (with their `agent="task:..."` attribution preserved on the record).
    // The event is flat — fields top-level on `event` — matching the live
    // tool_call_result reader.
    if (eventType === 'provenance') {
      handleProvenance({
        assistantMessageId,
        event: event as unknown as import('@/types/sse').ProvenanceEvent,
        setMessages: setMessagesForHandlers,
      });
      return;
    }

    // Handle subagent message events (filter them out from main chat view)
    if (isSubagent) {
      // With task:{task_id} format, the agent field IS the task key
      const taskId = getTaskIdFromEvent(event);

      if (!taskId) {
        return; // Don't process in main chat view
      }

      // Process the event with the correct taskId
      if (rt.updateSubagentCard) {

        // Use a stable message ID per task+run so all events from the same run
        // go into one message. Each resume bumps runIndex, creating a new message
        // so the card shows a unified conversation across resume boundaries.
        const taskRefs = getOrCreateTaskRefs(refs, taskId);
        const subagentAssistantMessageId = `subagent-${taskId}-assistant-${taskRefs.runIndex}`;

        if (eventType === 'message_chunk') {
          const contentType = (event.content_type || 'text') as string;
          handleSubagentMessageChunk({
            taskId,
            assistantMessageId: subagentAssistantMessageId,
            contentType,
            content: event.content as string,
            finishReason: event.finish_reason,
            elapsedMs: typeof event.elapsed_ms === 'number' ? event.elapsed_ms : undefined,
            atMs: typeof event._atMs === 'number' ? event._atMs : undefined,
            refs,
            updateSubagentCard: queueCardForHandlers,
          });
        } else if (eventType === 'tool_call_chunks') {
          handleSubagentToolCallChunks({
            taskId,
            assistantMessageId: subagentAssistantMessageId,
            chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
            refs,
            updateSubagentCard: queueCardForHandlers,
          });
        } else if (eventType === 'tool_calls') {
          handleSubagentToolCalls({
            taskId,
            assistantMessageId: subagentAssistantMessageId,
            toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
            atMs: typeof event._atMs === 'number' ? event._atMs : undefined,
            refs,
            updateSubagentCard: rt.updateSubagentCard,
          });
        } else if (eventType === 'tool_call_result') {
          const toolCallId = event.tool_call_id as string;

          handleSubagentToolCallResult({
            taskId,
            assistantMessageId: subagentAssistantMessageId,
            toolCallId: toolCallId,
            result: {
              content: event.content,
              content_type: event.content_type,
              tool_call_id: toolCallId,
              artifact: event.artifact,
              status: event.status,
            },
            refs,
            updateSubagentCard: rt.updateSubagentCard,
          });
        } else if (eventType === 'artifact') {
          // Task-lane artifacts don't render in the main chat view — but
          // thread-scoped side-channel pings stay honored regardless of
          // lane: a subagent's file writes / preview servers change the
          // same workspace the panels show. Everything else (todo_update,
          // widgets) is task-internal until the detail view renders them.
          const taskArtifactType = event.artifact_type as string;
          if (taskArtifactType === 'file_operation' && rt.onFileArtifact) {
            rt.onFileArtifact(event);
          } else if (taskArtifactType === 'preview_url' && rt.onPreviewUrl) {
            const payload = (event.payload || {}) as Record<string, unknown>;
            rt.onPreviewUrl({
              url: '',  // resolved by ChatView via authenticated endpoint
              port: payload.port as number,
              title: payload.title as string | undefined,
              command: payload.command as string | undefined,
              path: payload.path as string | undefined,
              loading: true,
            });
          }
        } else if (eventType === 'workflow_lifecycle') {
          handleWorkflowLifecycle({
            taskId,
            event: event as unknown as Record<string, unknown>,
            refs,
            updateSubagentCard: rt.updateSubagentCard,
          });
        } else if (eventType === 'steering_delivered') {
          if (event.content) {
            handleTaskSteeringAccepted({
              taskId,
              content: event.content as string,
              entries: event.entries as DeliveredInstruction[] | undefined,
              steered: true,
              refs,
              updateSubagentCard: rt.updateSubagentCard,
            });
          }
        } else if (eventType === 'user_message') {
          // Epoch-opening run boundary (spawn/resume instruction) — first
          // entry of the task stream. Same mechanics as a steering
          // follow-up: finalize the previous run, render the bubble,
          // open a new run. Only the main agent spawns or resumes a task.
          if (event.content) {
            handleTaskSteeringAccepted({
              taskId,
              content: event.content as string,
              entries: [{ content: event.content as string, sender: 'agent' }],
              steered: false,
              refs,
              updateSubagentCard: rt.updateSubagentCard,
            });
          }
        } else if (eventType === 'steering_returned') {
          // Run ended before the task steering was delivered — surface the
          // returned text in the task transcript.
          const returned = returnedTaskInstruction(event);
          if (returned) {
            // The notice goes up whether or not a bubble here was pending: the
            // frame is the server's word, and another tab may have sent it.
            returnTaskInstruction(taskRefs, returned, rt.t('chat.taskSteeringReturnedNotification'));
            rt.updateSubagentCard(taskId, { messages: taskRefs.messages });
          }
        } else if (eventType === 'error' || event.error) {
          // chan_close carries only the terminal status; the failure reason
          // exists only on this frame — surface it in the task transcript.
          // This frame is also the only place the task path carries
          // `error_type`, which is what separates a credit stop (the run was
          // cut short by the turn's budget) from an actual failure.
          const errMsg = (event.error || event.message || '') as string;
          const stopped = event.error_type === CREDIT_STOP_ERROR_TYPE;
          // The reason lands on the card as well, because the card is what
          // the MAIN transcript reads. Without it a background task stopped
          // for money reads there as a bare "Stopped" — indistinguishable
          // from one the user ended — while its only explanation sits in a
          // transcript they have to open the task to see.
          appendTaskNoticeMessage(
            taskId,
            rt.t(stopped ? 'chat.taskStoppedNotification' : 'chat.taskErrorNotification'),
            errMsg || undefined,
            errMsg
              ? { error: errMsg, ...(event.error_type ? { errorType: event.error_type } : {}) }
              : undefined,
          );
        }
      }
      return; // Don't process subagent events in main chat view
    }

    if (eventType === 'message_chunk') {
      // First content of the (possibly retried/fell-back) model call — the
      // transient retry/fallback pill has served its purpose.
      deps.clearModelStatus();
      const contentType = event.content_type || 'text';
      const eventId = event._eventId as number | undefined;

      // Handle reasoning_signal events
      if (contentType === 'reasoning_signal') {
        const signalContent = (event.content || '') as string;
        if (handleReasoningSignal({
          assistantMessageId,
          signalContent,
          refs,
          setMessages: setMessagesForHandlers,
          eventId,
          elapsedMs: typeof event.elapsed_ms === 'number' ? event.elapsed_ms : undefined,
        })) {
          return;
        }
      }

      // Handle reasoning content chunks
      if (contentType === 'reasoning' && event.content) {
        if (handleReasoningContent({
          assistantMessageId,
          content: event.content as string,
          refs,
          setMessages: queueMessagesForHandlers,
        })) {
          return;
        }
      }

      // Handle text content chunks
      if (contentType === 'text') {
        if (handleTextContent({
          assistantMessageId,
          content: event.content as string,
          finishReason: event.finish_reason,
          refs,
          setMessages: queueMessagesForHandlers,
          eventId,
          phase: event.phase,
        })) {
          return;
        }
      }

      // Skip other content types
      return;
    } else if (eventType === 'error' || event.error) {
      // The turn failed — clear any lingering retry/fallback pill, and any
      // switch suggestion (the fallback model didn't save the turn either).
      deps.clearModelStatus();
      rt.setFallbackSuggestion(null);
      // A failure that names its recovery ended before the run reached the
      // agent: there is nothing to reconnect to, and the reply's Retry sends
      // what the recovery says.
      if (event.recovery) refs.retryNotice = readRetryNotice(event);
      // One that names its run, a failure or a refusal, came after the server
      // started that run, which left it the thread's newest.
      if ((event.recovery || event.error_type === 'admission_conflict') && !isSubagent && typeof event.run_id === 'string') {
        noteLiveRun(rt, event.run_id);
      }
      // A send refused because the chat was busy says so in the view's words,
      // as a notice: the server's name the CLI's commands, and the reply's
      // Retry sends the message again.
      const busyCode = event.error_type === 'admission_conflict' && isBusyCode(event.code) ? event.code : null;
      // Only the frame that ends the server's own retries carries `max_retries`.
      const errorMessage = busyCode !== null
        ? rt.t(BUSY_REPLY_KEYS[busyCode])
        : typeof event.max_retries === 'number'
          ? rt.t('chat.autoRetryExhausted')
          : event.error || event.message || 'An error occurred while processing your request.';
      // Backend (stream_errors.error_event_data) enriches the event
      // with ``error_kind``, ``status_code`` and ``hints``. We route the
      // display by kind to avoid showing the same error twice:
      //   - upstream → inline card on the failed assistant turn (part of
      //     the transcript; the user's model choice is what triggered it)
      //   - internal → banner near the chat input (our service failed;
      //     don't pollute the conversation history)
      // Either way what the turn streamed stays its reply: the failure rides
      // apart from the content, so the reply never renders as the error.
      const kind = event.error_kind as 'upstream' | 'internal' | undefined;
      const structured: StructuredError | undefined = busyCode !== null
        ? { message: errorMessage as string, kind: 'notice' }
        : readStreamFailure(event, errorMessage as string);

      // A server older than this view frames its time limit, then the same
      // stop again as an internal error: the card already says it, and a
      // banner on top would report one failure twice.
      if (kind === 'internal' && turnShowsFailure()) return;
      failureShown = true;
      if (kind === 'internal') {
        // Banner, and the bubble stays: an internal error can land after the
        // turn streamed for minutes (a stream quota stop), and edit/regenerate
        // map UI position to turn_index by counting assistant bubbles, so none
        // may leave state. One that produced something shows as a failed turn
        // with it; an empty one (the reconnect cleanup's test: no segments, no
        // content) settles without `error`, which the render-time orphan skip
        // hides, as it does the 429 path's, unless it is a hidden opener's
        // (settleFailedBubble).
        // The headline alone, as a reload shows it: the frame's text is the
        // server's raw exception and stays on the wire for other readers.
        rt.setMessageError({ message: '', kind: 'internal' });
        settleFailedBubble({ assistantMessageId, refs, setMessages: setMessagesForHandlers, internal: true });
      } else {
        // Upstream (or unclassified legacy) — render inline. Clear any
        // stale banner from a prior turn so the error lives in one place.
        rt.setMessageError(null);
        settleFailedBubble({
          assistantMessageId, refs, setMessages: setMessagesForHandlers,
          failure: structured ?? { message: errorMessage as string },
        });
      }
    } else if (eventType === 'tool_call_chunks') {
      handleToolCallChunks({
        assistantMessageId,
        chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
        setMessages: queueMessagesForHandlers,
      });
      return;
    } else if (eventType === 'artifact') {
      const artifactType = event.artifact_type as string;
      if (artifactType === 'todo_update') {
        handleTodoUpdate({
          assistantMessageId,
          artifactType,
          artifactId: event.artifact_id as string,
          payload: event.payload || {},
          refs,
          setMessages: setMessagesForHandlers,
          eventId: event._eventId as number,
        });
      } else if (artifactType === 'html_widget') {
        handleHtmlWidget({
          assistantMessageId,
          artifactType,
          artifactId: event.artifact_id as string,
          payload: (event.payload || {}) as unknown as HtmlWidgetData,
          refs,
          setMessages: setMessagesForHandlers,
          eventId: event._eventId as number,
        });
      } else if (artifactType === 'chart_annotation') {
        applyAnnotationArtifact(artifactType, (event.payload || {}) as Record<string, unknown>);
      } else if (artifactType === 'file_operation' && rt.onFileArtifact) {
        rt.onFileArtifact(event);
      } else if (artifactType === 'preview_url' && rt.onPreviewUrl) {
        const payload = (event.payload || {}) as Record<string, unknown>;
        rt.onPreviewUrl({
          url: '',  // resolved by ChatView via authenticated endpoint
          port: payload.port as number,
          title: payload.title as string | undefined,
          command: payload.command as string | undefined,
          path: payload.path as string | undefined,
          loading: true,
        });
      } else if (artifactType === 'task') {
        const payload = (event.payload || {}) as Record<string, unknown>;
        const { task_id, action: rawAction, description, prompt, type } = payload;
        const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'steering_accepted') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })() as string;
        if (!task_id) return;
        const agentId = `task:${task_id}`;

        // Establish toolCallId → agentId mapping immediately, so clicking
        // the inline card before tool_call_result resolves correctly.
        {
          const updated = mapToolCallIdToAgentId(
            event.tool_call_id as string | undefined,
            agentId,
            action,
            pendingTaskToolCallIds,
            rt.subagentHistory.toolCalls,
          );
          pendingTaskToolCallIds.length = 0;
          pendingTaskToolCallIds.push(...updated);
        }

        // 'workflow' is a RunWorkflow launch: a distinct action (the history
        // projector claims no checkpoint namespace for it) but identical card
        // mechanics, and the payload already carries type 'workflow'. Its
        // progress arrives as workflow_lifecycle events on the same task lane.
        if (action === 'init' || action === 'workflow') {
          // A duplicate/replayed spawn for a task we already saw settle must
          // resurface with its REAL terminal outcome (completed/cancelled/error),
          // never a blanket 'completed' that would launder a failure as success.
          const settledOutcome = rt.terminalTaskOutcomesRef.current.get(task_id as string);
          if (rt.updateSubagentCard) {
            rt.updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${task_id}`,
              taskId: agentId,
              type: (type || 'general-purpose') as string,
              description: (description || '') as string,
              prompt: (prompt || description || '') as string,
              // A spawn artifact means "requested", not "working" — hold
              // 'initializing' until the first task event streams (which
              // promotes via deriveSubagentStatus) so the detail header
              // matches reality instead of claiming Running with no content.
              status: settledOutcome ?? 'initializing',
              isActive: !settledOutcome,
            });
          }
          if (!settledOutcome) {
            const currentThreadId = (event.thread_id || rt.threadIdRef.current) as string;
            deps.attachSubagentMux(currentThreadId, processEvent);
          }
        } else if (action === 'resume') {
          // Resume: reactivate the card and reattach the stream. The
          // transcript boundary (instruction bubble + run bump) arrives on
          // the task stream itself, as its epoch-opening user_message.
          // A genuine resume is the ONE legitimate terminal→active transition:
          // retract the observed terminal outcome so the settled-task guards
          // stop skipping it (it is live again under a new run).
          rt.terminalTaskOutcomesRef.current.delete(task_id as string);
          // The same retraction for WHY it stopped, and it has to reach both
          // writers rather than just the card below: the telemetry resolver
          // takes whichever of the two holds a reason, and refreshSubagentCard
          // copies history's back onto the card on the next open, so clearing
          // one alone is undone by the other. The ledger agrees on the next
          // reload, where the task's latest run is the resumed one and carries
          // no failure at all.
          rt.subagentHistory.patchEntry(agentId, { error: undefined, errorType: undefined });
          if (rt.updateSubagentCard) {
            // Prefer preserving the original spawn description (already on the card).
            // But after reconnect the card may have been wiped + recreated without a
            // description, so fall back to the history entry as a safety net.
            const historyEntry = rt.subagentHistory.get().entries[agentId];
            const historyDesc = historyEntry?.description;
            const historyPrompt = historyEntry?.prompt;
            rt.updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${task_id}`,
              taskId: agentId,
              type: (type || 'general-purpose') as string,
              status: 'active',
              isActive: true,
              // The card half of the retraction above. Card state is keyed by
              // task, and the resume's card shares that key with the one that
              // stopped, so leaving it set would show the stop notice on a task
              // actively working.
              error: undefined,
              errorType: undefined,
              ...(historyDesc ? { description: historyDesc } : {}),
              ...(historyPrompt ? { prompt: historyPrompt } : {}),
            });
          }

          // The resumed run announces itself on the control lane and the
          // mux admits its channel — no per-task abort/reopen dance.
          const currentThreadId = (event.thread_id || rt.threadIdRef.current) as string;
          deps.attachSubagentMux(currentThreadId, processEvent);
        } else if (action === 'update') {
          if (rt.updateSubagentCard) {
            rt.updateSubagentCard(agentId, { steeringMessage: prompt || payload.description });
          }
          // Update inline card to show "Updated" instead of "Resumed"
          rt.setMessages(prev => prev.map(msg => {
            if (msg.role !== 'assistant') return msg;
            const aMsg = msg as AssistantMessage;
            if (!aMsg.subagentTasks) return msg;
            let changed = false;
            const newTasks = { ...aMsg.subagentTasks };
            for (const [tcId, task] of Object.entries(newTasks)) {
              if (task.resumeTargetId === agentId && task.action === 'resume') {
                newTasks[tcId] = { ...task, action: 'update' };
                changed = true;
              }
            }
            return changed ? { ...aMsg, subagentTasks: newTasks } : msg;
          }));
        }
      }
      return;
    } else if (eventType === 'tool_calls') {
      // A tool call means the model call succeeded — drop the retry/fallback pill.
      deps.clearModelStatus();
      handleToolCalls({
        assistantMessageId,
        toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
        finishReason: event.finish_reason,
        refs,
        setMessages: setMessagesForHandlers,
        eventId: event._eventId as number,
      });
      // Queue new Task tool call IDs for matching with upcoming artifact 'spawned' events
      if (event.tool_calls) {
        for (const tc of event.tool_calls) {
          if ((tc.name === 'task' || tc.name === 'Task') && tc.id && !tc.args?.task_id) {
            pendingTaskToolCallIds.push(tc.id);
          }
          const refresh = REFRESH_ON_RESULT.get(tc.name);
          if (refresh && tc.id) pendingRefreshCalls.set(tc.id, refresh);
        }
      }
    } else if (eventType === 'tool_call_result') {
      // A tool result means the turn is producing output — drop the pill.
      deps.clearModelStatus();
      // Settle a card the history replay left pending, from the result of the
      // run that answered it.
      const unresolvedList = refs.unresolvedHistoryInterruptRef?.current;
      if (unresolvedList) {
        settleProposalFromResult(unresolvedList, event.tool_call_id as string | undefined, event, rt.setMessages);
      }

      const toolCallId = event.tool_call_id as string;

      // Build toolCallId → agentId mapping from Task tool artifact
      if (event.artifact?.task_id && toolCallId) {
        const agentId = `task:${event.artifact.task_id}`;
        rt.subagentHistory.toolCalls.set(toolCallId, agentId);
      }
      // A call that moved its command to the background, or started one
      // there, names it on its artifact. Its end can start a turn of its own,
      // which the report-back watch attaches in place, so the watch is armed
      // too; it owes nothing the tray names, since the command's row is there.
      if (event.artifact?.command_id) {
        deps.refreshCommands();
        deps.armReportBack(null);
      }

      handleToolCallResult({
        assistantMessageId,
        toolCallId,
        result: {
          content: event.content,
          content_type: event.content_type,
          tool_call_id: toolCallId,
          artifact: event.artifact,
          status: event.status,
        },
        refs,
        setMessages: setMessagesForHandlers,
      });

      // A card's approve click arms the report-back watch; a hand-off the user
      // approved in advance had no click, so its result arms it. A reconnect's
      // backlog too: it replays a run that is still live, and no report-back
      // can run on this thread while it is, so this one is still owed. Arming
      // is idempotent, and an in-page reconnect after a network drop does not
      // re-read /status, so this may be the only arm.
      if (preapprovedReportBack(event.content)) deps.armReportBack();

      if (isWorkspaceChangingToolSuccess(event.content)) deps.refreshWorkspaces();

      const refresh = toolCallId ? pendingRefreshCalls.get(toolCallId) : undefined;
      if (refresh) {
        pendingRefreshCalls.delete(toolCallId);
        deps[refresh]();
      }

      // Detect navigate_to_workspace action from start_question tool result
      if (rt.onWorkspaceCreated && typeof event.content === 'string') {
        try {
          const parsed = JSON.parse(event.content);
          if (parsed?.success && parsed?.action === 'navigate_to_workspace') {
            rt.onWorkspaceCreated({ workspaceId: parsed.workspace_id, question: parsed.question });
          }
        } catch { /* not JSON, ignore */ }
      }

      // Write the dispatch's outcome onto the approved card: the thread it
      // opens, or that it failed. After HITL resume, the tool_call_result
      // arrives on a NEW assistant message while the proposals live on the OLD
      // one (from the interrupt turn), so setCardFields searches every bubble.
      // Match by tool_call_id for exact correlation (safe under concurrent dispatches).
      if (rt.pendingPTCBackfillRef.current.size > 0 && typeof event.content === 'string') {
        const backfillPid = toolCallId ? rt.pendingPTCBackfillRef.current.get(toolCallId) : undefined;
        if (backfillPid) {
          rt.pendingPTCBackfillRef.current.delete(toolCallId);
          const fields = dispatchResultFields(event);
          rt.setMessages((prev) => setCardFields(prev, 'ptcAgentProposals', backfillPid, fields));
        }
      }
    } else if (eventType === 'interrupt') {
      projectLiveInterrupt(rt, event, assistantMessageId, refs);

      rt.setIsLoading(false);
      deps.releaseStreamOwnership();
      rt.currentMessageRef.current = null;
      if (wasInterruptedRef) wasInterruptedRef.current = true;
    }
  };

  const processEvent = (event: SSEEvent): void =>
    dispatchWithReplayStamp(refs, reconnectOrigin, event, dispatch);

  return Object.assign(processEvent, { currentBubble: () => assistantMessageId });
};

/**
 * Dispatch a mux frame with the stamp its channel asks for. A frame marked
 * `_replay` existed before the channel opened, so a processor born on a
 * reconnect (its bag flipped live once the main backlog flushed) folds it
 * as history. A processor born on a send ignores the mark: there a task's
 * replay from 0 is its whole, fresh transcript.
 */
export const dispatchWithReplayStamp = (
  refs: { isReconnect?: boolean },
  reconnectOrigin: boolean,
  event: SSEEvent,
  dispatch: (event: SSEEvent) => void,
): void => {
  if (event._replay !== true || !reconnectOrigin || refs.isReconnect) {
    dispatch(event);
    return;
  }
  refs.isReconnect = true;
  try {
    dispatch(event);
  } finally {
    refs.isReconnect = false;
  }
};
