/**
 * History replay: rebuild the transcript (main turns, interrupts, compaction
 * notices, task artifacts) from the server-side replay stream, then hand
 * replayed task events to the subagent projection. Extracted from
 * useChatMessages (carve B); consumes the HistoryRuntime lane port.
 *
 * A long thread is replayed a page at a time: the newest page when the thread
 * opens (loadConversationHistory), then older pages prepended as the reader
 * scrolls up (olderPages, which replays each through this loader).
 */

import i18n from '@/i18n';
import { isOnline } from '@/lib/network';
import type { AssistantMessage } from '@/types/chat';
import { replayThreadHistory } from '../../utils/api';
import { setStoredThreadId } from '../../hooks/utils/threadStorage';
import { updateMessage } from '../../hooks/utils/messageHelpers';
import type { HtmlWidgetData } from '../../hooks/utils/types';
import {
  buildModelFallbackSegment, appendNotificationSegmentOnce, mapToolCallIdToAgentId,
} from '../../hooks/utils/messageFinalizers';
import { handleContextWindowEvent } from '../../hooks/utils/contextWindowEvent';
import { handleProvenance } from '../stream/mainEventHandlers';
import {
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryTodoUpdate,
  handleHistoryHtmlWidget,
  handleHistorySteeringDelivered,
  handleHistoryTaskArtifactStatus,
  isSubagentHistoryEvent,
} from './historyHandlers';
import type {
  TokenUsage, SSEEvent, HistoryInterruptInfo, SubagentHistoryData, PairState,
} from '../types';
import { createApprovalEvidence, recordApprovalEvidence } from '../interrupts/claims';
import { projectHistoryInterrupt } from '../interrupts/fromHistoryEvent';
import type { HistoryRuntime } from '../runtime';
import type { HistoryPageInfo, LoadedHistoryPage } from './olderPages';
import { applyHistoryToolResult, markSteeredResumeCards, settleResumedInterrupts } from './resumeSettlement';

export interface ReplayHistoryDeps {
  /** Fold a model-fallback replay event into the fallback banner state. */
  applyFallbackSuggestion: (event: Record<string, unknown>) => void;
  /** Best-effort fetch of the thread's feedback state (useChatFeedback). */
  loadFeedback: (targetThreadId?: string) => Promise<void>;
  /** Subagent-lane projection, bound to the runtime by the composition root. */
  projectSubagentHistory: (byTaskId: Map<string, SubagentHistoryData>) => void;
  /** Runs in the commit that ends the load, after the replay's last write, with
   *  what the load will return. The caller settles here what only it can judge
   *  (a run that is over), so the transcript never appears unsettled first. */
  beforeReveal?: (ok: boolean) => void;
  /** Page size for the newest page; omitted replays the whole thread. */
  limit?: number;
  /** Replay the page before this turn instead, into a transcript of its own:
   *  an older page (loadOlderHistoryPage), with `signal` to abort its fetch. */
  beforeTurn?: number;
  signal?: AbortSignal;
  /** Where the loaded transcript starts, reported in the commit that reveals
   *  it, on success only; null when it holds no turn. */
  onPage?: (page: LoadedHistoryPage | null) => void;
}

export async function loadConversationHistory(
  rt: HistoryRuntime,
  deps: ReplayHistoryDeps,
): Promise<boolean> {
  const setMessagesForHandlers = rt.setMessages as unknown as (
    updater: (prev: Record<string, unknown>[]) => Record<string, unknown>[]
  ) => void;
  if (!rt.workspaceId || !rt.threadId || rt.threadId === '__default__' || rt.historyLoadingRef.current) {
    return false;
  }
  // An older page replays into a transcript of its own, which it adds to
  // rather than rebuilds, and its caller owns what a failure shows.
  const olderPage = deps.beforeTurn !== undefined;

  // The todo card shows only the last state, so it is written once, with the
  // transcript and the caller's settle step. Written per event, it showed the
  // replay's in-progress todos over the loading transcript, then shrank when
  // a run that was over got its leftovers marked stale.
  let lastTodoCard: Record<string, unknown> | null = null;
  const reveal = (ok: boolean) => {
    if (lastTodoCard) rt.updateTodoListCard?.(lastTodoCard);
    deps.beforeReveal?.(ok);
    rt.setIsLoadingHistory(false);
    rt.historyLoadingRef.current = false;
  };

  try {
    rt.historyLoadingRef.current = true;
    rt.historyHasUnresolvedInterruptRef.current = false;
    rt.setIsLoadingHistory(true);
    rt.setHistoryLoadFailed(false);
    rt.setMessageError(null);

    // Reset history-tracking state so a re-replay (e.g. after a failed
    // reconnect → rt.setReloadTrigger increment) starts from a clean slate.
    // Without this, the bubble-creation handlers in historyEventHandlers
    // would re-insert bubbles atop the prior load's bubbles. With the
    // newly deterministic bubble ids (`history-{role}-{pairIndex}`), the
    // duplicate insert would also trip React's same-key warnings.
    // ``isHistory: true`` only marks bubbles produced by this loader, so
    // any in-flight streaming bubble survives the filter. Older pages are
    // history too, which is why a reload asks for every turn they held.
    rt.newMessagesStartIndexRef.current = 0;
    // A re-replay rebuilds every history bubble from persisted events, so the
    // rendered-interrupt set must start empty and be repopulated by this pass;
    // otherwise stale ids would suppress cards this replay legitimately renders.
    // An older page starts from the ids the newer pages rendered.
    if (!olderPage) rt.renderedInterruptIdsRef.current.clear();
    rt.setMessages((prev) => prev.filter((m) => !m.isHistory));

    // Fresh attach (refresh or thread switch): clear the live-stream cursor so
    // the subsequent reconnect replays the run's stream from the start. We
    // hold no live events yet, and the replay id-space must not leak into the
    // live reconnect (see the note in the replay handler below). The
    // mid-stream disconnect-retry path does NOT call this loader, so its
    // resume cursor is preserved.
    rt.lastEventIdRef.current = null;

    const threadIdToUse = rt.threadId;

    // Track pairs being processed - use Map to handle multiple pairs
    const assistantMessagesByPair = new Map<number, string>(); // Map<turn_index, assistantMessageId>
    const pairStateByPair = new Map<number, PairState>(); // Map<turn_index, { contentOrderCounter, reasoningId, toolCallId }>

    // Track the currently active pair for artifacts (which don't have turn_index)
    // This ensures artifacts get the correct chronological order
    let currentActivePairIndex: number | null = null;
    let currentActivePairState: PairState | null | undefined = null;

    // Highest turn_index this replay delivered (every persisted turn emits at
    // least its user_message with turn_index). Assigned — not maxed — into
    // rt.lastRenderedTurnIndexRef after the replay, so a post-fork re-replay can
    // lower the watermark. -1 = replay delivered zero turns.
    let maxReplayedTurnIndex = -1;
    // Lowest, which is where an unpaged replay's transcript starts.
    let minReplayedTurnIndex: number | null = null;
    // The page header, when the replay was paged (null when it named no turn).
    let page: HistoryPageInfo | null | undefined;

    // Run ids of the turns this replay rendered (the server stamps run_id on
    // a user_message only once its run is terminal — a live run's stub
    // carries none). Fed to markRunsRendered so the report-back catch-up
    // never re-attaches a run whose turn is already on screen: the recents
    // list alone can't cover the post-finalize/pre-ack outbox window, and a
    // wake-queued id attaches without ever consulting /status.
    const replayedRunIds: string[] = [];

    // When each replayed turn's run settled, keyed by turn. Applied after the
    // dispatch loop rather than on arrival: the settle time belongs to the
    // turn's LAST assistant bubble, and a steered turn keeps re-pointing its
    // pair at a newer one, so only the finished map knows which bubble is the
    // tail. A live turn's stub carries no stamp — its bubble is settled by the
    // stream instead.
    const runSettledAtByPair = new Map<number, number>();

    // Track pending HITL interrupts from history to resolve status on next user_message
    const pendingHistoryInterrupts: HistoryInterruptInfo[] = [];
    // What each still-running resume turn answered, kept for the whole replay
    // because the evidence can arrive BEFORE the interrupt it answers: the tip
    // interrupt is appended once at the end of a checkpoint replay carrying no
    // turn_index, so a reload taken before the resume commits its boundary
    // replays it last — after the stamp that already answered it. See
    // interrupts/claims.ts for why only a live resume records any.
    const evidence = createApprovalEvidence();

    // Track subagent events by task ID for this history load
    // Map<taskId, { messages: Array, events: Array, description?: string, type?: string }>
    const subagentHistoryByTaskId = new Map<string, SubagentHistoryData>();
    // Track which agentIds had steering_accepted actions (for inline card "Updated" label)
    const steeredAgentIds = new Set<string>();
    // Main-agent tool calls this replay rendered, so a result whose call it did
    // not render can be left to the page that holds the call.
    const seenToolCallIds = new Set<string>();
    const carryEvents: SSEEvent[] = [];
    try {
      const onEvent = (_rawEvent: Record<string, unknown>): void => {
      // Cast to SSEEvent for type-safe field access within this callback
      const event = _rawEvent as SSEEvent;
      const eventType = event.event;
      const contentType = event.content_type;
      const hasRole = event.role !== undefined;
      const hasPairIndex = event.turn_index !== undefined;

      // NOTE: do NOT write `event._eventId` into `rt.lastEventIdRef` here.
      // Replay (`/messages/replay`) numbers events with a cumulative
      // per-thread counter, while the live workflow stream
      // (`workflow:stream:{tid}:{rid}`) resets its ids to 1 per run. After a
      // refresh, carrying the replay cursor into the live reconnect overshoots
      // the run's id space — the backend's XREAD blocks forever and the stream
      // delivers zero events (frozen response). `rt.lastEventIdRef` must only ever
      // track ids received on the LIVE stream (set in the streaming
      // `processEvent` handler); history dedup uses deterministic bubble ids.

      // A paged replay opens with where its page starts. An unpaged one never
      // sends this, which leaves `page` undefined: the whole thread, nothing older.
      if (eventType === 'history_page') {
        page = typeof event.first_turn_index === 'number'
          ? { firstTurnIndex: event.first_turn_index, hasMore: event.has_more === true }
          : null;
        return;
      }

      // compaction_chunk is the side channel for LLM output from the
      // compaction middleware (bracketed by context_window summarize
      // start/complete events). Drop here so it never merges into the
      // assistant message — future UI can subscribe separately to show
      // what was summarized.
      if (eventType === 'compaction_chunk') {
        return;
      }

      // Check if this is a subagent event - filter it out from main chat view
      const isSubagent = isSubagentHistoryEvent(event as Record<string, unknown>);

      // Update current active pair when we see an event with turn_index
      if (hasPairIndex) {
        const pairIndex = event.turn_index!;
        currentActivePairIndex = pairIndex;
        currentActivePairState = pairStateByPair.get(pairIndex);
        if (pairIndex > maxReplayedTurnIndex) maxReplayedTurnIndex = pairIndex;
        if (minReplayedTurnIndex === null || pairIndex < minReplayedTurnIndex) minReplayedTurnIndex = pairIndex;
      }

      // Handle context_window events from history (token_usage, summarize, offload)
      // Subagent context_window events are routed through the isSubagent block below.
      if (eventType === 'context_window' && !isSubagent) {
        handleContextWindowEvent(event, {
          getMsgId: () => currentActivePairIndex !== null
            ? (assistantMessagesByPair.get(currentActivePairIndex) ?? null) : null,
          nextOrder: () => {
            const eventId = event._eventId;
            if (eventId != null) return Number(eventId);
            if (currentActivePairState) {
              currentActivePairState.contentOrderCounter++;
              return currentActivePairState.contentOrderCounter;
            }
            return 0;
          },
          setMessages: rt.setMessages,
          setTokenUsage: rt.setTokenUsage,
          setIsCompacting: null,  // no start events in replayed history
          insertNotification: () => {},  // standalone notifications not needed in replay
          t: rt.t,
          offloadBatch: rt.offloadBatchRef,
        });
        return;
      }

      // Persisted model_fallback replays as its transcript notification
      // divider on the turn's assistant message (no transient pill on reload).
      // model_retry is not persisted, so there's no replay branch for it.
      if (eventType === 'model_fallback' && !isSubagent) {
        // Thread-level switch suggestion — history replays chronologically,
        // so the last turn's fallbacks win (user_message boundaries and
        // replayed error events clear it along the way).
        deps.applyFallbackSuggestion(event);
        const msgId = currentActivePairIndex !== null
          ? (assistantMessagesByPair.get(currentActivePairIndex) ?? null) : null;
        if (msgId) {
          const order = event._eventId != null
            ? Number(event._eventId)
            : (currentActivePairState ? ++currentActivePairState.contentOrderCounter : 0);
          const segment = buildModelFallbackSegment(event, rt.t, order);
          rt.setMessages((prev) => updateMessage(prev, msgId, (msg) => {
            if (msg.role !== 'assistant') return msg;
            return appendNotificationSegmentOnce(msg as AssistantMessage, segment);
          }));
        }
        return;
      }

      // An errored turn replays its persisted error event: the fallback
      // model didn't save the turn, so drop any switch suggestion its
      // model_fallback events set above. (No other replay reconstruction
      // happens for error events today — deliberately no `return`.)
      if (eventType === 'error' && !isSubagent) {
        rt.setFallbackSuggestion(null);
      }

      // Backward compat: handle old token_usage events from history
      if (eventType === 'token_usage') {
        const callInput = event.input_tokens || 0;
        const callOutput = event.output_tokens || 0;
        rt.setTokenUsage((prev: TokenUsage | null) => ({
          totalInput: (prev?.totalInput || 0) + callInput,
          totalOutput: (prev?.totalOutput || 0) + callOutput,
          lastOutput: callOutput,
          total: event.total_tokens || 0,
          threshold: event.threshold || prev?.threshold || 0,
        }));
        return;
      }

      // Handle steering_delivered events from sse_events (main agent only;
      // subagent steering_delivered events are routed through the isSubagent block below)
      if (eventType === 'steering_delivered' && hasPairIndex && !isSubagent) {
        handleHistorySteeringDelivered({
          event: event as Record<string, unknown>,
          pairIndex: event.turn_index!,
          assistantMessagesByPair,
          pairStateByPair,
          refs: { newMessagesStartIndexRef: rt.newMessagesStartIndexRef },
          setMessages: setMessagesForHandlers,
        });
        return;
      }

      // Handle provenance events from history replay. Re-attach the
      // accessed-data record to the turn's assistant message via the replay
      // `turn_index` envelope. Placed BEFORE the isSubagent block so
      // subagent-emitted records (agent="task:...") still re-attach to the
      // main turn's message on reload, mirroring the live dispatch.
      if (eventType === 'provenance' && hasPairIndex) {
        const pairIndex = event.turn_index!;
        currentActivePairIndex = pairIndex;
        currentActivePairState = pairStateByPair.get(pairIndex);

        const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
        if (!currentAssistantMessageId) {
          console.warn('[History] Received provenance for unknown turn_index:', pairIndex);
          return;
        }

        handleProvenance({
          assistantMessageId: currentAssistantMessageId,
          event: event as unknown as import('@/types/sse').ProvenanceEvent,
          setMessages: setMessagesForHandlers,
        });
        return;
      }

      // Handle subagent events - store them separately, don't process in main chat
      if (isSubagent) {
        // With task:{task_id} format, the agent field IS the task key
        const taskId = event.agent;

        if (taskId) {
          // Initialize subagent history storage if needed
          if (!subagentHistoryByTaskId.has(taskId)) {
            subagentHistoryByTaskId.set(taskId, {
              messages: [],
              events: [],
            });
          }

          const subagentHistory = subagentHistoryByTaskId.get(taskId)!;
          // Store the event for later processing
          subagentHistory.events.push(event);
        } else {
          console.warn('[History] Subagent event without agent field:', {
            eventType,
            agent: event.agent,
          });
        }

        // Don't process subagent events in main chat view
        return;
      }

      // Handle user_message events from history
      // Note: event.content may be empty for HITL resume pairs (an approval carries no text)
      if (eventType === 'user_message' && hasPairIndex) {
        if (typeof event.run_id === 'string' && event.run_id) {
          replayedRunIds.push(event.run_id);
        }
        if (typeof event.run_completed_at === 'string') {
          const settledAt = Date.parse(event.run_completed_at);
          if (Number.isFinite(settledAt)) {
            runSettledAtByPair.set(event.turn_index!, settledAt);
          }
        }
        // New-turn boundary: the switch suggestion only reflects the most
        // recent turn, so any earlier turn's fallback suggestion is stale.
        rt.setFallbackSuggestion(null);
        // Collect LLM models from query metadata (may differ across turns)
        if (event.metadata?.llm_model) {
          const llmModel = event.metadata.llm_model as string;
          rt.setThreadModels(prev => prev.includes(llmModel) ? prev : [...prev, llmModel]);
        }
        // The resolvers below settle the cards already on screen; this keeps
        // the same evidence for the interrupts still ahead of us.
        recordApprovalEvidence(evidence, event);

        // The resume settles the cards it answered; when they sit on the page
        // before this one, that page settles them from the carry instead.
        if (event.metadata?.hitl_interrupt_ids || event.metadata?.hitl_answers) carryEvents.push(event);
        settleResumedInterrupts(rt, pendingHistoryInterrupts, event);

        const pairIndex = event.turn_index!;
        const refs = {
          recentlySentTracker: rt.recentlySentTrackerRef.current,
          currentMessageRef: rt.currentMessageRef,
          newMessagesStartIndexRef: rt.newMessagesStartIndexRef,
        };

        handleHistoryUserMessage({
          event: event as Record<string, unknown>,
          pairIndex,
          assistantMessagesByPair,
          pairStateByPair,
          refs,
          setMessages: setMessagesForHandlers,
        });
        return;
      }

      // Handle message_chunk events (assistant messages)
      if (eventType === 'message_chunk' && hasRole && event.role === 'assistant' && hasPairIndex) {
        const pairIndex = event.turn_index!;
        const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
        const pairState = pairStateByPair.get(pairIndex);

        if (!currentAssistantMessageId || !pairState) {
          console.warn('[History] Received message_chunk for unknown turn_index:', pairIndex);
          return;
        }

        // Process reasoning_signal
        if (contentType === 'reasoning_signal') {
          const signalContent = (event.content as string) || '';
          handleHistoryReasoningSignal({
            assistantMessageId: currentAssistantMessageId,
            signalContent,
            pairIndex,
            pairState,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number | undefined,
            elapsedMs: typeof event.elapsed_ms === 'number' ? event.elapsed_ms : undefined,
          });
          return;
        }

        // Handle reasoning content
        if (contentType === 'reasoning' && event.content) {
          handleHistoryReasoningContent({
            assistantMessageId: currentAssistantMessageId,
            content: event.content as string,
            pairState,
            setMessages: setMessagesForHandlers,
          });
          return;
        }

        // Handle text content
        if (contentType === 'text' && event.content) {
          handleHistoryTextContent({
            assistantMessageId: currentAssistantMessageId,
            content: event.content as string,
            finishReason: event.finish_reason,
            pairState,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number | undefined,
            phase: event.phase,
          });
          return;
        }

        // A content-less close, a stopped turn's synthetic one included, goes
        // through the text handler, which owns what a close stamps.
        if (event.finish_reason) {
          handleHistoryTextContent({
            assistantMessageId: currentAssistantMessageId,
            content: '',
            finishReason: event.finish_reason,
            pairState,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number | undefined,
          });
          return;
        }
      }

      // Filter out tool_call_chunks events
      if (eventType === 'tool_call_chunks') {
        return;
      }

      // Handle artifact events (e.g., todo_update)
      // In history replay, artifacts DO have turn_index, so we can use it directly
      //
      // NOTE: `chart_annotation` artifacts are intentionally NOT replayed here
      // (only the live stream applies them to the annotation store). On reload,
      // every live chart (MarketView's, and the chat's chart tab through
      // `MarketChartSurface`) repopulates the store from Postgres with
      // `useChartAnnotationSync`, and the inline card renders from the
      // replayed `tool_call_result.artifact`.
      if (eventType === 'artifact') {
        const artifactType = event.artifact_type;
        if (artifactType === 'todo_update') {
          const payload = event.payload || {};

          // Floating todo card: last event wins, written at the reveal
          lastTodoCard = {
            todos: Array.isArray(payload.todos) ? payload.todos : [],
            total: payload.total || 0,
            completed: payload.completed || 0,
            in_progress: payload.in_progress || 0,
            pending: payload.pending || 0,
          };

          // Artifacts in history replay have turn_index - use it!
          if (hasPairIndex) {
            const pairIndex = event.turn_index!;
            // Update active pair tracking
            currentActivePairIndex = pairIndex;
            currentActivePairState = pairStateByPair.get(pairIndex);

            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);

            if (!currentAssistantMessageId || !pairState) {
              console.warn('[History] Received artifact for unknown turn_index:', pairIndex);
              return;
            }

            handleHistoryTodoUpdate({
              assistantMessageId: currentAssistantMessageId,
              artifactType: artifactType as string,
              artifactId: event.artifact_id as string,
              payload,
              pairState: pairState,
              setMessages: setMessagesForHandlers,
              eventId: event._eventId as number | undefined,
            });
          } else {
            // Fallback: artifacts without turn_index (shouldn't happen in history, but handle gracefully)
            console.warn('[History] Artifact without turn_index, using active pair fallback');
            let targetAssistantMessageId = null;
            let targetPairState = null;

            if (currentActivePairIndex !== null && currentActivePairState) {
              targetAssistantMessageId = assistantMessagesByPair.get(currentActivePairIndex);
              targetPairState = currentActivePairState;
            } else if (assistantMessagesByPair.size > 0) {
              const pairIndices = Array.from(assistantMessagesByPair.keys()).sort((a, b) => b - a);
              const lastPairIndex = pairIndices[0];
              targetAssistantMessageId = assistantMessagesByPair.get(lastPairIndex);
              targetPairState = pairStateByPair.get(lastPairIndex);
            }

            if (targetAssistantMessageId && targetPairState) {
              handleHistoryTodoUpdate({
                assistantMessageId: targetAssistantMessageId,
                artifactType: artifactType as string,
                artifactId: event.artifact_id as string,
                payload,
                pairState: targetPairState,
                setMessages: setMessagesForHandlers,
                eventId: event._eventId as number | undefined,
              });
            }
          }
        }
        if (artifactType === 'html_widget') {
          const payload = (event.payload || {}) as unknown as HtmlWidgetData;

          if (hasPairIndex) {
            const pairIndex = event.turn_index!;
            currentActivePairIndex = pairIndex;
            currentActivePairState = pairStateByPair.get(pairIndex);

            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);

            if (currentAssistantMessageId && pairState) {
              handleHistoryHtmlWidget({
                assistantMessageId: currentAssistantMessageId,
                artifactType: artifactType as string,
                artifactId: event.artifact_id as string,
                payload: payload as HtmlWidgetData | null,
                pairState,
                setMessages: setMessagesForHandlers,
                eventId: event._eventId as number | undefined,
              });
            }
          } else {
            let targetAssistantMessageId = null;
            let targetPairState = null;

            if (currentActivePairIndex !== null && currentActivePairState) {
              targetAssistantMessageId = assistantMessagesByPair.get(currentActivePairIndex);
              targetPairState = currentActivePairState;
            } else if (assistantMessagesByPair.size > 0) {
              const pairIndices = Array.from(assistantMessagesByPair.keys()).sort((a, b) => b - a);
              const lastPairIndex = pairIndices[0];
              targetAssistantMessageId = assistantMessagesByPair.get(lastPairIndex);
              targetPairState = pairStateByPair.get(lastPairIndex);
            }

            if (targetAssistantMessageId && targetPairState) {
              handleHistoryHtmlWidget({
                assistantMessageId: targetAssistantMessageId,
                artifactType: artifactType as string,
                artifactId: event.artifact_id as string,
                payload: payload as HtmlWidgetData | null,
                pairState: targetPairState,
                setMessages: setMessagesForHandlers,
                eventId: event._eventId as number | undefined,
              });
            }
          }
        }
        if (artifactType === 'task') {
          const payload = event.payload || {};
          const task_id = payload.task_id as string | undefined;
          const rawAction = payload.action as string | undefined;
          const description = payload.description as string | undefined;
          const prompt = payload.prompt as string | undefined;
          const type = payload.type as string | undefined;
          // Backend stamps the task's real status on every replayed task artifact
          // (payload.status). The top-level `status` is a hardcoded "completed"
          // and MUST be ignored.
          const stampedStatus = payload.status as string | undefined;
          // Ledger failure reason, stamped on any task that settled with one.
          const stampedError = payload.error as string | undefined;
          // Its machine spelling, which is what tells a stop the user can act
          // on (a credit stop) from one that is only worth reporting.
          const stampedErrorType = payload.error_type as string | undefined;
          const stampedRunStartedMs =
            typeof payload.projected_run_started_ms === 'number'
              ? payload.projected_run_started_ms
              : undefined;
          const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'steering_accepted') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })();
          if (task_id) {
            const agentId = `task:${task_id}`;
            if (!subagentHistoryByTaskId.has(agentId)) {
              subagentHistoryByTaskId.set(agentId, {
                messages: [],
                events: [],
                description: description || '',
                prompt: prompt || description || '',
                type: type || 'general-purpose',
                status: stampedStatus,
                error: stampedError,
                errorType: stampedErrorType,
                projectedRunStartedMs: stampedRunStartedMs,
              });
            } else {
              const existing = subagentHistoryByTaskId.get(agentId)!;
              if (description && !existing.description) existing.description = description;
              if (prompt && !existing.prompt) existing.prompt = prompt || description || '';
              if (type && !existing.type) existing.type = type;
              // The failure travels with the status it arrived with, and is
              // replaced by it rather than merged under it. Every replayed
              // task artifact carries a status; a resumed task's carries no
              // failure at all, so preserving the previous run's reason is
              // exactly how a retracted credit stop comes back on the next
              // reload — the live session clears both writers when the task
              // resumes, and replay would put one of them straight back.
              if (stampedStatus) {
                existing.status = stampedStatus;
                existing.error = stampedError;
                existing.errorType = stampedErrorType;
              }
              // Monotonic max: artifacts stamp claims-through-their-turn, and
              // page/artifact processing order must not regress the watermark.
              if (stampedRunStartedMs != null) {
                existing.projectedRunStartedMs =
                  existing.projectedRunStartedMs != null
                    ? Math.max(existing.projectedRunStartedMs, stampedRunStartedMs)
                    : stampedRunStartedMs;
              }
            }
            // Patch the inline card(s) for THIS artifact's tool_call_id so a
            // reborn "running" card reflects the stamped terminal status.
            handleHistoryTaskArtifactStatus({
              toolCallId: event.tool_call_id as string | undefined,
              taskId: task_id,
              status: stampedStatus,
              setMessages: setMessagesForHandlers,
            });
            // Track steering_accepted actions for inline card "Updated" label
            if (action === 'update') {
              steeredAgentIds.add(agentId);
            }
            rt.historyPendingTaskToolCallIdsRef.current = mapToolCallIdToAgentId(
              event.tool_call_id as string | undefined,
              agentId,
              action,
              rt.historyPendingTaskToolCallIdsRef.current,
              rt.subagentHistory.toolCalls,
            );
          }
        }
        return;
      }

      // Handle tool_calls events
      if (eventType === 'tool_calls' && hasPairIndex) {
        const pairIndex = event.turn_index!;
        // Update active pair tracking
        currentActivePairIndex = pairIndex;
        currentActivePairState = pairStateByPair.get(pairIndex);

        const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
        const pairState = pairStateByPair.get(pairIndex);

        if (!currentAssistantMessageId || !pairState) {
          console.warn('[History] Received tool_calls for unknown turn_index:', pairIndex);
          return;
        }

        // Queue task tool call IDs for matching against artifact 'spawned' events
        // Skip follow-up/resume calls (task_id present) — they target existing subagents
        if (event.tool_calls) {
          const taskToolCalls = event.tool_calls.filter(
            (tc) => (tc.name === 'task' || tc.name === 'Task') && tc.id && !tc.args?.task_id
          );
          const toolCallIds = taskToolCalls.map((tc) => tc.id).filter(Boolean) as string[];
          if (toolCallIds.length > 0) {
            rt.historyPendingTaskToolCallIdsRef.current = [
              ...rt.historyPendingTaskToolCallIdsRef.current,
              ...toolCallIds,
            ];
          }
        }

        for (const tc of event.tool_calls || []) {
          if (tc.id) seenToolCallIds.add(tc.id);
        }
        handleHistoryToolCalls({
          assistantMessageId: currentAssistantMessageId,
          toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
          pairState,
          setMessages: setMessagesForHandlers,
          eventId: event._eventId as number | undefined,
        });
        return;
      }

      // Handle tool_call_result events
      if (eventType === 'tool_call_result' && hasPairIndex) {
        const pairIndex = event.turn_index!;
        // Update active pair tracking
        currentActivePairIndex = pairIndex;
        currentActivePairState = pairStateByPair.get(pairIndex);

        const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
        const pairState = pairStateByPair.get(pairIndex);

        if (!currentAssistantMessageId || !pairState) {
          console.warn('[History] Received tool_call_result for unknown turn_index:', pairIndex);
          return;
        }

        // Build toolCallId → agentId mapping from Task tool artifact (preferred over order-based)
        const artifact = event.artifact as Record<string, unknown> | undefined;
        if (artifact?.task_id && event.tool_call_id) {
          const agentId = `task:${artifact.task_id}`;
          rt.subagentHistory.toolCalls.set(event.tool_call_id, agentId);

          // Ensure subagentHistoryByTaskId has description from artifact.
          // Resume calls are filtered out of the tool_calls handler, so this
          // is the only place to pick up the description for resumed tasks.
          if (artifact.description) {
            const existing = subagentHistoryByTaskId.get(agentId);
            if (existing) {
              if (!existing.description) existing.description = artifact.description as string;
              if (!existing.prompt) existing.prompt = (artifact.prompt || artifact.description || '') as string;
            } else {
              subagentHistoryByTaskId.set(agentId, {
                messages: [],
                events: [],
                description: artifact.description as string,
                prompt: (artifact.prompt || artifact.description || '') as string,
                type: (artifact.type || 'general-purpose') as string,
              });
            }
          }
        }

        // A gated call is answered in the turn after it; when that turn opens
        // this page, the call is on the page before, which applies the result.
        if (event.tool_call_id && !seenToolCallIds.has(event.tool_call_id)) carryEvents.push(event);

        applyHistoryToolResult({
          event,
          assistantMessageId: currentAssistantMessageId,
          pairState,
          pending: pendingHistoryInterrupts,
          setMessages: rt.setMessages,
        });
        return;
      }

      // Handle interrupt events during history replay
      if (eventType === 'interrupt') {
        projectHistoryInterrupt(rt, event, {
          currentActivePairIndex, assistantMessagesByPair, pairStateByPair, pendingHistoryInterrupts, evidence,
        });
        return;
      }

      // Handle replay_done event (final event)
      if (eventType === 'replay_done') {
        if (event.thread_id && event.thread_id !== rt.threadId && event.thread_id !== '__default__') {
          rt.setThreadId(event.thread_id);
          setStoredThreadId(rt.workspaceId, event.thread_id);
        }
      } else if (eventType === 'credit_usage') {
        // credit_usage indicates the end of one conversation pair — no-op boundary marker
      } else if (!eventType) {
        // Fallback: Handle events without event type
        if (event.thread_id && !hasRole && !contentType) {
          if (event.thread_id !== rt.threadId && event.thread_id !== '__default__') {
            rt.setThreadId(event.thread_id);
            setStoredThreadId(rt.workspaceId, event.thread_id);
          }
        }
      }
    };
      const result = await replayThreadHistory(threadIdToUse, onEvent, {
        limit: deps.limit, beforeTurn: deps.beforeTurn, signal: deps.signal,
      });
      // An older page that did not arrive whole is no page.
      if (olderPage && result?.aborted) return false;
      if (olderPage && result?.disconnected) throw new Error('History page interrupted');

      // If there's still a pending interrupt after replay (no subsequent user_message
      // resolved it), store it in a ref. loadAndMaybeReconnect will decide whether to
      // make it interactive (workflow paused) or reconnect to get resolution events
      // (workflow active = interrupt was answered but resolution is in Redis buffer).
      if (pendingHistoryInterrupts.length > 0) {
        rt.historyHasUnresolvedInterruptRef.current = true;
        rt.unresolvedHistoryInterruptRef.current = pendingHistoryInterrupts.map((p) => ({ ...p }));
        pendingHistoryInterrupts.length = 0;
      }

      // Build per-task transcripts + seed persistent refs from replayed
      // subagent events (no floating cards during history load).
      deps.projectSubagentHistory(subagentHistoryByTaskId);
    } catch (replayError: unknown) {
      // Handle 404 gracefully - it's expected for brand new threads that haven't been fully initialized yet
      if ((replayError as Error).message && (replayError as Error).message.includes('404')) {
        // Thread not found (404) is expected for brand-new threads — skip silently.
        // Don't set error message for 404 - it's expected for new threads
      } else {
        throw replayError; // Re-throw other errors
      }
    }

    // Replay settled (a 404 counts: brand-new thread, zero turns) — record
    // the watermark the reactivation staleness check compares against.
    rt.lastRenderedTurnIndexRef.current = maxReplayedTurnIndex;
    rt.replayedRunIdsRef.current = replayedRunIds;

    // Post-process: stamp each settled turn's end on the bubble that closes it.
    // Paired with the initiating user bubble's timestamp (the query's
    // created_at) this is the turn's duration. A bubble still streaming is the
    // live turn the replay ran alongside — the stream owns its stamp.
    if (runSettledAtByPair.size > 0) {
      const settledAtByMessageId = new Map<string, number>();
      for (const [pairIndex, settledAt] of runSettledAtByPair) {
        const tailId = assistantMessagesByPair.get(pairIndex);
        if (tailId) settledAtByMessageId.set(tailId, settledAt);
      }
      if (settledAtByMessageId.size > 0) {
        rt.setMessages(prev => prev.map(msg => {
          if (msg.role !== 'assistant' || msg.isStreaming) return msg;
          const settledAt = settledAtByMessageId.get(msg.id as string);
          return settledAt === undefined ? msg : { ...msg, completedAt: settledAt };
        }));
      }
    }

    // Post-process: update inline cards for steering_accepted actions to show "Updated"
    if (steeredAgentIds.size > 0) {
      rt.setMessages(prev => markSteeredResumeCards(prev, steeredAgentIds));
    }

    // The page header, or for an unpaged replay, the whole thread from its
    // first replayed turn.
    const start = page !== undefined ? page
      : minReplayedTurnIndex !== null ? { firstTurnIndex: minReplayedTurnIndex, hasMore: false }
      : null;
    deps.onPage?.(start && {
      ...start,
      lastTurnIndex: maxReplayedTurnIndex,
      carry: { events: carryEvents, subagents: subagentHistoryByTaskId, steeredAgentIds },
    });
    reveal(true);

    // Fetch feedback state for the thread (best-effort)
    if (rt.threadId) {
      await deps.loadFeedback(rt.threadId);
    }
    return true;
  } catch (error: unknown) {
    if (olderPage) throw error;
    console.error('[History] Error loading conversation history:', error);
    // Only show error if it's not a 404 (404 is expected for new threads).
    // 404 still counts as a successful "no prior history" load — caller can
    // safely mark the idempotency key.
    const errMsg = (error as Error).message || '';
    const isNotFound = errMsg.includes('404');
    if (errMsg && !isNotFound) {
      // A dropped link surfaces here as the browser's own string — "Failed to
      // fetch", "Load failed" — which we then render in red as if the request
      // were rejected. It was not: the turn is usually still running and the
      // reconnect resumes it from the cursor once the link returns. Say what
      // actually happened instead of quoting the transport at the user.
      rt.setMessageError(
        isOnline() ? errMsg : i18n.t('chat.offlineHistoryLoad'),
      );
    }
    // Set with the flag below, so the render that ends the load says whether
    // the transcript it left is the thread or what a failure stripped.
    rt.setHistoryLoadFailed(!isNotFound);
    reveal(isNotFound);
    return isNotFound;
  }
}
