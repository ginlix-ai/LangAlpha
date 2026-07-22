/**
 * Live SSE event router (carve C): interprets stream events and projects
 * them into UI state. Extracted from useChatMessages; consumes the
 * StreamRuntime lane port plus cross-lane orchestration callbacks.
 */

import { isUpstreamHint, type StructuredError } from '@/utils/rateLimitError';
import { applyAnnotationArtifact } from '@/pages/MarketView/stores/chartAnnotationStore';
import type { AssistantMessage } from '@/types/chat';
import { setStoredThreadId } from '../../hooks/utils/threadStorage';
import { createAssistantMessage, appendMessage, updateMessage } from '../../hooks/utils/messageHelpers';
import type { HtmlWidgetData } from '../../hooks/utils/types';
import {
  ZERO_USAGE, extractTokenUsageDelta, accumulateTokenUsage,
} from '../../utils/tokenUsage';
import { computeSteeringBoundary, shouldSkipSteeringRollback } from './steeringRollback';
import {
  buildModelFallbackSegment, appendNotificationSegmentOnce,
  isOnboardingRelatedToolSuccess, mapToolCallIdToAgentId,
} from '../../hooks/utils/messageFinalizers';
import { handleContextWindowEvent } from '../../hooks/utils/contextWindowEvent';
import {
  handleReasoningSignal, handleReasoningContent, handleTextContent,
  handleToolCalls, handleToolCallResult, handleToolCallChunks,
  handleTodoUpdate, handleHtmlWidget, handleProvenance,
} from './mainEventHandlers';
import {
  isSubagentEvent, handleSubagentMessageChunk, handleSubagentToolCallChunks,
  handleSubagentToolCalls, handleSubagentToolCallResult, handleTaskSteeringAccepted,
} from '../subagents/liveEventHandlers';
import { getOrCreateTaskRefs } from '../streamRefs';
import { handleMarketWatchUpdate, type MarketWatchState } from '../marketWatchEvents';
import type {
  MessageRecord, SSEEvent, HistoryInterruptInfo, StreamProcessorRefs, ModelOptions, ModelStatus,
} from '../types';
import { PROPOSAL_INTERRUPT_TYPES, PROPOSAL_DATA_KEY_MAP } from '../interrupts/buckets';
import { projectLiveInterrupt } from '../interrupts/fromLiveEvent';
import type { StreamRuntime } from '../runtime';

export interface StreamRouterDeps {
  /** Fold a model-fallback event into the fallback banner state. */
  applyFallbackSuggestion: (event: Record<string, unknown>) => void;
  applyModelStatus: (status: ModelStatus) => void;
  clearModelStatus: () => void;
  /** Steering re-entry for queued sends (demotion path re-dispatch). */
  handleSendSteering: (message: string, planMode?: boolean, additionalContext?: Record<string, unknown>[] | null, attachmentMeta?: Record<string, unknown>[] | null, modelOptions?: ModelOptions) => Promise<void>;
  insertNotification: (text: string, variant?: 'info' | 'success' | 'warning', detail?: string) => void;
  loadConversationHistory: () => Promise<boolean>;
  releaseStreamOwnership: () => void;
  attachSubagentMux: (tid: string, processEvent: (event: SSEEvent) => void, snapshotAtMs?: number) => void;
  setMarketWatch: React.Dispatch<React.SetStateAction<MarketWatchState | null>>;
}

/**
 * Creates a stream event processor that handles SSE events from the backend.
 * Used by both handleSendMessage (live) and reconnectToStream (reconnection).
 *
 * @param {string} assistantMessageId - The assistant message ID to update
 * @param {Object} refs - Refs for event handlers (contentOrderCounterRef, etc.)
 * @param {Function} getTaskIdFromEvent - Helper to route subagent events
 * @returns {Function} Event handler: (event) => void
 */
// TODO: type properly — refs should use a proper interface matching StreamRefs from streamEventHandlers
export const createStreamEventProcessor = (rt: StreamRuntime, deps: StreamRouterDeps, assistantMessageId: string, refs: StreamProcessorRefs, getTaskIdFromEvent: (event: SSEEvent) => string | null, wasInterruptedRef: { current: boolean } | null = null) => {
  const setMessagesForHandlers = rt.setMessages as unknown as (
    updater: (prev: Record<string, unknown>[]) => Record<string, unknown>[]
  ) => void;
  // Snapshot of the old assistant message's content order at the time the user
  // sent a steering message.  Used to roll back any content that leaked into the
  // old bubble due to stream-mode multiplexing (custom events can arrive after
  // message chunks from the post-injection model call).
  let steeringAtOrder: number | null = null;

  // FIFO queue for matching Task tool call IDs to artifact 'spawned' events.
  // Populated by the tool_calls handler, drained by the artifact/spawned handler.
  // This ensures toolCallIdToTaskIdMapRef is populated before tool_call_result.
  const pendingTaskToolCallIds: string[] = [];

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

  const processEvent = (event: SSEEvent): void => {
    const eventType = event.event || 'message_chunk';

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
      if (event.run_id) {
        rt.currentRunIdRef.current = event.run_id;
      }
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

    // (b) Live market-watch stamp — keep the persistent watch chip current
    // mid-turn. Swallowed here so it never leaks into the message stream.
    if (eventType === 'market_watch_update') {
      handleMarketWatchUpdate({ event, setMarketWatch: deps.setMarketWatch });
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

      // 1. Roll back old assistant message to the snapshot taken at steering_accepted
      //    time, removing any content that leaked due to stream-mode multiplexing.
      //    Then finalize it (isStreaming: false).
      rt.setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== oldAssistantId) return msg;
          if (msg.role !== 'assistant') return msg;
          const aMsg = msg as AssistantMessage;

          // Use closure-local snapshot or fall back to the shared ref
          // (steering_accepted only arrives on the secondary POST stream, so
          // the closure-local steeringAtOrder is typically null — the shared
          // ref is set by handleSendSteering on the secondary stream).
          const effectiveSteeringAtOrder = steeringAtOrder ?? refs.steeringAtOrderRef?.current ?? null;

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
          const boundary = effectiveSteeringAtOrder as number;
          const keptSegments = (aMsg.contentSegments || []).filter(
            (s) => s.order <= boundary
          );

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
      steeringAtOrder = null;
      if (refs.steeringAtOrderRef) refs.steeringAtOrderRef.current = null;

      // 2. Mark steering user messages as delivered, OR create them from event
      //    data if none exist (reconnect scenario — in-memory state was lost).
      rt.setMessages((prev) => {
        const hasSteeringMessages = prev.some((msg) => 'steering' in msg && msg.steering);
        if (hasSteeringMessages) {
          // Live path: mark existing steering messages as delivered
          return prev.map((msg) =>
            'steering' in msg && msg.steering ? { ...msg, steering: false, steeringDelivered: true } : msg
          );
        }
        // Reconnect path: create user bubbles from event payload
        const steeringMsgs = (event.messages || []).filter((qMsg) => qMsg.content);
        if (steeringMsgs.length === 0) return prev;
        const newUserMessages: MessageRecord[] = steeringMsgs.map((qMsg) => ({
          id: `steering-user-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
          role: 'user' as const,
          content: qMsg.content as string,
          contentType: 'text' as const,
          timestamp: qMsg.timestamp ? new Date((qMsg.timestamp as number) * 1000) : new Date(),
          isStreaming: false as const,
          steeringDelivered: true,
        }));
        return [...prev, ...newUserMessages];
      });

      // 3. Create new assistant message placeholder (steering continuation — not a new backend turn)
      const newAssistantId = `assistant-${Date.now()}`;
      const newAssistant = { ...createAssistantMessage(newAssistantId), isSteering: true };
      rt.setMessages((prev) => appendMessage(prev,newAssistant));

      // 4. Switch closure & refs to new assistant message
      assistantMessageId = newAssistantId;
      rt.currentMessageRef.current = newAssistantId;

      // 5. Reset content counters
      refs.contentOrderCounterRef.current = 0;
      refs.currentReasoningIdRef.current = null;
      refs.currentToolCallIdRef.current = null;
      return;
    }

    // Handle steering_returned for the MAIN agent — it finished before
    // consuming the steering message. Remove the steering user message from
    // chat and restore text to input box. Task-lane steering_returned is
    // handled in the isSubagent block (it must not mutate main-chat state).
    if (eventType === 'steering_returned' && !isSubagent) {
      const returnedMessages = event.messages || [];
      if (returnedMessages.length > 0) {
        // Remove steering user messages from the chat
        rt.setMessages((prev) => prev.filter((msg) => !('steering' in msg && msg.steering)));
        // Restore the text to the input box via state
        const combinedText = returnedMessages.map((m) => m.content).join('\n');
        rt.setReturnedSteering(combinedText);
      }
      return;
    }

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
        setIsCompacting: rt.setIsCompacting,
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
        deps.applyModelStatus({ kind: 'fallback', fromModel, toModel });
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
            refs,
            updateSubagentCard: rt.updateSubagentCard,
          });
        } else if (eventType === 'tool_call_chunks') {
          handleSubagentToolCallChunks({
            taskId,
            assistantMessageId: subagentAssistantMessageId,
            chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
            refs,
            updateSubagentCard: rt.updateSubagentCard,
          });
        } else if (eventType === 'tool_calls') {
          handleSubagentToolCalls({
            taskId,
            assistantMessageId: subagentAssistantMessageId,
            toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
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
        } else if (eventType === 'steering_delivered') {
          if (event.content) {
            handleTaskSteeringAccepted({
              taskId,
              content: event.content as string,
              refs,
              updateSubagentCard: rt.updateSubagentCard,
            });
          }
        } else if (eventType === 'user_message') {
          // Epoch-opening run boundary (spawn/resume instruction) — first
          // entry of the task stream. Same mechanics as a steering
          // follow-up: finalize the previous run, render the bubble,
          // open a new run.
          if (event.content) {
            handleTaskSteeringAccepted({
              taskId,
              content: event.content as string,
              refs,
              updateSubagentCard: rt.updateSubagentCard,
            });
          }
        } else if (eventType === 'steering_returned') {
          // Run ended before the task steering was delivered — surface the
          // returned text in the task transcript.
          const returned = (event.messages || []) as Array<{ content?: string }>;
          const combined = returned.map((m) => m.content || '').filter(Boolean).join('\n');
          if (combined) {
            appendTaskNotification(taskId, rt.t('chat.taskSteeringReturnedNotification'), combined);
          }
        } else if (eventType === 'error' || event.error) {
          // chan_close carries only the terminal status; the failure reason
          // exists only on this frame — surface it in the task transcript.
          const errMsg = (event.error || event.message || '') as string;
          appendTaskNotification(taskId, rt.t('chat.taskErrorNotification'), errMsg || undefined);
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
          setMessages: setMessagesForHandlers,
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
          setMessages: setMessagesForHandlers,
          eventId,
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
      const errorMessage = event.error || event.message || 'An error occurred while processing your request.';
      // Backend (streaming_handler.format_error_event) enriches the event
      // with ``error_kind``, ``status_code`` and ``hints``. We route the
      // display by kind to avoid showing the same error twice:
      //   - upstream → inline card on the failed assistant turn (part of
      //     the transcript; the user's model choice is what triggered it)
      //   - internal → banner near the chat input (our service failed;
      //     don't pollute the conversation history)
      const kind = event.error_kind as 'upstream' | 'internal' | undefined;
      // Models the resilience middleware tried this turn (primary + fallbacks).
      // Type-filtered defensively — the backend fields may be absent or partial.
      const attemptedModels = Array.isArray(event.attempted_models)
        ? (event.attempted_models as unknown[])
            .filter((m): m is Record<string, unknown> => !!m && typeof m === 'object')
            .map((m) => ({
              model: typeof m.model === 'string' ? (m.model as string) : '',
              error: typeof m.error === 'string' ? (m.error as string) : undefined,
              statusCode: typeof m.status_code === 'number'
                ? (m.status_code as number)
                : (m.status_code === null ? null : undefined),
              attempts: typeof m.attempts === 'number' ? (m.attempts as number) : undefined,
            }))
            .filter((m) => m.model)
        : undefined;
      const structured: StructuredError | undefined =
        kind === 'upstream' || kind === 'internal'
          ? {
              message: errorMessage as string,
              kind,
              statusCode: typeof event.status_code === 'number' ? event.status_code : undefined,
              // ``hints`` are only meaningful for upstream provider errors
              // (the bullets say "check your API key / plan / provider
              // status"). An internal error doesn't get hints even if the
              // backend ever starts emitting them for some future variant.
              hints: kind === 'upstream' && Array.isArray(event.hints)
                ? (event.hints.filter(isUpstreamHint) as StructuredError['hints'])
                : undefined,
              // User-configured model + the full attempt list, so the display
              // can show a model-aware headline and an "Also tried" line.
              model: typeof event.model === 'string' ? event.model : undefined,
              attemptedModels: attemptedModels && attemptedModels.length > 0 ? attemptedModels : undefined,
            }
          : undefined;

      if (kind === 'internal') {
        // Banner only — drop the optimistic assistant bubble entirely so the
        // transcript stays clean. Matches the 429 rate-limit path; leaving a
        // content-less bubble under the banner looks broken.
        rt.setMessageError(structured ?? (errorMessage as string));
        rt.setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
      } else {
        // Upstream (or unclassified legacy) — render inline. Clear any
        // stale banner from a prior turn so the error lives in one place.
        rt.setMessageError(null);
        rt.setMessages((prev) =>
          updateMessage(prev, assistantMessageId, (msg) => ({
            ...msg,
            content: msg.content || errorMessage,
            contentType: 'text',
            isStreaming: false,
            error: true,
            ...(structured ? { structuredError: structured } : {}),
          }))
        );
      }
    } else if (eventType === 'tool_call_chunks') {
      handleToolCallChunks({
        assistantMessageId,
        chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
        setMessages: setMessagesForHandlers,
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
            rt.toolCallIdToTaskIdMapRef.current,
          );
          pendingTaskToolCallIds.length = 0;
          pendingTaskToolCallIds.push(...updated);
        }

        if (action === 'init') {
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
          if (rt.updateSubagentCard) {
            // Prefer preserving the original spawn description (already on the card).
            // But after reconnect the card may have been wiped + recreated without a
            // description, so fall back to subagentHistoryRef as a safety net.
            const historyDesc = rt.subagentHistoryRef.current?.[agentId]?.description;
            const historyPrompt = rt.subagentHistoryRef.current?.[agentId]?.prompt;
            rt.updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${task_id}`,
              taskId: agentId,
              type: (type || 'general-purpose') as string,
              status: 'active',
              isActive: true,
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
        }
      }
    } else if (eventType === 'tool_call_result') {
      // A tool result means the turn is producing output — drop the pill.
      deps.clearModelStatus();
      // Check if this resolves an unresolved interrupt from history replay (FIFO array matching)
      const unresolvedList = refs.unresolvedHistoryInterruptRef?.current as HistoryInterruptInfo[] | undefined;
      if (unresolvedList && unresolvedList.length > 0 && typeof event.content === 'string') {
        const content = event.content as string;

        // Try create_workspace / start_question / ptc_agent / secretary actions
        const matchIdx = unresolvedList.findIndex((u: HistoryInterruptInfo) => PROPOSAL_INTERRUPT_TYPES.has(u.type));
        if (matchIdx !== -1) {
          const matched = unresolvedList[matchIdx];
          const dataKey = PROPOSAL_DATA_KEY_MAP[matched.type] || 'questionProposals';
          let resolvedStatus = 'approved';
          let resultPayload: Record<string, unknown> | null = null;
          if (content.startsWith('User declined')) {
            resolvedStatus = 'rejected';
          } else {
            try { const p = JSON.parse(content); if (p?.success === false) resolvedStatus = 'rejected'; resultPayload = p; } catch { /* not JSON */ }
          }
          const proposalId = matched.proposalId!;
          const extraFields: Record<string, unknown> = {};
          if (matched.type === 'ptc_agent' && resultPayload) {
            if (resultPayload.thread_id) extraFields.thread_id = resultPayload.thread_id;
            if (resultPayload.workspace_id) extraFields.workspace_id = resultPayload.workspace_id;
          }
          rt.setMessages((prev) =>
            updateMessage(prev,matched.assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              [dataKey]: {
                ...((msg as unknown as Record<string, Record<string, unknown>>)[dataKey] || {}),
                [proposalId]: {
                  ...((msg as unknown as Record<string, Record<string, Record<string, unknown>>>)[dataKey]?.[proposalId] || {}),
                  status: resolvedStatus,
                  ...extraFields,
                },
              },
            }; })
          );
          unresolvedList.splice(matchIdx, 1);
        }
      }

      const toolCallId = event.tool_call_id as string;

      // Build toolCallId → agentId mapping from Task tool artifact
      if (event.artifact?.task_id && toolCallId) {
        const agentId = `task:${event.artifact.task_id}`;
        rt.toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
      }

      handleToolCallResult({
        assistantMessageId,
        toolCallId,
        result: {
          content: event.content,
          content_type: event.content_type,
          tool_call_id: toolCallId,
          artifact: event.artifact,
        },
        refs,
        setMessages: setMessagesForHandlers,
      });

      // When onboarding-related tools succeed, sync onboarding_completed via PUT
      if (rt.onOnboardingRelatedToolComplete && isOnboardingRelatedToolSuccess(event.content)) {
        rt.onOnboardingRelatedToolComplete();
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

      // Update ptcAgentProposals with thread_id/workspace_id from tool result.
      // After HITL resume, the tool_call_result arrives on a NEW assistant message
      // while the proposals live on the OLD one (from the interrupt turn).
      // Match by tool_call_id for exact correlation (safe under concurrent dispatches).
      if (rt.pendingPTCBackfillRef.current.size > 0 && typeof event.content === 'string') {
        const backfillPid = toolCallId ? rt.pendingPTCBackfillRef.current.get(toolCallId) : undefined;
        if (backfillPid) {
          try {
            const parsed = JSON.parse(event.content);
            if (parsed?.success && parsed?.thread_id && parsed?.workspace_id) {
              rt.pendingPTCBackfillRef.current.delete(toolCallId);
              rt.setMessages((prev) =>
                prev.map((m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  const proposals = msg.ptcAgentProposals;
                  if (!proposals?.[backfillPid]) return m;
                  return {
                    ...msg,
                    ptcAgentProposals: {
                      ...proposals,
                      [backfillPid]: {
                        ...proposals[backfillPid],
                        thread_id: parsed.thread_id,
                        workspace_id: parsed.workspace_id,
                      },
                    },
                  };
                })
              );
            }
          } catch { /* not JSON, ignore */ }
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

  return processEvent;
};
