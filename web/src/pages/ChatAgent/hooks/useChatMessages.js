/**
 * Custom hook for managing chat messages and streaming
 * 
 * Handles:
 * - Message state management
 * - Thread ID management (persisted per workspace)
 * - Message sending with SSE streaming
 * - Conversation history loading
 * - Streaming updates and error handling
 * 
 * @param {string} workspaceId - The workspace ID for the chat session
 * @param {string} [initialThreadId] - Optional initial thread ID (from URL params)
 * @returns {Object} Message state and handlers
 */

import { useState, useRef, useEffect, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { sendChatMessageStream, replayThreadHistory, getWorkflowStatus, reconnectToWorkflowStream, sendHitlResponse, streamSubagentTaskEvents, fetchThreadTurns } from '../utils/api';
import { getStoredThreadId, setStoredThreadId } from './utils/threadStorage';
export { removeStoredThreadId } from './utils/threadStorage';
import { createUserMessage, createAssistantMessage, createNotificationMessage, insertMessage, appendMessage, updateMessage } from './utils/messageHelpers';
import { createRecentlySentTracker } from './utils/recentlySentTracker';
import {
  handleReasoningSignal,
  handleReasoningContent,
  handleTextContent,
  handleToolCalls,
  handleToolCallResult,
  handleToolCallChunks,
  handleTodoUpdate,
  isSubagentEvent,
  handleSubagentMessageChunk,
  handleSubagentToolCallChunks,
  handleSubagentToolCalls,
  handleSubagentToolCallResult,
  handleTaskMessageQueued,
  getOrCreateTaskRefs,
} from './utils/streamEventHandlers';
import {
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryToolCallResult,
  handleHistoryTodoUpdate,
  handleHistoryQueuedMessageInjected,
  isSubagentHistoryEvent,
} from './utils/historyEventHandlers';

/**
 * Checks if a tool result indicates an onboarding-related success.
 * Onboarding tools: update_user_data for risk_preference, watchlist_item, portfolio_holding.
 * @param {string|object} resultContent - Raw result content (JSON string or parsed object)
 * @returns {boolean}
 */
function isOnboardingRelatedToolSuccess(resultContent) {
  if (resultContent == null) return false;
  let parsed;
  if (typeof resultContent === 'string') {
    try {
      parsed = JSON.parse(resultContent);
    } catch {
      return false;
    }
  } else if (typeof resultContent === 'object') {
    parsed = resultContent;
  } else {
    return false;
  }
  if (!parsed || parsed.success !== true) return false;
  return !!(parsed.risk_preference || parsed.watchlist_item || parsed.portfolio_holding);
}

/**
 * Shared handler for context_window SSE events (token_usage, summarize, offload).
 * Used by both history replay and live stream to avoid duplication.
 *
 * @param {Object} event - The context_window event
 * @param {Object} callbacks
 * @param {Function} callbacks.getMsgId - Returns current assistant message ID (or null)
 * @param {Function} callbacks.nextOrder - Returns next content order counter value
 * @param {Function} callbacks.setMessages - React state setter for messages
 * @param {Function} callbacks.setTokenUsage - React state setter for token usage
 * @param {Function|null} callbacks.setIsCompacting - React state setter (null for history)
 * @param {Function} callbacks.insertNotification - Fallback: inserts standalone notification message
 * @param {Function} callbacks.t - i18n translation function
 * @param {React.MutableRefObject} callbacks.offloadBatch - Mutable ref for batching offload events
 */
function handleContextWindowEvent(event, { getMsgId, nextOrder, setMessages, setTokenUsage, setIsCompacting, insertNotification, t, offloadBatch }) {
  const action = event.action;

  if (action === 'token_usage') {
    const callInput = event.input_tokens || 0;
    const callOutput = event.output_tokens || 0;
    setTokenUsage((prev) => ({
      totalInput: (prev?.totalInput || 0) + callInput,
      totalOutput: (prev?.totalOutput || 0) + callOutput,
      lastOutput: callOutput,
      total: event.total_tokens || 0,
      threshold: event.threshold || prev?.threshold || 0,
    }));
    return;
  }

  if (action === 'summarize') {
    if (setIsCompacting && event.signal === 'start') {
      setIsCompacting('summarize');
      return;
    }
    if (setIsCompacting) setIsCompacting(false);
    if (event.signal === 'complete') {
      const text = t('chat.summarizedNotification', { from: event.original_message_count });
      const msgId = getMsgId();
      if (msgId) {
        const order = nextOrder();
        setMessages((prev) => updateMessage(prev, msgId, (msg) => ({
          ...msg,
          contentSegments: [...(msg.contentSegments || []), { type: 'notification', content: text, order }],
        })));
      } else {
        insertNotification(text);
      }
    }
    return;
  }

  if (action === 'offload') {
    if (event.signal === 'complete') {
      const batch = offloadBatch;

      // Accumulate counts
      if (event.kind === 'reads') {
        batch.current.reads += event.offloaded_reads || 0;
      } else if (event.kind === 'args') {
        batch.current.args += event.offloaded_args || 0;
      } else {
        // Manual /offload — combined event
        batch.current.args += event.offloaded_args || 0;
        batch.current.reads += event.offloaded_reads || 0;
      }

      // Capture msgId from first event in batch
      if (batch.current.msgId === undefined) {
        batch.current.msgId = getMsgId();
      }

      // Debounce: merge back-to-back offload events into a single notification
      clearTimeout(batch.current.timer);
      batch.current.timer = setTimeout(() => {
        const { args, reads, msgId } = batch.current;
        let text;
        if (args > 0 && reads > 0) {
          text = t('chat.offloadedNotification', { args, reads });
        } else if (reads > 0) {
          text = t('chat.offloadedReadsNotification', { count: reads });
        } else if (args > 0) {
          text = t('chat.offloadedArgsNotification', { count: args });
        }

        if (text) {
          if (msgId) {
            const order = nextOrder();
            setMessages((prev) => updateMessage(prev, msgId, (msg) => ({
              ...msg,
              contentSegments: [...(msg.contentSegments || []), { type: 'notification', content: text, order }],
            })));
          } else {
            insertNotification(text);
          }
        }

        // Reset batch
        batch.current = { args: 0, reads: 0, timer: null, msgId: undefined };
      }, 100);
    }
    return;
  }
}

export function useChatMessages(workspaceId, initialThreadId = null, updateTodoListCard = null, updateSubagentCard = null, inactivateAllSubagents = null, completePendingTodos = null, onOnboardingRelatedToolComplete = null, onFileArtifact = null, agentMode = 'ptc', clearSubagentCards = null, onWorkspaceCreated = null) {
  const { t } = useTranslation();
  // State
  const [messages, setMessages] = useState([]);
  const [threadId, setThreadId] = useState(() => {
    // If threadId is provided from URL, use it; otherwise use localStorage
    if (initialThreadId) {
      return initialThreadId;
    }
    return workspaceId ? getStoredThreadId(workspaceId) : '__default__';
  });
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [hasActiveSubagents, setHasActiveSubagents] = useState(false);  // Subagent streams open after main agent finished
  const [workspaceStarting, setWorkspaceStarting] = useState(false);  // Workspace is starting up (stopped/archived sandbox)
  const [isCompacting, setIsCompacting] = useState(false);  // Context compaction in progress (summarization/offload)
  const [messageError, setMessageError] = useState(null);
  // HITL (Human-in-the-Loop) plan mode interrupt state
  const [pendingInterrupt, setPendingInterrupt] = useState(null);
  // When user clicks Reject on a plan, this stores the interruptId so the next message
  // sent via handleSendMessage is routed as rejection feedback via hitl_response.
  const [pendingRejection, setPendingRejection] = useState(null);

  // Token usage tracking (for context window progress ring)
  const [tokenUsage, setTokenUsage] = useState(null);
  const [isShared, setIsShared] = useState(false);

  // Track current plan mode so HITL resume can forward it
  const currentPlanModeRef = useRef(false);

  // Refs for streaming state
  const currentMessageRef = useRef(null);
  const contentOrderCounterRef = useRef(0);
  const currentReasoningIdRef = useRef(null);
  const currentToolCallIdRef = useRef(null);
  const queuedAtOrderRef = useRef(null); // Shared across streams for queued message rollback

  // Refs for history loading state
  const historyLoadingRef = useRef(false);
  const historyMessagesRef = useRef(new Set()); // Track message IDs from history
  const newMessagesStartIndexRef = useRef(0); // Index where new messages start

  // Track if streaming is in progress to prevent history loading during streaming
  const isStreamingRef = useRef(false);

  // Track if history replay found an unresolved interrupt (skip reconnection in that case)
  const historyHasUnresolvedInterruptRef = useRef(false);
  // Store the full interrupt details from history so loadAndMaybeReconnect can decide
  // whether to make it interactive or reconnect to get resolution events
  const unresolvedHistoryInterruptRef = useRef([]);

  // Batch parallel interrupt responses: track all interrupt IDs in current batch
  // and collect individual responses until all are answered, then resume at once.
  const pendingInterruptIdsRef = useRef(new Set());
  const collectedHitlResponsesRef = useRef({});

  // Track the last received SSE event ID for reconnection
  const lastEventIdRef = useRef(null);
  // Ref-based thread ID for use inside closures (avoids stale React state in callbacks)
  const threadIdRef = useRef(threadId);
  // Batch back-to-back offload events into a single notification
  const offloadBatchRef = useRef({ args: 0, reads: 0, timer: null });
  // Track reconnection state for UI indicator
  const [isReconnecting, setIsReconnecting] = useState(false);

  // Track if this is a new conversation (for todo list card management)
  const isNewConversationRef = useRef(false);

  // Recently sent messages tracker
  const recentlySentTrackerRef = useRef(createRecentlySentTracker());

  // Map tool call IDs (from main agent's task tool calls) to agent_ids for routing subagent events
  const toolCallIdToTaskIdMapRef = useRef(new Map()); // Map<toolCallId, agentId>

  // Per-task SSE connections: taskId → AbortController
  const subagentStreamsRef = useRef(new Map());

  // Track completed task IDs to prevent reactivation by stale artifact events
  const completedTaskIdsRef = useRef(new Set());

  // Track subagent history loaded from replay so it can be shown lazily
  // Keyed by agent_id. Structure: { [agentId]: { taskId, description, type, messages, status, ... } }
  const subagentHistoryRef = useRef({});

  // Persistent subagent state refs — survives across turns so resumed subagents
  // retain messages from previous runs. Keyed by taskId (e.g., "task:k7Xm2p").
  const subagentStateRefsRef = useRef({});

  // During history load: queue task tool call IDs until the matching artifact 'spawned' event drains them
  const historyPendingTaskToolCallIdsRef = useRef([]);

  // Keep threadIdRef in sync with state (for use inside closures)
  useEffect(() => {
    threadIdRef.current = threadId;
    if (workspaceId && threadId && threadId !== '__default__') {
      setStoredThreadId(workspaceId, threadId);
    }
  }, [workspaceId, threadId]);

  // Reset thread ID when workspace or initialThreadId changes
  useEffect(() => {
    if (workspaceId) {
      // If initialThreadId is provided, use it; otherwise use localStorage
      const newThreadId = initialThreadId || getStoredThreadId(workspaceId);

      // Only update and clear if we're switching to a different thread
      // Don't clear if we're just updating from '__default__' to the actual thread ID (handled by streaming)
      const currentThreadId = threadId;
      const isThreadSwitch = currentThreadId &&
        currentThreadId !== '__default__' &&
        newThreadId !== '__default__' &&
        currentThreadId !== newThreadId;

      if (currentThreadId !== newThreadId) {
        setThreadId(newThreadId);
      }

      // Clear messages only when switching to a different existing thread
      // Preserve messages when transitioning from '__default__' to actual thread ID
      if (isThreadSwitch) {
        setMessages([]);
        // Reset refs
        contentOrderCounterRef.current = 0;
        currentReasoningIdRef.current = null;
        currentToolCallIdRef.current = null;
        queuedAtOrderRef.current = null;
        historyLoadingRef.current = false;
        historyMessagesRef.current.clear();
        newMessagesStartIndexRef.current = 0;
        recentlySentTrackerRef.current.clear();
      }
    }
  }, [workspaceId, initialThreadId]);

  /**
   * Loads conversation history for the current workspace and thread
   * Uses the threadId from state (which should be a valid thread ID, not '__default__')
   */
  const loadConversationHistory = async () => {
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current) {
      return;
    }

    try {
      historyLoadingRef.current = true;
      historyHasUnresolvedInterruptRef.current = false;
      setIsLoadingHistory(true);
      setMessageError(null);

      const threadIdToUse = threadId;
      console.log('[History] Loading history for thread:', threadIdToUse);

      // Track pairs being processed - use Map to handle multiple pairs
      const assistantMessagesByPair = new Map(); // Map<turn_index, assistantMessageId>
      const pairStateByPair = new Map(); // Map<turn_index, { contentOrderCounter, reasoningId, toolCallId }>
      
      // Track the currently active pair for artifacts (which don't have turn_index)
      // This ensures artifacts get the correct chronological order
      let currentActivePairIndex = null;
      let currentActivePairState = null;

      // Track pending HITL interrupts from history to resolve status on next user_message
      const pendingHistoryInterrupts = [];

      // Track subagent events by task ID for this history load
      // Map<taskId, { messages: Array, events: Array, description?: string, type?: string }>
      const subagentHistoryByTaskId = new Map();
      // Track which agentIds had message_queued actions (for inline card "Updated" label)
      const messageQueuedAgentIds = new Set();
      try {
        await replayThreadHistory(threadIdToUse, (event) => {
        const eventType = event.event;
        const contentType = event.content_type;
        const hasRole = event.role !== undefined;
        const hasPairIndex = event.turn_index !== undefined;

        // Track last event ID so reconnectToStream can deduplicate
        if (event._eventId != null) {
          lastEventIdRef.current = event._eventId;
        }

        // Check if this is a subagent event - filter it out from main chat view
        const isSubagent = isSubagentHistoryEvent(event);
        
        // Update current active pair when we see an event with turn_index
        if (hasPairIndex) {
          const pairIndex = event.turn_index;
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
          console.log('[History] Updated active pair to:', pairIndex, 'counter:', currentActivePairState?.contentOrderCounter);
        }

        // Handle context_window events from history (token_usage, summarize, offload)
        // Subagent context_window events are routed through the isSubagent block below.
        if (eventType === 'context_window' && !isSubagent) {
          handleContextWindowEvent(event, {
            getMsgId: () => currentActivePairIndex !== null
              ? assistantMessagesByPair.get(currentActivePairIndex) : null,
            nextOrder: () => {
              const eventId = event._eventId;
              if (eventId != null) return eventId;
              if (currentActivePairState) {
                currentActivePairState.contentOrderCounter++;
                return currentActivePairState.contentOrderCounter;
              }
              return 0;
            },
            setMessages,
            setTokenUsage,
            setIsCompacting: null,  // no start events in replayed history
            insertNotification: () => {},  // standalone notifications not needed in replay
            t,
            offloadBatch: offloadBatchRef,
          });
          return;
        }

        // Backward compat: handle old token_usage events from history
        if (eventType === 'token_usage') {
          const callInput = event.input_tokens || 0;
          const callOutput = event.output_tokens || 0;
          setTokenUsage((prev) => ({
            totalInput: (prev?.totalInput || 0) + callInput,
            totalOutput: (prev?.totalOutput || 0) + callOutput,
            lastOutput: callOutput,
            total: event.total_tokens || 0,
            threshold: event.threshold || prev?.threshold || 0,
          }));
          return;
        }

        // Handle queued_message_injected events from sse_events
        if (eventType === 'queued_message_injected' && hasPairIndex) {
          handleHistoryQueuedMessageInjected({
            event,
            pairIndex: event.turn_index,
            assistantMessagesByPair,
            pairStateByPair,
            refs: { newMessagesStartIndexRef, historyMessagesRef },
            setMessages,
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
                resumePoints: [],
              });
            }

            const subagentHistory = subagentHistoryByTaskId.get(taskId);
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
        // Note: event.content may be empty for HITL resume pairs (plan approval/rejection)
        if (eventType === 'user_message' && hasPairIndex) {
          // Resolve pending plan_approval interrupt from content (empty = approved, non-empty = rejected).
          {
            const idx = pendingHistoryInterrupts.findIndex((p) => p.type === 'plan_approval');
            if (idx !== -1) {
              const matched = pendingHistoryInterrupts[idx];
              const hasContent = event.content && event.content.trim();
              const resolvedStatus = hasContent ? 'rejected' : 'approved';
              setMessages((prev) =>
                updateMessage(prev, matched.assistantMessageId, (msg) => ({
                  ...msg,
                  planApprovals: {
                    ...(msg.planApprovals || {}),
                    [matched.planApprovalId]: {
                      ...(msg.planApprovals?.[matched.planApprovalId] || {}),
                      status: resolvedStatus,
                    },
                  },
                }))
              );
              pendingHistoryInterrupts.splice(idx, 1);
            }
          }

          // Resolve ask_user_question interrupts from resume query metadata (hitl_answers).
          // Persisted immediately by persist_query_start(), keyed by interrupt_id.
          {
            const hitlAnswers = event.metadata?.hitl_answers;
            if (hitlAnswers && pendingHistoryInterrupts.length > 0) {
              for (const [interruptId, answerValue] of Object.entries(hitlAnswers)) {
                const idx = pendingHistoryInterrupts.findIndex(
                  (p) => p.type === 'ask_user_question' && p.interruptId === interruptId
                );
                if (idx !== -1) {
                  const matched = pendingHistoryInterrupts[idx];
                  const resolvedStatus = answerValue !== null ? 'answered' : 'skipped';
                  setMessages((prev) =>
                    updateMessage(prev, matched.assistantMessageId, (msg) => ({
                      ...msg,
                      userQuestions: {
                        ...(msg.userQuestions || {}),
                        [matched.questionId]: {
                          ...(msg.userQuestions?.[matched.questionId] || {}),
                          status: resolvedStatus,
                          answer: answerValue,
                        },
                      },
                    }))
                  );
                  pendingHistoryInterrupts.splice(idx, 1);
                }
              }
            }
          }

          const pairIndex = event.turn_index;
          const refs = {
            recentlySentTracker: recentlySentTrackerRef.current,
            currentMessageRef,
            newMessagesStartIndexRef,
            historyMessagesRef,
          };

          handleHistoryUserMessage({
            event,
            pairIndex,
            assistantMessagesByPair,
            pairStateByPair,
            refs,
            messages,
            setMessages,
          });
          return;
        }

        // Handle message_chunk events (assistant messages)
        if (eventType === 'message_chunk' && hasRole && event.role === 'assistant' && hasPairIndex) {
          const pairIndex = event.turn_index;
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received message_chunk for unknown turn_index:', pairIndex);
            return;
          }

          // Process reasoning_signal
          if (contentType === 'reasoning_signal') {
            const signalContent = event.content || '';
            handleHistoryReasoningSignal({
              assistantMessageId: currentAssistantMessageId,
              signalContent,
              pairIndex,
              pairState,
              setMessages,
              eventId: event._eventId,
            });
            return;
          }

          // Handle reasoning content
          if (contentType === 'reasoning' && event.content) {
            handleHistoryReasoningContent({
              assistantMessageId: currentAssistantMessageId,
              content: event.content,
              pairState,
              setMessages,
            });
            return;
          }

          // Handle text content
          if (contentType === 'text' && event.content) {
            handleHistoryTextContent({
              assistantMessageId: currentAssistantMessageId,
              content: event.content,
              finishReason: event.finish_reason,
              pairState,
              setMessages,
              eventId: event._eventId,
            });
            return;
          }

          // Handle finish_reason (end of assistant message)
          if (event.finish_reason) {
            setMessages((prev) =>
              updateMessage(prev, currentAssistantMessageId, (msg) => ({
                ...msg,
                isStreaming: false,
              }))
            );
            return;
          }
        }

        // Filter out tool_call_chunks events
        if (eventType === 'tool_call_chunks') {
          return;
        }

        // Handle artifact events (e.g., todo_update)
        // In history replay, artifacts DO have turn_index, so we can use it directly
        if (eventType === 'artifact') {
          const artifactType = event.artifact_type;
          if (artifactType === 'todo_update') {
            const payload = event.payload || {};

            // Update floating todo card from history (last event wins, shows final state)
            if (updateTodoListCard) {
              updateTodoListCard({
                todos: payload.todos || [],
                total: payload.total || 0,
                completed: payload.completed || 0,
                in_progress: payload.in_progress || 0,
                pending: payload.pending || 0,
                fromHistory: true,
              });
            }

            // Artifacts in history replay have turn_index - use it!
            if (hasPairIndex) {
              const pairIndex = event.turn_index;
              // Update active pair tracking
              currentActivePairIndex = pairIndex;
              currentActivePairState = pairStateByPair.get(pairIndex);

              const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
              const pairState = pairStateByPair.get(pairIndex);

              if (!currentAssistantMessageId || !pairState) {
                console.warn('[History] Received artifact for unknown turn_index:', pairIndex);
                return;
              }

              console.log('[History] Processing todo_update artifact for pair:', pairIndex, 'counter:', pairState.contentOrderCounter);
              handleHistoryTodoUpdate({
                assistantMessageId: currentAssistantMessageId,
                artifactType,
                artifactId: event.artifact_id,
                payload,
                pairState: pairState,
                setMessages,
                eventId: event._eventId,
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
                  artifactType,
                  artifactId: event.artifact_id,
                  payload,
                  pairState: targetPairState,
                  setMessages,
                  eventId: event._eventId,
                });
              }
            }
          }
          if (artifactType === 'task') {
            const payload = event.payload || {};
            const { task_id, action: rawAction, description, prompt, type } = payload;
            const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'message_queued') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })();
            if (task_id) {
              const agentId = `task:${task_id}`;
              if (!subagentHistoryByTaskId.has(agentId)) {
                subagentHistoryByTaskId.set(agentId, {
                  messages: [],
                  events: [],
                  description: description || '',
                  prompt: prompt || description || '',
                  type: type || 'general-purpose',
                  resumePoints: [],
                });
              } else {
                const existing = subagentHistoryByTaskId.get(agentId);
                if (description && !existing.description) existing.description = description;
                if (prompt && !existing.prompt) existing.prompt = prompt || description || '';
                if (type && !existing.type) existing.type = type;
              }
              // Track resume boundaries for history replay
              if (action === 'resume') {
                const existing = subagentHistoryByTaskId.get(agentId);
                if (existing) {
                  existing.resumePoints = existing.resumePoints || [];
                  existing.resumePoints.push({
                    description: description || 'Resume',
                    turnIndex: event.turn_index,
                  });
                }
              }
              // Track message_queued actions for inline card "Updated" label
              if (action === 'update') {
                messageQueuedAgentIds.add(agentId);
              }
              // Map tool_call_id from the event context
              if (event.tool_call_id) {
                toolCallIdToTaskIdMapRef.current.set(event.tool_call_id, agentId);
              }
              // Match pending tool call IDs from earlier tool_calls events.
              // The artifact 'spawned' event drains the pending queue to
              // establish the toolCallId → agentId mapping for replay.
              if (action === 'init') {
                const pendingToolCallIds = historyPendingTaskToolCallIdsRef.current;
                if (pendingToolCallIds.length > 0) {
                  const toolCallId = pendingToolCallIds[0];
                  if (!toolCallIdToTaskIdMapRef.current.has(toolCallId)) {
                    toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
                  }
                  historyPendingTaskToolCallIdsRef.current = pendingToolCallIds.slice(1);
                }
              }
            }
          }
          return;
        }

        // Handle tool_calls events
        if (eventType === 'tool_calls' && hasPairIndex) {
          const pairIndex = event.turn_index;
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
            const toolCallIds = taskToolCalls.map((tc) => tc.id).filter(Boolean);
            if (toolCallIds.length > 0) {
              historyPendingTaskToolCallIdsRef.current = [
                ...historyPendingTaskToolCallIdsRef.current,
                ...toolCallIds,
              ];
            }
          }

          handleHistoryToolCalls({
            assistantMessageId: currentAssistantMessageId,
            toolCalls: event.tool_calls,
            pairState,
            setMessages,
            eventId: event._eventId,
          });
          return;
        }

        // Handle tool_call_result events
        if (eventType === 'tool_call_result' && hasPairIndex) {
          const pairIndex = event.turn_index;
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
          if (event.artifact?.task_id && event.tool_call_id) {
            const agentId = `task:${event.artifact.task_id}`;
            toolCallIdToTaskIdMapRef.current.set(event.tool_call_id, agentId);

            // Ensure subagentHistoryByTaskId has description from artifact.
            // Resume calls are filtered out of the tool_calls handler, so this
            // is the only place to pick up the description for resumed tasks.
            if (event.artifact.description) {
              const existing = subagentHistoryByTaskId.get(agentId);
              if (existing) {
                if (!existing.description) existing.description = event.artifact.description;
                if (!existing.prompt) existing.prompt = event.artifact.prompt || event.artifact.description || '';
              } else {
                subagentHistoryByTaskId.set(agentId, {
                  messages: [],
                  events: [],
                  description: event.artifact.description,
                  prompt: event.artifact.prompt || event.artifact.description || '',
                  type: event.artifact.type || 'general-purpose',
                });
              }
            }
          }

          handleHistoryToolCallResult({
            assistantMessageId: currentAssistantMessageId,
            toolCallId: event.tool_call_id,
            result: {
              content: event.content,
              content_type: event.content_type,
              tool_call_id: event.tool_call_id,
              artifact: event.artifact,
            },
            pairState,
            setMessages,
          });

          // Resolve pending create_workspace or start_question interrupt from tool_call_result
          {
            const idx = pendingHistoryInterrupts.findIndex((p) => p.type === 'create_workspace' || p.type === 'start_question');
            if (idx !== -1 && typeof event.content === 'string') {
              const matched = pendingHistoryInterrupts[idx];
              const content = event.content;
              const dataKey = matched.type === 'create_workspace' ? 'workspaceProposals' : 'questionProposals';

              let resolvedStatus = 'approved';
              if (content === 'User declined workspace creation.' || content === 'User declined starting the question.') {
                resolvedStatus = 'rejected';
              } else {
                try {
                  const parsed = JSON.parse(content);
                  if (parsed?.success === false) resolvedStatus = 'rejected';
                } catch { /* non-JSON → treat as approved */ }
              }

              setMessages((prev) =>
                updateMessage(prev, matched.assistantMessageId, (msg) => ({
                  ...msg,
                  [dataKey]: {
                    ...(msg[dataKey] || {}),
                    [matched.proposalId]: {
                      ...(msg[dataKey]?.[matched.proposalId] || {}),
                      status: resolvedStatus,
                    },
                  },
                }))
              );
              pendingHistoryInterrupts.splice(idx, 1);
            }
          }

          return;
        }

        // Handle interrupt events during history replay
        if (eventType === 'interrupt') {
          const pairIndex = event.turn_index ?? currentActivePairIndex;
          const interruptAssistantId = pairIndex != null ? assistantMessagesByPair.get(pairIndex) : null;
          const pairState = pairIndex != null ? pairStateByPair.get(pairIndex) : null;

          if (interruptAssistantId && pairState) {
            const actionType = event.action_requests?.[0]?.type;

            if (actionType === 'ask_user_question') {
              // --- User question interrupt (history) ---
              const questionId = event.interrupt_id || `question-history-${Date.now()}`;
              const questionData = event.action_requests[0];
              const order = event._eventId != null ? event._eventId : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev, interruptAssistantId, (msg) => ({
                  ...msg,
                  contentSegments: [
                    ...(msg.contentSegments || []),
                    { type: 'user_question', questionId, order },
                  ],
                  userQuestions: {
                    ...(msg.userQuestions || {}),
                    [questionId]: {
                      question: questionData.question,
                      options: questionData.options || [],
                      allow_multiple: questionData.allow_multiple || false,
                      interruptId: event.interrupt_id,
                      status: 'pending', // Default pending; resolved by tool_call_result or user_message
                      answer: null,
                    },
                  },
                }))
              );

              pendingHistoryInterrupts.push({
                type: 'ask_user_question',
                assistantMessageId: interruptAssistantId,
                questionId,
                interruptId: event.interrupt_id,
                answer: null,
              });
            } else if (actionType === 'create_workspace') {
              // --- Create workspace interrupt (history) ---
              const proposalId = event.interrupt_id || `workspace-history-${Date.now()}`;
              const proposalData = event.action_requests[0];
              const order = event._eventId != null ? event._eventId : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev, interruptAssistantId, (msg) => ({
                  ...msg,
                  contentSegments: [
                    ...(msg.contentSegments || []),
                    { type: 'create_workspace', proposalId, order },
                  ],
                  workspaceProposals: {
                    ...(msg.workspaceProposals || {}),
                    [proposalId]: {
                      workspace_name: proposalData.workspace_name,
                      workspace_description: proposalData.workspace_description,
                      interruptId: event.interrupt_id,
                      status: 'pending',
                    },
                  },
                }))
              );

              pendingHistoryInterrupts.push({
                type: 'create_workspace',
                assistantMessageId: interruptAssistantId,
                proposalId,
                interruptId: event.interrupt_id,
              });
            } else if (actionType === 'start_question') {
              // --- Start question interrupt (history) ---
              const proposalId = event.interrupt_id || `question-start-history-${Date.now()}`;
              const proposalData = event.action_requests[0];
              const order = event._eventId != null ? event._eventId : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev, interruptAssistantId, (msg) => ({
                  ...msg,
                  contentSegments: [
                    ...(msg.contentSegments || []),
                    { type: 'start_question', proposalId, order },
                  ],
                  questionProposals: {
                    ...(msg.questionProposals || {}),
                    [proposalId]: {
                      workspace_id: proposalData.workspace_id,
                      question: proposalData.question,
                      interruptId: event.interrupt_id,
                      status: 'pending',
                    },
                  },
                }))
              );

              pendingHistoryInterrupts.push({
                type: 'start_question',
                assistantMessageId: interruptAssistantId,
                proposalId,
                interruptId: event.interrupt_id,
              });
            } else {
              // --- Plan approval interrupt (existing) ---
              const planApprovalId = event.interrupt_id || `plan-history-${Date.now()}`;
              const description =
                event.action_requests?.[0]?.description ||
                event.action_requests?.[0]?.args?.plan ||
                'No plan description provided.';
              const order = event._eventId != null ? event._eventId : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev, interruptAssistantId, (msg) => ({
                  ...msg,
                  contentSegments: [
                    ...(msg.contentSegments || []),
                    { type: 'plan_approval', planApprovalId, order },
                  ],
                  planApprovals: {
                    ...(msg.planApprovals || {}),
                    [planApprovalId]: {
                      description,
                      interruptId: event.interrupt_id,
                      status: 'pending', // Default pending; resolved on next user_message
                    },
                  },
                }))
              );

              pendingHistoryInterrupts.push({
                type: 'plan_approval',
                assistantMessageId: interruptAssistantId,
                planApprovalId,
                interruptId: event.interrupt_id,
              });
            }
          }
          return;
        }

        // Handle replay_done event (final event)
        if (eventType === 'replay_done') {
          if (event.thread_id && event.thread_id !== threadId && event.thread_id !== '__default__') {
            console.log('[History] Final thread_id event:', event.thread_id);
            setThreadId(event.thread_id);
            setStoredThreadId(workspaceId, event.thread_id);
          }
        } else if (eventType === 'credit_usage') {
          // credit_usage indicates the end of one conversation pair
          console.log('[History] Credit usage event (end of pair):', event.turn_index);
        } else if (!eventType) {
          // Fallback: Handle events without event type
          if (event.thread_id && !hasRole && !contentType) {
            console.log('[History] Fallback: thread_id only event:', event.thread_id);
            if (event.thread_id !== threadId && event.thread_id !== '__default__') {
              setThreadId(event.thread_id);
              setStoredThreadId(workspaceId, event.thread_id);
            }
          }
        } else {
          // Log unhandled event types for debugging
          console.log('[History] Unhandled event type:', {
            eventType,
            contentType,
            hasRole,
            role: event.role,
            hasPairIndex,
          });
        }
      });

        console.log('[History] Replay completed');

        // If there's still a pending interrupt after replay (no subsequent user_message
        // resolved it), store it in a ref. loadAndMaybeReconnect will decide whether to
        // make it interactive (workflow paused) or reconnect to get resolution events
        // (workflow active = interrupt was answered but resolution is in Redis buffer).
        if (pendingHistoryInterrupts.length > 0) {
          console.log('[History] Unresolved interrupts detected:', pendingHistoryInterrupts.length, pendingHistoryInterrupts.map((p) => p.type));
          historyHasUnresolvedInterruptRef.current = true;
          unresolvedHistoryInterruptRef.current = pendingHistoryInterrupts.map((p) => ({ ...p }));
          pendingHistoryInterrupts.length = 0;
        }

        // Process stored subagent events and build their messages
        // NOTE: During history replay we DO NOT open floating cards automatically.
        // We only build per-task message history here; cards are created lazily
        // when the user clicks \"Open subagent details\" in the main chat view.
        if (subagentHistoryByTaskId.size > 0) {
          console.log('[History] Processing subagent history for', subagentHistoryByTaskId.size, 'tasks');
          
          // Process each subagent's events
          for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
            // Create temporary refs structure for processing
            let currentRunIndex = 0;
            const tempSubagentStateRefs = {
              [taskId]: {
                contentOrderCounterRef: { current: 0 },
                currentReasoningIdRef: { current: null },
                currentToolCallIdRef: { current: null },
                messages: [],
                runIndex: 0,
              },
            };

            const tempRefs = {
              subagentStateRefs: tempSubagentStateRefs,
              isReconnect: true, // Suppress Date.now() timestamps so items go straight to accordion zone
            };

            // History-specific no-op updater: prevents floating cards from being
            // created during history load while still letting handlers build
            // the in-memory message structures in tempSubagentStateRefs.
            const historyUpdateSubagentCard = () => {};

            // Pre-compute resume boundary turn indices from stored resumePoints
            const resumePoints = subagentHistory.resumePoints || [];
            const resumeByTurnIndex = new Map();
            for (const rp of resumePoints) {
              if (rp.turnIndex != null) {
                resumeByTurnIndex.set(rp.turnIndex, rp);
              }
            }
            let lastTurnIndex = null;

            // Process each event in chronological order
            console.log('[History] Processing', subagentHistory.events.length, 'events for task:', taskId, 'resumePoints:', resumePoints.length);
            for (let i = 0; i < subagentHistory.events.length; i++) {
              const event = subagentHistory.events[i];
              const eventType = event.event;
              const contentType = event.content_type;

              // Detect resume boundary: turn_index transitions to a resume turn
              const eventTurnIndex = event.turn_index;
              if (eventTurnIndex != null && eventTurnIndex !== lastTurnIndex && resumeByTurnIndex.has(eventTurnIndex)) {
                const resumePoint = resumeByTurnIndex.get(eventTurnIndex);
                const taskRefsLocal = tempSubagentStateRefs[taskId];

                // Finalize the previous run's last assistant message
                for (let j = taskRefsLocal.messages.length - 1; j >= 0; j--) {
                  if (taskRefsLocal.messages[j].role === 'assistant' && taskRefsLocal.messages[j].isStreaming) {
                    taskRefsLocal.messages[j] = { ...taskRefsLocal.messages[j], isStreaming: false };
                    break;
                  }
                }

                // Inject user message with resume instruction
                taskRefsLocal.messages.push({
                  id: `resume-${taskId}-${currentRunIndex + 1}`,
                  role: 'user',
                  content: resumePoint.description || 'Resume',
                  contentSegments: [{ type: 'text', content: resumePoint.description || 'Resume', order: 0 }],
                  reasoningProcesses: {},
                  toolCallProcesses: {},
                });

                // Bump run index and reset per-run counters
                currentRunIndex++;
                taskRefsLocal.runIndex = currentRunIndex;
                taskRefsLocal.contentOrderCounterRef.current = 0;
                taskRefsLocal.currentReasoningIdRef.current = null;
                taskRefsLocal.currentToolCallIdRef.current = null;

                console.log('[History] Resume boundary detected at turn_index:', eventTurnIndex, 'runIndex:', currentRunIndex);
              }
              if (eventTurnIndex != null) {
                lastTurnIndex = eventTurnIndex;
              }

              // Use per-run assistant message ID
              const assistantMessageId = `subagent-${taskId}-assistant-${currentRunIndex}`;

              console.log('[History] Processing subagent event', i + 1, 'of', subagentHistory.events.length, ':', {
                taskId,
                eventType,
                contentType,
                hasContent: !!event.content,
                hasToolCalls: !!event.tool_calls,
                toolCallId: event.tool_call_id,
              });

              if (eventType === 'message_chunk' && event.role === 'assistant') {
                const result = handleSubagentMessageChunk({
                  taskId,
                  assistantMessageId,
                  contentType,
                  content: event.content,
                  finishReason: event.finish_reason,
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
                console.log('[History] handleSubagentMessageChunk result:', result);
              } else if (eventType === 'tool_calls' && event.tool_calls) {
                const result = handleSubagentToolCalls({
                  taskId,
                  assistantMessageId,
                  toolCalls: event.tool_calls,
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
                console.log('[History] handleSubagentToolCalls result:', result);
              } else if (eventType === 'tool_call_result') {
                const result = handleSubagentToolCallResult({
                  taskId,
                  assistantMessageId,
                  toolCallId: event.tool_call_id,
                  result: {
                    content: event.content,
                    content_type: event.content_type,
                    tool_call_id: event.tool_call_id,
                    artifact: event.artifact,
                  },
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
                console.log('[History] handleSubagentToolCallResult result:', result);
              } else if (eventType === 'subagent_followup_injected' || eventType === 'turn_start') {
                // Legacy subagent_followup_injected had content (queued user message).
                // turn_start was an inter-model-call boundary — no longer emitted,
                // but old persisted data may still contain it. Just extract content.
                if (event.content) {
                  handleTaskMessageQueued({
                    taskId,
                    content: event.content,
                    refs: tempRefs,
                    updateSubagentCard: historyUpdateSubagentCard,
                  });
                  // Sync local run index — handleTaskMessageQueued bumps runIndex
                  currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
                }
              } else if (eventType === 'message_queued') {
                if (event.content) {
                  handleTaskMessageQueued({
                    taskId,
                    content: event.content,
                    refs: tempRefs,
                    updateSubagentCard: historyUpdateSubagentCard,
                  });
                  // Sync local run index — handleTaskMessageQueued bumps runIndex
                  currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
                }
              } else if (eventType === 'context_window') {
                // Embed notification as content segment in the assistant message
                const action = event.action;
                if (action === 'token_usage') {
                  // Skip — no per-subagent token display
                } else {
                  let text;
                  if (action === 'summarize' && event.signal === 'complete') {
                    text = t('chat.summarizedNotification', { from: event.original_message_count });
                  } else if (action === 'offload' && event.signal === 'complete') {
                    const args = event.offloaded_args || 0;
                    const reads = event.offloaded_reads || 0;
                    if (args > 0 && reads > 0) text = t('chat.offloadedNotification', { args, reads });
                    else if (reads > 0) text = t('chat.offloadedReadsNotification', { count: reads });
                    else if (args > 0) text = t('chat.offloadedArgsNotification', { count: args });
                  }
                  if (text) {
                    const taskRefsLocal = tempSubagentStateRefs[taskId];
                    const order = ++taskRefsLocal.contentOrderCounterRef.current;
                    // Find the last assistant message and append notification segment
                    const msgIdx = taskRefsLocal.messages.findLastIndex(m => m.role === 'assistant');
                    if (msgIdx !== -1) {
                      const msg = { ...taskRefsLocal.messages[msgIdx] };
                      msg.contentSegments = [...(msg.contentSegments || []), { type: 'notification', content: text, order }];
                      taskRefsLocal.messages[msgIdx] = msg;
                    }
                  }
                }
              } else {
                console.warn('[History] Unhandled subagent event type:', eventType);
              }
            }
            
            // Get final messages from temp refs
            const rawMessages = tempSubagentStateRefs[taskId]?.messages || [];

            // Finalize messages: set isStreaming=false and close open reasoning/tool
            // processes on the last assistant message so SubagentStatusBar shows 'completed'.
            const finalMessages = rawMessages.map((msg) => {
              if (msg.role !== 'assistant') return msg;
              // Only finalize the last assistant message (or all, to be safe)
              const m = { ...msg, isStreaming: false };
              if (m.toolCallProcesses) {
                const procs = { ...m.toolCallProcesses };
                for (const [id, proc] of Object.entries(procs)) {
                  if (proc.isInProgress) {
                    procs[id] = { ...proc, isInProgress: false, isComplete: true };
                  }
                }
                m.toolCallProcesses = procs;
              }
              if (m.reasoningProcesses) {
                const rps = { ...m.reasoningProcesses };
                for (const [id, rp] of Object.entries(rps)) {
                  if (rp.isReasoning) {
                    rps[id] = { ...rp, isReasoning: false, reasoningComplete: true };
                  }
                }
                m.reasoningProcesses = rps;
              }
              return m;
            });

            // Get task metadata from stored history
            const taskMetadata = subagentHistoryByTaskId.get(taskId);

            // Store history in ref so it can be used when the user explicitly
            // opens the subagent card from the main chat view. We do NOT
            // create the floating card here.
            if (!subagentHistoryRef.current) {
              subagentHistoryRef.current = {};
            }
            subagentHistoryRef.current[taskId] = {
              taskId,
              description: taskMetadata?.description || '',
              prompt: taskMetadata?.prompt || taskMetadata?.description || '',
              type: taskMetadata?.type || 'general-purpose',
              messages: finalMessages,
              status: 'completed', // History events are always completed
              toolCalls: 0,
              currentTool: '',
            };

            // Seed persistent subagent state refs from history so that
            // reconnect or future resume can append to the existing messages.
            subagentStateRefsRef.current[taskId] = {
              contentOrderCounterRef: { current: tempSubagentStateRefs[taskId].contentOrderCounterRef.current },
              currentReasoningIdRef: { current: null },
              currentToolCallIdRef: { current: null },
              messages: finalMessages,
              runIndex: currentRunIndex,
            };

            console.log('[History] Stored subagent history for task:', taskId, 'with', finalMessages.length, 'messages, runIndex:', currentRunIndex);
          }
        }
      } catch (replayError) {
        // Handle 404 gracefully - it's expected for brand new threads that haven't been fully initialized yet
        if (replayError.message && replayError.message.includes('404')) {
          console.log('[History] Thread not found (404) - this is normal for new threads, skipping history load');
          // Don't set error message for 404 - it's expected for new threads
        } else {
          throw replayError; // Re-throw other errors
        }
      }

      // NOTE: markAllSubagentTasksCompleted() is NOT called here because
      // loadAndMaybeReconnect will call it after determining whether the
      // workflow is still active (reconnect case) or truly completed.

      // Post-process: update inline cards for message_queued actions to show "Updated"
      if (messageQueuedAgentIds.size > 0) {
        setMessages(prev => prev.map(msg => {
          if (!msg.subagentTasks) return msg;
          let changed = false;
          const newTasks = { ...msg.subagentTasks };
          for (const [tcId, task] of Object.entries(newTasks)) {
            if (task.resumeTargetId && messageQueuedAgentIds.has(task.resumeTargetId) && task.action === 'resume') {
              newTasks[tcId] = { ...task, action: 'update' };
              changed = true;
            }
          }
          return changed ? { ...msg, subagentTasks: newTasks } : msg;
        }));
      }

      setIsLoadingHistory(false);
      historyLoadingRef.current = false;
    } catch (error) {
      console.error('[History] Error loading conversation history:', error);
      // Only show error if it's not a 404 (404 is expected for new threads)
      if (!error.message || !error.message.includes('404')) {
        setMessageError(error.message || 'Failed to load conversation history');
      }
      setIsLoadingHistory(false);
      historyLoadingRef.current = false;
    }
  };

  /**
   * Reconnects to an in-progress workflow stream after page refresh.
   * Creates an assistant message placeholder and processes live SSE events.
   */
  const reconnectToStream = async ({ activeTasks = [] } = {}) => {
    if (!threadId || threadId === '__default__') return;

    console.log('[Reconnect] Starting reconnection for thread:', threadId);

    // Clear subagent cards to prevent duplicate content from cache + Redis overlap
    if (clearSubagentCards) {
      clearSubagentCards();
    }
    completedTaskIdsRef.current.clear();

    setIsLoading(true);
    setIsReconnecting(true);
    isStreamingRef.current = true;

    // Create assistant message placeholder for reconnection
    const assistantMessageId = `assistant-reconnect-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

    {
      const assistantMessage = createAssistantMessage(assistantMessageId);
      // Replace trailing empty history assistant message (created by history replay for the
      // in-progress pair) to avoid a duplicate bubble. If the last message is a non-empty
      // history assistant or something else, just append normally.
      setMessages((prev) => {
        if (prev.length > 0) {
          const lastMsg = prev[prev.length - 1];
          if (
            lastMsg.role === 'assistant' &&
            lastMsg.isHistory &&
            (!lastMsg.contentSegments || lastMsg.contentSegments.length === 0) &&
            !lastMsg.content
          ) {
            return [...prev.slice(0, -1), assistantMessage];
          }
        }
        return appendMessage(prev, assistantMessage);
      });
      currentMessageRef.current = assistantMessageId;
    }

    // Prepare refs for event handlers — use persistent subagent state
    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      queuedAtOrderRef,
      updateTodoListCard,
      isNewConversation: false,
      subagentStateRefs: subagentStateRefsRef.current,
      updateSubagentCard: updateSubagentCard
        ? (agentId, data) => updateSubagentCard(agentId, { ...data, isReconnect: true })
        : (() => {}),
      isReconnect: true,
      unresolvedHistoryInterruptRef,
    };

    const wasInterruptedRef = { current: false };
    const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

    try {
      // Replay buffered events first — this processes artifact{task,spawned} events
      // which create subagent cards with the correct description/type. Per-task streams
      // are opened AFTER so they merge into existing cards instead of creating empty ones.
      const result = await reconnectToWorkflowStream(threadId, lastEventIdRef.current, processEvent);
      if (result?.disconnected) {
        throw new Error('Reconnection stream disconnected');
      }

      // Mark message as complete
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );

      // Pre-seed subagent cards from history for tasks whose artifact events were
      // cleared from the Redis buffer after the spawning turn persisted to DB.
      // This mirrors the Scenario B pre-seed at lines 1611-1626.
      if (activeTasks.length > 0 && updateSubagentCard && subagentHistoryRef.current) {
        for (const taskId of activeTasks) {
          const agentId = `task:${taskId}`;
          const historyData = subagentHistoryRef.current[agentId];
          if (historyData) {
            updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${taskId}`,
              taskId: agentId,
              description: historyData.description || '',
              prompt: historyData.prompt || historyData.description || '',
              type: historyData.type || 'general-purpose',
              status: 'active',
              isActive: true,
              isReconnect: true,
            });
          }
        }
      }

      // Now open per-task SSE streams for active subagents. Per-task endpoints
      // replay from their own Redis buffer so no events are lost.
      if (activeTasks.length > 0) {
        console.log('[Reconnect] Opening per-task streams for active tasks:', activeTasks);
        for (const taskId of activeTasks) {
          openSubagentStream(threadId, taskId, processEvent);
        }
      }
    } catch (err) {
      // 404/410 = workflow no longer available, not a real error
      const status = err.message?.match(/status:\s*(\d+)/)?.[1];
      if (status === '404' || status === '410') {
        console.log('[Reconnect] Workflow no longer available (', status, '), cleaning up');
      } else {
        console.error('[Reconnect] Error during reconnection:', err);
        setMessageError(err.message || 'Failed to reconnect to stream');
      }
    } finally {
      setIsReconnecting(false);

      // Clean up empty reconnect messages (no content segments = nothing was streamed)
      setMessages((prev) => {
        const msg = prev.find((m) => m.id === assistantMessageId);
        if (msg && (!msg.contentSegments || msg.contentSegments.length === 0) && !msg.content) {
          return prev.filter((m) => m.id !== assistantMessageId);
        }
        return prev;
      });

      if (!wasInterruptedRef.current) {
        cleanupAfterStreamEnd(assistantMessageId);
      }
    }
  };

  /**
   * Attempts to auto-reconnect after a mid-stream network disconnect.
   * Uses exponential backoff (1s, 2s, 4s, 8s, 16s) with up to 5 retries.
   * Falls back to cleanupAfterStreamEnd if workflow completes or retries exhaust.
   */
  const attemptReconnectAfterDisconnect = async (assistantMessageId) => {
    const MAX_RETRIES = 5;
    const BASE_DELAY = 1000;

    setIsReconnecting(true);

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      if (!threadId || threadId === '__default__') break;

      if (attempt > 0) {
        await new Promise((r) => setTimeout(r, BASE_DELAY * Math.pow(2, attempt - 1)));
      }

      try {
        const status = await getWorkflowStatus(threadId);
        if (!status.can_reconnect) {
          console.log('[Reconnect] Workflow no longer reconnectable, cleaning up');
          break;
        }

        console.log('[Reconnect] Attempt', attempt + 1, 'of', MAX_RETRIES);
        await reconnectToStream({ activeTasks: status.active_tasks || [] });

        setIsReconnecting(false);
        return;
      } catch (err) {
        console.warn('[Reconnect] Attempt', attempt + 1, 'failed:', err.message);
      }
    }

    setIsReconnecting(false);
    cleanupAfterStreamEnd(assistantMessageId);
  };

  // Load history when workspace or threadId changes, then check for reconnection
  useEffect(() => {
    console.log('[History] useEffect triggered, workspaceId:', workspaceId, 'threadId:', threadId, 'isStreaming:', isStreamingRef.current);

    // Guard: Only load if we have a workspaceId and a valid threadId (not '__default__')
    // Also skip if streaming is in progress (prevents race condition when thread ID changes during streaming)
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current || isStreamingRef.current) {
      console.log('[History] Skipping load:', {
        workspaceId,
        threadId,
        isLoading: historyLoadingRef.current,
        isStreaming: isStreamingRef.current,
        reason: !workspaceId ? 'no workspaceId' :
          !threadId ? 'no threadId' :
            threadId === '__default__' ? 'default thread' :
              historyLoadingRef.current ? 'already loading' :
                isStreamingRef.current ? 'streaming in progress' :
                  'unknown'
      });
      return;
    }

    let cancelled = false;

    const loadAndMaybeReconnect = async () => {
      console.log('[History] Calling loadConversationHistory for thread:', threadId);

      // Check workflow status FIRST, then load history.
      // Sequential order avoids a race where /replay lands before the backend
      // persists Turn N (on_background_workflow_complete) while /status already
      // sees COMPLETED — which would cause the frontend to skip reconnect and
      // miss the latest turn's events entirely.
      const status = await getWorkflowStatus(threadId).catch((statusErr) => {
        console.log('[Reconnect] Could not check workflow status:', statusErr.message);
        return { can_reconnect: false };
      });

      if (cancelled) return;

      // Capture share status from workflow status response
      if (status.is_shared !== undefined) {
        setIsShared(status.is_shared);
      }

      await loadConversationHistory();

      if (cancelled) return;

      if (historyHasUnresolvedInterruptRef.current && status.can_reconnect) {
        // Workflow is active → interrupt was answered, reconnect will deliver resolution
        console.log('[Reconnect] Unresolved interrupt from history, reconnecting to get resolution events');
        historyHasUnresolvedInterruptRef.current = false;
        await reconnectToStream({ activeTasks: status.active_tasks || [] });
        unresolvedHistoryInterruptRef.current = [];
      } else if (historyHasUnresolvedInterruptRef.current && !status.can_reconnect) {
        // Workflow genuinely paused → make interrupt(s) interactive
        const intInfos = unresolvedHistoryInterruptRef.current;
        if (intInfos.length > 0) {
          const intInfo = intInfos[0]; // Use first for setPendingInterrupt (single-slot state)
          console.log('[Reconnect] Workflow paused, making', intInfos.length, 'interrupt(s) interactive:', intInfos.map((p) => p.type));

          // Populate batching refs so answer/skip handlers can collect and batch-resume
          pendingInterruptIdsRef.current.clear();
          collectedHitlResponsesRef.current = {};
          for (const info of intInfos) {
            if (info.interruptId) {
              pendingInterruptIdsRef.current.add(info.interruptId);
            }
          }

          if (intInfo.type === 'ask_user_question') {
            setPendingInterrupt({
              type: 'ask_user_question',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              questionId: intInfo.questionId,
            });
          } else if (intInfo.type === 'create_workspace') {
            setPendingInterrupt({
              type: 'create_workspace',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              proposalId: intInfo.proposalId,
            });
          } else if (intInfo.type === 'start_question') {
            setPendingInterrupt({
              type: 'start_question',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              proposalId: intInfo.proposalId,
            });
          } else {
            // plan_approval
            setPendingInterrupt({
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              planApprovalId: intInfo.planApprovalId,
              planMode: true,
            });
          }
        }
        unresolvedHistoryInterruptRef.current = [];
        historyHasUnresolvedInterruptRef.current = false;
      } else if (status.can_reconnect) {
        console.log('[Reconnect] Workflow status:', status.status, 'can_reconnect:', status.can_reconnect, 'active_tasks:', status.active_tasks);
        await reconnectToStream({ activeTasks: status.active_tasks || [] });
      } else if (status.active_tasks && status.active_tasks.length > 0) {
        // Main workflow completed but subagent tasks still running.
        // Reopen per-task SSE streams so cards stay live after refresh.
        console.log('[Reconnect] Main workflow done, reopening per-task streams for active subagents:', status.active_tasks);
        const dummyAssistantId = `assistant-subagent-reconnect-${Date.now()}`;
        const refs = {
          contentOrderCounterRef,
          currentReasoningIdRef,
          currentToolCallIdRef,
          queuedAtOrderRef,
          updateTodoListCard,
          isNewConversation: false,
          subagentStateRefs: subagentStateRefsRef.current,
          updateSubagentCard: updateSubagentCard
            ? (agentId, data) => updateSubagentCard(agentId, { ...data, isReconnect: true })
            : (() => {}),
          isReconnect: true,
        };
        const processEvent = createStreamEventProcessor(dummyAssistantId, refs, getTaskIdFromEvent);
        // Pre-seed cards from history so per-task events don't create empty cards
        for (const taskId of status.active_tasks) {
          const agentId = `task:${taskId}`;
          const historyData = subagentHistoryRef.current?.[agentId];
          if (updateSubagentCard && historyData) {
            updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${taskId}`,
              taskId: agentId,
              description: historyData.description || '',
              prompt: historyData.prompt || historyData.description || '',
              type: historyData.type || 'general-purpose',
              status: 'active',
              isActive: true,
              isReconnect: true,
            });
          }
          openSubagentStream(threadId, taskId, processEvent);
        }
        setHasActiveSubagents(true);
      } else {
        // Workflow is not active — mark all subagent tasks as completed.
        // (Skipped when reconnecting because per-task SSE streams
        //  will deliver live events with the real status.)
        markAllSubagentTasksCompleted();
      }
    };

    loadAndMaybeReconnect();

    // Cleanup: Cancel loading if workspace or thread changes or component unmounts
    return () => {
      console.log('[History] Cleanup: canceling history load for workspace:', workspaceId, 'thread:', threadId);
      cancelled = true;
      historyLoadingRef.current = false;
      closeAllSubagentStreams();
      subagentStateRefsRef.current = {};
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId]);

  /**
   * Marks all subagentTasks in messages as 'completed'.
   * Called when the SSE stream ends or history finishes loading, because a finished
   * workflow implies all subagents are done. This is a safety net — artifact events
   * and per-task SSE streams should have already updated most of them, but the final
   * completion may not be persisted in sse_events or may have been missed.
   */
  const markAllSubagentTasksCompleted = () => {
    // Skip tasks with open per-task SSE streams (still active)
    const activeShortIds = new Set(subagentStreamsRef.current.keys());

    setMessages((prev) => {
      let anyChanged = false;
      const updated = prev.map((msg) => {
        if (!msg.subagentTasks || Object.keys(msg.subagentTasks).length === 0) return msg;
        let changed = false;
        const updatedTasks = { ...msg.subagentTasks };
        Object.keys(updatedTasks).forEach((toolCallId) => {
          const agentId = toolCallIdToTaskIdMapRef.current.get(toolCallId);
          // If the task still has an open per-task stream, skip it
          if (agentId) {
            const shortId = agentId.replace('task:', '');
            if (activeShortIds.has(shortId)) return;
          }

          if (updatedTasks[toolCallId].status !== 'completed') {
            updatedTasks[toolCallId] = { ...updatedTasks[toolCallId], status: 'completed' };
            changed = true;
          }
        });
        if (changed) anyChanged = true;
        return changed ? { ...msg, subagentTasks: updatedTasks } : msg;
      });
      return anyChanged ? updated : prev;
    });
  };

  /**
   * Open a dedicated per-task SSE stream for a subagent.
   * Events from the stream are routed through processEvent (same handler as the main stream).
   * Idempotent — skips if a stream is already open for this taskId.
   *
   * @param {string} tid - Thread ID
   * @param {string} shortTaskId - The 6-char task identifier (e.g., 'k7Xm2p')
   * @param {Function} processEvent - The event processor (from createStreamEventProcessor)
   */
  const openSubagentStream = (tid, shortTaskId, processEvent) => {
    if (subagentStreamsRef.current.has(shortTaskId)) return; // already open
    const controller = new AbortController();
    subagentStreamsRef.current.set(shortTaskId, controller);

    streamSubagentTaskEvents(tid, shortTaskId, processEvent, controller.signal)
      .catch((err) => {
        if (err.name !== 'AbortError') {
          console.error(`[SubagentStream:${shortTaskId}]`, err);
        }
      })
      .finally(() => {
        subagentStreamsRef.current.delete(shortTaskId);
        completedTaskIdsRef.current.add(shortTaskId);
        // Per-task stream close = task completion signal
        if (updateSubagentCard) {
          updateSubagentCard(`task:${shortTaskId}`, { status: 'completed', isActive: false });
        }
        // If this was the last open stream, clean up
        if (subagentStreamsRef.current.size === 0) {
          setHasActiveSubagents(false);
          if (inactivateAllSubagents) inactivateAllSubagents();
          markAllSubagentTasksCompleted();
        }
      });
  };

  /**
   * Abort all open per-task subagent streams.
   */
  const closeAllSubagentStreams = () => {
    for (const [, controller] of subagentStreamsRef.current) {
      controller.abort();
    }
    subagentStreamsRef.current.clear();
  };

  /**
   * Helper to get taskId from event.
   * Routes subagent events to the correct task based on agent ID mapping.
   * Defined at hook level so it can be shared between handleSendMessage and reconnectToStream.
   */
  const getTaskIdFromEvent = (event) => {
    // With task:{task_id} format, the task ID is embedded in the agent field.
    // e.g., agent = "task:pkyRHQ" → taskId = "task:pkyRHQ"
    // This is the agent_id used as key throughout the frontend.
    const agent = event?.agent;
    if (!agent || typeof agent !== 'string' || !agent.startsWith('task:')) {
      if (process.env.NODE_ENV === 'development') {
        console.warn('[Stream] Subagent event without task: agent field:', event);
      }
      return null;
    }
    return agent;
  };

  /**
   * Shared cleanup logic for all stream-end paths (send, reconnect, HITL resume).
   * Resets loading/streaming state, finalizes subagents, and auto-completes todos.
   */
  const cleanupAfterStreamEnd = (assistantMessageId) => {
    setIsLoading(false);
    setWorkspaceStarting(false);
    setIsCompacting(false);
    currentMessageRef.current = null;
    isStreamingRef.current = false;

    const hasOpenStreams = subagentStreamsRef.current.size > 0;
    if (!hasOpenStreams) {
      if (inactivateAllSubagents) inactivateAllSubagents();
      markAllSubagentTasksCompleted();
      closeAllSubagentStreams();
    }
    setHasActiveSubagents(hasOpenStreams);

    // Auto-complete pending todos
    if (completePendingTodos) completePendingTodos();
    setMessages((prev) => {
      const msg = prev.find((m) => m.id === assistantMessageId);
      if (!msg?.todoListProcesses || Object.keys(msg.todoListProcesses).length === 0) return prev;
      const entries = Object.entries(msg.todoListProcesses);
      const lastEntry = entries.reduce((a, b) => ((a[1].order || 0) >= (b[1].order || 0) ? a : b));
      const [lastKey, lastVal] = lastEntry;
      const hasIncomplete = lastVal.todos?.some((t) => t.status !== 'completed');
      if (!hasIncomplete) return prev;
      const completedTodos = lastVal.todos.map((t) => ({ ...t, status: 'completed' }));
      return prev.map((m) => m.id !== assistantMessageId ? m : {
        ...m,
        todoListProcesses: {
          ...m.todoListProcesses,
          [lastKey]: {
            ...lastVal,
            todos: completedTodos,
            completed: lastVal.total || completedTodos.length,
            in_progress: 0,
            pending: 0,
          },
        },
      });
    });
  };

  /**
   * Creates a stream event processor that handles SSE events from the backend.
   * Used by both handleSendMessage (live) and reconnectToStream (reconnection).
   *
   * @param {string} assistantMessageId - The assistant message ID to update
   * @param {Object} refs - Refs for event handlers (contentOrderCounterRef, etc.)
   * @param {Function} getTaskIdFromEvent - Helper to route subagent events
   * @returns {Function} Event handler: (event) => void
   */
  const createStreamEventProcessor = (assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef = null) => {
    // Snapshot of the old assistant message's content order at the time the user
    // sent a queued message.  Used to roll back any content that leaked into the
    // old bubble due to stream-mode multiplexing (custom events can arrive after
    // message chunks from the post-injection model call).
    let queuedAtOrder = null;

    // FIFO queue for matching Task tool call IDs to artifact 'spawned' events.
    // Populated by the tool_calls handler, drained by the artifact/spawned handler.
    // This ensures toolCallIdToTaskIdMapRef is populated before tool_call_result.
    const pendingTaskToolCallIds = [];

    const processEvent = (event) => {
      const eventType = event.event || 'message_chunk';

      // Track last event ID for reconnection
      if (event._eventId != null) {
        lastEventIdRef.current = event._eventId;
      }

      // Debug: Log all events to see what we're receiving
      if (event.artifact_type || eventType === 'artifact') {
        console.log('[Stream] Artifact event detected:', { eventType, event, artifact_type: event.artifact_type });
      }

      // Update thread_id if provided in the event (ref = synchronous for closures)
      if (event.thread_id && event.thread_id !== '__default__') {
        threadIdRef.current = event.thread_id;
        if (event.thread_id !== threadId) {
          setThreadId(event.thread_id);
          setStoredThreadId(workspaceId, event.thread_id);
        }
      }

      // Handle workspace_status events (workspace starting/ready)
      if (eventType === 'workspace_status') {
        setWorkspaceStarting(event.status === 'starting');
        return;
      }

      // Check if this is a subagent event - filter it out from main chat view
      const isSubagent = isSubagentEvent(event);

      // Debug: Log subagent event detection
      if (process.env.NODE_ENV === 'development' && isSubagent) {
        console.log('[Stream] Subagent event detected:', {
          eventType,
          agent: event.agent,
          id: event.id,
          content_type: event.content_type,
        });
      }

      // Handle message_queued events for the MAIN agent (user sent a message while agent streams).
      // Subagent message_queued events are handled below in the isSubagent block.
      if (eventType === 'message_queued' && !isSubagent) {
        // Record the content order counter so we can roll back leaked content
        // when queued_message_injected arrives (see handler below).
        queuedAtOrder = refs.contentOrderCounterRef.current;
        if (refs.queuedAtOrderRef) refs.queuedAtOrderRef.current = refs.contentOrderCounterRef.current;
        return;
      }

      // Handle queued_message_injected custom events (middleware picked up the queued message)
      if (eventType === 'queued_message_injected') {
        const oldAssistantId = assistantMessageId;

        // 1. Roll back old assistant message to the snapshot taken at message_queued
        //    time, removing any content that leaked due to stream-mode multiplexing.
        //    Then finalize it (isStreaming: false).
        setMessages((prev) =>
          prev.map((msg) => {
            if (msg.id !== oldAssistantId) return msg;

            // Use closure-local snapshot or fall back to the shared ref
            // (message_queued only arrives on the secondary POST stream, so
            // the closure-local queuedAtOrder is typically null — the shared
            // ref is set by handleSendQueuedMessage on the secondary stream).
            const effectiveQueuedAtOrder = queuedAtOrder ?? refs.queuedAtOrderRef?.current ?? null;

            // If no snapshot, just finalize
            if (effectiveQueuedAtOrder === null) {
              return { ...msg, isStreaming: false };
            }

            // Keep only segments at or before the queue point
            const keptSegments = (msg.contentSegments || []).filter(
              (s) => s.order <= effectiveQueuedAtOrder
            );

            // Rebuild plain-text content from kept text segments
            const keptContent = keptSegments
              .filter((s) => s.type === 'text')
              .map((s) => s.content || '')
              .join('');

            // Collect IDs of kept processes so we can prune orphans
            const keptReasoningIds = new Set(
              keptSegments.filter((s) => s.type === 'reasoning').map((s) => s.reasoningId)
            );
            const keptToolCallIds = new Set(
              keptSegments.filter((s) => s.type === 'tool_call').map((s) => s.toolCallId)
            );
            const keptTodoListIds = new Set(
              keptSegments.filter((s) => s.type === 'todo_list').map((s) => s.todoListId)
            );
            const keptSubagentIds = new Set(
              keptSegments.filter((s) => s.type === 'subagent_task').map((s) => s.subagentId)
            );

            const filterObj = (obj, keepSet) => {
              if (!obj) return {};
              const out = {};
              for (const [id, val] of Object.entries(obj)) {
                if (keepSet.has(id)) out[id] = val;
              }
              return out;
            };

            return {
              ...msg,
              contentSegments: keptSegments,
              content: keptContent,
              reasoningProcesses: filterObj(msg.reasoningProcesses, keptReasoningIds),
              toolCallProcesses: filterObj(msg.toolCallProcesses, keptToolCallIds),
              todoListProcesses: filterObj(msg.todoListProcesses, keptTodoListIds),
              subagentTasks: filterObj(msg.subagentTasks, keptSubagentIds),
              isStreaming: false,
            };
          })
        );
        queuedAtOrder = null;
        if (refs.queuedAtOrderRef) refs.queuedAtOrderRef.current = null;

        // 2. Mark queued user messages as delivered, OR create them from event
        //    data if none exist (reconnect scenario — in-memory state was lost).
        setMessages((prev) => {
          const hasQueuedMessages = prev.some((msg) => msg.queued);
          if (hasQueuedMessages) {
            // Live path: mark existing queued messages as delivered
            return prev.map((msg) =>
              msg.queued ? { ...msg, queued: false, queueDelivered: true } : msg
            );
          }
          // Reconnect path: create user bubbles from event payload
          const queuedMsgs = (event.messages || []).filter((qMsg) => qMsg.content);
          if (queuedMsgs.length === 0) return prev;
          const newUserMessages = queuedMsgs.map((qMsg) => ({
            id: `queued-user-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            role: 'user',
            content: qMsg.content,
            contentType: 'text',
            timestamp: qMsg.timestamp ? new Date(qMsg.timestamp * 1000) : new Date(),
            isStreaming: false,
            queueDelivered: true,
          }));
          return [...prev, ...newUserMessages];
        });

        // 3. Create new assistant message placeholder
        const newAssistantId = `assistant-${Date.now()}`;
        const newAssistant = createAssistantMessage(newAssistantId);
        setMessages((prev) => appendMessage(prev, newAssistant));

        // 4. Switch closure & refs to new assistant message
        assistantMessageId = newAssistantId;
        currentMessageRef.current = newAssistantId;

        // 5. Reset content counters
        refs.contentOrderCounterRef.current = 0;
        refs.currentReasoningIdRef.current = null;
        refs.currentToolCallIdRef.current = null;
        return;
      }

      // Handle unified context_window events (token_usage, summarize, offload)
      if (eventType === 'context_window') {
        if (isSubagent) {
          // For subagent context_window events, embed notification as a content
          // segment inside the current assistant message (same as main chat) so
          // it appears at the correct chronological position.
          const taskId = getTaskIdFromEvent(event);
          if (taskId && event.action !== 'token_usage') {
            const action = event.action;
            let text;
            if (action === 'summarize' && event.signal === 'complete') {
              text = t('chat.summarizedNotification', { from: event.original_message_count });
            } else if (action === 'offload' && event.signal === 'complete') {
              const args = event.offloaded_args || 0;
              const reads = event.offloaded_reads || 0;
              if (args > 0 && reads > 0) text = t('chat.offloadedNotification', { args, reads });
              else if (reads > 0) text = t('chat.offloadedReadsNotification', { count: reads });
              else if (args > 0) text = t('chat.offloadedArgsNotification', { count: args });
            }
            if (text && updateSubagentCard) {
              const taskRefs = getOrCreateTaskRefs(refs, taskId);
              const order = ++taskRefs.contentOrderCounterRef.current;
              // Find the last assistant message and append the notification segment
              const updatedMessages = [...taskRefs.messages];
              const msgIdx = updatedMessages.findLastIndex(m => m.role === 'assistant');
              if (msgIdx !== -1) {
                const msg = { ...updatedMessages[msgIdx] };
                msg.contentSegments = [...(msg.contentSegments || []), { type: 'notification', content: text, order }];
                updatedMessages[msgIdx] = msg;
              }
              taskRefs.messages = updatedMessages;
              updateSubagentCard(taskId, { messages: updatedMessages });
            }
          }
          return;
        }
        handleContextWindowEvent(event, {
          getMsgId: () => currentMessageRef.current,
          nextOrder: () => {
            const eventId = event._eventId;
            return eventId != null ? eventId : ++refs.contentOrderCounterRef.current;
          },
          setMessages,
          setTokenUsage,
          setIsCompacting,
          insertNotification,
          t,
          offloadBatch: offloadBatchRef,
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
        if (updateSubagentCard) {

          // Use a stable message ID per task+run so all events from the same run
          // go into one message. Each resume bumps runIndex, creating a new message
          // so the card shows a unified conversation across resume boundaries.
          const taskRefs = getOrCreateTaskRefs(refs, taskId);
          const subagentAssistantMessageId = `subagent-${taskId}-assistant-${taskRefs.runIndex}`;

          if (eventType === 'message_chunk') {
            const contentType = event.content_type || 'text';
            handleSubagentMessageChunk({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              contentType,
              content: event.content,
              finishReason: event.finish_reason,
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'tool_call_chunks') {
            handleSubagentToolCallChunks({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              chunks: event.tool_call_chunks,
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'tool_calls') {
            handleSubagentToolCalls({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              toolCalls: event.tool_calls,
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'tool_call_result') {
            const toolCallId = event.tool_call_id;

            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] Subagent tool_call_result event:', {
                taskId,
                assistantMessageId: subagentAssistantMessageId,
                toolCallId,
                eventId: event.id,
                hasContent: !!event.content,
              });
            }

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
              updateSubagentCard,
            });
          } else if (eventType === 'artifact') {
            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] Filtering out subagent artifact event:', {
                artifactType: event.artifact_type,
                taskId,
                agent: event.agent,
              });
            }
          } else if (eventType === 'message_queued') {
            if (event.content) {
              handleTaskMessageQueued({
                taskId,
                content: event.content,
                refs,
                updateSubagentCard,
              });
            }
          }
        }
        return; // Don't process subagent events in main chat view
      }

      if (eventType === 'message_chunk') {
        const contentType = event.content_type || 'text';
        const eventId = event._eventId;

        // Handle reasoning_signal events
        if (contentType === 'reasoning_signal') {
          const signalContent = event.content || '';
          if (handleReasoningSignal({
            assistantMessageId,
            signalContent,
            refs,
            setMessages,
            eventId,
          })) {
            return;
          }
        }

        // Handle reasoning content chunks
        if (contentType === 'reasoning' && event.content) {
          if (handleReasoningContent({
            assistantMessageId,
            content: event.content,
            refs,
            setMessages,
          })) {
            return;
          }
        }

        // Handle text content chunks
        if (contentType === 'text') {
          if (handleTextContent({
            assistantMessageId,
            content: event.content,
            finishReason: event.finish_reason,
            refs,
            setMessages,
            eventId,
          })) {
            return;
          }
        }

        // Skip other content types
        return;
      } else if (eventType === 'error' || event.error) {
        const errorMessage = event.error || event.message || 'An error occurred while processing your request.';
        setMessageError(errorMessage);
        setMessages((prev) =>
          updateMessage(prev, assistantMessageId, (msg) => ({
            ...msg,
            content: msg.content || errorMessage,
            contentType: 'text',
            isStreaming: false,
            error: true,
          }))
        );
      } else if (eventType === 'tool_call_chunks') {
        handleToolCallChunks({
          assistantMessageId,
          chunks: event.tool_call_chunks,
          setMessages,
        });
        return;
      } else if (eventType === 'artifact') {
        const artifactType = event.artifact_type;
        console.log('[Stream] Received artifact event:', { artifactType, artifactId: event.artifact_id, payload: event.payload });
        if (artifactType === 'todo_update') {
          console.log('[Stream] Processing todo_update artifact for assistant message:', assistantMessageId);
          const result = handleTodoUpdate({
            assistantMessageId,
            artifactType,
            artifactId: event.artifact_id,
            payload: event.payload || {},
            refs,
            setMessages,
            eventId: event._eventId,
          });
          console.log('[Stream] handleTodoUpdate result:', result);
        } else if (artifactType === 'file_operation' && onFileArtifact) {
          onFileArtifact(event);
        } else if (artifactType === 'task') {
          const payload = event.payload || {};
          const { task_id, action: rawAction, description, prompt, type } = payload;
          const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'message_queued') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })();
          if (!task_id) return;
          const agentId = `task:${task_id}`;

          if (action === 'init') {
            // Drain pending Task tool call ID to establish toolCallId → agentId mapping
            // immediately, so clicking the inline card before tool_call_result resolves correctly
            if (pendingTaskToolCallIds.length > 0) {
              const toolCallId = pendingTaskToolCallIds.shift();
              toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
            }
            const alreadyCompleted = completedTaskIdsRef.current.has(task_id);
            if (updateSubagentCard) {
              updateSubagentCard(agentId, {
                agentId,
                displayId: `Task-${task_id}`,
                taskId: agentId,
                type: type || 'general-purpose',
                description: description || '',
                prompt: prompt || description || '',
                status: alreadyCompleted ? 'completed' : 'active',
                isActive: !alreadyCompleted,
              });
            }
            if (!alreadyCompleted) {
              const currentThreadId = event.thread_id || threadIdRef.current;
              openSubagentStream(currentThreadId, task_id, processEvent);
            }
          } else if (action === 'resume') {
            // Resume: preserve existing messages, inject user boundary, bump runIndex
            const taskRefsForResume = getOrCreateTaskRefs(refs, agentId);

            // Finalize the last assistant message from the previous run
            const updatedMessages = [...taskRefsForResume.messages];
            for (let i = updatedMessages.length - 1; i >= 0; i--) {
              if (updatedMessages[i].role === 'assistant' && updatedMessages[i].isStreaming) {
                updatedMessages[i] = { ...updatedMessages[i], isStreaming: false };
                break;
              }
            }

            // Inject user message with resume instruction
            updatedMessages.push({
              id: `resume-${agentId}-${Date.now()}`,
              role: 'user',
              content: prompt || description || 'Resume',
              contentSegments: [{ type: 'text', content: prompt || description || 'Resume', order: 0 }],
              reasoningProcesses: {},
              toolCallProcesses: {},
            });

            // Bump runIndex and reset per-run counters
            taskRefsForResume.runIndex = (taskRefsForResume.runIndex || 0) + 1;
            taskRefsForResume.contentOrderCounterRef.current = 0;
            taskRefsForResume.currentReasoningIdRef.current = null;
            taskRefsForResume.currentToolCallIdRef.current = null;
            taskRefsForResume.messages = updatedMessages;

            if (updateSubagentCard) {
              // Prefer preserving the original spawn description (already on the card).
              // But after reconnect the card may have been wiped + recreated without a
              // description, so fall back to subagentHistoryRef as a safety net.
              const historyDesc = subagentHistoryRef.current?.[agentId]?.description;
              const historyPrompt = subagentHistoryRef.current?.[agentId]?.prompt;
              updateSubagentCard(agentId, {
                agentId,
                displayId: `Task-${task_id}`,
                taskId: agentId,
                type: type || 'general-purpose',
                status: 'active',
                isActive: true,
                messages: updatedMessages,
                ...(historyDesc ? { description: historyDesc } : {}),
                ...(historyPrompt ? { prompt: historyPrompt } : {}),
              });
            }

            // Abort existing stream before opening new one (race condition safety)
            const existingController = subagentStreamsRef.current.get(task_id);
            if (existingController) {
              existingController.abort();
              subagentStreamsRef.current.delete(task_id);
            }

            const currentThreadId = event.thread_id || threadIdRef.current;
            openSubagentStream(currentThreadId, task_id, processEvent);
          } else if (action === 'update') {
            if (updateSubagentCard) {
              updateSubagentCard(agentId, { queuedMessage: prompt || payload.description });
            }
            // Update inline card to show "Updated" instead of "Resumed"
            setMessages(prev => prev.map(msg => {
              if (!msg.subagentTasks) return msg;
              let changed = false;
              const newTasks = { ...msg.subagentTasks };
              for (const [tcId, task] of Object.entries(newTasks)) {
                if (task.resumeTargetId === agentId && task.action === 'resume') {
                  newTasks[tcId] = { ...task, action: 'update' };
                  changed = true;
                }
              }
              return changed ? { ...msg, subagentTasks: newTasks } : msg;
            }));
          }
        }
        return;
      } else if (eventType === 'tool_calls') {
        handleToolCalls({
          assistantMessageId,
          toolCalls: event.tool_calls,
          finishReason: event.finish_reason,
          refs,
          setMessages,
          eventId: event._eventId,
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
        // Check if this resolves an unresolved interrupt from history replay (FIFO array matching)
        const unresolvedList = refs.unresolvedHistoryInterruptRef?.current;
        if (unresolvedList?.length > 0 && typeof event.content === 'string') {
          const content = event.content;

          // Try create_workspace / start_question
          const matchIdx = unresolvedList.findIndex((u) => u.type === 'create_workspace' || u.type === 'start_question');
          if (matchIdx !== -1) {
            const matched = unresolvedList[matchIdx];
            const dataKey = matched.type === 'create_workspace' ? 'workspaceProposals' : 'questionProposals';
            let resolvedStatus = 'approved';
            if (content === 'User declined workspace creation.' || content === 'User declined starting the question.') {
              resolvedStatus = 'rejected';
            } else {
              try { if (JSON.parse(content)?.success === false) resolvedStatus = 'rejected'; } catch { /* not JSON */ }
            }
            setMessages((prev) =>
              updateMessage(prev, matched.assistantMessageId, (msg) => ({
                ...msg,
                [dataKey]: {
                  ...(msg[dataKey] || {}),
                  [matched.proposalId]: {
                    ...(msg[dataKey]?.[matched.proposalId] || {}),
                    status: resolvedStatus,
                  },
                },
              }))
            );
            unresolvedList.splice(matchIdx, 1);
          }
        }

        const toolCallId = event.tool_call_id;

        // Build toolCallId → agentId mapping from Task tool artifact
        if (event.artifact?.task_id && toolCallId) {
          const agentId = `task:${event.artifact.task_id}`;
          toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
          if (process.env.NODE_ENV === 'development') {
            console.log('[Stream] Mapped toolCallId to agentId from artifact:', {
              toolCallId,
              agentId,
              description: event.artifact.description,
            });
          }
        }

        handleToolCallResult({
          assistantMessageId,
          toolCallId: event.tool_call_id,
          result: {
            content: event.content,
            content_type: event.content_type,
            tool_call_id: event.tool_call_id,
            artifact: event.artifact,
          },
          refs,
          setMessages,
        });

        // When onboarding-related tools succeed, sync onboarding_completed via PUT
        if (onOnboardingRelatedToolComplete && isOnboardingRelatedToolSuccess(event.content)) {
          onOnboardingRelatedToolComplete();
        }

        // Detect navigate_to_workspace action from start_question tool result
        if (onWorkspaceCreated && typeof event.content === 'string') {
          try {
            const parsed = JSON.parse(event.content);
            if (parsed?.success && parsed?.action === 'navigate_to_workspace') {
              onWorkspaceCreated({ workspaceId: parsed.workspace_id, question: parsed.question });
            }
          } catch { /* not JSON, ignore */ }
        }
      } else if (eventType === 'interrupt') {
        const actionType = event.action_requests?.[0]?.type;

        if (actionType === 'ask_user_question') {
          // --- User question interrupt ---
          const questionId = event.interrupt_id || `question-${Date.now()}`;
          const questionData = event.action_requests[0];
          const order = event._eventId != null ? event._eventId : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev, assistantMessageId, (msg) => ({
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'user_question', questionId, order },
              ],
              userQuestions: {
                ...(msg.userQuestions || {}),
                [questionId]: {
                  question: questionData.question,
                  options: questionData.options || [],
                  allow_multiple: questionData.allow_multiple || false,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                  answer: null,
                },
              },
              isStreaming: false,
            }))
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id);
          setPendingInterrupt({
            type: 'ask_user_question',
            interruptId: event.interrupt_id,
            assistantMessageId,
            questionId,
          });
        } else if (actionType === 'create_workspace') {
          // --- Create workspace interrupt ---
          const proposalId = event.interrupt_id || `workspace-${Date.now()}`;
          const proposalData = event.action_requests[0];
          const order = event._eventId != null ? event._eventId : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev, assistantMessageId, (msg) => ({
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'create_workspace', proposalId, order },
              ],
              workspaceProposals: {
                ...(msg.workspaceProposals || {}),
                [proposalId]: {
                  workspace_name: proposalData.workspace_name,
                  workspace_description: proposalData.workspace_description,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }))
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id);
          setPendingInterrupt({
            type: 'create_workspace',
            interruptId: event.interrupt_id,
            assistantMessageId,
            proposalId,
          });
        } else if (actionType === 'start_question') {
          // --- Start question interrupt ---
          const proposalId = event.interrupt_id || `question-start-${Date.now()}`;
          const proposalData = event.action_requests[0];
          const order = event._eventId != null ? event._eventId : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev, assistantMessageId, (msg) => ({
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'start_question', proposalId, order },
              ],
              questionProposals: {
                ...(msg.questionProposals || {}),
                [proposalId]: {
                  workspace_id: proposalData.workspace_id,
                  question: proposalData.question,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }))
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id);
          setPendingInterrupt({
            type: 'start_question',
            interruptId: event.interrupt_id,
            assistantMessageId,
            proposalId,
          });
        } else {
          // --- Plan approval interrupt (existing) ---
          const planApprovalId = event.interrupt_id || `plan-${Date.now()}`;
          const description =
            event.action_requests?.[0]?.description ||
            event.action_requests?.[0]?.args?.plan ||
            'No plan description provided.';

          const order = event._eventId != null ? event._eventId : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev, assistantMessageId, (msg) => ({
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'plan_approval', planApprovalId, order },
              ],
              planApprovals: {
                ...(msg.planApprovals || {}),
                [planApprovalId]: {
                  description,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }))
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id);
          setPendingInterrupt({
            interruptId: event.interrupt_id,
            actionRequests: event.action_requests || [],
            threadId: event.thread_id,
            assistantMessageId,
            planApprovalId,
            planMode: event.action_requests?.some(r => r.name === 'SubmitPlan') || currentPlanModeRef.current,
          });
        }

        setIsLoading(false);
        isStreamingRef.current = false;
        currentMessageRef.current = null;
        if (wasInterruptedRef) wasInterruptedRef.current = true;
      }
    };

    return processEvent;
  };

  /**
   * Handles sending a message while the agent is already streaming.
   * The backend will queue it for injection before the next LLM call.
   */
  const handleSendQueuedMessage = async (message, planMode = false, additionalContext = null, attachmentMeta = null) => {
    // Show user message in chat with queued indicator
    const userMessage = createUserMessage(message, attachmentMeta);
    userMessage.queued = true;
    recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);
    setMessages((prev) => appendMessage(prev, userMessage));

    try {
      // Send to same endpoint — backend will auto-queue and return message_queued SSE
      await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        [],
        planMode,
        (event) => {
          const eventType = event.event || 'message_chunk';
          if (eventType === 'message_queued') {
            // Snapshot the content order counter so the primary stream's
            // queued_message_injected handler can roll back leaked content.
            queuedAtOrderRef.current = contentOrderCounterRef.current;
            // Update the user message to reflect queued status
            setMessages((prev) =>
              updateMessage(prev, userMessage.id, (msg) => ({
                ...msg,
                queued: true,
                queuePosition: event.position,
              }))
            );
          }
        },
        additionalContext,
        agentMode
      );
    } catch (err) {
      console.error('Error queuing message:', err);
      // Update user message to show queue failure
      setMessages((prev) =>
        updateMessage(prev, userMessage.id, (msg) => ({
          ...msg,
          queued: false,
          queueError: err.message || 'Failed to queue message',
        }))
      );
    }
  };

  /**
   * Handles sending a message and streaming the response
   *
   * @param {string} message - The user's message
   * @param {boolean} planMode - Whether to use plan mode
   * @param {Array|null} additionalContext - Optional additional context for skill loading
   * @param {Array|null} attachmentMeta - Optional attachment metadata for user message display
   */
  const handleSendMessage = async (message, planMode = false, additionalContext = null, attachmentMeta = null) => {
    const hasContent = message.trim() || (additionalContext && additionalContext.length > 0);
    if (!workspaceId || !hasContent) {
      return;
    }

    // If agent is already streaming, send as queued message
    if (isLoading) {
      return handleSendQueuedMessage(message, planMode, additionalContext, attachmentMeta);
    }

    // Store planMode so HITL interrupt handler can access it
    currentPlanModeRef.current = planMode;

    // Intercept: if a plan was rejected, route this message as rejection feedback
    if (pendingRejection) {
      const { interruptId, planMode: rejectionPlanMode } = pendingRejection;
      setPendingRejection(null);

      // Show user message in chat
      const userMsg = createUserMessage(message);
      recentlySentTrackerRef.current.track(message.trim(), userMsg.timestamp, userMsg.id);
      setMessages((prev) => appendMessage(prev, userMsg));

      // Send as rejection feedback via hitl_response
      const hitlResponse = {
        [interruptId]: {
          decisions: [{ type: 'reject', message: message.trim() }],
        },
      };
      return resumeWithHitlResponse(hitlResponse, rejectionPlanMode);
    }

    // Create and add user message
    const userMessage = createUserMessage(message, attachmentMeta);
    recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);

    // Check if this is a new conversation
    // Only consider it a new conversation if:
    // 1. There are no messages at all, OR
    // 2. We're starting a new thread (threadId is '__default__')
    // This determines if we should overwrite the existing todo list card
    // Note: We don't consider it a new conversation just because all messages are from history
    // - the user might continue the conversation, and we want to keep the todo list card
    const isNewConversation = messages.length === 0 || threadId === '__default__';
    isNewConversationRef.current = isNewConversation;

    // Add user message after history messages
    setMessages((prev) => {
      const newMessages = appendMessage(prev, userMessage);
      // Update new messages start index if this is the first new message
      if (newMessagesStartIndexRef.current === prev.length) {
        newMessagesStartIndexRef.current = newMessages.length;
      }
      return newMessages;
    });

    setIsLoading(true);
    setMessageError(null);
    setHasActiveSubagents(false);
    completedTaskIdsRef.current.clear();
    // Mark streaming as in progress to prevent history loading during streaming
    isStreamingRef.current = true;

    // Create assistant message placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    // Reset counters for this new message
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

    const assistantMessage = createAssistantMessage(assistantMessageId);

    // Add assistant message after history messages
    setMessages((prev) => {
      const newMessages = appendMessage(prev, assistantMessage);
      // Update new messages start index
      newMessagesStartIndexRef.current = newMessages.length;
      return newMessages;
    });
    currentMessageRef.current = assistantMessageId;

    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      // Prepare refs for event handlers — use persistent subagent state
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        queuedAtOrderRef,
        updateTodoListCard,
        isNewConversation: isNewConversationRef.current,
        subagentStateRefs: subagentStateRefsRef.current,
        updateSubagentCard: updateSubagentCard || (() => {}),
      };

      // Create the event processor using the shared factory
      const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

      const result = await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        [],
        planMode,
        processEvent,
        additionalContext,
        agentMode
      );

      if (result?.disconnected) {
        console.log('[Send] Stream disconnected, attempting reconnect');
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      // Mark message as complete (use live ref in case queued_message_injected switched it)
      {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev, finalId, (msg) => ({
            ...msg,
            isStreaming: false,
          }))
        );
      }
    } catch (err) {
          // Handle rate limit (429) — show limit message and remove optimistic assistant message
          if (err.status === 429) {
            const info = err.rateLimitInfo || {};
            const limitMsg = info.type === 'credit_limit'
              ? `Daily credit limit reached (${info.used_credits}/${info.credit_limit} credits). Resets at midnight UTC.`
              : info.type === 'workspace_limit'
                ? `Active workspace limit reached (${info.current}/${info.limit}).`
                : info.message || 'Rate limit exceeded. Please try again later.';
            setMessageError(limitMsg);
            setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
          } else {
            console.error('Error sending message:', err);
            setMessageError(err.message || 'Failed to send message');
            setMessages((prev) =>
              updateMessage(prev, assistantMessageId, (msg) => ({
                ...msg,
                content: msg.content || 'Failed to send message. Please try again.',
                isStreaming: false,
                error: true,
              }))
            );
          }
        } finally {
          if (!wasDisconnected && !wasInterruptedRef.current) {
            // Mark message as complete (use live ref in case queued_message_injected switched it)
            const finalId = currentMessageRef.current || assistantMessageId;
            setMessages((prev) =>
              updateMessage(prev, finalId, (msg) => ({
                ...msg,
                isStreaming: false,
              }))
            );

            cleanupAfterStreamEnd(finalId);
          }
        }
      };

  /**
   * Resumes an interrupted workflow with an HITL response (approve or reject).
   * Follows the same pattern as handleSendMessage but sends messages: [] with hitl_response.
   */
  const resumeWithHitlResponse = useCallback(async (hitlResponse, planMode = false) => {
    setPendingInterrupt(null);
    pendingInterruptIdsRef.current.clear();
    collectedHitlResponsesRef.current = {};

    // Create assistant message placeholder
    const assistantMessageId = `assistant-hitl-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

    const assistantMessage = createAssistantMessage(assistantMessageId);
    setMessages((prev) => appendMessage(prev, assistantMessage));
    currentMessageRef.current = assistantMessageId;

    setIsLoading(true);
    setMessageError(null);
    isStreamingRef.current = true;

    // Prepare refs for event handlers — use persistent subagent state
    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      queuedAtOrderRef,
      updateTodoListCard,
      isNewConversation: false,
      subagentStateRefs: subagentStateRefsRef.current,
      updateSubagentCard: updateSubagentCard || (() => {}),
    };

    const wasInterruptedRef = { current: false };
    const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

    let wasDisconnected = false;
    try {
      const result = await sendHitlResponse(
        workspaceId,
        threadId,
        hitlResponse,
        processEvent,
        planMode
      );

      if (result?.disconnected) {
        console.log('[HITL] Stream disconnected, attempting reconnect');
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      // Mark message as complete (use live ref in case queued_message_injected switched it)
      {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev, finalId, (msg) => ({
            ...msg,
            isStreaming: false,
          }))
        );
      }
    } catch (err) {
      console.error('[HITL] Error resuming workflow:', err);
      setMessageError(err.message || 'Failed to resume workflow');
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) => ({
          ...msg,
          content: msg.content || 'Failed to resume workflow. Please try again.',
          isStreaming: false,
          error: true,
        }))
      );
    } finally {
      if (!wasDisconnected && !wasInterruptedRef.current) {
        const finalId = currentMessageRef.current || assistantMessageId;
        cleanupAfterStreamEnd(finalId);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, updateTodoListCard, updateSubagentCard, inactivateAllSubagents, completePendingTodos]);

  const handleApproveInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, assistantMessageId, planApprovalId, planMode } = pendingInterrupt;

    // Update plan card status to "approved"
    setMessages((prev) =>
      updateMessage(prev, assistantMessageId, (msg) => ({
        ...msg,
        planApprovals: {
          ...(msg.planApprovals || {}),
          [planApprovalId]: {
            ...(msg.planApprovals?.[planApprovalId] || {}),
            status: 'approved',
          },
        },
      }))
    );

    const hitlResponse = {
      [interruptId]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, planMode);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, assistantMessageId, planApprovalId, planMode } = pendingInterrupt;

    // Update plan card status to "rejected"
    setMessages((prev) =>
      updateMessage(prev, assistantMessageId, (msg) => ({
        ...msg,
        planApprovals: {
          ...(msg.planApprovals || {}),
          [planApprovalId]: {
            ...(msg.planApprovals?.[planApprovalId] || {}),
            status: 'rejected',
          },
        },
      }))
    );

    // Store interruptId + planMode so next handleSendMessage routes as rejection feedback
    setPendingRejection({ interruptId, planMode });
    setPendingInterrupt(null);
  }, [pendingInterrupt]);

  const handleAnswerQuestion = useCallback((answer, questionId, interruptId) => {
    if (!questionId || !interruptId) return;

    // Optimistically mark the card as answered
    setMessages((prev) =>
      prev.map((msg) => {
        if (!msg.userQuestions?.[questionId]) return msg;
        return {
          ...msg,
          userQuestions: {
            ...msg.userQuestions,
            [questionId]: {
              ...msg.userQuestions[questionId],
              status: 'answered',
              answer,
            },
          },
        };
      })
    );

    // Collect this response for batching (parallel interrupts need all responses at once)
    collectedHitlResponsesRef.current[interruptId] = { decisions: [{ type: 'approve', message: answer }] };

    // Check if all pending interrupts have been responded to
    const pending = pendingInterruptIdsRef.current;
    const collected = collectedHitlResponsesRef.current;
    if (pending.size > 0 && [...pending].every((id) => collected[id])) {
      const batchedResponse = { ...collected };
      resumeWithHitlResponse(batchedResponse, false);
    }
  }, [resumeWithHitlResponse]);

  const handleSkipQuestion = useCallback((questionId, interruptId) => {
    if (!questionId || !interruptId) return;

    // Mark the card as skipped
    setMessages((prev) =>
      prev.map((msg) => {
        if (!msg.userQuestions?.[questionId]) return msg;
        return {
          ...msg,
          userQuestions: {
            ...msg.userQuestions,
            [questionId]: {
              ...msg.userQuestions[questionId],
              status: 'skipped',
            },
          },
        };
      })
    );

    // Collect this response for batching (parallel interrupts need all responses at once)
    collectedHitlResponsesRef.current[interruptId] = { decisions: [{ type: 'reject' }] };

    // Check if all pending interrupts have been responded to
    const pending = pendingInterruptIdsRef.current;
    const collected = collectedHitlResponsesRef.current;
    if (pending.size > 0 && [...pending].every((id) => collected[id])) {
      const batchedResponse = { ...collected };
      resumeWithHitlResponse(batchedResponse, false);
    }
  }, [resumeWithHitlResponse]);

  const handleApproveCreateWorkspace = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'create_workspace') return;
    const { interruptId, proposalId } = pendingInterrupt;

    setMessages((prev) =>
      prev.map((msg) => {
        if (!msg.workspaceProposals?.[proposalId]) return msg;
        return {
          ...msg,
          workspaceProposals: {
            ...msg.workspaceProposals,
            [proposalId]: {
              ...msg.workspaceProposals[proposalId],
              status: 'approved',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectCreateWorkspace = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'create_workspace') return;
    const { interruptId, proposalId } = pendingInterrupt;

    setMessages((prev) =>
      prev.map((msg) => {
        if (!msg.workspaceProposals?.[proposalId]) return msg;
        return {
          ...msg,
          workspaceProposals: {
            ...msg.workspaceProposals,
            [proposalId]: {
              ...msg.workspaceProposals[proposalId],
              status: 'rejected',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId]: { decisions: [{ type: 'reject' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleApproveStartQuestion = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'start_question') return;
    const { interruptId, proposalId } = pendingInterrupt;

    setMessages((prev) =>
      prev.map((msg) => {
        if (!msg.questionProposals?.[proposalId]) return msg;
        return {
          ...msg,
          questionProposals: {
            ...msg.questionProposals,
            [proposalId]: {
              ...msg.questionProposals[proposalId],
              status: 'approved',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectStartQuestion = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'start_question') return;
    const { interruptId, proposalId } = pendingInterrupt;

    setMessages((prev) =>
      prev.map((msg) => {
        if (!msg.questionProposals?.[proposalId]) return msg;
        return {
          ...msg,
          questionProposals: {
            ...msg.questionProposals,
            [proposalId]: {
              ...msg.questionProposals[proposalId],
              status: 'rejected',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId]: { decisions: [{ type: 'reject' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const insertNotification = useCallback((text, variant = 'info') => {
    setMessages((prev) => appendMessage(prev, createNotificationMessage(text, variant)));
  }, []);

  // =====================================================================
  // Edit / Regenerate / Retry handlers
  // =====================================================================

  /** Lazy-cached turn checkpoint data. Invalidated after each edit/regenerate. */
  const turnCheckpointsRef = useRef(null);

  /**
   * Helper: get or fetch turn checkpoints for the current thread.
   * Caches the result in turnCheckpointsRef until invalidated.
   */
  const getTurnCheckpoints = useCallback(async () => {
    if (turnCheckpointsRef.current) return turnCheckpointsRef.current;
    if (!threadId || threadId === '__default__') return null;
    try {
      const data = await fetchThreadTurns(threadId);
      turnCheckpointsRef.current = data;
      return data;
    } catch (err) {
      console.error('[useChatMessages] Failed to fetch turn checkpoints:', err);
      return null;
    }
  }, [threadId]);

  /**
   * Helper: run a checkpoint-based stream (shared by edit, regenerate, retry).
   * Sets up assistant placeholder, event processor, and handles the stream lifecycle.
   */
  const streamFromCheckpoint = useCallback(async (message, checkpointId, truncateIndex, forkFromTurn = null) => {
    if (isLoading) return;

    setIsLoading(true);
    setMessageError(null);
    setHasActiveSubagents(false);
    completedTaskIdsRef.current.clear();
    isStreamingRef.current = true;

    // Truncate messages and add new user message (if editing) + assistant placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

    const assistantMessage = createAssistantMessage(assistantMessageId);
    const userMessage = message ? createUserMessage(message) : null;

    if (userMessage) {
      recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);
    }

    setMessages((prev) => {
      const truncated = prev.slice(0, truncateIndex);
      const newMsgs = userMessage
        ? [...truncated, userMessage, assistantMessage]
        : [...truncated, assistantMessage];
      newMessagesStartIndexRef.current = newMsgs.length;
      return newMsgs;
    });
    currentMessageRef.current = assistantMessageId;

    // Invalidate turn checkpoints cache (branch creates new checkpoints)
    turnCheckpointsRef.current = null;

    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        queuedAtOrderRef,
        updateTodoListCard,
        isNewConversation: false,
        subagentStateRefs: subagentStateRefsRef.current,
        updateSubagentCard: updateSubagentCard || (() => {}),
      };
      const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

      const result = await sendChatMessageStream(
        message || '',
        workspaceId,
        threadId,
        [],
        false,
        processEvent,
        null,
        agentMode,
        undefined, // locale
        undefined, // timezone
        checkpointId,
        forkFromTurn
      );

      if (result?.disconnected) {
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      const finalId = currentMessageRef.current || assistantMessageId;
      setMessages((prev) =>
        updateMessage(prev, finalId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );
    } catch (err) {
      console.error('[streamFromCheckpoint] Error:', err);
      setMessageError(err.message || 'Failed to process request');
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) => ({
          ...msg,
          content: msg.content || 'Failed to process request. Please try again.',
          isStreaming: false,
          error: true,
        }))
      );
    } finally {
      if (!wasDisconnected && !wasInterruptedRef.current) {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev, finalId, (msg) => ({
            ...msg,
            isStreaming: false,
          }))
        );
        cleanupAfterStreamEnd(finalId);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoading, workspaceId, threadId, agentMode]);

  /**
   * Edit a user message: truncate to before that message, send modified content
   * from the checkpoint before the original message was added.
   */
  const handleEditMessage = useCallback(async (messageId, newContent) => {
    if (!newContent?.trim()) return;

    const msgIndex = messages.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // Count user messages up to and including this one to get turn_index
    const userMsgsBefore = messages.slice(0, msgIndex + 1).filter((m) => m.role === 'user');
    const turnIndex = userMsgsBefore.length - 1;

    const turnsData = await getTurnCheckpoints();
    if (!turnsData?.turns?.[turnIndex]) {
      setMessageError('Unable to edit: checkpoint data unavailable');
      return;
    }

    const checkpointId = turnsData.turns[turnIndex].edit_checkpoint_id;
    if (!checkpointId) {
      setMessageError('Unable to edit: this is the first message');
      return;
    }

    await streamFromCheckpoint(newContent, checkpointId, msgIndex, turnIndex);
  }, [messages, getTurnCheckpoints, streamFromCheckpoint]);

  /**
   * Regenerate an assistant response: truncate the assistant message,
   * re-run from the checkpoint that has the user message but before AI response.
   */
  const handleRegenerate = useCallback(async (messageId) => {
    const msgIndex = messages.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // Count user messages before this assistant message to get turn_index
    const userMsgsBefore = messages.slice(0, msgIndex).filter((m) => m.role === 'user');
    const turnIndex = userMsgsBefore.length - 1;

    const turnsData = await getTurnCheckpoints();
    if (!turnsData?.turns?.[turnIndex]) {
      setMessageError('Unable to regenerate: checkpoint data unavailable');
      return;
    }

    const checkpointId = turnsData.turns[turnIndex].regenerate_checkpoint_id;
    // Truncate at the assistant message (keep everything before it, including user msg)
    await streamFromCheckpoint(null, checkpointId, msgIndex, turnIndex);
  }, [messages, getTurnCheckpoints, streamFromCheckpoint]);

  /**
   * Retry the last failed/errored turn from the latest checkpoint.
   */
  const handleRetry = useCallback(async () => {
    const turnsData = await getTurnCheckpoints();
    const checkpointId = turnsData?.retry_checkpoint_id;
    if (!checkpointId) {
      setMessageError('Unable to retry: no checkpoint available');
      return;
    }

    if (!turnsData.turns?.length) {
      setMessageError('Unable to retry: checkpoint data unavailable');
      return;
    }

    // Find the last error message and truncate from there
    const lastErrorIndex = messages.findLastIndex((m) => m.error);
    const truncateIndex = lastErrorIndex !== -1 ? lastErrorIndex : messages.length;

    // Retry overwrites the last turn
    const forkFromTurn = turnsData.turns.length - 1;
    await streamFromCheckpoint(null, checkpointId, truncateIndex, forkFromTurn);
  }, [messages, getTurnCheckpoints, streamFromCheckpoint]);

  return {
    messages,
    threadId,
    isLoading,
    hasActiveSubagents,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    isLoadingHistory,
    isReconnecting,
    messageError,
    handleSendMessage,
    pendingInterrupt,
    pendingRejection,
    handleApproveInterrupt,
    handleRejectInterrupt,
    handleAnswerQuestion,
    handleSkipQuestion,
    handleApproveCreateWorkspace,
    handleRejectCreateWorkspace,
    handleApproveStartQuestion,
    handleRejectStartQuestion,
    tokenUsage,
    isShared,
    insertNotification,
    handleEditMessage,
    handleRegenerate,
    handleRetry,
    // Resolve subagentId (e.g. toolCallId from segment) to stable agent_id for card operations.
    resolveSubagentIdToAgentId: (subagentId) =>
      toolCallIdToTaskIdMapRef.current.get(subagentId) || subagentId,
    // Expose subagent history for lazy loading. Resolves toolCallId -> agent_id via mapping.
    // Returns { ...historyData, agentId } so caller can use agentId for card operations.
    getSubagentHistory: (subagentId) => {
      const agentId = toolCallIdToTaskIdMapRef.current.get(subagentId) || subagentId;
      const data = subagentHistoryRef.current?.[agentId];
      return data ? { ...data, agentId } : null;
    },
  };
}
