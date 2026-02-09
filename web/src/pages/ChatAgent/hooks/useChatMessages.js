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
import { getAuthUserId } from '@/api/client';
import { sendChatMessageStream, replayThreadHistory, getWorkflowStatus, reconnectToWorkflowStream, sendHitlResponse, DEFAULT_USER_ID } from '../utils/api';
import { getStoredThreadId, setStoredThreadId } from './utils/threadStorage';
export { removeStoredThreadId } from './utils/threadStorage';
import { createUserMessage, createAssistantMessage, insertMessage, appendMessage, updateMessage } from './utils/messageHelpers';
import { createRecentlySentTracker } from './utils/recentlySentTracker';
import {
  handleReasoningSignal,
  handleReasoningContent,
  handleTextContent,
  handleToolCalls,
  handleToolCallResult,
  handleTodoUpdate,
  handleSubagentStatus,
  isSubagentEvent,
  handleSubagentMessageChunk,
  handleSubagentToolCalls,
  handleSubagentToolCallResult,
} from './utils/streamEventHandlers';
import {
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryToolCallResult,
  handleHistoryTodoUpdate,
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

export function useChatMessages(workspaceId, initialThreadId = null, updateTodoListCard = null, updateSubagentCard = null, inactivateAllSubagents = null, minimizeInactiveSubagents = null, completePendingTodos = null, onOnboardingRelatedToolComplete = null, onFileArtifact = null, agentMode = 'ptc') {
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
  const [messageError, setMessageError] = useState(null);
  // HITL (Human-in-the-Loop) plan mode interrupt state
  const [pendingInterrupt, setPendingInterrupt] = useState(null);
  // When user clicks Reject on a plan, this stores the interruptId so the next message
  // sent via handleSendMessage is routed as rejection feedback via hitl_response.
  const [pendingRejection, setPendingRejection] = useState(null);

  // Refs for streaming state
  const currentMessageRef = useRef(null);
  const contentOrderCounterRef = useRef(0);
  const currentReasoningIdRef = useRef(null);
  const currentToolCallIdRef = useRef(null);

  // Refs for history loading state
  const historyLoadingRef = useRef(false);
  const historyMessagesRef = useRef(new Set()); // Track message IDs from history
  const newMessagesStartIndexRef = useRef(0); // Index where new messages start

  // Track if streaming is in progress to prevent history loading during streaming
  const isStreamingRef = useRef(false);

  // Track the last received SSE event ID for reconnection
  const lastEventIdRef = useRef(null);
  // Track reconnection state for UI indicator
  const [isReconnecting, setIsReconnecting] = useState(false);

  // Track if this is a new conversation (for todo list card management)
  const isNewConversationRef = useRef(false);

  // Recently sent messages tracker
  const recentlySentTrackerRef = useRef(createRecentlySentTracker());

  // Track active subagent tasks and map agent IDs (UUID-based: "type:uuid")
  const activeSubagentTasksRef = useRef(new Map()); // Map<agentId, taskInfo> - keyed by agent_id
  const agentToTaskMapRef = useRef(new Map()); // Map<agentId, agentId> - event.agent (agent_id) -> card key (agent_id)
  // Map tool call IDs (from main agent's task tool calls) to agent_ids for routing subagent events
  const toolCallIdToTaskIdMapRef = useRef(new Map()); // Map<toolCallId, agentId>
  // Map display_id ("Task-1") -> agent_id for resolving completed_tasks in subagent_status
  const displayIdToAgentIdMapRef = useRef(new Map());
  // Track pending task tool calls (received before subagent_status)
  // Structure: Array<{toolCallId, timestamp}> - ordered list of task tool calls
  const pendingTaskToolCallsRef = useRef([]); // Array<{toolCallId, timestamp}>
  // Track the order of agent IDs as they first appear in subagent events
  // This helps us match agent IDs to task IDs when multiple subagents run in parallel
  // Structure: Array<agentId> - ordered list of agent IDs as they first appear
  const agentIdOrderRef = useRef([]); // Array<agentId>
  // Track which task IDs have been mapped to agent IDs
  // This helps us assign unmapped agent IDs to unmapped tasks
  // Structure: Set<taskId> - set of task IDs that have been mapped
  const mappedTaskIdsRef = useRef(new Set()); // Set<taskId>

  // Track subagent history loaded from replay so it can be shown lazily
  // Keyed by agent_id. Structure: { [agentId]: { taskId, description, type, messages, status, ... } }
  const subagentHistoryRef = useRef({});

  // During history load: store agent_ids from subagent_status and task tool call IDs for order-based matching
  const historyPendingAgentIdsRef = useRef([]);
  const historyPendingTaskToolCallIdsRef = useRef([]);

  // Update thread ID in localStorage whenever it changes
  useEffect(() => {
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
      setIsLoadingHistory(true);
      setMessageError(null);

      const threadIdToUse = threadId;
      console.log('[History] Loading history for thread:', threadIdToUse);

      // Track pairs being processed - use Map to handle multiple pairs
      const assistantMessagesByPair = new Map(); // Map<pair_index, assistantMessageId>
      const pairStateByPair = new Map(); // Map<pair_index, { contentOrderCounter, reasoningId, toolCallId }>
      
      // Track the currently active pair for artifacts (which don't have pair_index)
      // This ensures artifacts get the correct chronological order
      let currentActivePairIndex = null;
      let currentActivePairState = null;

      // Track pending HITL interrupt from history to resolve status on next user_message
      let pendingHistoryInterrupt = null;

      // Track subagent events by task ID for this history load
      // Map<taskId, { messages: Array, events: Array, description?: string, type?: string }>
      const subagentHistoryByTaskId = new Map();
      // Use a fresh mapping for this history replay, seeded from the live ref
      const agentToTaskMap = new Map(agentToTaskMapRef.current); // Map<agentId, taskId>

      try {
        await replayThreadHistory(threadIdToUse, (event) => {
        const eventType = event.event;
        const contentType = event.content_type;
        const hasRole = event.role !== undefined;
        const hasPairIndex = event.pair_index !== undefined;
        
        // Check if this is a subagent event - filter it out from main chat view
        const isSubagent = isSubagentHistoryEvent(event);
        
        // Update current active pair when we see an event with pair_index
        if (hasPairIndex) {
          const pairIndex = event.pair_index;
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
          console.log('[History] Updated active pair to:', pairIndex, 'counter:', currentActivePairState?.contentOrderCounter);
        }

        // Handle subagent_status events - build agent_id mapping and subagent history storage
        if (eventType === 'subagent_status') {
          // Preferred format: active_tasks with agent_id
          const activeTasks = event.active_tasks || [];
          const completedTasks = event.completed_tasks || [];
          const allTaskItems = [...activeTasks, ...completedTasks];

          // Fallback format: active_subagents / completed_subagents (arrays of agent_id strings)
          const fallbackActive = event.active_subagents || [];
          const fallbackCompleted = event.completed_subagents || [];
          const fallbackAll = [...fallbackActive, ...fallbackCompleted];

          const agentIds = [];
          allTaskItems.forEach((task) => {
            if (!task) return;
            const agentId = task.agent_id || task.agent;
            const displayId = task.id;
            if (agentId) {
              agentIds.push(agentId);
              agentToTaskMap.set(agentId, agentId);
              agentToTaskMapRef.current.set(agentId, agentId);
              if (displayId) {
                displayIdToAgentIdMapRef.current.set(displayId, agentId);
              }
              if (!subagentHistoryByTaskId.has(agentId)) {
                subagentHistoryByTaskId.set(agentId, {
                  messages: [],
                  events: [],
                  description: task.description || '',
                  type: task.type || 'general-purpose',
                });
              }
            }
          });

          fallbackAll.forEach((agentId) => {
            if (agentId && !subagentHistoryByTaskId.has(agentId)) {
              agentIds.push(agentId);
              agentToTaskMap.set(agentId, agentId);
              agentToTaskMapRef.current.set(agentId, agentId);
              subagentHistoryByTaskId.set(agentId, {
                messages: [],
                events: [],
                description: '',
                type: 'general-purpose',
              });
            }
          });

          if (agentIds.length > 0) {
            historyPendingAgentIdsRef.current = agentIds;
            const pendingCalls = historyPendingTaskToolCallIdsRef.current;
            if (pendingCalls.length > 0) {
              const minLen = Math.min(pendingCalls.length, agentIds.length);
              for (let i = 0; i < minLen; i++) {
                toolCallIdToTaskIdMapRef.current.set(pendingCalls[i], agentIds[i]);
              }
              historyPendingTaskToolCallIdsRef.current = [];
            }
          }
          return;
        }

        // Handle subagent events - store them separately, don't process in main chat
        if (isSubagent) {
          // Get task ID from agent mapping
          let taskId = null;
          if (event.agent && agentToTaskMap.has(event.agent)) {
            taskId = agentToTaskMap.get(event.agent);
          }

          // Fallback: if we only have a single known subagent task in this thread,
          // assume all subagent events belong to that task. This matches the current
          // architecture where one background subagent task is active per pair.
          if (!taskId && subagentHistoryByTaskId.size === 1) {
            const [onlyTaskId] = Array.from(subagentHistoryByTaskId.keys());
            taskId = onlyTaskId;
            console.log('[History] Using single-task fallback for subagent event:', {
              taskId,
              eventType,
              agent: event.agent,
            });
          }
          
          if (taskId) {
            // Initialize subagent history storage if needed
            if (!subagentHistoryByTaskId.has(taskId)) {
              subagentHistoryByTaskId.set(taskId, {
                messages: [],
                events: [],
              });
            }
            
            const subagentHistory = subagentHistoryByTaskId.get(taskId);
            // Store the event for later processing
            subagentHistory.events.push(event);
            
            console.log('[History] Stored subagent event:', {
              taskId,
              eventType,
              agent: event.agent,
              totalEvents: subagentHistory.events.length,
            });
          } else {
            console.warn('[History] Subagent event without task ID mapping:', {
              eventType,
              agent: event.agent,
              availableAgents: Array.from(agentToTaskMap.keys()),
            });
          }
          
          // Don't process subagent events in main chat view
          return;
        }

        // Handle user_message events from history
        // Note: event.content may be empty for HITL resume pairs (plan approval/rejection)
        if (eventType === 'user_message' && hasPairIndex) {
          // Resolve pending plan approval status based on this user message
          if (pendingHistoryInterrupt) {
            const hasContent = event.content && event.content.trim();
            const resolvedStatus = hasContent ? 'rejected' : 'approved';
            const { assistantMessageId: planMsgId, planApprovalId } = pendingHistoryInterrupt;

            setMessages((prev) =>
              updateMessage(prev, planMsgId, (msg) => ({
                ...msg,
                planApprovals: {
                  ...(msg.planApprovals || {}),
                  [planApprovalId]: {
                    ...(msg.planApprovals?.[planApprovalId] || {}),
                    status: resolvedStatus,
                  },
                },
              }))
            );
            pendingHistoryInterrupt = null;
          }

          const pairIndex = event.pair_index;
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
          const pairIndex = event.pair_index;
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received message_chunk for unknown pair_index:', pairIndex);
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
        // In history replay, artifacts DO have pair_index, so we can use it directly
        if (eventType === 'artifact') {
          const artifactType = event.artifact_type;
          if (artifactType === 'todo_update') {
            // Artifacts in history replay have pair_index - use it!
            if (hasPairIndex) {
              const pairIndex = event.pair_index;
              // Update active pair tracking
              currentActivePairIndex = pairIndex;
              currentActivePairState = pairStateByPair.get(pairIndex);
              
              const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
              const pairState = pairStateByPair.get(pairIndex);

              if (!currentAssistantMessageId || !pairState) {
                console.warn('[History] Received artifact for unknown pair_index:', pairIndex);
                return;
              }

              console.log('[History] Processing todo_update artifact for pair:', pairIndex, 'counter:', pairState.contentOrderCounter);
              handleHistoryTodoUpdate({
                assistantMessageId: currentAssistantMessageId,
                artifactType,
                artifactId: event.artifact_id,
                payload: event.payload || {},
                pairState: pairState,
                setMessages,
              });
            } else {
              // Fallback: artifacts without pair_index (shouldn't happen in history, but handle gracefully)
              console.warn('[History] Artifact without pair_index, using active pair fallback');
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
                  payload: event.payload || {},
                  pairState: targetPairState,
                  setMessages,
                });
              }
            }
          }
          return;
        }

        // Handle tool_calls events
        if (eventType === 'tool_calls' && hasPairIndex) {
          const pairIndex = event.pair_index;
          // Update active pair tracking
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
          
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received tool_calls for unknown pair_index:', pairIndex);
            return;
          }

          // Extract task tool call IDs and map to agent_ids (from subagent_status) by order
          if (event.tool_calls) {
            const taskToolCalls = event.tool_calls.filter((tc) => (tc.name === 'task' || tc.name === 'Task') && tc.id);
            const agentIds = historyPendingAgentIdsRef.current;
            const toolCallIds = taskToolCalls.map((tc) => tc.id).filter(Boolean);
            if (agentIds.length > 0 && toolCallIds.length > 0) {
              const minLen = Math.min(toolCallIds.length, agentIds.length);
              for (let i = 0; i < minLen; i++) {
                toolCallIdToTaskIdMapRef.current.set(toolCallIds[i], agentIds[i]);
              }
            } else if (toolCallIds.length > 0) {
              historyPendingTaskToolCallIdsRef.current = [
                ...historyPendingTaskToolCallIdsRef.current,
                ...toolCallIds,
              ];
            }
            taskToolCalls.forEach((toolCall, i) => {
              const agentId = agentIds[i];
              if (agentId && !subagentHistoryByTaskId.has(agentId)) {
                subagentHistoryByTaskId.set(agentId, {
                  messages: [],
                  events: [],
                  description: toolCall.args?.description || '',
                  type: toolCall.args?.subagent_type || 'general-purpose',
                });
              }
            });
          }

          handleHistoryToolCalls({
            assistantMessageId: currentAssistantMessageId,
            toolCalls: event.tool_calls,
            pairState,
            setMessages,
          });
          return;
        }

        // Handle tool_call_result events
        if (eventType === 'tool_call_result' && hasPairIndex) {
          const pairIndex = event.pair_index;
          // Update active pair tracking
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
          
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received tool_call_result for unknown pair_index:', pairIndex);
            return;
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
          return;
        }

        // Handle interrupt events during history replay — inject plan_approval
        // segment into the current assistant message. Status will be resolved
        // when the next user_message arrives (empty = approved, has content = rejected).
        if (eventType === 'interrupt') {
          const pairIndex = event.pair_index ?? currentActivePairIndex;
          const interruptAssistantId = pairIndex != null ? assistantMessagesByPair.get(pairIndex) : null;
          const pairState = pairIndex != null ? pairStateByPair.get(pairIndex) : null;

          if (interruptAssistantId && pairState) {
            const planApprovalId = event.interrupt_id || `plan-history-${Date.now()}`;
            const description =
              event.action_requests?.[0]?.description ||
              event.action_requests?.[0]?.args?.plan ||
              'No plan description provided.';
            pairState.contentOrderCounter++;
            const order = pairState.contentOrderCounter;

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
                    status: 'approved', // Default; resolved on next user_message
                  },
                },
              }))
            );

            pendingHistoryInterrupt = {
              assistantMessageId: interruptAssistantId,
              planApprovalId,
            };
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
          console.log('[History] Credit usage event (end of pair):', event.pair_index);
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
        
        // Process stored subagent events and build their messages
        // NOTE: During history replay we DO NOT open floating cards automatically.
        // We only build per-task message history here; cards are created lazily
        // when the user clicks \"Open subagent details\" in the main chat view.
        if (subagentHistoryByTaskId.size > 0) {
          console.log('[History] Processing subagent history for', subagentHistoryByTaskId.size, 'tasks');
          
          // Process each subagent's events
          for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
            // Create temporary refs structure for processing
            const tempSubagentStateRefs = {
              [taskId]: {
                contentOrderCounterRef: { current: 0 },
                currentReasoningIdRef: { current: null },
                currentToolCallIdRef: { current: null },
                messages: [],
              },
            };
            
            const tempRefs = {
              subagentStateRefs: tempSubagentStateRefs,
            };

            // History-specific no-op updater: prevents floating cards from being
            // created during history load while still letting handlers build
            // the in-memory message structures in tempSubagentStateRefs.
            const historyUpdateSubagentCard = () => {};
            
            // Process each event in chronological order
            console.log('[History] Processing', subagentHistory.events.length, 'events for task:', taskId);
            for (let i = 0; i < subagentHistory.events.length; i++) {
              const event = subagentHistory.events[i];
              const eventType = event.event;
              const contentType = event.content_type;
              // Use a consistent message ID for all events from the same subagent
              // In history, subagent events might not have consistent IDs, so we use taskId-based ID
              const assistantMessageId = event.id || `subagent-${taskId}-msg`;
              
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
              } else {
                console.warn('[History] Unhandled subagent event type:', eventType);
              }
            }
            
            // Get final messages from temp refs
            const finalMessages = tempSubagentStateRefs[taskId]?.messages || [];

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
              type: taskMetadata?.type || 'general-purpose',
              messages: finalMessages,
              status: 'completed', // History events are always completed
              toolCalls: 0,
              currentTool: '',
            };

            console.log('[History] Stored subagent history for task:', taskId, 'with', finalMessages.length, 'messages');
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
  const reconnectToStream = async (lastEventId = null) => {
    if (!threadId || threadId === '__default__') return;

    console.log('[Reconnect] Starting reconnection for thread:', threadId, 'lastEventId:', lastEventId);
    setIsLoading(true);
    setIsReconnecting(true);
    isStreamingRef.current = true;

    // Create assistant message placeholder for reconnected content
    const assistantMessageId = `assistant-reconnect-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

    const assistantMessage = createAssistantMessage(assistantMessageId);
    setMessages((prev) => appendMessage(prev, assistantMessage));
    currentMessageRef.current = assistantMessageId;

    // Prepare refs for event handlers
    const subagentStateRefs = {};
    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      updateTodoListCard,
      isNewConversation: false,
      subagentStateRefs,
      updateSubagentCard: updateSubagentCard || (() => {}),
      isReconnect: true,
    };

    const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent);

    try {
      await reconnectToWorkflowStream(threadId, lastEventId, processEvent);

      // Mark message as complete
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );
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
      setIsLoading(false);
      setIsReconnecting(false);
      isStreamingRef.current = false;
      currentMessageRef.current = null;

      // Clean up empty reconnect messages (no content segments = nothing was streamed)
      setMessages((prev) => {
        const msg = prev.find((m) => m.id === assistantMessageId);
        if (msg && (!msg.contentSegments || msg.contentSegments.length === 0) && !msg.content) {
          return prev.filter((m) => m.id !== assistantMessageId);
        }
        return prev;
      });

      if (inactivateAllSubagents) {
        inactivateAllSubagents();
      }
      if (minimizeInactiveSubagents) {
        minimizeInactiveSubagents();
      }

      // Auto-complete pending todos in floating card and inline message
      if (completePendingTodos) {
        completePendingTodos();
      }
      setMessages((prev) => {
        const msg = prev.find((m) => m.id === assistantMessageId);
        if (!msg?.todoListProcesses || Object.keys(msg.todoListProcesses).length === 0) return prev;
        // Find the last todoListProcesses entry (highest order) and mark all todos completed
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
    }
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

      // Run history load and workflow status check in parallel to save ~100-300ms
      const [, status] = await Promise.all([
        loadConversationHistory(),
        getWorkflowStatus(threadId).catch((statusErr) => {
          console.log('[Reconnect] Could not check workflow status:', statusErr.message);
          return { can_reconnect: false };
        }),
      ]);

      if (cancelled) return;

      if (status.can_reconnect) {
        console.log('[Reconnect] Workflow status:', status.status, 'can_reconnect:', status.can_reconnect);
        await reconnectToStream(lastEventIdRef.current);
      }
    };

    loadAndMaybeReconnect();

    // Cleanup: Cancel loading if workspace or thread changes or component unmounts
    return () => {
      console.log('[History] Cleanup: canceling history load for workspace:', workspaceId, 'thread:', threadId);
      cancelled = true;
      historyLoadingRef.current = false;
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId]);

  /**
   * Helper to get taskId from event.
   * Routes subagent events to the correct task based on agent ID mapping.
   * Defined at hook level so it can be shared between handleSendMessage and reconnectToStream.
   */
  const getTaskIdFromEvent = (event) => {
    if (!event.agent) {
      if (process.env.NODE_ENV === 'development') {
        console.warn('[Stream] Subagent event without agent field:', event);
      }
      return null;
    }

    const agentId = event.agent;

    // Strategy 1: Direct mapping from agent ID to task ID
    if (agentToTaskMapRef.current.has(agentId)) {
      const taskId = agentToTaskMapRef.current.get(agentId);
      mappedTaskIdsRef.current.add(taskId);
      if (process.env.NODE_ENV === 'development') {
        console.log('[Stream] Found task ID from agent mapping:', { agentId, taskId });
      }
      return taskId;
    }

    // Strategy 2: Map via tool_call_id
    if (event.tool_call_id && toolCallIdToTaskIdMapRef.current.has(event.tool_call_id)) {
      const taskId = toolCallIdToTaskIdMapRef.current.get(event.tool_call_id);
      agentToTaskMapRef.current.set(agentId, taskId);
      mappedTaskIdsRef.current.add(taskId);
      if (process.env.NODE_ENV === 'development') {
        console.log('[Stream] Found task ID from tool_call_id mapping:', { agentId, toolCallId: event.tool_call_id, taskId });
      }
      return taskId;
    }

    // Strategy 3: Match unmapped agents to unmapped tasks
    const activeTasks = Array.from(activeSubagentTasksRef.current.keys());
    const agentIdOrder = agentIdOrderRef.current;
    const mappedTaskIds = mappedTaskIdsRef.current;

    if (!agentIdOrder.includes(agentId)) {
      agentIdOrder.push(agentId);
      if (process.env.NODE_ENV === 'development') {
        console.log('[Stream] Added agent ID to order list:', { agentId, order: agentIdOrder.length, totalTasks: activeTasks.length });
      }
    }

    const unmappedTasks = activeTasks.filter(taskId => !mappedTaskIds.has(taskId));
    if (unmappedTasks.length > 0) {
      const taskId = unmappedTasks[0];
      agentToTaskMapRef.current.set(agentId, taskId);
      mappedTaskIds.add(taskId);
      if (process.env.NODE_ENV === 'development') {
        console.log('[Stream] Matched agent to unmapped task:', { agentId, taskId });
      }
      return taskId;
    }

    // Strategy 3b: Order-based fallback
    const agentIndex = agentIdOrder.indexOf(agentId);
    if (agentIndex >= 0 && agentIndex < activeTasks.length) {
      const taskId = activeTasks[agentIndex];
      agentToTaskMapRef.current.set(agentId, taskId);
      mappedTaskIds.add(taskId);
      if (process.env.NODE_ENV === 'development') {
        console.log('[Stream] Matched agent to task by order (fallback):', { agentId, agentIndex, taskId });
      }
      return taskId;
    }

    // Strategy 4: Single-task fallback
    if (activeTasks.length === 1) {
      const taskId = activeTasks[0];
      agentToTaskMapRef.current.set(agentId, taskId);
      if (process.env.NODE_ENV === 'development') {
        console.log('[Stream] Using single-task fallback for agent:', { agentId, taskId });
      }
      return taskId;
    }

    // Strategy 5: Cannot route
    if (process.env.NODE_ENV === 'development') {
      console.warn('[Stream] Cannot route subagent event - no mapping found:', {
        agentId, activeTasks, agentIndex, agentIdOrder,
        hasToolCallId: !!event.tool_call_id, toolCallId: event.tool_call_id,
      });
    }
    return null;
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
  const createStreamEventProcessor = (assistantMessageId, refs, getTaskIdFromEvent) => {
    return (event) => {
      const eventType = event.event || 'message_chunk';

      // Track last event ID for reconnection
      if (event._eventId != null) {
        lastEventIdRef.current = event._eventId;
      }

      // Debug: Log all events to see what we're receiving
      if (event.artifact_type || eventType === 'artifact') {
        console.log('[Stream] Artifact event detected:', { eventType, event, artifact_type: event.artifact_type });
      }

      // Update thread_id if provided in the event
      if (event.thread_id && event.thread_id !== threadId && event.thread_id !== '__default__') {
        setThreadId(event.thread_id);
        setStoredThreadId(workspaceId, event.thread_id);
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

      // Handle subagent_status events
      if (eventType === 'subagent_status') {
        const subagentStatus = {
          active_tasks: event.active_tasks || [],
          completed_tasks: event.completed_tasks || [],
          active_subagents: event.active_subagents || [],
          completed_subagents: event.completed_subagents || [],
        };

        activeSubagentTasksRef.current.clear();

        // Preferred format: active_tasks with agent_id
        const activeTasks = subagentStatus.active_tasks;
        const completedTasks = subagentStatus.completed_tasks;
        const allTaskObjects = activeTasks.filter((t) => t && (t.agent_id || t.agent));

        // Fallback format: active_subagents (array of agent_id strings)
        const fallbackActive = subagentStatus.active_subagents;
        const fallbackCompleted = subagentStatus.completed_subagents;
        const allTasksForMapping = allTaskObjects.length > 0
          ? allTaskObjects
          : [...(Array.isArray(fallbackActive) ? fallbackActive : []), ...(Array.isArray(fallbackCompleted) ? fallbackCompleted : [])].filter(Boolean).map((aid) => ({ agent_id: aid, agent: aid }));

        const pendingCalls = pendingTaskToolCallsRef.current;
        if (pendingCalls.length > 0 && allTasksForMapping.length > 0) {
          const minLength = Math.min(pendingCalls.length, allTasksForMapping.length);
          for (let i = 0; i < minLength; i++) {
            const toolCallId = pendingCalls[i].toolCallId;
            const task = allTasksForMapping[i];
            const agentId = task.agent_id || task.agent;
            if (toolCallId && agentId) {
              toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
            }
          }
          pendingTaskToolCallsRef.current = [];
        }

        mappedTaskIdsRef.current.clear();

        allTaskObjects.forEach((task) => {
          const agentId = task.agent_id || task.agent;
          if (agentId) {
            activeSubagentTasksRef.current.set(agentId, task);
            agentToTaskMapRef.current.set(agentId, agentId);
            if (task.id) {
              displayIdToAgentIdMapRef.current.set(task.id, agentId);
            }
          }
        });

        if (updateSubagentCard) {
          handleSubagentStatus({
            subagentStatus,
            updateSubagentCard,
            displayIdToAgentIdMap: displayIdToAgentIdMapRef.current,
          });
        }

        // Also update message-level subagentTasks status for completed tasks.
        // The Task tool_call_result only means "task was launched", not "task finished".
        // The real completion signal comes here via completed_tasks in subagent_status.
        const resolvedCompletedAgentIds = new Set();
        const agentIdToResult = new Map();
        completedTasks.forEach((item) => {
          if (typeof item === 'string') {
            const aid = displayIdToAgentIdMapRef.current.get(item) || item;
            resolvedCompletedAgentIds.add(aid);
          } else if (item && typeof item === 'object') {
            const aid = item.agent_id || item.agent || (item.id && displayIdToAgentIdMapRef.current.get(item.id));
            if (aid) {
              resolvedCompletedAgentIds.add(aid);
              if (item.result) agentIdToResult.set(aid, item.result);
            }
          }
        });

        if (resolvedCompletedAgentIds.size > 0) {
          // Build reverse map: agentId → toolCallId
          const agentIdToToolCallId = new Map();
          toolCallIdToTaskIdMapRef.current.forEach((agentId, toolCallId) => {
            agentIdToToolCallId.set(agentId, toolCallId);
          });

          setMessages((prev) =>
            prev.map((msg) => {
              if (!msg.subagentTasks || Object.keys(msg.subagentTasks).length === 0) return msg;
              let changed = false;
              const updatedSubagentTasks = { ...msg.subagentTasks };
              resolvedCompletedAgentIds.forEach((agentId) => {
                const toolCallId = agentIdToToolCallId.get(agentId);
                if (toolCallId && updatedSubagentTasks[toolCallId]) {
                  const existing = updatedSubagentTasks[toolCallId];
                  const result = agentIdToResult.get(agentId) || existing.result || null;
                  if (existing.status !== 'completed' || (result && !existing.result)) {
                    updatedSubagentTasks[toolCallId] = { ...existing, status: 'completed', result };
                    changed = true;
                  }
                }
              });
              return changed ? { ...msg, subagentTasks: updatedSubagentTasks } : msg;
            })
          );
        }

        return;
      }

      // Handle subagent message events (filter them out from main chat view)
      if (isSubagent) {
        let taskId = getTaskIdFromEvent(event);

        // Debug logging for routing
        if (process.env.NODE_ENV === 'development') {
          console.log('[Stream] Routing subagent event:', {
            eventType,
            agent: event.agent,
            toolCallId: event.tool_call_id,
            initialTaskId: taskId,
            activeTasks: Array.from(activeSubagentTasksRef.current.keys()),
            agentOrder: agentIdOrderRef.current,
            agentToTaskMap: Array.from(agentToTaskMapRef.current.entries()),
            toolCallToTaskMap: Array.from(toolCallIdToTaskIdMapRef.current.entries()),
          });
        }

        // If we couldn't determine taskId, try to build mapping from available info
        if (!taskId && event.agent) {
          const agentId = event.agent;

          // Strategy: If we have tool_call_id, try to find the corresponding task
          if (event.tool_call_id && toolCallIdToTaskIdMapRef.current.has(event.tool_call_id)) {
            const mappedTaskId = toolCallIdToTaskIdMapRef.current.get(event.tool_call_id);
            // Cache the agent-to-task mapping for future events
            agentToTaskMapRef.current.set(agentId, mappedTaskId);
            taskId = mappedTaskId; // Use the mapped task ID
            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] Built agent-to-task mapping from tool_call_id:', {
                agentId,
                toolCallId: event.tool_call_id,
                taskId: mappedTaskId,
              });
            }
          }
        }

        // If we still don't have a taskId, log and skip this event
        if (!taskId) {
          if (process.env.NODE_ENV === 'development') {
            console.warn('[Stream] Cannot route subagent event - no task ID found:', {
              agent: event.agent,
              eventType,
              toolCallId: event.tool_call_id,
              activeTasks: Array.from(activeSubagentTasksRef.current.keys()),
              pendingToolCalls: pendingTaskToolCallsRef.current.length,
              agentOrder: agentIdOrderRef.current,
              agentToTaskMap: Array.from(agentToTaskMapRef.current.entries()),
            });
          }
          return; // Don't process in main chat view
        }

        // Log successful routing
        if (process.env.NODE_ENV === 'development') {
          console.log('[Stream] Successfully routed subagent event to task:', {
            agent: event.agent,
            taskId,
            eventType,
          });
        }

        // Process the event with the correct taskId
        if (updateSubagentCard) {
          // Build agent-to-task mapping if not already present
          if (!agentToTaskMapRef.current.has(event.agent)) {
            agentToTaskMapRef.current.set(event.agent, taskId);
            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] Built agent-to-task mapping from event:', {
                agent: event.agent,
                taskId,
                eventType,
              });
            }
          }

          const subagentAssistantMessageId = event.id || `subagent-${Date.now()}`;

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
          }
        }
        return; // Don't process subagent events in main chat view
      }

      // Handle different event types (main agent only)
      if (isSubagent) {
        console.warn('[Stream] Subagent event reached main agent handler - this should not happen:', {
          eventType,
          agent: event.agent,
        });
        return;
      }

      if (eventType === 'message_chunk') {
        const contentType = event.content_type || 'text';

        // Handle reasoning_signal events
        if (contentType === 'reasoning_signal') {
          const signalContent = event.content || '';
          if (handleReasoningSignal({
            assistantMessageId,
            signalContent,
            refs,
            setMessages,
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
        return;
      } else if (eventType === 'artifact') {
        if (isSubagent) {
          return;
        }

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
          });
          console.log('[Stream] handleTodoUpdate result:', result);
        } else if (artifactType === 'file_operation' && onFileArtifact) {
          onFileArtifact(event);
        }
        return;
      } else if (eventType === 'tool_calls') {
        // Track 'task' tool calls for mapping to task IDs
        if (event.tool_calls && Array.isArray(event.tool_calls)) {
          event.tool_calls.forEach((toolCall) => {
            if ((toolCall.name === 'task' || toolCall.name === 'Task') && toolCall.id) {
              pendingTaskToolCallsRef.current.push({
                toolCallId: toolCall.id,
                timestamp: Date.now(),
              });
              if (process.env.NODE_ENV === 'development') {
                console.log('[Stream] Tracked task tool call for mapping:', {
                  toolCallId: toolCall.id,
                  description: toolCall.args?.description,
                  pendingCount: pendingTaskToolCallsRef.current.length,
                });
              }
            }
          });
        }

        handleToolCalls({
          assistantMessageId,
          toolCalls: event.tool_calls,
          finishReason: event.finish_reason,
          refs,
          setMessages,
        });
      } else if (eventType === 'tool_call_result') {
        const toolCallId = event.tool_call_id;
        if (toolCallId && !toolCallIdToTaskIdMapRef.current.has(toolCallId)) {
          if (process.env.NODE_ENV === 'development') {
            console.log('[Stream] Received tool_call_result for task tool:', {
              toolCallId,
              agent: event.agent,
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
      } else if (eventType === 'interrupt') {
        // HITL plan mode interrupt — agent is paused, waiting for user approval.
        // Inject a plan_approval content segment into the assistant message so
        // the plan card renders inline in the message list.
        const planApprovalId = event.interrupt_id || `plan-${Date.now()}`;
        const description =
          event.action_requests?.[0]?.description ||
          event.action_requests?.[0]?.args?.plan ||
          'No plan description provided.';

        const order = refs.contentOrderCounterRef.current++;

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

        setPendingInterrupt({
          interruptId: event.interrupt_id,
          actionRequests: event.action_requests || [],
          threadId: event.thread_id,
          assistantMessageId,
          planApprovalId,
        });

        setIsLoading(false);
        isStreamingRef.current = false;
        currentMessageRef.current = null;
      }
    };
  };

  /**
   * Handles sending a message and streaming the response
   *
   * @param {string} message - The user's message
   * @param {boolean} planMode - Whether to use plan mode
   * @param {Array|null} additionalContext - Optional additional context for skill loading
   */
  const handleSendMessage = async (message, planMode = false, additionalContext = null) => {
    if (!workspaceId || !message.trim() || isLoading) {
      return;
    }

    // Intercept: if a plan was rejected, route this message as rejection feedback
    if (pendingRejection) {
      const { interruptId } = pendingRejection;
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
      return resumeWithHitlResponse(hitlResponse);
    }

    // Create and add user message
    const userMessage = createUserMessage(message);
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

    try {
      // Build message history for API (filter out assistant messages)
      const messageHistory = messages
        .filter((msg) => msg.role === 'user')
        .map((msg) => ({
          role: msg.role,
          content: msg.content,
        }));

      // Prepare refs for event handlers
      const subagentStateRefs = {}; // Will be populated as subagents are detected
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        updateTodoListCard,
        isNewConversation: isNewConversationRef.current,
        subagentStateRefs,
        updateSubagentCard: updateSubagentCard || (() => {}),
      };

      // Create the event processor using the shared factory
      const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent);

      await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        messageHistory,
        planMode,
        processEvent,
        getAuthUserId() || DEFAULT_USER_ID,
        additionalContext,
        agentMode
      );

      // Mark message as complete
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );
    } catch (err) {
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
        } finally {
          setIsLoading(false);
          currentMessageRef.current = null;
          // Mark streaming as complete - this will allow history loading to proceed if thread ID changed
          isStreamingRef.current = false;
          
          // Inactivate all subagent cards at the end of streaming
          // This prevents task ID collisions when new subagents are created with the same IDs
          if (inactivateAllSubagents) {
            inactivateAllSubagents();
            if (process.env.NODE_ENV === 'development') {
              console.log('[useChatMessages] Inactivated all subagents at end of streaming');
            }
          }
          
          // Minimize all inactive subagent cards at the end of streaming
          // This keeps the UI clean by hiding cards from previous conversations
          if (minimizeInactiveSubagents) {
            minimizeInactiveSubagents();
            if (process.env.NODE_ENV === 'development') {
              console.log('[useChatMessages] Minimized all inactive subagents at end of streaming');
            }
          }

          // Auto-complete pending todos in floating card and inline message
          if (completePendingTodos) {
            completePendingTodos();
          }
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
        }
      };

  /**
   * Resumes an interrupted workflow with an HITL response (approve or reject).
   * Follows the same pattern as handleSendMessage but sends messages: [] with hitl_response.
   */
  const resumeWithHitlResponse = useCallback(async (hitlResponse) => {
    setPendingInterrupt(null);

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

    // Prepare refs for event handlers
    const subagentStateRefs = {};
    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      updateTodoListCard,
      isNewConversation: false,
      subagentStateRefs,
      updateSubagentCard: updateSubagentCard || (() => {}),
    };

    const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent);

    try {
      await sendHitlResponse(
        workspaceId,
        threadId,
        hitlResponse,
        processEvent,
        getAuthUserId() || DEFAULT_USER_ID
      );

      // Mark message as complete
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );
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
      setIsLoading(false);
      currentMessageRef.current = null;
      isStreamingRef.current = false;

      if (inactivateAllSubagents) {
        inactivateAllSubagents();
      }
      if (minimizeInactiveSubagents) {
        minimizeInactiveSubagents();
      }

      // Auto-complete pending todos in floating card and inline message
      if (completePendingTodos) {
        completePendingTodos();
      }
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
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, updateTodoListCard, updateSubagentCard, inactivateAllSubagents, minimizeInactiveSubagents, completePendingTodos]);

  const handleApproveInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, assistantMessageId, planApprovalId } = pendingInterrupt;

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
    resumeWithHitlResponse(hitlResponse);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, assistantMessageId, planApprovalId } = pendingInterrupt;

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

    // Store interruptId so next handleSendMessage routes as rejection feedback
    setPendingRejection({ interruptId });
    setPendingInterrupt(null);
  }, [pendingInterrupt]);

  return {
    messages,
    threadId,
    isLoading,
    isLoadingHistory,
    isReconnecting,
    messageError,
    handleSendMessage,
    pendingInterrupt,
    pendingRejection,
    handleApproveInterrupt,
    handleRejectInterrupt,
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
