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

import { useState, useRef, useEffect } from 'react';
import { sendChatMessageStream, replayThreadHistory, DEFAULT_USER_ID } from '../utils/api';
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

export function useChatMessages(workspaceId, initialThreadId = null, updateTodoListCard = null, updateSubagentCard = null) {
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

  // Track if this is a new conversation (for todo list card management)
  const isNewConversationRef = useRef(false);

  // Recently sent messages tracker
  const recentlySentTrackerRef = useRef(createRecentlySentTracker());

  // Track active subagent tasks and map agent IDs to task IDs
  const activeSubagentTasksRef = useRef(new Map()); // Map<taskId, taskInfo>
  const agentToTaskMapRef = useRef(new Map()); // Map<agentId, taskId> - maps "tools:..." to taskId

  // Track subagent history loaded from replay so it can be shown lazily
  // Structure: { [taskId]: { taskId, description, type, messages, status, toolCalls, currentTool } }
  const subagentHistoryRef = useRef({});

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

        // Handle subagent_status events - build agent-to-task mapping
        if (eventType === 'subagent_status') {
          const activeTasks = event.active_tasks || [];
          const completedTasks = event.completed_tasks || [];
          
          // Build agent-to-task mapping from active and completed tasks
          [...activeTasks, ...completedTasks].forEach((task) => {
            if (task && task.id && task.agent) {
              agentToTaskMap.set(task.agent, task.id);
              agentToTaskMapRef.current.set(task.agent, task.id);
              console.log('[History] Mapped agent to task:', {
                agent: task.agent,
                taskId: task.id,
              });
              
              // Initialize subagent history storage for this task
              if (!subagentHistoryByTaskId.has(task.id)) {
                subagentHistoryByTaskId.set(task.id, {
                  messages: [],
                  events: [],
                  description: task.description || '',
                  type: task.type || 'general-purpose',
                });
              }
            }
          });
          
          // Don't process subagent_status in main chat view
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
        if (eventType === 'user_message' && event.content && hasPairIndex) {
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

          // Extract task IDs from 'task' tool calls and initialize storage
          // Note: Agent-to-task mapping will come from subagent_status events
          if (event.tool_calls) {
            event.tool_calls.forEach((toolCall) => {
              if (toolCall.name === 'task' && toolCall.id) {
                const taskId = toolCall.id;
                // Initialize subagent history storage for this task
                if (!subagentHistoryByTaskId.has(taskId)) {
                  subagentHistoryByTaskId.set(taskId, {
                    messages: [],
                    events: [],
                    description: toolCall.args?.description || '',
                    type: toolCall.args?.subagent_type || 'general-purpose',
                  });
                }
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
            },
            pairState,
            setMessages,
          });
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

  // Load history when workspace or threadId changes
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

    console.log('[History] Calling loadConversationHistory for thread:', threadId);
    loadConversationHistory();

    // Cleanup: Cancel loading if workspace or thread changes or component unmounts
    return () => {
      console.log('[History] Cleanup: canceling history load for workspace:', workspaceId, 'thread:', threadId);
      historyLoadingRef.current = false;
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId]);

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

      // Helper to get taskId from agent ID or use most recent active task
      const getTaskIdFromEvent = (event) => {
        if (!event.agent) return null;
        
        // Check if we have a mapping for this agent ID
        const agentId = event.agent;
        if (agentToTaskMapRef.current.has(agentId)) {
          return agentToTaskMapRef.current.get(agentId);
        }
        
        // If only one active task, use it
        const activeTasks = Array.from(activeSubagentTasksRef.current.keys());
        if (activeTasks.length === 1) {
          const taskId = activeTasks[0];
          // Cache the mapping
          agentToTaskMapRef.current.set(agentId, taskId);
          return taskId;
        }
        
        // If multiple active tasks, try to match by checking recent events
        // For now, return the first active task as fallback
        return activeTasks[0] || null;
      };

      await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        messageHistory,
        planMode,
        (event) => {
          const eventType = event.event || 'message_chunk';
          
          // Debug: Log all events to see what we're receiving
          if (event.artifact_type || eventType === 'artifact') {
            console.log('[Stream] Artifact event detected:', { eventType, event, artifact_type: event.artifact_type });
          }

          // Update thread_id if provided in the event
          // Note: We don't trigger history loading here because isStreamingRef is still true
          // History will be loaded after streaming completes (in the finally block)
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
            };
            
            // Debug: Log subagent status events to help identify issues
            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] subagent_status event:', {
                active_tasks_count: subagentStatus.active_tasks.length,
                completed_tasks_count: subagentStatus.completed_tasks.length,
                active_tasks: subagentStatus.active_tasks.map(t => ({ id: t?.id, hasId: !!t?.id })),
                completed_tasks: subagentStatus.completed_tasks.map(t => ({ id: t?.id, hasId: !!t?.id })),
              });
            }
            
            // Update active tasks tracking
            activeSubagentTasksRef.current.clear();
            subagentStatus.active_tasks.forEach((task) => {
              // Only track tasks with valid IDs
              if (task && task.id) {
                activeSubagentTasksRef.current.set(task.id, task);
              } else if (process.env.NODE_ENV === 'development') {
                console.warn('[Stream] Skipping task without ID in active_tasks:', task);
              }
            });
            
            // Handle subagent status
            if (updateSubagentCard) {
              handleSubagentStatus({
                subagentStatus,
                updateSubagentCard,
              });
            }
            return; // Don't process subagent_status in main chat view
          }

          // Handle subagent message events (filter them out from main chat view)
          if (isSubagent) {
            const taskId = getTaskIdFromEvent(event);
            if (taskId && updateSubagentCard) {
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
                // Extract tool_call_id from event
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
                  },
                  refs,
                  updateSubagentCard,
                });
              } else if (eventType === 'artifact') {
                // Subagent artifact events (e.g., todo_update) - skip them
                // They should be handled by the subagent's own message processing
                // For now, we just filter them out to prevent duplication
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
          // Double-check that this is NOT a subagent event (safety check)
          if (isSubagent) {
            // This shouldn't happen if the code above is correct, but add safety check
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
            // Handle errors
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
            // Filter out tool_call_chunks events
            return;
          } else if (eventType === 'artifact') {
            // Check if artifact is from subagent - if so, skip it (subagent artifacts are handled separately)
            if (isSubagent) {
              return; // Don't process subagent artifacts in main chat view
            }
            
            // Handle artifact events (e.g., todo_update) - main agent only
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
            }
            return;
          } else if (eventType === 'tool_calls') {
            handleToolCalls({
              assistantMessageId,
              toolCalls: event.tool_calls,
              finishReason: event.finish_reason,
              refs,
              setMessages,
            });
          } else if (eventType === 'tool_call_result') {
            handleToolCallResult({
              assistantMessageId,
              toolCallId: event.tool_call_id,
              result: {
                content: event.content,
                content_type: event.content_type,
                tool_call_id: event.tool_call_id,
              },
              refs,
              setMessages,
            });
          }
        },
        DEFAULT_USER_ID,
        additionalContext
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
        }
      };

  return {
    messages,
    threadId,
    isLoading,
    isLoadingHistory,
    messageError,
    handleSendMessage,
    // Expose subagent history for lazy loading in floating cards
    getSubagentHistory: (taskId) => subagentHistoryRef.current?.[taskId] || null,
  };
}
