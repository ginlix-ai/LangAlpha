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
} from './utils/streamEventHandlers';
import {
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryToolCallResult,
  handleHistoryTodoUpdate,
} from './utils/historyEventHandlers';

export function useChatMessages(workspaceId, initialThreadId = null, updateTodoListCard = null) {
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

      try {
        await replayThreadHistory(threadIdToUse, (event) => {
        const eventType = event.event;
        const contentType = event.content_type;
        const hasRole = event.role !== undefined;
        const hasPairIndex = event.pair_index !== undefined;
        
        // Update current active pair when we see an event with pair_index
        if (hasPairIndex) {
          const pairIndex = event.pair_index;
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
          console.log('[History] Updated active pair to:', pairIndex, 'counter:', currentActivePairState?.contentOrderCounter);
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
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        updateTodoListCard,
        isNewConversation: isNewConversationRef.current,
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

          // Handle different event types
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
            // Handle artifact events (e.g., todo_update)
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
  };
}
