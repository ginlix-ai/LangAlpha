import { useState, useRef, useEffect } from 'react';
import { sendChatMessageStream, getConversations, replayThreadHistory } from '../utils/api';

/**
 * Storage key prefix for thread IDs
 */
const THREAD_ID_STORAGE_PREFIX = 'workspace_thread_id_';

/**
 * Gets the stored thread ID for a workspace from localStorage
 * @param {string} workspaceId - The workspace ID
 * @returns {string} The stored thread ID or '__default__' if not found
 */
const getStoredThreadId = (workspaceId) => {
  if (!workspaceId) return '__default__';
  try {
    const stored = localStorage.getItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`);
    return stored || '__default__';
  } catch (error) {
    console.warn('Failed to read thread ID from localStorage:', error);
    return '__default__';
  }
};

/**
 * Stores the thread ID for a workspace in localStorage
 * @param {string} workspaceId - The workspace ID
 * @param {string} threadId - The thread ID to store
 */
const setStoredThreadId = (workspaceId, threadId) => {
  if (!workspaceId || !threadId || threadId === '__default__') return;
  try {
    localStorage.setItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`, threadId);
  } catch (error) {
    console.warn('Failed to save thread ID to localStorage:', error);
  }
};

/**
 * Custom hook for managing chat messages and streaming
 * 
 * Handles:
 * - Message state management
 * - Thread ID management (persisted per workspace)
 * - Message sending with SSE streaming
 * - Assistant message placeholder creation
 * - Streaming updates and error handling
 * 
 * @param {string} workspaceId - The workspace ID for the chat session
 * @returns {Object} Message state and handlers
 */
export function useChatMessages(workspaceId) {
  const [messages, setMessages] = useState([]);
  const [threadId, setThreadId] = useState(() => {
    // Initialize thread ID from localStorage for this workspace
    return workspaceId ? getStoredThreadId(workspaceId) : '__default__';
  });
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [messageError, setMessageError] = useState(null);
  const currentMessageRef = useRef(null);
  // Refs to track content order and reasoning state during streaming
  const contentOrderCounterRef = useRef(0);
  const currentReasoningIdRef = useRef(null);
  // Track current tool call ID being processed
  const currentToolCallIdRef = useRef(null);
  // Track history loading state and message sources
  const historyLoadingRef = useRef(false);
  const historyMessagesRef = useRef(new Set()); // Track message IDs from history
  const newMessagesStartIndexRef = useRef(0); // Index where new messages start

  // Update thread ID in localStorage whenever it changes
  useEffect(() => {
    if (workspaceId && threadId && threadId !== '__default__') {
      setStoredThreadId(workspaceId, threadId);
    }
  }, [workspaceId, threadId]);

  // Reset thread ID when workspace changes
  useEffect(() => {
    if (workspaceId) {
      const storedThreadId = getStoredThreadId(workspaceId);
      setThreadId(storedThreadId);
      // Clear messages when switching workspaces
      setMessages([]);
      // Reset refs
      contentOrderCounterRef.current = 0;
      currentReasoningIdRef.current = null;
      currentToolCallIdRef.current = null;
      historyLoadingRef.current = false;
      historyMessagesRef.current.clear();
      newMessagesStartIndexRef.current = 0;
    }
  }, [workspaceId]);

  /**
   * Loads conversation history for the current workspace
   * Finds the thread_id for the workspace and replays the conversation
   */
  const loadConversationHistory = async () => {
    if (!workspaceId || historyLoadingRef.current) {
      return;
    }

    try {
      historyLoadingRef.current = true;
      setIsLoadingHistory(true);
      setMessageError(null);

      // Step 1: Get all conversations to find thread_id for this workspace
      console.log('[History] Loading conversations for workspace:', workspaceId);
      const conversationsData = await getConversations();
      console.log('[History] Conversations data:', conversationsData);
      const threads = conversationsData.threads || [];
      console.log('[History] Found threads:', threads.length);
      
      // Find thread for this workspace (assuming one thread per workspace)
      const workspaceThread = threads.find(
        (thread) => thread.workspace_id === workspaceId
      );

      console.log('[History] Workspace thread:', workspaceThread);

      if (!workspaceThread || !workspaceThread.thread_id) {
        // No history found for this workspace
        console.log('[History] No thread found for workspace');
        setIsLoadingHistory(false);
        historyLoadingRef.current = false;
        return;
      }

      const foundThreadId = workspaceThread.thread_id;
      console.log('[History] Found thread ID:', foundThreadId);
      
      // Update thread ID if different
      if (foundThreadId !== threadId && foundThreadId !== '__default__') {
        setThreadId(foundThreadId);
        setStoredThreadId(workspaceId, foundThreadId);
      }

      // Step 2: Replay conversation history
      console.log('[History] Starting replay for thread:', foundThreadId);
      // Track pairs being processed - use Map to handle multiple pairs
      const assistantMessagesByPair = new Map(); // Map<pair_index, assistantMessageId>
      const pairStateByPair = new Map(); // Map<pair_index, { contentOrderCounter, reasoningId, toolCallId }>

      await replayThreadHistory(foundThreadId, (event) => {
        // Debug: Log all events to see what we're receiving
        console.log('[History] Received event:', event);
        
        // History events come in SSE format with 'event' field indicating the type
        // Event types: 'user_message', 'message_chunk', 'credit_usage', 'replay_done'
        const eventType = event.event;
        const contentType = event.content_type;
        const hasRole = event.role !== undefined;
        const hasPairIndex = event.pair_index !== undefined;
        const hasContent = !!event.content;
        const hasThreadId = !!event.thread_id;
        
        console.log('[History] Event analysis:', {
          eventType,
          contentType,
          hasRole,
          role: event.role,
          hasPairIndex,
          pairIndex: event.pair_index,
          hasContent,
          hasThreadId,
          contentPreview: hasContent ? event.content.substring(0, 50) : null,
        });

        // Handle user_message events from history
        if (eventType === 'user_message' && event.content && hasPairIndex) {
          const pairIndex = event.pair_index;
          console.log('[History] Processing user message, pair_index:', pairIndex);
          
          // Check if this is a new pair (not already processed)
          if (!assistantMessagesByPair.has(pairIndex)) {
            // Initialize state for this pair
            pairStateByPair.set(pairIndex, {
              contentOrderCounter: 0,
              reasoningId: null,
              toolCallId: null,
            });

            // Create user message
            const currentUserMessageId = `history-user-${pairIndex}-${Date.now()}`;
            const userMessage = {
              id: currentUserMessageId,
              role: 'user',
              content: event.content,
              contentType: 'text',
              timestamp: event.timestamp ? new Date(event.timestamp) : new Date(),
              isHistory: true,
            };

            console.log('[History] Adding user message:', userMessage);
            setMessages((prev) => {
              // Insert history messages before new messages
              const insertIndex = newMessagesStartIndexRef.current;
              const newMessages = [
                ...prev.slice(0, insertIndex),
                userMessage,
                ...prev.slice(insertIndex),
              ];
              historyMessagesRef.current.add(currentUserMessageId);
              newMessagesStartIndexRef.current = insertIndex + 1;
              console.log('[History] Messages after adding user:', newMessages.length);
              return newMessages;
            });

            // Create assistant message placeholder
            const currentAssistantMessageId = `history-assistant-${pairIndex}-${Date.now()}`;
            assistantMessagesByPair.set(pairIndex, currentAssistantMessageId);
            
            const assistantMessage = {
              id: currentAssistantMessageId,
              role: 'assistant',
              content: '',
              contentType: 'text',
              timestamp: event.timestamp ? new Date(event.timestamp) : new Date(),
              isStreaming: false, // History messages are not streaming
              isHistory: true,
              contentSegments: [],
              reasoningProcesses: {},
              toolCallProcesses: {},
            };

            setMessages((prev) => {
              // Insert history messages before new messages
              const insertIndex = newMessagesStartIndexRef.current;
              const newMessages = [
                ...prev.slice(0, insertIndex),
                assistantMessage,
                ...prev.slice(insertIndex),
              ];
              historyMessagesRef.current.add(currentAssistantMessageId);
              newMessagesStartIndexRef.current = insertIndex + 1;
              return newMessages;
            });
          }
          return;
        }

        // Handle message_chunk events (assistant messages)
        // These are similar to live stream events but come with event='message_chunk'
        // Look up the assistant message by pair_index
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
            
            if (signalContent === 'start') {
              const reasoningId = `history-reasoning-${pairIndex}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
              pairState.reasoningId = reasoningId;
              pairState.contentOrderCounter++;
              const currentOrder = pairState.contentOrderCounter;

              setMessages((prev) =>
                prev.map((msg) => {
                  if (msg.id !== currentAssistantMessageId) return msg;

                  const newSegments = [
                    ...(msg.contentSegments || []),
                    {
                      type: 'reasoning',
                      reasoningId,
                      order: currentOrder,
                    },
                  ];

                  const newReasoningProcesses = {
                    ...(msg.reasoningProcesses || {}),
                    [reasoningId]: {
                      content: '',
                      isReasoning: false, // History: already complete
                      reasoningComplete: true,
                      order: currentOrder,
                    },
                  };

                  return {
                    ...msg,
                    contentSegments: newSegments,
                    reasoningProcesses: newReasoningProcesses,
                  };
                })
              );
              return;
            } else if (signalContent === 'complete') {
              if (pairState.reasoningId) {
                const reasoningId = pairState.reasoningId;
                setMessages((prev) =>
                  prev.map((msg) => {
                    if (msg.id !== currentAssistantMessageId) return msg;

                    const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
                    if (reasoningProcesses[reasoningId]) {
                      reasoningProcesses[reasoningId] = {
                        ...reasoningProcesses[reasoningId],
                        isReasoning: false,
                        reasoningComplete: true,
                      };
                    }

                    return {
                      ...msg,
                      reasoningProcesses,
                    };
                  })
                );
                pairState.reasoningId = null;
              }
              return;
            }
          }

          // Handle reasoning content
          if (contentType === 'reasoning' && event.content && pairState.reasoningId) {
            const reasoningId = pairState.reasoningId;
            setMessages((prev) =>
              prev.map((msg) => {
                if (msg.id !== currentAssistantMessageId) return msg;

                const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
                if (reasoningProcesses[reasoningId]) {
                  reasoningProcesses[reasoningId] = {
                    ...reasoningProcesses[reasoningId],
                    content: (reasoningProcesses[reasoningId].content || '') + event.content,
                    isReasoning: false,
                    reasoningComplete: true,
                  };
                }

                return {
                  ...msg,
                  reasoningProcesses,
                };
              })
            );
            return;
          }

          // Handle text content
          if (contentType === 'text' && event.content) {
            pairState.contentOrderCounter++;
            const currentOrder = pairState.contentOrderCounter;

            setMessages((prev) =>
              prev.map((msg) => {
                if (msg.id !== currentAssistantMessageId) return msg;

                const newSegments = [
                  ...(msg.contentSegments || []),
                  {
                    type: 'text',
                    content: event.content,
                    order: currentOrder,
                  },
                ];

                const accumulatedText = (msg.content || '') + event.content;

                return {
                  ...msg,
                  contentSegments: newSegments,
                  content: accumulatedText,
                  contentType: 'text',
                };
              })
            );
            return;
          }

          // Handle finish_reason (end of assistant message)
          if (event.finish_reason) {
            setMessages((prev) =>
              prev.map((msg) =>
                msg.id === currentAssistantMessageId
                  ? {
                      ...msg,
                      isStreaming: false,
                    }
                  : msg
              )
            );
            return;
          }
        }

        // Handle tool_call_chunks - filter out (same as live messages)
        if (eventType === 'tool_call_chunks') {
          /**
           * Filter out tool_call_chunks events - we don't process or display them in the UI.
           * We only handle tool_calls and tool_call_result events.
           */
          return;
        }

        // Handle artifact events - filter out
        if (eventType === 'artifact') {
          /**
           * Filter out artifact events - we don't process or display them in the UI.
           */
          return;
        }

        // Handle tool_calls events (separate event type, not inside message_chunk)
        if (eventType === 'tool_calls' && hasPairIndex) {
          /**
           * Handle tool_calls events - complete tool call information
           * This contains the tool name, arguments, and tool_call_id
           */
          const pairIndex = event.pair_index;
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received tool_calls for unknown pair_index:', pairIndex);
            return;
          }

          if (event.tool_calls && Array.isArray(event.tool_calls)) {
            event.tool_calls.forEach((toolCall) => {
              const toolCallId = toolCall.id;

              if (toolCallId) {
                pairState.contentOrderCounter++;
                const currentOrder = pairState.contentOrderCounter;

                setMessages((prev) =>
                  prev.map((msg) => {
                    if (msg.id !== currentAssistantMessageId) return msg;

                    const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
                    const contentSegments = [...(msg.contentSegments || [])];

                    // Create new tool call process if it doesn't exist
                    if (!toolCallProcesses[toolCallId]) {
                      contentSegments.push({
                        type: 'tool_call',
                        toolCallId,
                        order: currentOrder,
                      });

                      toolCallProcesses[toolCallId] = {
                        toolName: toolCall.name,
                        toolCall: toolCall,
                        toolCallResult: null,
                        isInProgress: false, // History: already complete
                        isComplete: false,
                        order: currentOrder,
                      };
                    } else {
                      // Update existing tool call process with complete tool call data
                      toolCallProcesses[toolCallId] = {
                        ...toolCallProcesses[toolCallId],
                        toolName: toolCall.name,
                        toolCall: toolCall,
                      };
                    }

                    return {
                      ...msg,
                      contentSegments,
                      toolCallProcesses,
                    };
                  })
                );
              }
            });
          }
          return;
        }

        // Handle tool_call_result events (separate event type, not inside message_chunk)
        if (eventType === 'tool_call_result' && hasPairIndex) {
          /**
           * Handle tool_call_result events - result of tool execution
           * This contains the tool_call_id and the result content
           */
          const pairIndex = event.pair_index;
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received tool_call_result for unknown pair_index:', pairIndex);
            return;
          }

          const toolCallId = event.tool_call_id;

          if (toolCallId) {
            setMessages((prev) =>
              prev.map((msg) => {
                if (msg.id !== currentAssistantMessageId) return msg;

                const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
                if (toolCallProcesses[toolCallId]) {
                  toolCallProcesses[toolCallId] = {
                    ...toolCallProcesses[toolCallId],
                    toolCallResult: {
                      content: event.content,
                      content_type: event.content_type,
                      tool_call_id: event.tool_call_id,
                    },
                    isInProgress: false,
                    isComplete: true,
                  };
                } else {
                  // If tool call process doesn't exist, create it (edge case)
                  pairState.contentOrderCounter++;
                  const currentOrder = pairState.contentOrderCounter;

                  const contentSegments = [
                    ...(msg.contentSegments || []),
                    {
                      type: 'tool_call',
                      toolCallId,
                      order: currentOrder,
                    },
                  ];

                  toolCallProcesses[toolCallId] = {
                    toolName: 'Unknown Tool',
                    toolCall: null,
                    toolCallResult: {
                      content: event.content,
                      content_type: event.content_type,
                      tool_call_id: event.tool_call_id,
                    },
                    isInProgress: false,
                    isComplete: true,
                    order: currentOrder,
                  };

                  return {
                    ...msg,
                    contentSegments,
                    toolCallProcesses,
                  };
                }

                return {
                  ...msg,
                  toolCallProcesses,
                };
              })
            );
          }
          return;
        }

        // Handle replay_done event (final event)
        if (eventType === 'replay_done') {
          // This is the final event, just update thread_id if needed
          if (event.thread_id && event.thread_id !== threadId && event.thread_id !== '__default__') {
            console.log('[History] Final thread_id event:', event.thread_id);
            setThreadId(event.thread_id);
            setStoredThreadId(workspaceId, event.thread_id);
          }
        } else if (eventType === 'credit_usage') {
          // credit_usage indicates the end of one conversation pair
          // But there may be more pairs after this, so we don't reset anything
          // Just log it for debugging
          console.log('[History] Credit usage event (end of pair):', event.pair_index);
        } else if (!eventType) {
          // Fallback: Handle events without event type (shouldn't happen with SSE)
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
            event,
            contentType,
            hasRole,
            role: event.role,
            hasPairIndex,
          });
        }
      });

      console.log('[History] Replay completed');
      setIsLoadingHistory(false);
      historyLoadingRef.current = false;
    } catch (error) {
      console.error('[History] Error loading conversation history:', error);
      console.error('[History] Error details:', error.stack);
      setMessageError(error.message || 'Failed to load conversation history');
      setIsLoadingHistory(false);
      historyLoadingRef.current = false;
    }
  };

  // Load history when workspace changes
  useEffect(() => {
    console.log('[History] useEffect triggered, workspaceId:', workspaceId);
    
    // Guard: Only load if we have a workspaceId and we're not already loading
    if (!workspaceId || historyLoadingRef.current) {
      console.log('[History] Skipping load:', { workspaceId, isLoading: historyLoadingRef.current });
      return;
    }
    
    console.log('[History] Calling loadConversationHistory');
    loadConversationHistory();
    
    // Cleanup: Cancel loading if workspace changes or component unmounts
    return () => {
      console.log('[History] Cleanup: canceling history load for workspace:', workspaceId);
      historyLoadingRef.current = false;
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId]);

  /**
   * Handles sending a message and streaming the response
   * 
   * @param {string} message - The user's message
   * @param {boolean} planMode - Whether to use plan mode
   */
  const handleSendMessage = async (message, planMode = false) => {
    if (!workspaceId || !message.trim() || isLoading) {
      return;
    }

    // Add user message to history
    const userMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: message,
      contentType: 'text', // User messages are always text
      timestamp: new Date(),
    };

    // Add user message after history messages
    setMessages((prev) => {
      const newMessages = [...prev, userMessage];
      // Update new messages start index if this is the first new message
      if (newMessagesStartIndexRef.current === prev.length) {
        newMessagesStartIndexRef.current = newMessages.length;
      }
      return newMessages;
    });
    setIsLoading(true);
    setMessageError(null);

    // Create assistant message placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    // Reset counters for this new message
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;
    
    const assistantMessage = {
      id: assistantMessageId,
      role: 'assistant',
      content: '',
      contentType: 'text', // Default content type for text messages
      timestamp: new Date(),
      isStreaming: true,
      // Content segments in chronological order
      contentSegments: [],
      // Reasoning processes indexed by reasoningId
      reasoningProcesses: {},
      // Tool call processes indexed by tool_call_id
      toolCallProcesses: {},
    };

    // Add assistant message after history messages
    setMessages((prev) => {
      const newMessages = [...prev, assistantMessage];
      // Update new messages start index
      newMessagesStartIndexRef.current = newMessages.length;
      return newMessages;
    });
    currentMessageRef.current = assistantMessageId;

    try {
      // Build message history for API
      const messageHistory = messages.map((msg) => ({
        role: msg.role,
        content: msg.content,
      }));

      await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        messageHistory,
        planMode,
        (event) => {
          const eventType = event.event || 'message_chunk';
          
          // Update thread_id if provided in the event
          // This will automatically save to localStorage via useEffect
          if (event.thread_id && event.thread_id !== threadId && event.thread_id !== '__default__') {
            setThreadId(event.thread_id);
            // Also save immediately to ensure persistence
            setStoredThreadId(workspaceId, event.thread_id);
          }
          
          // Handle different event types
          if (eventType === 'message_chunk') {
            const contentType = event.content_type || 'text';
            
            // Handle reasoning_signal events
            if (contentType === 'reasoning_signal') {
              const signalContent = event.content || '';
              
              if (signalContent === 'start') {
                // Reasoning process has started - create new reasoning process
                const reasoningId = `reasoning-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
                currentReasoningIdRef.current = reasoningId;
                contentOrderCounterRef.current++;
                const currentOrder = contentOrderCounterRef.current;
                
                setMessages((prev) =>
                  prev.map((msg) => {
                    if (msg.id !== assistantMessageId) return msg;
                    
                    // Add reasoning start segment
                    const newSegments = [
                      ...(msg.contentSegments || []),
                      {
                        type: 'reasoning',
                        reasoningId,
                        order: currentOrder,
                      },
                    ];
                    
                    // Initialize reasoning process
                    const newReasoningProcesses = {
                      ...(msg.reasoningProcesses || {}),
                      [reasoningId]: {
                        content: '',
                        isReasoning: true,
                        reasoningComplete: false,
                        order: currentOrder,
                      },
                    };
                    
                    return {
                      ...msg,
                      contentSegments: newSegments,
                      reasoningProcesses: newReasoningProcesses,
                    };
                  })
                );
                return;
              } else if (signalContent === 'complete') {
                // Reasoning process has completed
                if (currentReasoningIdRef.current) {
                  const reasoningId = currentReasoningIdRef.current;
                  setMessages((prev) =>
                    prev.map((msg) => {
                      if (msg.id !== assistantMessageId) return msg;
                      
                      const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
                      if (reasoningProcesses[reasoningId]) {
                        reasoningProcesses[reasoningId] = {
                          ...reasoningProcesses[reasoningId],
                          isReasoning: false,
                          reasoningComplete: true,
                        };
                      }
                      
                      return {
                        ...msg,
                        reasoningProcesses,
                      };
                    })
                  );
                  currentReasoningIdRef.current = null;
                }
                return;
              }
            }
            
            // Handle reasoning content chunks
            if (contentType === 'reasoning' && event.content) {
              // Accumulate reasoning content for current reasoning process
              if (currentReasoningIdRef.current) {
                const reasoningId = currentReasoningIdRef.current;
                setMessages((prev) =>
                  prev.map((msg) => {
                    if (msg.id !== assistantMessageId) return msg;
                    
                    const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
                    if (reasoningProcesses[reasoningId]) {
                      reasoningProcesses[reasoningId] = {
                        ...reasoningProcesses[reasoningId],
                        content: (reasoningProcesses[reasoningId].content || '') + event.content,
                        isReasoning: true,
                      };
                    }
                    
                    return {
                      ...msg,
                      reasoningProcesses,
                    };
                  })
                );
              }
              return;
            }
            
            // Handle text content chunks
            if (contentType === 'text') {
              // Handle finish_reason
              if (event.finish_reason) {
                if (event.finish_reason === 'tool_calls' && !event.content) {
                  // Message is requesting tool calls, don't mark as complete yet
                  // Tool calls will be handled by tool_calls event handler
                  return;
                } else if (!event.content) {
                  // This is a metadata chunk with finish_reason but no content
                  // Mark message as complete but don't update content
                  setMessages((prev) =>
                    prev.map((msg) =>
                      msg.id === assistantMessageId
                        ? {
                            ...msg,
                            isStreaming: false,
                          }
                        : msg
                    )
                  );
                  return;
                }
                // If finish_reason exists but content also exists, continue to process content
              }
              
              // Process text content chunks
              if (event.content) {
                contentOrderCounterRef.current++;
                const currentOrder = contentOrderCounterRef.current;
                setMessages((prev) =>
                  prev.map((msg) => {
                    if (msg.id !== assistantMessageId) return msg;
                    
                    // Add text segment
                    const newSegments = [
                      ...(msg.contentSegments || []),
                      {
                        type: 'text',
                        content: event.content,
                        order: currentOrder,
                      },
                    ];
                    
                    // Also maintain backward compatibility with content field
                    const accumulatedText = (msg.content || '') + event.content;
                    
                    return {
                      ...msg,
                      contentSegments: newSegments,
                      content: accumulatedText, // Keep for backward compatibility
                      contentType: 'text',
                      isStreaming: true,
                    };
                  })
                );
              } else if (event.finish_reason) {
                // Message is complete (finish_reason present with no content means end of stream)
                setMessages((prev) =>
                  prev.map((msg) =>
                    msg.id === assistantMessageId
                      ? {
                          ...msg,
                          isStreaming: false,
                        }
                      : msg
                  )
                );
              }
              return;
            }
            
            // Skip other content types or unknown content types
            return;
          } else if (eventType === 'error' || event.error) {
            // Handle errors
            const errorMessage = event.error || event.message || 'An error occurred while processing your request.';
            setMessageError(errorMessage);
            setMessages((prev) =>
              prev.map((msg) =>
                msg.id === assistantMessageId
                  ? {
                      ...msg,
                      content: msg.content || errorMessage,
                      contentType: 'text',
                      isStreaming: false,
                      error: true,
                    }
                  : msg
              )
            );
          } else if (eventType === 'tool_call_chunks') {
            /**
             * Filter out tool_call_chunks events - we don't process or display them in the UI.
             * We only handle tool_calls and tool_call_result events.
             */
            return;
          } else if (eventType === 'tool_calls') {
            /**
             * Handle tool_calls events - complete tool call information
             * This contains the tool name, arguments, and tool_call_id.
             * 
             * Note: tool_calls array items correspond to tool_call_chunks by array index.
             * We need to match them and update the tool_call_id.
             */
            if (event.tool_calls && Array.isArray(event.tool_calls)) {
              event.tool_calls.forEach((toolCall, arrayIndex) => {
                const toolCallId = toolCall.id;
                
                if (toolCallId) {
                  setMessages((prev) =>
                    prev.map((msg) => {
                      if (msg.id !== assistantMessageId) return msg;
                      
                      const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
                      const contentSegments = [...(msg.contentSegments || [])];
                      
                      // Create new tool call process if it doesn't exist
                      if (!toolCallProcesses[toolCallId]) {
                        // Create new tool call process if it doesn't exist
                        contentOrderCounterRef.current++;
                        const currentOrder = contentOrderCounterRef.current;
                        
                        contentSegments.push({
                          type: 'tool_call',
                          toolCallId,
                          order: currentOrder,
                        });
                        
                        toolCallProcesses[toolCallId] = {
                          toolName: toolCall.name,
                          toolCall: toolCall,
                          toolCallResult: null,
                          isInProgress: true,
                          isComplete: false,
                          order: currentOrder,
                        };
                      } else {
                        // Update existing tool call process with complete tool call data
                        toolCallProcesses[toolCallId] = {
                          ...toolCallProcesses[toolCallId],
                          toolName: toolCall.name,
                          toolCall: toolCall,
                          isInProgress: true,
                        };
                      }
                      
                      return {
                        ...msg,
                        contentSegments,
                        toolCallProcesses,
                      };
                    })
                  );
                  
                  // Check if this is the last tool call (finish_reason indicates tool calls are done)
                  if (event.finish_reason === 'tool_calls') {
                    // Mark all tool calls as waiting for results (not complete yet, but not actively calling)
                    setMessages((prev) =>
                      prev.map((msg) => {
                        if (msg.id !== assistantMessageId) return msg;
                        
                        const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
                        Object.keys(toolCallProcesses).forEach((id) => {
                          toolCallProcesses[id] = {
                            ...toolCallProcesses[id],
                            isInProgress: false, // Tool call sent, waiting for result
                          };
                        });
                        
                        return {
                          ...msg,
                          toolCallProcesses,
                        };
                      })
                    );
                  }
                }
              });
            }
          } else if (eventType === 'tool_call_result') {
            /**
             * Handle tool_call_result events - result of tool execution
             * This contains the tool_call_id and the result content
             */
            const toolCallId = event.tool_call_id;
            
            if (toolCallId) {
              setMessages((prev) =>
                prev.map((msg) => {
                  if (msg.id !== assistantMessageId) return msg;
                  
                  const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
                  if (toolCallProcesses[toolCallId]) {
                    toolCallProcesses[toolCallId] = {
                      ...toolCallProcesses[toolCallId],
                      toolCallResult: {
                        content: event.content,
                        content_type: event.content_type,
                        tool_call_id: event.tool_call_id,
                      },
                      isInProgress: false,
                      isComplete: true,
                    };
                  } else {
                    // If tool call process doesn't exist, create it (edge case)
                    contentOrderCounterRef.current++;
                    const currentOrder = contentOrderCounterRef.current;
                    
                    const newSegments = [
                      ...(msg.contentSegments || []),
                      {
                        type: 'tool_call',
                        toolCallId,
                        order: currentOrder,
                      },
                    ];
                    
                    toolCallProcesses[toolCallId] = {
                      toolName: 'Unknown Tool',
                      toolCall: null,
                      toolCallResult: {
                        content: event.content,
                        content_type: event.content_type,
                        tool_call_id: event.tool_call_id,
                      },
                      isInProgress: false,
                      isComplete: true,
                      order: currentOrder,
                    };
                    
                    return {
                      ...msg,
                      contentSegments: newSegments,
                      toolCallProcesses,
                    };
                  }
                  
                  return {
                    ...msg,
                    toolCallProcesses,
                  };
                })
              );
              
              // Reset current tool call ID after result is received
              if (currentToolCallIdRef.current === toolCallId) {
                currentToolCallIdRef.current = null;
              }
            }
          }
        }
      );

      // Mark message as complete
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === assistantMessageId
            ? {
                ...msg,
                isStreaming: false,
              }
            : msg
        )
      );
    } catch (err) {
      console.error('Error sending message:', err);
      setMessageError(err.message || 'Failed to send message');
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === assistantMessageId
            ? {
                ...msg,
                content: msg.content || 'Failed to send message. Please try again.',
                isStreaming: false,
                error: true,
              }
            : msg
        )
      );
    } finally {
      setIsLoading(false);
      currentMessageRef.current = null;
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
