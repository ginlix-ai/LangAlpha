/**
 * Hook for managing TradingCenter flash mode chat
 * Simplified version of ChatAgent's useChatMessages for one-time flash mode conversations
 * 
 * Features:
 * - Flash mode only (agent_mode: "flash")
 * - No history loading (always starts fresh)
 * - Threads persist across navigation (stored in flash workspace)
 * - Simplified message parsing (no subagents, no todo lists)
 */

import { useState, useRef, useEffect, useCallback } from 'react';
import { sendFlashChatMessage } from '../utils/api';


/**
 * Creates a user message object
 */
function createUserMessage(content) {
  return {
    id: `user-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    role: 'user',
    content: content.trim(),
    contentType: 'text',
    timestamp: new Date().toISOString(),
  };
}

/**
 * Creates an assistant message placeholder
 */
function createAssistantMessage(id) {
  return {
    id,
    role: 'assistant',
    content: '',
    contentType: 'text',
    isStreaming: true,
    timestamp: new Date().toISOString(),
    contentSegments: [],
    reasoningProcesses: {},
  };
}

/**
 * Appends a message to the messages array
 */
function appendMessage(messages, newMessage) {
  return [...messages, newMessage];
}

/**
 * Hook for managing TradingCenter flash mode chat
 * @returns {Object} Chat state and handlers
 */
// Batch flush interval (ms) — SSE events are buffered and flushed at this rate
const BATCH_FLUSH_INTERVAL_MS = 150;

export function useTradingChat() {
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const threadIdRef = useRef('__default__');
  const contentOrderCounterRef = useRef(0);
  const currentReasoningIdRef = useRef(null);

  // --- Batching infrastructure ---
  // Pending updates accumulate here; flushed on a timer
  const pendingUpdatesRef = useRef([]);
  const flushTimerRef = useRef(null);

  /**
   * Queue a message-transform function and schedule a batched flush.
   * Each `updater` is a function (messages: Message[]) => Message[]
   */
  const queueUpdate = useCallback((updater) => {
    pendingUpdatesRef.current.push(updater);

    if (!flushTimerRef.current) {
      flushTimerRef.current = setTimeout(() => {
        flushTimerRef.current = null;
        const updates = pendingUpdatesRef.current;
        if (updates.length === 0) return;
        pendingUpdatesRef.current = [];
        // Apply all queued transforms in a single setState
        setMessages((prev) => updates.reduce((msgs, fn) => fn(msgs), prev));
      }, BATCH_FLUSH_INTERVAL_MS);
    }
  }, []);

  /**
   * Flush any remaining queued updates immediately (used at stream end).
   */
  const flushUpdates = useCallback(() => {
    clearTimeout(flushTimerRef.current);
    flushTimerRef.current = null;
    const updates = pendingUpdatesRef.current;
    if (updates.length === 0) return;
    pendingUpdatesRef.current = [];
    setMessages((prev) => updates.reduce((msgs, fn) => fn(msgs), prev));
  }, []);

  /**
   * Handles text message chunk events with chronological ordering
   */
  function handleMessageChunk({ assistantMessageId, content }) {
    if (!assistantMessageId || !content) return false;

    queueUpdate((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        // Find or create text content segment
        const segments = [...(msg.contentSegments || [])];
        let textSegment = segments.find((s) => s.type === 'text');

        if (!textSegment) {
          contentOrderCounterRef.current++;
          textSegment = {
            type: 'text',
            order: contentOrderCounterRef.current,
            content: '',
          };
          segments.push(textSegment);
        }

        // Accumulate content in the segment
        textSegment.content = (textSegment.content || '') + content;

        return {
          ...msg,
          content: (msg.content || '') + content,
          contentSegments: segments,
        };
      })
    );
    return true;
  }

  /**
   * Handles reasoning signal events
   */
  function handleReasoningSignal({ assistantMessageId, signalContent }) {
    if (signalContent === 'start') {
      const reasoningId = `reasoning-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      currentReasoningIdRef.current = reasoningId;
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;

      queueUpdate((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

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
      return true;
    } else if (signalContent === 'complete') {
      if (currentReasoningIdRef.current) {
        const reasoningId = currentReasoningIdRef.current;
        queueUpdate((prev) =>
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
      return true;
    }
    return false;
  }

  /**
   * Handles reasoning content chunks
   */
  function handleReasoningContent({ assistantMessageId, content }) {
    if (currentReasoningIdRef.current && content) {
      const reasoningId = currentReasoningIdRef.current;
      queueUpdate((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
          if (reasoningProcesses[reasoningId]) {
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              content: (reasoningProcesses[reasoningId].content || '') + content,
            };
          }

          return {
            ...msg,
            reasoningProcesses,
          };
        })
      );
      return true;
    }
    return false;
  }

  /**
   * Handles tool calls events
   */
  function handleToolCalls({ assistantMessageId, toolCalls, finishReason }) {
    if (!toolCalls || !Array.isArray(toolCalls)) {
      return false;
    }

    toolCalls.forEach((toolCall) => {
      const toolCallId = toolCall.id;

      if (toolCallId) {
        queueUpdate((prev) =>
          prev.map((msg) => {
            if (msg.id !== assistantMessageId) return msg;

            const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
            const contentSegments = [...(msg.contentSegments || [])];

            if (!toolCallProcesses[toolCallId]) {
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
      }
    });

    if (finishReason === 'tool_calls') {
      queueUpdate((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
          Object.keys(toolCallProcesses).forEach((id) => {
            toolCallProcesses[id] = {
              ...toolCallProcesses[id],
              isInProgress: false,
            };
          });

          return {
            ...msg,
            toolCallProcesses,
          };
        })
      );
    }

    return true;
  }

  /**
   * Handles tool call result events
   */
  function handleToolCallResult({ assistantMessageId, toolCallId, result }) {
    if (!toolCallId) {
      return false;
    }

    queueUpdate((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };

        const resultContent = result.content || '';
        const isFailed = /failed|error|Error|ERROR|exception|Exception|failed:|error:/i.test(resultContent);

        if (toolCallProcesses[toolCallId]) {
          toolCallProcesses[toolCallId] = {
            ...toolCallProcesses[toolCallId],
            toolCallResult: {
              content: result.content,
              content_type: result.content_type,
              tool_call_id: result.tool_call_id,
            },
            isInProgress: false,
            isComplete: true,
            isFailed: isFailed,
          };
        } else {
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
              content: result.content,
              content_type: result.content_type,
              tool_call_id: result.tool_call_id,
            },
            isInProgress: false,
            isComplete: true,
            isFailed: isFailed,
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

    return true;
  }

  /**
   * Handles sending a message in flash mode
   */
  const handleSendMessage = async (message, additionalContext = null) => {
    if (!message.trim() || isLoading) {
      return;
    }

    // Create and add user message
    const userMessage = createUserMessage(message);
    setMessages((prev) => appendMessage(prev, userMessage));

    setIsLoading(true);
    setError(null);

    // Create assistant message placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;

    const assistantMessage = createAssistantMessage(assistantMessageId);
    setMessages((prev) => appendMessage(prev, assistantMessage));

    let hasReceivedEvents = false;
    let hasReceivedError = false;

    try {
      await sendFlashChatMessage(
        message,
        threadIdRef.current,
        (event) => {
          hasReceivedEvents = true;
          const eventType = event.event || 'message_chunk';
          
          if (process.env.NODE_ENV === 'development') {
            console.log('[TradingChat] Received event:', eventType, event);
          }

          // Update thread_id if provided in the event
          if (event.thread_id && event.thread_id !== threadIdRef.current && event.thread_id !== '__default__') {
            threadIdRef.current = event.thread_id;
          }

          // Handle different event types
          if (eventType === 'message_chunk') {
            const contentType = event.content_type || 'text';
            
            // Handle reasoning_signal
            if (contentType === 'reasoning_signal') {
              const signalContent = event.content || '';
              handleReasoningSignal({
                assistantMessageId,
                signalContent,
              });
            }
            // Handle reasoning content
            else if (contentType === 'reasoning' && event.content) {
              handleReasoningContent({
                assistantMessageId,
                content: event.content,
              });
            }
            // Handle text content
            else if (contentType === 'text' && event.content) {
              handleMessageChunk({
                assistantMessageId,
                content: event.content,
              });
            }
          } else if (eventType === 'error') {
            hasReceivedError = true;
            const errorMessage = event.error || event.message || 'An error occurred';
            console.error('[TradingChat] Server error event:', errorMessage, event);

            // Flush pending batched updates before setting error
            flushUpdates();

            // Set error state
            setError(errorMessage);
            setIsLoading(false);

            // Update message with error
            setMessages((prev) =>
              prev.map((msg) => {
                if (msg.id !== assistantMessageId) return msg;
                return {
                  ...msg,
                  error: errorMessage,
                  isStreaming: false,
                };
              })
            );
          }
        },
        'en-US',
        'America/New_York',
        additionalContext
      );

      // Flush any remaining batched updates
      flushUpdates();

      // Mark message as complete (only if no error was received)
      if (!hasReceivedError) {
        setMessages((prev) =>
          prev.map((msg) => {
            if (msg.id !== assistantMessageId) return msg;
            return {
              ...msg,
              isStreaming: false,
            };
          })
        );
      }

      // Always stop loading
      setIsLoading(false);
      
      if (process.env.NODE_ENV === 'development') {
        if (hasReceivedError) {
          console.log('[TradingChat] Stream completed with error');
        } else {
          console.log('[TradingChat] Stream completed successfully');
        }
      }
    } catch (err) {
      console.error('[TradingChat] Error sending message:', err);

      // Flush any remaining batched updates
      flushUpdates();

      // Handle rate limit (429) — show friendly message and remove empty assistant placeholder
      if (err.status === 429) {
        const info = err.rateLimitInfo || {};
        const limitMsg = info.type === 'credit_limit'
          ? `Daily credit limit reached (${info.used_credits}/${info.credit_limit} credits). Resets at midnight UTC.`
          : info.type === 'burst_limit'
            ? 'Too many concurrent requests. Please wait a moment.'
            : info.message || 'Rate limit exceeded. Please try again later.';
        setError(limitMsg);
        // Remove the empty assistant placeholder — no content to show
        setMessages((prev) => prev.filter((msg) => msg.id !== assistantMessageId));
      } else {
        // Mark message as not streaming
        setMessages((prev) =>
          prev.map((msg) => {
            if (msg.id !== assistantMessageId) return msg;
            return {
              ...msg,
              isStreaming: false,
            };
          })
        );

        // Only set error if we haven't received any events
        if (!hasReceivedEvents) {
          setError(err.message || 'Failed to send message');
          setMessages((prev) =>
            prev.map((msg) => {
              if (msg.id !== assistantMessageId) return msg;
              return {
                ...msg,
                error: err.message || 'Failed to send message',
              };
            })
          );
        } else {
          if (process.env.NODE_ENV === 'development') {
            console.warn('[TradingChat] Stream interrupted but received partial data, marking as complete');
          }
        }
      }
    } finally {
      setIsLoading(false);
    }
  };

  // Cleanup: clear flush timer on unmount
  useEffect(() => {
    return () => {
      clearTimeout(flushTimerRef.current);
    };
  }, []);

  return {
    messages,
    isLoading,
    error,
    handleSendMessage,
  };
}
