/**
 * Hook for managing MarketView flash mode chat
 * Simplified version of ChatAgent's useChatMessages for one-time flash mode conversations
 *
 * Features:
 * - Flash mode only (agent_mode: "flash")
 * - No history loading (always starts fresh)
 * - Threads persist across navigation (stored in flash workspace)
 * - Simplified message parsing (no subagents, no todo lists)
 */

import { useState, useRef, useEffect, useCallback } from 'react';
import { buildRateLimitError, type StructuredError } from '@/utils/rateLimitError';
import { sendFlashChatMessage } from '../utils/api';

// --- Local message types (simplified subset of ChatAgent types) ---

interface ContentSegment {
  type: 'text' | 'reasoning' | 'tool_call';
  content?: string;
  order: number;
  reasoningId?: string;
  toolCallId?: string;
}

interface ReasoningProcess {
  content: string;
  isReasoning: boolean;
  reasoningComplete: boolean;
  order: number;
  reasoningTitle?: string | null;
  _completedAt?: number;
}

interface ToolCallResult {
  content: string | unknown;
  content_type: string;
  tool_call_id: string;
  artifact?: Record<string, unknown>;
}

interface ToolCallProcess {
  toolName: string;
  toolCall: Record<string, unknown> | null;
  toolCallResult: ToolCallResult | null;
  isInProgress: boolean;
  isComplete: boolean;
  isFailed?: boolean;
  order: number;
}

interface AttachmentMeta {
  name: string;
  type: string;
  size?: number;
  [key: string]: unknown;
}

export interface MarketChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  contentType: string;
  timestamp: string;
  isStreaming?: boolean;
  error?: string;
  attachments?: AttachmentMeta[];
  contentSegments?: ContentSegment[];
  reasoningProcesses?: Record<string, ReasoningProcess>;
  toolCallProcesses?: Record<string, ToolCallProcess>;
}

export interface UseMarketChatReturn {
  messages: MarketChatMessage[];
  isLoading: boolean;
  error: string | StructuredError | null;
  handleSendMessage: (message: string, additionalContext?: unknown, attachmentMeta?: AttachmentMeta[] | null, model?: string | null) => Promise<void>;
}

type MessageUpdater = (messages: MarketChatMessage[]) => MarketChatMessage[];

/**
 * Creates a user message object
 */
function createUserMessage(content: string, attachments: AttachmentMeta[] | null = null): MarketChatMessage {
  const msg: MarketChatMessage = {
    id: `user-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    role: 'user',
    content: content.trim(),
    contentType: 'text',
    timestamp: new Date().toISOString(),
  };
  if (attachments && attachments.length > 0) {
    msg.attachments = attachments;
  }
  return msg;
}

/**
 * Creates an assistant message placeholder
 */
function createAssistantMessage(id: string): MarketChatMessage {
  return {
    id,
    role: 'assistant',
    content: '',
    contentType: 'text',
    isStreaming: true,
    timestamp: new Date().toISOString(),
    contentSegments: [],
    reasoningProcesses: {},
    toolCallProcesses: {},
  };
}

/**
 * Appends a message to the messages array
 */
function appendMessage(messages: MarketChatMessage[], newMessage: MarketChatMessage): MarketChatMessage[] {
  return [...messages, newMessage];
}

// Batch flush interval (ms) — SSE events are buffered and flushed at this rate
const BATCH_FLUSH_INTERVAL_MS = 150;

export function useMarketChat(): UseMarketChatReturn {
  const [messages, setMessages] = useState<MarketChatMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | StructuredError | null>(null);
  const threadIdRef = useRef('__default__');
  const contentOrderCounterRef = useRef(0);
  const currentReasoningIdRef = useRef<string | null>(null);

  // --- Batching infrastructure ---
  // Pending updates accumulate here; flushed on a timer
  const pendingUpdatesRef = useRef<MessageUpdater[]>([]);
  const flushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  /**
   * Queue a message-transform function and schedule a batched flush.
   * Each `updater` is a function (messages: MarketChatMessage[]) => MarketChatMessage[]
   */
  const queueUpdate = useCallback((updater: MessageUpdater): void => {
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
  const flushUpdates = useCallback((): void => {
    if (flushTimerRef.current) clearTimeout(flushTimerRef.current);
    flushTimerRef.current = null;
    const updates = pendingUpdatesRef.current;
    if (updates.length === 0) return;
    pendingUpdatesRef.current = [];
    setMessages((prev) => updates.reduce((msgs, fn) => fn(msgs), prev));
  }, []);

  /**
   * Handles text message chunk events with chronological ordering
   */
  function handleMessageChunk({ assistantMessageId, content }: { assistantMessageId: string; content: string }): boolean {
    if (!assistantMessageId || !content) return false;

    contentOrderCounterRef.current++;
    const currentOrder = contentOrderCounterRef.current;

    queueUpdate((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...(msg.contentSegments || []),
          { type: 'text' as const, content, order: currentOrder },
        ];

        return {
          ...msg,
          content: (msg.content || '') + content,
          contentSegments: newSegments,
        };
      })
    );
    return true;
  }

  /**
   * Handles reasoning signal events
   */
  function handleReasoningSignal({ assistantMessageId, signalContent }: { assistantMessageId: string; signalContent: string }): boolean {
    if (signalContent === 'start') {
      const reasoningId = `reasoning-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      currentReasoningIdRef.current = reasoningId;
      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;

      flushUpdates();
      setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const newSegments: ContentSegment[] = [
            ...(msg.contentSegments || []),
            {
              type: 'reasoning' as const,
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
        flushUpdates();
        setMessages((prev) =>
          prev.map((msg) => {
            if (msg.id !== assistantMessageId) return msg;

            const reasoningProcesses = { ...(msg.reasoningProcesses || {}) };
            if (reasoningProcesses[reasoningId]) {
              reasoningProcesses[reasoningId] = {
                ...reasoningProcesses[reasoningId],
                isReasoning: false,
                reasoningComplete: true,
                reasoningTitle: null,
                _completedAt: Date.now(),
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
  function handleReasoningContent({ assistantMessageId, content }: { assistantMessageId: string; content: string }): boolean {
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
   * Handles tool_calls events
   */
  function handleToolCalls({ assistantMessageId, toolCalls }: { assistantMessageId: string; toolCalls: Array<Record<string, unknown>> }): boolean {
    if (!assistantMessageId || !toolCalls || toolCalls.length === 0) return false;

    for (const toolCall of toolCalls) {
      const toolCallId = (toolCall.id as string) || `tc-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
      const toolName = (toolCall.name as string) || 'unknown';

      contentOrderCounterRef.current++;
      const currentOrder = contentOrderCounterRef.current;

      queueUpdate((prev) =>
        prev.map((msg) => {
          if (msg.id !== assistantMessageId) return msg;

          const newSegments = [
            ...(msg.contentSegments || []),
            { type: 'tool_call' as const, toolCallId, order: currentOrder },
          ];

          const newToolCallProcesses = {
            ...(msg.toolCallProcesses || {}),
            [toolCallId]: {
              toolName,
              toolCall: toolCall,
              toolCallResult: null,
              isInProgress: true,
              isComplete: false,
              order: currentOrder,
            },
          };

          return {
            ...msg,
            contentSegments: newSegments,
            toolCallProcesses: newToolCallProcesses,
          };
        })
      );
    }
    return true;
  }

  /**
   * Handles tool_call_result events
   */
  function handleToolCallResult({ assistantMessageId, toolCallId, result }: { assistantMessageId: string; toolCallId: string; result: ToolCallResult }): boolean {
    if (!assistantMessageId || !toolCallId) return false;

    const isFailed = typeof result.content === 'string' && (result.content as string).startsWith('ERROR');

    queueUpdate((prev) =>
      prev.map((msg) => {
        if (msg.id !== assistantMessageId) return msg;

        const toolCallProcesses = { ...(msg.toolCallProcesses || {}) };
        if (toolCallProcesses[toolCallId]) {
          toolCallProcesses[toolCallId] = {
            ...toolCallProcesses[toolCallId],
            toolCallResult: result,
            isInProgress: false,
            isComplete: true,
            isFailed,
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
  const handleSendMessage = async (message: string, additionalContext: unknown = null, attachmentMeta: AttachmentMeta[] | null = null, model: string | null = null): Promise<void> => {
    if (!message.trim() || isLoading) {
      return;
    }

    // Create and add user message (with attachment metadata for display)
    const userMessage = createUserMessage(message, attachmentMeta);
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
        (event: Record<string, unknown>) => {
          hasReceivedEvents = true;
          const eventType = (event.event as string) || 'message_chunk';
          
          if (process.env.NODE_ENV === 'development') {
            console.log('[MarketChat] Received event:', eventType, event);
          }

          // Update thread_id if provided in the event
          if (event.thread_id && event.thread_id !== threadIdRef.current && event.thread_id !== '__default__') {
            threadIdRef.current = event.thread_id as string;
          }

          // Handle different event types
          if (eventType === 'message_chunk') {
            const contentType = (event.content_type as string) || 'text';

            // Handle reasoning_signal
            if (contentType === 'reasoning_signal') {
              const signalContent = (event.content as string) || '';
              handleReasoningSignal({
                assistantMessageId,
                signalContent,
              });
            }
            // Handle reasoning content
            else if (contentType === 'reasoning' && event.content) {
              handleReasoningContent({
                assistantMessageId,
                content: event.content as string,
              });
            }
            // Handle text content
            else if (contentType === 'text' && event.content) {
              handleMessageChunk({
                assistantMessageId,
                content: event.content as string,
              });
            }
          } else if (eventType === 'tool_calls') {
            const toolCalls = (event.tool_calls || []) as Array<Record<string, unknown>>;
            handleToolCalls({
              assistantMessageId,
              toolCalls,
            });
          } else if (eventType === 'tool_call_result') {
            const toolCallId = event.tool_call_id as string;
            if (toolCallId) {
              handleToolCallResult({
                assistantMessageId,
                toolCallId,
                result: {
                  content: (event.content as string) || '',
                  content_type: (event.content_type as string) || 'text',
                  tool_call_id: toolCallId,
                  artifact: event.artifact as Record<string, unknown> | undefined,
                },
              });
            }
          } else if (eventType === 'error') {
            hasReceivedError = true;
            const errorMessage = (event.error as string) || (event.message as string) || 'An error occurred';
            console.error('[MarketChat] Server error event:', errorMessage, event);

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
        additionalContext,
        model
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
          console.log('[MarketChat] Stream completed with error');
        } else {
          console.log('[MarketChat] Stream completed successfully');
        }
      }
    } catch (err: unknown) {
      console.error('[MarketChat] Error sending message:', err);

      // Flush any remaining batched updates
      flushUpdates();

      const streamErr = err as { status?: number; rateLimitInfo?: Record<string, unknown>; errorInfo?: Record<string, unknown>; message?: string };

      // Handle rate limit (429) — show friendly message and remove empty assistant placeholder
      if (streamErr.status === 429) {
        const info = streamErr.rateLimitInfo || {};
        const accountUrl = (import.meta.env.VITE_ACCOUNT_URL as string | undefined) || '/account';
        const structured = buildRateLimitError(info as Record<string, unknown>, accountUrl);
        setError(structured);
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
          // Build structured error with link when backend provides one
          const errorInfo = streamErr.errorInfo;
          if (errorInfo?.link) {
            setError({
              message: (errorInfo.message as string) || streamErr.message || 'An error occurred.',
              link: errorInfo.link as { url: string; label: string },
            });
          } else if (streamErr.status === 403) {
            setError({
              message: streamErr.message || 'Access denied.',
              link: { url: '/setup/method', label: 'Configure providers' },
            });
          } else {
            setError(streamErr.message || 'Failed to send message');
          }
          setMessages((prev) =>
            prev.map((msg) => {
              if (msg.id !== assistantMessageId) return msg;
              return {
                ...msg,
                error: streamErr.message || 'Failed to send message',
              };
            })
          );
        } else {
          if (process.env.NODE_ENV === 'development') {
            console.warn('[MarketChat] Stream interrupted but received partial data, marking as complete');
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
      if (flushTimerRef.current) clearTimeout(flushTimerRef.current);
    };
  }, []);

  return {
    messages,
    isLoading,
    error,
    handleSendMessage,
  };
}
