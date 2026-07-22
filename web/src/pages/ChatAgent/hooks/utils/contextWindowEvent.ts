/**
 * context_window SSE event handling (compaction/offload notifications + token
 * usage). Extracted verbatim from useChatMessages module scope (W1).
 */

import type { SSEEvent, ContextWindowCallbacks, TokenUsage } from '../../session/types';
import type { AssistantMessage } from '@/types/chat';
import { updateMessage } from './messageHelpers';

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
function handleContextWindowEvent(event: SSEEvent, { getMsgId, nextOrder, setMessages, setTokenUsage, setIsCompacting, insertNotification, t, offloadBatch }: ContextWindowCallbacks): void {
  const action = event.action;

  if (action === 'token_usage') {
    const callInput = event.input_tokens || 0;
    const callOutput = event.output_tokens || 0;
    setTokenUsage((prev: TokenUsage | null) => ({
      totalInput: (prev?.totalInput || 0) + callInput,
      totalOutput: (prev?.totalOutput || 0) + callOutput,
      lastOutput: callOutput,
      total: event.total_tokens || 0,
      threshold: event.threshold || prev?.threshold || 0,
    }));
    return;
  }

  if (action === 'summarize') {
    // SSE action value "summarize" preserved as wire protocol; the UI surfaces
    // this as context compaction.
    if (setIsCompacting && event.signal === 'start') {
      setIsCompacting('summarize');
      return;
    }
    if (setIsCompacting) setIsCompacting(false);
    if (event.signal === 'complete') {
      const text = t('chat.compactedNotification', { from: event.original_message_count });
      const detail = (event.summary_text as string | undefined) || undefined;
      const msgId = getMsgId();
      if (msgId) {
        const order = nextOrder();
        setMessages((prev) => updateMessage(prev,msgId, (msg) => {
          if (msg.role !== 'assistant') return msg;
          const aMsg = msg as AssistantMessage;
          return {
            ...aMsg,
            contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order, detail }],
          };
        }));
      } else {
        insertNotification(text, 'info', detail);
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
      if (batch.current.timer) clearTimeout(batch.current.timer);
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
            setMessages((prev) => updateMessage(prev,msgId, (msg) => {
              if (msg.role !== 'assistant') return msg;
              const aMsg = msg as AssistantMessage;
              return {
                ...aMsg,
                contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order }],
              };
            }));
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


export { handleContextWindowEvent };
