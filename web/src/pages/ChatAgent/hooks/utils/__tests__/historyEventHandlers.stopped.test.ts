/**
 * History replay of a stopped turn.
 *
 * A hard-stopped turn persists synthetic close events to sse_events:
 *   reasoning_signal:"complete"  → closes the reasoning block
 *   finish_reason:"stopped"      → stamps the message stopped
 *
 * On reload these flow through handleHistoryReasoningSignal /
 * handleHistoryTextContent with no special-casing, so the replayed message
 * renders the closed reasoning fragment + the per-message "⏹ Stopped" chip
 * (driven by the `stopped` flag) — matching the live finalize.
 */
import { describe, it, expect } from 'vitest';
import {
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
} from '../historyEventHandlers';
import type { MessageRecord } from '../types';

interface PairState {
  contentOrderCounter: number;
  reasoningId: string | null;
  toolCallId: string | null;
}

function makeStore(initial: MessageRecord[]) {
  const store = { messages: initial.slice() };
  const setMessages = (
    updater: ((prev: MessageRecord[]) => MessageRecord[]) | MessageRecord[],
  ) => {
    store.messages = typeof updater === 'function' ? updater(store.messages) : updater;
  };
  return { store, setMessages };
}

describe('historyEventHandlers — stopped turn replay', () => {
  it('renders a closed reasoning fragment and stamps the message stopped', () => {
    const assistantMessageId = 'history-assistant-0';
    const { store, setMessages } = makeStore([
      {
        id: assistantMessageId,
        role: 'assistant',
        content: '',
        contentType: 'text',
        isStreaming: false,
        isHistory: true,
        contentSegments: [],
        reasoningProcesses: {},
        toolCallProcesses: {},
      },
    ]);
    const pairState: PairState = { contentOrderCounter: 0, reasoningId: null, toolCallId: null };

    // Replay: reasoning starts, gets content, then the synthetic close events.
    handleHistoryReasoningSignal({ assistantMessageId, signalContent: 'start', pairIndex: 0, pairState, setMessages });
    handleHistoryReasoningContent({ assistantMessageId, content: 'partial thought before stop', pairState, setMessages });
    handleHistoryReasoningSignal({ assistantMessageId, signalContent: 'complete', pairIndex: 0, pairState, setMessages });
    // Synthetic terminal: finish_reason "stopped" with no content.
    handleHistoryTextContent({ assistantMessageId, content: '', finishReason: 'stopped', pairState, setMessages });

    const msg = store.messages.find((m) => m.id === assistantMessageId)!;
    const procs = msg.reasoningProcesses as Record<string, Record<string, unknown>>;
    const reasoning = procs[Object.keys(procs)[0]];

    // The reasoning fragment is closed (not stuck "thinking"), with its content.
    expect(reasoning.reasoningComplete).toBe(true);
    expect(reasoning.isReasoning).toBe(false);
    expect(reasoning.content).toContain('partial thought before stop');
    // The message is stamped stopped (drives the chip) and not streaming.
    expect((msg as { stopped?: boolean }).stopped).toBe(true);
    expect(msg.isStreaming).toBe(false);
  });

  it('a normal (non-stopped) finish_reason does NOT stamp stopped', () => {
    const assistantMessageId = 'history-assistant-1';
    const { store, setMessages } = makeStore([
      { id: assistantMessageId, role: 'assistant', content: 'done', isStreaming: false, reasoningProcesses: {} },
    ]);
    const pairState: PairState = { contentOrderCounter: 0, reasoningId: null, toolCallId: null };

    handleHistoryTextContent({ assistantMessageId, content: '', finishReason: 'stop', pairState, setMessages });

    const msg = store.messages.find((m) => m.id === assistantMessageId)!;
    expect((msg as { stopped?: boolean }).stopped).toBeUndefined();
    expect(msg.isStreaming).toBe(false);
  });
});
