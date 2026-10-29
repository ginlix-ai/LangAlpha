// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { handleReasoningSignal, handleTextContent, handleToolCalls } from '../mainEventHandlers';
import { dispatchWithReplayStamp } from '../processStreamEvent';
import type { MessageRecord, SetMessages } from '../../../hooks/utils/types';
import type { StreamRefs } from '../../streamRefs';

// React applies a setMessages updater after the synchronous flush of a
// reconnect backlog has already flipped the refs bag live. The live-zone
// stamps must therefore be read when the event arrives, not when the
// updater runs, or every completion the backlog carried takes a live turn
// and folds away 1.8 s after catch-up.
function deferredSetMessages(initial: MessageRecord[]) {
  const updaters: ((prev: MessageRecord[]) => MessageRecord[])[] = [];
  const setMessages = ((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
    updaters.push(updater);
  }) as unknown as SetMessages;
  const apply = () => updaters.reduce((msgs, u) => u(msgs), initial);
  return { setMessages, apply };
}

function reconnectRefs(): StreamRefs {
  return {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    isReconnect: true,
  };
}

describe('reconnect stamps are read on arrival', () => {
  it('a reasoning completion held in the backlog folds even if the bag flips before the updater runs', () => {
    const refs = reconnectRefs();
    const { setMessages, apply } = deferredSetMessages([{ id: 'a', role: 'assistant', contentSegments: [] } as MessageRecord]);
    handleReasoningSignal({ assistantMessageId: 'a', signalContent: 'start', refs, setMessages });
    handleReasoningSignal({ assistantMessageId: 'a', signalContent: 'complete', refs, setMessages });
    refs.isReconnect = false;
    const [msg] = apply();
    const [proc] = Object.values(msg.reasoningProcesses as Record<string, Record<string, unknown>>);
    expect(proc._completedAt).toBe(1);
  });

  it('a reasoning start held in the backlog carries no start time', () => {
    // `_startedAt` is what `useThinkingClock` counts from. Stamped with the
    // reconnect instant, a thought replayed mid-stream reads "Thinking for 0s"
    // and climbs from the moment the tab came back, instead of falling back to
    // the pending label that says the start is unknown.
    const refs = reconnectRefs();
    const { setMessages, apply } = deferredSetMessages([{ id: 'a', role: 'assistant', contentSegments: [] } as MessageRecord]);
    handleReasoningSignal({ assistantMessageId: 'a', signalContent: 'start', refs, setMessages });
    refs.isReconnect = false;
    const [msg] = apply();
    const [proc] = Object.values(msg.reasoningProcesses as Record<string, Record<string, unknown>>);
    expect(proc._startedAt).toBeUndefined();
  });

  it('a backlog thought with no server duration measures none of its own', () => {
    // The two stamps have to agree about which world they are in. With the
    // completion pinned to 1 and the start taken live, the subtraction runs
    // across both and yields a large negative duration.
    const refs = reconnectRefs();
    const { setMessages, apply } = deferredSetMessages([{ id: 'a', role: 'assistant', contentSegments: [] } as MessageRecord]);
    handleReasoningSignal({ assistantMessageId: 'a', signalContent: 'start', refs, setMessages });
    handleReasoningSignal({ assistantMessageId: 'a', signalContent: 'complete', refs, setMessages });
    refs.isReconnect = false;
    const [msg] = apply();
    const [proc] = Object.values(msg.reasoningProcesses as Record<string, Record<string, unknown>>);
    expect(proc.elapsedMs).toBeUndefined();
  });

  it('a tool call held in the backlog folds even if the bag flips before the updater runs', () => {
    const refs = reconnectRefs();
    const { setMessages, apply } = deferredSetMessages([{ id: 'a', role: 'assistant', contentSegments: [] } as MessageRecord]);
    handleToolCalls({
      assistantMessageId: 'a',
      toolCalls: [{ id: 't1', name: 'bash', args: {} }],
      finishReason: undefined,
      refs,
      setMessages,
    });
    refs.isReconnect = false;
    const [msg] = apply();
    expect((msg.toolCallProcesses as Record<string, Record<string, unknown>>).t1._createdAt).toBe(1);
  });

  it('a live tool call gets a real timestamp', () => {
    const refs = { ...reconnectRefs(), isReconnect: false };
    const { setMessages, apply } = deferredSetMessages([{ id: 'a', role: 'assistant', contentSegments: [] } as MessageRecord]);
    handleToolCalls({ assistantMessageId: 'a', toolCalls: [{ id: 't1', name: 'bash', args: {} }], finishReason: undefined, refs, setMessages });
    const [msg] = apply();
    expect((msg.toolCallProcesses as Record<string, Record<string, unknown>>).t1._createdAt as number).toBeGreaterThan(1);
  });
});

describe('fallback orders are taken on arrival', () => {
  it('a tool call without an event id sorts before text that arrived after it', () => {
    const refs = { ...reconnectRefs(), isReconnect: false };
    const { setMessages, apply } = deferredSetMessages([{ id: 'a', role: 'assistant', contentSegments: [] } as MessageRecord]);
    handleToolCalls({ assistantMessageId: 'a', toolCalls: [{ id: 't1', name: 'bash', args: {} }], finishReason: undefined, refs, setMessages });
    handleTextContent({ assistantMessageId: 'a', content: 'after', finishReason: undefined, refs, setMessages });
    const [msg] = apply();
    const order = (type: string) => (msg.contentSegments as { type: string; order: number }[]).find((s) => s.type === type)!.order;
    expect(order('tool_call')).toBeLessThan(order('text'));
  });
});

describe('replay frames from the thread mux', () => {
  const seen = (refs: { isReconnect?: boolean }, reconnectOrigin: boolean, replay: boolean) => {
    let stamp: boolean | undefined;
    dispatchWithReplayStamp(refs, reconnectOrigin, { event: 'message_chunk', ...(replay ? { _replay: true } : {}) }, () => {
      stamp = refs.isReconnect;
    });
    return [stamp, refs.isReconnect];
  };

  it('fold as history on a reconnect-born processor whose bag already flipped live', () => {
    expect(seen({ isReconnect: false }, true, true)).toEqual([true, false]);
  });

  it('stay live on a send-born processor, where a replay from 0 is the fresh transcript', () => {
    expect(seen({ isReconnect: false }, false, true)).toEqual([false, false]);
  });

  it('leave an unmarked frame and a still-replaying bag alone', () => {
    expect(seen({ isReconnect: false }, true, false)).toEqual([false, false]);
    expect(seen({ isReconnect: true }, true, true)).toEqual([true, true]);
  });
});
