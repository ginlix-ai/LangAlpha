// @vitest-environment node
/**
 * A subagent's thought, and the message its run opened, are timed by when its
 * writer captured the frame, not by when this page drew it: a replayed
 * backlog arrives all at once. A frame that carries no time leaves them
 * untimed rather than stamped now.
 */
import { describe, it, expect, vi } from 'vitest';
import { handleSubagentMessageChunk, handleSubagentToolCalls } from '../liveEventHandlers';
import type { StreamRefs } from '../../streamRefs';

const TASK = 'task:Qm3xLp';
const MESSAGE = 'subagent-task:Qm3xLp-assistant-0';

function setup() {
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
  } as unknown as StreamRefs;
  const updateSubagentCard = vi.fn();
  const calls = (atMs?: number, ...ids: string[]) =>
    handleSubagentToolCalls({
      taskId: TASK,
      assistantMessageId: MESSAGE,
      toolCalls: ids.map((id) => ({ id, name: 'bash', args: {} })),
      refs,
      updateSubagentCard,
      atMs,
    });
  const signal = (content: 'start' | 'complete', atMs?: number) =>
    handleSubagentMessageChunk({
      taskId: TASK,
      assistantMessageId: MESSAGE,
      contentType: 'reasoning_signal',
      content,
      finishReason: undefined,
      refs,
      updateSubagentCard,
      atMs,
    });
  const message = () => refs.subagentStateRefs![TASK].messages.find((m) => m.id === MESSAGE) as Record<string, unknown>;
  const thought = () => Object.values(message().reasoningProcesses as Record<string, Record<string, unknown>>)[0];
  return { refs, calls, signal, message, thought };
}

describe('live subagent thought times', () => {
  it('times a thought by its frames, though the backlog lands in one instant', () => {
    vi.useFakeTimers({ toFake: ['Date'], now: 9_000 });
    try {
      const { signal, message, thought } = setup();
      signal('start', 1_000);
      signal('complete', 4_000);
      expect(thought()).toMatchObject({ reasoningComplete: true, elapsedMs: 3_000 });
      // The message keeps when its run wrote it, which places its prose.
      expect(message()).toMatchObject({ openedAtMs: 1_000 });
    } finally {
      vi.useRealTimers();
    }
  });

  it('leaves a reconnect\'s thought untimed when its frames carry no time', () => {
    const { refs, signal, message, thought } = setup();
    refs.isReconnect = true;
    signal('start');
    signal('complete');
    expect(thought()).toMatchObject({ reasoningComplete: true, elapsedMs: undefined });
    expect(message()).not.toHaveProperty('openedAtMs');
  });

  it('stamps the message a call opens with the call\'s time', () => {
    const { calls, message } = setup();
    calls(2_000, 'a');
    expect(message()).toMatchObject({ openedAtMs: 2_000 });
  });

  it('gives an untimed message its first call\'s time, and keeps it', () => {
    const { signal, calls, message } = setup();
    signal('start');
    expect(message()).not.toHaveProperty('openedAtMs');
    calls(2_000, 'a');
    calls(3_000, 'b');
    expect(message()).toMatchObject({ openedAtMs: 2_000 });
  });
});
