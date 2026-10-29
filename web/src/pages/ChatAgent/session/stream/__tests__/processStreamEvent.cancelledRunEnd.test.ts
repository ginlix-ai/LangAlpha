// @vitest-environment node
/**
 * A cancel from outside the tab ends the run's own stream with a bare
 * `run_end {outcome: "cancelled"}`. The bubble settles as stopped on it, its
 * open thought and tool rows closed, the way a reload shows the turn; left
 * open, the thought's clock ticks on until a reload.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const BUBBLE = 'a1';

function setup() {
  let messages: ChatMessage[] = [{
    id: BUBBLE, role: 'assistant', content: '', isStreaming: true, contentSegments: [],
    reasoningProcesses: {}, toolCallProcesses: {},
  } as unknown as ChatMessage];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    threadId: 'thread-1',
    setThreadId: vi.fn(),
    pendingPTCBackfillRef: { current: new Map() },
    subagentHistory: { toolCalls: new Map() },
    setTokenUsage: vi.fn(),
    t: (k: string) => k,
  };
  const deps = { clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn() };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    BUBBLE,
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const bubble = () => messages.find((m) => m.id === BUBBLE) as unknown as Record<string, unknown>;
  const thinking = () => {
    process({ event: 'message_chunk', content_type: 'reasoning_signal', content: 'start' } as SSEEvent);
    process({ event: 'message_chunk', content_type: 'reasoning', content: 'Bond markets began' } as SSEEvent);
  };
  return { process, bubble, thinking, rt };
}

const runEnd = (outcome: string) => ({ event: 'run_end', thread_id: 'thread-1', run_id: 'run-1', outcome } as SSEEvent);

describe('live: the stream of a run cancelled from elsewhere', () => {
  it('settles the bubble as stopped and closes its open thought', () => {
    const { process, bubble, thinking } = setup();
    thinking();
    expect(Object.values(bubble().reasoningProcesses as Record<string, Record<string, unknown>>)[0].isReasoning).toBe(true);

    process(runEnd('cancelled'));

    expect(bubble()).toMatchObject({ isStreaming: false, stopped: true });
    const thought = Object.values(bubble().reasoningProcesses as Record<string, Record<string, unknown>>)[0];
    expect(thought.isReasoning).toBe(false);
    expect(typeof thought.elapsedMs).toBe('number');
  });

  it('folds a tool row still in progress', () => {
    const { process, bubble } = setup();
    process({ event: 'tool_calls', tool_calls: [{ id: 'call-1', name: 'WebFetch', args: { url: 'https://example.com' } }] } as SSEEvent);
    const before = (bubble().toolCallProcesses as Record<string, Record<string, unknown>>)['call-1'];
    expect(before.isInProgress).toBe(true);

    process(runEnd('cancelled'));

    expect((bubble().toolCallProcesses as Record<string, Record<string, unknown>>)['call-1'])
      .toMatchObject({ isInProgress: false, isComplete: true });
  });

  it('takes the thread\'s token totals from the same frame', () => {
    const { process, bubble, rt } = setup();

    process({ ...runEnd('cancelled'), token_totals: { input_tokens: 1200, output_tokens: 340 } } as SSEEvent);

    expect(bubble()).toMatchObject({ isStreaming: false, stopped: true });
    const apply = rt.setTokenUsage.mock.calls[0][0] as (prev: null) => Record<string, unknown>;
    expect(apply(null)).toMatchObject({ totalInput: 1200, totalOutput: 340, fromServer: true });
  });

  it('leaves a run that completed for its owner to settle', () => {
    const { process, bubble, thinking } = setup();
    thinking();

    process(runEnd('completed'));

    expect(bubble().stopped).toBeUndefined();
    expect(bubble().isStreaming).toBe(true);
  });
});
