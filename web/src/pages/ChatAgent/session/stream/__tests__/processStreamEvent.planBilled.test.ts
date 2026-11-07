// @vitest-environment node
/**
 * A reply whose last request was billed to usage credits gets its notice
 * live, under the reply's text: once, and only for the main agent.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

function setup() {
  const refreshClaudeCode = vi.fn();
  let messages = [{ id: 'a1', role: 'assistant', content: 'Done.', contentSegments: [{ type: 'text', content: 'Done.', order: 1 }] }] as unknown as ChatMessage[];
  const rt = {
    setMessages: (updater: (prev: ChatMessage[]) => ChatMessage[]) => { messages = updater(messages); },
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    currentRunIdRef: { current: null },
    latestRunIdRef: { current: null },
    latestRunEpochRef: { current: 0 },
    currentMessageRef: { current: 'a1' },
    threadId: 'thread-1',
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const refs = {
    contentOrderCounterRef: { current: 1 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: null },
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { clearModelStatus: () => {}, refreshClaudeCode } as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    () => null,
  );
  return { process, refreshClaudeCode, notices: () => (messages[0] as unknown as { contentSegments: Array<Record<string, unknown>> }).contentSegments.filter((s) => s.type === 'notification') };
}

const billed = (agent: string, eventId: number) => ({
  event: 'claude_code_plan', thread_id: 'thread-1', agent, reason: 'billed_credits', window: 'five_hour',
  until: '2026-11-06T20:40:00Z', _eventId: eventId,
}) as unknown as SSEEvent;

describe('live: a reply billed to usage credits', () => {
  it('draws its notice once, and none for a subagent', () => {
    const { process, notices } = setup();
    process(billed('task:k3', 4));
    process(billed('model:abc', 5));
    process(billed('model:abc', 6));
    expect(notices()).toEqual([
      { type: 'notification', order: 5, planBilled: { window: 'five_hour', until: '2026-11-06T20:40:00Z' } },
    ]);
  });

  it('rereads the Claude plan for a main agent notice, and for none else', () => {
    const { process, refreshClaudeCode } = setup();
    process(billed('task:k3', 4));
    process({ ...billed('model:abc', 5), window: 'thirty_day' } as unknown as SSEEvent);
    expect(refreshClaudeCode).not.toHaveBeenCalled();
    process(billed('model:abc', 6));
    expect(refreshClaudeCode).toHaveBeenCalledTimes(1);
  });
});
