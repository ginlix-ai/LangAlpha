// @vitest-environment node
/**
 * The end of a turn rereads the Claude plan behind the user's Claude Code
 * tokens, which its requests report, so the picker's meter and the
 * composer's row do not keep the plan from before it. A subagent's run end
 * does not.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

function setup() {
  let messages = [{ id: 'a1', role: 'assistant', content: 'Done.', contentSegments: [] }] as unknown as ChatMessage[];
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
    setNextSummary: vi.fn(),
    nextSummaryOwedRef: { current: false },
    t: (k: string) => k,
  };
  const refreshClaudeCodePlan = vi.fn();
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { clearModelStatus: () => {}, refreshClaudeCodePlan } as unknown as Processor[1],
    'a1',
    {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: {},
      isReconnect: false,
    } as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  return { process, refreshClaudeCodePlan };
}

describe('the end of a turn', () => {
  it.each(['success', 'cancelled'])('rereads the Claude plan once the main agent\'s run ends (%s)', (outcome) => {
    const { process, refreshClaudeCodePlan } = setup();
    process({ event: 'message_chunk', agent: 'model:abc', content_type: 'text', content: 'Done.' } as unknown as SSEEvent);
    expect(refreshClaudeCodePlan).not.toHaveBeenCalled();
    process({ event: 'run_end', thread_id: 'thread-1', agent: 'model:abc', outcome } as unknown as SSEEvent);
    expect(refreshClaudeCodePlan).toHaveBeenCalledOnce();
  });

  it('leaves it to the main run when a subagent\'s run ends', () => {
    const { process, refreshClaudeCodePlan } = setup();
    process({ event: 'run_end', thread_id: 'thread-1', agent: 'task:k3', outcome: 'success' } as unknown as SSEEvent);
    expect(refreshClaudeCodePlan).not.toHaveBeenCalled();
  });
});
