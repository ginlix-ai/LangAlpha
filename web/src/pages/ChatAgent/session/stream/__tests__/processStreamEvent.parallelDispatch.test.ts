// @vitest-environment node
/**
 * A reconnect settles the cards history replay left pending by the same rule
 * replay does (replayHistory.parallelDispatch.test.ts): two hand-offs approved
 * in one resume each take the result of their own call, whatever order the
 * results stream in.
 */
import { describe, expect, it, vi } from 'vitest';
import type { AssistantMessage, ChatMessage } from '@/types/chat';
import type { HistoryInterruptInfo, SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const BUBBLE = 'history-assistant-0';

const pendingCard = (workspace: string) => ({ status: 'pending', question: `Job for ${workspace}`, workspace_name: workspace });

const pendingEntry = (proposalId: string, toolCallId: string): HistoryInterruptInfo => ({
  type: 'ptc_agent', assistantMessageId: BUBBLE, proposalId, interruptId: proposalId, toolCallId,
});

function setup() {
  let messages: ChatMessage[] = [{
    id: BUBBLE,
    role: 'assistant',
    ptcAgentProposals: { 'int-a': pendingCard('ws-a'), 'int-b': pendingCard('ws-b') },
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
    pendingPTCBackfillRef: { current: new Map() },
    subagentHistory: { toolCalls: new Map() },
  };
  const deps = { clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn() };
  // Cards replayed in one order (b, a), results stream in the other (a, b).
  const unresolved = [pendingEntry('int-b', 'call-b'), pendingEntry('int-a', 'call-a')];
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    isReconnect: true,
    unresolvedHistoryInterruptRef: { current: unresolved },
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const dispatched = (toolCallId: string, workspace: string, thread: string) =>
    process({
      event: 'tool_call_result',
      tool_call_id: toolCallId,
      content: JSON.stringify({ success: true, workspace_id: workspace, thread_id: thread, status: 'dispatched' }),
    } as SSEEvent);
  const cards = () =>
    (messages.find((m) => m.id === BUBBLE) as unknown as AssistantMessage)
      .ptcAgentProposals as unknown as Record<string, Record<string, unknown>>;
  return { dispatched, cards, unresolved };
}

describe('reconnect: parallel dispatches', () => {
  it('gives each card the thread its own call dispatched', () => {
    const { dispatched, cards, unresolved } = setup();

    dispatched('call-a', 'ws-a', 'analyst-a');
    dispatched('call-b', 'ws-b', 'analyst-b');

    expect(cards()['int-a']).toMatchObject({ status: 'approved', workspace_id: 'ws-a', thread_id: 'analyst-a' });
    expect(cards()['int-b']).toMatchObject({ status: 'approved', workspace_id: 'ws-b', thread_id: 'analyst-b' });
    expect(unresolved).toEqual([]);
  });
});
