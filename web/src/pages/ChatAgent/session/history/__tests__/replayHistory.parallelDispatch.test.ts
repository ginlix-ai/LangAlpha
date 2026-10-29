// @vitest-environment node
/**
 * Two hand-offs approved in one resume each keep their own thread.
 *
 * Parallel dispatches pause as separate interrupts, and nothing orders them
 * after the tool calls: the cards can replay in one order and the dispatch
 * results in the other. Each card has to take the result of its own call, or
 * its slip opens the other analyst's thread and shows that one's status.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const dispatchCard = (interruptId: string, toolCallId: string, workspace: string) => ({
  event: 'interrupt',
  data: {
    thread_id: 'thread-1',
    turn_index: 0,
    interrupt_id: interruptId,
    action_requests: [
      { type: 'ptc_agent', question: `Job for ${workspace}`, workspace_id: workspace, workspace_name: workspace, tool_call_id: toolCallId },
    ],
  },
});

const dispatched = (toolCallId: string, workspace: string, thread: string) => ({
  event: 'tool_call_result',
  data: {
    thread_id: 'thread-1',
    turn_index: 1,
    tool_call_id: toolCallId,
    content: JSON.stringify({ success: true, workspace_id: workspace, thread_id: thread, status: 'dispatched' }),
  },
});

function cards(messages: ChatMessage[]) {
  const bubble = messages.find((m) => m.id === 'history-assistant-0') as unknown as AssistantMessage;
  return bubble.ptcAgentProposals as unknown as Record<string, Record<string, unknown>>;
}

beforeEach(() => vi.clearAllMocks());

describe('history replay — parallel dispatches', () => {
  it('gives each card the thread its own call dispatched', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Hand off both jobs' } },
        dispatchCard('int-b', 'call-b', 'ws-b'),
        dispatchCard('int-a', 'call-a', 'ws-a'),
        {
          event: 'user_message',
          data: { thread_id: 'thread-1', turn_index: 1, content: '', metadata: { hitl_interrupt_ids: ['int-b', 'int-a'] } },
        },
        dispatched('call-a', 'ws-a', 'analyst-a'),
        dispatched('call-b', 'ws-b', 'analyst-b'),
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(cards(read())['int-a']).toMatchObject({ status: 'approved', workspace_id: 'ws-a', thread_id: 'analyst-a' });
    expect(cards(read())['int-b']).toMatchObject({ status: 'approved', workspace_id: 'ws-b', thread_id: 'analyst-b' });
  });
});
