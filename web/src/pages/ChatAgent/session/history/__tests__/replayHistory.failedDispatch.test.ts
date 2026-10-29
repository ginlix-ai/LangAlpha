// @vitest-environment node
/**
 * A reload reads a hand-off the user approved but whose dispatch failed as
 * approved and failed, never as declined. Only the decline is a plain
 * "User declined" string; every failure after the approval is a JSON result
 * with `success: false`.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

function replayWithResult(content: string) {
  return replayOf([
    { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Hand it to NVDA' } },
    {
      event: 'interrupt',
      data: {
        thread_id: 'thread-1',
        turn_index: 0,
        interrupt_id: 'int-1',
        action_requests: [
          { type: 'ptc_agent', question: 'Job', workspace_id: 'ws-1', workspace_name: 'NVDA', tool_call_id: 'call-1' },
        ],
      },
    },
    {
      event: 'user_message',
      data: { thread_id: 'thread-1', turn_index: 1, content: '', metadata: { hitl_interrupt_ids: ['int-1'] } },
    },
    { event: 'tool_call_result', data: { thread_id: 'thread-1', turn_index: 1, tool_call_id: 'call-1', content } },
  ]);
}

async function replayedCard(content: string) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayWithResult(content));
  await loadConversationHistory(rt, makeDeps());
  const bubble = read().find((m: ChatMessage) => m.id === 'history-assistant-0') as unknown as AssistantMessage;
  return (bubble.ptcAgentProposals as unknown as Record<string, Record<string, unknown>>)['int-1'];
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a hand-off whose dispatch failed', () => {
  it('stays approved and says it failed when the thread was busy', async () => {
    const card = await replayedCard(JSON.stringify({ success: false, error: 'That thread is still working.' }));
    expect(card).toMatchObject({ status: 'approved', dispatch_failed: true });
    expect(card.thread_id).toBeUndefined();
  });

  it('keeps the thread an unknown outcome names', async () => {
    const card = await replayedCard(JSON.stringify({
      success: false,
      error: 'dispatch_failed',
      outcome: 'unknown_retained',
      thread_id: 'analyst-1',
      workspace_id: 'ws-1',
    }));
    expect(card).toMatchObject({ status: 'approved', dispatch_failed: true, thread_id: 'analyst-1' });
  });

  it('says a call cancelled when the chat changed agents never started', async () => {
    const card = await replayedCard(
      'Not run: this chat changed agents before the user answered. Ask again if it is still wanted.',
    );
    expect(card).toMatchObject({ status: 'approved', dispatch_failed: true });
  });

  it('still reads a decline as declined', async () => {
    const card = await replayedCard('User declined PTC agent dispatch.');
    expect(card.status).toBe('rejected');
    expect(card.dispatch_failed).toBeUndefined();
  });
});
