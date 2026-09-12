/**
 * A tool-approval interrupt the backend raised again must replay answerable.
 *
 * `hitl_interrupt_ids` is stamped when the resume is requested, not when the
 * graph consumes the Command, so a resume that terminated without consuming it
 * settles the cards on replay while the backend still has the calls stopped.
 * The re-raise is the later evidence: without honouring it both cards replay
 * `approved` with no pending interrupt, and the turn cannot be answered at all.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import type { MessageRecord } from '../../types';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const ARGS = { acc_id: '12345678', code: 'US.AAPL', side: 'BUY', qty: '1' };

/** One interrupt stopping two orders, which is how a two-order turn arrives. */
const ACTION_REQUESTS = [
  { name: 'mcp__moomoo__trading_order_place', args: ARGS },
  { name: 'mcp__moomoo__trading_order_place', args: { ...ARGS, code: 'US.MSFT' } },
];

const STOPPED_TURN = [
  { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Place both orders' } },
  {
    event: 'interrupt',
    data: {
      thread_id: 'thread-1',
      turn_index: 0,
      interrupt_id: 'int-1',
      action_requests: ACTION_REQUESTS,
    },
  },
];

/** The resume turn, terminal (it carries a run_id) and approving both calls. */
const RESUME_TURN = [
  {
    event: 'user_message',
    data: {
      thread_id: 'thread-1',
      turn_index: 1,
      run_id: 'run-1',
      content: '',
      metadata: { hitl_interrupt_ids: ['int-1'] },
    },
  },
];

/** The same interrupt raised again, riding the resume turn's boundary. */
const RE_RAISE = {
  event: 'interrupt',
  data: {
    thread_id: 'thread-1',
    turn_index: 1,
    interrupt_id: 'int-1',
    action_requests: ACTION_REQUESTS,
  },
};

function cardsOn(messages: MessageRecord[]) {
  const bubble = messages.find((m) => m.id === 'history-assistant-0') as unknown as AssistantMessage;
  return bubble?.toolApprovals || {};
}

function approvalSegments(messages: MessageRecord[]) {
  return (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
    .flatMap((b) => (b.contentSegments || []).filter((sg) => sg.type === 'tool_approval'));
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: tool approval re-raise', () => {
  it('restores every card the re-raised interrupt stopped, and re-arms the answer', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([...STOPPED_TURN, ...RESUME_TURN, RE_RAISE]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(cardsOn(read())['int-1#0']?.status).toBe('pending');
    expect(cardsOn(read())['int-1#1']?.status).toBe('pending');
    // Still two cards: the re-raise restores, it never renders a second pair.
    expect(approvalSegments(read())).toHaveLength(2);
    // The re-arm path in useChatMessages reads exactly these two.
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([
      expect.objectContaining({ type: 'tool_approval', interruptId: 'int-1', proposalId: 'int-1#0' }),
      expect.objectContaining({ type: 'tool_approval', interruptId: 'int-1', proposalId: 'int-1#1' }),
    ]);
  });

  it('leaves a resume that was actually consumed settled', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([...STOPPED_TURN, ...RESUME_TURN]));

    await loadConversationHistory(rt, makeDeps());

    expect(cardsOn(read())['int-1#0']?.status).toBe('approved');
    expect(cardsOn(read())['int-1#1']?.status).toBe('approved');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('settles a batch that a later attempt finally consumed', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        ...STOPPED_TURN,
        ...RESUME_TURN,
        RE_RAISE,
        {
          event: 'user_message',
          data: {
            thread_id: 'thread-1',
            turn_index: 2,
            run_id: 'run-2',
            content: '',
            metadata: { hitl_interrupt_ids: ['int-1'] },
          },
        },
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(cardsOn(read())['int-1#0']?.status).toBe('approved');
    expect(cardsOn(read())['int-1#1']?.status).toBe('approved');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });
});
