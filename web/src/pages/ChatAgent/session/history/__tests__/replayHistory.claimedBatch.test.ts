/**
 * A batch interrupt that replays AFTER the resume that claimed it.
 *
 * One interrupt stopping two orders raises two cards, `int-1#0` and `int-1#1`,
 * and the resume must answer both. On a reload taken inside the window where
 * the resume is requested but the graph has not consumed the Command, that
 * interrupt replays last, after the claim that answers it, so the settle runs
 * against N cards rather than one.
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

const ACTION_REQUESTS = [
  { name: 'mcp__moomoo__trading_order_place', args: ARGS },
  { name: 'mcp__moomoo__trading_order_place', args: { ...ARGS, code: 'US.MSFT' } },
];

const OPENING_TURN = {
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: 0, content: 'Place both orders' },
};

/** The live resume: no run_id, so it is still running and its claim is trusted. */
const LIVE_RESUME = {
  event: 'user_message',
  data: {
    thread_id: 'thread-1',
    turn_index: 1,
    content: '',
    metadata: { hitl_interrupt_ids: ['int-1'] },
  },
};

/** The tip copy: appended once at the end of a checkpoint replay, unplaced. */
const TAIL_INTERRUPT = {
  event: 'interrupt',
  data: { thread_id: 'thread-1', interrupt_id: 'int-1', action_requests: ACTION_REQUESTS },
};

function allCards(messages: MessageRecord[]) {
  return Object.fromEntries(
    (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
      .flatMap((b) => Object.entries(b.toolApprovals || {})),
  ) as Record<string, { status?: string }>;
}

function approvalSegments(messages: MessageRecord[]) {
  return (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
    .flatMap((b) => (b.contentSegments || []).filter((sg) => sg.type === 'tool_approval'));
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a batch claimed before it replays', () => {
  it('settles every card the claim answered, and leaves nothing queued', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, LIVE_RESUME, TAIL_INTERRUPT]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    // The two visible cards are the ones the user can see, so they are the ones
    // that have to stop offering Approve and Reject on a running resume.
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('approved');
    // No card under the bare interrupt id: nothing renders it, and writing one
    // is how the settle used to miss the pair that is actually on screen.
    expect(Object.keys(cards).sort()).toEqual(['int-1#0', 'int-1#1']);
    expect(approvalSegments(read())).toHaveLength(2);
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('leaves the whole batch pending when the claim proves no outcome', async () => {
    // A batched resume shares one content across its interrupts, so an absent
    // answer beside a reject message names none of them. Half a batch settled
    // is worse than none: the resume would be short a decision.
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        OPENING_TURN,
        {
          event: 'user_message',
          data: {
            thread_id: 'thread-1',
            turn_index: 1,
            content: 'not this one',
            metadata: { hitl_interrupt_ids: ['int-1', 'int-2'] },
          },
        },
        TAIL_INTERRUPT,
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('pending');
    expect(cards['int-1#1']?.status).toBe('pending');
    expect(rt.unresolvedHistoryInterruptRef.current).toHaveLength(2);
  });
});
