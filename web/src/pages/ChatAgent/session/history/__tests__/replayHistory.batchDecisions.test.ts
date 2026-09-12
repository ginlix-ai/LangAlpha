/**
 * A batch answered with differing decisions, replayed from the record of them.
 *
 * `hitl_decisions` keeps one decision per stopped call, in the order the
 * interrupt raised them, so slot i answers card `<interrupt_id>#i`. Both replay
 * orders have to read it that way: the approved order keeps reading approved,
 * and the rejected one keeps its own reason.
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

/** The interrupt on its own turn, which is how a settled turn replays it. */
const PLACED_INTERRUPT = {
  event: 'interrupt',
  data: {
    thread_id: 'thread-1',
    turn_index: 0,
    interrupt_id: 'int-1',
    action_requests: ACTION_REQUESTS,
  },
};

/** The tip copy: appended once at the end of a checkpoint replay, unplaced. */
const TAIL_INTERRUPT = {
  event: 'interrupt',
  data: { thread_id: 'thread-1', interrupt_id: 'int-1', action_requests: ACTION_REQUESTS },
};

const DECISIONS = [
  { type: 'approve', message: null },
  { type: 'reject', message: 'not this one' },
];

/** Approve AAPL, reject MSFT with a reason. */
function mixedResume(runId?: string) {
  return {
    event: 'user_message',
    data: {
      thread_id: 'thread-1',
      turn_index: 1,
      content: 'not this one',
      ...(runId ? { run_id: runId } : {}),
      metadata: {
        hitl_interrupt_ids: ['int-1'],
        hitl_answers: { 'int-1': null },
        hitl_decisions: { 'int-1': DECISIONS },
      },
    },
  };
}

function allCards(messages: MessageRecord[]) {
  return Object.fromEntries(
    (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
      .flatMap((b) => Object.entries(b.toolApprovals || {})),
  ) as Record<string, { status?: string; reason?: string | null }>;
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a recorded batch of differing decisions', () => {
  it('gives each card its own verdict when the interrupt replays first', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, PLACED_INTERRUPT, mixedResume('run-1')]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#0']?.reason).toBeNull();
    expect(cards['int-1#1']?.status).toBe('rejected');
    expect(cards['int-1#1']?.reason).toBe('not this one');
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('gives each card its own verdict when the resume claimed the interrupt first', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, mixedResume(), TAIL_INTERRUPT]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#0']?.reason).toBeNull();
    expect(cards['int-1#1']?.status).toBe('rejected');
    expect(cards['int-1#1']?.reason).toBe('not this one');
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('keeps the floor for a resume that recorded no decisions', async () => {
    const { rt, read } = buildRuntime();
    const legacyResume = {
      event: 'user_message',
      data: {
        thread_id: 'thread-1',
        turn_index: 1,
        content: '',
        run_id: 'run-1',
        metadata: { hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': null } },
      },
    };
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, PLACED_INTERRUPT, legacyResume]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('approved');
  });
});
