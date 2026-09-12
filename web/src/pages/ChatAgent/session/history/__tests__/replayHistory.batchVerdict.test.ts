/**
 * What a replayed card may claim when one interrupt stopped several calls.
 *
 * The resume persists one answer per interrupt, not one per call, so a turn
 * that approved AAPL and rejected MSFT leaves behind exactly what rejecting
 * both leaves behind. Neither replay order may turn that into a per-call
 * verdict: a rejected badge on an order that is executing is the reading that
 * gets it placed a second time.
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

/**
 * The resume for a mixed batch. A bare reject is the one reject shape the
 * server records, and it records it against the interrupt, not the call, so
 * this is also what rejecting every call in the batch looks like.
 */
function mixedResume(runId?: string) {
  return {
    event: 'user_message',
    data: {
      thread_id: 'thread-1',
      turn_index: 1,
      content: '',
      ...(runId ? { run_id: runId } : {}),
      metadata: { hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': null } },
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

describe('history replay: a batch answered with differing decisions', () => {
  it('marks no card rejected when the interrupt replays before its resume', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, PLACED_INTERRUPT, mixedResume('run-1')]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('approved');
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('marks no card rejected when the resume claimed the interrupt first', async () => {
    // The reload taken inside the claim window: the resume is still running,
    // so its cards must settle, and settling them on the one recorded answer
    // is what put a rejected badge on the order that was approved.
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, mixedResume(), TAIL_INTERRUPT]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('approved');
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('still gives a single stopped call its exact verdict', async () => {
    // One call, one answer: the record does name it, so a rejected order has to
    // keep reading rejected.
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        OPENING_TURN,
        {
          ...PLACED_INTERRUPT,
          data: { ...PLACED_INTERRUPT.data, action_requests: [ACTION_REQUESTS[0]] },
        },
        mixedResume('run-1'),
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(allCards(read())['int-1']?.status).toBe('rejected');
  });
});
