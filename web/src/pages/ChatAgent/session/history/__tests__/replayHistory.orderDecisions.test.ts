/**
 * A reloaded thread settles its order cards by attempt id.
 *
 * `hitl_decisions` answers slot i with decision i, which is right only while
 * the interrupt's requests and the resume's list stay in step. `order_decisions`
 * names the attempt each verdict belongs to, so it survives an order the
 * positional list would misassign, and it has to win where the two disagree,
 * because the wrong badge on an order is what makes someone place it twice.
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

const AAPL = {
  name: 'mcp__moomoo__trading_order_place',
  args: { acc_id: '12345678', code: 'US.AAPL', side: 'BUY', qty: '1' },
  tool_call_id: 'call-aapl',
  attempt_id: 'attempt-aapl',
  order: { action: 'place', mode: 'live', vendor: 'moomoo', symbol: 'US.AAPL', side: 'buy', qty: '1' },
};

const MSFT = {
  name: 'mcp__moomoo__trading_order_place',
  args: { acc_id: '12345678', code: 'US.MSFT', side: 'SELL', qty: '2' },
  tool_call_id: 'call-msft',
  attempt_id: 'attempt-msft',
  order: { action: 'place', mode: 'live', vendor: 'moomoo', symbol: 'US.MSFT', side: 'sell', qty: '2' },
};

const OPENING_TURN = {
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: 0, content: 'Place both orders' },
};

function interruptOn(actionRequests: unknown[], placed: boolean) {
  return {
    event: 'interrupt',
    data: {
      thread_id: 'thread-1',
      ...(placed ? { turn_index: 0 } : {}),
      interrupt_id: 'int-1',
      kind: 'order_approval',
      action_requests: actionRequests,
    },
  };
}

/**
 * Approve AAPL, reject MSFT: recorded keyed, and recorded positionally the
 * other way round. Nothing sends a payload like this; it is how a test tells
 * which record was read, and the keyed one is the one that has to win.
 */
function crossedResume(runId?: string) {
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
        hitl_decisions: {
          'int-1': [
            { type: 'reject', message: 'positional' },
            { type: 'approve', message: null },
          ],
        },
        order_decisions: {
          'attempt-aapl': { type: 'approve' },
          'attempt-msft': { type: 'reject', message: 'not this one' },
        },
      },
    },
  };
}

function allCards(messages: MessageRecord[]) {
  return Object.fromEntries(
    (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
      .flatMap((b) => Object.entries(b.toolApprovals || {})),
  ) as Record<string, { status?: string; reason?: string | null; attemptId?: string }>;
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a resume that recorded order decisions', () => {
  it('settles each card from its own attempt when the interrupt replays first', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, interruptOn([AAPL, MSFT], true), crossedResume('run-1')]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.attemptId).toBe('attempt-aapl');
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('rejected');
    expect(cards['int-1#1']?.reason).toBe('not this one');
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('settles them the same way when the resume claimed the interrupt first', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, crossedResume(), interruptOn([AAPL, MSFT], false)]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('rejected');
    expect(cards['int-1#1']?.reason).toBe('not this one');
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('keys the order and leaves the unkeyed call to its slot', async () => {
    const { rt, read } = buildRuntime();
    const quote = { name: 'mcp__moomoo__quote_stock_quote', args: { code: 'US.MSFT' } };
    const mixedResume = {
      event: 'user_message',
      data: {
        thread_id: 'thread-1',
        turn_index: 1,
        content: 'no',
        run_id: 'run-1',
        metadata: {
          hitl_interrupt_ids: ['int-1'],
          hitl_answers: { 'int-1': null },
          // Slot 0 is the order, and it says the opposite of the map.
          hitl_decisions: {
            'int-1': [
              { type: 'reject', message: 'positional' },
              { type: 'reject', message: 'no quotes' },
            ],
          },
          order_decisions: { 'attempt-aapl': { type: 'approve' } },
        },
      },
    };
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, interruptOn([AAPL, quote], true), mixedResume]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1#0']?.status).toBe('approved');
    expect(cards['int-1#1']?.status).toBe('rejected');
    expect(cards['int-1#1']?.reason).toBe('no quotes');
  });

  it('falls back to the positional record for a keyed order the map never named', async () => {
    const { rt, read } = buildRuntime();
    const legacyResume = {
      event: 'user_message',
      data: {
        thread_id: 'thread-1',
        turn_index: 1,
        content: '',
        run_id: 'run-1',
        metadata: {
          hitl_interrupt_ids: ['int-1'],
          hitl_answers: { 'int-1': null },
          hitl_decisions: { 'int-1': [{ type: 'reject', message: 'too big' }] },
        },
      },
    };
    api.replayThreadHistory.mockImplementation(
      replayOf([OPENING_TURN, interruptOn([AAPL], true), legacyResume]),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = allCards(read());
    expect(cards['int-1']?.status).toBe('rejected');
    expect(cards['int-1']?.reason).toBe('too big');
  });
});
