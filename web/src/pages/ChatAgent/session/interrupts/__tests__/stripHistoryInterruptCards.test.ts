/**
 * The reconnect strip has to remove the card the redelivery will replace.
 *
 * A re-raised interrupt restores its cards on the bubble that first rendered
 * them, while its pending entries are queued against the bubble the re-raise
 * rode. The strip runs off those entries and releases the interrupt id for the
 * reconnect stream, so a strip that misses the card leaves the history copy
 * standing beside the redelivered one.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));
vi.mock('../../../utils/api', () => ({ replayThreadHistory: api.replayThreadHistory }));

import { loadConversationHistory } from '../../history/replayHistory';
import { stripHistoryInterruptCards } from '../buckets';
import type { MessageRecord } from '../../types';
import { buildRuntime, makeDeps, replayOf } from '../../history/__tests__/replayHarness';

const ARGS = { acc_id: '12345678', code: 'US.AAPL', side: 'BUY', qty: '1' };
const ACTION_REQUESTS = [
  { name: 'mcp__moomoo__trading_order_place', args: ARGS },
  { name: 'mcp__moomoo__trading_order_place', args: { ...ARGS, code: 'US.MSFT' } },
];

/** The thread as it replays after a resume failed and the graph re-raised. */
const RE_RAISED_THREAD = [
  { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Place both orders' } },
  {
    event: 'interrupt',
    data: { thread_id: 'thread-1', turn_index: 0, interrupt_id: 'int-1', action_requests: ACTION_REQUESTS },
  },
  {
    event: 'user_message',
    data: {
      thread_id: 'thread-1', turn_index: 1, run_id: 'run-1', content: '',
      metadata: { hitl_interrupt_ids: ['int-1'] },
    },
  },
  {
    event: 'interrupt',
    data: { thread_id: 'thread-1', turn_index: 1, interrupt_id: 'int-1', action_requests: ACTION_REQUESTS },
  },
];

function approvalSegments(messages: MessageRecord[]) {
  return (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
    .flatMap((b) => (b.contentSegments || []).filter((sg) => sg.type === 'tool_approval'));
}

function approvalCards(messages: MessageRecord[]) {
  return (messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[])
    .flatMap((b) => Object.keys(b.toolApprovals || {}));
}

beforeEach(() => vi.clearAllMocks());

describe('reconnect strip of replayed interrupt cards', () => {
  it('removes a re-raised batch whose entries name a different bubble', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf(RE_RAISED_THREAD));
    await loadConversationHistory(rt, makeDeps());

    const stripList = rt.unresolvedHistoryInterruptRef.current;
    expect(stripList).toHaveLength(2);
    expect(approvalSegments(read())).toHaveLength(2);

    const stripped = stripHistoryInterruptCards(read(), stripList);

    // Nothing left for the reconnect's redelivery to duplicate.
    expect(approvalSegments(stripped)).toHaveLength(0);
    expect(approvalCards(stripped)).toEqual([]);
  });

  it('strips by card id, and leaves cards no entry names', async () => {
    const messages: MessageRecord[] = [
      {
        id: 'bubble-1',
        role: 'assistant',
        content: '',
        contentSegments: [
          { type: 'tool_approval', proposalId: 'int-1#0', order: 0 },
          { type: 'tool_approval', proposalId: 'int-2', order: 1 },
        ],
        toolApprovals: {
          'int-1#0': { status: 'pending' },
          'int-2': { status: 'pending' },
        },
      } as unknown as MessageRecord,
    ];

    const stripped = stripHistoryInterruptCards(messages, [
      { type: 'tool_approval', assistantMessageId: 'bubble-9', proposalId: 'int-1#0', interruptId: 'int-1' },
    ]);

    // The named card goes even though its entry points at another bubble; the
    // unnamed one stays, because the strip is the pending set and nothing more.
    expect(approvalCards(stripped)).toEqual(['int-2']);
  });
});
