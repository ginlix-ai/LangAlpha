/**
 * A reloaded thread still shows the order receipt.
 *
 * The receipt is the only record of an order in chat, and a reload does not
 * replay the SSE stream: it replays the checkpoint, where the receipt rides on
 * the tool message's artifact. So the wire event's artifact has to reach the
 * tool-call process verbatim, and the render gate has to say yes to it there
 * for the same reason it did live. Both halves are asserted here, because
 * either one failing loses the card and leaves the turn showing a bare tool row
 * for an order that was actually placed.
 *
 * The gated order is the case that broke: a call stopped for approval is
 * answered in the turn after the one that made it, so its result arrives under
 * a new assistant message that has never heard of the call. Paired on that
 * message alone it was dropped as an orphan, and every approved order in chat
 * lost its receipt.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import type { MessageRecord } from '../../types';
import { isInlineArtifactReady } from '../../../components/charts/InlineArtifactCards';
import { orderReceiptOf } from '../../../components/mcp/useOrderReceipt';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const TOOL_NAME = 'mcp__moomoo__sim_trade_input_order';

const ARTIFACT = {
  type: 'order_receipt',
  direct_mcp: {
    server: 'moomoo',
    tool: 'sim_trade_input_order',
    vendor: 'moomoo',
    order: { action: 'place', mode: 'paper' },
  },
  order_receipt: {
    attempt_id: 'attempt-1',
    vendor: 'moomoo',
    tool: 'sim_trade_input_order',
    action: 'place',
    mode: 'paper',
    account_ref: '1234567',
    order: {
      action: 'place',
      mode: 'paper',
      vendor: 'moomoo',
      account_ref: '1234567',
      symbol: 'AAPL',
      side: 'buy',
      qty: '1',
      order_type: 'limit',
      limit_price: '50',
    },
    outcome: {
      status: 'submitted',
      vendor_order_id: '900101',
      route: { market: '100' },
      failure: null,
    },
  },
};

const EVENTS = [
  {
    event: 'user_message',
    data: { thread_id: 'thread-1', turn_index: 0, content: 'Buy 1 AAPL at 50' },
  },
  {
    event: 'tool_calls',
    data: {
      thread_id: 'thread-1',
      turn_index: 0,
      tool_calls: [
        { id: 'call-1', name: TOOL_NAME, args: { symbol: 'AAPL', qty: '1', price: '50' } },
      ],
    },
  },
  {
    event: 'tool_call_result',
    data: {
      thread_id: 'thread-1',
      turn_index: 0,
      tool_call_id: 'call-1',
      content: '{"ret_code":0,"data":{"order_id":"900101"}}',
      status: 'success',
      artifact: ARTIFACT,
    },
  },
];

/**
 * The same order, stopped at the gate: the call is made in one turn and the
 * resume answers it in the next, under its own assistant message.
 */
const GATED_EVENTS = [
  {
    event: 'user_message',
    data: { thread_id: 'thread-1', turn_index: 0, content: 'Buy 1 AAPL at 50' },
  },
  {
    event: 'tool_calls',
    data: {
      thread_id: 'thread-1',
      turn_index: 0,
      tool_calls: [
        { id: 'call-1', name: TOOL_NAME, args: { symbol: 'AAPL', qty: '1', price: '50' } },
      ],
    },
  },
  {
    event: 'interrupt',
    data: {
      thread_id: 'thread-1',
      turn_index: 0,
      interrupt_id: 'int-1',
      kind: 'order_approval',
      action_requests: [
        {
          type: 'tool_approval',
          name: TOOL_NAME,
          args: { symbol: 'AAPL', qty: '1', price: '50' },
          tool_call_id: 'call-1',
          attempt_id: 'attempt-1',
        },
      ],
    },
  },
  // The resume opens a new turn, and with it a new assistant message.
  { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: '' } },
  {
    event: 'tool_call_result',
    data: {
      thread_id: 'thread-1',
      turn_index: 1,
      tool_call_id: 'call-1',
      content: '{"ret_code":0,"data":{"order_id":"900101"}}',
      status: 'success',
      artifact: ARTIFACT,
    },
  },
];

function processOf(messages: MessageRecord[], toolCallId: string) {
  const assistants = messages.filter((m) => m.role === 'assistant') as unknown as AssistantMessage[];
  for (const message of assistants) {
    const proc = (message.toolCallProcesses || {})[toolCallId];
    if (proc) return proc;
  }
  return undefined;
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: an order receipt on a tool result', () => {
  it('carries the artifact onto the tool call verbatim', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf(EVENTS));

    await loadConversationHistory(rt, makeDeps());

    const artifact = processOf(read(), 'call-1')?.toolCallResult?.artifact;
    expect(artifact).toEqual(ARTIFACT);
    expect(orderReceiptOf(artifact)?.outcome.status).toBe('submitted');
    expect(orderReceiptOf(artifact)?.outcome.vendor_order_id).toBe('900101');
  });

  it('leaves the replayed call ready to draw its card', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf(EVENTS));

    await loadConversationHistory(rt, makeDeps());

    const proc = processOf(read(), 'call-1');
    expect(isInlineArtifactReady(proc?.toolName, proc?.toolCallResult?.artifact)).toBe(true);
  });

  // The result comes back a turn later than the call, so it has to find the
  // message that made it. Dropped instead, an approved order kept its timeline
  // row and lost the only statement of what the brokerage did with it.
  it('lands on the call it answers when a gate split the two across turns', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf(GATED_EVENTS));

    await loadConversationHistory(rt, makeDeps());

    const proc = processOf(read(), 'call-1');
    expect(proc?.toolCallResult?.artifact).toEqual(ARTIFACT);
    expect(isInlineArtifactReady(proc?.toolName, proc?.toolCallResult?.artifact)).toBe(true);
    expect(orderReceiptOf(proc?.toolCallResult?.artifact)?.outcome.vendor_order_id).toBe('900101');
  });
});
