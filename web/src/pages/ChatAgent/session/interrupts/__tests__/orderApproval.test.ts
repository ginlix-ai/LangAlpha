/**
 * A stopped order is answered by name, not by where it sat.
 *
 * The positional half of a resume is only right while the interrupt's requests
 * and the resume's decisions line up. An order cannot rest on that, so every
 * keyed request carries an attempt id from the interrupt through the card to
 * the resume, and the server refuses a keyed attempt it was not handed a
 * verdict for. These lock the three places that has to hold: what the card
 * keeps, what the resume sends, and what a replay reads back.
 */
import { describe, it, expect, vi } from 'vitest';
import type { AssistantMessage } from '@/types/chat';
import type { ActionRequest, OrderProposal } from '@/types/sse';
import { projectLiveInterrupt } from '../fromLiveEvent';
import type { StreamRuntime } from '../../runtime';
import type { MessageRecord, SSEEvent, StreamProcessorRefs } from '../../types';
import {
  buildToolApprovalState,
  isToolApprovalRequest,
  orderDecisionFields,
  readOrderDecisions,
  toolApprovalCards,
} from '../toolApprovalCard';
import { buildToolApprovalResumeEntry } from '../answerBoard';

const ORDER: OrderProposal = {
  action: 'place',
  mode: 'live',
  vendor: 'moomoo',
  account_ref: '12345678',
  instrument: { kind: 'equity', symbol: 'US.AAPL' },
  side: 'buy',
  qty: '1',
  order_type: 'limit',
  limit_price: '100',
};

const KEYED: ActionRequest = {
  name: 'mcp__moomoo__trading_order_place',
  args: { acc_id: '12345678', code: 'US.AAPL' },
  tool_call_id: 'call-1',
  attempt_id: 'attempt-1',
  order: ORDER,
};

describe('the card an order interrupt builds', () => {
  it('keeps the tool call, the attempt and the order summary', () => {
    const state = buildToolApprovalState(KEYED, 'int-1', 0, 1);
    expect(state.toolCallId).toBe('call-1');
    expect(state.attemptId).toBe('attempt-1');
    expect(state.order?.mode).toBe('live');
    expect(state.order?.instrument).toEqual({ kind: 'equity', symbol: 'US.AAPL' });
  });

  it('leaves an unkeyed request unkeyed rather than inventing an id', () => {
    const state = buildToolApprovalState(
      { name: 'mcp__moomoo__quote_stock_quote', args: {} },
      'int-1',
      0,
      1,
    );
    expect(state.attemptId).toBeUndefined();
    expect(state.order).toBeNull();
  });

  // The order gate raises its own interrupt, so its requests need not be named
  // the way a directly bound MCP tool is. One that fell through would render a
  // plan-approval card over a live order.
  it('recognizes a keyed request whatever it is named', () => {
    expect(isToolApprovalRequest({ name: 'place_equity_order', attempt_id: 'a-1' })).toBe(true);
    expect(isToolApprovalRequest({ name: 'place_equity_order', order: ORDER })).toBe(true);
    expect(isToolApprovalRequest({ name: 'SubmitPlan', args: { plan: 'x' } })).toBe(false);
    expect(isToolApprovalRequest(undefined)).toBe(false);
  });

  it('carries the attempt onto every card of a batch', () => {
    const cards = toolApprovalCards(
      [KEYED, { ...KEYED, attempt_id: 'attempt-2', tool_call_id: 'call-2' }],
      'int-1',
      'fallback',
    );
    expect(cards.map((c) => c.state.attemptId)).toEqual(['attempt-1', 'attempt-2']);
    expect(cards.map((c) => c.proposalId)).toEqual(['int-1#0', 'int-1#1']);
    // The target is minted here, so no settler has to read the id's suffix back.
    expect(cards.map((c) => c.target)).toEqual([
      { kind: 'attempt', attemptId: 'attempt-1', index: 0 },
      { kind: 'attempt', attemptId: 'attempt-2', index: 1 },
    ]);
  });
});

describe('the resume one interrupt is answered with', () => {
  it('names every keyed attempt, because absence is a refusal', () => {
    const entry = buildToolApprovalResumeEntry([
      { target: { kind: 'attempt', attemptId: 'attempt-1', index: 0 }, decision: { type: 'approve' } },
      {
        target: { kind: 'attempt', attemptId: 'attempt-2', index: 1 },
        decision: { type: 'reject', message: 'not this one' },
      },
    ]);
    expect(entry.order_decisions).toEqual({
      'attempt-1': { type: 'approve' },
      'attempt-2': { type: 'reject', message: 'not this one' },
    });
    // Positional stays populated so a mixed payload is unambiguous.
    expect(entry.decisions).toEqual([
      { type: 'approve' },
      { type: 'reject', message: 'not this one' },
    ]);
  });

  it('answers a mixed interrupt both ways at once', () => {
    const entry = buildToolApprovalResumeEntry([
      { target: { kind: 'position', index: 0 }, decision: { type: 'approve' } },
      { target: { kind: 'attempt', attemptId: 'attempt-2', index: 1 }, decision: { type: 'approve' } },
    ]);
    expect(entry.decisions).toHaveLength(2);
    expect(Object.keys(entry.order_decisions || {})).toEqual(['attempt-2']);
  });

  it('sends no map at all when nothing in the interrupt was keyed', () => {
    const entry = buildToolApprovalResumeEntry([
      { target: { kind: 'position', index: 0 }, decision: { type: 'approve' } },
    ]);
    expect(entry).toEqual({ decisions: [{ type: 'approve' }] });
  });
});

describe('the verdict a replay reads back', () => {
  it('reads the map a resume recorded, and nothing that is not one', () => {
    expect(readOrderDecisions({ order_decisions: { 'attempt-1': { type: 'approve' } } })).toEqual({
      'attempt-1': { type: 'approve' },
    });
    expect(readOrderDecisions({ order_decisions: [] })).toBeUndefined();
    expect(readOrderDecisions({})).toBeUndefined();
    expect(readOrderDecisions(undefined)).toBeUndefined();
  });

  it('keeps a reject reason and drops an unrecognized verdict', () => {
    expect(orderDecisionFields({ type: 'reject', message: '  too big  ' })).toEqual({
      status: 'rejected',
      reason: 'too big',
    });
    expect(orderDecisionFields({ type: 'approve', message: 'ignored' })).toEqual({
      status: 'approved',
      reason: null,
    });
    expect(orderDecisionFields({ type: 'defer' })).toBeNull();
    expect(orderDecisionFields(undefined)).toBeNull();
  });
});

describe('the live projection of an order interrupt', () => {
  function build() {
    let current: MessageRecord[] = [
      { id: 'a-1', role: 'assistant', content: '', contentSegments: [] } as unknown as MessageRecord,
    ];
    const rt = {
      setMessages: ((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
        current = updater(current);
      }) as StreamRuntime['setMessages'],
      setPendingInterrupt: vi.fn(),
      pendingInterruptIdsRef: { current: new Set<string>() },
      renderedInterruptIdsRef: { current: new Set<string>() },
      currentPlanModeRef: { current: false },
    } as unknown as StreamRuntime;
    const refs = { contentOrderCounterRef: { current: 0 } } as unknown as StreamProcessorRefs;
    return { rt, refs, read: () => current };
  }

  // The gate raises its own interrupt, so its requests need not be named the
  // way a directly bound MCP tool is. Falling through here would draw a plan
  // card over a live order and leave the resume with nothing to answer it.
  it('draws approval cards for a request the direct-name test would miss', () => {
    const { rt, refs, read } = build();
    projectLiveInterrupt(
      rt,
      {
        type: 'interrupt',
        interrupt_id: 'int-1',
        kind: 'order_approval',
        action_requests: [{ name: 'place_equity_order', args: { symbol: 'AAPL' }, attempt_id: 'attempt-1' }],
      } as unknown as SSEEvent,
      'a-1',
      refs,
    );

    const msg = read()[0] as AssistantMessage;
    expect(msg.planApprovals).toBeUndefined();
    expect(msg.toolApprovals?.['int-1']).toMatchObject({
      status: 'pending',
      attemptId: 'attempt-1',
      toolName: 'place_equity_order',
    });
  });
});
