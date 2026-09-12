/**
 * An interrupt whose action request names a direct MCP tool (`mcp__*`) is a
 * tool approval, not a plan approval: both projections must write a
 * `toolApprovals` card that carries the tool and its exact arguments, and
 * leave the plan branch for everything else it used to catch.
 */
import { describe, it, expect, vi } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

import { projectLiveInterrupt } from '../fromLiveEvent';
import { projectHistoryInterrupt, type HistoryInterruptContext } from '../fromHistoryEvent';
import { createApprovalEvidence } from '../claims';
import type { StreamRuntime, HistoryRuntime } from '../../runtime';
import type { MessageRecord, SSEEvent, StreamProcessorRefs, PairState } from '../../types';

type Ref<T> = { current: T };
const ref = <T,>(current: T): Ref<T> => ({ current });

const INTERRUPT_ID = 'b4ed0001';
const ARGS = {
  acc_id: '12345678',
  code: 'US.AAPL',
  side: 'BUY',
  order_type: 'LIMIT',
  qty: '1',
  price: '100',
  time_in_force: 'DAY',
};

function toolEvent(extra: Partial<SSEEvent> = {}): SSEEvent {
  return {
    type: 'interrupt',
    thread_id: 't-1',
    interrupt_id: INTERRUPT_ID,
    action_requests: [
      {
        name: 'mcp__moomoo__trading_order_place',
        args: ARGS,
        description: JSON.stringify({ tool: 'mcp__moomoo__trading_order_place', arguments: ARGS }, null, 2),
      },
    ],
    role: 'assistant',
    finish_reason: 'interrupt',
    ...extra,
  } as unknown as SSEEvent;
}

/** One interrupt stopping two orders, which is how a two-order turn arrives. */
function batchedToolEvent(): SSEEvent {
  const second = { ...ARGS, code: 'US.MSFT' };
  return {
    ...(toolEvent() as unknown as Record<string, unknown>),
    action_requests: [
      { name: 'mcp__moomoo__trading_order_place', args: ARGS },
      { name: 'mcp__moomoo__trading_order_place', args: second },
    ],
  } as unknown as SSEEvent;
}

function planEvent(): SSEEvent {
  return {
    type: 'interrupt',
    interrupt_id: 'plan-1',
    action_requests: [{ name: 'SubmitPlan', description: '# The plan' }],
  } as unknown as SSEEvent;
}

const bubble = (id: string): MessageRecord =>
  ({ id, role: 'assistant', content: '', contentSegments: [] }) as unknown as MessageRecord;

describe('live projection', () => {
  function build(messages: MessageRecord[]) {
    let current = messages;
    const rt = {
      setMessages: ((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
        current = updater(current);
      }) as StreamRuntime['setMessages'],
      setPendingInterrupt: vi.fn(),
      pendingInterruptIdsRef: ref(new Set<string>()),
      renderedInterruptIdsRef: ref(new Set<string>()),
      currentPlanModeRef: ref(false),
    } as unknown as StreamRuntime;
    const refs = { contentOrderCounterRef: ref(0) } as unknown as StreamProcessorRefs;
    return { rt, refs, read: () => current };
  }

  it('renders a tool approval card with the exact arguments', () => {
    const { rt, refs, read } = build([bubble('a-1')]);
    projectLiveInterrupt(rt, toolEvent(), 'a-1', refs);

    const msg = read()[0] as AssistantMessage;
    expect(msg.contentSegments).toEqual([{ type: 'tool_approval', proposalId: INTERRUPT_ID, order: 1 }]);
    expect(msg.planApprovals).toBeUndefined();
    const card = msg.toolApprovals?.[INTERRUPT_ID];
    expect(card).toMatchObject({
      status: 'pending',
      toolName: 'mcp__moomoo__trading_order_place',
      server: 'moomoo',
      tool: 'trading_order_place',
      interruptId: INTERRUPT_ID,
    });
    // Unmasked: the card is where the user reads what is actually sent.
    expect(card?.args).toEqual(ARGS);
    expect(msg.isStreaming).toBe(false);

    expect(rt.pendingInterruptIdsRef.current.has(INTERRUPT_ID)).toBe(true);
    expect(rt.setPendingInterrupt).toHaveBeenCalledWith({
      type: 'tool_approval',
      interruptId: INTERRUPT_ID,
      assistantMessageId: 'a-1',
      proposalId: INTERRUPT_ID,
    });
  });

  it('renders one card per stopped call when an interrupt batches them', () => {
    // Rendering only the first left the second order invisible, and the resume
    // one decision short of what the server demands.
    const { rt, refs, read } = build([bubble('a-1')]);
    projectLiveInterrupt(rt, batchedToolEvent(), 'a-1', refs);

    const msg = read()[0] as AssistantMessage;
    expect(msg.contentSegments).toHaveLength(2);
    const cards = Object.values(msg.toolApprovals || {});
    expect(cards).toHaveLength(2);
    expect(cards.map((c) => c.actionIndex)).toEqual([0, 1]);
    expect(cards.every((c) => c.actionCount === 2)).toBe(true);
    expect(cards.map((c) => c.args.code)).toEqual(['US.AAPL', 'US.MSFT']);
  });

  it('still routes a SubmitPlan interrupt to the plan card', () => {
    const { rt, refs, read } = build([bubble('a-1')]);
    projectLiveInterrupt(rt, planEvent(), 'a-1', refs);
    const msg = read()[0] as AssistantMessage;
    expect(msg.contentSegments[0].type).toBe('plan_approval');
    expect(msg.toolApprovals).toBeUndefined();
    expect(msg.planApprovals?.['plan-1']?.description).toBe('# The plan');
  });

  it('suppresses the duplicate card on a re-raise but keeps it answerable', () => {
    const { rt, refs, read } = build([bubble('a-1'), bubble('a-2')]);
    projectLiveInterrupt(rt, toolEvent(), 'a-1', refs);
    rt.pendingInterruptIdsRef.current.clear();
    projectLiveInterrupt(rt, toolEvent(), 'a-2', refs);
    const [first, second] = read() as AssistantMessage[];
    expect(first.contentSegments).toHaveLength(1);
    expect(second.contentSegments).toHaveLength(0);
    expect(rt.pendingInterruptIdsRef.current.has(INTERRUPT_ID)).toBe(true);
  });

  it('puts a settled card back to pending when the same interrupt is re-raised', () => {
    // The click settles the card optimistically, so after a refused resume the
    // card reads "Approved" while the backend still holds the call. The
    // re-raise is the only signal that arrives, and the suppression leaves the
    // fresh bubble with no segment, so the recovery has to land on the
    // original card or the turn hangs with nothing left to answer it.
    const { rt, refs, read } = build([bubble('a-1')]);
    projectLiveInterrupt(rt, batchedToolEvent(), 'a-1', refs);

    const ids = Object.keys((read()[0] as AssistantMessage).toolApprovals || {});
    let current = read();
    current = current.map((m) => ({
      ...(m as AssistantMessage),
      toolApprovals: Object.fromEntries(
        Object.entries((m as AssistantMessage).toolApprovals || {}).map(([id, card]) => [
          id,
          { ...card, status: id === ids[0] ? 'approved' : 'rejected', reason: null },
        ]),
      ),
    })) as unknown as MessageRecord[];

    const second = build([...current, bubble('a-2')]);
    second.rt.renderedInterruptIdsRef.current.add(INTERRUPT_ID);
    projectLiveInterrupt(second.rt, batchedToolEvent(), 'a-2', second.refs);

    const [first, fresh] = second.read() as AssistantMessage[];
    expect(ids.map((id) => first.toolApprovals?.[id]?.status)).toEqual(['pending', 'pending']);
    // No twin card, and nothing unrenderable left on the fresh bubble.
    expect(first.contentSegments).toHaveLength(2);
    expect(fresh.contentSegments || []).toHaveLength(0);
    expect(fresh.toolApprovals).toBeUndefined();
    // Tracked again, so the retried decision is not dropped.
    expect(second.rt.pendingInterruptIdsRef.current.has(INTERRUPT_ID)).toBe(true);
  });
});

describe('history projection', () => {
  function build(messages: MessageRecord[]) {
    let current = messages;
    const rt = {
      setMessages: ((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
        current = updater(current);
      }) as HistoryRuntime['setMessages'],
      renderedInterruptIdsRef: ref(new Set<string>()),
    } as unknown as HistoryRuntime;
    const pairState: PairState = { contentOrderCounter: 0, reasoningId: null, toolCallId: null, steeringBatches: 0 };
    const ctx: HistoryInterruptContext = {
      currentActivePairIndex: 0,
      assistantMessagesByPair: new Map([[0, 'a-1']]),
      pairStateByPair: new Map([[0, pairState]]),
      pendingHistoryInterrupts: [],
      evidence: createApprovalEvidence(),
    };
    return { rt, ctx, read: () => current };
  }

  it('rebuilds the same card and queues a tool_approval entry', () => {
    const { rt, ctx, read } = build([bubble('a-1')]);
    projectHistoryInterrupt(rt, toolEvent({ turn_index: 0 }), ctx);

    const msg = read()[0] as AssistantMessage;
    expect(msg.contentSegments).toEqual([{ type: 'tool_approval', proposalId: INTERRUPT_ID, order: 1 }]);
    expect(msg.toolApprovals?.[INTERRUPT_ID]).toMatchObject({
      status: 'pending',
      server: 'moomoo',
      tool: 'trading_order_place',
      args: ARGS,
    });
    expect(msg.planApprovals).toBeUndefined();
    expect(ctx.pendingHistoryInterrupts).toEqual([
      {
        type: 'tool_approval',
        assistantMessageId: 'a-1',
        proposalId: INTERRUPT_ID,
        interruptId: INTERRUPT_ID,
        target: { kind: 'position', index: 0 },
      },
    ]);
  });


  it('queues one pending entry per stopped call in a batched interrupt', () => {
    // The resolver settles a card per queued entry, so a batch that queued one
    // entry left its other cards pending with live controls after a reload.
    const { rt, ctx, read } = build([bubble('a-1')]);
    projectHistoryInterrupt(rt, batchedToolEvent(), ctx);

    const msg = read()[0] as AssistantMessage;
    expect(msg.contentSegments).toHaveLength(2);
    expect(Object.keys(msg.toolApprovals || {})).toHaveLength(2);
    expect(ctx.pendingHistoryInterrupts.filter((p) => p.type === 'tool_approval')).toHaveLength(2);
  });
  it('settles the card from a claim recorded by a still-running resume', () => {
    const { rt, ctx, read } = build([bubble('a-1')]);
    ctx.evidence.claims.set(INTERRUPT_ID, { answer: null, content: '', batched: false });
    projectHistoryInterrupt(rt, toolEvent(), ctx);

    const msg = read()[0] as AssistantMessage;
    expect(msg.toolApprovals?.[INTERRUPT_ID]?.status).toBe('rejected');
    expect(ctx.pendingHistoryInterrupts).toHaveLength(0);
  });

  it('still routes a SubmitPlan interrupt to the plan card', () => {
    const { rt, ctx, read } = build([bubble('a-1')]);
    projectHistoryInterrupt(rt, planEvent(), ctx);
    const msg = read()[0] as AssistantMessage;
    expect(msg.contentSegments[0].type).toBe('plan_approval');
    expect(ctx.pendingHistoryInterrupts[0].type).toBe('plan_approval');
  });
});
