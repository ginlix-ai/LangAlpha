/**
 * The plan branch is the fallthrough of both interrupt projections, so every
 * more specific branch ahead of it is a chance to swallow a `SubmitPlan`. These
 * two cases pin the boundary: a plan interrupt reaches the plan card, and
 * writes nothing into the tool-approval bucket on the way.
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

function planEvent(): SSEEvent {
  return {
    type: 'interrupt',
    interrupt_id: 'plan-1',
    action_requests: [{ name: 'SubmitPlan', description: '# The plan' }],
  } as unknown as SSEEvent;
}

const bubble = (id: string): MessageRecord =>
  ({ id, role: 'assistant', content: '', contentSegments: [] }) as unknown as MessageRecord;

describe('plan interrupt routing', () => {
  it('routes a SubmitPlan interrupt to the plan card on the live path', () => {
    let current = [bubble('a-1')];
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

    projectLiveInterrupt(rt, planEvent(), 'a-1', refs);

    const msg = current[0] as AssistantMessage;
    expect(msg.contentSegments[0].type).toBe('plan_approval');
    expect(msg.toolApprovals).toBeUndefined();
    expect(msg.planApprovals?.['plan-1']?.description).toBe('# The plan');
  });

  it('routes a SubmitPlan interrupt to the plan card on the history path', () => {
    let current = [bubble('a-1')];
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

    projectHistoryInterrupt(rt, planEvent(), ctx);

    const msg = current[0] as AssistantMessage;
    expect(msg.contentSegments[0].type).toBe('plan_approval');
    expect(msg.toolApprovals).toBeUndefined();
    expect(ctx.pendingHistoryInterrupts[0].type).toBe('plan_approval');
  });
});
