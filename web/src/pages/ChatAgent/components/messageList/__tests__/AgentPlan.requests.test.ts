/**
 * A request's plan: one per message the user sent, kept through the turns
 * that carry it on, report-backs included. The tray and the transcript read
 * the same plans, and the tray takes a request's plan only while a step is
 * left, so the transcript hides only the one the tray holds.
 */
import { describe, expect, it } from 'vitest';
import type { ChatMessage, PlanStep } from '@/types/chat';
import { createAssistantMessage, createUserMessage } from '../../../hooks/utils/messageHelpers';
import { latestPlanSteps, planProgress, threadPlans } from '../requestPlans';

function ask(id: string, extra: Partial<ChatMessage> = {}): ChatMessage {
  return { ...createUserMessage(id, undefined, { id }), ...extra } as ChatMessage;
}

function reply(id: string, ...snapshots: PlanStep['status'][][]): ChatMessage {
  return {
    ...createAssistantMessage(id, undefined),
    planSnapshots: snapshots.map((statuses, order) => ({
      order,
      steps: statuses.map((status, i) => ({ title: `Step ${i + 1}`, status })),
    })),
  };
}

const statuses = (steps: readonly { status: string }[] | undefined) => (steps ?? []).map((step) => step.status);

describe('latestPlanSteps', () => {
  it('follows the highest order, the later of two at the same order', () => {
    const steps = (status: PlanStep['status']) => [{ title: 'x', status }];
    expect(statuses(latestPlanSteps([
      { order: 5, steps: steps('completed') },
      { order: 3, steps: steps('pending') },
      { order: 5, steps: steps('in_progress') },
    ]))).toEqual(['in_progress']);
    expect(latestPlanSteps(undefined)).toEqual([]);
  });
});

describe('threadPlans: one plan per request', () => {
  it('keeps the newest snapshot of each request', () => {
    const { latest, underMessages } = threadPlans([ask('u1'), reply('a1', ['in_progress', 'pending'], ['completed', 'in_progress'])], false, false);
    expect(statuses(latest?.steps)).toEqual(['completed', 'in_progress']);
    expect([...underMessages.keys()]).toEqual(['u1']);
  });

  it('carries a plan through report-back turns of the same request', () => {
    const { underMessages } = threadPlans([
      ask('u1'),
      reply('a1', ['in_progress', 'pending']),
      ask('r1', { provenance: { kind: 'task_report' } } as Partial<ChatMessage>),
      reply('a2', ['completed', 'completed']),
    ], false, false);
    expect([...underMessages.keys()]).toEqual(['u1']);
    expect(statuses(underMessages.get('u1')?.steps)).toEqual(['completed', 'completed']);
  });

  it('starts a new request at the next message the user sent, and keeps the latest plan the thread set', () => {
    const { latest, underMessages } = threadPlans([ask('u1'), reply('a1', ['completed']), ask('u2'), reply('a2')], false, false);
    expect([...underMessages.keys()]).toEqual(['u1']);
    expect(statuses(latest?.steps)).toEqual(['completed']);
  });

  it('keeps a steer inside the request it was typed into', () => {
    const { underMessages } = threadPlans([
      ask('u1'),
      reply('a1', ['in_progress', 'pending']),
      ask('s1', { steeringDelivered: true } as Partial<ChatMessage>),
      reply('a2', ['completed', 'in_progress']),
    ], false, false);
    expect([...underMessages.keys()]).toEqual(['u1']);
    expect(statuses(underMessages.get('u1')?.steps)).toEqual(['completed', 'in_progress']);
  });
});

describe('threadPlans: a page that starts inside a request', () => {
  // The newest page of history can open at a report-back or wake turn whose
  // user message is on an older page not loaded yet.
  const page = [
    ask('r1', { provenance: { kind: 'task_report' } } as Partial<ChatMessage>),
    reply('a1', ['completed', 'in_progress', 'pending']),
  ];

  it('still keeps the plan for the Status panel, with nothing to draw it under', () => {
    const { latest, underMessages } = threadPlans(page, false, true);
    expect(statuses(latest?.steps)).toEqual(['completed', 'in_progress', 'pending']);
    expect(latest?.state).toBe('waiting');
    expect(underMessages.size).toBe(0);
  });

  it('puts it in the tray while the turn runs', () => {
    expect(statuses(threadPlans(page, true, false).tray ?? undefined)).toEqual(['completed', 'in_progress', 'pending']);
  });

  it('ends it once the user asks for something new', () => {
    const { latest, underMessages } = threadPlans([...page, ask('u2'), reply('a2')], false, false);
    expect(latest?.state).toBe('ended');
    expect(underMessages.size).toBe(0);
  });
});

describe('threadPlans: the tray and the transcript', () => {
  it('moves a running plan to the tray and leaves earlier ones in the transcript', () => {
    const { tray, latest, underMessages } = threadPlans([ask('u1'), reply('a1', ['completed']), ask('u2'), reply('a2', ['in_progress', 'pending'])], true, false);
    expect(statuses(tray ?? [])).toEqual(['in_progress', 'pending']);
    expect([...underMessages.keys()]).toEqual(['u1']);
    // The panel follows the same steps the tray holds.
    expect(latest?.steps).toBe(tray);
  });

  it('keeps a finished plan in the transcript while its turn writes the answer', () => {
    const { tray, underMessages } = threadPlans([ask('u1'), reply('a1', ['completed', 'completed'])], true, false);
    expect(tray).toBeNull();
    expect(underMessages.get('u1')).toEqual({ steps: expect.any(Array), state: 'live' });
  });

  it('keeps a finished plan in the transcript through a report-back turn', () => {
    const { tray, underMessages } = threadPlans([
      ask('u1'),
      reply('a1', ['completed', 'completed']),
      ask('r1', { provenance: { kind: 'task_report' } } as Partial<ChatMessage>),
      reply('a2'),
    ], true, false);
    expect(tray).toBeNull();
    expect(underMessages.has('u1')).toBe(true);
  });

  it('holds nothing in the tray once the turn ends, waiting or not', () => {
    const messages = [ask('u1'), reply('a1', ['in_progress', 'pending'])];
    expect(threadPlans(messages, false, false).tray).toBeNull();
    expect(threadPlans(messages, false, true).tray).toBeNull();
    expect(threadPlans(messages, false, false).underMessages.has('u1')).toBe(true);
  });
});

describe('threadPlans: where an unfinished plan stands', () => {
  const messages = [
    ask('u1'), reply('a1', ['completed', 'in_progress']),
    ask('u2'), reply('a2', ['in_progress', 'pending']),
  ];

  it('waits while something will bring the agent back', () => {
    const { latest, underMessages } = threadPlans(messages, false, true);
    expect(underMessages.get('u2')?.state).toBe('waiting');
    expect(latest?.state).toBe('waiting');
  });

  it('ends once nothing will', () => {
    const { latest, underMessages } = threadPlans(messages, false, false);
    expect(underMessages.get('u2')?.state).toBe('ended');
    expect(latest?.state).toBe('ended');
  });

  it('ends for every request but the last, waiting or not', () => {
    expect(threadPlans(messages, false, true).underMessages.get('u1')?.state).toBe('ended');
  });

  it('is live while its turn runs', () => {
    expect(threadPlans(messages, true, true).latest?.state).toBe('live');
  });
});

describe('planProgress', () => {
  const plan = (...list: PlanStep['status'][]) => list.map((status, i) => ({ title: `Step ${i + 1}`, status }));

  it('counts the steps done and names the one in progress', () => {
    expect(planProgress(plan('completed', 'in_progress', 'pending'))).toEqual({ done: 1, total: 3, current: 1 });
  });

  it('names the first step not started between steps, and none once all are done', () => {
    expect(planProgress(plan('completed', 'pending', 'pending')).current).toBe(1);
    expect(planProgress(plan('completed', 'completed')).current).toBe(-1);
  });
});
