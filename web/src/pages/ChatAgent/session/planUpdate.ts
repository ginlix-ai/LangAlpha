/**
 * The agent's plan, read from its `plan_update` artifacts (`UpdatePlan`; an
 * older thread's `TodoWrite` replays in the same shape). The stream, a replay
 * and a share keep a plan the same way: a snapshot per update on the bubble.
 */
import type { PlanSnapshot, PlanStep } from '@/types/chat';
import type { SetMessages } from '../hooks/utils/types';

const PLAN_UPDATE = 'plan_update';

const STATUSES: ReadonlySet<string> = new Set<PlanStep['status']>(['pending', 'in_progress', 'completed']);

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object';
}

function statusOf(value: unknown): PlanStep['status'] {
  return typeof value === 'string' && STATUSES.has(value) ? (value as PlanStep['status']) : 'pending';
}

/** The steps an artifact event carries, or null when it carries no plan. */
export function readPlanSteps(artifactType: unknown, payload: unknown): PlanStep[] | null {
  if (artifactType !== PLAN_UPDATE) return null;
  const steps = isRecord(payload) ? payload.steps : null;
  return (Array.isArray(steps) ? steps : []).filter(isRecord).map((item) => ({
    title: typeof item.title === 'string' ? item.title : '',
    status: statusOf(item.status),
  }));
}

/**
 * Sets one plan snapshot on the bubble `bubbleId`. `order` is the event's
 * place in its turn (the backend event id where there is one, so live, a
 * reconnect and a replay agree), which a steering split cuts the snapshots by.
 * A frame delivered again lands on the same order and replaces its first copy.
 */
export function applyPlanSnapshot(setMessages: SetMessages, bubbleId: string, steps: PlanStep[], order: number): void {
  setMessages((prev) => prev.map((msg) => {
    if (msg.id !== bubbleId) return msg;
    // A record is loosely typed: typing the field on it would break every
    // structural reader of the record (`MessageLike` and its kin).
    const snapshots: PlanSnapshot[] = Array.isArray(msg.planSnapshots) ? msg.planSnapshots : [];
    const others = snapshots.filter((snapshot) => snapshot.order !== order);
    return { ...msg, planSnapshots: [...others, { order, steps }] };
  }));
}
