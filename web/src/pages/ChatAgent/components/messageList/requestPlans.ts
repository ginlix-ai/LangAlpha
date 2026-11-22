/**
 * The plan each request set, read from the transcript once per change of it
 * (`threadPlans`). The tray, the transcript and the Status panel all read the
 * result, so a plan is in one place.
 */
import type { TFunction } from 'i18next';
import type { PlanSnapshot, PlanStep } from '@/types/chat';
import { isReportRow, opensTurn, type MessageLike } from './messagePredicates';

/** What the plans are read from: a message's turn role and its snapshots. */
export type PlanMessage = MessageLike & {
  id?: unknown;
  provenance?: unknown;
  timestamp?: unknown;
  planSnapshots?: readonly PlanSnapshot[];
};

/** When the newest request began (epoch ms): the time of the message that
 *  asked for it, null where that message is not loaded. A report-back's row
 *  carries the request on rather than starting one. */
export function requestBegan(messages: readonly PlanMessage[]): number | null {
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const message = messages[i];
    if (!opensTurn(message) || isReportRow(message)) continue;
    return message.timestamp instanceof Date ? message.timestamp.getTime() : null;
  }
  return null;
}

/** The plan as it stands on a bubble: every update is a snapshot, and the
 *  newest one (highest order, the later on a tie) is the one the reader follows. */
export function latestPlanSteps(snapshots: readonly PlanSnapshot[] | undefined): PlanStep[] {
  let newest: PlanSnapshot | null = null;
  for (const snapshot of snapshots ?? []) {
    if (!newest || snapshot.order >= newest.order) newest = snapshot;
  }
  return newest?.steps ?? [];
}

/**
 * Where a plan stands: `live` while its request's turn runs, `waiting` once
 * the turn ended on work that will bring the agent back to it (a subagent, a
 * command, an Analyst, a wakeup, a question to the reader), and `ended` once
 * nothing will. Only an ended plan's unfinished steps read as not done.
 */
export type PlanState = 'live' | 'waiting' | 'ended';

export interface ShownPlan {
  steps: PlanStep[];
  state: PlanState;
}

interface RequestPlan {
  /** The message that asked for the work, null where it is not loaded. */
  openerId: string | null;
  steps: PlanStep[];
}

/**
 * Each request's plan, newest snapshot wins: a request runs from a message
 * the user sent until the next one, through every turn between, report-back
 * turns included, since they carry the same work on. Every request has an
 * entry, empty steps for one that set no plan, in thread order, so the last
 * is the request being worked now. The loaded history can start inside a
 * request (at a report-back or wake turn whose opener is on an older page), so
 * the transcript opens on a request with no anchor, which still holds a plan.
 */
function plansByRequest(messages: readonly PlanMessage[]): RequestPlan[] {
  let current: RequestPlan = { openerId: null, steps: [] };
  const plans: RequestPlan[] = [current];
  for (const message of messages) {
    if (opensTurn(message) && !isReportRow(message)) {
      current = { openerId: typeof message.id === 'string' ? message.id : null, steps: [] };
      plans.push(current);
      continue;
    }
    if (message.role !== 'assistant') continue;
    const steps = latestPlanSteps(message.planSnapshots);
    if (steps.length > 0) current.steps = steps;
  }
  return plans;
}

function hasStepLeft(steps: readonly PlanStep[]): boolean {
  return steps.some((step) => step.status !== 'completed');
}

export interface ThreadPlans {
  /** The running request's steps while one is left, which the composer tray
   *  follows. Once the last is done the agent is still writing its answer
   *  (or a report-back turn carries the request on), and the plan belongs
   *  back under the message that asked for it. */
  tray: PlanStep[] | null;
  /** The thread's latest plan, for the Status panel: kept after its request
   *  ends until a new one replaces it. */
  latest: ShownPlan | null;
  /** The plans drawn under the messages that asked for them, by message id:
   *  all but the one in the tray, and one whose message is not loaded. */
  underMessages: ReadonlyMap<string, ShownPlan>;
}

/**
 * Every plan the thread shows, and where. Only the last request's plan can
 * still move: `live` while a turn runs, else `waiting` while `waiting` holds.
 * The tray takes a plan only while loading, so `waiting` never decides it.
 */
export function threadPlans(messages: readonly PlanMessage[], isLoading: boolean, waiting: boolean): ThreadPlans {
  const plans = plansByRequest(messages);
  const lastState: PlanState = isLoading ? 'live' : waiting ? 'waiting' : 'ended';
  const last = plans.at(-1);
  const tray = isLoading && last && hasStepLeft(last.steps) ? last : null;
  let latest: ShownPlan | null = null;
  const underMessages = new Map<string, ShownPlan>();
  for (const plan of plans) {
    if (plan.steps.length === 0) continue;
    const shown: ShownPlan = { steps: plan.steps, state: plan === last ? lastState : 'ended' };
    latest = shown;
    if (plan !== tray && plan.openerId !== null) underMessages.set(plan.openerId, shown);
  }
  return { tray: tray?.steps ?? null, latest, underMessages };
}

/** How far along a plan is: steps done, steps in all, and the step being
 *  worked (the one in progress, else the first not started), -1 when none is. */
export function planProgress(steps: readonly PlanStep[]): { done: number; total: number; current: number } {
  const running = steps.findIndex((step) => step.status === 'in_progress');
  return {
    done: steps.filter((step) => step.status === 'completed').length,
    total: steps.length,
    current: running >= 0 ? running : steps.findIndex((step) => step.status === 'pending'),
  };
}

/** A plan's progress as its heading reads it, after the plan's own name:
 *  the step a running turn is on, else how many steps got done. */
export function planHeading(steps: readonly PlanStep[], state: PlanState, t: TFunction): string {
  const { done, total, current } = planProgress(steps);
  return state === 'live' && current >= 0
    ? t('chat.plan.headingRunning', { current: current + 1, total })
    : t('chat.plan.headingSettled', { done, total });
}

export function stepTitle(step: PlanStep, index: number): string {
  return step.title.trim() || `${index + 1}`;
}
