/**
 * Reading a plan back out of an older thread. The agent no longer has
 * `SubmitPlan`, but threads written before it lost the tool still carry its
 * calls, and the call is the plan's only record now.
 */
import type { ToolCallProcessRecord } from './types';
import type { ToolCallData, ToolCallResultData } from './activityTypes';

/** What became of a submitted plan. `unreviewed` is a plan whose review was
 *  never answered, which nothing can answer now. */
export type PastPlanOutcome = 'approved' | 'rejected' | 'unreviewed';

/** `submit_plan` is the same tool in threads from before the PascalCase rename. */
const PLAN_TOOL_NAMES = new Set(['SubmitPlan', 'submit_plan']);

/** The only reply the plan tool itself ever gave, and it ran only once the
 *  plan was approved. */
const PLAN_APPROVED_REPLY = 'Plan approved.';
/** What the agent runtime leaves on a call whose review was never answered:
 *  "was cancelled" from older deepagents, "did not complete" from newer ones,
 *  which also mark it as an error. Its "could not be executed" line (malformed
 *  arguments) is deliberately absent, since no review was waiting there. */
const CALL_LEFT_UNANSWERED = /^Tool call \S+ with id \S+ (?:was cancelled|did not complete)\b/;

export function isPlanTool(name: unknown): boolean {
  return typeof name === 'string' && PLAN_TOOL_NAMES.has(name);
}

/** A plan's result is one of three things: the approval reply, the review's
 *  rejection (the reviewer's feedback or a stock line), or the unanswered-call
 *  line above. Rejection is read as what remains rather than from the error
 *  status alone, because results recorded before tool status was streamed
 *  carry none, and there a rejection looks like any other reply. The
 *  unanswered-call line is tested before the status because newer runtimes
 *  stamp it as an error, which would otherwise read as a rejection. */
function pastPlanOutcome(proc: ToolCallProcessRecord): PastPlanOutcome {
  const result = proc.toolCallResult as ToolCallResultData | null | undefined;
  if (!result) return 'unreviewed';
  const content = typeof result.content === 'string' ? result.content.trim() : '';
  if (CALL_LEFT_UNANSWERED.test(content)) return 'unreviewed';
  if (proc.isFailed) return 'rejected';
  if (content.startsWith(PLAN_APPROVED_REPLY)) return 'approved';
  if (!content) return 'unreviewed';
  return 'rejected';
}

/** Null when the call carries no plan text, which leaves nothing to show. */
export function readPastPlan(
  proc: ToolCallProcessRecord,
): { description: string; outcome: PastPlanOutcome } | null {
  const description = (proc.toolCall as ToolCallData | undefined)?.args?.description;
  if (typeof description !== 'string' || !description.trim()) return null;
  return { description, outcome: pastPlanOutcome(proc) };
}
