import { registerAuthReset } from '@/lib/authResets';
import { threadFeedObservation } from '@/lib/threadLifecycle/store';
import type { WorkflowStatusResponse } from './api/messages';
import { getWorkflowStatus } from './api/workflowStatus';

/** How long a status asked for at boot still counts as this load's snapshot. */
const PRIMED_MAX_AGE_MS = 10_000;

/** What the read was sent against: the user feed's last word on the thread. */
type StatusRead = { status: Promise<WorkflowStatusResponse>; sentAt: number; feed: string };

const primed = new Map<string, StatusRead>();

// An answer read for one account is never the next one's.
registerAuthReset(() => primed.clear());

/**
 * Asks for a thread's status at boot, beside the lookup of its workspace,
 * rather than once the view has mounted on that workspace: on a cold open of
 * a thread link the two reads otherwise ran a round trip apart. The history
 * load still reads its replay only after this status answers, so the order
 * the reconnect relies on holds.
 */
export function primeWorkflowStatus(threadId: string): void {
  if (primed.has(threadId)) return;
  const status = getWorkflowStatus(threadId);
  // Settled here too, so a status nobody takes is never an unhandled rejection.
  status.catch(() => undefined);
  primed.set(threadId, { status, sentAt: Date.now(), feed: feedMark(threadId) });
}

/**
 * A history load's status and its snapshot moment, the time it was asked for,
 * since the server's answer reflects the world no earlier than that. The one
 * primed at boot is taken once and only while fresh; a later load, or one
 * whose primed read failed, asks again with `ask`, the caller's own read.
 *
 * A primed answer is asked again when the user feed has reported anything
 * about the thread since it was sent. A run another tab or an automation
 * started, or started and finished, between boot and the view's mount is one
 * the view's catch-up takes as seen, so this read is the only one that would
 * show it, with the token totals and the rest the status carries.
 */
export async function readWorkflowStatus(
  threadId: string,
  ask: (threadId: string) => Promise<WorkflowStatusResponse>,
): Promise<{ status: WorkflowStatusResponse; sentAt: number }> {
  const entry = primed.get(threadId);
  primed.delete(threadId);
  if (entry && Date.now() - entry.sentAt <= PRIMED_MAX_AGE_MS) {
    try {
      const status = await entry.status;
      if (feedMark(threadId) === entry.feed) return { status, sentAt: entry.sentAt };
    } catch {
      // Asked again below, as a load with nothing primed would.
    }
  }
  const sentAt = Date.now();
  return { status: await ask(threadId), sentAt };
}

function feedMark(threadId: string): string {
  const seen = threadFeedObservation(threadId);
  return seen ? `${seen.runId ?? ''}:${seen.runSeq ?? ''}:${seen.status}` : '';
}
