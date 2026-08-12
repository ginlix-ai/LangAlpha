/**
 * Pure decision helpers for the manual-compaction Stop control in ChatView.
 * Kept out of the component so the routing / error-classification / staleness
 * rules can be unit-tested without rendering the whole chat surface.
 */

export type StopRoute = 'compaction' | 'workflow';

/**
 * Route the single Stop control. A manual compaction has no streaming turn
 * (isLoading=false) so it tears down the compaction; anything else — a running
 * turn, including an auto Tier-2 summarize inside the stream — tears down the
 * workflow.
 */
export function routeStopAction(opts: {
  isCompacting: string | boolean | null | undefined;
  isLoading: boolean;
}): StopRoute {
  return opts.isCompacting && !opts.isLoading ? 'compaction' : 'workflow';
}

/**
 * A manual compaction (/compact or /offload) is already in flight: the
 * compacting flag is set and there is no streaming turn. Re-firing /compact or
 * /offload in that state would 409 ("compaction_in_progress") on the backend
 * and, worse, the duplicate trigger bumps the generation token (RT#2) before
 * its rejection lands — stranding isCompacting, because the first compaction's
 * completion is now a superseded generation that can no longer clear the flag.
 * So a duplicate trigger must be refused before it enters the generation
 * protocol. During an auto Tier-2 summarize isLoading is true, so this returns
 * false and the action falls through to the backend's workflow_active gate.
 */
export function isManualCompactionInFlight(opts: {
  isCompacting: string | boolean | null | undefined;
  isLoading: boolean;
}): boolean {
  return routeStopAction(opts) === 'compaction';
}

/** Pull a structured detail code from an axios-style rejection, if present. */
export function compactionErrorCode(err: unknown): string | undefined {
  return (
    err as { response?: { data?: { detail?: { code?: string } } } } | undefined
  )?.response?.data?.detail?.code;
}

/**
 * Did the user stop this manual compaction (vs. it genuinely erroring)? The
 * backend's cancellation wrapper tags a user-cancelled request with a
 * structured 409 {code:"request_cancelled"}; honor that even when the local
 * "user stopped" flag was already consumed by a rapid stop→retrigger.
 */
export function isUserStoppedCompaction(opts: {
  userStopped: boolean;
  errorCode?: string;
}): boolean {
  return opts.userStopped || opts.errorCode === 'request_cancelled';
}

/**
 * Only the most recent compaction owns the isCompacting flag. A late
 * resolution/rejection from a superseded compaction must not flip the flag and
 * unmask the input while a newer compaction is still running (RT#2).
 */
export function shouldClearCompactingFlag(
  myGeneration: number,
  currentGeneration: number,
): boolean {
  return myGeneration === currentGeneration;
}
