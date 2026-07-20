/**
 * Single source of truth for a subagent's display status.
 *
 * Three non-terminal phases, in order of authority:
 *   - terminal (`completed`/`cancelled`/`error`): immutable; only a genuine
 *     settle (stream terminal, backend liveness stamp, explicit cancel) sets it.
 *   - explicit live (`active`/`running`): a positive liveness signal — a task
 *     event, an `active_tasks` snapshot, or an accepted resume — wins over
 *     transcript shape, so a known-running card never regresses to
 *     "Initializing" just because its messages haven't accumulated locally yet.
 *   - `initializing`: spawned, but no positive signal has arrived. Streamed
 *     content IS a positive signal, so a card mid-transcript is promoted even
 *     if a late status write still reads 'initializing'.
 *
 * Transcript shape (a finalized last assistant message) is deliberately NOT
 * treated as completion evidence: a resumed task finalizes its previous run's
 * message while very much alive, which is how the nav tree and the detail header
 * used to disagree. Message shape is only the legacy fallback for a card whose
 * status is missing/unknown.
 */
export type SubagentDisplayStatus =
  | 'initializing'
  | 'active'
  | 'completed'
  | 'cancelled'
  | 'error';

const TERMINAL_STATUSES: ReadonlySet<string> = new Set(['completed', 'cancelled', 'error']);

/**
 * A subagent status is terminal when the task has settled — completed, cancelled,
 * or errored. Terminal status is authoritative and monotonic: once observed, no
 * stale-liveness signal may revert a card back to a running/initializing state.
 */
export function isTerminalStatus(status: string | undefined | null): boolean {
  return status != null && TERMINAL_STATUSES.has(status);
}

export function deriveSubagentStatus(agent: {
  status?: string;
  messages?: unknown[];
}): SubagentDisplayStatus {
  const status = agent.status;
  if (status === 'completed' || status === 'cancelled' || status === 'error') {
    return status;
  }
  if (status === 'active' || status === 'running') return 'active';
  // Explicit 'initializing' and missing/legacy status share the same rule:
  // promote to 'active' once any content has streamed, else hold 'initializing'.
  const hasMessages = !!agent.messages && agent.messages.length > 0;
  return hasMessages ? 'active' : 'initializing';
}
