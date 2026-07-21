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

export type SubagentTerminalStatus = 'completed' | 'cancelled' | 'error';

const TERMINAL_STATUSES: ReadonlySet<string> = new Set(['completed', 'cancelled', 'error']);

/**
 * A subagent status is terminal when the task has settled — completed, cancelled,
 * or errored. Terminal status is authoritative and monotonic: once observed, no
 * stale-liveness signal may revert a card back to a running/initializing state.
 */
export function isTerminalStatus(
  status: string | undefined | null,
): status is SubagentTerminalStatus {
  return status != null && TERMINAL_STATUSES.has(status);
}

/**
 * A tool result reports failure iff it is the bare failure ToolMessage:
 * string content prefixed "Error" (backend convention — e.g. "Error: could
 * not start Task-…") with no artifact attached. A result carrying an
 * artifact is never a failure. For Task results this is the settle-or-spin
 * discriminator: a failed spawn opens no channel, so no chan_close will ever
 * arrive — the caller must stamp 'error' from this signal alone.
 */
export function isToolResultFailure(result: {
  content?: unknown;
  artifact?: unknown;
}): boolean {
  return (
    typeof result.content === 'string' &&
    result.content.trim().startsWith('Error') &&
    !result.artifact
  );
}

/**
 * Normalize a backend wire status (run-ledger or legacy spellings) into the
 * display vocabulary. 'failed' and 'interrupted' collapse to 'error' — task
 * HITL is descoped, so an interrupted task run is a failure, matching the
 * server's history stamping. Live spellings collapse to 'active'. Unknown or
 * absent values return null so callers keep their own default instead of
 * inventing a settle.
 */
export function normalizeWireStatus(
  status: string | undefined | null,
): SubagentDisplayStatus | null {
  switch (status) {
    case 'completed':
    case 'cancelled':
      return status;
    case 'error':
    case 'failed':
    case 'interrupted':
      return 'error';
    case 'in_progress':
    case 'running':
    case 'active':
      return 'active';
    case 'initializing':
      return 'initializing';
    default:
      return null;
  }
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
