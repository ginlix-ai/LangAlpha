/**
 * Single source of truth for a subagent's display status.
 *
 * The card's `status` field is authoritative for terminal states — it is only
 * set on genuine settle (stream terminal, backend liveness stamp, or explicit
 * cancel). Everything else displays as running. Transcript shape (a finalized
 * last assistant message) is deliberately NOT treated as completion evidence:
 * a resumed task finalizes its previous run's message while very much alive,
 * which is how the nav tree and the detail header used to disagree.
 */
export type SubagentDisplayStatus =
  | 'initializing'
  | 'active'
  | 'completed'
  | 'cancelled'
  | 'error';

export function deriveSubagentStatus(agent: {
  status?: string;
  messages?: unknown[];
}): SubagentDisplayStatus {
  const status = agent.status;
  if (status === 'completed' || status === 'cancelled' || status === 'error') {
    return status;
  }
  if (!agent.messages || agent.messages.length === 0) return 'initializing';
  return 'active';
}
