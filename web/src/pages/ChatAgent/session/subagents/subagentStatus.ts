import { isDirectToolName } from '../../utils/directTools';

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

// What separates the failure envelope from its detail, across every producer
// that reaches the prefix rule: ':' for the "ERROR: "/"Error: " convention the
// tools, middleware and Task/RunWorkflow refusals share, and '\n' for
// ExecuteCode's "ERROR\n<traceback>". A space is deliberately absent: it is
// what an English sentence starts with, and admitting it is the whole false
// positive.
const ERROR_DELIMITERS: ReadonlySet<string> = new Set([':', '\n']);

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
 * The one predicate that decides whether a tool result is a failure, for every
 * consumer (timeline row, render blocks, detail header, subagent settle).
 *
 * An explicit `status` of 'error' decides failure outright, and a result
 * carrying an artifact is never a failure. An explicit 'success' only silences
 * the "Refused:" rule, which is the heuristic that value was carried on the
 * wire to fix (a direct MCP tool whose own successful output opens with that
 * prefix; every other tool is free to print it too, so the rule stays off
 * without a resolved `toolName`). The "Error" prefix rule keeps firing under
 * any status, case-insensitively, because producers demonstrably do not all
 * stamp their failures and a defaulted 'success' would silence the only settle
 * signal a failed Task launch has: it opens no channel, so no chan_close ever
 * arrives and the card spins forever.
 *
 * That prefix has to be the failure envelope and not the first word of a
 * sentence, so "error" only counts when the text ends there or continues with
 * one of ERROR_DELIMITERS. Every producer that reaches this rule writes one of
 * those; ordinary output that merely opens with the word ("Error rate: 0%" out
 * of a Bash command that exited 0) continues with a letter and is left alone.
 *
 * `toolName` is the raw wire name (`mcp__<server>__<tool>` for a direct tool).
 */
export function isToolResultFailure(result: {
  content?: unknown;
  artifact?: unknown;
  status?: unknown;
  toolName?: unknown;
}): boolean {
  if (result.status === 'error') return true;
  const claimsSuccess = typeof result.status === 'string' && result.status !== '';
  if (typeof result.content !== 'string' || result.artifact) return false;
  const text = result.content.trim().toLowerCase();
  // `undefined` past the end is the whole result being the bare envelope,
  // which is what an empty ExecuteCode stderr trims down to.
  if (text.slice(0, 5) === 'error') {
    const next = text[5];
    if (next === undefined || ERROR_DELIMITERS.has(next)) return true;
  }
  if (claimsSuccess) return false;
  return (
    text.startsWith('refused:') &&
    typeof result.toolName === 'string' &&
    isDirectToolName(result.toolName)
  );
}

/**
 * The raw wire tool name recorded when the call streamed, read back out of a
 * message's tool-call map. Absent when the result outran its `tool_calls`
 * event and no name has been recorded yet.
 */
export function toolNameOf(
  processes: Record<string, Record<string, unknown>>,
  toolCallId: string,
): string | undefined {
  const name = processes[toolCallId]?.toolName;
  return typeof name === 'string' ? name : undefined;
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

/** The only agent fields a nav-tree row renders. Rows are projected once at
 * the publisher (useSubagentTabs) with identity kept stable while these
 * fields are unchanged — that stability is what keeps streamed subagent
 * chunks from re-rendering the whole tree. */
export interface SidebarAgentRow {
  id: string;
  name: string;
  description: string;
  isMainAgent: boolean;
  status: SubagentDisplayStatus;
}

/** What a row is called: a subagent by its task's description when it has one. */
export function sidebarAgentRowLabel(row: SidebarAgentRow): string {
  const description = row.description.trim();
  return !row.isMainAgent && description ? description : row.name;
}

export function toSidebarAgentRow(agent: {
  id: string;
  name: string;
  description?: string;
  isMainAgent?: boolean;
  status?: string;
  messages?: unknown[];
}): SidebarAgentRow {
  return {
    id: agent.id,
    name: agent.name,
    // JSON wire shape may carry null (or other non-strings) — normalize here
    // so row consumers can trim/render without re-guarding.
    description: typeof agent.description === 'string' ? agent.description : '',
    isMainAgent: !!agent.isMainAgent,
    status: agent.isMainAgent ? 'active' : deriveSubagentStatus(agent),
  };
}

export function sidebarAgentRowsEqual(
  a: readonly SidebarAgentRow[],
  b: readonly SidebarAgentRow[],
): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    const y = b[i];
    if (
      x.id !== y.id ||
      x.name !== y.name ||
      x.description !== y.description ||
      x.isMainAgent !== y.isMainAgent ||
      x.status !== y.status
    ) {
      return false;
    }
  }
  return true;
}
