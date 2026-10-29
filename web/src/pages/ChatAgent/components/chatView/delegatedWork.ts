/**
 * The work a thread handed off, as the tray lists it: each task and each
 * Analyst it dispatched, kept while it runs and for a few turns after it last
 * did anything, so the reader can jump back to it. Each item ages on its own
 * clock. Pure, so the rules are pinned without rendering anything.
 */
import type { ChatMessage } from '@/types/chat';
import {
  deriveSubagentStatus,
  isTerminalStatus,
  type SidebarAgentRow,
} from '../../session/subagents/subagentStatus';
import type { ReportBackOwedThread } from '../../utils/api';
import { preapprovedCardOf } from '../../utils/preapprovedCards';
import { taskIdFromAgentId } from '../../utils/agentId';
import { isReportKind } from '../messageList/messagePredicates';

/** Counted turns an item stays for after it was last active. */
export const ROSTER_IDLE_TURNS = 5;

/**
 * When each turn that started new work here began (epoch ms): typed here,
 * from a chat app, an automation, or handed in by another agent. A report-back
 * only brings finished work in, and steering joins a turn already running. A
 * turn counts once the agent answers it: a send the server refused, or that
 * failed before any reply arrived, keeps its bubble but started nothing.
 */
export function countedTurnStarts(messages: readonly ChatMessage[]): number[] {
  const starts: number[] = [];
  let unanswered: number | null = null;
  for (const m of messages) {
    if (m.role === 'assistant') {
      // A failed send's local error reply, where a refusal removes the reply.
      if (m.error && m.contentSegments.length === 0 && Object.keys(m.toolCallProcesses).length === 0) continue;
      if (unanswered !== null) starts.push(unanswered);
      unanswered = null;
      continue;
    }
    if (m.role !== 'user' || m.steering || m.steeringDelivered || m.queued) continue;
    // A steer the server refused, left in place with its error.
    if ((m as { queueError?: unknown }).queueError) continue;
    const kind = m.provenance?.kind;
    unanswered = isReportKind(kind) ? null : m.timestamp.getTime();
  }
  return starts;
}

/**
 * Whether an item stays: while it runs, then until ROSTER_IDLE_TURNS counted
 * turns have started since it was last active. Without a known activity time
 * it shows only while it runs.
 */
export function staysOnRoster(
  running: boolean,
  lastActivityMs: number | null,
  turnStarts: readonly number[],
): boolean {
  if (running) return true;
  if (lastActivityMs == null) return false;
  let since = 0;
  for (const start of turnStarts) if (start > lastActivityMs) since += 1;
  return since < ROSTER_IDLE_TURNS;
}

function latest(...times: (number | null | undefined)[]): number | null {
  let best: number | null = null;
  for (const t of times) if (typeof t === 'number' && Number.isFinite(t) && (best === null || t > best)) best = t;
  return best;
}

export interface RosterTask extends SidebarAgentRow {
  running: boolean;
}

/** The fields of a live card the roster reads. */
interface TaskCardLike {
  subagentData?: {
    displayId?: string;
    description?: string;
    status?: string;
    messages?: unknown[];
    ownerTaskId?: string;
    lastActivityMs?: number;
  };
}

/** The fields of a history entry the roster reads. */
interface TaskHistoryLike {
  description: string;
  status: string;
  messages: unknown[];
  ownerTaskId?: string;
  lastActivityMs?: number;
  projectedRunStartedMs?: number;
}

interface TaskDraft {
  name: string;
  description: string;
  status: SidebarAgentRow['status'];
  lastActivityMs: number | null;
}

/**
 * The thread's tasks on the roster, running ones first, then the most
 * recently active. A task is known from its live card, from the history the
 * page loaded, or from its report-back row; a workflow's own steps are reached
 * through the workflow's card and stay off. `workRunning` is whether anything
 * can be running at all: a task's status alone can read live after its run
 * ended unseen.
 */
export function taskRoster({
  cards,
  history,
  messages,
  turnStarts,
  workRunning,
}: {
  cards: Readonly<Record<string, TaskCardLike>>;
  history: Readonly<Record<string, TaskHistoryLike>>;
  messages: readonly ChatMessage[];
  turnStarts: readonly number[];
  workRunning: boolean;
}): RosterTask[] {
  const drafts = new Map<string, TaskDraft>();

  for (const [agentId, entry] of Object.entries(history)) {
    if (entry.ownerTaskId) continue;
    drafts.set(agentId, {
      name: `Task-${taskIdFromAgentId(agentId) ?? agentId}`,
      description: entry.description,
      status: deriveSubagentStatus({ status: entry.status, messages: entry.messages }),
      // Before the ledger reported activity, the newest run's start.
      lastActivityMs: latest(entry.lastActivityMs ?? entry.projectedRunStartedMs),
    });
  }

  for (const [cardId, card] of Object.entries(cards)) {
    if (!cardId.startsWith('subagent-')) continue;
    const agentId = cardId.slice('subagent-'.length);
    const sd = card.subagentData;
    // A card seeded on reload before its workflow replayed has no owner yet.
    if (!sd || sd.ownerTaskId || history[agentId]?.ownerTaskId || agentId === 'main') continue;
    const known = drafts.get(agentId);
    drafts.set(agentId, {
      name: sd.displayId || known?.name || `Task-${taskIdFromAgentId(agentId) ?? agentId}`,
      description: sd.description || known?.description || '',
      // The card is live; history is the page load.
      status: deriveSubagentStatus({ status: sd.status ?? 'initializing', messages: sd.messages }),
      lastActivityMs: latest(sd.lastActivityMs, known?.lastActivityMs),
    });
  }

  // Newest first, so the latest report is the one that says how a task ended.
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const m = messages[i];
    if (m.role !== 'user' || m.provenance?.kind !== 'task_report') continue;
    const { taskId, displayId, description, failed } = m.provenance;
    const agentId = `task:${taskId}`;
    if (history[agentId]?.ownerTaskId) continue;
    const known = drafts.get(agentId);
    const reportedMs = m.timestamp.getTime();
    const outcome = failed ? 'error' : 'completed';
    // Still open here while nothing runs, it went quiet unseen: its report
    // says how. A settled status says more than the report can.
    const stale = known && !isTerminalStatus(known.status) && !workRunning;
    drafts.set(agentId, known
      ? {
        ...known,
        // A card its close created has no description yet; the report has one.
        description: known.description || (description ?? ''),
        status: stale ? outcome : known.status,
        lastActivityMs: latest(known.lastActivityMs, reportedMs),
      }
      : { name: displayId, description: description ?? '', status: outcome, lastActivityMs: reportedMs });
  }

  const rows: (RosterTask & { lastActivityMs: number | null })[] = [];
  for (const [id, draft] of drafts) {
    const running = workRunning && !isTerminalStatus(draft.status);
    if (!staysOnRoster(running, draft.lastActivityMs, turnStarts)) continue;
    rows.push({
      id,
      name: draft.name,
      description: draft.description,
      isMainAgent: false,
      status: draft.status,
      running,
      lastActivityMs: draft.lastActivityMs,
    });
  }
  rows.sort((a, b) => Number(b.running) - Number(a.running) || (b.lastActivityMs ?? 0) - (a.lastActivityMs ?? 0));
  return rows.map(({ lastActivityMs: _, ...row }) => row);
}

export function rosterTasksEqual(a: readonly RosterTask[], b: readonly RosterTask[]): boolean {
  return a.length === b.length && a.every((x, i) => {
    const y = b[i];
    return x.id === y.id && x.name === y.name && x.description === y.description
      && x.status === y.status && x.running === y.running;
  });
}

/** An Analyst thread this thread handed work to, before its status is read. */
export interface AnalystCandidate {
  threadId: string;
  workspaceId: string | null;
  workspaceName: string | null;
  /** The thread's title, or failing that the question it was handed. */
  title: string | null;
  /** The latest hand-off to it or report from it in this transcript (epoch ms). */
  seenMs: number | null;
  /** It still owes this thread a report. */
  owed: boolean;
  /** Every hand-off to it seen here failed. One whose outcome is unknown
   *  still names the thread, where a run may yet turn up. */
  dispatchFailed: boolean;
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value ? value : null;
}

/**
 * Every Analyst thread this thread has handed work to, as far as the loaded
 * transcript and the owed list show: a hand-off the user approved, one made
 * without asking, a report it sent back, and every thread still owing one.
 */
export function analystCandidates(
  messages: readonly ChatMessage[],
  owedThreads: readonly ReportBackOwedThread[] | null,
): AnalystCandidate[] {
  const byId = new Map<string, AnalystCandidate>();
  const note = (next: AnalystCandidate) => {
    const known = byId.get(next.threadId);
    byId.set(next.threadId, known
      ? {
        threadId: known.threadId,
        workspaceId: known.workspaceId ?? next.workspaceId,
        workspaceName: known.workspaceName ?? next.workspaceName,
        title: next.owed ? (next.title ?? known.title) : (known.title ?? next.title),
        seenMs: latest(known.seenMs, next.seenMs),
        owed: known.owed || next.owed,
        dispatchFailed: known.dispatchFailed && next.dispatchFailed,
      }
      : next);
  };

  for (const m of messages) {
    const at = m.timestamp.getTime();
    if (m.role === 'user') {
      if (m.provenance?.kind === 'analyst_report') {
        const { threadId, workspaceId, workspaceName } = m.provenance;
        note({ threadId, workspaceId, workspaceName, title: null, seenMs: at, owed: false, dispatchFailed: false });
      }
      continue;
    }
    if (m.role !== 'assistant') continue;
    for (const proposal of Object.values(m.ptcAgentProposals ?? {})) {
      if (!proposal.thread_id) continue;
      note({
        threadId: proposal.thread_id,
        workspaceId: proposal.workspace_id ?? null,
        workspaceName: proposal.workspace_name ?? null,
        title: str(proposal.question),
        seenMs: at,
        owed: false,
        dispatchFailed: proposal.dispatch_failed === true,
      });
    }
    for (const proc of Object.values(m.toolCallProcesses)) {
      const card = preapprovedCardOf(proc);
      const threadId = card?.type === 'ptc_agent' ? str(card.proposal.thread_id) : null;
      if (!card || !threadId) continue;
      note({
        threadId,
        workspaceId: str(card.proposal.workspace_id),
        workspaceName: str(card.proposal.workspace_name),
        title: str(card.proposal.question),
        seenMs: at,
        owed: false,
        dispatchFailed: card.proposal.dispatch_failed === true,
      });
    }
  }

  for (const owed of owedThreads ?? []) {
    note({
      threadId: owed.thread_id,
      workspaceId: owed.workspace_id ?? null,
      workspaceName: owed.workspace_name ?? null,
      title: owed.title ?? null,
      seenMs: null,
      owed: true,
      dispatchFailed: false,
    });
  }
  return [...byId.values()];
}
