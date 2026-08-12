/**
 * Pure reducer for workflow-run progress, fed by `workflow_lifecycle` SSE
 * events. One reducer serves both transports — the live mux sink and the
 * history replay both deliver the same flattened event shape
 * (`{event: 'workflow_lifecycle', phase: 'run_started' | ..., ...fields}`),
 * so live and reloaded threads render identical cards.
 *
 * The reducer is replay-tolerant: reconnect buffers may re-deliver events
 * already applied, so children upsert by `seq` and never regress from a
 * settled status back to running, phases dedupe by title, and a repeated log
 * line collapses. There is deliberately no run-level terminal guard — a card
 * that saw `run_completed` before the frames preceding it would drop every
 * child on the floor.
 */
import type { SubagentDisplayStatus, SubagentTerminalStatus } from './subagentStatus';

/** Card `type` discriminant for a workflow run task — the value the backend
 *  stamps on the launch artifact and every frontend surface renders on. */
export const WORKFLOW_TASK_TYPE = 'workflow';

/** Run outcomes the driver's `run_completed` reports. */
export type WorkflowRunTerminalStatus = 'completed' | 'failed' | 'cancelled';

export type WorkflowRunStatus = 'running' | WorkflowRunTerminalStatus;

/** Child outcome vocabulary as emitted by the backend driver's `child_done` —
 *  the only statuses the wire can settle a child on. */
export type WorkflowChildTerminalStatus =
  | 'ok'
  | 'error'
  | 'timeout'
  | 'invalid_schema'
  | 'cancelled';

/** A child row's status: the wire's outcomes plus the locally seeded
 *  pre-outcome state (`child_started` carries no status of its own). */
export type WorkflowChildStatus = 'running' | WorkflowChildTerminalStatus;

/**
 * One flattened `workflow_lifecycle` event. `phase` selects which of the other
 * fields the backend populated; everything is `unknown` because this is the
 * wire, and the reducer narrows each field where it reads it. The index
 * signature admits the envelope keys the transports flatten in alongside.
 */
export interface WorkflowLifecycleFrame {
  phase?: unknown;
  /** run_started */
  name?: unknown;
  description?: unknown;
  source?: unknown;
  /** phase */
  title?: unknown;
  /** log */
  message?: unknown;
  /** child_started / child_done */
  seq?: unknown;
  label?: unknown;
  subagent_type?: unknown;
  workflow_phase?: unknown;
  child_task_id?: unknown;
  duration_s?: unknown;
  tokens_used?: unknown;
  /** child_done / run_completed */
  status?: unknown;
  error?: unknown;
  tokens_spent?: unknown;
  /** run_completed */
  children_total?: unknown;
  result_preview?: unknown;
  [key: string]: unknown;
}

export interface WorkflowChild {
  seq: number;
  label: string;
  subagentType: string;
  phase: string | null;
  childTaskId: string | null;
  status: WorkflowChildStatus;
  durationS: number | null;
  error: string | null;
  tokensUsed: number | null;
}

export interface WorkflowRunState {
  name: string;
  description: string;
  source: string | null;
  status: WorkflowRunStatus;
  error: string | null;
  phases: string[];
  currentPhase: string | null;
  children: WorkflowChild[];
  logs: string[];
  tokensSpent: number | null;
  childrenTotal: number | null;
  durationS: number | null;
  resultPreview: string | null;
}

const MAX_LOGS = 50;

const RUN_TERMINAL: ReadonlySet<string> = new Set<WorkflowRunTerminalStatus>([
  'completed',
  'failed',
  'cancelled',
]);
const CHILD_TERMINAL: ReadonlySet<string> = new Set<WorkflowChildTerminalStatus>([
  'ok',
  'error',
  'timeout',
  'invalid_schema',
  'cancelled',
]);

export function isWorkflowRunTerminal(
  status: string | undefined | null,
): status is WorkflowRunTerminalStatus {
  return status != null && RUN_TERMINAL.has(status);
}

export function isWorkflowChildTerminal(
  status: string | undefined | null,
): status is WorkflowChildTerminalStatus {
  return status != null && CHILD_TERMINAL.has(status);
}

/** Find the run that dispatched `childTaskId` (short id) among projected
 *  history entries, returning the owner's agent id and the child's row.
 *
 *  A child's own lane carries anonymous content — its label, type and owner
 *  live only on the run's reduced state. Lazy single-child hydration projects
 *  a one-entry map, so `projectSubagentHistory`'s in-map backfill can't see
 *  the run; this resolves it from the entries already on hand. */
export function findWorkflowChildOwner(
  entries: Record<string, { workflowRun?: WorkflowRunState }> | null | undefined,
  childTaskId: string,
): { ownerTaskId: string; child: WorkflowChild } | null {
  if (!entries || !childTaskId) return null;
  for (const [ownerTaskId, entry] of Object.entries(entries)) {
    const child = entry?.workflowRun?.children.find(
      (c) => c.childTaskId === childTaskId,
    );
    if (child) return { ownerTaskId, child };
  }
  return null;
}

/** The subagent name a task carries when nothing named one. Doubles as the
 *  "type not known yet" sentinel: an entry still holding it yields to the
 *  dispatch record on the run that spawned it. */
export const DEFAULT_SUBAGENT_TYPE = 'general-purpose';

/**
 * Who a workflow child is, as every surface that opens one needs to state it.
 *
 * A child's own lane is anonymous — its label, dispatch type and outcome live
 * only on the dispatching run's reduced state. Anything the caller already
 * knows wins; the run's row is the backfill. The single subtlety is the type:
 * `DEFAULT_SUBAGENT_TYPE` is a placeholder, not an answer, so a known type
 * still holding it yields to what the run actually dispatched.
 */
export function deriveChildIdentity(
  child: Pick<WorkflowChild, 'label' | 'subagentType' | 'status'> | undefined,
  known: { description?: string; type?: string } = {},
): { description: string; type: string; status: SubagentTerminalStatus | undefined } {
  const namedType = known.type && known.type !== DEFAULT_SUBAGENT_TYPE ? known.type : undefined;
  return {
    description: known.description || child?.label || '',
    type: namedType || child?.subagentType || DEFAULT_SUBAGENT_TYPE,
    status: workflowChildDisplayStatus(child?.status),
  };
}

/** Map a run status onto the shared subagent display vocabulary (card status). */
export function workflowRunDisplayStatus(status: WorkflowRunStatus): SubagentDisplayStatus {
  switch (status) {
    case 'completed':
      return 'completed';
    case 'failed':
      return 'error';
    case 'cancelled':
      return 'cancelled';
    default:
      return 'active';
  }
}

/** Map a run status onto the inline task-card status vocabulary (`STATUS_UI`).
 *  Accepts the card's own status too, so a run with no lifecycle state yet
 *  still resolves a header from whatever the launch record recorded. */
export function workflowRunCardKind(
  status: string | null | undefined,
): 'running' | 'completed' | 'cancelled' | 'error' {
  switch (status) {
    case 'completed':
      return 'completed';
    case 'failed':
    case 'error':
      return 'error';
    case 'cancelled':
      return 'cancelled';
    default:
      return 'running';
  }
}

/** Map a run-ledger outcome onto a run status, mirroring the history
 *  projector's `_mapped_run_status`. `null` while the row is absent or still
 *  open — nothing to reconcile.
 *
 *  The two must agree: a run whose worker died settles from this on the live
 *  stream and from the projector's synthesis after a reload, and a viewer who
 *  reloads must not watch the card change verdict. */
export function workflowRunStatusFromLedger(
  ledgerStatus: string | null | undefined,
): WorkflowRunTerminalStatus | null {
  if (!ledgerStatus || ledgerStatus === 'in_progress') return null;
  if (ledgerStatus === 'completed') return 'completed';
  if (ledgerStatus === 'cancelled') return 'cancelled';
  return 'failed';
}

/** Map a child's status onto the subagent card vocabulary so a replayed or
 *  lazily hydrated child lands with its real terminal state. `undefined` while
 *  the child is still running — the caller decides what an unsettled child
 *  should read as. */
export function workflowChildDisplayStatus(
  status: string | null | undefined,
): SubagentTerminalStatus | undefined {
  switch (status) {
    case 'ok':
      return 'completed';
    case 'error':
    case 'timeout':
    case 'invalid_schema':
      return 'error';
    case 'cancelled':
      return 'cancelled';
    default:
      return undefined;
  }
}

export function createWorkflowRunState(
  init: Partial<Pick<WorkflowRunState, 'name' | 'description'>> = {},
): WorkflowRunState {
  return {
    name: init.name ?? '',
    description: init.description ?? '',
    source: null,
    status: 'running',
    error: null,
    phases: [],
    currentPhase: null,
    children: [],
    logs: [],
    tokensSpent: null,
    childrenTotal: null,
    durationS: null,
    resultPreview: null,
  };
}

function asString(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function upsertChild(
  children: WorkflowChild[],
  seq: number,
  patch: Partial<WorkflowChild>,
): WorkflowChild[] {
  const index = children.findIndex((child) => child.seq === seq);
  if (index < 0) {
    const created: WorkflowChild = {
      seq,
      label: '',
      subagentType: '',
      phase: null,
      childTaskId: null,
      status: 'running',
      durationS: null,
      error: null,
      tokensUsed: null,
      ...patch,
    };
    return [...children, created].sort((a, b) => a.seq - b.seq);
  }
  const existing = children[index];
  // A settled child never regresses to running — replay re-delivers
  // child_started after child_done was already applied.
  if (isWorkflowChildTerminal(existing.status) && patch.status === 'running') {
    patch = { ...patch, status: existing.status };
  }
  const next = children.slice();
  next[index] = { ...existing, ...patch };
  return next;
}

/**
 * Apply one flattened `workflow_lifecycle` event. Returns `prev` unchanged
 * for unknown phases so callers can cheaply reference-compare.
 */
export function applyWorkflowLifecycle(
  prev: WorkflowRunState | undefined,
  evt: WorkflowLifecycleFrame,
): WorkflowRunState {
  const state = prev ?? createWorkflowRunState();
  const phase = asString(evt.phase);

  switch (phase) {
    case 'run_started': {
      return {
        ...state,
        name: asString(evt.name) ?? state.name,
        description: asString(evt.description) ?? state.description,
        source: asString(evt.source) ?? state.source,
      };
    }
    case 'phase': {
      const title = asString(evt.title);
      if (!title) return state;
      return {
        ...state,
        currentPhase: title,
        phases: state.phases.includes(title) ? state.phases : [...state.phases, title],
      };
    }
    case 'log': {
      const message = asString(evt.message);
      if (!message || state.logs[state.logs.length - 1] === message) return state;
      return { ...state, logs: [...state.logs, message].slice(-MAX_LOGS) };
    }
    case 'child_started': {
      const seq = asNumber(evt.seq);
      if (seq == null) return state;
      return {
        ...state,
        children: upsertChild(state.children, seq, {
          label: asString(evt.label) ?? '',
          subagentType: asString(evt.subagent_type) ?? '',
          phase: asString(evt.workflow_phase),
          childTaskId: asString(evt.child_task_id),
          status: 'running',
        }),
      };
    }
    case 'child_done': {
      const seq = asNumber(evt.seq);
      if (seq == null) return state;
      const status = asString(evt.status);
      return {
        ...state,
        children: upsertChild(state.children, seq, {
          status: isWorkflowChildTerminal(status) ? status : 'error',
          durationS: asNumber(evt.duration_s),
          phase: asString(evt.workflow_phase),
          childTaskId: asString(evt.child_task_id),
          error: asString(evt.error),
          tokensUsed: asNumber(evt.tokens_used),
        }),
        tokensSpent: asNumber(evt.tokens_spent) ?? state.tokensSpent,
      };
    }
    case 'run_completed': {
      const status = asString(evt.status);
      const runStatus = isWorkflowRunTerminal(status) ? status : 'failed';
      // A stopped or failed run can settle with children still marked running:
      // the driver emits their terminal frames on the unwind, but a run
      // recorded before it did has none, and nothing else would ever clear the
      // spinner. A completed run always accounts for every child, so leaving
      // those alone keeps a real inconsistency visible.
      const settleLeftovers = runStatus === 'cancelled' || runStatus === 'failed';
      return {
        ...state,
        status: runStatus,
        children: settleLeftovers
          ? state.children.map((child) =>
              child.status === 'running' ? { ...child, status: 'cancelled' } : child,
            )
          : state.children,
        error: asString(evt.error),
        resultPreview: asString(evt.result_preview) ?? state.resultPreview,
        childrenTotal: asNumber(evt.children_total) ?? state.childrenTotal,
        tokensSpent: asNumber(evt.tokens_spent) ?? state.tokensSpent,
        durationS: asNumber(evt.duration_s) ?? state.durationS,
      };
    }
    default:
      return state;
  }
}

/**
 * Pick the workflow-run state for a card, live source first. Mirrors the
 * telemetry resolver's dual-source rule: the live card (fed by SSE) wins when
 * hydrated; the history entry covers post-refresh reads before any live event.
 */
export function resolveWorkflowRun(
  cardState: WorkflowRunState | undefined,
  historyState: WorkflowRunState | undefined,
): WorkflowRunState | undefined {
  return cardState ?? historyState;
}
