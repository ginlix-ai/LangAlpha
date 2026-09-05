import { describe, it, expect } from 'vitest';
import {
  DEFAULT_SUBAGENT_TYPE,
  applyWorkflowLifecycle,
  createWorkflowRunState,
  deriveChildIdentity,
  isWorkflowRunTerminal,
  resolveWorkflowRun,
  workflowRunDisplayStatus,
  type WorkflowRunState,
} from '../workflowRunState';

const fold = (events: Array<Record<string, unknown>>, seed?: WorkflowRunState) =>
  events.reduce<WorkflowRunState | undefined>(
    (state, evt) => applyWorkflowLifecycle(state, evt),
    seed,
  )!;

describe('applyWorkflowLifecycle', () => {
  it('folds a full run lifecycle into terminal state', () => {
    const state = fold([
      { phase: 'run_started', name: 'briefs', description: 'Fan out research' },
      { phase: 'phase', title: 'Research' },
      { phase: 'child_started', seq: 0, label: 'AAPL', subagent_type: 'research', workflow_phase: 'Research', child_task_id: 'c0' },
      { phase: 'child_started', seq: 1, label: 'NVDA', subagent_type: 'research', workflow_phase: 'Research', child_task_id: 'c1' },
      { phase: 'log', message: '2 dispatched' },
      { phase: 'child_done', seq: 0, status: 'ok', duration_s: 12.4, workflow_phase: 'Research', child_task_id: 'c0', tokens_spent: 42000 },
      { phase: 'phase', title: 'Synthesize' },
      { phase: 'child_done', seq: 1, status: 'timeout', duration_s: 60.0, workflow_phase: 'Research', child_task_id: 'c1', tokens_spent: 90000 },
      { phase: 'run_completed', status: 'completed', error: null, children_total: 2, tokens_spent: 90000, duration_s: 75.2 },
    ]);

    expect(state.name).toBe('briefs');
    expect(state.description).toBe('Fan out research');
    expect(state.status).toBe('completed');
    expect(state.phases).toEqual(['Research', 'Synthesize']);
    expect(state.children).toHaveLength(2);
    expect(state.children[0]).toMatchObject({ seq: 0, status: 'ok', durationS: 12.4 });
    expect(state.children[1]).toMatchObject({ seq: 1, status: 'timeout', childTaskId: 'c1' });
    expect(state.logs).toEqual(['2 dispatched']);
    expect(state.tokensSpent).toBe(90000);
    expect(state.childrenTotal).toBe(2);
    expect(state.durationS).toBe(75.2);
  });

  it('upserts children by seq — re-delivered child_started never regresses a settled child', () => {
    const settled = fold([
      { phase: 'child_started', seq: 0, label: 'A', subagent_type: 'research', child_task_id: 'c0' },
      { phase: 'child_done', seq: 0, status: 'ok', duration_s: 3.0, child_task_id: 'c0' },
    ]);
    const replayed = applyWorkflowLifecycle(settled, {
      phase: 'child_started', seq: 0, label: 'A', subagent_type: 'research', child_task_id: 'c0',
    });
    expect(replayed.children).toHaveLength(1);
    expect(replayed.children[0].status).toBe('ok');
  });

  it('records child_done for a seq never started (out-of-order delivery)', () => {
    const state = fold([{ phase: 'child_done', seq: 3, status: 'error', duration_s: 1.0 }]);
    expect(state.children).toEqual([
      expect.objectContaining({ seq: 3, status: 'error', durationS: 1.0 }),
    ]);
  });

  it('maps an unknown terminal child status to error and unknown run status to failed', () => {
    const child = fold([{ phase: 'child_done', seq: 0, status: 'exploded' }]);
    expect(child.children[0].status).toBe('error');
    const run = fold([{ phase: 'run_completed', status: 'exploded' }]);
    expect(run.status).toBe('failed');
  });

  it('preserves invalid_schema as its own child status', () => {
    // Guards the CHILD_TERMINAL membership: drop `invalid_schema` from that set
    // and the branch above silently recasts it as `error`, which would render
    // a schema miss in failure red instead of its own amber.
    const state = fold([{ phase: 'child_done', seq: 0, status: 'invalid_schema', duration_s: 2.5 }]);
    expect(state.children[0].status).toBe('invalid_schema');
  });

  it('keeps both invalid_schema children of one agent() call as separate rows', () => {
    // A schema'd agent() that fails validation retries, so one call can emit
    // two `child_done` frames with `invalid_schema` — different seq and
    // child_task_id, same label. Two subagents really were dispatched and
    // billed, so both must show: deduping these by label would hide work the
    // user paid for.
    const state = fold([
      { phase: 'child_started', seq: 0, label: 'AAPL', subagent_type: 'research', child_task_id: 'c0' },
      { phase: 'child_done', seq: 0, status: 'invalid_schema', duration_s: 4.0, child_task_id: 'c0', tokens_used: 900 },
      { phase: 'child_started', seq: 1, label: 'AAPL', subagent_type: 'research', child_task_id: 'c1' },
      { phase: 'child_done', seq: 1, status: 'invalid_schema', duration_s: 5.0, child_task_id: 'c1', tokens_used: 1100 },
    ]);
    expect(state.children).toHaveLength(2);
    expect(state.children.map((c) => c.status)).toEqual(['invalid_schema', 'invalid_schema']);
    expect(state.children.map((c) => c.childTaskId)).toEqual(['c0', 'c1']);
    expect(state.children.map((c) => c.label)).toEqual(['AAPL', 'AAPL']);
    expect(state.children.map((c) => c.tokensUsed)).toEqual([900, 1100]);
  });

  it('dedupes phases by title and consecutive duplicate log lines', () => {
    const state = fold([
      { phase: 'phase', title: 'Find' },
      { phase: 'log', message: 'round 1' },
      { phase: 'log', message: 'round 1' },
      { phase: 'phase', title: 'Verify' },
      { phase: 'phase', title: 'Find' },
    ]);
    expect(state.phases).toEqual(['Find', 'Verify']);
    expect(state.currentPhase).toBe('Find');
    expect(state.logs).toEqual(['round 1']);
  });

  it('caps logs at 50 entries, keeping the newest', () => {
    const events = Array.from({ length: 60 }, (_, i) => ({ phase: 'log', message: `line ${i}` }));
    const state = fold(events);
    expect(state.logs).toHaveLength(50);
    expect(state.logs[0]).toBe('line 10');
    expect(state.logs[49]).toBe('line 59');
  });

  it('returns state unchanged for unknown phases', () => {
    const seed = createWorkflowRunState({ name: 'x' });
    expect(applyWorkflowLifecycle(seed, { phase: 'mystery' })).toBe(seed);
    expect(applyWorkflowLifecycle(seed, {})).toBe(seed);
  });

  it('carries a failed run_completed error into state', () => {
    const state = fold([
      { phase: 'run_completed', status: 'failed', error: 'Run timed out after 600s', children_total: 1 },
    ]);
    expect(state.status).toBe('failed');
    expect(state.error).toBe('Run timed out after 600s');
    expect(isWorkflowRunTerminal(state.status)).toBe(true);
  });

  it('settles children still running when the run stops', () => {
    const state = fold([
      { phase: 'child_started', seq: 0, label: 'A', subagent_type: 'research' },
      { phase: 'child_started', seq: 1, label: 'B', subagent_type: 'research' },
      { phase: 'child_done', seq: 0, status: 'ok', duration_s: 1.0 },
      { phase: 'run_completed', status: 'cancelled', error: 'Workflow cancelled' },
    ]);
    expect(state.children.map((c) => c.status)).toEqual(['ok', 'cancelled']);
  });

  it('leaves a completed run’s children alone', () => {
    // Every child of a completed run reported a terminal frame, so a leftover
    // spinner there is a real inconsistency and should stay visible.
    const state = fold([
      { phase: 'child_started', seq: 0, label: 'A', subagent_type: 'research' },
      { phase: 'run_completed', status: 'completed' },
    ]);
    expect(state.children[0].status).toBe('running');
  });

  it('carries the detail fields — source, per-child error/tokens, result preview', () => {
    const state = fold([
      { phase: 'run_started', name: 'briefs', source: 'saved' },
      { phase: 'child_started', seq: 0, label: 'A', subagent_type: 'research', child_task_id: 'c0' },
      { phase: 'child_done', seq: 0, status: 'error', duration_s: 2.0, child_task_id: 'c0', error: 'ERROR: boom', tokens_used: 1250 },
      { phase: 'run_completed', status: 'completed', error: null, children_total: 1, result_preview: '{"v": 1}' },
    ]);
    expect(state.source).toBe('saved');
    expect(state.children[0].error).toBe('ERROR: boom');
    expect(state.children[0].tokensUsed).toBe(1250);
    expect(state.resultPreview).toBe('{"v": 1}');
  });
});

describe('workflowRunDisplayStatus', () => {
  it('maps run statuses onto the shared display vocabulary', () => {
    expect(workflowRunDisplayStatus('running')).toBe('active');
    expect(workflowRunDisplayStatus('completed')).toBe('completed');
    expect(workflowRunDisplayStatus('failed')).toBe('error');
    expect(workflowRunDisplayStatus('cancelled')).toBe('cancelled');
  });
});

describe('resolveWorkflowRun', () => {
  it('prefers the live card state and falls back to history', () => {
    const live = createWorkflowRunState({ name: 'live' });
    const hist = createWorkflowRunState({ name: 'hist' });
    expect(resolveWorkflowRun(live, hist)).toBe(live);
    expect(resolveWorkflowRun(undefined, hist)).toBe(hist);
    expect(resolveWorkflowRun(undefined, undefined)).toBeUndefined();
  });
});

describe('deriveChildIdentity', () => {
  // Three surfaces open a workflow child — the detail drill-in, the replay
  // backfill and the lazy hydration — and each used to spell the precedence
  // itself. These pin the one rule they now share.
  const dispatched = { label: 'scan the logs', subagentType: 'research', status: 'ok' as const };

  it('reads a child straight off the run when nothing else is known', () => {
    expect(deriveChildIdentity(dispatched)).toEqual({
      description: 'scan the logs',
      type: 'research',
      status: 'completed',
    });
  });

  it('lets a caller-known description and type win over the run', () => {
    const identity = deriveChildIdentity(dispatched, {
      description: 'what the user typed',
      type: 'equity-analyst',
    });
    expect(identity.description).toBe('what the user typed');
    expect(identity.type).toBe('equity-analyst');
  });

  it('treats the default subagent name as unknown, not as an answer', () => {
    // A replayed child lane defaults to `general-purpose` because its own
    // transcript is anonymous; the run's dispatch record is the real answer.
    expect(deriveChildIdentity(dispatched, { type: DEFAULT_SUBAGENT_TYPE }).type).toBe('research');
  });

  it('falls back to the default only when neither side names a type', () => {
    expect(deriveChildIdentity({ ...dispatched, subagentType: '' }).type)
      .toBe(DEFAULT_SUBAGENT_TYPE);
    expect(deriveChildIdentity(undefined).type).toBe(DEFAULT_SUBAGENT_TYPE);
  });

  it('leaves an unsettled child without a status for the caller to decide', () => {
    expect(deriveChildIdentity({ ...dispatched, status: 'running' }).status).toBeUndefined();
    expect(deriveChildIdentity(undefined).status).toBeUndefined();
  });

  it('yields empty identity fields rather than undefined for a missing child', () => {
    expect(deriveChildIdentity(undefined).description).toBe('');
  });
});
