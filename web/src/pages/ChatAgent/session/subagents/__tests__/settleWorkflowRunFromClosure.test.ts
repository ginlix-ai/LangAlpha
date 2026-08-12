import { describe, it, expect, vi } from 'vitest';
import { settleWorkflowRunFromClosure } from '../liveEventHandlers';
import {
  applyWorkflowLifecycle,
  workflowRunStatusFromLedger,
  type WorkflowRunState,
} from '../workflowRunState';
import type { TaskRefs } from '../../streamRefs';

const TASK = 'task:wf1';

/** A run mid-flight: started, one phase, two children still running. */
const runningRun = (): WorkflowRunState =>
  [
    { phase: 'run_started', name: 'briefs', description: 'Fan out' },
    { phase: 'phase', title: 'Research' },
    { phase: 'child_started', seq: 0, label: 'AAPL', child_task_id: 'c0' },
    { phase: 'child_started', seq: 1, label: 'NVDA', child_task_id: 'c1' },
  ].reduce<WorkflowRunState | undefined>(
    (state, evt) => applyWorkflowLifecycle(state, evt),
    undefined,
  )!;

const refsWith = (run: WorkflowRunState | undefined): Record<string, TaskRefs> => ({
  [TASK]: {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    messages: [],
    runIndex: 0,
    ...(run ? { workflowRun: run } : {}),
  } as TaskRefs,
});

describe('workflowRunStatusFromLedger', () => {
  // Must stay in lockstep with the projector's _mapped_run_status, or a
  // reloading viewer sees the card change its verdict.
  it.each([
    ['completed', 'completed'],
    ['cancelled', 'cancelled'],
    ['error', 'failed'],
    ['interrupted', 'failed'],
  ])('maps ledger %s to %s', (ledger, expected) => {
    expect(workflowRunStatusFromLedger(ledger)).toBe(expected);
  });

  it('declines to reconcile an absent or still-open row', () => {
    expect(workflowRunStatusFromLedger(null)).toBeNull();
    expect(workflowRunStatusFromLedger(undefined)).toBeNull();
    expect(workflowRunStatusFromLedger('in_progress')).toBeNull();
  });
});

describe('settleWorkflowRunFromClosure', () => {
  it('settles a running run from the ledger outcome when its worker died', () => {
    const subagentStateRefs = refsWith(runningRun());
    const updateSubagentCard = vi.fn();

    const handled = settleWorkflowRunFromClosure({
      taskId: TASK,
      outcome: 'error',
      subagentStateRefs,
      updateSubagentCard,
    });

    expect(handled).toBe(true);
    // The card reads workflowRun.status ahead of its own status, so the run
    // state — not just the card stamp — has to reach terminal.
    expect(subagentStateRefs[TASK].workflowRun?.status).toBe('failed');
    expect(updateSubagentCard).toHaveBeenCalledWith(
      TASK,
      expect.objectContaining({ status: 'error', isActive: false }),
    );
  });

  it('clears the spinner on children the dead driver never settled', () => {
    const subagentStateRefs = refsWith(runningRun());
    settleWorkflowRunFromClosure({
      taskId: TASK,
      outcome: 'error',
      subagentStateRefs,
      updateSubagentCard: vi.fn(),
    });
    expect(
      subagentStateRefs[TASK].workflowRun?.children.map((c) => c.status),
    ).toEqual(['cancelled', 'cancelled']);
  });

  const completedRun = (): WorkflowRunState =>
    applyWorkflowLifecycle(runningRun(), {
      phase: 'run_completed',
      status: 'completed',
      result_preview: '{"ok":true}',
      children_total: 2,
    });

  it('adopts the ledger verdict over a terminal frame that contradicts it', () => {
    // The driver stamps its frame BEFORE the ledger CAS, so a raced cancel can
    // leave a card reading "completed" against a row that says otherwise. The
    // projector reconciles the same snapshot on reload; live must agree, or the
    // verdict changes under the viewer when they refresh.
    const subagentStateRefs = refsWith(completedRun());
    const updateSubagentCard = vi.fn();

    const handled = settleWorkflowRunFromClosure({
      taskId: TASK,
      outcome: 'cancelled',
      subagentStateRefs,
      updateSubagentCard,
    });

    expect(handled).toBe(true);
    expect(subagentStateRefs[TASK].workflowRun?.status).toBe('cancelled');
    expect(updateSubagentCard).toHaveBeenCalledWith(
      TASK,
      expect.objectContaining({ status: 'cancelled', isActive: false }),
    );
  });

  it('keeps the frame detail the ledger outcome does not carry', () => {
    // Reconciling replaces the status, not the frame: `run_end` has no result,
    // no totals and no duration, so overwriting with it would strip the panel.
    const subagentStateRefs = refsWith(completedRun());
    settleWorkflowRunFromClosure({
      taskId: TASK,
      outcome: 'error',
      subagentStateRefs,
      updateSubagentCard: vi.fn(),
    });

    const run = subagentStateRefs[TASK].workflowRun;
    expect(run?.status).toBe('failed');
    expect(run?.resultPreview).toBe('{"ok":true}');
    expect(run?.childrenTotal).toBe(2);
  });

  it('leaves a terminal frame the ledger agrees with untouched', () => {
    const subagentStateRefs = refsWith(completedRun());
    const updateSubagentCard = vi.fn();

    const handled = settleWorkflowRunFromClosure({
      taskId: TASK,
      outcome: 'completed',
      subagentStateRefs,
      updateSubagentCard,
    });

    expect(handled).toBe(false);
    expect(updateSubagentCard).not.toHaveBeenCalled();
  });

  it('preserves the driver failure detail instead of the generic fallback', () => {
    // A run that failed with a reason and then closed as `error` must keep the
    // reason — the projector only fills its fallback text when none is present.
    const failed = applyWorkflowLifecycle(runningRun(), {
      phase: 'run_completed',
      status: 'failed',
      error: 'Workflow timeout: script exceeded cpu budget',
    });
    const subagentStateRefs = refsWith(failed);

    settleWorkflowRunFromClosure({
      taskId: TASK,
      outcome: 'cancelled',
      subagentStateRefs,
      updateSubagentCard: vi.fn(),
    });

    const run = subagentStateRefs[TASK].workflowRun;
    expect(run?.status).toBe('cancelled');
    expect(run?.error).toBe('Workflow timeout: script exceeded cpu budget');
  });

  it('leaves a plain subagent task alone', () => {
    const subagentStateRefs = refsWith(undefined);
    const updateSubagentCard = vi.fn();
    expect(
      settleWorkflowRunFromClosure({
        taskId: TASK,
        outcome: 'error',
        subagentStateRefs,
        updateSubagentCard,
      }),
    ).toBe(false);
    expect(updateSubagentCard).not.toHaveBeenCalled();
  });

  it('does nothing without a terminal ledger outcome', () => {
    const subagentStateRefs = refsWith(runningRun());
    expect(
      settleWorkflowRunFromClosure({
        taskId: TASK,
        outcome: null,
        subagentStateRefs,
        updateSubagentCard: vi.fn(),
      }),
    ).toBe(false);
    expect(subagentStateRefs[TASK].workflowRun?.status).toBe('running');
  });
});
