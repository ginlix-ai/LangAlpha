/**
 * The inactive-card gate vs. workflow-run progress. A run task reloaded from
 * history is never `isActive`, and its progress frames carry no messages and
 * no tokenUsage — so before `workflowRun` counted as content, every live frame
 * of a reloaded run was dropped and the console rendered blank.
 */
import { describe, it, expect } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useCardState } from '../useCardState';
import {
  applyWorkflowLifecycle,
  type WorkflowRunState,
} from '../../session/subagents/workflowRunState';

const AGENT_ID = 'task:wf-001';
const CARD_ID = `subagent-${AGENT_ID}`;

/** The write refreshSubagentCard makes for a run that already has a history
 *  entry — the state every reloaded thread starts a live run from. */
function seedInactiveRunCard(update: ReturnType<typeof useCardState>['updateSubagentCard']): void {
  update(AGENT_ID, {
    agentId: AGENT_ID,
    taskId: AGENT_ID,
    type: 'workflow',
    isHistory: true,
    isActive: false,
    status: 'active',
    messages: [],
  });
}

/** One `workflow_lifecycle` frame reduced the way handleWorkflowLifecycle does. */
function progressPatch(prev: WorkflowRunState | undefined, evt: Record<string, unknown>) {
  const next = applyWorkflowLifecycle(prev, evt);
  return { patch: { type: 'workflow', workflowRun: next, status: 'active' }, next };
}

describe('useCardState — workflow-run progress on an inactive card', () => {
  it('lands a workflowRun-only patch on a card the gate treats as inactive', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedInactiveRunCard(result.current.updateSubagentCard));
    expect(result.current.cards[CARD_ID].subagentData?.isActive).toBe(false);
    expect(result.current.cards[CARD_ID].subagentData?.workflowRun).toBeUndefined();

    const { patch, next } = progressPatch(undefined, {
      phase: 'child_started',
      seq: 0,
      label: 'stage one',
      subagent_type: 'general-purpose',
      child_task_id: 'wf-001-c0',
    });
    act(() => result.current.updateSubagentCard(AGENT_ID, patch));

    const run = result.current.cards[CARD_ID].subagentData?.workflowRun;
    expect(run).toEqual(next);
    expect(run?.children).toHaveLength(1);
    expect(run?.children[0].label).toBe('stage one');
  });

  it('keeps the card inactive — progress is content, not a reactivation', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedInactiveRunCard(result.current.updateSubagentCard));
    const { patch } = progressPatch(undefined, { phase: 'log', message: 'first line' });
    act(() => result.current.updateSubagentCard(AGENT_ID, patch));

    expect(result.current.cards[CARD_ID].subagentData?.isActive).toBe(false);
  });

  it('accumulates successive frames instead of stopping at the first', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedInactiveRunCard(result.current.updateSubagentCard));
    let run: WorkflowRunState | undefined;
    for (const message of ['line one', 'line two', 'line three']) {
      const frame = progressPatch(run, { phase: 'log', message });
      run = frame.next;
      act(() => result.current.updateSubagentCard(AGENT_ID, frame.patch));
    }

    expect(result.current.cards[CARD_ID].subagentData?.workflowRun?.logs).toEqual([
      'line one',
      'line two',
      'line three',
    ]);
  });

  it('still drops a pure status update on the same inactive card', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedInactiveRunCard(result.current.updateSubagentCard));
    const before = result.current.cards[CARD_ID];
    act(() => result.current.updateSubagentCard(AGENT_ID, { status: 'active', currentTool: 'bash' }));

    expect(result.current.cards[CARD_ID]).toBe(before);
  });

  it('settles the card on the terminal frame', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedInactiveRunCard(result.current.updateSubagentCard));
    const done = applyWorkflowLifecycle(undefined, {
      phase: 'run_completed',
      status: 'completed',
      duration_s: 12.5,
    });
    act(() =>
      result.current.updateSubagentCard(AGENT_ID, {
        type: 'workflow',
        workflowRun: done,
        status: 'completed',
        isActive: false,
      }),
    );

    const card = result.current.cards[CARD_ID].subagentData;
    expect(card?.status).toBe('completed');
    expect(card?.workflowRun?.status).toBe('completed');
  });
});
