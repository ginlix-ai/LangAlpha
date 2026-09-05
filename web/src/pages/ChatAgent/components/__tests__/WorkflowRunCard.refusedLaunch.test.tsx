/**
 * A RunWorkflow call the backend refused — an unknown name, a cap, an
 * unreadable script — starts no run at all. There is no lifecycle stream to
 * settle the card and no task channel that will ever close, so the card is
 * settled from the launch record alone, and the reply it was refused with is
 * the only account of why.
 *
 * The failure mode this locks is the card reading "Running" forever, with the
 * agent's own text three lines below already saying the launch failed.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { TaskSegmentCard } from '../messageList/TaskSegmentCard';
import { WorkflowRunContext } from '../WorkflowRunContext';
import {
  createWorkflowRunState,
  WORKFLOW_TASK_TYPE,
} from '../../session/subagents/workflowRunState';
import type { WorkflowRunState } from '../../session/subagents/workflowRunState';
import type { SubagentTaskRecord } from '@/types/chat';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const REFUSAL =
  "Error: Unknown workflow 'no-such-workflow-xyz'. Workflows live in " +
  '.agents/workflows/<name>.js.';

function renderCard(
  record: Partial<SubagentTaskRecord>,
  run?: WorkflowRunState,
): void {
  render(
    <WorkflowRunContext.Provider value={() => run}>
      <TaskSegmentCard
        subagentId="tc-1"
        task={{
          subagentId: 'tc-1',
          description: '',
          prompt: '',
          type: WORKFLOW_TASK_TYPE,
          action: 'init',
          status: 'running',
          ...record,
        }}
      />
    </WorkflowRunContext.Provider>,
  );
}

describe('WorkflowRunCard refused launch', () => {
  it('shows the refusal reason on a launch that never became a run', () => {
    renderCard({
      subagentId: 'tc-1',
      description: 'no-such-workflow-xyz',
      type: 'workflow',
      action: 'init',
      status: 'error',
      result: REFUSAL,
    });

    expect(screen.getByTestId('workflow-failure-detail')).toHaveTextContent(
      "Unknown workflow 'no-such-workflow-xyz'",
    );
    expect(screen.getByText('chat.taskCard.statusFailed')).toBeInTheDocument();
  });

  it('leaves a launch that did start to its run for the account of why', () => {
    // Its launch reply reads "Workflow run started", which explains nothing
    // about the failure — the run's own error is the one that does.
    renderCard(
      {
        subagentId: 'tc-1',
        description: 'ticker-briefs',
        type: 'workflow',
        action: 'init',
        status: 'running',
        result: 'Workflow run started: **Task-A1b2C3**',
      },
      {
        ...createWorkflowRunState(),
        status: 'failed',
        error: 'the script threw on line 4',
      },
    );

    expect(screen.getByTestId('workflow-failure-detail')).toHaveTextContent(
      'the script threw on line 4',
    );
  });

  it('never explains a failure with the reply of a launch that succeeded', () => {
    // A settled-failed card with no resolvable run: a shared link mounts no
    // run state, and an authed view has none until it hydrates. Neither is
    // evidence the launch was refused, so the reply of a launch that *did*
    // start must not be promoted into the reason the run failed.
    renderCard({
      subagentId: 'tc-1',
      description: 'ticker-briefs',
      type: 'workflow',
      action: 'init',
      status: 'error',
      result: 'Workflow run started: **Task-A1b2C3**',
    });

    expect(screen.queryByTestId('workflow-failure-detail')).toBeNull();
    expect(screen.getByText('chat.taskCard.statusFailed')).toBeInTheDocument();
  });

  it('does not fall back to the launch reply when a failed run carries no error', () => {
    renderCard(
      {
        subagentId: 'tc-1',
        description: 'ticker-briefs',
        type: 'workflow',
        action: 'init',
        status: 'running',
        result: 'Workflow run started: **Task-A1b2C3**',
      },
      { ...createWorkflowRunState(), status: 'failed' },
    );

    expect(screen.queryByTestId('workflow-failure-detail')).toBeNull();
  });

  it('still shows nothing extra while the launch is in flight', () => {
    renderCard({
      subagentId: 'tc-1',
      description: 'ticker-briefs',
      type: 'workflow',
      action: 'init',
      status: 'running',
    });

    expect(screen.queryByTestId('workflow-failure-detail')).toBeNull();
    // getAllByText: a running chip carries the key twice — the visible text
    // and the ascii loader's screen-reader label.
    expect(screen.getAllByText('chat.taskCard.statusRunning').length).toBeGreaterThan(0);
  });
});
