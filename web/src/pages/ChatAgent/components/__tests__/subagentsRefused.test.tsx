/**
 * With subagents turned off on a thread, a Task or RunWorkflow call is refused
 * before anything starts and comes back as "Refused: ..." under an error
 * status. The row reads as that refusal: couldn't start, with the reason,
 * never running, and a spawn that never ran opens nothing, since opening it
 * would mint a tab for a subagent that does not exist.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageActionsProvider } from '../messageList/MessageActionsContext';
import { MessageContentSegments } from '../messageList/MessageContentSegments';
import { WorkflowRunContext } from '../WorkflowRunContext';
import { WORKFLOW_TASK_TYPE } from '../../session/subagents/workflowRunState';
import type { SubagentTaskRecord } from '@/types/chat';

const REFUSED = 'Refused: the user has turned subagents off for this thread. Do the work yourself.';

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

// Rendered by the transcript, which draws a workflow run as its own card and a
// spawn as a row of a subagent run.
function renderCard(record: Partial<SubagentTaskRecord>, onOpen = vi.fn()) {
  const task: SubagentTaskRecord = {
    subagentId: 'tc-1',
    description: 'Pull the last four 10-Qs',
    prompt: '',
    type: 'research',
    action: 'init',
    status: 'running',
    ...record,
  };
  render(
    <WorkflowRunContext.Provider value={() => undefined}>
      <MessageActionsProvider actions={{ onOpenSubagentTask: onOpen }}>
        <MessageContentSegments
          segments={[{ type: 'subagent_task', order: 0, subagentId: 'tc-1' }] as SegmentsProps['segments']}
          reasoningProcesses={{}}
          toolCallProcesses={{}}
          todoListProcesses={{}}
          subagentTasks={{ 'tc-1': task }}
          isStreaming={false}
        />
      </MessageActionsProvider>
    </WorkflowRunContext.Provider>,
  );
  return onOpen;
}

describe('a launch refused with subagents off', () => {
  it("reads as a spawn that couldn't start, with its reason, and opens nothing", () => {
    renderCard({ status: 'error', launchFailed: true, result: REFUSED });

    expect(screen.getByTestId('subagent-row-status')).toHaveTextContent("Couldn't start");
    const detail = screen.getByTestId('subagent-row-detail');
    expect(detail).toHaveTextContent('The user has turned subagents off');
    // The envelope and the instructions the model reads are not the user's to see.
    expect(detail).not.toHaveTextContent('Refused:');
    expect(detail).not.toHaveTextContent('Do the work yourself');
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.queryByTestId('subagent-row-elapsed')).toBeNull();
  });

  it('says why in words written for the user when the reply names the switch', () => {
    renderCard({ status: 'error', launchFailed: true, launchRefusal: 'subagents_off', result: REFUSED });

    const detail = screen.getByTestId('subagent-row-detail');
    expect(detail).toHaveTextContent('Subagents are off for this chat');
    expect(detail).not.toHaveTextContent('Do the work yourself');
  });

  it('reads as a refused follow-up, which still opens the task it was aimed at', () => {
    const onOpen = renderCard({
      action: 'update',
      resumeTargetId: 'task:A1b2C3',
      status: 'error',
      launchFailed: true,
      result: REFUSED,
    });

    expect(screen.getByTestId('subagent-row-detail')).toHaveTextContent('turned subagents off');
    fireEvent.click(screen.getByRole('button'));
    expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ subagentId: 'task:A1b2C3' }));
  });

  it('reads as a failed workflow run with its reason, and opens nothing', () => {
    renderCard({
      type: WORKFLOW_TASK_TYPE,
      description: 'ticker-briefs',
      status: 'error',
      launchFailed: true,
      result: REFUSED,
    });

    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.getByTestId('workflow-failure-detail')).toHaveTextContent('turned subagents off');
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('leaves a spawn that did run, then failed, openable and without its launch reply', () => {
    const onOpen = renderCard({ status: 'error', result: 'Task-A1b2C3 started in the background.' });

    expect(screen.getByTestId('subagent-row-status')).toHaveTextContent('Failed');
    expect(screen.queryByText(/started in the background/)).toBeNull();
    fireEvent.click(screen.getByRole('button'));
    expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ subagentId: 'tc-1' }));
  });
});
