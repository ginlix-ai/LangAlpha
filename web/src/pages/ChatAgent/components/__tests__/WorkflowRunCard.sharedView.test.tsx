/**
 * Read-only honesty pins for the inline workflow-run card (rebase Call 4):
 *  - without onOpen (shared links strip it) the shell renders inert — no
 *    button role, focus stop, pointer tooltip, or chevron promising a panel
 *  - the "details unavailable" note keys on WorkflowRunContext *provider
 *    absence* (a surface property), never on run === undefined, which in an
 *    authed view just means the lifecycle state hasn't hydrated yet
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import WorkflowRunCard from '../WorkflowRunCard';
import { WorkflowRunContext } from '../WorkflowRunContext';
import { createWorkflowRunState } from '../../session/subagents/workflowRunState';

const baseProps = {
  subagentId: 'wf-1',
  description: 'Audit the release branch',
  status: 'running',
};

describe('WorkflowRunCard — read-only surfaces', () => {
  it('is a keyboard-focusable button with a tooltip when onOpen is provided', () => {
    const onOpen = vi.fn();
    render(
      <WorkflowRunContext.Provider value={() => undefined}>
        <WorkflowRunCard {...baseProps} onOpen={onOpen} />
      </WorkflowRunContext.Provider>,
    );
    const card = screen.getByRole('button');
    expect(card).toHaveAttribute('tabIndex', '0');
    expect(card).toHaveAttribute('title');
    fireEvent.click(card);
    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(onOpen.mock.calls[0][0]).toMatchObject({ subagentId: 'wf-1' });
  });

  it('renders inert without onOpen — no button role, focus stop, or tooltip', () => {
    render(
      <WorkflowRunContext.Provider value={() => undefined}>
        <WorkflowRunCard {...baseProps} />
      </WorkflowRunContext.Provider>,
    );
    expect(screen.queryByRole('button')).toBeNull();
    expect(document.querySelector('[tabindex]')).toBeNull();
    expect(document.querySelector('[title]')).toBeNull();
  });

  it('shows the unavailability note when no WorkflowRunContext provider is mounted', () => {
    render(<WorkflowRunCard {...baseProps} />);
    expect(screen.getByTestId('workflow-detail-unavailable')).toHaveTextContent(
      "Run details aren't available in shared view",
    );
  });

  it('suppresses the note when the provider is mounted but the run has not hydrated', () => {
    render(
      <WorkflowRunContext.Provider value={() => undefined}>
        <WorkflowRunCard {...baseProps} />
      </WorkflowRunContext.Provider>,
    );
    expect(screen.queryByTestId('workflow-detail-unavailable')).toBeNull();
  });

  it('suppresses the note when a direct workflowRun override is supplied', () => {
    render(
      <WorkflowRunCard
        {...baseProps}
        workflowRun={createWorkflowRunState({ name: 'audit', description: 'Audit run' })}
      />,
    );
    expect(screen.queryByTestId('workflow-detail-unavailable')).toBeNull();
  });
});
