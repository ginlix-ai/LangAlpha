/**
 * Coverage for the shared workflow-run presentation helpers — the duration
 * formatter and the per-child status icon, both deduped out of the inline run
 * card and the detail panel so the two surfaces cannot drift apart again.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  formatRunDuration,
  summarizeRun,
  workflowChildLabelKey,
  workflowChildStatusColor,
  WorkflowChildRow,
  WorkflowChildStatusIcon,
} from '../workflowRunUi';
import type {
  WorkflowChild,
  WorkflowChildStatus,
  WorkflowRunState,
} from '../../session/subagents/workflowRunState';
import { createWorkflowRunState } from '../../session/subagents/workflowRunState';

describe('formatRunDuration', () => {
  it('returns null for a missing or non-finite duration', () => {
    expect(formatRunDuration(null)).toBeNull();
    expect(formatRunDuration(undefined)).toBeNull();
    expect(formatRunDuration(NaN)).toBeNull();
    expect(formatRunDuration(Infinity)).toBeNull();
  });

  it('renders one decimal of seconds under a minute', () => {
    expect(formatRunDuration(0)).toBe('0.0s');
    expect(formatRunDuration(0.44)).toBe('0.4s');
    expect(formatRunDuration(12.34)).toBe('12.3s');
    expect(formatRunDuration(59.4)).toBe('59.4s');
  });

  it('renders zero-padded minutes and seconds at or above a minute', () => {
    expect(formatRunDuration(60)).toBe('1m 00s');
    expect(formatRunDuration(61.2)).toBe('1m 01s');
    expect(formatRunDuration(127.4)).toBe('2m 07s');
  });

  it('carries a rounded-up remainder into the minute instead of printing 60s', () => {
    // Rounding the remainder independently of the minutes lets it reach 60,
    // which reads as an impossible `1m 60s` for any duration in the top half
    // second of a minute.
    expect(formatRunDuration(119.6)).toBe('2m 00s');
    expect(formatRunDuration(119.5)).toBe('2m 00s');
    expect(formatRunDuration(3599.7)).toBe('60m 00s');
  });
});

describe('workflowChildLabelKey', () => {
  it('maps each child status onto its own label key', () => {
    expect(workflowChildLabelKey('running')).toBe('chat.workflowRun.childRunning');
    expect(workflowChildLabelKey('ok')).toBe('chat.workflowRun.childDone');
    expect(workflowChildLabelKey('invalid_schema')).toBe('chat.workflowRun.childInvalid');
    expect(workflowChildLabelKey('cancelled')).toBe('chat.workflowRun.childStopped');
    expect(workflowChildLabelKey('timeout')).toBe('chat.workflowRun.childTimedOut');
    expect(workflowChildLabelKey('error')).toBe('chat.workflowRun.childFailed');
  });

  it('falls back to the failure key for a status outside the union', () => {
    expect(workflowChildLabelKey('exploded' as WorkflowChildStatus)).toBe(
      'chat.workflowRun.childFailed',
    );
  });
});

describe('WorkflowChildStatusIcon', () => {
  it.each([
    ['running', 'Running'],
    ['ok', 'Done'],
    ['invalid_schema', 'Invalid result'],
    ['cancelled', 'Stopped'],
    ['timeout', 'Timed out'],
    ['error', 'Failed'],
  ] as Array<[WorkflowChildStatus, string]>)(
    'names the %s icon for screen readers',
    (status, label) => {
      render(<WorkflowChildStatusIcon status={status} />);
      expect(screen.getByLabelText(label)).toBeInTheDocument();
    },
  );

  it('renders the ascii liveness glyph only while the child is running', () => {
    // Liveness is the shared ascii Loader (role="status"), not a spinning
    // lucide icon — the same glyph as the nav tree and the task cards.
    const { unmount } = render(<WorkflowChildStatusIcon status="running" />);
    expect(screen.getByRole('status', { name: 'Running' })).toBeInTheDocument();
    unmount();

    render(<WorkflowChildStatusIcon status="ok" />);
    expect(screen.queryByRole('status')).toBeNull();
  });

  it('defaults to the inline-card size and honours the panel override', () => {
    const { unmount } = render(<WorkflowChildStatusIcon status="ok" />);
    expect(screen.getByLabelText('Done')).toHaveStyle({ width: '11px', height: '11px' });
    unmount();

    render(<WorkflowChildStatusIcon status="ok" size={12} />);
    expect(screen.getByLabelText('Done')).toHaveStyle({ width: '12px', height: '12px' });
  });

  it('renders the failure icon for a status outside the union', () => {
    render(<WorkflowChildStatusIcon status={'exploded' as WorkflowChildStatus} />);
    expect(screen.getByLabelText('Failed')).toBeInTheDocument();
  });
});

function child(patch: Partial<WorkflowChild> = {}): WorkflowChild {
  return {
    seq: 0,
    label: 'scan the logs',
    subagentType: 'research',
    phase: null,
    childTaskId: 'abc123',
    status: 'ok',
    durationS: 12.3,
    error: null,
    tokensUsed: null,
    ...patch,
  };
}

describe('WorkflowChildRow', () => {
  it('names an unlabelled child by its dispatch position on both surfaces', () => {
    const { unmount } = render(<WorkflowChildRow child={child({ label: '', seq: 3 })} surface="card" />);
    expect(screen.getByText('agent 4')).toBeInTheDocument();
    unmount();

    render(<WorkflowChildRow child={child({ label: '', seq: 3 })} surface="detail" />);
    expect(screen.getByText('agent 4')).toBeInTheDocument();
  });

  it('drops an unknown dispatch type on the card but keeps the detail column', () => {
    const bare = child({ subagentType: '' });
    const { unmount } = render(<WorkflowChildRow child={bare} surface="card" />);
    expect(screen.getByTestId('workflow-child-row').querySelectorAll('span')).toHaveLength(2);
    unmount();

    render(<WorkflowChildRow child={bare} surface="detail" />);
    expect(screen.getByTestId('workflow-detail-child-row').querySelectorAll('span')).toHaveLength(3);
  });

  it('leaves an unfinished duration blank on the card and names the state on the detail', () => {
    // Ignore the liveness glyph's screen-reader label — this pin is about the
    // visible duration cell, which stays blank on the card surface.
    const live = child({ status: 'running', durationS: null });
    const { unmount } = render(<WorkflowChildRow child={live} surface="card" />);
    expect(screen.queryByText('Running', { ignore: '.sr-only' })).not.toBeInTheDocument();
    unmount();

    render(<WorkflowChildRow child={live} surface="detail" />);
    expect(screen.getByText('Running', { ignore: '.sr-only' })).toBeInTheDocument();
  });

  it('renders the telemetry cell only when the caller supplies one', () => {
    render(<WorkflowChildRow child={child()} surface="detail" meta="4 tool calls · 1.2k" />);
    expect(screen.getByText('4 tool calls · 1.2k')).toBeInTheDocument();
  });

  it('is inert without onOpen and a keyboard-activable control with it', () => {
    const { unmount } = render(<WorkflowChildRow child={child()} surface="detail" />);
    expect(screen.getByTestId('workflow-detail-child-row')).not.toHaveAttribute('role', 'button');
    unmount();

    const onOpen = vi.fn();
    render(<WorkflowChildRow child={child()} surface="detail" onOpen={onOpen} />);
    const row = screen.getByRole('button');
    fireEvent.click(row);
    fireEvent.keyDown(row, { key: 'Enter' });
    fireEvent.keyDown(row, { key: ' ' });
    fireEvent.keyDown(row, { key: 'a' });
    expect(onOpen).toHaveBeenCalledTimes(3);
  });
});

describe('summarizeRun', () => {
  const withChildren = (children: WorkflowChild[], patch: Partial<WorkflowRunState> = {}) => ({
    ...createWorkflowRunState(),
    children,
    ...patch,
  });

  it('reports empty totals for a run that has not reported yet', () => {
    expect(summarizeRun(undefined)).toEqual({
      children: [],
      doneCount: 0,
      agentCount: 0,
      duration: null,
    });
  });

  it('counts every settled child as done, whatever its outcome', () => {
    const summary = summarizeRun(
      withChildren([
        child({ seq: 0, status: 'ok' }),
        child({ seq: 1, status: 'error' }),
        child({ seq: 2, status: 'cancelled' }),
        child({ seq: 3, status: 'running' }),
      ]),
    );
    expect(summary.doneCount).toBe(3);
    expect(summary.agentCount).toBe(4);
  });

  it('prefers the run\'s promised total over the children dispatched so far', () => {
    const summary = summarizeRun(withChildren([child()], { childrenTotal: 9, durationS: 61.2 }));
    expect(summary.agentCount).toBe(9);
    expect(summary.duration).toBe('1m 01s');
  });
});

describe('workflowChildStatusColor', () => {
  it('gives a schema miss the same amber the row already shows', () => {
    // The detail panel prints the child's error directly beneath its row. A
    // hardcoded loss colour there made one child read calm amber and alarming
    // red at once, for a child that ran fine and only mismatched its schema.
    expect(workflowChildStatusColor('invalid_schema')).toBe('var(--color-warning)');
    expect(workflowChildStatusColor('error')).toBe('var(--color-icon-danger)');
    expect(workflowChildStatusColor('invalid_schema')).not.toBe(
      workflowChildStatusColor('error'),
    );
  });

  it('falls back to the error colour for a status it does not know', () => {
    expect(workflowChildStatusColor('unheard-of' as WorkflowChildStatus)).toBe(
      workflowChildStatusColor('error'),
    );
  });
});
