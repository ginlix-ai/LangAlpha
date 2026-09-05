/**
 * The "finishing current step" task state is DERIVED from the message's own
 * credit pauses at render time, never written onto the task record. These pin
 * the two properties that choice buys: a still-running task reads as finishing
 * while its turn holds an unanswered pause, and it goes back to running the
 * moment the pause resolves — with no second write, and nothing for history
 * replay to reproduce.
 */
import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { CreditPauseState, SubagentTaskRecord } from '@/types/chat';
import { MessageContentSegments } from '../MessageContentSegments';
import { CreditPausePendingProvider } from '../../CreditPausePendingContext';

// The status chip paints its word twice (the ascii liveness glyph carries it as
// its screen-reader label, the chip renders it visibly), so count instead of
// asserting a single node.
const shows = (label: string): boolean => screen.queryAllByText(label).length > 0;

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

/** MessageBubble wraps every message in this provider; the cards read it. */
function Segments(p: SegmentsProps): React.ReactElement {
  return (
    <CreditPausePendingProvider creditPauses={p.creditPauses}>
      <MessageContentSegments {...p} />
    </CreditPausePendingProvider>
  );
}

const RUNNING_TASK: Record<string, SubagentTaskRecord> = {
  'tc-1': {
    subagentId: 'tc-1',
    description: 'Research the filing',
    prompt: 'Research the filing',
    type: 'research',
    action: 'init',
    status: 'running',
  },
};

function props(creditPauses: Record<string, CreditPauseState>): SegmentsProps {
  return {
    segments: [
      { type: 'subagent_task', order: 0, subagentId: 'tc-1' },
      ...(Object.keys(creditPauses).length
        ? [{ type: 'credit_pause', order: 1, proposalId: 'int-1' }]
        : []),
    ] as SegmentsProps['segments'],
    reasoningProcesses: {},
    toolCallProcesses: {},
    todoListProcesses: {},
    subagentTasks: RUNNING_TASK,
    creditPauses,
    isStreaming: false,
    isAssistant: true,
  };
}

const PENDING: Record<string, CreditPauseState> = {
  'int-1': { status: 'pending', message: 'Out of credits.', interruptId: 'int-1' },
};
const RESUMED: Record<string, CreditPauseState> = {
  'int-1': { status: 'resumed', message: 'Out of credits.', interruptId: 'int-1' },
};

describe('MessageContentSegments — credit pause and task status', () => {
  it('leaves a running task alone when the message has no pause', () => {
    render(<Segments {...props({})} />);
    expect(shows('Running')).toBe(true);
    expect(shows('Finishing current step')).toBe(false);
  });

  it('reads a running task as finishing while a pause on this message is pending', () => {
    render(<Segments {...props(PENDING)} />);
    expect(shows('Finishing current step')).toBe(true);
    expect(shows('Running')).toBe(false);
  });

  it('reverts to running when the pause resolves, with no write to the task record', () => {
    const view = render(<Segments {...props(PENDING)} />);
    expect(shows('Finishing current step')).toBe(true);

    view.rerender(<Segments {...props(RESUMED)} />);

    expect(shows('Running')).toBe(true);
    expect(shows('Finishing current step')).toBe(false);
    // The record the projection wrote is still exactly what it wrote — the
    // pausing state never touched it, which is why nothing has to undo it.
    expect(RUNNING_TASK['tc-1'].status).toBe('running');
  });

  it('does not touch a task that is not running', () => {
    const completed: Record<string, SubagentTaskRecord> = {
      'tc-1': { ...RUNNING_TASK['tc-1'], status: 'completed' },
    };
    render(<Segments {...props(PENDING)} subagentTasks={completed} />);
    expect(shows('Completed')).toBe(true);
    expect(shows('Finishing current step')).toBe(false);
  });
});
