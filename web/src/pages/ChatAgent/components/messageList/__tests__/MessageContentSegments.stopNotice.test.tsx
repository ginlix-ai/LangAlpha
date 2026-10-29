/**
 * Where the credit-stop notice sits, and why it sits there.
 *
 * The agent's closing prose was written before the gate fired and still
 * promises a result ("I'll surface the summary as soon as it completes"), so a
 * notice rendered beside the task card is read first and contradicted second.
 * It belongs at the foot of the message, after the prose, where the turn's
 * outcome goes.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import type { SubagentTaskRecord } from '@/types/chat';
import { MessageContentSegments } from '../MessageContentSegments';
import { SubagentTelemetryContext } from '../../SubagentTelemetryContext';
import type { SubagentTelemetry } from '../../../session/subagents/resolveSubagentTelemetry';

// Every telemetry read, in order. The notices read once per render, and the
// run rows that also read are stubbed below, so this is a render count of the
// notices.
const telemetryReads: string[] = [];
vi.mock('../../SubagentTelemetryContext', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../SubagentTelemetryContext')>();
  return {
    ...actual,
    useSubagentTelemetries: (ids: readonly string[]) => {
      telemetryReads.push(...ids);
      return actual.useSubagentTelemetries(ids);
    },
  };
});
vi.mock('../SubagentRun', () => {
  const SubagentRun = () => <div data-testid="subagent-run" />;
  return { SubagentRun, default: SubagentRun };
});

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

const PROSE = "I'll surface the summary as soon as it completes.";

const STOPPED_TASK: Record<string, SubagentTaskRecord> = {
  'tc-1': {
    subagentId: 'tc-1',
    description: 'Analyze NVDA recent technical setup',
    prompt: 'Analyze NVDA recent technical setup',
    type: 'equity-analyst',
    action: 'init',
    status: 'cancelled',
  },
};

const props: SegmentsProps = {
  segments: [
    { type: 'subagent_task', order: 0, subagentId: 'tc-1' },
    { type: 'text', order: 1, content: PROSE },
  ] as SegmentsProps['segments'],
  reasoningProcesses: {},
  toolCallProcesses: {},
  todoListProcesses: {},
  subagentTasks: STOPPED_TASK,
  isStreaming: false,
};

function renderWith(telemetry: Partial<SubagentTelemetry> | undefined) {
  return render(
    <MemoryRouter>
      <SubagentTelemetryContext.Provider
        value={{
          resolve: () =>
            telemetry
              ? { toolCalls: 13, tokenUsage: { input: 0, output: 0, total: 0 }, ...telemetry }
              : undefined,
          followsRuns: true,
        }}
      >
        <MessageContentSegments {...props} />
      </SubagentTelemetryContext.Provider>
    </MemoryRouter>,
  );
}

const CREDIT_STOP = {
  stopReason: 'Monthly credit limit reached (50/50 credits)',
  stopReasonType: 'credit_stop',
};

describe('MessageContentSegments: credit-stop notice placement', () => {
  it('renders the notice after the closing prose, not beside the task card', () => {
    const { container } = renderWith(CREDIT_STOP);
    const notice = screen.getByTestId('subagent-credit-stop-notice');
    const prose = screen.getByText(PROSE, { exact: false });
    expect(notice).toBeInTheDocument();
    expect(
      prose.compareDocumentPosition(notice) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(container.querySelectorAll('[data-testid="subagent-credit-stop-notice"]')).toHaveLength(1);
  });

  it('renders nothing at the foot for a stop the user cannot act on', () => {
    renderWith({ stopReason: 'transport_lost: the stream tore mid-run' });
    expect(screen.queryByTestId('subagent-credit-stop-notice')).toBeNull();
  });

  it('renders nothing at the foot for an ordinary turn', () => {
    renderWith(undefined);
    expect(screen.queryByTestId('subagent-credit-stop-notice')).toBeNull();
  });
});

describe('MessageContentSegments: credit-stop notice while prose streams', () => {
  // Renders, not the work inside one: compiled, the notice caches its links on
  // the reason, so a count of that work stays flat with the memo removed.
  it('does not re-render the notices for a chunk of prose', () => {
    const telemetry = { toolCalls: 13, tokenUsage: { input: 0, output: 0, total: 0 }, ...CREDIT_STOP };
    const source = { resolve: (): SubagentTelemetry => telemetry, followsRuns: true };
    const at = (text: string) => (
      <MemoryRouter>
        <SubagentTelemetryContext.Provider value={source}>
          <MessageContentSegments
            {...props}
            isStreaming
            subagentTasks={{}}
            segments={[
              { type: 'subagent_task', order: 0, subagentId: 'tc-1' },
              { type: 'subagent_task', order: 1, subagentId: 'tc-2' },
              { type: 'text', order: 2, content: text },
            ] as SegmentsProps['segments']}
          />
        </SubagentTelemetryContext.Provider>
      </MemoryRouter>
    );
    let text = PROSE;
    telemetryReads.length = 0;
    const { rerender } = render(at(text));
    expect(new Set(telemetryReads)).toEqual(new Set(['tc-1', 'tc-2']));
    telemetryReads.length = 0;
    for (let i = 0; i < 20; i++) {
      text += ' More';
      rerender(at(text));
    }
    expect(telemetryReads).toHaveLength(0);
    // Both tasks stopped under one denial, which is one notice.
    expect(screen.getAllByTestId('subagent-credit-stop-notice')).toHaveLength(1);
  });
});
