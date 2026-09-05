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
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router-dom';
import type { SubagentTaskRecord } from '@/types/chat';
import { MessageContentSegments } from '../MessageContentSegments';
import { SubagentTelemetryContext } from '../../SubagentTelemetryContext';
import type { SubagentTelemetry } from '../../../session/subagents/resolveSubagentTelemetry';

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
  isAssistant: true,
};

function renderWith(telemetry: Partial<SubagentTelemetry> | undefined) {
  return render(
    <MemoryRouter>
      <SubagentTelemetryContext.Provider
        value={() =>
          telemetry
            ? { toolCalls: 13, tokenUsage: { input: 0, output: 0, total: 0 }, ...telemetry }
            : undefined
        }
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

describe('MessageContentSegments — credit-stop notice placement', () => {
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
