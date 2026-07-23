/**
 * The detail-view "clue inside" for a Failed card: the status chip alone said
 * "Failed" with no cause. These lock that an errored agent renders the reason
 * banner (with the ledger message when present), and that a non-errored agent
 * never does.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';

// The bar imports the api layer (raw fetch) and a Markdown renderer; neither
// is under test here.
vi.mock('../../utils/api', () => ({ sendSubagentMessage: vi.fn() }));
vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => <span>{content}</span>,
}));

import SubagentStatusBar from '../SubagentStatusBar';

const baseAgent = {
  name: 'Task-k7Xm2p',
  type: 'equity-analyst',
  description: 'Research the semiconductor selloff',
  messages: [],
};

describe('SubagentStatusBar — failure reason banner', () => {
  it('shows the reason banner with the ledger message for an errored task', () => {
    render(
      <SubagentStatusBar
        agent={{ ...baseAgent, status: 'error', error: 'transport_lost: subagent event spill failed' }}
        threadId="t-1"
      />,
    );
    expect(screen.getByText('This agent stopped with an error')).toBeInTheDocument();
    expect(screen.getByText('transport_lost: subagent event spill failed')).toBeInTheDocument();
  });

  it('shows the banner headline even when no ledger reason is available', () => {
    render(<SubagentStatusBar agent={{ ...baseAgent, status: 'error' }} threadId="t-1" />);
    expect(screen.getByText('This agent stopped with an error')).toBeInTheDocument();
  });

  it('renders no error banner for a completed task', () => {
    render(<SubagentStatusBar agent={{ ...baseAgent, status: 'completed' }} threadId="t-1" />);
    expect(screen.queryByText('This agent stopped with an error')).toBeNull();
  });

  it('shows Failed, not "Running", for a task reaped mid-tool-call', () => {
    // A run reaped mid-WebFetch leaves that tool call forever "in progress";
    // the terminal outcome must win over the derived current tool so the bar
    // never contradicts its own error banner with a "Running: WebFetch" spinner.
    const { container } = render(
      <SubagentStatusBar
        agent={{ ...baseAgent, status: 'error', error: 'worker_lost: no live executor', currentTool: 'WebFetch' }}
        threadId="t-1"
      />,
    );
    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.queryByText('Running: WebFetch')).toBeNull();
    expect(container.querySelector('.animate-spin')).toBeNull();
    expect(screen.getByText('This agent stopped with an error')).toBeInTheDocument();
  });
});
