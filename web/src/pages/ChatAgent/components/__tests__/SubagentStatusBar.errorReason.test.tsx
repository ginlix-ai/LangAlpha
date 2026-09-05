/**
 * The detail-view "clue inside" for a Failed card: the status chip alone said
 * "Failed" with no cause. These lock that an errored agent renders the reason
 * banner (with the ledger message when present), and that a non-errored agent
 * never does.
 *
 * A credit stop is the one terminal that is neither: it settles ``cancelled``
 * because nothing malfunctioned, but it still carries a reason, so it keeps
 * the banner in neutral dress rather than losing the explanation along with
 * the danger styling. A plain cancel has no reason and grows no banner.
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

  it('shows a stopped task its reason, without calling it an error', () => {
    const denial = 'Not enough credits to continue.';
    render(
      <SubagentStatusBar
        agent={{ ...baseAgent, status: 'cancelled', error: denial }}
        threadId="t-1"
      />,
    );
    expect(screen.getByText('This agent stopped')).toBeInTheDocument();
    expect(screen.getByText(denial)).toBeInTheDocument();
    expect(screen.queryByText('This agent stopped with an error')).toBeNull();
    expect(screen.queryByText('Failed')).toBeNull();
  });

  it('renders no banner for a plain cancel, which carries no reason', () => {
    render(<SubagentStatusBar agent={{ ...baseAgent, status: 'cancelled' }} threadId="t-1" />);
    expect(screen.queryByText('This agent stopped')).toBeNull();
    expect(screen.queryByText('This agent stopped with an error')).toBeNull();
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
    // Neither spinner form may appear: the legacy animate-spin arc nor the
    // shared ascii Loader (role="status") that Running renders now.
    expect(container.querySelector('.animate-spin')).toBeNull();
    expect(container.querySelector('[role="status"]')).toBeNull();
    expect(screen.getByText('This agent stopped with an error')).toBeInTheDocument();
  });
});
