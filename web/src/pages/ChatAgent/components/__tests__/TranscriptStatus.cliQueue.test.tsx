import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { CliQueueWait } from '@/lib/claudeCodeQueue';
import { CLAUDE_CODE_CHOICE_URL, CLAUDE_CODE_COMPUTER_PARAM } from '@/lib/claudeCode';
import { TranscriptStatus } from '../chatView/TranscriptStatus';

vi.mock('../QueuedAutomationNotice', () => ({ QueuedAutomationNotice: () => null }));

const WAIT: CliQueueWait = { waitId: 'w1', computerId: 'c1', name: 'MacBook-Pro-7.local', running: 2, cap: 2, settable: true, max: 8 };

function renderStatus(cliQueue: CliQueueWait | null) {
  return renderWithProviders(
    <TranscriptStatus
      modelStatus={null}
      isLoading
      retired={null}
      turnStatus={{ workspaceStarting: false, turnHeld: null, cliQueue }}
      model={null}
      compaction={null}
      queuedSend={false}
      threadId="thread-1"
      active
    />,
  );
}

describe('TranscriptStatus – waiting for a Claude Code CLI', () => {
  it('names the computer and how many run there, with a way to run more there', () => {
    renderStatus(WAIT);
    const status = screen.getByRole('status');
    expect(status).toHaveTextContent('Waiting for Claude Code on MacBook-Pro-7.local: 2 agents already running there.');
    const link = screen.getByRole('link', { name: 'Run more at once' });
    expect(link).toHaveAttribute('href', `${CLAUDE_CODE_CHOICE_URL}&${CLAUDE_CODE_COMPUTER_PARAM}=c1`);
    // The link follows the line without being read out with it.
    expect(status).not.toContainElement(link);
  });

  it('says a computer at the most it runs is at it, and offers nothing', () => {
    renderStatus({ ...WAIT, running: 8, cap: 8 });
    expect(screen.getByText("Waiting for Claude Code on MacBook-Pro-7.local: 8 agents already running there. That's the most it runs at once.")).toBeInTheDocument();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('offers nothing for a computer whose count follows its size', () => {
    renderStatus({ ...WAIT, name: null, running: 1, cap: 1, settable: false });
    expect(screen.getByText('Waiting for Claude Code on your computer: 1 agent already running there.')).toBeInTheDocument();
    expect(screen.queryByText(/most it runs/)).not.toBeInTheDocument();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('shows nothing while no turn waits', () => {
    renderStatus(null);
    expect(screen.queryByText(/Waiting for Claude Code/)).not.toBeInTheDocument();
  });
});
