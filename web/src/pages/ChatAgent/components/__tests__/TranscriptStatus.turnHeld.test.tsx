import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { TranscriptStatus } from '../chatView/TranscriptStatus';

vi.mock('../QueuedAutomationNotice', () => ({ QueuedAutomationNotice: () => null }));

function renderStatus(props: { turnHeld: string | null; workspaceStarting?: false | 'starting' }) {
  return renderWithProviders(
    <TranscriptStatus
      modelStatus={null}
      isLoading={false}
      retired={null}
      workspaceStarting={props.workspaceStarting ?? false}
      turnHeld={props.turnHeld}
      isCompacting={false}
      compactionTokens={null}
      queuedSend={false}
      threadId="thread-1"
      active
    />,
  );
}

describe('TranscriptStatus – turn held line', () => {
  it('compact reuses the compacting copy', () => {
    renderStatus({ turnHeld: 'compact' });
    expect(screen.getByText('Compacting conversation...')).toBeInTheDocument();
  });

  it('handoff names the previous model', () => {
    renderStatus({ turnHeld: 'handoff' });
    expect(
      screen.getByText('Waiting for the previous model to write out its summary...'),
    ).toBeInTheDocument();
  });

  it.each(['offload', '', 'something-new'])('"%s" falls back to the generic wait copy', (reason) => {
    renderStatus({ turnHeld: reason });
    expect(
      screen.getByText('Waiting for the current operation on this conversation to finish...'),
    ).toBeInTheDocument();
  });

  it('renders nothing while not held, and beside the workspace line when both apply', () => {
    const { unmount } = renderStatus({ turnHeld: null });
    expect(screen.queryByText(/Waiting for the/)).not.toBeInTheDocument();
    unmount();
    renderStatus({ turnHeld: 'handoff', workspaceStarting: 'starting' });
    expect(screen.getByText('Starting workspace...')).toBeInTheDocument();
    expect(screen.getByText(/previous model/)).toBeInTheDocument();
  });
});
