import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { TranscriptStatus } from '../chatView/TranscriptStatus';

vi.mock('../QueuedAutomationNotice', () => ({ QueuedAutomationNotice: () => null }));

function renderStatus(props: { turnHeld: string | null; workspaceStarting?: false | 'starting'; model?: string | null }) {
  return renderWithProviders(
    <TranscriptStatus
      modelStatus={null}
      isLoading={false}
      retired={null}
      workspaceStarting={props.workspaceStarting ?? false}
      turnHeld={props.turnHeld}
      model={props.model ?? null}
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

  // Who writes the brief is settled only once one is written, so the wait
  // names only the model the thread goes to.
  it('a brief names the model the thread goes to', () => {
    renderStatus({ turnHeld: 'brief', model: 'claude-sonnet-5-5-code' });
    expect(screen.getByText('Handing off context to Sonnet 5.5 Code...')).toBeInTheDocument();
    expect(screen.queryByText(/previous model/)).not.toBeInTheDocument();
  });

  it('a brief with no model known yet says the new model', () => {
    renderStatus({ turnHeld: 'brief' });
    expect(screen.getByText('Handing off context to the new model...')).toBeInTheDocument();
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
