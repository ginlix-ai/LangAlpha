/**
 * PTCAgentCard — pending approval flow + the live dispatch status indicator.
 *
 * The approved card reads its dispatched thread's liveness from the shared
 * DispatchStatusProvider (one batched getDispatchLiveness request per turn) and
 * maps the backend WorkflowStatus enum onto a pill: active → Working,
 * interrupted → Needs input, completed → Completed, etc. The kicker shows the
 * workspace name. These tests mock the api module so the card's wiring is
 * exercised without a real backend.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, fireEvent } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('../../utils/api', () => ({
  getDispatchLiveness: vi.fn(),
}));

// The card now localizes every label via useTranslation(); mock it so `t()`
// echoes the key, letting assertions target stable keys instead of English copy.
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

import { getDispatchLiveness } from '../../utils/api';
import PTCAgentCard from '../PTCAgentCard';
import { DispatchStatusProvider } from '../../hooks/usePTCDispatchStatus';

const mockLiveness = getDispatchLiveness as unknown as Mock;

const APPROVED = {
  status: 'approved' as const,
  question: 'Is NVDA overvalued vs AMD on datacenter share?',
  workspace_name: 'Semiconductors',
  thread_id: 'thread-123',
  workspace_id: 'ws-1',
};

/** Render an approved card inside the batched-liveness provider. */
function renderApproved(proposalData = APPROVED) {
  return renderWithProviders(
    <DispatchStatusProvider>
      <PTCAgentCard proposalData={proposalData} onApprove={vi.fn()} onReject={vi.fn()} />
    </DispatchStatusProvider>,
  );
}

describe('PTCAgentCard — pending approval', () => {
  beforeEach(() => vi.clearAllMocks());

  it('shows the workspace name as the kicker, question, and actions; never polls liveness', () => {
    const onApprove = vi.fn();
    renderWithProviders(
      <PTCAgentCard
        proposalData={{ status: 'pending', question: 'Compare NVDA vs AMD', workspace_name: 'Semiconductors', report_back: true }}
        onApprove={onApprove}
        onReject={vi.fn()}
      />,
    );

    expect(screen.getByText('Semiconductors')).toBeInTheDocument();
    expect(screen.getByText('chat.ptcCard.awaitingApproval')).toBeInTheDocument();
    expect(screen.getByText('Compare NVDA vs AMD')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
    expect(mockLiveness).not.toHaveBeenCalled();
  });

  it('exposes the report-back control as a switch reflecting its checked state', () => {
    renderWithProviders(
      <PTCAgentCard
        proposalData={{ status: 'pending', question: 'Q', report_back: true }}
        onApprove={vi.fn()}
        onReject={vi.fn()}
      />,
    );

    const toggle = screen.getByRole('switch');
    expect(toggle).toHaveAttribute('aria-checked', 'true');
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-checked', 'false');
  });

  it('approves with the current report-back choice', () => {
    const onApprove = vi.fn();
    renderWithProviders(
      <PTCAgentCard
        proposalData={{ status: 'pending', question: 'Q', report_back: true }}
        onApprove={onApprove}
        onReject={vi.fn()}
      />,
    );

    // Toggle report-back off, then approve.
    fireEvent.click(screen.getByRole('switch'));
    fireEvent.click(screen.getByRole('button', { name: /approve/i }));
    expect(onApprove).toHaveBeenCalledWith({ report_back: false });
  });
});

describe('PTCAgentCard — live dispatch status', () => {
  beforeEach(() => vi.clearAllMocks());

  it('shows "Working" while the dispatched run is active, via one batched liveness read', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'active', run_id: 'run-1', can_reconnect: true },
    ]);
    renderApproved();

    await waitFor(() => expect(screen.getByText('chat.ptcCard.statusWorking')).toBeInTheDocument());
    expect(mockLiveness).toHaveBeenCalledWith(['thread-123']);
    expect(screen.getByText('Semiconductors')).toBeInTheDocument(); // workspace kicker
    // Footer hint + CTA come from the same STATUS_UI row as the pill.
    expect(screen.getByText('chat.ptcCard.hintWorking')).toBeInTheDocument();
    expect(screen.getByText('chat.ptcCard.ctaOpenThread')).toBeInTheDocument();
  });

  // Backend WorkflowStatus → pill / hint / CTA row (a missing liveness row means
  // the run hasn't registered yet → 'starting').
  it.each<[string | null, string, string | null, string]>([
    [null, 'statusStarting', 'hintProvisioning', 'ctaOpenThread'],
    ['interrupted', 'statusNeedsInput', null, 'ctaAnswerContinue'],
    ['completed', 'statusCompleted', null, 'ctaOpenThread'],
    ['failed', 'statusFailed', 'hintFailed', 'ctaViewThread'],
    ['cancelled', 'statusStopped', 'hintStopped', 'ctaViewThread'],
  ])('maps wire status %s → %s pill + footer', async (wire, pill, hint, cta) => {
    mockLiveness.mockResolvedValue(
      wire === null ? [] : [{ thread_id: 'thread-123', status: wire, run_id: null, can_reconnect: false }],
    );
    renderApproved();

    await waitFor(() => expect(screen.getByText(`chat.ptcCard.${pill}`)).toBeInTheDocument());
    if (hint) expect(screen.getByText(`chat.ptcCard.${hint}`)).toBeInTheDocument();
    expect(screen.getByText(`chat.ptcCard.${cta}`)).toBeInTheDocument();
  });
});

describe('PTCAgentCard — rejected', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders a quiet "Research declined" row and does not poll', () => {
    renderWithProviders(
      <PTCAgentCard proposalData={{ status: 'rejected', question: 'Q' }} onApprove={vi.fn()} onReject={vi.fn()} />,
    );
    expect(screen.getByText('chat.ptcCard.researchDeclined')).toBeInTheDocument();
    expect(mockLiveness).not.toHaveBeenCalled();
  });

  it('toggles aria-expanded when the declined row is opened', () => {
    renderWithProviders(
      <PTCAgentCard proposalData={{ status: 'rejected', question: 'Q' }} onApprove={vi.fn()} onReject={vi.fn()} />,
    );
    const toggle = screen.getByRole('button', { name: /researchDeclined/i });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
  });
});
