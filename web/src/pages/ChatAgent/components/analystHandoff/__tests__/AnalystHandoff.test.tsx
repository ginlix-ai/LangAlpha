/**
 * A Chief of Staff hand-off to a workspace analyst, as a reply draws one, in
 * each answer: the approval card, the slip that follows a handed-off run, and
 * the declined row.
 *
 * The slip reads its run's liveness from the shared DispatchStatusProvider
 * (one batched getHandoffLiveness request per turn) and maps the backend
 * status onto the hand-off's phase. The api module is mocked so the wiring is
 * exercised without a backend, and `t()` echoes the key so assertions target
 * stable keys rather than English copy.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, screen, waitFor, fireEvent } from '@testing-library/react';
import { Route, Routes, useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { applyFeedEvent, resetThreadLifecycle } from '@/lib/threadLifecycle/store';

const liveness = vi.hoisted(() => ({ threads: vi.fn(), runs: vi.fn() }));
vi.mock('../../../utils/api', () => ({
  // One batched read, answered per thread and per run by the two mocks.
  getHandoffLiveness: vi.fn(async ({ threads, runs }: { threads: string[]; runs: string[] }) => ({
    liveness: (await liveness.threads(threads)) ?? [],
    runs: runs.length ? ((await liveness.runs(runs)) ?? []) : [],
  })),
}));

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
  Trans: ({ i18nKey }: { i18nKey: string }) => i18nKey,
}));

import type { PTCAgentProposalState } from '@/types/chat';
import { DispatchStatusProvider } from '../../../hooks/usePTCDispatchStatus';
import { AnalystHandoffRun } from '../../messageList/AnalystHandoffRun';
import { MessageActionsProvider, type MessageActions } from '../../messageList/MessageActionsContext';
import { HandoffPlacesContext, type HandoffPlace } from '../reportLinks';

const mockLiveness = liveness.threads;
const mockRuns = liveness.runs;

const APPROVED = {
  status: 'approved' as const,
  question: 'Is NVDA overvalued vs AMD on datacenter share?',
  workspace_name: 'Semiconductors',
  thread_id: 'thread-123',
  workspace_id: 'ws-1',
};

type Proposal = PTCAgentProposalState;

/** One hand-off on its own in a reply, its answers taken as the card gives them. */
function Handoff({
  proposal,
  onApprove = vi.fn(),
  onReject = vi.fn(),
}: {
  proposal: Proposal;
  onApprove?: (overrides: { report_back?: boolean } | undefined) => void;
  onReject?: () => void;
}) {
  return (
    <AnalystHandoffRun
      members={[{ key: 'k', proposalId: 'p-k', proposal }]}
      onApprove={(_, overrides) => onApprove(overrides)}
      onReject={() => onReject()}
    />
  );
}

function Where() {
  const location = useLocation();
  return <span data-testid="where">{location.pathname}</span>;
}

/** An approved hand-off inside the batched-liveness provider, with where the
 *  list placed it (the report that answers it, when it was made) and how the
 *  host reveals a message, if it can. */
function renderApproved(
  proposalData: Proposal = APPROVED,
  { place = null as HandoffPlace | null, onRevealMessage = undefined as MessageActions['onRevealMessage'] } = {},
) {
  const key = proposalData.tool_call_id ?? proposalData.interruptId;
  const places = new Map<string, HandoffPlace>(place && key ? [[key, place]] : []);
  return renderWithProviders(
    <DispatchStatusProvider>
      <MessageActionsProvider actions={{ onRevealMessage }}>
        <HandoffPlacesContext value={places}>
          <Routes>
            <Route
              path="*"
              element={
                <>
                  <Handoff proposal={proposalData} />
                  <Where />
                </>
              }
            />
          </Routes>
        </HandoffPlacesContext>
      </MessageActionsProvider>
    </DispatchStatusProvider>,
  );
}

/** The slip, named by its task. */
const slip = (name: string = APPROVED.question) => screen.getByRole('button', { name });

/** Whether the slip tells a screen reader `key`: the status words are part of
 *  the slip's description, whole (`finished` is not `finishedAfter`). */
function describedAs(key: string, name?: string) {
  expect(slip(name)).toHaveAccessibleDescription(new RegExp(`${key.replace(/\./g, '\\.')}(?![A-Za-z])`));
}

/** What the slip announces: a change after its first read, and only that. */
const announced = () => screen.getByRole('status');

function renderPending(onApprove = vi.fn(), onReject = vi.fn(), question = 'Compare NVDA vs AMD') {
  renderWithProviders(
    <Handoff
      proposal={{ status: 'pending', question, workspace_name: 'Semiconductors', report_back: true }}
      onApprove={onApprove}
      onReject={onReject}
    />,
  );
  return { onApprove, onReject };
}

describe('A hand-off: awaiting an answer', () => {
  beforeEach(() => vi.clearAllMocks());

  it('names the hand-off and shows the brief and the three answers; never polls liveness', () => {
    renderPending();

    expect(screen.getByText('chat.ptcCard.handOffTo')).toBeInTheDocument();
    expect(screen.getByText('Compare NVDA vs AMD')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'chat.ptcCard.handOff' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'chat.ptcCard.alwaysApprove' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'chat.ptcCard.decline' })).toBeInTheDocument();
    expect(mockLiveness).not.toHaveBeenCalled();
  });

  it('offers report-back as a labelled checkbox reflecting its state', () => {
    renderPending();

    const box = screen.getByRole('checkbox', { name: 'chat.ptcCard.reportBack' });
    expect(box).toBeChecked();
    fireEvent.click(box);
    expect(box).not.toBeChecked();
  });

  it('hands off with the current report-back choice', () => {
    const { onApprove } = renderPending();

    fireEvent.click(screen.getByRole('checkbox', { name: 'chat.ptcCard.reportBack' }));
    fireEvent.click(screen.getByRole('button', { name: 'chat.ptcCard.handOff' }));
    expect(onApprove).toHaveBeenCalledWith({ report_back: false });
  });

  it('declines', () => {
    const { onReject } = renderPending();

    fireEvent.click(screen.getByRole('button', { name: 'chat.ptcCard.decline' }));
    expect(onReject).toHaveBeenCalledOnce();
  });

  describe('a brief longer than two lines', () => {
    // jsdom lays nothing out: a clamped line reads as overflowing here.
    beforeEach(() => {
      vi.spyOn(HTMLElement.prototype, 'scrollHeight', 'get').mockReturnValue(120);
      vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(39);
    });
    afterEach(() => vi.restoreAllMocks());

    it('opens in place behind an expander', () => {
      renderPending(vi.fn(), vi.fn(), '## Goal\nRebuild the model\n\n- Use the 10-Q');

      // Clamped, the brief reads as one line, its headings closed off.
      expect(screen.getByText('Goal: Rebuild the model Use the 10-Q.')).toBeInTheDocument();
      const more = screen.getByRole('button', { name: 'chat.ptcCard.briefMore' });
      expect(more).toHaveAttribute('aria-expanded', 'false');

      fireEvent.click(more);
      const less = screen.getByRole('button', { name: 'chat.ptcCard.briefLess' });
      expect(less).toHaveAttribute('aria-expanded', 'true');
      expect(screen.getByRole('heading', { name: 'Goal' })).toBeInTheDocument();
    });
  });

  it('shows no expander for a brief that fits', () => {
    renderPending();
    expect(screen.queryByRole('button', { name: 'chat.ptcCard.briefMore' })).toBeNull();
  });
});

describe('A hand-off: a handed-off run', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => resetThreadLifecycle());

  it('reads the run through one batched liveness read and opens its thread', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'active', run_id: 'run-1', can_reconnect: true },
    ]);
    renderApproved();

    await waitFor(() => describedAs('chat.ptcCard.statusWorking'));
    expect(mockLiveness).toHaveBeenCalledWith(['thread-123']);
    expect(screen.getByText('Semiconductors')).toBeInTheDocument();

    // The whole slip is the way in, named by its task.
    fireEvent.click(slip());
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/thread-123');
  });

  it('times a run it opened mid-way from the run start, not from mount', async () => {
    const startedAt = new Date(Date.now() - 95_000).toISOString();
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true, run_started_at: startedAt },
    ]);
    renderApproved();

    await waitFor(() => expect(screen.getByText(/^1:3\d$/)).toBeInTheDocument());
  });

  // Backend status → the hand-off's words (a missing liveness row means the
  // run hasn't registered yet → starting).
  it.each<[string | null, string]>([
    [null, 'statusStarting'],
    ['interrupted', 'needsAnswer'],
    ['completed', 'finished'],
    ['failed', 'couldntFinish'],
    ['cancelled', 'stopped'],
  ])('maps wire status %s → %s', async (wire, words) => {
    mockLiveness.mockResolvedValue(
      wire === null ? [] : [{ thread_id: 'thread-123', status: wire, run_id: null, can_reconnect: false }],
    );
    renderApproved();

    await waitFor(() => describedAs(`chat.ptcCard.${words}`));
  });

  it('asks for an answer with a pill that the slip itself carries out', async () => {
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'interrupted', run_id: null, can_reconnect: false }]);
    renderApproved();

    await waitFor(() => expect(screen.getByText('chat.ptcCard.answer')).toBeInTheDocument());
    // The pill is a hint, not a second button.
    expect(screen.getAllByRole('button')).toHaveLength(1);
  });

  it('says how long a settled run took', async () => {
    mockLiveness.mockResolvedValue([
      {
        thread_id: 'thread-123',
        status: 'cancelled',
        run_id: null,
        can_reconnect: false,
        run_started_at: '2026-10-21T10:00:00Z',
        run_finished_at: '2026-10-21T10:06:30Z',
      },
    ]);
    renderApproved();

    await waitFor(() => describedAs('chat.ptcCard.stoppedAfter'));
  });

  it('describes nothing before its first read, and announces nothing on it', async () => {
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'completed', run_id: null, can_reconnect: false }]);
    renderApproved();

    // The default `starting` is not where the run stands.
    expect(slip()).not.toHaveAccessibleDescription(/chat\.ptcCard\.statusStarting/);
    await waitFor(() => describedAs('chat.ptcCard.finished'));
    // A page load reads every hand-off; none of them is news.
    expect(announced()).toBeEmptyDOMElement();
  });

  it('announces a change after its first read, once', async () => {
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true }]);
    renderApproved();
    await waitFor(() => describedAs('chat.ptcCard.statusWorking'));
    expect(announced()).toBeEmptyDOMElement();

    act(() => applyFeedEvent({ type: 'run_settled', thread_id: 'thread-123', run_id: 'run-1', run_seq: 1, status: 'completed' }));
    expect(announced()).toHaveTextContent('chat.ptcCard.finished');
    // The words sit in the live region now, not twice over.
    expect(slip()).toHaveAccessibleDescription(/^(?![^]*finished[^]*finished)[^]*chat\.ptcCard\.finished$/);
    // The push asks for a read of its own, which settles here.
    await waitFor(() => expect(mockLiveness).toHaveBeenCalledTimes(2));
  });
});

describe('A hand-off: a hand-off pinned to its run', () => {
  beforeEach(() => vi.clearAllMocks());

  const PINNED = { ...APPROVED, run_id: 'run-1' };
  const RUN_STARTED = '2026-10-21T10:00:00Z';

  it('reads its own run once that run is over, whatever the thread runs since', async () => {
    // A later turn on the same thread is running now.
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'running', run_id: 'run-2', can_reconnect: true }]);
    mockRuns.mockResolvedValue([
      { run_id: 'run-1', thread_id: 'thread-123', status: 'completed', run_started_at: RUN_STARTED, run_finished_at: '2026-10-21T10:00:07.500Z' },
    ]);
    renderApproved(PINNED);

    await waitFor(() => describedAs('chat.ptcCard.finishedAfter'));
    expect(mockRuns).toHaveBeenCalledWith(['run-1']);
    expect(screen.queryByText(/^\d+:\d\d$/)).toBeNull();
  });

  it('follows the thread while its own run is live, timed from that run', async () => {
    const startedAt = new Date(Date.now() - 95_000).toISOString();
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true }]);
    mockRuns.mockResolvedValue([{ run_id: 'run-1', thread_id: 'thread-123', status: 'running', run_started_at: startedAt }]);
    renderApproved(PINNED);

    await waitFor(() => describedAs('chat.ptcCard.statusWorking'));
    await waitFor(() => expect(screen.getByText(/^1:3\d$/)).toBeInTheDocument());
  });

  it('follows the thread once its own run stopped to ask, since the answer runs there', async () => {
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'completed', run_id: null, can_reconnect: false }]);
    mockRuns.mockResolvedValue([{ run_id: 'run-1', thread_id: 'thread-123', status: 'interrupted', run_started_at: RUN_STARTED }]);
    renderApproved(PINNED);

    await waitFor(() => describedAs('chat.ptcCard.finished'));
  });

  it('ends on the run its report is on, whatever the thread runs after', async () => {
    // run-1 stopped to ask, run-2 carried it on and reported, run-3 is a later turn.
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'running', run_id: 'run-3', can_reconnect: true }]);
    mockRuns.mockImplementation(async (ids: string[]) => [
      { run_id: 'run-1', thread_id: 'thread-123', status: 'interrupted', run_started_at: RUN_STARTED },
      { run_id: 'run-2', thread_id: 'thread-123', status: 'completed', run_started_at: RUN_STARTED, run_finished_at: '2026-10-21T10:00:48Z' },
    ].filter((r) => ids.includes(r.run_id)));
    renderApproved({ ...PINNED, tool_call_id: 'call-1' }, { place: { reportId: 'msg-report', reportRunId: 'run-2', outcome: 'completed' } });

    await waitFor(() => describedAs('chat.ptcCard.finishedAfter'));
    expect(mockRuns).toHaveBeenCalledWith(['run-2']);
    expect(screen.getByRole('button', { name: 'chat.ptcCard.seeReport' })).toBeInTheDocument();
  });

  it('reads the thread while its run is not known yet', async () => {
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'interrupted', run_id: null, can_reconnect: false }]);
    mockRuns.mockResolvedValue([]);
    renderApproved(PINNED);

    await waitFor(() => describedAs('chat.ptcCard.needsAnswer'));
  });
});

describe('A hand-off: the report link', () => {
  beforeEach(() => vi.clearAllMocks());

  const COMPLETED = [{ thread_id: 'thread-123', status: 'completed', run_id: null, can_reconnect: false }];
  const KEYED = { ...APPROVED, tool_call_id: 'call-1' };

  it('goes down to the report that answers it', async () => {
    mockLiveness.mockResolvedValue(COMPLETED);
    const onRevealMessage = vi.fn(() => true);
    renderApproved(KEYED, { place: { reportId: 'msg-report' }, onRevealMessage });

    await waitFor(() => describedAs('chat.ptcCard.finished'));
    fireEvent.click(screen.getByRole('button', { name: 'chat.ptcCard.seeReport' }));
    expect(onRevealMessage).toHaveBeenCalledExactlyOnceWith('msg-report');
    expect(screen.getByTestId('where')).toHaveTextContent(/^\/$/);
  });

  it('opens the thread when its report row is not on the page', async () => {
    mockLiveness.mockResolvedValue(COMPLETED);
    const onRevealMessage = vi.fn(() => false);
    renderApproved(KEYED, { place: { reportId: 'msg-report' }, onRevealMessage });

    await waitFor(() => expect(screen.getByRole('button', { name: 'chat.ptcCard.seeReport' })).toBeInTheDocument());
    fireEvent.click(screen.getByRole('button', { name: 'chat.ptcCard.seeReport' }));
    expect(onRevealMessage).toHaveBeenCalledOnce();
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/thread-123');
  });

  it('opens the thread where the view has no way to the report', async () => {
    mockLiveness.mockResolvedValue(COMPLETED);
    renderApproved(KEYED, { place: { reportId: 'msg-report' } });

    await waitFor(() => expect(screen.getByRole('button', { name: 'chat.ptcCard.seeReport' })).toBeInTheDocument());
    fireEvent.click(screen.getByRole('button', { name: 'chat.ptcCard.seeReport' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/thread-123');
  });

  it('offers no report link before the report arrives', async () => {
    mockLiveness.mockResolvedValue(COMPLETED);
    renderApproved(KEYED);

    await waitFor(() => describedAs('chat.ptcCard.finished'));
    expect(screen.queryByRole('button', { name: 'chat.ptcCard.seeReport' })).toBeNull();
  });

  it('offers no report link while the run is still going', async () => {
    mockLiveness.mockResolvedValue([{ thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true }]);
    renderApproved(KEYED, { place: { reportId: 'msg-report' } });

    await waitFor(() => describedAs('chat.ptcCard.statusWorking'));
    expect(screen.queryByRole('button', { name: 'chat.ptcCard.seeReport' })).toBeNull();
  });
});

describe('A hand-off: the run\'s title', () => {
  beforeEach(() => vi.clearAllMocks());

  it('leads with the thread\'s title and its workspace\'s current name', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true, title: 'NVDA vs AMD datacenter share', workspace_name: 'Chips' },
    ]);
    renderApproved();

    await waitFor(() => expect(screen.getByText('NVDA vs AMD datacenter share')).toBeInTheDocument());
    expect(screen.getByText('Chips')).toBeInTheDocument();
    // The title replaces the brief; the thread holds the brief.
    expect(screen.queryByText(APPROVED.question)).toBeNull();
  });

  it('reads a title that is still the placeholder as no title', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true, title: APPROVED.question },
    ]);
    renderApproved();

    await waitFor(() => describedAs('chat.ptcCard.statusWorking'));
    expect(screen.getByText(APPROVED.question)).toBeInTheDocument();
  });

  it('shows a generated title that happens to open the brief', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true, title: 'Is NVDA overvalued' },
    ]);
    renderApproved();

    await waitFor(() => expect(screen.getByText('Is NVDA overvalued')).toBeInTheDocument());
  });
});

describe('A hand-off: a dispatch that failed after approval', () => {
  beforeEach(() => vi.clearAllMocks());

  const UNKNOWN = { ...APPROVED, tool_call_id: 'call-1', dispatch_failed: true, dispatch_unknown: true };
  const MADE_AT = Date.parse('2026-10-21T10:00:00Z');

  /** The slip's phase, which a slip with nothing to open says in no description. */
  const phase = () => document.querySelector('[data-handoff-phase]')?.getAttribute('data-handoff-phase');

  it('reads as couldn\'t start, not starting, when no thread was started', () => {
    renderApproved({ ...APPROVED, thread_id: undefined, dispatch_failed: true });

    expect(phase()).toBe('not_started');
    expect(screen.getAllByText('chat.ptcCard.couldntStart')).toHaveLength(2);
    // Nothing to open or poll.
    expect(screen.queryByRole('button')).toBeNull();
    expect(mockLiveness).not.toHaveBeenCalled();
  });

  it('reads as couldn\'t start, and does not poll, when a named thread definitely ran nothing', () => {
    // A continuation that failed names its thread, whose other runs are not this one.
    renderApproved({ ...APPROVED, dispatch_failed: true });

    expect(phase()).toBe('not_started');
    describedAs('chat.ptcCard.couldntStart');
    expect(mockLiveness).not.toHaveBeenCalled();
  });

  it('reads as couldn\'t start while the thread an unknown outcome named shows no run', async () => {
    mockLiveness.mockResolvedValue([]);
    renderApproved(UNKNOWN, { place: { madeAt: MADE_AT } });

    await waitFor(() => expect(mockLiveness).toHaveBeenCalledWith(['thread-123']));
    await waitFor(() => describedAs('chat.ptcCard.couldntStart'));
  });

  it('follows a run that turns up on that thread after the hand-off', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-123', status: 'running', run_id: 'run-1', can_reconnect: true, run_started_at: '2026-10-21T10:00:05Z' },
    ]);
    renderApproved(UNKNOWN, { place: { madeAt: MADE_AT } });

    await waitFor(() => describedAs('chat.ptcCard.statusWorking'));
  });

  it('does not take a run the thread started before the hand-off for its own', async () => {
    mockLiveness.mockResolvedValue([
      {
        thread_id: 'thread-123',
        status: 'completed',
        run_id: null,
        can_reconnect: false,
        run_started_at: '2026-10-21T09:40:00Z',
        run_finished_at: '2026-10-21T09:50:00Z',
      },
    ]);
    renderApproved(UNKNOWN, { place: { madeAt: MADE_AT } });

    await waitFor(() => expect(mockLiveness).toHaveBeenCalled());
    await waitFor(() => describedAs('chat.ptcCard.couldntStart'));
    expect(phase()).toBe('not_started');
  });
});

describe('A hand-off: declined', () => {
  beforeEach(() => vi.clearAllMocks());

  it('is one quiet row that opens to the brief, and does not poll', () => {
    renderWithProviders(
      <Handoff proposal={{ status: 'rejected', question: 'Q', workspace_name: 'Semis' }} />,
    );
    const toggle = screen.getByRole('button', { name: /chat\.ptcCard\.declined/ });
    expect(toggle).toHaveTextContent('Semis');
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(mockLiveness).not.toHaveBeenCalled();
  });
});
