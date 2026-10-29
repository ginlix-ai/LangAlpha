/**
 * Hand-offs made side by side in a reply: those already handed off read as one
 * group under the analyst's mark, counted by where they stand, while one still
 * awaiting an answer, or declined, keeps its own place in the order.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { applyFeedEvent, resetThreadLifecycle } from '@/lib/threadLifecycle/store';

const liveness = vi.hoisted(() => ({ threads: vi.fn(), runs: vi.fn(), steers: vi.fn() }));
vi.mock('../../../utils/api', () => ({
  // One batched read, answered per thread, per run and per steer by the mocks.
  getHandoffLiveness: vi.fn(async ({ threads, runs, steers }: { threads: string[]; runs: string[]; steers: string[] }) => ({
    liveness: (await liveness.threads(threads)) ?? [],
    runs: runs.length ? ((await liveness.runs(runs)) ?? []) : [],
    steers: steers.length ? ((await liveness.steers(steers)) ?? []) : [],
  })),
}));

import { DispatchStatusProvider } from '../../../hooks/usePTCDispatchStatus';
import { HandoffPlacesContext, type HandoffPlace } from '../../analystHandoff/reportLinks';
import { AnalystHandoffRun, type AnalystHandoffMember } from '../AnalystHandoffRun';

const mockLiveness = liveness.threads;

const member = (key: string, status: 'approved' | 'pending' | 'rejected', threadId?: string): AnalystHandoffMember => ({
  key,
  proposalId: `p-${key}`,
  proposal: { status, question: `Brief ${key}`, workspace_name: `Workspace ${key}`, thread_id: threadId, interruptId: `i-${key}` },
});

/** The run inside the batched-liveness provider, with where the list placed
 *  each hand-off, by its key. */
function renderRun(members: AnalystHandoffMember[], places: Record<string, HandoffPlace> = {}) {
  const onApprove = vi.fn();
  renderWithProviders(
    <DispatchStatusProvider>
      <HandoffPlacesContext value={new Map(Object.entries(places))}>
        <AnalystHandoffRun members={members} onApprove={onApprove} onReject={vi.fn()} />
      </HandoffPlacesContext>
    </DispatchStatusProvider>,
  );
  return { onApprove };
}

describe('AnalystHandoffRun', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => resetThreadLifecycle());

  it('groups the handed-off runs under one header that counts where they stand', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'th-a', status: 'running', run_id: 'r-a', can_reconnect: true },
      { thread_id: 'th-b', status: 'running', run_id: 'r-b', can_reconnect: true },
      { thread_id: 'th-c', status: 'completed', run_id: null, can_reconnect: false },
    ]);
    const { onApprove } = renderRun(
      [member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b'), member('c', 'approved', 'th-c'), member('d', 'pending')],
      { 'i-c': { reportId: 'msg-c' } },
    );

    const header = screen.getByRole('button', { name: 'Handed off to 3 analysts' });
    expect(header).toHaveAttribute('aria-expanded', 'true');
    await waitFor(() => expect(header).toHaveAccessibleDescription('2 working, 1 finished'));
    expect(mockLiveness).toHaveBeenCalledWith(['th-a', 'th-b', 'th-c']);
    // Each run is a row of its own, named by its task. The finished one's
    // report waits to be linked until the other two have theirs.
    expect(screen.getByRole('button', { name: 'Brief a' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'See report' })).toBeNull();

    // The proposal still awaiting an answer stands on its own, answerable.
    fireEvent.click(screen.getByRole('button', { name: 'Hand off' }));
    expect(onApprove).toHaveBeenCalledWith(expect.objectContaining({ question: 'Brief d' }), { report_back: true }, 'p-d', 'i-d');

    fireEvent.click(header);
    expect(header).toHaveAttribute('aria-expanded', 'false');
    // The id the header controls stays in the document while folded.
    expect(document.getElementById(header.getAttribute('aria-controls')!)).not.toBeNull();
  });

  it('keeps counting and announcing its runs while folded', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'th-a', status: 'running', run_id: 'r-a', can_reconnect: true },
      { thread_id: 'th-b', status: 'running', run_id: 'r-b', can_reconnect: true },
    ]);
    renderRun([member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
    const header = screen.getByRole('button', { name: 'Handed off to 2 analysts' });
    await waitFor(() => expect(header).toHaveAccessibleDescription('2 working'));
    // A page load's first read is history, not news.
    expect(screen.getByRole('status')).toBeEmptyDOMElement();

    fireEvent.click(header);
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Brief a' })).toBeNull());
    act(() => applyFeedEvent({ type: 'run_settled', thread_id: 'th-a', run_id: 'r-a', run_seq: 1, status: 'completed' }));
    expect(screen.getByRole('status')).toHaveTextContent('1 working, 1 finished');
  });

  describe('links the reports of a group alike', () => {
    const finished = () => mockLiveness.mockResolvedValue([
      { thread_id: 'th-a', status: 'completed', run_id: null, can_reconnect: false },
      { thread_id: 'th-b', status: 'completed', run_id: null, can_reconnect: false },
    ]);
    const members = [member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')];

    it('links none while every report follows straight on', async () => {
      finished();
      renderRun(members, { 'i-a': { reportId: 'msg-a', reportFollows: true }, 'i-b': { reportId: 'msg-b', reportFollows: true } });
      await waitFor(() => expect(screen.getByRole('button', { name: 'Brief b' })).toHaveAccessibleDescription('Workspace b Finished'));
      expect(screen.queryByRole('button', { name: 'See report' })).toBeNull();
    });

    it('links all once one report does not follow', async () => {
      finished();
      renderRun(members, { 'i-a': { reportId: 'msg-a', reportFollows: true }, 'i-b': { reportId: 'msg-b', reportFollows: false } });
      await waitFor(() => expect(screen.getAllByRole('button', { name: 'See report' })).toHaveLength(2));
      expect(screen.getAllByRole('button', { name: 'See report' })[0]).toHaveAccessibleDescription('Brief a');
    });

    it('links the others when one was asked not to report back', async () => {
      // Its report will never come, so waiting on it would hold the others' links for good.
      finished();
      const quiet = { ...members[0], proposal: { ...members[0].proposal, report_back: false } };
      renderRun([quiet, members[1]], { 'i-b': { reportId: 'msg-b', reportFollows: false } });
      await waitFor(() => expect(screen.getAllByRole('button', { name: 'See report' })).toHaveLength(1));
      expect(screen.getByRole('button', { name: 'See report' })).toHaveAccessibleDescription('Brief b');
    });

    it('links the others when one could not start', async () => {
      mockLiveness.mockResolvedValue([{ thread_id: 'th-b', status: 'completed', run_id: null, can_reconnect: false }]);
      const failed = { ...members[0], proposal: { ...members[0].proposal, dispatch_failed: true } };
      renderRun([failed, members[1]], { 'i-b': { reportId: 'msg-b', reportFollows: false } });
      await waitFor(() => expect(screen.getAllByRole('button', { name: 'See report' })).toHaveLength(1));
      expect(screen.getByRole('button', { name: 'Brief a' })).toHaveAccessibleDescription(/Couldn't start$/);
    });

    it('links none while a run still has no report', async () => {
      mockLiveness.mockResolvedValue([
        { thread_id: 'th-a', status: 'running', run_id: 'r-a', can_reconnect: true },
        { thread_id: 'th-b', status: 'completed', run_id: null, can_reconnect: false },
      ]);
      renderRun(members, { 'i-b': { reportId: 'msg-b', reportFollows: false } });
      await waitFor(() => expect(screen.getByRole('button', { name: 'Brief b' })).toHaveAccessibleDescription('Workspace b Finished'));
      expect(screen.queryByRole('button', { name: 'See report' })).toBeNull();
    });
  });

  it('gives each row no live region of its own: the summary speaks for them', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'th-a', status: 'running', run_id: 'r-a', can_reconnect: true },
      { thread_id: 'th-b', status: 'completed', run_id: null, can_reconnect: false },
    ]);
    renderRun([member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Brief b' })).toHaveAccessibleDescription('Workspace b Finished'));
    expect(screen.getAllByRole('status')).toHaveLength(1);
  });

  it('reads a hand-off whose steer no run ever took in as one that could not start', async () => {
    // The run it was steered into ended without reading it, and none took it in after.
    mockLiveness.mockResolvedValue([{ thread_id: 'th-a', status: 'completed', run_id: null, can_reconnect: false }]);
    liveness.runs.mockResolvedValue([{ run_id: 'run-1', thread_id: 'th-a', status: 'completed' }]);
    liveness.steers.mockResolvedValue([{ steer_id: 'steer-1', thread_id: 'th-a', state: 'dropped', run_id: null }]);
    const steered = member('a', 'approved', 'th-a');
    renderRun([{ ...steered, proposal: { ...steered.proposal, run_id: 'run-1', steer_id: 'steer-1' } }]);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Brief a' })).toHaveAccessibleDescription(/Couldn't start$/));
    // Nothing took it in, so it was not sent into a run.
    expect(screen.queryByText('Steered')).toBeNull();
  });

  describe('answered in place', () => {
    /** The run, re-drawable with its members as an answer changes them. */
    function renderLive(members: AnalystHandoffMember[]) {
      const tree = (ms: AnalystHandoffMember[]) => (
        <DispatchStatusProvider>
          <HandoffPlacesContext value={new Map()}>
            <AnalystHandoffRun members={ms} onApprove={vi.fn()} onReject={vi.fn()} />
          </HandoffPlacesContext>
        </DispatchStatusProvider>
      );
      const { rerender } = renderWithProviders(tree(members));
      return (next: AnalystHandoffMember[]) => rerender(tree(next));
    }

    it('takes an approved hand-off into the group beside it, which grows rather than remounts', async () => {
      // Above the group, so the group's first member changes, and the group
      // keeps the key it was drawn under rather than its new first member's.
      mockLiveness.mockResolvedValue([]);
      const answer = renderLive([member('p', 'pending'), member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
      const header = screen.getByRole('button', { name: 'Handed off to 2 analysts' });

      answer([member('p', 'approved', 'th-p'), member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
      // The same header, now counting three: the group took the row in.
      expect(screen.getByRole('button', { name: 'Handed off to 3 analysts' })).toBe(header);
      expect(screen.getByRole('button', { name: 'Brief p' })).toBeInTheDocument();
      // The card folds out of the run, and cannot be answered again meanwhile.
      expect(document.querySelector('[data-handoff-approval]')!.closest('[inert]')).not.toBeNull();
      await waitFor(() => expect(document.querySelector('[data-handoff-approval]')).toBeNull());
    });

    it('hands keyboard focus from the answered card to the slip it became', () => {
      mockLiveness.mockResolvedValue([]);
      const answer = renderLive([member('p', 'pending')]);
      screen.getByRole('button', { name: 'Hand off' }).focus();

      // Approved a moment ago, it has no thread to open yet: the row holds focus.
      answer([member('p', 'approved')]);
      expect(document.activeElement).toHaveAttribute('data-handoff-member', 'p');
    });

    it('hands keyboard focus from the declined card to its line\'s toggle', () => {
      const answer = renderLive([member('p', 'pending')]);
      screen.getByRole('button', { name: 'Decline' }).focus();

      answer([member('p', 'rejected')]);
      expect(document.activeElement).toHaveTextContent('Hand-off declined');
      expect(document.activeElement).toHaveAttribute('aria-expanded', 'false');
    });

    it('leaves focus the reader took elsewhere where it is', () => {
      const answer = renderLive([member('p', 'pending'), member('q', 'pending')]);
      screen.getAllByRole('button', { name: 'Hand off' })[0].focus();
      const elsewhere = screen.getAllByRole('button', { name: 'Hand off' })[1];
      elsewhere.focus();

      answer([member('p', 'approved'), member('q', 'pending')]);
      expect(document.activeElement).toBe(elsewhere);
    });
  });

  it('keeps the order: a declined hand-off between two splits them into slips', () => {
    mockLiveness.mockResolvedValue([]);
    renderRun([member('a', 'approved', 'th-a'), member('x', 'rejected'), member('b', 'approved', 'th-b')]);

    expect(screen.queryByRole('button', { name: /Handed off to/ })).toBeNull();
    const buttons = screen.getAllByRole('button').map((b) => b.textContent ?? '');
    expect(buttons.findIndex((b) => b.includes('Brief a'))).toBeLessThan(buttons.findIndex((b) => b.includes('Hand-off declined')));
    expect(buttons.findIndex((b) => b.includes('Hand-off declined'))).toBeLessThan(buttons.findIndex((b) => b.includes('Brief b')));
  });

  describe('draws where a run stands', () => {
    const thread = (id: string, status: 'completed' | 'failed' | 'running') => ({
      thread_id: id, status, run_id: status === 'running' ? `r-${id}` : null, can_reconnect: status === 'running',
    });
    /** The line under the task, which the status leads. */
    const statusOf = (row: HTMLElement) => row.querySelector('button')!.nextElementSibling!.firstElementChild as HTMLElement;
    const rowOf = (phase: string) => document.querySelector<HTMLElement>(`[data-handoff-phase="${phase}"]`)!;

    describe('in a group', () => {
      // The header's mark stands for every row's avatar, so the avatar's place
      // holds the run's glyph, and the words beside the task have none of their own.
      it.each<[string, 'completed' | 'failed' | 'running', string]>([
        ['finished', 'completed', 'svg mask'],
        ['failed', 'failed', '.lucide-circle-alert'],
        ['working', 'running', '[role="status"]'],
      ])('puts a %s run\'s glyph in the first column, and none beside its words', async (phase, status, glyph) => {
        mockLiveness.mockResolvedValue([thread('th-a', status), thread('th-b', 'running')]);
        renderRun([member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
        await waitFor(() => expect(rowOf(phase)).not.toBeNull());

        const row = rowOf(phase);
        const column = row.firstElementChild as HTMLElement;
        expect(column.querySelector(glyph)).not.toBeNull();
        expect(column.querySelector('.rounded-full')).toBeNull();
        expect(statusOf(row).querySelector(`${glyph}, svg`)).toBeNull();
      });

      it('cuts a finished run\'s check through its disc, rather than drawing one over it', async () => {
        mockLiveness.mockResolvedValue([thread('th-a', 'completed'), thread('th-b', 'completed')]);
        renderRun([member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
        await waitFor(() => expect(screen.getAllByText('Finished', { selector: '.sr-only' })).toHaveLength(2));

        for (const row of document.querySelectorAll<HTMLElement>('[data-handoff-phase="finished"]')) {
          const column = row.firstElementChild as HTMLElement;
          const cut = column.querySelector('svg mask');
          expect(cut?.id).toBeTruthy();
          expect(column.querySelector('circle')).toHaveAttribute('mask', `url(#${cut!.id})`);
          expect(row.querySelector('.lucide-check')).toBeNull();
          expect(statusOf(row)).toHaveTextContent(/^Finished$/);
        }
      });

      it('gives each row\'s mask an id of its own', async () => {
        mockLiveness.mockResolvedValue([thread('th-a', 'completed'), thread('th-b', 'completed')]);
        renderRun([member('a', 'approved', 'th-a'), member('b', 'approved', 'th-b')]);
        await waitFor(() => expect(screen.getAllByText('Finished', { selector: '.sr-only' })).toHaveLength(2));

        const ids = [...document.querySelectorAll('[data-handoff-phase="finished"] mask')].map((m) => m.id);
        expect(new Set(ids).size).toBe(2);
      });
    });

    describe('alone', () => {
      it('keeps the analyst\'s avatar in the first column, with no run glyph in its place', async () => {
        mockLiveness.mockResolvedValue([thread('th-a', 'completed')]);
        renderRun([member('a', 'approved', 'th-a')]);
        await waitFor(() => expect(rowOf('finished')).not.toBeNull());

        const row = rowOf('finished');
        const column = row.firstElementChild as HTMLElement;
        expect(column).toHaveClass('rounded-full');
        expect(column.querySelector('svg')).toHaveAttribute('viewBox', '0 0 56 56');
        expect(row.querySelector('mask')).toBeNull();
        expect(row.querySelector('.lucide-check')).toBeNull();
      });

      it('keeps a working run\'s glyph beside its words, not in the avatar\'s place', async () => {
        mockLiveness.mockResolvedValue([thread('th-a', 'running')]);
        renderRun([member('a', 'approved', 'th-a')]);
        await waitFor(() => expect(rowOf('working')).not.toBeNull());

        const row = rowOf('working');
        expect((row.firstElementChild as HTMLElement).querySelector('[role="status"]')).toBeNull();
        expect(statusOf(row).querySelector('[role="status"]')).not.toBeNull();
        expect(statusOf(row)).toHaveTextContent('Working');
      });
    });
  });
});
