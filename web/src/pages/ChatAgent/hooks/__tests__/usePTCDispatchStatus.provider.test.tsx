/**
 * DispatchStatusProvider — the turn-level batched dispatch-status reader.
 *
 * Many hand-offs in a Chief of Staff turn register their thread id with one provider;
 * the provider runs a SINGLE getHandoffLiveness query over one sorted ask,
 * distributes each id's status slice, and stops polling once every run is
 * terminal. These tests mock getHandoffLiveness so the batching + distribution
 * + cadence wiring is exercised without a backend.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { applyFeedEvent, resetThreadLifecycle } from '@/lib/threadLifecycle/store';

const liveness = vi.hoisted(() => ({ threads: vi.fn(), runs: vi.fn(), steers: vi.fn() }));
vi.mock('../../utils/api', () => ({
  // One batched read, answered per thread, per run and per steer by the mocks.
  getHandoffLiveness: vi.fn(async ({ threads, runs, steers }: { threads: string[]; runs: string[]; steers: string[] }) => ({
    liveness: (await liveness.threads(threads)) ?? [],
    runs: runs.length ? ((await liveness.runs(runs)) ?? []) : [],
    steers: steers.length ? ((await liveness.steers(steers)) ?? []) : [],
  })),
}));

import { getHandoffLiveness } from '../../utils/api';
import { DispatchStatusProvider, useDispatchStatuses, type DispatchPin } from '../usePTCDispatchStatus';

const mockLiveness = liveness.threads;
const mockRuns = liveness.runs;
const mockSteers = liveness.steers;

const THREAD: DispatchPin = { kind: 'thread' };

/** Minimal consumer that surfaces a single thread's resolved dispatch status.
 *  `tag` disambiguates the testid when two consumers watch the SAME thread. */
function Probe({ id, tag }: { id: string; tag?: string }) {
  const [{ status }] = useDispatchStatuses([{ threadId: id, pin: THREAD }]);
  return <div data-testid={`probe-${tag ?? id}`}>{status}</div>;
}

/** A consumer that also surfaces the run's times, as `status|start|finish`. */
function TimedProbe({ id }: { id: string }) {
  const [{ status, startedAt, finishedAt }] = useDispatchStatuses([{ threadId: id, pin: THREAD }]);
  return <div data-testid={`timed-${id}`}>{`${status}|${startedAt ?? '-'}|${finishedAt ?? '-'}`}</div>;
}

/** A hand-off pinned to its run (or to a time), as `status|start|finish`. */
function PinnedProbe({ id, pin }: { id: string; pin: DispatchPin }) {
  const [{ status, startedAt, finishedAt }] = useDispatchStatuses([{ threadId: id, pin }]);
  return <div data-testid={`pinned-${id}`}>{`${status}|${startedAt ?? '-'}|${finishedAt ?? '-'}`}</div>;
}

const STARTED = '2026-10-21T09:00:00.000Z';
const FINISHED = '2026-10-21T09:18:03.500Z';
const STARTED_MS = Date.parse(STARTED);
const FINISHED_MS = Date.parse(FINISHED);

function livenessRow(status: string, finished: string | null = null): Record<string, unknown> {
  const live = status === 'running';
  return {
    thread_id: 'thread-a', status, run_id: live ? 'run-a' : null, can_reconnect: live,
    run_started_at: STARTED, run_finished_at: finished,
  };
}

function push(type: 'run_started' | 'run_settled', status: string, seq = 1) {
  act(() => applyFeedEvent({ type, thread_id: 'thread-a', run_id: `run-${seq}`, run_seq: seq, status }));
}

/** A rejection shaped like the API client's, which carries the response. */
function httpError(status: number) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status },
  });
}

/** The app's query defaults, one retry for a failed read. The retry is made
 *  at once, so a failed poll is two calls a millisecond apart, and the checks
 *  below land between polls rather than on one. */
function appQueryClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: 1, retryDelay: 0, gcTime: 0 } } });
}

/** A turn with one card for thread-a, plus an optional second card on the SAME
 *  thread — the shape a resumed dispatch takes (same id set, same query key). */
function turn(resumed: boolean) {
  return (
    <DispatchStatusProvider>
      <Probe id="thread-a" />
      {resumed && <Probe id="thread-a" tag="resume" />}
    </DispatchStatusProvider>
  );
}

describe('DispatchStatusProvider', () => {
  beforeEach(() => vi.clearAllMocks());

  it('runs ONE batched query for all registered ids and distributes each status', async () => {
    mockLiveness.mockResolvedValue([
      { thread_id: 'thread-a', status: 'active', run_id: 'run-a', can_reconnect: true },
      { thread_id: 'thread-b', status: 'completed', run_id: null, can_reconnect: false },
    ]);

    renderWithProviders(
      <DispatchStatusProvider>
        <Probe id="thread-b" />
        <Probe id="thread-a" />
      </DispatchStatusProvider>,
    );

    await waitFor(() => expect(screen.getByTestId('probe-thread-a')).toHaveTextContent('running'));
    // One request for the whole turn, carrying BOTH ids (sorted) — no per-card
    // fan-out, no second timer.
    expect(mockLiveness).toHaveBeenCalledTimes(1);
    expect(mockLiveness).toHaveBeenCalledWith(['thread-a', 'thread-b']);
    // Each card reads its own slice from the shared result.
    expect(screen.getByTestId('probe-thread-b')).toHaveTextContent('completed');
  });

  it('stops polling once every registered run is terminal', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'completed', run_id: null, can_reconnect: false },
        { thread_id: 'thread-b', status: 'failed', run_id: null, can_reconnect: false },
      ]);
      renderWithProviders(
        <DispatchStatusProvider>
          <Probe id="thread-a" />
          <Probe id="thread-b" />
        </DispatchStatusProvider>,
      );
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
      // All terminal → refetchInterval resolves to false; advancing well past
      // both the fast and steady cadences fires no further requests.
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('wakes the dormant query when a consumer re-registers a terminal thread (resumed dispatch)', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'completed', run_id: null, can_reconnect: false },
      ]);
      const { rerender } = renderWithProviders(turn(false));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
      expect(screen.getByTestId('probe-thread-a')).toHaveTextContent('completed');
      // Terminal → the shared query goes dormant.
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);

      // The user resumes a dispatch onto the SAME finished thread: the server
      // is active again and a second card registers the same id — same id set,
      // same query key, so only the wake invalidation can refetch.
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'active', run_id: 'run-a2', can_reconnect: true },
      ]);
      rerender(turn(true));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(2);
      // Both cards flip off the stale terminal slice.
      expect(screen.getByTestId('probe-thread-a')).toHaveTextContent('running');
      expect(screen.getByTestId('probe-resume')).toHaveTextContent('running');
      // Cadence is re-armed: the next poll fires on the fast interval.
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not add a fetch when a consumer re-registers a still-live thread', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'active', run_id: 'run-a', can_reconnect: true },
      ]);
      const { rerender } = renderWithProviders(turn(false));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
      // A second card on a live thread is a plain refcount bump — the wake is
      // terminal-gated, so no extra immediate fetch.
      rerender(turn(true));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
      // Polling simply continues on the existing cadence.
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('grants the full starting-cap budget again after a wake, with fast cadence', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'active', run_id: 'run-a', can_reconnect: true },
      ]);
      const { rerender } = renderWithProviders(turn(false));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      // Accumulate lifetime fetches well past STARTING_POLL_CAP (30) while live.
      for (let i = 0; i < 40; i++) {
        await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
      }
      expect(mockLiveness.mock.calls.length).toBeGreaterThan(30);
      // Run finishes → dormant.
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'completed', run_id: null, can_reconnect: false },
      ]);
      await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
      const afterComplete = mockLiveness.mock.calls.length;
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness.mock.calls.length).toBe(afterComplete);

      // Resume: the new run hasn't registered yet, so liveness omits the id
      // and the slice maps to 'starting'.
      mockLiveness.mockResolvedValue([]);
      rerender(turn(true));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      const afterWake = mockLiveness.mock.calls.length;
      expect(afterWake).toBe(afterComplete + 1);
      // With lifetime-cumulative counters the starting cap would trip
      // instantly (lifetime polls >= 30) and the cadence would already be
      // steady (10s). The window-scoped counters grant the full budget again
      // AND re-apply the fast 4s cadence relative to the wake.
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      expect(mockLiveness.mock.calls.length).toBe(afterWake + 2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('backs off and then gives up while the liveness read keeps failing', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockRejectedValue(httpError(503));
      renderWithProviders(turn(false), { queryClient: appQueryClient() });
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      // A failed read is still a finished poll: the fast window is five polls.
      await act(async () => { await vi.advanceTimersByTimeAsync(18_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(2 * 5);
      // Then the steady cadence, not 4s forever.
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(2 * 11);
      // Never read once, the dispatch shows 'starting' on every round, so the
      // starting cap ends the watch.
      await act(async () => { await vi.advanceTimersByTimeAsync(10 * 60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(2 * 30);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps a live run on the steady cadence through a failing read', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'active', run_id: 'run-a', can_reconnect: true },
      ]);
      renderWithProviders(turn(false), { queryClient: appQueryClient() });
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      mockLiveness.mockRejectedValue(httpError(401));
      await act(async () => { await vi.advanceTimersByTimeAsync(18_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(1 + 2 * 4);
      // The last answer still says running, so a 401 (a session that can come
      // back) neither stops the poll nor keeps it fast.
      await act(async () => { await vi.advanceTimersByTimeAsync(10 * 60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(1 + 2 * 64);
      expect(screen.getByTestId('probe-thread-a')).toHaveTextContent('running');
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops polling on a refusal that asking again will not change', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockRejectedValue(httpError(403));
      renderWithProviders(turn(false), { queryClient: appQueryClient() });
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps polling while a run is still live', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([
        { thread_id: 'thread-a', status: 'active', run_id: 'run-a', can_reconnect: true },
      ]);
      renderWithProviders(
        <DispatchStatusProvider>
          <Probe id="thread-a" />
        </DispatchStatusProvider>,
      );
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
      // A live run polls again on the fast cadence (4s).
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('DispatchStatusProvider: when the run started and settled', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => resetThreadLifecycle());

  it('gives a settled run its start and finish', async () => {
    mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
    renderWithProviders(<DispatchStatusProvider><TimedProbe id="thread-a" /></DispatchStatusProvider>);
    await waitFor(() =>
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`));
  });

  it('gives a live run its start and no finish', async () => {
    mockLiveness.mockResolvedValue([livenessRow('running')]);
    renderWithProviders(<DispatchStatusProvider><TimedProbe id="thread-a" /></DispatchStatusProvider>);
    await waitFor(() =>
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`running|${STARTED_MS}|-`));
  });

  it('reads at once when a push settles the run, then stops', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([livenessRow('running')]);
      renderWithProviders(<DispatchStatusProvider><TimedProbe id="thread-a" /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);

      mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
      push('run_settled', 'completed');
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      // Not on the next poll's tick: the push asked for it.
      expect(mockLiveness).toHaveBeenCalledTimes(2);
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`);

      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('leaves a read that still says live to the poll, which stops once it settles', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([livenessRow('running')]);
      renderWithProviders(<DispatchStatusProvider><TimedProbe id="thread-a" /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });

      // The push outran the ledger: the read it asks for is still live.
      push('run_settled', 'completed');
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockLiveness).toHaveBeenCalledTimes(2);
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|-`);

      // No read of its own beyond that one: the poll's next tick reads it.
      mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
      // The poll's tick, then the beat the query's notify takes.
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      await act(async () => { await vi.advanceTimersByTimeAsync(10); });
      expect(mockLiveness).toHaveBeenCalledTimes(3);
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`);

      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not read again for a push the first read already answers', async () => {
    vi.useFakeTimers();
    try {
      push('run_settled', 'completed');
      mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
      renderWithProviders(<DispatchStatusProvider><TimedProbe id="thread-a" /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      // One read, and never one without the thread in it.
      expect(mockLiveness).toHaveBeenCalledTimes(1);
      expect(mockLiveness).toHaveBeenCalledWith(['thread-a']);
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`);
    } finally {
      vi.useRealTimers();
    }
  });

  it('drops the times of a settled run once a push says a new one started', async () => {
    mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
    renderWithProviders(<DispatchStatusProvider><TimedProbe id="thread-a" /></DispatchStatusProvider>);
    await waitFor(() =>
      expect(screen.getByTestId('timed-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`));

    push('run_started', 'running', 2);
    expect(screen.getByTestId('timed-thread-a')).toHaveTextContent('running|-|-');
  });
});

describe('DispatchStatusProvider: a hand-off pinned to its run', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => resetThreadLifecycle());

  const runRow = (status: string, finished: string | null = null) => ({
    run_id: 'run-1', thread_id: 'thread-a', status, run_started_at: STARTED, run_finished_at: finished,
  });
  const LATER = '2026-10-21T10:00:00.000Z';
  const RUN_1: DispatchPin = { kind: 'run', runId: 'run-1' };

  it('reads the pinned runs in the same request as the threads', async () => {
    mockLiveness.mockResolvedValue([livenessRow('running')]);
    mockRuns.mockResolvedValue([runRow('running')]);
    renderWithProviders(
      <DispatchStatusProvider>
        <PinnedProbe id="thread-a" pin={RUN_1} />
        <Probe id="thread-b" />
      </DispatchStatusProvider>,
    );
    await waitFor(() => expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${STARTED_MS}|-`));
    expect(getHandoffLiveness).toHaveBeenCalledTimes(1);
    expect(getHandoffLiveness).toHaveBeenCalledWith(expect.objectContaining({ threads: ['thread-a', 'thread-b'], runs: ['run-1'] }));
  });

  it('reads its own run once it is over, whatever the thread runs next, and stops polling', async () => {
    vi.useFakeTimers();
    try {
      // The thread's latest run is a later turn, still going.
      mockLiveness.mockResolvedValue([{ ...livenessRow('running'), run_started_at: LATER }]);
      mockRuns.mockResolvedValue([runRow('completed', FINISHED)]);
      renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={RUN_1} /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`);

      // Neither a push for the thread's new run nor time moves it.
      push('run_started', 'running', 2);
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`);
      expect(mockLiveness).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('takes a push while its run is live, timed from that run, then reads the finish', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([livenessRow('running')]);
      mockRuns.mockResolvedValue([runRow('running')]);
      renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={RUN_1} /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${STARTED_MS}|-`);

      mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
      mockRuns.mockResolvedValue([runRow('completed', FINISHED)]);
      push('run_settled', 'completed');
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`);
      expect(mockLiveness).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('follows the thread once its run stopped to ask, since the answer is a new run', async () => {
    mockLiveness.mockResolvedValue([{ ...livenessRow('completed', FINISHED) }]);
    mockRuns.mockResolvedValue([runRow('interrupted')]);
    renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={RUN_1} /></DispatchStatusProvider>);
    await waitFor(() => expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`));
  });

  it('holds the thread\'s earlier read away from a run not asked about yet', async () => {
    mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
    let answer: (rows: unknown[]) => void = () => {};
    mockRuns.mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const { rerender } = renderWithProviders(<DispatchStatusProvider><Probe id="thread-a" /></DispatchStatusProvider>);
    await waitFor(() => expect(screen.getByTestId('probe-thread-a')).toHaveTextContent('completed'));

    // A new hand-off to the same thread: the read on hand is of its last run.
    mockLiveness.mockResolvedValue([{ ...livenessRow('running'), run_started_at: LATER }]);
    rerender(<DispatchStatusProvider><Probe id="thread-a" /><PinnedProbe id="thread-a" pin={RUN_1} /></DispatchStatusProvider>);
    expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent('starting|-|-');

    await act(async () => { answer([{ ...runRow('running'), run_started_at: LATER }]); });
    await waitFor(() => expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${Date.parse(LATER)}|-`));
  });

  it('with `since`, follows a run still live then, which a steered hand-off joined', async () => {
    mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
    renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={{ kind: 'thread', since: STARTED_MS + 1 }} /></DispatchStatusProvider>);
    await waitFor(() => expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`));
  });

  it('with `since`, takes no run already over by then, and keeps polling', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
      renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={{ kind: 'thread', since: FINISHED_MS + 1 }} /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent('starting|-|-');

      // The run the dispatch may have started turns up on the next poll.
      mockLiveness.mockResolvedValue([{ ...livenessRow('running'), run_started_at: LATER }]);
      await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
      await act(async () => { await vi.advanceTimersByTimeAsync(10); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${Date.parse(LATER)}|-`);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('DispatchStatusProvider: a hand-off steered into a run already going', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => resetThreadLifecycle());

  const LATER = '2026-10-21T10:00:00.000Z';
  const LATER_MS = Date.parse(LATER);
  const STEERED: DispatchPin = { kind: 'steer', steerId: 'steer-1', steeredInto: 'run-1' };
  const steerRow = (state: string, runId: string | null = null) =>
    ({ steer_id: 'steer-1', thread_id: 'thread-a', state, run_id: runId });
  const runView = (runId: string, status: string, started: string, finished: string | null = null) =>
    ({ run_id: runId, thread_id: 'thread-a', status, run_started_at: started, run_finished_at: finished });
  const laterTurn = () => [{ ...livenessRow('running'), run_started_at: LATER }];

  beforeEach(() => {
    // The run it was steered into ended without reading it.
    mockRuns.mockResolvedValue([runView('run-1', 'completed', STARTED, FINISHED)]);
  });

  it('follows the run that took its steer in, which the read carries unasked', async () => {
    mockLiveness.mockResolvedValue(laterTurn());
    mockRuns.mockResolvedValue([runView('run-1', 'completed', STARTED, FINISHED), runView('run-2', 'running', LATER)]);
    mockSteers.mockResolvedValue([steerRow('taken', 'run-2')]);
    renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={STEERED} /></DispatchStatusProvider>);
    await waitFor(() => expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${LATER_MS}|-`));
    // Asked for its steer and the run it went into, never the run that took it.
    expect(vi.mocked(getHandoffLiveness).mock.calls.map(([ask]) => [ask.runs, ask.steers])).toEqual([[['run-1'], ['steer-1']]]);
  });

  it('reads a queued steer as working while the run it went into is live, starting once that is over', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue([livenessRow('running')]);
      mockRuns.mockResolvedValue([runView('run-1', 'running', STARTED)]);
      mockSteers.mockResolvedValue([steerRow('queued')]);
      renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={STEERED} /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${STARTED_MS}|-`);

      // That run ends without reading it: not the hand-off's end.
      mockLiveness.mockResolvedValue([livenessRow('completed', FINISHED)]);
      mockRuns.mockResolvedValue([runView('run-1', 'completed', STARTED, FINISHED)]);
      await act(async () => { await vi.advanceTimersByTimeAsync(4_010); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent('starting|-|-');

      // The poll goes on, and finds the run that takes it in.
      mockLiveness.mockResolvedValue(laterTurn());
      mockRuns.mockResolvedValue([runView('run-1', 'completed', STARTED, FINISHED), runView('run-2', 'running', LATER)]);
      mockSteers.mockResolvedValue([steerRow('taken', 'run-2')]);
      await act(async () => { await vi.advanceTimersByTimeAsync(4_010); });
      expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`running|${LATER_MS}|-`);
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops polling once its steer was dropped, while the thread runs on', async () => {
    vi.useFakeTimers();
    try {
      mockLiveness.mockResolvedValue(laterTurn());
      mockSteers.mockResolvedValue([steerRow('dropped')]);
      renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={STEERED} /></DispatchStatusProvider>);
      await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
      expect(mockLiveness).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('falls back to the run it was steered into when the read does not know the steer', async () => {
    mockLiveness.mockResolvedValue(laterTurn());
    mockSteers.mockResolvedValue([]);
    renderWithProviders(<DispatchStatusProvider><PinnedProbe id="thread-a" pin={STEERED} /></DispatchStatusProvider>);
    await waitFor(() => expect(screen.getByTestId('pinned-thread-a')).toHaveTextContent(`completed|${STARTED_MS}|${FINISHED_MS}`));
    // From the one read: the run was asked for beside the steer.
    expect(getHandoffLiveness).toHaveBeenCalledTimes(1);
  });
});
