/**
 * An Analyst row's status: what its thread's run says, and for a hand-off
 * that failed with no run on its thread, failed, as the hand-off's card says.
 * An outcome the dispatch never learned keeps the thread, so a run that turns
 * up there still shows. The ledger read made for the threads' current run
 * states is the newer word; the run feed stands in while it is in flight.
 */
import { afterEach, describe, it, expect, vi } from 'vitest';
import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { applyFeedEvent, applySnapshot, resetThreadLifecycle } from '@/lib/threadLifecycle/store';
import type { DispatchLiveness } from '../../../utils/api';
import type { AnalystCandidate } from '../delegatedWork';
import { useAnalystRoster } from '../useAnalystRoster';

const liveness = vi.hoisted(() => vi.fn<(ids: string[]) => Promise<DispatchLiveness[]>>());
vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHandoffLiveness: async ({ threads }: { threads: string[] }) => ({ liveness: await liveness(threads), runs: [], steers: [] }),
}));

const failedHandOff: AnalystCandidate = {
  threadId: 'thread-semis', workspaceId: 'ws-semis', workspaceName: 'Semis', title: 'Size the market',
  seenMs: 1_700_000_000_000, owed: false, dispatchFailed: true,
};

const handOff: AnalystCandidate = { ...failedHandOff, dispatchFailed: false };

function row(status: string, lastActivityMs?: number): DispatchLiveness[] {
  const last_activity_at = lastActivityMs === undefined ? null : new Date(lastActivityMs).toISOString();
  return [{ thread_id: 'thread-semis', status, run_id: null, can_reconnect: false, last_activity_at }];
}

function readRoster(status: string, candidate = failedHandOff, turnStarts: readonly number[] = []) {
  liveness.mockResolvedValue(row(status));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  return renderHook(() => useAnalystRoster([candidate], turnStarts), { wrapper });
}

function feed(type: 'run_started' | 'run_settled', seq: number) {
  const status = type === 'run_started' ? 'running' : 'completed';
  applyFeedEvent({ type, thread_id: 'thread-semis', run_id: `run-${seq}`, run_seq: seq, status });
}

afterEach(() => {
  resetThreadLifecycle();
  liveness.mockReset();
});

describe('useAnalystRoster — a hand-off that failed to dispatch', () => {
  it('shows it failed while no run turns up on its thread', async () => {
    const { result } = readRoster('idle');
    await waitFor(() => expect(liveness).toHaveBeenCalled());
    expect(result.current).toEqual([expect.objectContaining({ threadId: 'thread-semis', status: 'failed', running: false })]);
  });

  it('follows a run that turns up there after all', async () => {
    const { result } = readRoster('running');
    await waitFor(() => expect(result.current[0]?.status).toBe('running'));
    expect(result.current[0]?.running).toBe(true);
  });
});

describe('useAnalystRoster — an Analyst waiting on the user', () => {
  it('stays past five turns without reading as running', async () => {
    const turnStarts = [1, 2, 3, 4, 5, 6].map((n) => handOff.seenMs! + n * 60_000);
    const { result } = readRoster('interrupted', handOff, turnStarts);
    await waitFor(() => expect(result.current[0]?.status).toBe('needs_input'));
    expect(result.current[0]?.running).toBe(false);
  });

  it('reads as waiting, not running, while it still owes its report', async () => {
    const { result } = readRoster('interrupted', { ...handOff, owed: true });
    await waitFor(() => expect(result.current[0]?.status).toBe('needs_input'));
    expect(result.current[0]?.running).toBe(false);
  });
});

describe('useAnalystRoster — the row\'s name', () => {
  it('keeps the thread\'s title once it no longer owes a report', async () => {
    liveness.mockResolvedValue([{ ...row('completed')[0], title: 'Semis market sizing' }]);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useAnalystRoster([handOff], []), { wrapper });
    await waitFor(() => expect(result.current[0]?.title).toBe('Semis market sizing'));
  });

  it('names its workspace as it is called now', async () => {
    liveness.mockResolvedValue([{ ...row('completed')[0], workspace_name: 'Semiconductors' }]);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useAnalystRoster([handOff], []), { wrapper });
    await waitFor(() => expect(result.current[0]?.workspaceName).toBe('Semiconductors'));
  });

  it('falls back to the question it was handed until the thread is named', async () => {
    const { result } = readRoster('running', handOff);
    await waitFor(() => expect(result.current[0]?.status).toBe('running'));
    expect(result.current[0]?.title).toBe('Size the market');
  });
});

describe('useAnalystRoster — the feed against the ledger', () => {
  it('reads the ledger again once a snapshot shows a run whose settle never arrived is over', async () => {
    feed('run_started', 3);
    const { result } = readRoster('running', handOff);
    await waitFor(() => expect(result.current[0]?.status).toBe('running'));

    liveness.mockResolvedValue(row('completed'));
    act(() => applySnapshot({ as_of_seq: 5, oldest_included_unseen_seq: 0, live: [], unseen: [] }));

    await waitFor(() => expect(result.current[0]?.status).toBe('completed'));
    expect(liveness).toHaveBeenCalledTimes(2);
    expect(result.current[0]?.running).toBe(false);
  });

  it('follows a new run the ledger has before the feed does', async () => {
    feed('run_settled', 3);
    const { result } = readRoster('running', handOff);
    await waitFor(() => expect(result.current[0]?.status).toBe('running'));
    expect(result.current[0]?.running).toBe(true);
  });

  it('keeps an Analyst that just settled until the ledger says when', async () => {
    const started = 1_700_000_000_000;
    const turnStarts = [1, 2, 3, 4, 5].map((n) => started + n * 60_000);
    feed('run_started', 3);
    liveness.mockResolvedValue(row('running', started));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useAnalystRoster([{ ...handOff, seenMs: started }], turnStarts), { wrapper });
    await waitFor(() => expect(result.current[0]?.running).toBe(true));

    let land!: (rows: DispatchLiveness[]) => void;
    liveness.mockReturnValue(new Promise((resolve) => { land = resolve; }));
    act(() => feed('run_settled', 3));

    await waitFor(() => expect(liveness).toHaveBeenCalledTimes(2));
    expect(result.current).toEqual([expect.objectContaining({ status: 'completed', running: false })]);
    await act(async () => land(row('completed', started + 6 * 60_000)));
    expect(result.current).toEqual([expect.objectContaining({ status: 'completed', running: false })]);
  });
});
