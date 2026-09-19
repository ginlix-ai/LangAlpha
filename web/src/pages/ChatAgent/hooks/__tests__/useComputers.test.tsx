/**
 * The status fan-out: one subscription per machine, not per workspace.
 *
 * Workspace rows here are the live ones from the wt3 stack, where four
 * projects share computer 5319ad0e, the arrangement that makes subscribing
 * per workspace four connections for one piece of news.
 *
 * The hook takes no arguments: it reads both watch sets from the caches, which
 * is what lets it mount once above every surface that can start a machine. So
 * every case here seeds the workspace list cache instead of passing rows.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { waitFor } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';

vi.mock('../../utils/api', () => ({
  getComputers: vi.fn(),
  streamComputerEvents: vi.fn(async () => {}),
  streamWorkspaceEvents: vi.fn(async () => {}),
}));

import { getComputers, streamComputerEvents, streamWorkspaceEvents } from '../../utils/api';
import { useComputerStatusFanout } from '../useComputers';

const mockGetComputers = getComputers as Mock;
const mockStreamComputer = streamComputerEvents as Mock;
const mockStreamWorkspace = streamWorkspaceEvents as Mock;

const COMPUTER_ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const LIST_KEY = queryKeys.workspaces.list({ limit: 20 });

// Four of the live rows: same machine, same sandbox_id, distinct dir_name.
const WORKSPACES: Workspace[] = [
  { workspace_id: 'ws-gamma', name: 'Gamma Live Join', status: 'running', computer_id: COMPUTER_ID, dir_name: 'gamma-live-join-514c' },
  { workspace_id: 'ws-alpha', name: 'Alpha Research', status: 'running', computer_id: COMPUTER_ID, dir_name: 'alpha-research-b8ad' },
  { workspace_id: 'ws-beta', name: 'Beta Models', status: 'running', computer_id: COMPUTER_ID, dir_name: 'beta-models-2ddd' },
  { workspace_id: 'ws-flash', name: 'Flash', status: 'flash' },
];

function computer(status: string) {
  return {
    computers: [
      {
        computer_id: COMPUTER_ID,
        user_id: 'wp13-final-1789525377',
        kind: 'daytona',
        name: 'Alpha Research',
        status,
        resource_tier: 'standard',
        is_always_on: false,
        is_primary: true,
        root_dir: '/home/workspace',
      },
    ],
    total: 1,
  };
}

/**
 * gcTime has to outlive the assertions: an observer-less entry written by
 * setQueryData is collected the moment it is written at gcTime 0.
 */
function mount(workspaces: Workspace[] = WORKSPACES) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity } },
  });
  queryClient.setQueryData(LIST_KEY, { workspaces, total: workspaces.length });
  return renderHookWithProviders(() => useComputerStatusFanout(), { queryClient });
}

beforeEach(() => {
  vi.clearAllMocks();
  mockStreamComputer.mockImplementation(async () => {});
  mockStreamWorkspace.mockImplementation(async () => {});
});

describe('useComputerStatusFanout', () => {
  it('opens ONE stream for a machine three workspaces share', async () => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    mount();

    await waitFor(() => expect(mockStreamComputer).toHaveBeenCalledTimes(1));
    expect(mockStreamComputer.mock.calls[0][0]).toBe(COMPUTER_ID);
  });

  it('fans one transition out to every workspace on the machine', async () => {
    mockGetComputers.mockResolvedValue(computer('stopping'));
    let emit: ((status: string) => void) | null = null;
    mockStreamComputer.mockImplementation(async (_id: string, onStatus: (s: string) => void) => {
      emit = onStatus;
      await new Promise(() => {}); // stay open, like a live stream
    });

    const { queryClient } = mount();

    await waitFor(() => expect(emit).not.toBeNull());
    emit!('stopped');

    const list = queryClient.getQueryData<{ workspaces: Workspace[] }>(LIST_KEY);
    expect(list?.workspaces.map((w) => w.status)).toEqual([
      'stopped', 'stopped', 'stopped',
      'flash', // no computer_id, so the machine does not speak for it
    ]);
    // And the machine's own cache entry moved with it.
    const computers = queryClient.getQueryData<{ computers: Array<{ status: string }> }>(
      queryKeys.computers.lists(),
    );
    expect(computers?.computers[0].status).toBe('stopped');
  });

  it('holds no stream open for a machine at rest', async () => {
    mockGetComputers.mockResolvedValue(computer('stopped'));
    mount();
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamComputer).not.toHaveBeenCalled();
  });

  it('holds no stream open for a machine that is already running', async () => {
    mockGetComputers.mockResolvedValue(computer('running'));
    mount();
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamComputer).not.toHaveBeenCalled();
  });

  it('falls back to the workspace channel for an in-flight row that names no machine', async () => {
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    mount([
      { workspace_id: 'ws-legacy', name: 'Legacy', status: 'starting' },
      { workspace_id: 'ws-flash', name: 'Flash', status: 'flash' },
    ]);

    await waitFor(() => expect(mockStreamWorkspace).toHaveBeenCalledTimes(1));
    // Only the unbound row that is actually moving.
    expect(mockStreamWorkspace.mock.calls[0][0]).toBe('ws-legacy');
    expect(mockStreamComputer).not.toHaveBeenCalled();
  });

  it('holds no stream open for an unbound row at rest, or one with no status', async () => {
    // A page of rows the 044 backfill has not bound yet would otherwise spend
    // the origin's six HTTP/1.1 connections waiting for news nobody will send.
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    mount([
      { workspace_id: 'ws-stopped', name: 'Stopped', status: 'stopped' },
      { workspace_id: 'ws-unknown', name: 'No status' } as Workspace,
    ]);

    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamWorkspace).not.toHaveBeenCalled();
  });

  it('arms the unbound watch from a cache write, with no list in its props', async () => {
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    const { queryClient } = mount([
      { workspace_id: 'ws-legacy', name: 'Legacy', status: 'stopped' },
    ]);
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamWorkspace).not.toHaveBeenCalled();

    // What an action does: reflect its own 202 into the cache.
    queryClient.setQueryData(LIST_KEY, {
      workspaces: [{ workspace_id: 'ws-legacy', name: 'Legacy', status: 'starting' }],
      total: 1,
    });

    await waitFor(() => expect(mockStreamWorkspace).toHaveBeenCalledTimes(1));
    expect(mockStreamWorkspace.mock.calls[0][0]).toBe('ws-legacy');
  });

  it('watches a machine that holds no workspaces yet', async () => {
    // The row a user just pressed Start on in the computers list. Nothing to
    // fan out to, and it is still the row that has to stop saying 'Starting'.
    mockGetComputers.mockResolvedValue(computer('starting'));
    mount([]);

    await waitFor(() => expect(mockStreamComputer).toHaveBeenCalledTimes(1));
    expect(mockStreamComputer.mock.calls[0][0]).toBe(COMPUTER_ID);
  });

  it('reconciles when a stream closes without having reported a terminal status', async () => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    // Closes straight away, having said nothing: the server's 600 s cap or a
    // dropped link. The transition may have finished without us.
    mockStreamComputer.mockImplementation(async () => {});
    const { queryClient } = mount();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await waitFor(() => expect(invalidate).toHaveBeenCalled());
    const keys = invalidate.mock.calls.map((c) => JSON.stringify(c[0]?.queryKey));
    expect(keys).toContain(JSON.stringify(queryKeys.computers.lists()));
    expect(keys).toContain(JSON.stringify(queryKeys.workspaces.lists()));
  });

  it('listens again when the stream dropped and the machine is still moving', async () => {
    // One dropped link during a start used to end the watch for good: the
    // refetch still said 'starting', the watch key did not change, and the
    // row stayed on "Starting" until something unrelated refetched.
    mockGetComputers.mockResolvedValue(computer('starting'));
    mockStreamComputer.mockImplementation(async () => {}); // drops at once, says nothing
    mount();

    await waitFor(
      () => expect(mockStreamComputer.mock.calls.length).toBeGreaterThanOrEqual(2),
      { timeout: 4000 },
    );
    expect(mockStreamComputer.mock.calls.every((c) => c[0] === COMPUTER_ID)).toBe(true);
  });

  it('stops listening once the reconcile finds the machine at rest', async () => {
    mockGetComputers
      .mockResolvedValueOnce(computer('starting'))
      .mockResolvedValue(computer('running'));
    mockStreamComputer.mockImplementation(async () => {});
    mount();

    await waitFor(() => expect(mockGetComputers.mock.calls.length).toBeGreaterThanOrEqual(2));
    // Give a would-be second subscription time to appear; it must not.
    await new Promise((r) => setTimeout(r, 800));
    expect(mockStreamComputer).toHaveBeenCalledTimes(1);
  });

  it('does not reconcile when the close followed a terminal status', async () => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    mockStreamComputer.mockImplementation(async (_id: string, onStatus: (s: string) => void) => {
      onStatus('running');
    });
    const { queryClient } = mount();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await waitFor(() => {
      const computers = queryClient.getQueryData<{ computers: Array<{ status: string }> }>(
        queryKeys.computers.lists(),
      );
      expect(computers?.computers[0].status).toBe('running');
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(invalidate).not.toHaveBeenCalled();
  });

  it('aborts its streams on unmount', async () => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    let signal: AbortSignal | null = null;
    mockStreamComputer.mockImplementation(async (_id: string, _cb: unknown, sig: AbortSignal) => {
      signal = sig;
      await new Promise(() => {});
    });

    const { unmount } = mount();
    await waitFor(() => expect(signal).not.toBeNull());
    expect(signal!.aborted).toBe(false);
    unmount();
    expect(signal!.aborted).toBe(true);
  });
});
