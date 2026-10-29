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
import { act, waitFor } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';
import type { StatusFrame } from '../../utils/api/statusStream';

vi.mock('../../utils/api', () => ({
  getComputers: vi.fn(),
  streamComputerEvents: vi.fn(async () => {}),
  streamWorkspaceEvents: vi.fn(async () => {}),
}));

import { getComputers, streamComputerEvents, streamWorkspaceEvents } from '../../utils/api';
import { removeComputerRow, useComputerStatusFanout } from '../useComputers';
import { usePrepareStage } from '../computerPreparation';

const mockGetComputers = getComputers as Mock;
type ComputerOnFrame = (frame: StatusFrame) => void;
const mockStreamComputer = streamComputerEvents as Mock;
const mockStreamWorkspace = streamWorkspaceEvents as Mock;

const COMPUTER_ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const LIST_KEY = queryKeys.workspaces.list({ limit: 20 });

// Four of the live rows: same machine, same sandbox_id, distinct dir_name.
const WORKSPACES: Workspace[] = [
  { workspace_id: 'ws-gamma', name: 'Gamma Live Join', is_home: false, status: 'running', computer_id: COMPUTER_ID, dir_name: 'gamma-live-join-514c' },
  { workspace_id: 'ws-alpha', name: 'Alpha Research', is_home: false, status: 'running', computer_id: COMPUTER_ID, dir_name: 'alpha-research-b8ad' },
  { workspace_id: 'ws-beta', name: 'Beta Models', is_home: false, status: 'running', computer_id: COMPUTER_ID, dir_name: 'beta-models-2ddd' },
  { workspace_id: 'ws-home', name: 'Home', is_home: true, status: 'unbound' },
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
  runAuthResets();
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
    let emit: ComputerOnFrame | null = null;
    mockStreamComputer.mockImplementation(async (_id: string, onFrame: ComputerOnFrame) => {
      emit = onFrame;
      await new Promise(() => {}); // stay open, like a live stream
    });

    const { queryClient } = mount();

    await waitFor(() => expect(emit).not.toBeNull());
    emit!({ status: 'stopped' });

    const list = queryClient.getQueryData<{ workspaces: Workspace[] }>(LIST_KEY);
    expect(list?.workspaces.map((w) => w.status)).toEqual([
      'stopped', 'stopped', 'stopped',
      'unbound', // Home names no computer_id, so the machine does not speak for it
    ]);
    // And the machine's own cache entry moved with it.
    const computers = queryClient.getQueryData<{ computers: Array<{ status: string }> }>(
      queryKeys.computers.lists(),
    );
    expect(computers?.computers[0].status).toBe('stopped');
  });

  it('holds no stream open for a stopped machine awaiting start', async () => {
    mockGetComputers.mockResolvedValue(computer('stopped'));
    mount();
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamComputer).not.toHaveBeenCalled();
  });

  it('watches a creating machine, whose preparation runs without anyone acting', async () => {
    mockGetComputers.mockResolvedValue(computer('creating'));
    mount([]);
    await waitFor(() => expect(mockStreamComputer).toHaveBeenCalledTimes(1));
    expect(mockStreamComputer.mock.calls[0][0]).toBe(COMPUTER_ID);
  });

  it("holds a new sandbox's build stage from the stream", async () => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    mockStreamComputer.mockImplementation(async (_id: string, onFrame: ComputerOnFrame) => {
      onFrame({ status: 'starting', stage: 'tools' });
      await new Promise(() => {});
    });
    mount([]);
    const { result } = renderHookWithProviders(() => usePrepareStage(COMPUTER_ID));
    await waitFor(() => expect(result.current).toBe('tools'));
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
      { workspace_id: 'ws-legacy', name: 'Legacy', is_home: false, status: 'starting' },
      { workspace_id: 'ws-home', name: 'Home', is_home: true, status: 'unbound' },
    ]);

    await waitFor(() => expect(mockStreamWorkspace).toHaveBeenCalledTimes(1));
    // Only the unbound row that is actually moving.
    expect(mockStreamWorkspace.mock.calls[0][0]).toBe('ws-legacy');
    expect(mockStreamComputer).not.toHaveBeenCalled();
  });

  it('caches the computer assigned by a legacy workspace stream', async () => {
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    let emit: ((status: string, sandboxState?: string, computerId?: string) => void) | null = null;
    mockStreamWorkspace.mockImplementation(async (
      _id: string,
      onStatus: (status: string, sandboxState?: string, computerId?: string) => void,
    ) => {
      emit = onStatus;
      await new Promise(() => {});
    });
    const { queryClient } = mount([
      { workspace_id: 'ws-legacy', name: 'Legacy', is_home: false, status: 'starting' },
    ]);
    queryClient.setQueryData(queryKeys.workspaces.detail('ws-legacy'), {
      workspace_id: 'ws-legacy',
      name: 'Legacy',
      status: 'starting',
    });

    await waitFor(() => expect(emit).not.toBeNull());
    act(() => emit!('running', undefined, COMPUTER_ID));

    const list = queryClient.getQueryData<{ workspaces: Workspace[] }>(LIST_KEY);
    expect(list?.workspaces[0]).toMatchObject({
      status: 'running',
      computer_id: COMPUTER_ID,
    });
    expect(queryClient.getQueryData(queryKeys.workspaces.detail('ws-legacy'))).toMatchObject({
      status: 'running',
      computer_id: COMPUTER_ID,
    });
  });

  it('holds no stream open for an unbound row at rest, or one with no status', async () => {
    // A page of rows the 044 backfill has not bound yet would otherwise spend
    // the origin's six HTTP/1.1 connections waiting for news nobody will send.
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    mount([
      { workspace_id: 'ws-stopped', name: 'Stopped', is_home: false, status: 'stopped' },
      { workspace_id: 'ws-unknown', name: 'No status' } as Workspace,
    ]);

    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamWorkspace).not.toHaveBeenCalled();
  });

  it('arms the unbound watch from a cache write, with no list in its props', async () => {
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    const { queryClient } = mount([
      { workspace_id: 'ws-legacy', name: 'Legacy', is_home: false, status: 'stopped' },
    ]);
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(mockStreamWorkspace).not.toHaveBeenCalled();

    // What an action does: reflect its own 202 into the cache.
    queryClient.setQueryData(LIST_KEY, {
      workspaces: [{ workspace_id: 'ws-legacy', name: 'Legacy', is_home: false, status: 'starting' }],
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
    mockStreamComputer.mockImplementation(async (_id: string, onFrame: ComputerOnFrame) => {
      onFrame({ status: 'running' });
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
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: queryKeys.computers.lists() });
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: queryKeys.workspaces.lists() });
    // Reaching running re-reads a breakdown taken while the machine was down.
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.computers.storage(COMPUTER_ID) });
  });

  // The server settles a failed start back to stopped and keeps why on the
  // row before it sends the frame, so the row is read again rather than
  // left without its reason until some later read: 'error' from a start
  // route, 'stopped' from a start a chat turn made, which reverts its claim.
  it.each(['error', 'stopped'])('reads the row back after a failed start ending %s, which brings its persisted failure', async (frameStatus) => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    mockStreamComputer.mockImplementation(async (_id: string, onFrame: ComputerOnFrame) => {
      mockGetComputers.mockResolvedValue({
        ...computer('stopped'),
        computers: [{
          ...computer('stopped').computers[0],
          start_failure: { code: 'root_dir', message: 'The folder cannot be used.', owner: null, remedy: 'reinstall', at: '2026-10-25T16:40:00Z' },
        }],
      });
      onFrame(frameStatus === 'error' ? { status: 'error', error: 'The folder cannot be used.' } : { status: 'stopped' });
      // Held open past the render the frame causes, which tears the watch down.
      await new Promise(() => {});
    });
    const { queryClient } = mount();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await waitFor(() => expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.computers.lists() }));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.workspaces.lists() });
    await waitFor(() => {
      const row = queryClient.getQueryData<{ computers: Array<{ status: string; start_failure?: { remedy: string } }> }>(
        queryKeys.computers.lists(),
      )?.computers[0];
      expect(row?.status).toBe('stopped');
      expect(row?.start_failure?.remedy).toBe('reinstall');
    });
  });

  it('keeps the failure the frame brought when the re-read fails', async () => {
    mockGetComputers.mockResolvedValue(computer('starting'));
    mockStreamComputer.mockImplementation(async (_id: string, onFrame: ComputerOnFrame) => {
      mockGetComputers.mockRejectedValue(new Error('offline'));
      onFrame({
        status: 'error',
        error: 'The folder is held.',
        start_failure: { code: 'folder_marked', message: 'The folder is held.', owner: 'other-1', remedy: 'reinstall', at: '2026-10-26T04:00:00Z' },
      });
      await new Promise(() => {});
    });
    const { queryClient } = mount();

    await waitFor(() => {
      const row = queryClient.getQueryData<{ computers: Array<{ start_failure?: { remedy: string } | null }> }>(
        queryKeys.computers.lists(),
      )?.computers[0];
      expect(row?.start_failure?.remedy).toBe('reinstall');
    });
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

describe('removeComputerRow', () => {
  it('takes the machine off the cached list and counts it out, ahead of the refetch', () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { gcTime: Infinity } } });
    const key = queryKeys.computers.lists();
    const [kept] = computer('running').computers;
    queryClient.setQueryData(key, { computers: [kept, { ...kept, computer_id: 'second', is_primary: false }], total: 2 });

    removeComputerRow(queryClient, 'second');
    expect(queryClient.getQueryData(key)).toEqual({ computers: [kept], total: 1 });

    // A machine the list no longer holds leaves it as it is.
    const before = queryClient.getQueryData(key);
    removeComputerRow(queryClient, 'second');
    expect(queryClient.getQueryData(key)).toBe(before);
  });
});
