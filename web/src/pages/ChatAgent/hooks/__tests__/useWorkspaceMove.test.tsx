/**
 * A move is followed by re-reading it, and the read that sees it end is the
 * one that refreshes what the move changed. That refresh must not reach the
 * move's own key: it cancels the very read that saw the ending before its
 * answer is stored, the next read sees the same ending and cancels itself
 * too, and the dialog sits on the running phase while the endpoint is hit
 * back to back.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { act } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import type { WorkspaceMove, WorkspaceMovePhase } from '@/types/api';

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  getWorkspaceMove: vi.fn(),
  moveWorkspace: vi.fn(),
  cancelWorkspaceMove: vi.fn(),
}));

import { cancelWorkspaceMove, getWorkspaceMove, moveWorkspace } from '../../utils/api';
import {
  WORKSPACE_MOVE_POLL_MS,
  getWatchedMove,
  useCancelWorkspaceMove,
  useMoveWorkspace,
  useWorkspaceMove,
} from '../useWorkspaceMove';

const mockGetMove = getWorkspaceMove as Mock;
const mockMove = moveWorkspace as Mock;
const mockCancel = cancelWorkspaceMove as Mock;

const WS = '0d6f1f9e-2c8b-4f3e-9a51-7c1e2b9d4a10';
const SOURCE = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const TARGET = '3b788b47-352e-4cfb-94f0-920b599ea227';

// A read that cancels itself starts the next one at once, so that regression
// is a loop with no timer in it, which would starve the fake clock and hang
// the run. Past this many reads the answer never comes, which ends the loop
// and leaves the test to fail on its assertions instead.
const READ_LIMIT = 20;

/** What the server holds for the workspace's latest move. */
let server: WorkspaceMove | null = null;

function move(phase: WorkspaceMovePhase, overrides: Partial<WorkspaceMove> = {}): WorkspaceMove {
  return {
    move_id: 'c1a4e0de-6b7f-4f4b-8d2e-9a3b5c7d1e20',
    workspace_id: WS,
    source_computer_id: SOURCE,
    target_computer_id: TARGET,
    make_primary: false,
    phase,
    error: null,
    created_at: '2026-10-18T09:00:00Z',
    ...overrides,
  };
}

function client() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
}

/**
 * Run the clock forward, then a moment more: the cache tells its observers on
 * a zero-delay timer, which a fake clock queues one tick later when a timer
 * (the poll) set it, so a read landing at the end of `ms` would not reach
 * React yet.
 */
async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
    await vi.advanceTimersByTimeAsync(10);
  });
}

/** The workspace's move as a surface reads it, plus the request that starts one. */
async function mountFollowing() {
  const queryClient = client();
  const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
  const rendered = renderHookWithProviders(
    () => ({ move: useWorkspaceMove(WS), request: useMoveWorkspace(), cancel: useCancelWorkspaceMove() }),
    { queryClient },
  );
  await advance(0);
  return { ...rendered, invalidate };
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers();
  runAuthResets();
  server = null;
  mockGetMove.mockImplementation(() =>
    mockGetMove.mock.calls.length > READ_LIMIT ? new Promise(() => {}) : Promise.resolve(server),
  );
  mockMove.mockImplementation(async () => {
    server = move('preparing');
    return server;
  });
});

afterEach(() => {
  vi.useRealTimers();
});

describe('useWorkspaceMove', () => {
  it('stores the read that sees the move end, refreshes membership, and stops polling', async () => {
    const { result, invalidate } = await mountFollowing();
    expect(result.current.move.data).toBeNull();

    await act(async () => {
      await result.current.request.mutateAsync({ workspaceId: WS, computerId: TARGET, name: 'Research' });
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.move.data?.phase).toBe('preparing');

    server = move('done', { committed_at: '2026-10-18T09:01:00Z', finished_at: '2026-10-18T09:01:05Z' });
    const readsBefore = mockGetMove.mock.calls.length;
    await advance(WORKSPACE_MOVE_POLL_MS);

    expect(result.current.move.data).toEqual(server);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.workspaces.all });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.computers.lists() });
    expect(mockGetMove.mock.calls.length - readsBefore).toBe(1);

    // An ended move is not polled.
    await advance(WORKSPACE_MOVE_POLL_MS * 5);
    expect(mockGetMove.mock.calls.length - readsBefore).toBe(1);
  });

  it('keeps polling a move that is still running', async () => {
    const { result } = await mountFollowing();
    await act(async () => {
      await result.current.request.mutateAsync({ workspaceId: WS, computerId: TARGET, name: 'Research' });
      await vi.advanceTimersByTimeAsync(0);
    });
    const readsBefore = mockGetMove.mock.calls.length;

    server = move('backing_up');
    await advance(WORKSPACE_MOVE_POLL_MS);
    expect(result.current.move.data?.phase).toBe('backing_up');
    server = move('committed');
    await advance(WORKSPACE_MOVE_POLL_MS);
    expect(result.current.move.data?.phase).toBe('committed');
    expect(mockGetMove.mock.calls.length - readsBefore).toBe(2);
  });

  it('a cancel that lands cancelled lets go of the watched move, so nobody tells it', async () => {
    const { result } = await mountFollowing();
    await act(async () => {
      await result.current.request.mutateAsync({ workspaceId: WS, computerId: TARGET, name: 'Research' });
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(getWatchedMove(WS)?.moveId).toBe(move('preparing').move_id);

    mockCancel.mockImplementation(async () => {
      server = move('cancelled', { finished_at: '2026-10-18T09:00:30Z' });
      return server;
    });
    await act(async () => {
      await result.current.cancel.mutateAsync({ workspaceId: WS });
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(getWatchedMove(WS)).toBeUndefined();
    expect(result.current.move.data?.phase).toBe('cancelled');
    const readsBefore = mockGetMove.mock.calls.length;
    await advance(WORKSPACE_MOVE_POLL_MS * 3);
    expect(mockGetMove.mock.calls.length).toBe(readsBefore);
  });
});
