/**
 * How the ending of a move this tab asked for reaches the user. A dialog
 * showing the move tells it there; otherwise one toast does, however many
 * outcome watchers are mounted (the app mounts two). Make-primary moves Home
 * the same way and ends in the primary toast. A cancelled or replaced move
 * changed nothing worth telling, so it tells nothing.
 *
 * Toasts are matched by catalog key through `i18n.t`, not by their English,
 * so a copy change does not break these.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { useState } from 'react';
import { QueryClient } from '@tanstack/react-query';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';

import i18n from '@/i18n';
import { renderHookWithProviders, renderWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputersResponse, WorkspaceMove, WorkspaceMovePhase } from '@/types/api';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  getComputers: vi.fn(),
  getWorkspace: vi.fn(),
  getWorkspaceMove: vi.fn(),
  moveWorkspace: vi.fn(),
  cancelWorkspaceMove: vi.fn(),
  makeComputerPrimary: vi.fn(),
}));

import { toast } from '@/components/ui/use-toast';
import { getComputers, getWorkspace, getWorkspaceMove, makeComputerPrimary, moveWorkspace } from '../../utils/api';
import { useComputerActions } from '../../hooks/useComputerActions';
import {
  WORKSPACE_MOVE_POLL_MS,
  getWatchedMove,
  seedWorkspaceMove,
  useWorkspaceMove,
  watchMove,
} from '../../hooks/useWorkspaceMove';
import MoveWorkspaceDialog, { type MoveWorkspaceTarget } from '../MoveWorkspaceDialog';
import { WorkspaceMoveOutcomes } from '../WorkspaceMoveOutcomes';

const mockToast = toast as unknown as Mock;
const mockGetComputers = getComputers as Mock;
const mockGetMove = getWorkspaceMove as Mock;
const mockMoveWorkspace = moveWorkspace as Mock;
const mockMakePrimary = makeComputerPrimary as Mock;
const mockGetWorkspace = getWorkspace as Mock;

const ALPHA = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const BETA = '3b788b47-352e-4cfb-94f0-920b599ea227';
const WS = '0d6f1f9e-2c8b-4f3e-9a51-7c1e2b9d4a10';
const HOME = '9e2a7c41-5b3d-4f8e-a1c6-2d7b8e4f0a93';
const MOVE_ID = 'c1a4e0de-6b7f-4f4b-8d2e-9a3b5c7d1e20';
const HOME_MOVE_ID = '6f0b2d8a-1c4e-4a7b-9e3d-5b8c2a1f7e64';

function computer(computer_id: string, name: string, is_primary: boolean): Computer {
  return {
    computer_id,
    user_id: 'u1',
    kind: 'daytona',
    name,
    status: 'running',
    resource_tier: 'standard',
    is_always_on: false,
    is_primary,
    workspace_count: 1,
    root_dir: '/home/workspace',
    provider_ref: `sbx-${name.toLowerCase()}`,
    spec_change: null,
    ssh: null,
  };
}

const MACHINES: ComputersResponse = {
  computers: [computer(ALPHA, 'Alpha', true), computer(BETA, 'Beta', false)],
  total: 2,
};

function move(phase: WorkspaceMovePhase, overrides: Partial<WorkspaceMove> = {}): WorkspaceMove {
  return {
    move_id: MOVE_ID,
    workspace_id: WS,
    source_computer_id: ALPHA,
    target_computer_id: BETA,
    make_primary: false,
    phase,
    error: null,
    created_at: '2026-10-18T09:00:00Z',
    ...overrides,
  };
}

/** What the server holds as each workspace's latest move. */
let server: Record<string, WorkspaceMove | null> = {};

function client() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
}

/**
 * Run the clock forward, then a moment more: the cache tells its observers on
 * a zero-delay timer, which a fake clock queues one tick later when a timer
 * (the poll) set it.
 */
async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
    await vi.advanceTimersByTimeAsync(10);
  });
}

/** What an accepted request leaves behind: the move in the cache, and watched. */
async function follow(queryClient: QueryClient, accepted: WorkspaceMove, label: string) {
  await act(async () => {
    await seedWorkspaceMove(queryClient, accepted);
    watchMove(accepted, label);
  });
  await advance(0);
}

/** Both mounts the app has: beside the workspace menus and in the computers host. */
function Outcomes() {
  return (
    <>
      <WorkspaceMoveOutcomes />
      <WorkspaceMoveOutcomes />
    </>
  );
}

function DialogAndOutcomes() {
  const [target, setTarget] = useState<MoveWorkspaceTarget | null>({
    workspace_id: WS,
    name: 'Research',
    computer_id: ALPHA,
  });
  return (
    <>
      <MoveWorkspaceDialog target={target} onClose={() => setTarget(null)} />
      <Outcomes />
    </>
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers();
  runAuthResets();
  server = {};
  mockGetComputers.mockResolvedValue(MACHINES);
  mockGetMove.mockImplementation(async (workspaceId: string) => server[workspaceId] ?? null);
  mockMoveWorkspace.mockImplementation(async (workspaceId: string) => {
    server[workspaceId] = move('preparing');
    return server[workspaceId];
  });
});

afterEach(() => {
  vi.useRealTimers();
});

describe('WorkspaceMoveOutcomes', () => {
  it('tells a move that ends with no dialog open once, with the watcher mounted twice', async () => {
    const { queryClient } = renderWithProviders(<Outcomes />, { queryClient: client() });
    server[WS] = move('preparing');
    await follow(queryClient, move('preparing'), 'Research');
    expect(mockToast).not.toHaveBeenCalled();

    server[WS] = move('done', { finished_at: '2026-10-18T09:01:05Z' });
    await advance(WORKSPACE_MOVE_POLL_MS);
    await advance(WORKSPACE_MOVE_POLL_MS * 3);

    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast).toHaveBeenCalledWith({
      title: i18n.t('workspace.move.done', { computer: 'Beta' }),
      description: 'Research',
    });
    expect(getWatchedMove(WS)).toBeUndefined();
  });

  it.each([
    ['cancelled', move('cancelled', { finished_at: '2026-10-18T09:00:30Z' })],
    ['replaced by a newer move', move('done', { move_id: '2b7e9c14-8d3a-4f6e-b5c1-0a9d7e3f2c48' })],
  ])('tells nothing of a move that was %s', async (_how, latest) => {
    const { queryClient } = renderWithProviders(<Outcomes />, { queryClient: client() });
    server[WS] = move('backing_up');
    await follow(queryClient, move('backing_up'), 'Research');

    server[WS] = latest;
    await advance(WORKSPACE_MOVE_POLL_MS);
    await advance(WORKSPACE_MOVE_POLL_MS * 3);

    expect(mockToast).not.toHaveBeenCalled();
    expect(getWatchedMove(WS)).toBeUndefined();
  });

  it('toasts a failed move as destructive, in the server\'s words', async () => {
    const reason = 'Beta stopped answering during the copy. Your files are back on Alpha.';
    const { queryClient } = renderWithProviders(<Outcomes />, { queryClient: client() });
    server[WS] = move('staged');
    await follow(queryClient, move('staged'), 'Research');

    server[WS] = move('failed', { error: { code: 'target_unreachable', message: reason } });
    await advance(WORKSPACE_MOVE_POLL_MS);
    await advance(WORKSPACE_MOVE_POLL_MS * 3);

    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast).toHaveBeenCalledWith({
      variant: 'destructive',
      title: i18n.t('workspace.move.failedTitle', { name: 'Research' }),
      description: reason,
    });
  });

  it('falls back to the generic line for a failure that carries no message', async () => {
    const { queryClient } = renderWithProviders(<Outcomes />, { queryClient: client() });
    server[WS] = move('staged');
    await follow(queryClient, move('staged'), 'Research');

    server[WS] = move('failed');
    await advance(WORKSPACE_MOVE_POLL_MS);

    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast).toHaveBeenCalledWith({
      variant: 'destructive',
      title: i18n.t('workspace.move.failedTitle', { name: 'Research' }),
      description: i18n.t('workspace.move.failed'),
    });
  });
});

describe('MoveWorkspaceDialog', () => {
  // Real timers: the panel leaves on its exit animation, which framer runs on
  // a clock the fake one does not drive.
  beforeEach(() => {
    vi.useRealTimers();
  });

  it('shows the ending itself while open, and nothing tells it again once closed', async () => {
    const { queryClient } = renderWithProviders(<DialogAndOutcomes />, { queryClient: client() });
    const submit = await screen.findByRole('button', { name: i18n.t('workspace.move.submit') });
    await waitFor(() => expect(submit).toBeEnabled());

    fireEvent.click(submit);
    expect(await screen.findByText(i18n.t('workspace.move.copying'))).toBeInTheDocument();
    expect(mockMoveWorkspace).toHaveBeenCalledWith(WS, BETA);
    expect(getWatchedMove(WS)?.moveId).toBe(MOVE_ID);

    // Any read can be the one that sees the ending; this one stands in for the
    // poll, so the test does not wait out its interval.
    server[WS] = move('done', { finished_at: '2026-10-18T09:01:05Z' });
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.workspaceMoves.workspace(WS) });
    });
    expect(await screen.findByText(i18n.t('workspace.move.done', { computer: 'Beta' }))).toBeInTheDocument();
    expect(mockToast).not.toHaveBeenCalled();
    expect(getWatchedMove(WS)?.moveId).toBe(MOVE_ID);

    // The footer's Close, not the header's icon button of the same name.
    const close = screen
      .getAllByRole('button', { name: i18n.t('common.close') })
      .find((button) => button.textContent === i18n.t('common.close'));
    expect(close).toBeDefined();
    fireEvent.click(close!);
    // Leaving, it still shows the ending it was closed on, not the picker.
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: i18n.t('workspace.move.submit') })).not.toBeInTheDocument();
    expect(screen.getByText(i18n.t('workspace.move.done', { computer: 'Beta' }))).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    expect(mockToast).not.toHaveBeenCalled();
    expect(getWatchedMove(WS)).toBeUndefined();
  });

  it('offers a restore from the backup when the machine the user brought does not answer', async () => {
    // Alpha is the user's own machine; it fails to start, so the copy cannot begin.
    mockGetComputers.mockResolvedValue({
      ...MACHINES,
      computers: [{ ...computer(ALPHA, 'Alpha', true), kind: 'ssh', status: 'error' }, computer(BETA, 'Beta', false)],
    });
    mockGetWorkspace.mockResolvedValue({
      workspace_id: WS, name: 'Research', status: 'stopped', computer_id: ALPHA,
      files_backed_up_at: new Date(Date.now() - 3 * 86_400_000).toISOString(),
    });
    const { queryClient } = renderWithProviders(<DialogAndOutcomes />, { queryClient: client() });
    const submit = await screen.findByRole('button', { name: i18n.t('workspace.move.submit') });
    await waitFor(() => expect(submit).toBeEnabled());
    fireEvent.click(submit);
    await waitFor(() => expect(mockMoveWorkspace).toHaveBeenCalledWith(WS, BETA));

    const reason = "The computer this workspace is on couldn't start, so its files couldn't be copied. Try again once it is running.";
    server[WS] = move('failed', { error: { code: 'source_unavailable', message: reason } });
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.workspaceMoves.workspace(WS) });
    });

    const offer = await screen.findByText(/Alpha didn't answer, so the files couldn't be copied from it/);
    await waitFor(() => expect(offer).toHaveTextContent(/restore this workspace on Beta from its backup taken 3\s?d(ays)? ago\.$/));
    // The offer says what failed, in place of the server's sentence.
    expect(screen.queryByText(reason)).not.toBeInTheDocument();

    mockMoveWorkspace.mockImplementation(async () => {
      server[WS] = move('preparing', { move_id: '4d1c9e7a-2b5f-4a8e-9c3d-6f0b1e2a7d58', from_backup: true });
      return server[WS];
    });
    fireEvent.click(screen.getByRole('button', { name: i18n.t('workspace.move.fromBackup.button') }));
    expect(screen.getByText("Files changed since that backup aren't included. The folder on Alpha is kept, moved to _internal/leftovers once it answers again.")).toBeInTheDocument();
    expect(mockMoveWorkspace).toHaveBeenCalledTimes(1);

    const confirm = screen
      .getAllByRole('button', { name: i18n.t('workspace.move.fromBackup.button') })
      .at(-1)!;
    fireEvent.click(confirm);
    await waitFor(() => expect(mockMoveWorkspace).toHaveBeenCalledWith(WS, BETA, { fromBackup: true }));
    expect(await screen.findByText(i18n.t('workspace.move.fromBackup.restoring'))).toBeInTheDocument();
  });
});

describe('make primary', () => {
  // Real timers: the case below has to run outside act, and the testing
  // library's waitFor polls on a clock the fake one would stop.
  beforeEach(() => {
    vi.useRealTimers();
  });

  /** The computer list, which reads Home's move to hold its button, beside both watchers. */
  async function mountComputers() {
    const queryClient = client();
    const { result } = renderHookWithProviders(
      () => {
        useWorkspaceMove(HOME);
        return useComputerActions();
      },
      { queryClient },
    );
    renderWithProviders(<Outcomes />, { queryClient });
    await waitFor(() =>
      expect(queryClient.getQueryState(queryKeys.workspaceMoves.workspace(HOME))?.status).toBe('success'),
    );
    return { result, queryClient };
  }

  it('toasts the primary outcome once the Home move it started ends', async () => {
    const { result, queryClient } = await mountComputers();
    // Home has never moved, so the cache holds a settled null, not nothing.
    expect(queryClient.getQueryData(queryKeys.workspaceMoves.workspace(HOME))).toBeNull();

    const homeMove = move('preparing', { move_id: HOME_MOVE_ID, workspace_id: HOME, make_primary: true });
    mockMakePrimary.mockImplementation(async () => {
      server[HOME] = { ...homeMove, phase: 'done', finished_at: '2026-10-18T09:02:00Z' };
      return { computer_id: BETA, is_primary: false, move: homeMove };
    });

    // Outside act, where a click's continuation runs: there React renders the
    // watchers as soon as the move is watched, before the rest of the success
    // handler. A watch made before the seed is read against the earlier null
    // in that window and dropped as replaced.
    result.current.makePrimary.mutate({ computerId: BETA, name: 'Beta' });
    await waitFor(() => expect(mockToast).toHaveBeenCalled());
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast).toHaveBeenCalledWith({ title: i18n.t('computer.makePrimary.done', { name: 'Beta' }) });
    expect(getWatchedMove(HOME)).toBeUndefined();
  });

  it('a flip that needs no move refreshes the machines and tells nothing', async () => {
    const { result, queryClient } = await mountComputers();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    mockMakePrimary.mockResolvedValue({ computer_id: BETA, is_primary: true, move: null });

    await act(async () => {
      await result.current.makePrimary.mutateAsync({ computerId: BETA, name: 'Beta' });
    });

    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.computers.lists() });
    expect(queryClient.getQueryData(queryKeys.workspaceMoves.workspace(HOME))).toBeNull();
    expect(getWatchedMove(HOME)).toBeUndefined();
    expect(mockToast).not.toHaveBeenCalled();
  });
});
