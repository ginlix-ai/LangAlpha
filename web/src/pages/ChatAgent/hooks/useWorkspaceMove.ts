/**
 * A workspace's move to another computer, read back from the server.
 *
 * The move runs after the request that asked for it has answered, so every
 * surface reads its progress from the move itself, never from a mutation
 * still being pending: closing the dialog stops nothing, and reopening it
 * shows the same move. Make-primary moves Home the same way, so its progress
 * is read through here too.
 *
 * The outcome is told wherever the user is by then. A move this tab asked for
 * is kept in a small store until someone has been shown how it ended: the
 * dialog that is showing it, or else a toast (`WorkspaceMoveOutcomes`).
 */
import { useEffect, useSyncExternalStore } from 'react';
import { queryOptions, useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';

import { registerAuthReset } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import { createValueStore } from '@/lib/valueStore';
import type { WorkspaceMove, WorkspaceMovePhase } from '@/types/api';

import { cancelWorkspaceMove, getWorkspaceMove, moveWorkspace } from '../utils/api';
import { retryUnlessClientError } from '@/api/errors';
import { invalidateMachineStorage } from './useComputers';
import { invalidateWorkspaceMembership } from './workspaceRowActions';

/** How often a running move is re-read. */
export const WORKSPACE_MOVE_POLL_MS = 2000;

const ACTIVE_PHASES = ['preparing', 'backing_up', 'staged', 'committed'] as const satisfies readonly WorkspaceMovePhase[];

/** A move still running. Narrowed on the phase, so a move that fails the test stays a move. */
export type ActiveWorkspaceMove = WorkspaceMove & { phase: (typeof ACTIVE_PHASES)[number] };

export function isMoveActive(move: WorkspaceMove | null | undefined): move is ActiveWorkspaceMove {
  return !!move && (ACTIVE_PHASES as readonly WorkspaceMovePhase[]).includes(move.phase);
}

/** Still copying, before the folder is set aside: the only phases a cancel can stop. */
export function isMoveCancellable(move: WorkspaceMove | null | undefined): boolean {
  return move?.phase === 'preparing' || move?.phase === 'backing_up';
}

/**
 * Refresh what a move changes: which computer the workspace is on, both
 * machines' counts and disks, and for make-primary which one is primary.
 * Every ending, not only `done`: a move that failed after setting the folder
 * aside put it back, which the rows report too.
 */
function settleMove(queryClient: QueryClient, move: WorkspaceMove) {
  invalidateWorkspaceMembership(queryClient);
  invalidateMachineStorage(queryClient, move.source_computer_id);
  invalidateMachineStorage(queryClient, move.target_computer_id);
}

/** Settle the move `before` held if `after` no longer has it running. */
function noticeEnding(queryClient: QueryClient, before: WorkspaceMove | null | undefined, after: WorkspaceMove | null) {
  if (!isMoveActive(before) || (isMoveActive(after) && after.move_id === before.move_id)) return;
  settleMove(queryClient, after?.move_id === before.move_id ? after : before);
}

function workspaceMoveQuery(queryClient: QueryClient, workspaceId: string) {
  const queryKey = queryKeys.workspaceMoves.workspace(workspaceId);
  return queryOptions({
    queryKey,
    // The ending is noticed here rather than in an effect, so it is acted on
    // once per read however many surfaces are following the move.
    queryFn: async () => {
      const before = queryClient.getQueryData<WorkspaceMove | null>(queryKey);
      const move = await getWorkspaceMove(workspaceId);
      noticeEnding(queryClient, before, move);
      return move;
    },
    // Each opening reads the move afresh: it may have ended, or started
    // elsewhere, since the last look.
    staleTime: 0,
    retry: retryUnlessClientError,
    refetchInterval: (query) => (isMoveActive(query.state.data) ? WORKSPACE_MOVE_POLL_MS : false),
  });
}

/**
 * The workspace's latest move, re-read every {@link WORKSPACE_MOVE_POLL_MS}
 * while it runs. Ended moves stay readable, so a caller decides whether an
 * old ending is worth showing.
 */
export function useWorkspaceMove(workspaceId: string | null | undefined, options: { enabled?: boolean } = {}) {
  const queryClient = useQueryClient();
  return useQuery({
    ...workspaceMoveQuery(queryClient, workspaceId ?? ''),
    enabled: (options.enabled ?? true) && !!workspaceId,
  });
}

/**
 * Put an accepted move in the cache, so its progress shows at once and the
 * poll starts. A read that left before the request would land after this and
 * put the previous move back, which also stops the poll, so it is cancelled.
 */
export async function seedWorkspaceMove(queryClient: QueryClient, move: WorkspaceMove): Promise<void> {
  const queryKey = queryKeys.workspaceMoves.workspace(move.workspace_id);
  await queryClient.cancelQueries({ queryKey });
  const before = queryClient.getQueryData<WorkspaceMove | null>(queryKey);
  queryClient.setQueryData(queryKey, move);
  // A cancel can answer with a move that had already ended.
  noticeEnding(queryClient, before, move);
}

// --- Moves whose ending is still to be told ---

/** A move this tab asked for. `label` is what its outcome names. */
export interface WatchedMove {
  moveId: string;
  /** The workspace for a move, the computer for a make-primary. */
  label: string;
}

const watched = createValueStore<Readonly<Record<string, WatchedMove>>>({});
// How many dialogs are showing each workspace's move right now. One showing
// it tells the ending itself, so no toast repeats it.
const showing = createValueStore<Readonly<Record<string, number>>>({});

registerAuthReset(() => {
  watched.set({});
  showing.set({});
});

/**
 * Keep a move this tab asked for until its ending has been shown. Seed the
 * move first: a watcher that reads the workspace's previous move, or none,
 * takes that as the move having been replaced and drops it untold.
 */
export function watchMove(move: WorkspaceMove, label: string): void {
  watched.set({ ...watched.get(), [move.workspace_id]: { moveId: move.move_id, label } });
}

/**
 * Hand the watched move over to whoever is about to show how it ended, once:
 * the second caller for the same ending gets null.
 */
export function takeWatchedMove(workspaceId: string, moveId: string): WatchedMove | null {
  const current = watched.get();
  const entry = current[workspaceId];
  if (!entry || entry.moveId !== moveId) return null;
  const rest = { ...current };
  delete rest[workspaceId];
  watched.set(rest);
  return entry;
}

export function getWatchedMove(workspaceId: string): WatchedMove | undefined {
  return watched.get()[workspaceId];
}

/** Every move whose ending is still to be told, by workspace id. */
export function useWatchedMoves(): Readonly<Record<string, WatchedMove>> {
  return useSyncExternalStore(watched.subscribe, watched.get);
}

/** Whether a dialog is showing this workspace's move. */
export function useMoveShowing(workspaceId: string): boolean {
  return useSyncExternalStore(showing.subscribe, () => (showing.get()[workspaceId] ?? 0) > 0);
}

/** Mark this workspace's move as shown for as long as the caller is mounted. */
export function useShowMove(workspaceId: string): void {
  useEffect(() => {
    const bump = (by: number) => {
      const current = showing.get();
      showing.set({ ...current, [workspaceId]: (current[workspaceId] ?? 0) + by });
    };
    bump(1);
    return () => bump(-1);
  }, [workspaceId]);
}

// --- Mutations ---

/**
 * Ask for a move. Accepted, the move is seeded for the dialog to follow and
 * watched so its ending reaches the user even if the dialog is closed by then.
 * On the hook, not on `mutate`, so this still runs when the dialog is gone.
 */
export function useMoveWorkspace() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ workspaceId, computerId, fromBackup }: {
      workspaceId: string;
      computerId: string;
      name: string;
      /** Restore onto the target from the backup, without the source. */
      fromBackup?: boolean;
    }) => (fromBackup ? moveWorkspace(workspaceId, computerId, { fromBackup }) : moveWorkspace(workspaceId, computerId)),
    onSuccess: async (move, { name }) => {
      await seedWorkspaceMove(queryClient, move);
      watchMove(move, name);
    },
  });
}

/** Cancel a move still copying. The answer is the move as it now stands, cancelled or ended. */
export function useCancelWorkspaceMove() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ workspaceId }: { workspaceId: string }) => cancelWorkspaceMove(workspaceId),
    onSuccess: async (move) => {
      // A cancelled move changed nothing, so nobody needs telling.
      if (move.phase === 'cancelled') takeWatchedMove(move.workspace_id, move.move_id);
      await seedWorkspaceMove(queryClient, move);
    },
  });
}
