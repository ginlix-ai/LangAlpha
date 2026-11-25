/**
 * The fork mutation as its readers see it. A fork runs as one React Query
 * mutation under `queryKeys.threads.fork()` (`useForkThread`), so the button
 * that started it, the sidebar's row for the fork it is making and every
 * other fork button read the same state rather than each tracking its own.
 */
import { useIsMutating, useMutationState } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';

/** What a fork is asked to make: the variables of the fork mutation. */
export interface ForkRequest {
  /** The thread forked from. */
  threadId: string;
  /** The last turn the fork carries on from. */
  turnIndex: number;
  /** The workspace the fork is made in, where the sidebar shows it. */
  workspaceId: string;
  /** The title asked for; undefined keeps the source's. */
  title: string | undefined;
}

/** A fork whose request is still running, keyed apart from threads: it has
 *  no id until it lands. */
export interface PendingFork extends ForkRequest {
  key: string;
}

const pendingForks = (mutationKey: readonly unknown[]) => ({
  filters: { mutationKey, status: 'pending' as const },
  select: (mutation: { mutationId: number; state: { variables: unknown } }): PendingFork => ({
    key: `pending-fork:${mutation.mutationId}`,
    ...(mutation.state.variables as ForkRequest),
  }),
});

/** Forks being made in `workspaceId`, newest first, for the sidebar. A
 *  filter's predicate is read again only when the mutations change, so the
 *  workspace is matched as this renders. */
export function usePendingForks(workspaceId: string): PendingFork[] {
  const pending = useMutationState(pendingForks(queryKeys.threads.fork()));
  return pending.filter((fork) => fork.workspaceId === workspaceId).reverse();
}

/** The fork button on `turnIndex` of `threadId`: whether its own fork is
 *  running, and whether any fork is, which holds every button, since one
 *  fork runs at a time. */
export function useForkButtonState(threadId: string | undefined, turnIndex: number): { forking: boolean; held: boolean } {
  const mutationKey = queryKeys.threads.fork();
  const pending = useMutationState(pendingForks(mutationKey));
  const held = useIsMutating({ mutationKey }) > 0;
  const forking = pending.some((fork) => fork.threadId === threadId && fork.turnIndex === turnIndex);
  return { forking, held };
}
