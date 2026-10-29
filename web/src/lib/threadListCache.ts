/**
 * Direct React Query cache patches for thread rows.
 *
 * A thread row lives in several cache-key variants at once (the sidebar's
 * page-sized `byWorkspace` keys, ThreadGallery's infinite key, dashboard
 * widget keys, `recent`, `detail`), so these helpers patch by prefix via
 * `getQueriesData` instead of targeting one key, and route every rewrite
 * through `threadRowActions` so the finite and infinite shapes are both
 * covered. setQueryData (not invalidate) keeps updates live — no refetch
 * round-trip — matching the `patchWorkspace` pattern in useNavigationData.
 */
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { isArchivedThreadsKey, patchThreadRows } from '@/lib/threadRowActions';
import type { Thread } from '@/types/api';

/**
 * Prepend a just-created thread into every cached list for its workspace so
 * the sidebar/gallery paint the row immediately, without waiting for the
 * reconcile refetch. No-op on lists that already contain the id (refetch won).
 */
export function insertOptimisticThread(queryClient: QueryClient, thread: Thread): void {
  patchThreadRows(
    queryClient,
    queryKeys.threads.byWorkspace(thread.workspace_id),
    (rows) => (rows.some((t) => t.thread_id === thread.thread_id) ? rows : [thread, ...rows]),
    {
      // Page 0 only — an infinite list's later pages are older slices.
      headOnly: true,
      // A just-created thread is never archived, so the gallery's archived
      // view (same prefix) must not receive it.
      skipKey: isArchivedThreadsKey,
    },
  );
}

/**
 * Apply a (generated or renamed) title to every cached thread list holding
 * the row, plus the detail entry and the hand-off cards' liveness reads.
 * Entries without the row are skipped — this never invents rows, only
 * rewrites titles in place, so list order and pagination are untouched.
 *
 * `updatedAt` is the title write's row timestamp (every server title write
 * bumps updated_at): a patch not newer than the cached row is dropped, so a
 * delayed generated-title feed event can't overwrite a manual rename that
 * already refreshed the cache. Without it the patch applies unconditionally
 * (local rename flows that already won the server race).
 *
 * `from` limits the patch to entries still showing that title, for a write
 * that undoes one of our own: an entry another writer has moved on since keeps
 * its newer title.
 */
export function patchThreadTitle(
  queryClient: QueryClient,
  threadId: string,
  title: string,
  updatedAt?: string,
  { from }: { from?: string } = {},
): void {
  const eventTs = updatedAt ? Date.parse(updatedAt) : NaN;
  const isStale = (row: Thread): boolean => {
    if (from !== undefined && row.title !== from) return true;
    if (Number.isNaN(eventTs) || !row.updated_at) return false;
    const rowTs = Date.parse(row.updated_at);
    return !Number.isNaN(rowTs) && eventTs <= rowTs;
  };
  const apply = <T extends Thread>(row: T): T =>
    isStale(row) ? row : { ...row, title, ...(updatedAt ? { updated_at: updatedAt } : {}) };
  patchThreadRows(queryClient, queryKeys.threads.all, (rows) =>
    rows.some((t) => t.thread_id === threadId)
      ? rows.map((t) => (t.thread_id === threadId ? apply(t) : t))
      : rows,
  );
  queryClient.setQueryData(
    queryKeys.threads.detail(threadId),
    (old: Thread | undefined) => (old ? apply(old) : old),
  );
  // The hand-off cards' and the roster's liveness reads, rows with no
  // updated_at. A card stops reading once its run settles, so a title written
  // after that reaches it only from here. Unchanged entries return undefined,
  // which skips the write: returning the same rows would count as a fresh read
  // and restart the cards' poll timers. A cleared title is stored as null, as
  // the server reads it.
  queryClient.setQueriesData<{ liveness: LivenessRow[] }>(
    { queryKey: queryKeys.threads.dispatchLivenessAll() },
    (data) => {
      const matches = (r: LivenessRow) => r.thread_id === threadId && (from === undefined || (r.title ?? '') === from);
      if (!data?.liveness.some(matches)) return undefined;
      const liveness = data.liveness.map((r) => (matches(r) ? { ...r, title: title || null } : r));
      return { ...data, liveness };
    },
  );
}

type LivenessRow = { thread_id: string; title?: string | null };

/**
 * Apply a thread's share status to every cached thread list holding the row,
 * so the gallery's public-link badge follows a share made from anywhere.
 * Lists already in agreement are left untouched.
 */
export function patchThreadShared(queryClient: QueryClient, threadId: string, isShared: boolean): void {
  patchThreadRows<Thread & { is_shared?: boolean }>(queryClient, queryKeys.threads.all, (rows) =>
    rows.some((t) => t.thread_id === threadId && Boolean(t.is_shared) !== isShared)
      ? rows.map((t) => (t.thread_id === threadId ? { ...t, is_shared: isShared } : t))
      : rows,
  );
}

/**
 * Stamp a seen cursor onto every cached row of the thread; rows are re-seeded
 * into the lifecycle store, which owns the dot. The cursor only ever advances
 * (a stale patch can't resurrect a cleared dot).
 */
export function patchThreadSeen(
  queryClient: QueryClient,
  threadId: string,
  lastSeenSeq: number,
): void {
  patchThreadRows(queryClient, queryKeys.threads.all, (rows) =>
    rows.some((t) => t.thread_id === threadId)
      ? rows.map((t) =>
          t.thread_id === threadId
            ? { ...t, last_seen_run_seq: Math.max(Number(t.last_seen_run_seq ?? 0), lastSeenSeq) }
            : t,
        )
      : rows,
  );
}
