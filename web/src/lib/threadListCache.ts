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
import { registerAuthReset } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import { isArchivedThreadsKey, mapThreadCacheEntry, patchThreadRows } from '@/lib/threadRowActions';
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

const titleHolds = new Map<string, () => void>();
// A hold outlives the cache it watches: the next session's reads must not
// take a title a signed-out session was saving.
registerAuthReset(() => {
  for (const release of [...titleHolds.values()]) release();
});

/**
 * Keep a title being saved on the thread's cached rows until its write
 * answers. A list or detail read that went out before the rename answers with
 * the title the server held then, and landing it would put the old title back
 * for as long as the write is out (the gallery's first page, still loading
 * when the sidebar renames, did exactly that). So while held, a read that
 * lands is rewritten to the held title before anything renders it. Only
 * fetched results are rewritten: a patch (a generated title, a refusal's
 * restore) is someone else's word and stands. A later hold on the thread
 * replaces this one. The rename ends it once its write settles, before it reads
 * the lists again: `saved()` after a write that saved, `release()` after one
 * that failed.
 */
export function holdThreadTitle(
  queryClient: QueryClient,
  threadId: string,
  title: string,
): { saved: () => void; release: () => void } {
  titleHolds.get(threadId)?.();
  const retitle = (rows: Thread[]): Thread[] =>
    rows.some((t) => t.thread_id === threadId && t.title !== title)
      ? rows.map((t) => (t.thread_id === threadId ? { ...t, title } : t))
      : rows;
  // The hand-off cards' and the roster's liveness reads hold a cleared title
  // as null, as patchThreadTitle writes it. Undefined when nothing differs,
  // since writing the same rows would restart the cards' poll timers.
  const held = title || null;
  const reliven = (data: { liveness?: LivenessRow[] }) => {
    const rows = data.liveness ?? [];
    if (!rows.some((r) => r.thread_id === threadId && (r.title ?? null) !== held)) return undefined;
    return { ...data, liveness: rows.map((r) => (r.thread_id === threadId ? { ...r, title: held } : r)) };
  };
  // Set once the write has saved: the reads still out then, each by the
  // promise of its fetch, and only they.
  let draining: Map<string, Promise<unknown> | undefined> | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const unsubscribe = queryClient.getQueryCache().subscribe((event) => {
    if (event.type !== 'updated') return;
    const { queryKey, queryHash, state } = event.query;
    if (queryKey[0] !== queryKeys.threads.all[0]) return;
    if (draining) {
      if (!draining.has(queryHash)) return;
      // An invalidation silently cancels an active query's read and fetches
      // again under the same hash. That read is after the write, so it is the
      // server's word, and the one this hold was for is gone.
      if (event.query.promise !== draining.get(queryHash)) {
        draining.delete(queryHash);
        if (draining.size === 0) release();
        return;
      }
    }
    if (event.action.type === 'success' && !event.action.manual) {
      const data = state.data as (Thread & { liveness?: LivenessRow[] }) | undefined;
      const next = data?.thread_id === threadId
        ? (data.title !== title ? { ...data, title } : undefined)
        : Array.isArray(data?.liveness)
          ? reliven(data)
          : mapThreadCacheEntry(data, retitle);
      if (next !== undefined) queryClient.setQueryData(queryKey, next);
    }
    if (draining && state.fetchStatus === 'idle') {
      draining.delete(queryHash);
      if (draining.size === 0) release();
    }
  });
  function release(saved = false) {
    if (saved && !draining) {
      // An invalidation cancels an active query's read and fetches again, but
      // a read with no observer (a prefetch, a list whose view has gone) keeps
      // going and would land the title from before the rename. Once the write
      // has saved, the hold lasts until the reads out at that moment settle.
      // A write that failed lets go at once: a read from before it is right.
      draining = new Map(
        queryClient.getQueryCache()
          .findAll({ queryKey: queryKeys.threads.all, fetchStatus: 'fetching' })
          .map((q) => [q.queryHash, q.promise]),
      );
      if (draining.size > 0) {
        timer = setTimeout(() => release(), 60_000);
        return;
      }
    }
    clearTimeout(timer);
    unsubscribe();
    if (titleHolds.get(threadId) === release) titleHolds.delete(threadId);
  }
  titleHolds.set(threadId, release);
  return { saved: () => release(true), release: () => release() };
}

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
