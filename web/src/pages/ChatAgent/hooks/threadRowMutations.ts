import type { QueryClient } from '@tanstack/react-query';
import { toast } from '@/components/ui/use-toast';
import { authSessionCheck, registerAuthReset } from '@/lib/authResets';
import { isThreadRemoved, markThreadArchived } from '@/lib/deletedThreads';
import { queryKeys } from '@/lib/queryKeys';
import { holdThreadTitle, patchThreadTitle } from '@/lib/threadListCache';
import { isArchivedThreadsKey, patchThreadRows, rollbackThreadRows } from '@/lib/threadRowActions';
import { getThread, updateThread, updateThreadTitle } from '../utils/api';
import { denialMessage } from '../utils/denialMessage';
import { getNavPrefs } from '../utils/navPrefs';
import { workspaceThreadsQuery } from '../utils/threadQueries';
import { releaseThreadOrderFreeze } from './navOrderFreeze';

type Translate = Parameters<typeof denialMessage>[1];
type Row = { thread_id: string };
type TurnsEntry = { turns?: Row[] };

/**
 * Map the thread's conversation matches in one cached search answer (a page,
 * or a paged answer's pages), which list a thread's turns apart from its row.
 * Undefined when the entry holds none of them.
 */
function mapSearchTurns(data: unknown, threadId: string, mapRows: (rows: Row[]) => Row[]): unknown {
  const mapPage = (page: TurnsEntry) =>
    Array.isArray(page?.turns) && page.turns.some((row) => row.thread_id === threadId) ? { ...page, turns: mapRows(page.turns) } : page;
  if (!data || typeof data !== 'object') return undefined;
  const entry = data as TurnsEntry & { pages?: TurnsEntry[] };
  if (Array.isArray(entry.pages)) {
    const pages = entry.pages.map(mapPage);
    return pages.some((page, i) => page !== entry.pages![i]) ? { ...entry, pages } : undefined;
  }
  const next = mapPage(entry);
  return next === entry ? undefined : next;
}

/**
 * Patch one thread's row in every cached list holding it (the sidebar's
 * pages, the gallery's, the recent threads, the palette's search answers and
 * the conversation matches in them, and the nav tree's "Show more" store), save it with PATCH /threads/{id}, and put
 * every row back if the server refuses. Resolves false then rather than
 * rejecting: a pin or archive button calls this and moves on. The lists are
 * read again once it saved.
 */
async function patchThreadRow(
  queryClient: QueryClient,
  workspaceId: string,
  threadId: string,
  updates: { is_pinned?: boolean; archived?: boolean },
  mapRows: (rows: Row[]) => Row[],
  skipKey?: (key: readonly unknown[]) => boolean,
): Promise<boolean> {
  // The matches first: the rows' patch below then snapshots an answer it
  // shares with them as the matches left it, so putting back both in the
  // reverse order leaves the answer as it was.
  const previousTurns: Array<[readonly unknown[], unknown]> = [];
  for (const [key, data] of queryClient.getQueriesData({ queryKey: queryKeys.threads.searchAll() })) {
    const next = mapSearchTurns(data, threadId, mapRows);
    if (next === undefined) continue;
    previousTurns.push([key, data]);
    queryClient.setQueryData(key, next);
  }
  const previous = patchThreadRows<Row>(
    queryClient,
    queryKeys.threads.all,
    (rows) => (rows.some((row) => row.thread_id === threadId) ? mapRows(rows) : rows),
    { skipKey },
  );
  try {
    await updateThread(threadId, updates);
  } catch (e) {
    rollbackThreadRows(queryClient, previous);
    for (const [key, data] of previousTurns) queryClient.setQueryData(key, data);
    console.warn('[threadRowMutations] Failed to update thread:', e);
    return false;
  }
  // Past this point the server holds the change: nothing below rolls back.
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.recentAll() });
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.searchAll() });
  return true;
}

/**
 * Pin a thread to the top of its workspace, or unpin it. Resolves whether the
 * server took it. Once it has, the workspace's threads are read again and only
 * then is the nav tree's frozen order released (the same rule as a workspace's
 * pin): any render while the cache is merely stale would re-freeze the old
 * order, and an unpinned row would keep its pinned-era place for the session.
 * The page-0 entry is fetched explicitly, since the tree's cache-only
 * observers make that query "disabled" and refetchQueries skips it.
 */
export async function pinThreadRow(queryClient: QueryClient, workspaceId: string, threadId: string, pinned: boolean): Promise<boolean> {
  const updates = { is_pinned: pinned };
  const ok = await patchThreadRow(queryClient, workspaceId, threadId, updates, (rows) =>
    rows.map((row) => (row.thread_id === threadId ? { ...row, ...updates } : row)),
  );
  if (!ok) return false;
  try {
    await queryClient.fetchQuery({ ...workspaceThreadsQuery(workspaceId, getNavPrefs().threadPageSize), staleTime: 0 });
  } catch (e) {
    // Released anyway: re-snapshotting from the patched cache beats a stuck order.
    console.warn('[threadRowMutations] Post-pin refetch failed:', e);
  }
  releaseThreadOrderFreeze(workspaceId);
  return true;
}

/**
 * Archive a thread, or bring one back. It leaves the lists it was in at once
 * (an archived row lives only in the gallery's archived view) and joins the
 * others when they are read again. Resolves whether the server took it, so a
 * caller moves off the thread only once it has.
 */
export async function setThreadArchived(queryClient: QueryClient, workspaceId: string, threadId: string, archived: boolean): Promise<boolean> {
  // Before the rows leave, for the nav tree's reveal to read as they do; a
  // refusal puts back what was known before.
  const known = isThreadRemoved(threadId);
  markThreadArchived(threadId, archived);
  const ok = await patchThreadRow(
    queryClient,
    workspaceId,
    threadId,
    { archived },
    (rows) => rows.filter((row) => row.thread_id !== threadId),
    // The list the row is joining holds it already when an earlier read did.
    archived ? isArchivedThreadsKey : (key) => !isArchivedThreadsKey(key),
  );
  if (!ok) markThreadArchived(threadId, known);
  return ok;
}

export function archiveThreadRow(queryClient: QueryClient, workspaceId: string, threadId: string): Promise<boolean> {
  return setThreadArchived(queryClient, workspaceId, threadId, true);
}

// Each thread's latest rename, shared by every surface that renames (each
// cached ChatView on mobile mounts its own nav drawer, and the palette its own).
const renameSeq = new Map<string, number>();
registerAuthReset(() => renameSeq.clear());

/**
 * Rename a thread. The field that asked closes at once, so the new title
 * lands in every cache before the server answers, and a refusal puts the old
 * one back and says so in a toast. A thread's renames reach the server one
 * at a time (updateThreadTitle queues them), and only the latest one writes
 * the caches when it answers: an earlier one answering must not show its
 * title over a later one. A refusal puts back the title the server holds,
 * read again, and only where the refused title still shows: a remembered one
 * may be an earlier rename's that never saved, and an entry another writer
 * has moved on since keeps its newer title. An answer that arrives after a
 * sign-out belongs to the session before it and is dropped.
 */
export async function renameThreadRow(
  queryClient: QueryClient,
  workspaceId: string,
  threadId: string,
  title: string,
  previousTitle: string,
  t: Translate,
): Promise<void> {
  const seq = (renameSeq.get(threadId) ?? 0) + 1;
  renameSeq.set(threadId, seq);
  const isLatest = () => renameSeq.get(threadId) === seq;
  const sameSession = authSessionCheck();
  patchThreadTitle(queryClient, threadId, title);
  const hold = holdThreadTitle(queryClient, threadId, title);
  try {
    const updated = await updateThreadTitle(threadId, title).then(
      (saved) => {
        hold.saved();
        return saved;
      },
      (e: unknown) => {
        hold.release();
        throw e;
      },
    );
    if (!sameSession()) return;
    if (isLatest()) patchThreadTitle(queryClient, threadId, updated.title ?? title, updated.updated_at);
  } catch (e) {
    if (!sameSession()) return;
    console.warn('[threadRowMutations] Failed to rename thread:', e);
    toast({ variant: 'destructive', title: t('thread.failedRenameThread'), description: denialMessage(e, t) });
    if (!isLatest()) return;
    const held = await getThread(threadId).catch(() => null);
    if (!sameSession() || !isLatest()) return;
    patchThreadTitle(queryClient, threadId, held?.title ?? previousTitle, undefined, { from: title });
  }
  if (!isLatest()) return;
  // A read already out when the field closed lands the title being saved
  // (holdThreadTitle), not the server's, and a refused rename may have put
  // back one that never saved: read the server's title again now that it
  // has answered.
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.detail(threadId) });
  // A conversation match names its thread by the title it was found under.
  queryClient.invalidateQueries({ queryKey: queryKeys.threads.searchAll() });
}
