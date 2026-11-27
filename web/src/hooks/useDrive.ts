import { useEffect, useEffectEvent } from 'react';
import { keepPreviousData, useInfiniteQuery, useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { ApiError } from '@/api/client';
import {
  clearHistory,
  driveErrorType,
  explainVersion,
  getAttribution,
  getChanges,
  getEntry,
  getRecord,
  getSpaces,
  getVersionText,
  listEntries,
  listMemos,
  listTrash,
  listVersions,
  makeFolder,
  moveEntry,
  purgeTrash,
  restoreTrash,
  restoreVersion,
  saveFile,
  setMemo,
  trashEntry,
  undoSave,
  type MemoChange,
} from '@/api/drive';
import { retryUnlessClientError } from '@/api/errors';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { driveBodyKey } from '@/pages/Drive/driveBody';
import type { DriveClearShown, DriveEntry, DriveMemoQuery, DriveRecordItem, DriveSpace, DriveVersionText } from '@/types/drive';

/** How often the page asks what changed while it is in view. */
export const CHANGES_POLL_MS = 5_000;

/** The ref a space is addressed by: `personal` for the person's own, else its id. */
export function spaceRef(space: Pick<DriveSpace, 'id' | 'kind'>): string {
  return space.kind === 'personal' ? 'personal' : space.id;
}

export function useDriveSpaces() {
  return useQuery({
    queryKey: queryKeys.drive.spaces(),
    queryFn: getSpaces,
    staleTime: 30_000,
  });
}

/** A folder's children; a folder that is gone answers 404, which the page shows. */
export function useDriveList(space: string | null, path: string) {
  return useQuery({
    queryKey: queryKeys.drive.list(space ?? '', path),
    queryFn: () => listEntries(space!, path),
    enabled: !!space,
    staleTime: 15_000,
    retry: retryUnlessClientError,
  });
}

/** Every folder in the space, for the Move to picker. */
export function useDriveFolders(space: string | null, enabled: boolean) {
  return useQuery({
    queryKey: queryKeys.drive.folders(space ?? ''),
    queryFn: async () => (await listEntries(space!, '', true)).filter((e) => e.kind === 'folder'),
    enabled: !!space && enabled,
    staleTime: 15_000,
  });
}

export function useDriveEntry(space: string | null, path: string | null) {
  return useQuery({
    queryKey: queryKeys.drive.entry(space ?? '', path ?? ''),
    queryFn: () => getEntry(space!, path!),
    enabled: !!space && !!path,
    staleTime: 15_000,
    retry: retryUnlessClientError,
  });
}

export function useDriveVersions(space: string | null, entryId: string | null, enabled = true) {
  return useQuery({
    queryKey: queryKeys.drive.versions(space ?? '', entryId ?? ''),
    queryFn: () => listVersions(space!, entryId!),
    enabled: !!space && !!entryId && enabled,
    staleTime: 15_000,
  });
}

/** Every memo in the space, a page at a time, in the order and with the filters asked for. */
export function useDriveMemos(space: string | null, query: DriveMemoQuery) {
  return useInfiniteQuery({
    queryKey: queryKeys.drive.memos(space ?? '', query),
    queryFn: ({ pageParam }) => listMemos(space!, query, pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (page) => page.next,
    enabled: !!space,
    staleTime: 15_000,
    placeholderData: keepPreviousData,
  });
}

/**
 * A file's record, newest first, a page at a time. A page the server ran out
 * of time counting lines for is read again, each read counting further.
 */
export function useDriveRecord(space: string | null, entryId: string | null) {
  return useInfiniteQuery({
    queryKey: queryKeys.drive.record(space ?? '', entryId ?? ''),
    queryFn: ({ pageParam }) => getRecord(space!, entryId!, pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (page) => page.next,
    enabled: !!space && !!entryId,
    staleTime: 15_000,
    refetchInterval: (query) => (query.state.data?.pages.some((page) => page.counting) ? 2_000 : false),
  });
}

/** How many items past the page asked for the record reads on for unasked, as the CLI does. */
export const CHANGE_READ_MAX = 1000;

/**
 * How the loaded record ends. `cut`: the oldest item is a version of a change
 * an older page may hold more of, so its row, which totals its versions, is
 * not whole yet. `readOn`: that row reaches back to the item at `askedAt`,
 * the last of the newest page the person asked for, so the rest of it is read
 * unasked, up to `CHANGE_READ_MAX` items past that page; the next ask reads
 * as far again. A row some other page ends inside, another change or a later
 * part of this one past an event, waits to be asked for, so one ask reads one
 * row on and never the whole history.
 */
export function recordEnd(
  items: readonly DriveRecordItem[],
  hasOlder: boolean,
  askedAt: number,
): { cut: boolean; readOn: boolean } {
  const last = items.at(-1);
  if (!hasOlder || last?.type !== 'version' || !last.change_id) return { cut: false, readOn: false };
  let run = 0;
  for (let i = items.length - 1; i >= 0; i--) {
    const item = items[i];
    if (item.type !== 'version' || item.change_id !== last.change_id) break;
    run++;
  }
  return { cut: true, readOn: askedAt >= items.length - run && items.length - 1 - askedAt < CHANGE_READ_MAX };
}

/**
 * The server reading text out of a PDF or Word file can outlast one request;
 * it answers `unavailable` with when to ask again, which is waited out a few
 * times. Anything else it refused stands.
 */
function retryWhileExtracting(count: number, err: Error): boolean {
  return count < 4 && driveErrorType(err) === 'unavailable';
}

function extractingDelay(count: number, err: Error): number {
  const after = err instanceof ApiError ? err.retryAfter : null;
  return Math.min(15_000, after && after > 0 ? after * 1000 : 2000 * (count + 1));
}

/**
 * Whether a version's text came from other bytes than those it was asked for:
 * a save folded into the head rewrites its bytes under the same seq, and the
 * text is asked for by seq alone.
 */
export function textMoved(text: DriveVersionText | undefined, sha: string | undefined): boolean {
  return !!text && !!sha && text.sha256 !== sha;
}

/**
 * Whether any of these texts came from other bytes than its source named.
 * Each new mismatch asks the source again, and every answer the source gives
 * (a retry after a failed ask included) reads the texts still apart again,
 * since the source can answer the bytes it named before (a fold and its
 * undo), which leaves the text's key, and so its query, as it was. A reread
 * answering the same bytes is no new mismatch, so it asks nothing further
 * until the source answers again: one round per distinct answer.
 */
export function useTextsApart(
  source: { refetch: () => Promise<unknown>; dataUpdatedAt: number },
  reads: { read: { data: DriveVersionText | undefined; refetch: (options?: { cancelRefetch?: boolean }) => Promise<unknown> }; sha: string | undefined }[],
): boolean {
  const moved = reads.filter(({ read, sha }) => textMoved(read.data, sha));
  const answered = moved.map(({ read, sha }) => `${sha}:${read.data!.sha256}`).join();
  // Read at call time, so only a text still apart is asked for, and one whose
  // key the source's answer moved joins the read already under way.
  const reread = useEffectEvent(() => {
    for (const { read } of moved) void read.refetch({ cancelRefetch: false });
  });
  const refetchSource = source.refetch;
  useEffect(() => {
    if (answered) void refetchSource();
  }, [answered, refetchSource]);
  const sourceAnswered = source.dataUpdatedAt;
  useEffect(() => {
    if (sourceAnswered) reread();
  }, [sourceAnswered]);
  return moved.length > 0;
}

/**
 * One version's text, cached by the bytes it came from. A read the server
 * failed at is one it tries again later, so that answer alone goes stale and
 * is asked for again on the next visit, as does one that came from other bytes.
 */
export function useDriveVersionText(space: string | null, entryId: string | null, name: string, version: { seq: number; sha: string } | null) {
  return useQuery({
    queryKey: queryKeys.drive.versionText(space ?? '', entryId ?? '', name, version?.seq ?? 0, version?.sha ?? ''),
    queryFn: () => getVersionText(space!, entryId!, version!.seq),
    enabled: !!space && !!entryId && !!version,
    staleTime: (query) => {
      const data = query.state.data;
      if (textMoved(data, query.queryKey.at(-1) as string)) return 0;
      return data?.status === 'failed' ? 60_000 : Infinity;
    },
    gcTime: 5 * 60_000,
    retry: retryWhileExtracting,
    retryDelay: extractingDelay,
  });
}

/**
 * Which version wrote each line of a memo's head, cached by the head: its seq
 * as well as its bytes, since a restore can bring back an earlier head's
 * bytes under a new version.
 */
export function useDriveAttribution(space: string | null, entryId: string | null, head: { seq: number; sha: string } | null) {
  return useQuery({
    queryKey: queryKeys.drive.attribution(space ?? '', entryId ?? '', head?.seq ?? 0, head?.sha ?? ''),
    queryFn: () => getAttribution(space!, entryId!),
    enabled: !!space && !!entryId && !!head,
    staleTime: Infinity,
    gcTime: 5 * 60_000,
    retry: retryWhileExtracting,
    retryDelay: extractingDelay,
  });
}

/** The newest `limit` trash items; a larger limit is the next page, the current one kept on screen meanwhile. */
export function useDriveTrash(space: string | null, limit: number, enabled = true) {
  return useQuery({
    queryKey: queryKeys.drive.trashPage(space ?? '', limit),
    queryFn: () => listTrash(space!, limit),
    enabled: !!space && enabled,
    staleTime: 15_000,
    placeholderData: keepPreviousData,
  });
}

/**
 * Every change to a space reads its lists, entries, versions and trash again,
 * and the usage. The entries at and under `gone` (a path moved or trashed) are
 * only marked stale: read now, while the page still shows them, they would
 * answer 404 before the page has moved on.
 */
export function invalidateDriveSpace(queryClient: QueryClient, space: string, gone?: string) {
  const entryKey = queryKeys.drive.entry(space, '').slice(0, -1);
  const isGone = (key: readonly unknown[]) => {
    if (gone === undefined || key.length !== entryKey.length + 1 || !entryKey.every((k, i) => key[i] === k)) return false;
    const path = key[entryKey.length];
    return typeof path === 'string' && (path === gone || path.startsWith(`${gone}/`));
  };
  void queryClient.invalidateQueries({ queryKey: queryKeys.drive.space(space), predicate: (q) => !isGone(q.queryKey) });
  if (gone !== undefined) void queryClient.invalidateQueries({ queryKey: queryKeys.drive.space(space), predicate: (q) => isGone(q.queryKey), refetchType: 'none' });
  void queryClient.invalidateQueries({ queryKey: queryKeys.drive.spaces() });
}

export function useDriveMutations(space: string | null) {
  const queryClient = useQueryClient();
  const settle = () => {
    if (space) invalidateDriveSpace(queryClient, space);
  };
  /** Settles a change that took the entry at `path` away; refused, nothing went. */
  const settleGone = (path: string, failed: boolean) => {
    if (space) invalidateDriveSpace(queryClient, space, failed ? undefined : path);
  };
  const mkdir = useMutation({
    mutationFn: (path: string) => makeFolder(space!, path),
    onSettled: settle,
  });
  const move = useMutation({
    mutationFn: ({ from, to }: { from: string; to: string }) => moveEntry(space!, from, to),
    onSettled: (_moved, err, { from }) => settleGone(from, !!err),
  });
  const trash = useMutation({
    mutationFn: (path: string) => trashEntry(space!, path),
    onSettled: (_id, err, path) => settleGone(path, !!err),
  });
  const restore = useMutation({
    mutationFn: (trashId: string) => restoreTrash(space!, trashId),
    onSettled: settle,
  });
  const purge = useMutation({
    mutationFn: (trashId?: string) => purgeTrash(space!, trashId),
    onSettled: settle,
  });
  const restoreTo = useMutation({
    mutationFn: ({ entryId, seq }: { entryId: string; seq: number }) => restoreVersion(space!, entryId, seq),
    onSettled: settle,
  });
  const memo = useMutation({
    mutationFn: ({ entryId, change }: { entryId: string; change: MemoChange }) => setMemo(space!, entryId, change),
    onSettled: settle,
  });
  const undo = useMutation({
    mutationFn: ({ entryId, ifMatch }: { entryId: string; ifMatch: string | null }) => undoSave(space!, entryId, ifMatch),
    onSettled: settle,
  });
  // Moves the entry's change cursor and leaves a `clear` event in its record.
  // The versions it took may be named by the entry's attribution, so that
  // and the text read out of them go too.
  const clear = useMutation({
    mutationFn: ({ entryId, shown }: { entryId: string; shown: DriveClearShown }) => clearHistory(space!, entryId, shown),
    onSuccess: (_cleared, { entryId }) => {
      if (!space) return;
      void queryClient.resetQueries({ queryKey: queryKeys.drive.attributionOf(space, entryId) });
      void queryClient.resetQueries({ queryKey: queryKeys.drive.versionTextOf(space, entryId) });
    },
    onSettled: settle,
  });
  const explain = useMutation({
    mutationFn: ({ entryId, seq, note }: { entryId: string; seq: number; note: string }) => explainVersion(space!, entryId, seq, note),
    onSettled: settle,
  });
  return { mkdir, move, trash, restore, purge, restoreTo, setMemo: memo, undo, clear, explain };
}

export interface SaveVars {
  path: string;
  /** The file as the editor loaded it: what its body is cached under. */
  entry: Pick<DriveEntry, 'id' | 'sha256' | 'name' | 'is_text' | 'size'>;
  text: string;
  ifMatch: string;
  sitting: string;
  /** Why, on a memo: kept with the version this save makes. */
  note?: string | null;
  /** The server is busy holding other uploads; the save goes again by itself. */
  onBusy?: () => void;
}

/**
 * Saves an editor's text over the version it loaded. The bytes just written
 * are the head's, so the viewer takes them from the save rather than reading
 * them back, and the entry takes the server's answer.
 */
export function useSaveDriveFile(space: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ path, text, ifMatch, sitting, note, onBusy }: SaveVars) =>
      saveFile(space, path, new Blob([text]), { ifMatch, sitting, note, onBusy }),
    onSuccess: (saved, { path, entry, text }) => {
      const prior = queryClient.getQueryData<FileBody>(driveBodyKey(space, entry, entry.sha256 ?? ''));
      if (prior && saved.sha256) {
        queryClient.setQueryData<FileBody>(
          driveBodyKey(space, saved, saved.sha256),
          { ...prior, content: text, version: saved.sha256 },
        );
      }
      queryClient.setQueryData(queryKeys.drive.entry(space, path), saved);
      invalidateDriveSpace(queryClient, space);
    },
  });
}

/** One answer from the change feed, kept as the query's data so its cursor is where the next ask starts. */
interface ChangeBatch {
  cursor: number;
  entries: DriveEntry[];
  resync: boolean;
  /**
   * The cursor moved past the one asked from. With no entries, the change
   * has none behind it (the trash emptied, the sweep's trims), so the space
   * is read again and there is nothing to follow.
   */
  moved: boolean;
}

/**
 * Follows the space's change feed while the page is in view, so what the
 * agent writes shows up without a reload. It starts from the space's
 * `change_seq` as `/spaces` read it, so nothing between that read and the
 * first poll is missed; with no cursor yet, a cursor past the end answers
 * with the current one and nothing to apply. Each batch that changed
 * anything reads the space again and hands the changed entries to
 * `onChanged`; a cursor that moved with no entries (the trash emptied in
 * another tab) only reads the space again. A feed that lost its place (`resync`: too many changes, or a
 * purge past the cursor) hands over no entries, since they are not the whole
 * story: the refetch of the listing, the open file and the trash is.
 */
export function useDriveChanges(
  space: string | null,
  startSeq: number | null,
  onChanged: (entries: DriveEntry[], resync: boolean) => void,
) {
  const queryClient = useQueryClient();
  const tell = useEffectEvent(onChanged);
  const key = queryKeys.drive.changes(space ?? '');
  const { data } = useQuery({
    queryKey: key,
    queryFn: async (): Promise<ChangeBatch> => {
      // A failed poll leaves the last batch in place, so the next asks from the same cursor.
      const since = queryClient.getQueryData<ChangeBatch>(key)?.cursor ?? startSeq;
      const found = await getChanges(space!, since ?? Number.MAX_SAFE_INTEGER);
      return since === null
        ? { cursor: found.cursor, entries: [], resync: false, moved: false }
        : { ...found, moved: found.cursor !== since };
    },
    enabled: !!space,
    refetchInterval: CHANGES_POLL_MS,
    // A hidden tab asks nothing, and catches up at once when it is shown again.
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: 'always',
    // A server restarting answers 404 or 502 for a moment; the next tick asks
    // again, and a feature turned off for good shows in the page's own reads.
    retry: false,
    // A cursor belongs to the page that read it: a page opened later starts
    // from its own read of the spaces.
    gcTime: 0,
  });

  useEffect(() => {
    if (!space || !data) return;
    if (!data.resync && data.entries.length === 0) {
      if (data.moved) invalidateDriveSpace(queryClient, space);
      return;
    }
    invalidateDriveSpace(queryClient, space);
    // Who wrote a memo's lines is kept by its head, which a clear of its
    // earlier versions from elsewhere leaves as it was.
    if (data.resync) {
      void queryClient.invalidateQueries({ queryKey: queryKeys.drive.attributions(space) });
    } else {
      for (const e of data.entries) {
        void queryClient.invalidateQueries({ queryKey: queryKeys.drive.attributionOf(space, e.id) });
      }
    }
    tell(data.resync ? [] : data.entries, data.resync);
  }, [data, space, queryClient]);
}
