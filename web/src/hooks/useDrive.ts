import { useEffect, useEffectEvent } from 'react';
import { keepPreviousData, useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  clearHistory,
  getChanges,
  getEntry,
  getSpaces,
  listEntries,
  listTrash,
  listVersions,
  makeFolder,
  moveEntry,
  purgeTrash,
  restoreTrash,
  restoreVersion,
  saveFile,
  trashEntry,
} from '@/api/drive';
import { retryUnlessClientError } from '@/api/errors';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { driveBodyKey } from '@/pages/Drive/driveBody';
import type { DriveEntry, DriveSpace } from '@/types/drive';

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
  // The entry is unchanged and the feed says nothing: its versions and the usage are read again here.
  const clear = useMutation({
    mutationFn: (entryId: string) => clearHistory(space!, entryId),
    onSettled: settle,
  });
  return { mkdir, move, trash, restore, purge, restoreTo, clear };
}

export interface SaveVars {
  path: string;
  /** The file as the editor loaded it: what its body is cached under. */
  entry: Pick<DriveEntry, 'id' | 'sha256' | 'name' | 'is_text' | 'size'>;
  text: string;
  ifMatch: string;
  sitting: string;
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
    mutationFn: ({ path, text, ifMatch, sitting, onBusy }: SaveVars) =>
      saveFile(space, path, new Blob([text]), { ifMatch, sitting, onBusy }),
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
    tell(data.resync ? [] : data.entries, data.resync);
  }, [data, space, queryClient]);
}
