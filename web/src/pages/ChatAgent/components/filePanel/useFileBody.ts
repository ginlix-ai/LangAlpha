import { useCallback, useMemo } from 'react';
import { replaceEqualDeep, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { isStale } from '@/lib/rangedFile';
import { bodyMode, readFileBody, type BodyReaders, type FileBody } from './fileBody';
import { categorizeFileError, type FileError } from './fileErrors';

/** A body is worth re-reading after a minute; a tab switch inside that window is free. */
const BODY_STALE_MS = 60_000;

/**
 * A re-read that finds a ranged file at the version already open keeps the
 * body it had, so the document on screen keeps its loader and the pages it has
 * read rather than reloading over an unchanged file. One marked stale, found
 * changed or with a slice its viewer lost, is replaced all the same: its
 * document waits on a slice it will never give.
 * The file is kept or replaced whole, never merged into a copy, which would
 * not be the object its own reads mark stale.
 */
function keepSameVersion(prev: unknown, next: unknown): unknown {
  const was = (prev as FileBody | undefined)?.ranged;
  const now = (next as FileBody).ranged;
  if (!was && !now) return replaceEqualDeep(prev, next);
  const same = was && now && !isStale(was) && was.etag === now.etag && was.length === now.length;
  return same ? prev : next;
}

interface BodyCacheArgs {
  /** The workspace id, or the panel's mount id where a share has none. */
  scope: string;
  workspaceId: string;
  readers: BodyReaders;
}

/**
 * The file-body cache: one React Query entry per path, which is what makes
 * switching tabs instant and what let the panel stop guarding reads with a
 * sequence token. A read that lands late lands on its own key, not on
 * whichever file the reader has since opened.
 */
export function useFileBodyCache({ scope, workspaceId, readers }: BodyCacheArgs) {
  const queryClient = useQueryClient();

  const keyFor = useCallback(
    (path: string) => queryKeys.workspaceFiles.body(scope, path, bodyMode(path)),
    [scope],
  );

  const options = useCallback((path: string) => ({
    queryKey: keyFor(path),
    queryFn: () => readFileBody(readers, workspaceId, path),
    staleTime: BODY_STALE_MS,
    structuralSharing: keepSameVersion,
    // A file that is not there does not become there by asking three more
    // times, and the error is the thing the panel has to show.
    retry: false,
  }), [keyFor, readers, workspaceId]);

  /**
   * Read a body imperatively, filling the same cache a tab reads. This is what
   * lets a reference lookup ask "does this path open?" and then hand the
   * answer to a tab that renders it without a second request.
   */
  const fetchBody = useCallback(
    (path: string) => queryClient.fetchQuery(options(path)),
    [queryClient, options],
  );

  /** Mark bodies stale — one path, or every body in this scope. */
  const invalidate = useCallback((path?: string) => {
    queryClient.invalidateQueries({
      queryKey: path ? keyFor(path) : queryKeys.workspaceFiles.bodies(scope),
    });
  }, [queryClient, keyFor, scope]);

  /** Write a body back after a save, so the viewer shows what was written. */
  const patchBody = useCallback((path: string, patch: Partial<FileBody>) => {
    queryClient.setQueryData<FileBody>(keyFor(path), (prev) => (prev ? { ...prev, ...patch } : prev));
  }, [queryClient, keyFor]);

  return useMemo(
    () => ({ keyFor, options, fetchBody, invalidate, patchBody }),
    [keyFor, options, fetchBody, invalidate, patchBody],
  );
}

export type FileBodyCache = ReturnType<typeof useFileBodyCache>;

interface UseFileBodyArgs {
  cache: FileBodyCache;
  path: string | null;
  /** The workspace's own state, which decides whether a 404 reads as "not backed up". */
  workspaceStatus?: string;
}

/**
 * The active tab's body. A path with no in-browser viewer never fetches: it
 * reports the binary-file error the download card is built on.
 */
export function useFileBody({ cache, path, workspaceStatus }: UseFileBodyArgs) {
  const mode = path ? bodyMode(path) : null;
  const query = useQuery({
    ...cache.options(path ?? ''),
    enabled: !!path && mode !== 'none',
  });

  const error: FileError | null = useMemo(() => {
    if (!path) return null;
    if (mode === 'none') return { category: 'binary_file' };
    return query.error ? categorizeFileError(query.error, workspaceStatus) : null;
  }, [path, mode, query.error, workspaceStatus]);

  // React Query keeps the previous body and its timestamp through a background
  // refetch and after a failed one, so the timestamp alone would say bytes are
  // in hand that have not arrived, and a changed tab would lose its marker on
  // the stale body it still shows.
  const settled = query.isSuccess && !query.isFetching;
  return {
    body: (path && mode !== 'none' ? query.data : null) ?? null,
    loading: !!path && mode !== 'none' && query.isPending,
    error,
    /** When the bytes on screen were read; 0 while a read is in flight or failed. */
    readAt: settled ? query.dataUpdatedAt : 0,
    refetch: query.refetch,
  };
}
