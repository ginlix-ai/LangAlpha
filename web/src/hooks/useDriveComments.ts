/**
 * What the Drive reads for comments on its files: the counts its list rows
 * mark, and an HTML file's text with the page bridge in it. The comments
 * themselves are the artifact layer's (`useArtifact`), keyed by the file's
 * entry id under the owner's Home.
 */
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getCommentCounts, readPage, type DrivePageBridge } from '@/api/drive';
import { retryUnlessClientError } from '@/api/errors';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import { keepSameFile } from '@/pages/Drive/driveBody';
import type { DriveCommentCount, DriveCommentCounts } from '@/types/drive';

/** Comments are kept on files in the person's own Drive only. */
export const COMMENTS_SPACE = 'personal';

/**
 * One file's counts, or null where it has none to show. Every file's come in
 * one read, shared by the rows that mark them, and each row takes its own
 * from it (`select`), so a count that moves redraws that row alone. A comment
 * event or the owner's own comment write reads it again (`commentCountsAll`);
 * a space with no comments asks nothing.
 */
export function useDriveCommentCount(space: string | null, entryId: string): DriveCommentCount | null {
  const { data } = useQuery({
    queryKey: queryKeys.drive.commentCounts(space ?? ''),
    queryFn: () => getCommentCounts(space!),
    enabled: space === COMMENTS_SPACE,
    staleTime: 30_000,
    // A server without comments on the Drive answers 404: no markers, and no retries.
    retry: retryUnlessClientError,
    select: (counts: DriveCommentCounts) => {
      const count = counts[entryId];
      return count && (count.open > 0 || count.unread > 0) ? count : null;
    },
  });
  return data ?? null;
}

/** One HTML file at one content and version, read with one bridge. */
export interface DrivePageRead {
  entryId: string;
  sha: string;
  /** A version looked back at; null for the head. Part of the read's key. */
  seq: number | null;
  /** The version the bytes are read at: `seq`, or the head's own as its entry named it beside `sha`. */
  at: number | null;
  inject: DrivePageBridge;
}

/** A page's text with its bridge, and the hash of the bytes it was read from. */
export interface DrivePageText {
  sha: string;
  doc: string;
}

/**
 * An HTML file's text with a page bridge in its head, as the frame draws it.
 * Fixed by the bytes' hash, so a save reads it again and a look back at the
 * same bytes does not. While a new save of the same page is read, the last
 * one stays (`isPlaceholderData`, its own `sha`), so the page (its state, its
 * scroll, a comment over it) stays on screen until the new text is in.
 *
 * Which bridge a page in the person's own Drive takes hangs on Home's id
 * (comments are kept under it), which a fresh page may not have yet: a read
 * for the view bridge there waits, unasked, until Home is read, so a page
 * opened cold is read once, with the bridge it ends up wanting. Waiting
 * inside the read was not enough: Home's answer reached the read a task
 * before it reached the page, and the view read had gone out by then. Should
 * Home not be read, the view bridge is read as asked.
 */
export function useDrivePage(space: string | null, page: DrivePageRead | null) {
  const queryClient = useQueryClient();
  const home = useQuery({ ...homeWorkspaceQuery(queryClient), enabled: space === COMMENTS_SPACE });
  const waitsForHome = page?.inject === 'view' && space === COMMENTS_SPACE && home.isPending;
  const keyAt = (sha: string) => queryKeys.drive.page(space ?? '', page?.entryId ?? '', sha, page?.inject ?? '', page?.seq ?? null);
  return useQuery({
    queryKey: keyAt(page?.sha ?? ''),
    queryFn: async (): Promise<DrivePageText> => ({ sha: page!.sha, doc: await readPage(space!, page!.entryId, page!.inject, page!.at) }),
    placeholderData: keepSameFile(keyAt),
    enabled: !!space && !!page && !waitsForHome,
    staleTime: Infinity,
    gcTime: 5 * 60_000,
    retry: false,
  });
}
