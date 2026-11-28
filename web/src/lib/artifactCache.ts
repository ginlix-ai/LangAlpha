/**
 * What a change to an artifact makes stale, in one place for the user feed,
 * a chat send, a file view's own writes and a turn's end. The grant is never
 * among it: a new grant reloads every frame open on the file.
 */
import type { QueryClient, QueryKey } from '@tanstack/react-query';
import type { ArtifactKind } from '@/types/artifacts';
import { queryKeys } from '@/lib/queryKeys';

export interface ArtifactRef {
  artifactId: string;
  /**
   * The file's workspace, whose reads by path hold its record. Unknown, every
   * read by path is read again, since any of them may hold it.
   */
  workspaceId?: string | null;
  /** The record's kind. Unknown, it may be a Drive file's, whose own reads go too. */
  kind?: ArtifactKind;
}

/** Every file, or every thread's queue: what changed is not known more narrowly. */
export const ALL = 'all';

/** Every Drive file's comments, which no comment event names: a file trashed, restored, purged or saved. */
const EVERY_DRIVE_FILE = 'drive';

const mayBeDrive = (ref: ArtifactRef): boolean => ref.kind === undefined || ref.kind === 'drive';

/**
 * Comments changed: a send took them or gave them back, an answer arrived, a
 * turn ended, or a view wrote or read one. Read again: the lists and records
 * (their counts) of `files`, or of every file when a turn may have touched
 * ones this view does not know of; and `queue`, the thread whose queue they
 * wait in or left, or every queue when a comment can have moved between
 * threads. `listed`: the lists already hold the server's answer, so only
 * what it moved elsewhere is read.
 *
 * A Drive file's comments are read by two more: the Drive's list markers,
 * which count every Drive file's in one read, and its default destination,
 * the chat its last comment went to (`worked`). Those go only for a Drive
 * file, or one whose kind is not known; every file's destinations go too
 * when which files changed is not known, since a thread they offer may have
 * gone. Only a view showing them reads them again.
 */
export function refreshArtifactComments(
  qc: QueryClient,
  files: readonly ArtifactRef[] | typeof ALL | typeof EVERY_DRIVE_FILE,
  queue?: string | null,
  { listed = false }: { listed?: boolean } = {},
): void {
  const stale: QueryKey[] = [];
  if (files === ALL) {
    stale.push(queryKeys.artifacts.allComments, queryKeys.artifacts.allDetails, queryKeys.artifacts.allByPath);
  } else if (files !== EVERY_DRIVE_FILE) {
    for (const { artifactId, workspaceId } of files) {
      if (!listed) stale.push(queryKeys.artifacts.comments(artifactId));
      stale.push(
        queryKeys.artifacts.detail(artifactId),
        workspaceId ? queryKeys.artifacts.pathsIn(workspaceId) : queryKeys.artifacts.allByPath,
      );
    }
  }
  if (typeof files === 'string') {
    stale.push(queryKeys.drive.commentCountsAll, queryKeys.artifacts.allDestinations);
  } else {
    const drive = files.filter(mayBeDrive);
    if (drive.length > 0) stale.push(queryKeys.drive.commentCountsAll);
    for (const { artifactId } of drive) stale.push(queryKeys.artifacts.destinationsOf(artifactId));
  }
  if (queue === ALL) stale.push(queryKeys.artifacts.allQueued);
  else if (queue && queue !== '__default__') stale.push(queryKeys.artifacts.queued(queue));
  for (const queryKey of stale) void qc.invalidateQueries({ queryKey });
}

/**
 * The record changed: a publish, an Update, a version keep updated cut, an
 * unshare, the setting toggled, a bind or unbind. Its detail, versions,
 * status and destinations are read again, and the workspace's share links,
 * which list what is shared.
 */
export function refreshArtifactRecord(qc: QueryClient, { artifactId, workspaceId }: ArtifactRef): void {
  void qc.invalidateQueries({ queryKey: queryKeys.artifacts.detail(artifactId) });
  void qc.invalidateQueries({ queryKey: queryKeys.artifacts.versions(artifactId) });
  void qc.invalidateQueries({ queryKey: queryKeys.artifacts.status(artifactId) });
  void qc.invalidateQueries({ queryKey: queryKeys.artifacts.destinationsOf(artifactId) });
  if (workspaceId) {
    void qc.invalidateQueries({ queryKey: queryKeys.artifacts.pathsIn(workspaceId) });
    void qc.invalidateQueries({ queryKey: queryKeys.shareLinks.byWorkspace(workspaceId) });
  }
}

/**
 * A thread was archived, deleted or brought back, which no artifact event
 * says: comments name it, and destinations offer it.
 */
export function refreshArtifactThreads(qc: QueryClient): void {
  refreshArtifactComments(qc, ALL, ALL);
}

/**
 * A Drive file was trashed, restored, purged or saved, which no comment
 * event says: a file in the trash leaves every chat's queue and the list
 * markers, one brought back returns to them, and an agent's save can make
 * its chat the file's default destination.
 */
export function refreshDriveComments(qc: QueryClient): void {
  refreshArtifactComments(qc, EVERY_DRIVE_FILE, ALL);
}

/**
 * The feed was away, and its events have no replay. Status stays out of it:
 * it reads the computer, and a file view asks for it when it needs it.
 */
export function refreshEveryArtifact(qc: QueryClient): void {
  refreshArtifactThreads(qc);
  void qc.invalidateQueries({ queryKey: [...queryKeys.artifacts.all, 'versions'] });
  void qc.invalidateQueries({ queryKey: queryKeys.shareLinks.all });
}
