/**
 * What a change to an artifact makes stale, in one place for the user feed,
 * a chat send, a file view's own writes and a turn's end. The grant is never
 * among it: a new grant reloads every frame open on the file.
 */
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';

export interface ArtifactRef {
  artifactId: string;
  /**
   * The file's workspace, whose reads by path hold its record. Unknown, every
   * read by path is read again, since any of them may hold it.
   */
  workspaceId?: string | null;
}

/** Every file, or every thread's queue: what changed is not known more narrowly. */
export const ALL = 'all';

/**
 * Comments changed: a send took them or gave them back, an answer arrived, a
 * turn ended, or a view wrote or read one. Read again: the lists and records
 * (their counts) of `files`, or of every file when a turn may have touched
 * ones this view does not know of; and `queue`, the thread whose queue they
 * wait in or left, or every queue when a comment can have moved between
 * threads. `listed`: the lists already hold the server's answer, so only
 * what it moved elsewhere is read.
 */
export function refreshArtifactComments(
  qc: QueryClient,
  files: readonly ArtifactRef[] | typeof ALL,
  queue?: string | null,
  { listed = false }: { listed?: boolean } = {},
): void {
  if (files === ALL) {
    void qc.invalidateQueries({ queryKey: queryKeys.artifacts.allComments });
    void qc.invalidateQueries({ queryKey: queryKeys.artifacts.allDetails });
    void qc.invalidateQueries({ queryKey: queryKeys.artifacts.allByPath });
  } else {
    for (const { artifactId, workspaceId } of files) {
      if (!listed) void qc.invalidateQueries({ queryKey: queryKeys.artifacts.comments(artifactId) });
      void qc.invalidateQueries({ queryKey: queryKeys.artifacts.detail(artifactId) });
      void qc.invalidateQueries({
        queryKey: workspaceId ? queryKeys.artifacts.pathsIn(workspaceId) : queryKeys.artifacts.allByPath,
      });
    }
  }
  if (queue === ALL) void qc.invalidateQueries({ queryKey: queryKeys.artifacts.allQueued });
  else if (queue && queue !== '__default__') void qc.invalidateQueries({ queryKey: queryKeys.artifacts.queued(queue) });
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
  void qc.invalidateQueries({ queryKey: queryKeys.artifacts.allDestinations });
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
