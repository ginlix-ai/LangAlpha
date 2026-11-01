/**
 * A workspace file's record (its artifact) and the owner's comments on it.
 *
 * A view reads the record by path, which answers null for a file never
 * recorded, so any tab can ask without an error. The owner's own views of a
 * file make it (`useOwnedArtifact`), one get-or-create per file shared by
 * every view and by a write that needs the id (`useEnsureArtifact`). Writes
 * land the server's answer in the cache at once and read again what they
 * touched. The user feed's `artifact` and `artifact_comment` events
 * invalidate the same keys (`lib/artifactCache`), which is how another tab's
 * write or the agent's answer arrives without polling.
 */
import { useCallback, useEffect, useMemo } from 'react';
import { useMutation, useQuery, useQueryClient, type QueryClient, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { ALL, refreshArtifactComments, refreshArtifactRecord } from '@/lib/artifactCache';
import { artifactUnread, readAll, readThrough, unreadComments, type ArtifactUnread } from '@/lib/artifactCommentReads';
import { GRANT_RENEW_MARGIN_MS, MIN_RENEW_INTERVAL_MS, msUntilRenewal } from '@/lib/expiry';
import { isClientError, retryUnlessClientError } from '@/api/errors';
import { workspaceRelativePath } from '@/pages/ChatAgent/utils/agentPaths';
import {
  bindArtifact,
  createArtifactGrant,
  ensureArtifact,
  findArtifact,
  getArtifact,
  getArtifactDestinations,
  getArtifactStatus,
  listArtifactComments,
  listArtifactVersions,
  listQueuedArtifactComments,
  markArtifactCommentRead,
  markArtifactCommentsRead,
  publishArtifact,
  setArtifactKeepUpdated,
  unbindArtifact,
  unshareArtifact,
} from '@/pages/ChatAgent/utils/api/artifacts';
import type {
  Artifact,
  ArtifactComment,
  ArtifactDestinations,
  ArtifactGrant,
  ArtifactStatus,
  ArtifactVersion,
  PublishArtifactRequest,
  QueuedArtifactComment,
} from '@/types/api';

/** A grant lasts 12h; keep the entry that long so a re-opened version does not mint again. */
const GRANT_GC_MS = 12 * 60 * 60_000;

/** The key a path is read under: the spelling the file routes answer for, so two spellings share one entry. */
export function artifactPathKey(path: string, root?: string | null): string {
  return workspaceRelativePath(path, root);
}

/**
 * Put a record the server answered into every entry that holds it: its
 * detail, its own path, and any other spelling a tab asked by.
 */
export function putArtifact(qc: QueryClient, artifact: Artifact): void {
  qc.setQueryData(queryKeys.artifacts.detail(artifact.artifact_id), artifact);
  qc.setQueryData(queryKeys.artifacts.byPath(artifact.workspace_id, artifact.path), artifact);
  qc.setQueriesData<Artifact | null>(
    { queryKey: queryKeys.artifacts.pathsIn(artifact.workspace_id) },
    (cached) => (cached?.artifact_id === artifact.artifact_id ? artifact : cached),
  );
}

/** Move the record's counts in every entry that holds it, for a read shown before the server answers. */
function patchArtifactEverywhere(qc: QueryClient, artifactId: string, patch: (a: Artifact) => Artifact): void {
  qc.setQueryData<Artifact>(queryKeys.artifacts.detail(artifactId), (a) => (a ? patch(a) : a));
  qc.setQueriesData<Artifact | null>(
    { queryKey: queryKeys.artifacts.allByPath },
    (cached) => (cached?.artifact_id === artifactId ? patch(cached) : cached),
  );
}

/** The record of one artifact, from wherever a view has it: its detail, or a tab's read by path. */
function cachedArtifact(qc: QueryClient, artifactId: string): Artifact | undefined {
  const detail = qc.getQueryData<Artifact>(queryKeys.artifacts.detail(artifactId));
  if (detail) return detail;
  for (const [, data] of qc.getQueriesData<Artifact | null>({ queryKey: queryKeys.artifacts.allByPath })) {
    if (data?.artifact_id === artifactId) return data;
  }
  return undefined;
}

// --- Records ---

/** The read a file tab makes, shared with a prefetch. */
export function artifactByPathQuery(workspaceId: string, path: string) {
  return {
    queryKey: queryKeys.artifacts.byPath(workspaceId, path),
    queryFn: () => findArtifact(workspaceId, path),
    staleTime: 30_000,
    retry: retryUnlessClientError,
  };
}

/**
 * A file's record by its workspace path, or null for a file never recorded.
 * Never makes one. `path` is read as the file routes read it, so an absolute
 * sandbox path and the workspace-relative one are one entry.
 */
export function useArtifactByPath(
  workspaceId: string | null | undefined,
  path: string | null | undefined,
  { enabled = true, root }: { enabled?: boolean; root?: string | null } = {},
): UseQueryResult<Artifact | null, Error> {
  const key = path ? artifactPathKey(path, root) : '';
  return useQuery<Artifact | null, Error>({
    ...artifactByPathQuery(workspaceId ?? '', key),
    enabled: enabled && !!workspaceId && !!key,
  });
}

/** One record by id; 404 when it is not the caller's. */
export function useArtifact(artifactId: string | null | undefined): UseQueryResult<Artifact, Error> {
  return useQuery<Artifact, Error>({
    queryKey: queryKeys.artifacts.detail(artifactId ?? ''),
    queryFn: () => getArtifact(artifactId!),
    enabled: !!artifactId,
    staleTime: 30_000,
    retry: retryUnlessClientError,
  });
}

/**
 * Get-or-create, private, as a query so every view of the file and every
 * write that needs the id share one request. The answer is seeded into the
 * reads by path once any still in flight is cancelled: one sent before the
 * record existed would land null over it.
 */
function ensureQuery(qc: QueryClient, workspaceId: string, key: string) {
  return {
    queryKey: queryKeys.artifacts.ensured(workspaceId, key),
    queryFn: async (): Promise<Artifact> => {
      const artifact = await ensureArtifact(workspaceId, key);
      const asked = queryKeys.artifacts.byPath(workspaceId, key);
      const canonical = queryKeys.artifacts.byPath(workspaceId, artifact.path);
      await Promise.all([
        qc.cancelQueries({ queryKey: asked, exact: true }),
        qc.cancelQueries({ queryKey: canonical, exact: true }),
      ]);
      putArtifact(qc, artifact);
      qc.setQueryData(asked, artifact);
      return artifact;
    },
    // A record once made stays made; the read by path is what notices one dropped.
    staleTime: Infinity,
    retry: retryUnlessClientError,
  };
}

export interface OwnedArtifact {
  /** Undefined until the record is made or read. */
  record: Artifact | undefined;
  /** The record could not be made, so nothing that names it can start. */
  error: Error | null;
  fetching: boolean;
  retry: () => void;
}

/**
 * The record of a file its owner is looking at, made if it has none. Every
 * view asking for the same file shares the one ensure; the record itself is
 * then read by path, where a publish and the user feed keep it current. A
 * record dropped while a view is open (the file renamed away and written
 * again) is made again, so the view never sits on a file with no record.
 */
export function useOwnedArtifact(
  workspaceId: string | null | undefined,
  path: string | null | undefined,
  { enabled = true, root }: { enabled?: boolean; root?: string | null } = {},
): OwnedArtifact {
  const qc = useQueryClient();
  const key = path ? artifactPathKey(path, root) : '';
  const on = enabled && !!workspaceId && !!key;
  const ensured = useQuery<Artifact, Error>({ ...ensureQuery(qc, workspaceId ?? '', key), enabled: on });
  // Read only once ensured: the ensure seeds it, so this view sends no GET of its own.
  const read = useQuery<Artifact | null, Error>({
    ...artifactByPathQuery(workspaceId ?? '', key),
    enabled: on && ensured.isSuccess,
  });
  const dropped = on && ensured.isSuccess && read.data === null && read.dataUpdatedAt > ensured.dataUpdatedAt;
  const { refetch } = ensured;
  useEffect(() => {
    if (dropped) void refetch({ cancelRefetch: false });
  }, [dropped, refetch]);
  return {
    record: read.data !== undefined ? read.data ?? undefined : ensured.data,
    error: ensured.error,
    fetching: ensured.isFetching,
    retry: () => void refetch(),
  };
}

/**
 * The record for a file, for a write that must name it (a first comment, a
 * bind) from a view that may not have it yet. A record already read is used
 * as it is; otherwise this joins the views' ensure, or starts it.
 */
export function useEnsureArtifact(): (workspaceId: string, path: string) => Promise<Artifact> {
  const qc = useQueryClient();
  return useCallback(async (workspaceId: string, path: string) => {
    const key = artifactPathKey(path);
    const known = qc.getQueryData<Artifact | null>(queryKeys.artifacts.byPath(workspaceId, key));
    return known ?? qc.fetchQuery(ensureQuery(qc, workspaceId, key));
  }, [qc]);
}

// --- Where comments go ---

/**
 * The default destination and the "To:" menu's candidates. `besideThreadId`
 * is the chat beside the panel; null in the standalone view.
 */
export function useArtifactDestinations(
  artifactId: string | null | undefined,
  besideThreadId: string | null,
): UseQueryResult<ArtifactDestinations, Error> {
  return useQuery<ArtifactDestinations, Error>({
    queryKey: queryKeys.artifacts.destinations(artifactId ?? '', besideThreadId),
    queryFn: () => getArtifactDestinations(artifactId!, besideThreadId),
    enabled: !!artifactId,
    staleTime: 30_000,
    retry: retryUnlessClientError,
  });
}

/** Bind and unbind. A bind makes the record if the file has none. */
export function useArtifactBinding() {
  const qc = useQueryClient();
  const ensure = useEnsureArtifact();
  const settle = (artifact: Artifact) => {
    putArtifact(qc, artifact);
    void qc.invalidateQueries({ queryKey: queryKeys.artifacts.destinationsOf(artifact.artifact_id) });
  };
  const bind = useMutation({
    mutationFn: async ({ workspaceId, path, threadId }: { workspaceId: string; path: string; threadId: string }) => {
      const artifact = await ensure(workspaceId, path);
      return bindArtifact(artifact.artifact_id, threadId);
    },
    onSuccess: settle,
  });
  const unbind = useMutation({
    mutationFn: (artifactId: string) => unbindArtifact(artifactId),
    onSuccess: settle,
  });
  return { bind, unbind };
}

// --- Publish and versions ---

/**
 * The live file against the latest version, and what a publish now would
 * carry. It reads the computer, so a view asks only while it shows it: the
 * crumb of a published file, and the Publish dialog (`always` re-reads on
 * open, as the review step must describe the files as they are now).
 */
export function useArtifactStatus(
  artifactId: string | null | undefined,
  { enabled = true, always = false }: { enabled?: boolean; always?: boolean } = {},
): UseQueryResult<ArtifactStatus, Error> {
  return useQuery<ArtifactStatus, Error>({
    queryKey: queryKeys.artifacts.status(artifactId ?? ''),
    queryFn: () => getArtifactStatus(artifactId!),
    enabled: enabled && !!artifactId,
    staleTime: always ? 0 : 30_000,
    refetchOnMount: always ? 'always' : true,
    // Each read copies the file's whole list off the computer. A change
    // arrives as an event; only the dialog reviewing it reads again on focus.
    refetchOnWindowFocus: always,
    retry: retryUnlessClientError,
  });
}

/** Newest first. */
export function useArtifactVersions(
  artifactId: string | null | undefined,
  { enabled = true }: { enabled?: boolean } = {},
): UseQueryResult<ArtifactVersion[], Error> {
  return useQuery<ArtifactVersion[], Error>({
    queryKey: queryKeys.artifacts.versions(artifactId ?? ''),
    queryFn: () => listArtifactVersions(artifactId!),
    enabled: enabled && !!artifactId,
    staleTime: 30_000,
    retry: retryUnlessClientError,
  });
}

/**
 * Where the owner frames a published version from, renewed before it lapses.
 * Renewal only matters to the next load: a frame already showing a version
 * keeps the document it has.
 */
export function useArtifactGrant(artifactId: string | null | undefined): UseQueryResult<ArtifactGrant, Error> {
  return useQuery<ArtifactGrant, Error>({
    queryKey: queryKeys.artifacts.grant(artifactId ?? ''),
    queryFn: () => createArtifactGrant(artifactId!),
    enabled: !!artifactId,
    staleTime: ({ state }) =>
      msUntilRenewal(state.data?.expires_in, state.dataUpdatedAt, GRANT_RENEW_MARGIN_MS, state.dataUpdatedAt),
    refetchInterval: ({ state }) =>
      isClientError(state.error)
        ? false
        : Math.max(msUntilRenewal(state.data?.expires_in, state.dataUpdatedAt, GRANT_RENEW_MARGIN_MS), MIN_RENEW_INTERVAL_MS),
    gcTime: GRANT_GC_MS,
    retry: retryUnlessClientError,
  });
}

/**
 * Publish (and Update), unshare and keep updated, on a record the view
 * already holds (`useOwnedArtifact`). Each answers with the record, which
 * replaces every cached copy; versions and status are read again, and the
 * workspace's share links, which list what is shared.
 */
export function useArtifactPublishing() {
  const qc = useQueryClient();
  const settle = (artifact: Artifact) => {
    putArtifact(qc, artifact);
    refreshArtifactRecord(qc, { artifactId: artifact.artifact_id, workspaceId: artifact.workspace_id });
  };
  const publish = useMutation({
    mutationFn: ({ artifactId, body }: { artifactId: string; body: PublishArtifactRequest }) =>
      publishArtifact(artifactId, body),
    onSuccess: ({ artifact }) => settle(artifact),
    // A list that changed under the review is read again for the next look.
    onError: (_err, { artifactId }) => {
      void qc.invalidateQueries({ queryKey: queryKeys.artifacts.status(artifactId) });
    },
  });
  const unshare = useMutation({
    mutationFn: (artifactId: string) => unshareArtifact(artifactId),
    onSuccess: settle,
  });
  const keepUpdated = useMutation({
    mutationFn: ({ artifactId, on }: { artifactId: string; on: boolean }) => setArtifactKeepUpdated(artifactId, on),
    onSuccess: settle,
  });
  return { publish, unshare, keepUpdated };
}

// --- Comments ---

/**
 * Every comment on the file, by pin number. Each write invalidates it, and
 * so does an `artifact_comment` event, which is how an agent's answer or
 * another tab's edit arrives.
 */
export function useArtifactComments(artifactId: string | null | undefined): UseQueryResult<ArtifactComment[], Error> {
  return useQuery<ArtifactComment[], Error>({
    queryKey: queryKeys.artifacts.comments(artifactId ?? ''),
    queryFn: () => listArtifactComments(artifactId!),
    enabled: !!artifactId,
    staleTime: 30_000,
    retry: retryUnlessClientError,
  });
}

/** The file's comments as the server has them now, into the same cache entry the cards read. */
export function fetchArtifactComments(queryClient: QueryClient, artifactId: string): Promise<ArtifactComment[]> {
  return queryClient.fetchQuery<ArtifactComment[], Error>({
    queryKey: queryKeys.artifacts.comments(artifactId),
    queryFn: () => listArtifactComments(artifactId),
    staleTime: 0,
  });
}

/** What a thread's next send carries: the comments queued for it, on any file. */
export function useQueuedArtifactComments(threadId: string | null | undefined): UseQueryResult<QueuedArtifactComment[], Error> {
  return useQuery<QueuedArtifactComment[], Error>({
    queryKey: queryKeys.artifacts.queued(threadId ?? ''),
    queryFn: () => listQueuedArtifactComments(threadId!),
    enabled: !!threadId && threadId !== '__default__',
    staleTime: 30_000,
    retry: retryUnlessClientError,
  });
}

/** What a read moved, to put back if the server refuses it. */
interface ReadSnapshot {
  comments: ArtifactComment[] | undefined;
  artifact: Artifact | undefined;
}

/**
 * Move the file's cached comments by `move` at once, and the record's count
 * (of comments with something unread) by how many the move cleared, or to
 * `unreadAfter` when the read covers the whole file.
 */
async function moveReads(
  qc: QueryClient,
  artifactId: string,
  move: (comments: ArtifactComment[]) => ArtifactComment[],
  unreadAfter?: number,
): Promise<ReadSnapshot> {
  const commentsKey = queryKeys.artifacts.comments(artifactId);
  await qc.cancelQueries({ queryKey: commentsKey });
  const comments = qc.getQueryData<ArtifactComment[]>(commentsKey);
  const artifact = cachedArtifact(qc, artifactId);
  let lost = 0;
  if (comments) {
    const next = move(comments);
    lost = unreadComments(comments) - unreadComments(next);
    qc.setQueryData<ArtifactComment[]>(commentsKey, next);
  }
  if (artifact) {
    const unread = unreadAfter ?? Math.max(0, artifact.unread - lost);
    if (unread !== artifact.unread) patchArtifactEverywhere(qc, artifactId, (a) => ({ ...a, unread }));
  }
  return { comments, artifact };
}

function restoreReads(qc: QueryClient, artifactId: string, snapshot: ReadSnapshot | undefined): void {
  if (snapshot?.comments) qc.setQueryData(queryKeys.artifacts.comments(artifactId), snapshot.comments);
  const was = snapshot?.artifact;
  if (was) patchArtifactEverywhere(qc, artifactId, (a) => ({ ...a, unread: was.unread }));
}

/**
 * Read again what a read changed: the comments, the record's count wherever a
 * tab holds it (no workspace is known here), and every queue, whose rows
 * carry the counts too.
 */
function refreshReads(qc: QueryClient, artifactId: string): void {
  refreshArtifactComments(qc, [{ artifactId }], ALL);
}

/**
 * The owner has read a comment up to `through`, the `seq` of the newest
 * message the view displayed. Shown at once and put back if refused; the
 * server tells the owner's other tabs, so their counts clear too.
 */
export function useMarkArtifactCommentRead(artifactId: string | null | undefined) {
  const qc = useQueryClient();
  const id = artifactId ?? '';
  return useMutation({
    mutationFn: ({ commentId, through }: { commentId: string; through: number }) =>
      markArtifactCommentRead(id, commentId, through),
    onMutate: ({ commentId, through }) =>
      moveReads(qc, id, (comments) => comments.map((c) => (c.comment_id === commentId ? readThrough(c, through) : c))),
    onSuccess: (comment) => {
      qc.setQueryData<ArtifactComment[]>(queryKeys.artifacts.comments(id), (prev) =>
        prev?.map((c) => (c.comment_id === comment.comment_id ? comment : c)),
      );
    },
    onError: (_err, _vars, snapshot) => restoreReads(qc, id, snapshot),
    onSettled: () => refreshReads(qc, id),
  });
}

/** The owner has read every comment on the file, each up to its latest message. */
export function useMarkArtifactCommentsRead(artifactId: string | null | undefined) {
  const qc = useQueryClient();
  const id = artifactId ?? '';
  return useMutation({
    mutationFn: () => markArtifactCommentsRead(id),
    onMutate: () => moveReads(qc, id, (comments) => readAll(comments), 0),
    onSuccess: (comments) => {
      qc.setQueryData<ArtifactComment[]>(queryKeys.artifacts.comments(id), comments);
    },
    onError: (_err, _vars, snapshot) => restoreReads(qc, id, snapshot),
    onSettled: () => refreshReads(qc, id),
  });
}

/**
 * A file's unread, from its comments: the comments holding an unread agent
 * message, in pin order so a view can open the first, and how many. Where
 * the comments are not loaded, the record's own `unread` is the same count.
 */
export function useArtifactUnread(artifactId: string | null | undefined): ArtifactUnread {
  const { data } = useArtifactComments(artifactId);
  return useMemo(() => artifactUnread(data ?? []), [data]);
}
