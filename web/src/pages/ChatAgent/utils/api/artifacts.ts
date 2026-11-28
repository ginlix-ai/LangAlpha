/**
 * Artifacts (owner side): a workspace file's record, its published versions,
 * where its comments go, and the comments themselves.
 *
 * A leaf, like `shareLinks.ts`: consumers import it directly. A comment reaches
 * the agent only through a chat send that names it (`artifact_comment_ids`);
 * these calls write it, queue it or hold it back.
 */
import { api } from '@/api/client';
import i18n from '@/i18n';
import { ARTIFACT_KINDS, kindOf, type TargetParamName } from '@/lib/artifactKind';
import { deviceTimezone } from '@/lib/deviceTimezone';
import type { PageLoads } from '@/lib/pageLoads';
import { randomUUID } from '@/lib/randomUUID';
import type {
  AddArtifactCommentMessageRequest,
  Artifact,
  ArtifactComment,
  ArtifactDestinations,
  ArtifactGrant,
  ArtifactKind,
  ArtifactReading,
  ArtifactStatus,
  ArtifactTarget,
  ArtifactVersion,
  CreateArtifactCommentRequest,
  DestinationThread,
  PublishArtifactRequest,
  PublishArtifactResponse,
  QueuedArtifactComment,
  UpdateArtifactCommentRequest,
  ShareAudience,
} from '@/types/api';
import { sendChatMessageStream } from './messages';
import type { StreamRequestError } from './transport';

const artifactPath = (artifactId: string) => `/api/v1/artifacts/${encodeURIComponent(artifactId)}`;
const workspaceArtifactsPath = (workspaceId: string) =>
  `/api/v1/workspaces/${encodeURIComponent(workspaceId)}/artifacts`;

/** A payload as the server sends it: an older one leaves out `kind`. */
type Wire<T extends { kind: ArtifactKind }> = Omit<T, 'kind'> & { kind?: ArtifactKind };

/** A payload's kind, read here as it arrives, so nothing past this layer decodes it again. */
function kinded<T extends { kind: ArtifactKind }>(x: Wire<T>): T {
  return { ...x, kind: kindOf(x) } as T;
}

const asRecord = (data: Wire<Artifact>) => kinded<Artifact>(data);
const asComment = (data: Wire<ArtifactComment>) => kinded<ArtifactComment>(data);

// --- Records ---

/** A target as the records route names it: by its kind's parameter (`ARTIFACT_KINDS`), only one. */
export function targetParam(target: ArtifactTarget): Partial<Record<TargetParamName, string>> {
  return { [ARTIFACT_KINDS[target.kind].param]: target.path };
}

/** The target's record if it has one; never makes it, so a tab can ask for any HTML file. */
export async function findArtifact(workspaceId: string, target: ArtifactTarget): Promise<Artifact | null> {
  const { data } = await api.get<{ artifact: Wire<Artifact> | null }>(workspaceArtifactsPath(workspaceId), { params: targetParam(target) });
  return data.artifact && asRecord(data.artifact);
}

/** The target's record, made private if new. 400 for a path that may not be shared, or a chart id that is not one. */
export async function ensureArtifact(workspaceId: string, target: ArtifactTarget): Promise<Artifact> {
  const { data } = await api.post<Wire<Artifact>>(workspaceArtifactsPath(workspaceId), targetParam(target));
  return asRecord(data);
}

/** The record of each timeframe of `symbol`'s chart the workspace has, each with its own counts. */
export async function listSymbolCharts(workspaceId: string, symbol: string): Promise<Artifact[]> {
  const { data } = await api.get<{ charts: Wire<Artifact>[] }>(`${workspaceArtifactsPath(workspaceId)}/charts`, { params: { symbol } });
  return data.charts.map(asRecord);
}

/** 404 when it is not the caller's. */
export async function getArtifact(artifactId: string): Promise<Artifact> {
  const { data } = await api.get<Wire<Artifact>>(artifactPath(artifactId));
  return asRecord(data);
}

export async function setArtifactKeepUpdated(artifactId: string, keepUpdated: boolean): Promise<Artifact> {
  const { data } = await api.patch<Wire<Artifact>>(artifactPath(artifactId), { keep_updated: keepUpdated });
  return asRecord(data);
}

/**
 * Keep the file for its owner without sharing it, which makes a page a
 * dashboard: the server holds a version, cut now and again at the end of each
 * turn whose files changed, so it opens with the computer off. A keep carries
 * what the open page reported loading, as a publish does: a file it fetches
 * by a path built in code is named by nothing else.
 */
export async function setArtifactKept(artifactId: string, kept: boolean, loads?: PageLoads): Promise<Artifact> {
  const body = kept && loads ? { kept, loaded: loads.now, earlier: loads.earlier } : { kept };
  const { data } = await api.put<Wire<Artifact>>(`${artifactPath(artifactId)}/kept`, body);
  return asRecord(data);
}

/** Take a file the page does not need to draw off what a publish carries, or put it back. */
export async function setArtifactFileLeftOut(artifactId: string, path: string, leftOut: boolean): Promise<Artifact> {
  const { data } = await api.post<Wire<Artifact>>(`${artifactPath(artifactId)}/left-out`, { path, left_out: leftOut });
  return asRecord(data);
}

// --- Where comments go ---

/** Send the file's comments to `threadId` until unbound. 409 `destination_unavailable`. */
export async function bindArtifact(artifactId: string, threadId: string): Promise<Artifact> {
  const { data } = await api.put<Wire<Artifact>>(`${artifactPath(artifactId)}/binding`, { thread_id: threadId });
  return asRecord(data);
}

export async function unbindArtifact(artifactId: string): Promise<Artifact> {
  const { data } = await api.delete<Wire<Artifact>>(`${artifactPath(artifactId)}/binding`);
  return asRecord(data);
}

/**
 * The default destination and the "To:" menu's candidates. `besideThreadId` is
 * the thread open beside the panel; leave it out in the standalone view.
 */
export async function getArtifactDestinations(
  artifactId: string,
  besideThreadId?: string | null,
): Promise<ArtifactDestinations> {
  const { data } = await api.get<ArtifactDestinations>(`${artifactPath(artifactId)}/destinations`, {
    params: besideThreadId ? { beside: besideThreadId } : undefined,
  });
  return data;
}

/** How many chats the "Another chat…" search lists at most. */
export const DESTINATION_THREADS_LIMIT = 20;

/**
 * The chats a comment on the record could go to, most recently active first,
 * `q` matching anywhere in a title whatever its case: what "Another chat…"
 * lists.
 */
export async function listDestinationThreads(
  artifactId: string,
  q: string,
  limit = DESTINATION_THREADS_LIMIT,
): Promise<DestinationThread[]> {
  const params: Record<string, string | number> = { limit };
  if (q) params.q = q;
  const { data } = await api.get<{ threads: DestinationThread[] }>(`${artifactPath(artifactId)}/destination-threads`, { params });
  return data.threads;
}

/**
 * The thread, if comments on the record may still go there: a chat picked
 * through "Another chat…" for a draft kept since. Null where it may not.
 */
export async function findDestinationThread(artifactId: string, threadId: string): Promise<DestinationThread | null> {
  const { data } = await api.get<{ threads: DestinationThread[] }>(`${artifactPath(artifactId)}/destination-threads`, {
    params: { thread: threadId, limit: 1 },
  });
  return data.threads.find((th) => th.thread_id === threadId) ?? null;
}

// --- Publish and versions ---

/**
 * The live file against the latest version, and what a publish now would
 * carry, with what the page reported loading added to what a scan of its
 * source finds. Reads the computer (or the stored copy when it is stopped):
 * 404 `file_missing`, 422 over a cap, 503 when it cannot be read now.
 */
export async function getArtifactStatus(artifactId: string, loads: PageLoads): Promise<ArtifactStatus> {
  const { data } = await api.post<ArtifactStatus>(`${artifactPath(artifactId)}/status`, {
    loaded: loads.now,
    earlier: loads.earlier,
  });
  return data;
}

/**
 * Share and cut a version from the reviewed list. 409 `files_changed` carries
 * the new list; an unchanged file cuts nothing (`cut: false`).
 */
export async function publishArtifact(artifactId: string, body: PublishArtifactRequest): Promise<PublishArtifactResponse> {
  const { data } = await api.post<Omit<PublishArtifactResponse, 'artifact'> & { artifact: Wire<Artifact> }>(
    `${artifactPath(artifactId)}/publish`, body,
  );
  return { ...data, artifact: asRecord(data.artifact) };
}

/** Stop sharing; every version is kept and sharing again resumes at the same link. */
/** Who the link opens to; on a shared link it holds from every visitor's next request. */
export async function setArtifactAudience(artifactId: string, audience: ShareAudience): Promise<Artifact> {
  const { data } = await api.put<Wire<Artifact>>(`${artifactPath(artifactId)}/audience`, { audience });
  return asRecord(data);
}

export async function unshareArtifact(artifactId: string): Promise<Artifact> {
  const { data } = await api.post<Wire<Artifact>>(`${artifactPath(artifactId)}/unshare`);
  return asRecord(data);
}

/** Newest first. */
export async function listArtifactVersions(artifactId: string): Promise<ArtifactVersion[]> {
  const { data } = await api.get<{ versions: ArtifactVersion[] }>(`${artifactPath(artifactId)}/versions`);
  return data.versions;
}

/** Where the owner frames a published version from (read only). */
export async function createArtifactGrant(artifactId: string): Promise<ArtifactGrant> {
  const { data } = await api.post<ArtifactGrant>(`${artifactPath(artifactId)}/grant`);
  return data;
}

// --- Comments ---

const commentsPath = (artifactId: string) => `${artifactPath(artifactId)}/comments`;
const commentPath = (artifactId: string, commentId: string) =>
  `${commentsPath(artifactId)}/${encodeURIComponent(commentId)}`;

/** Every comment on the file, by pin number, each with its messages. */
export async function listArtifactComments(artifactId: string): Promise<ArtifactComment[]> {
  const { data } = await api.get<{ comments: Wire<ArtifactComment>[] }>(commentsPath(artifactId));
  return data.comments.map(asComment);
}

/** A new comment on the live file; it waits as a note or in a thread's queue until a send carries it. */
export async function createArtifactComment(
  artifactId: string,
  body: CreateArtifactCommentRequest,
): Promise<ArtifactComment> {
  const { data } = await api.post<Wire<ArtifactComment>>(commentsPath(artifactId), body);
  return asComment(data);
}

/** The owner writes again on a comment, which reopens it if resolved. */
export async function addArtifactCommentMessage(
  artifactId: string,
  commentId: string,
  body: AddArtifactCommentMessageRequest,
): Promise<ArtifactComment> {
  const { data } = await api.post<Wire<ArtifactComment>>(`${commentPath(artifactId, commentId)}/messages`, body);
  return asComment(data);
}

/**
 * Rewrite the owner's words no send has taken yet, in a note or a queued
 * comment; the anchor stays. 409 `comment_sent` once a send has them.
 */
export async function editArtifactCommentMessage(
  artifactId: string,
  commentId: string,
  messageId: string,
  body: string,
): Promise<ArtifactComment> {
  const { data } = await api.patch<Wire<ArtifactComment>>(`${commentPath(artifactId, commentId)}/messages/${messageId}`, { body });
  return asComment(data);
}

/**
 * Queue a comment (for a thread), hold it back as a note, move it to another
 * thread's queue, resolve or reopen it. 409 `comment_sent`, `comment_resolved`,
 * `destination_required`, `destination_unavailable`.
 */
export async function updateArtifactComment(
  artifactId: string,
  commentId: string,
  body: UpdateArtifactCommentRequest,
): Promise<ArtifactComment> {
  const { data } = await api.patch<Wire<ArtifactComment>>(commentPath(artifactId, commentId), body);
  return asComment(data);
}

export async function deleteArtifactComment(artifactId: string, commentId: string): Promise<void> {
  await api.delete(commentPath(artifactId, commentId));
}

/**
 * The owner has read a comment up to message `through`, the newest one the
 * view displayed: a reply that landed after it stays unread.
 */
export async function markArtifactCommentRead(
  artifactId: string,
  commentId: string,
  through: number,
): Promise<ArtifactComment> {
  const { data } = await api.post<Wire<ArtifactComment>>(`${commentPath(artifactId, commentId)}/read`, { through });
  return asComment(data);
}

/** The owner has read every comment on the file; answers with its comments. */
export async function markArtifactCommentsRead(artifactId: string): Promise<ArtifactComment[]> {
  const { data } = await api.post<{ comments: Wire<ArtifactComment>[] }>(`${commentsPath(artifactId)}/read`);
  return data.comments.map(asComment);
}

/** What the thread's next send carries: the comments queued for it, on any artifact. */
export async function listQueuedArtifactComments(threadId: string): Promise<QueuedArtifactComment[]> {
  const { data } = await api.get<{ comments: Wire<QueuedArtifactComment>[] }>(
    `/api/v1/artifacts/threads/${encodeURIComponent(threadId)}/queued-comments`,
  );
  return data.comments.map((c) => kinded<QueuedArtifactComment>(c));
}

// --- Sending ---

/** A thread the comments go to, or the workspace a new thread for them is made in. */
export type CommentTarget = { threadId: string } | { newThreadIn: string };

/**
 * A send that did not go, with the `code` its answer named
 * (`comments_unavailable`, a busy state, ...) and its `reason` when the words
 * were written for the reader (the server's, or the chat's own); null when
 * all that is known is that it failed.
 */
export class CommentSendRefused extends Error {
  readonly code: string | null;
  readonly reason: string | null;

  constructor(reason: string | null, code: string | null = null) {
    const said = reason?.trim() ? reason : null;
    super(said ?? 'The comments were not sent.');
    this.name = 'CommentSendRefused';
    this.code = code;
    this.reason = said;
  }
}

/**
 * Send comments to a thread no chat on screen is reading, or to a new one,
 * and resolve with the thread they went to. `reading` says how much the agent
 * answers in that thread and how much under each comment.
 *
 * The stream is read only until the server has the send: a steer's
 * acceptance, or the run's first frame, from which the run goes on detached
 * from this connection. Leaving before then would cancel the send while the
 * computer starts, so a stopped computer keeps this pending until it is up.
 * Posts go through `postComments` (`artifactComments/commentPosts.ts`), which
 * keeps the page from leaving meanwhile.
 */
export async function postArtifactComments(
  commentIds: readonly string[],
  to: CommentTarget,
  { reading, locale, timezone }: { reading: ArtifactReading; locale?: string; timezone?: string },
): Promise<{ threadId: string }> {
  const controller = new AbortController();
  let threadId = 'threadId' in to ? to.threadId : null;
  let taken = false;
  let refusal: Record<string, unknown> | null = null;
  try {
    await sendChatMessageStream('', 'newThreadIn' in to ? to.newThreadIn : '', threadId, {
      onEvent: (event) => {
        if (taken || refusal) return;
        // The run failed before its workflow began and stays on the thread:
        // it holds the comments, and its Retry carries them on.
        if ((event.event === 'error' || event.event === 'retry') && event.recovery === 'retry') {
          taken = true;
          controller.abort();
          return;
        }
        if (event.event === 'error') {
          refusal = event;
          return;
        }
        if (event.event === 'metadata' || event.event === 'steering_accepted') {
          if (typeof event.thread_id === 'string' && event.thread_id) threadId = event.thread_id;
          taken = true;
          controller.abort();
        }
      },
      onRunIdResolved: (_runId, headerThreadId) => {
        if (headerThreadId) threadId = headerThreadId;
      },
      locale: locale ?? (i18n.language || navigator.language || 'en-US'),
      timezone: timezone ?? deviceTimezone(),
      requestKey: randomUUID(),
      signal: controller.signal,
      artifactComments: { ids: [...commentIds], reading },
    });
  } catch (err) {
    const info = (err as { errorInfo?: { code?: unknown; thread_id?: unknown } }).errorInfo;
    // A copy of this send reached the server first: it has the comments.
    if (info?.code === 'duplicate_request') {
      const copyThread = typeof info.thread_id === 'string' ? info.thread_id : threadId;
      if (copyThread) return { threadId: copyThread };
    }
    const { reason } = err as StreamRequestError;
    throw new CommentSendRefused(reason ?? null, typeof info?.code === 'string' ? info.code : null);
  }
  const refused = refusal as Record<string, unknown> | null;
  if (refused) {
    throw new CommentSendRefused(
      typeof refused.error === 'string' ? refused.error : null,
      typeof refused.code === 'string' ? refused.code : null,
    );
  }
  if (!taken || !threadId) throw new CommentSendRefused(null);
  return { threadId };
}
