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
import { deviceTimezone } from '@/lib/deviceTimezone';
import { randomUUID } from '@/lib/randomUUID';
import type {
  AddArtifactCommentMessageRequest,
  Artifact,
  ArtifactComment,
  ArtifactDestinations,
  ArtifactGrant,
  ArtifactLookup,
  ArtifactReading,
  ArtifactStatus,
  ArtifactVersion,
  CreateArtifactCommentRequest,
  PublishArtifactRequest,
  PublishArtifactResponse,
  QueuedArtifactComment,
  UpdateArtifactCommentRequest,
} from '@/types/api';
import { sendChatMessageStream } from './messages';

const artifactPath = (artifactId: string) => `/api/v1/artifacts/${encodeURIComponent(artifactId)}`;
const workspaceArtifactsPath = (workspaceId: string) =>
  `/api/v1/workspaces/${encodeURIComponent(workspaceId)}/artifacts`;

// --- Records ---

/** The file's record if it has one; never makes it, so a tab can ask for any HTML file. */
export async function findArtifact(workspaceId: string, path: string): Promise<Artifact | null> {
  const { data } = await api.get<ArtifactLookup>(workspaceArtifactsPath(workspaceId), { params: { path } });
  return data.artifact;
}

/** The file's record, made private if new. 400 for a path that may not be shared. */
export async function ensureArtifact(workspaceId: string, path: string): Promise<Artifact> {
  const { data } = await api.post<Artifact>(workspaceArtifactsPath(workspaceId), { path });
  return data;
}

/** 404 when it is not the caller's. */
export async function getArtifact(artifactId: string): Promise<Artifact> {
  const { data } = await api.get<Artifact>(artifactPath(artifactId));
  return data;
}

export async function setArtifactKeepUpdated(artifactId: string, keepUpdated: boolean): Promise<Artifact> {
  const { data } = await api.patch<Artifact>(artifactPath(artifactId), { keep_updated: keepUpdated });
  return data;
}

// --- Where comments go ---

/** Send the file's comments to `threadId` until unbound. 409 `destination_unavailable`. */
export async function bindArtifact(artifactId: string, threadId: string): Promise<Artifact> {
  const { data } = await api.put<Artifact>(`${artifactPath(artifactId)}/binding`, { thread_id: threadId });
  return data;
}

export async function unbindArtifact(artifactId: string): Promise<Artifact> {
  const { data } = await api.delete<Artifact>(`${artifactPath(artifactId)}/binding`);
  return data;
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

// --- Publish and versions ---

/**
 * The live file against the latest version, and what a publish now would
 * carry. Reads the computer (or the stored copy when it is stopped): 404
 * `file_missing`, 422 over a cap, 503 when it cannot be read now.
 */
export async function getArtifactStatus(artifactId: string): Promise<ArtifactStatus> {
  const { data } = await api.get<ArtifactStatus>(`${artifactPath(artifactId)}/status`);
  return data;
}

/**
 * Share and cut a version from the reviewed list. 409 `files_changed` carries
 * the new list; an unchanged file cuts nothing (`cut: false`).
 */
export async function publishArtifact(artifactId: string, body: PublishArtifactRequest): Promise<PublishArtifactResponse> {
  const { data } = await api.post<PublishArtifactResponse>(`${artifactPath(artifactId)}/publish`, body);
  return data;
}

/** Stop sharing; every version is kept and sharing again resumes at the same link. */
export async function unshareArtifact(artifactId: string): Promise<Artifact> {
  const { data } = await api.post<Artifact>(`${artifactPath(artifactId)}/unshare`);
  return data;
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
  const { data } = await api.get<{ comments: ArtifactComment[] }>(commentsPath(artifactId));
  return data.comments;
}

/** A new comment on the live file; it waits as a note or in a thread's queue until a send carries it. */
export async function createArtifactComment(
  artifactId: string,
  body: CreateArtifactCommentRequest,
): Promise<ArtifactComment> {
  const { data } = await api.post<ArtifactComment>(commentsPath(artifactId), body);
  return data;
}

/** The owner writes again on a comment, which reopens it if resolved. */
export async function addArtifactCommentMessage(
  artifactId: string,
  commentId: string,
  body: AddArtifactCommentMessageRequest,
): Promise<ArtifactComment> {
  const { data } = await api.post<ArtifactComment>(`${commentPath(artifactId, commentId)}/messages`, body);
  return data;
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
  const { data } = await api.patch<ArtifactComment>(commentPath(artifactId, commentId), body);
  return data;
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
  const { data } = await api.post<ArtifactComment>(`${commentPath(artifactId, commentId)}/read`, { through });
  return data;
}

/** The owner has read every comment on the file; answers with its comments. */
export async function markArtifactCommentsRead(artifactId: string): Promise<ArtifactComment[]> {
  const { data } = await api.post<{ comments: ArtifactComment[] }>(`${commentsPath(artifactId)}/read`);
  return data.comments;
}

/** What the thread's next send carries: the comments queued for it, on any artifact. */
export async function listQueuedArtifactComments(threadId: string): Promise<QueuedArtifactComment[]> {
  const { data } = await api.get<{ comments: QueuedArtifactComment[] }>(
    `/api/v1/artifacts/threads/${encodeURIComponent(threadId)}/queued-comments`,
  );
  return data.comments;
}

// --- Sending ---

/** A thread the comments go to, or the workspace a new thread for them is made in. */
export type CommentTarget = { threadId: string } | { newThreadIn: string };

/** A send the server refused, with the `code` its answer named (`comments_unavailable`, a busy state, ...). */
export class CommentSendRefused extends Error {
  readonly code: string | null;
  readonly status: number | null;

  constructor(message: string, code: string | null, status: number | null) {
    super(message);
    this.name = 'CommentSendRefused';
    this.code = code;
    this.status = status;
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
    const status = (err as { status?: unknown }).status;
    throw new CommentSendRefused(
      err instanceof Error ? err.message : String(err),
      typeof info?.code === 'string' ? info.code : null,
      typeof status === 'number' ? status : null,
    );
  }
  const refused = refusal as Record<string, unknown> | null;
  if (refused) {
    const message = typeof refused.error === 'string' ? refused.error : 'The comments were not sent.';
    throw new CommentSendRefused(message, typeof refused.code === 'string' ? refused.code : null, 409);
  }
  if (!taken || !threadId) throw new CommentSendRefused('The comments were not sent.', null, null);
  return { threadId };
}
