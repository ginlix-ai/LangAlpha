import { kindOf } from '@/lib/artifactTarget';
import type { ArtifactKind } from '@/types/api';

/**
 * A comment on a file, a chart or a Drive file, as far as opening it on the
 * comment needs. Read once off whatever names it, so nothing downstream has
 * to tell a Drive path from an entry id.
 */
export type ArtifactCommentRef = {
  artifactId: string;
  workspaceId: string;
  commentId: string;
} & (
  /** A file by its workspace-relative path; a chart by its id. */
  | { kind: 'file' | 'chart'; path: string }
  /**
   * A Drive file by its entry id, which finds it wherever it has moved, and
   * the path it had where the payload names one. A payload with no entry id
   * leaves only that path to open.
   */
  | { kind: 'drive'; entryId: string | null; path: string | null }
);

/**
 * What `reply_to_comment` leaves on its result (its `artifact_comment` tool
 * artifact, the snake-case fields of the same names): the comment answered,
 * whether it ended resolved, and the start of the answer, clipped to 1000
 * characters at a word. One reader for the transcript line and the click, so
 * the two cannot disagree.
 */
export type ArtifactCommentReply = ArtifactCommentRef & {
  number: number;
  resolved: boolean;
  /** The answer, clipped at a word; the whole of it is under the comment. */
  text: string;
  clipped: boolean;
};

/** A comment row as the wire names it: on a Drive file, `path` is where it is now and `entry_id` the file. */
export interface WireCommentPlace {
  artifact_id: string;
  workspace_id: string;
  comment_id: string;
  kind: ArtifactKind;
  path: string;
  entry_id?: string | null;
}

/** A listed comment (a send's chip, a queued row) as a ref. */
export function commentRef(c: WireCommentPlace): ArtifactCommentRef {
  const on = { artifactId: c.artifact_id, workspaceId: c.workspace_id, commentId: c.comment_id };
  return c.kind === 'drive'
    ? { ...on, kind: 'drive', entryId: c.entry_id || null, path: c.path || null }
    : { ...on, kind: c.kind, path: c.path };
}

/** How a view opens a comment, one opener per kind; a kind it has no place to open is left out. */
export type ArtifactCommentOpeners = Partial<Record<ArtifactKind, (comment: ArtifactCommentRef) => void>>;

/** The reply a tool artifact names, or null when it is not one. */
export function readArtifactCommentReply(artifact: unknown): ArtifactCommentReply | null {
  if (typeof artifact !== 'object' || artifact === null) return null;
  const a = artifact as Record<string, unknown>;
  if (a.type !== 'artifact_comment') return null;
  const { artifact_id: artifactId, comment_id: commentId, number } = a;
  if (typeof artifactId !== 'string' || !artifactId || typeof commentId !== 'string' || !commentId) return null;
  if (typeof number !== 'number' || !Number.isInteger(number)) return null;
  const on = { artifactId, workspaceId: typeof a.workspace_id === 'string' ? a.workspace_id : '', commentId };
  const named = typeof a.path === 'string' ? a.path : '';
  const kind = kindOf(a);
  // A reply names a Drive file by its record's key, its entry id, and no path.
  const place: ArtifactCommentRef = kind === 'drive'
    ? { ...on, kind, entryId: named || null, path: null }
    : { ...on, kind, path: named };
  return {
    ...place,
    number,
    resolved: a.resolved === true,
    text: typeof a.text === 'string' ? a.text : '',
    clipped: a.clipped === true,
  };
}

/** Consecutive replies to one file, which one line says together so the file
 *  is named once, resolved or not. A comment answered twice is listed once,
 *  where it was first, as its latest reply left it: "comments 2 and 2" would
 *  read as a slip. */
export function groupCommentReplies<T extends { reply: ArtifactCommentReply }>(members: readonly T[]): T[][] {
  const groups: T[][] = [];
  for (const member of members) {
    const last = groups[groups.length - 1];
    if (!last || last[0].reply.artifactId !== member.reply.artifactId) {
      groups.push([member]);
      continue;
    }
    const again = last.findIndex((m) => m.reply.commentId === member.reply.commentId);
    if (again < 0) last.push(member);
    else last[again] = { ...last[again], reply: member.reply };
  }
  return groups;
}
