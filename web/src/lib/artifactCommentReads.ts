/**
 * How far the owner has read a file's comments. The server keeps a cursor per
 * comment, `read_through`, the `seq` of the newest message they have seen;
 * the agent's messages past it are unread, and the owner's own never are.
 * These are the reads a view draws from, and the moves the cache makes before
 * the server answers, held to the server's rules: a cursor only moves forward
 * and never past the comment's latest message.
 */
import type { ArtifactComment, ArtifactCommentMessage } from '@/types/api';

type Thread = { messages: readonly Pick<ArtifactCommentMessage, 'seq' | 'author'>[] };

/** The `seq` of a comment's newest message, or its newest by `author`; 0 for none. */
export function latestSeq(comment: Thread, author?: ArtifactCommentMessage['author']): number {
  return comment.messages.reduce((max, m) => (author && m.author !== author ? max : Math.max(max, m.seq)), 0);
}

/** The agent's messages past `readThrough`, what `unread` counts. */
export function unreadPast(comment: Thread, readThrough: number): number {
  return comment.messages.filter((m) => m.author === 'agent' && m.seq > readThrough).length;
}

/**
 * `comment` read through message `through`, as the server will answer: the
 * cursor kept where it is if it is already further, and never past the
 * latest message. A reply the view had not displayed stays unread.
 */
export function readThrough<T extends Thread & Pick<ArtifactComment, 'read_through' | 'unread'>>(comment: T, through: number): T {
  const next = Math.max(comment.read_through, Math.min(through, latestSeq(comment)));
  if (next === comment.read_through) return comment;
  return { ...comment, read_through: next, unread: unreadPast(comment, next) };
}

/** Every comment read through its latest message. */
export function readAll<T extends Thread & Pick<ArtifactComment, 'read_through' | 'unread'>>(comments: readonly T[]): T[] {
  return comments.map((c) => readThrough(c, latestSeq(c)));
}

/**
 * A file's unread counts comments, not messages: a comment is what the owner
 * opens, and opening it clears it. Each comment's own `unread` counts messages.
 */
export interface ArtifactUnread {
  /** The comments holding an agent message the owner has not read: the record's `unread`, and `commentIds.length`. */
  count: number;
  /** Those comments in the order given (the list's, by pin number), so the first unread is `commentIds[0]`. */
  commentIds: string[];
}

export function artifactUnread(comments: readonly Pick<ArtifactComment, 'comment_id' | 'unread'>[]): ArtifactUnread {
  const commentIds = comments.filter((c) => c.unread > 0).map((c) => c.comment_id);
  return { count: commentIds.length, commentIds };
}

/** How many comments hold something unread, for moving the record's count by what a read cleared. */
export function unreadComments(comments: readonly Pick<ArtifactComment, 'comment_id' | 'unread'>[]): number {
  return artifactUnread(comments).count;
}
