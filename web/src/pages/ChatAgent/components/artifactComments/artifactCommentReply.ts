/**
 * What `reply_to_comment` leaves on its result (its `artifact_comment` tool
 * artifact, the snake-case fields of the same names): the comment answered,
 * whether it ended resolved, and the start of the answer, clipped to 1000
 * characters at a word. One reader for the transcript line and the click, so
 * the two cannot disagree.
 */
export interface ArtifactCommentReply {
  artifactId: string;
  workspaceId: string;
  /** The file, workspace-relative. */
  path: string;
  commentId: string;
  number: number;
  resolved: boolean;
  /** The answer, clipped at a word; the whole of it is under the comment. */
  text: string;
  clipped: boolean;
}

/** A comment on a file, as far as opening the file on it needs. */
export interface ArtifactCommentRef {
  artifactId: string;
  workspaceId: string;
  /** Workspace-relative. */
  path: string;
  commentId: string;
}

/** The reply a tool artifact names, or null when it is not one. */
export function readArtifactCommentReply(artifact: unknown): ArtifactCommentReply | null {
  if (typeof artifact !== 'object' || artifact === null) return null;
  const a = artifact as Record<string, unknown>;
  if (a.type !== 'artifact_comment') return null;
  const { artifact_id: artifactId, comment_id: commentId, number } = a;
  if (typeof artifactId !== 'string' || !artifactId || typeof commentId !== 'string' || !commentId) return null;
  if (typeof number !== 'number' || !Number.isInteger(number)) return null;
  return {
    artifactId,
    workspaceId: typeof a.workspace_id === 'string' ? a.workspace_id : '',
    path: typeof a.path === 'string' ? a.path : '',
    commentId,
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
