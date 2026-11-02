/**
 * A chat send that carries file comments, as the chat shows it. The server
 * keeps the same shape on the turn (`artifact_comments` on the query row and
 * the steer events), so a view's own send, a steer's acceptance and a replay
 * all draw one bubble.
 */
import type {
  ArtifactComment,
  ArtifactCommentsShown,
  ArtifactReading,
  CommentSend,
  ShownArtifactComment,
} from '@/types/api';
import { oneLine } from './commentModel';
import { basename } from '../../utils/fileRefResolver';

// The server's clip lengths, so the bubble a send draws before the server
// answers reads as the replay will.
const SHOWN_CHARS = 500;
const SHOWN_ANCHOR_CHARS = 200;

function clip(text: string, limit: number): string {
  return text.length <= limit ? text : `${text.slice(0, limit - 1).trimEnd()}…`;
}

function anchorText(value: string | undefined): string | null {
  return oneLine(value, SHOWN_ANCHOR_CHARS) || null;
}

/** The last segment of a workspace path, what a file is called where it has no title. */
export function fileNameOf(path: string): string {
  return basename(path) || path;
}

/** What a comment's file is called: the document's title when it had one, else its name. */
export function commentFileTitle(comment: Pick<ShownArtifactComment, 'title' | 'path'>): string {
  return comment.title?.trim() || fileNameOf(comment.path);
}

/** The owner's words a send of `comment` delivers: those not sent yet. */
export function unsentWords(comment: Pick<ArtifactComment, 'messages'>): string[] {
  return comment.messages.filter((m) => m.author === 'user' && m.sent_at == null).map((m) => m.body);
}

/** The owner's newest words in a comment, sent or not. */
export function latestOwnerWords(comment: Pick<ArtifactComment, 'messages'>): string {
  for (let i = comment.messages.length - 1; i >= 0; i--) {
    const m = comment.messages[i];
    if (m.author === 'user') return m.body;
  }
  return '';
}

/** A comment with the workspace its file is in, which a send's bubble names. */
export type CommentInWorkspace = ArtifactComment & { workspace_id: string };

/** What a send of `comments` with `note` will show, built before the server
 *  answers with its own. */
export function shownFromQueued(
  note: string,
  reading: ArtifactReading,
  comments: readonly CommentInWorkspace[],
): ArtifactCommentsShown {
  return {
    note,
    reading,
    comments: comments.map((c) => {
      const delivered = unsentWords(c);
      return {
        comment_id: c.comment_id,
        artifact_id: c.artifact_id,
        workspace_id: c.workspace_id,
        path: c.path,
        title: c.title,
        number: c.number,
        text: clip(delivered.join('\n'), SHOWN_CHARS),
        follow_up: c.messages.length > delivered.length,
        label: anchorText(c.anchor.label),
        quote: anchorText(c.anchor.quote),
      };
    }),
  };
}

const str = (v: unknown): string | null => (typeof v === 'string' ? v : null);

/** What one send may carry (`MAX_ARTIFACT_COMMENTS_PER_SEND`, `models/chat.py`); the rest wait for the next. */
export const MAX_COMMENTS_PER_SEND = 50;

/** A turn's or a delivery's `artifact_comments`, or null for anything else. Read
 *  loosely: a row written before a field existed still draws. */
export function readArtifactCommentsShown(raw: unknown): ArtifactCommentsShown | null {
  if (!raw || typeof raw !== 'object') return null;
  const { note, reading, comments } = raw as { note?: unknown; reading?: unknown; comments?: unknown };
  if (!Array.isArray(comments)) return null;
  const shown: ShownArtifactComment[] = [];
  for (const item of comments) {
    if (!item || typeof item !== 'object') continue;
    const c = item as Record<string, unknown>;
    const commentId = str(c.comment_id);
    const artifactId = str(c.artifact_id);
    if (!commentId || !artifactId || typeof c.number !== 'number') continue;
    shown.push({
      comment_id: commentId,
      artifact_id: artifactId,
      workspace_id: str(c.workspace_id) ?? '',
      path: str(c.path) ?? '',
      title: str(c.title),
      number: c.number,
      text: str(c.text) ?? '',
      follow_up: c.follow_up === true,
      label: str(c.label),
      quote: str(c.quote),
    });
  }
  if (shown.length === 0) return null;
  return { note: str(note) ?? '', reading: reading === 'file' ? 'file' : 'chat', comments: shown };
}

/** The comments a send shows, as its request carries them; undefined for a send of none. */
export function commentSendOf(shown: ArtifactCommentsShown | null | undefined): CommentSend | undefined {
  if (!shown || shown.comments.length === 0) return undefined;
  return { ids: shown.comments.map((c) => c.comment_id), reading: shown.reading };
}

/**
 * The comments a send carries as one key, whatever their order: what tells
 * two sends of one note apart, for the request key and for a replay meeting
 * the copy on screen alike. Empty for a send of none.
 */
export function commentSendKey(send: Pick<CommentSend, 'ids'> | null | undefined): string {
  return send ? [...send.ids].sort().join(',') : '';
}

/** Comments grouped by file, in the order the send shows them. */
export function commentsByFile<T extends { artifact_id: string; path: string; title: string | null }>(
  comments: readonly T[],
): { artifactId: string; title: string; comments: T[] }[] {
  const groups: { artifactId: string; title: string; comments: T[] }[] = [];
  for (const c of comments) {
    const group = groups.find((g) => g.artifactId === c.artifact_id);
    if (group) group.comments.push(c);
    else groups.push({ artifactId: c.artifact_id, title: commentFileTitle(c), comments: [c] });
  }
  return groups;
}

/**
 * The text a turn's opener is matched by when a replay meets the copy a view
 * already shows. A comment send's stored query is the block the agent read,
 * while the view's copy holds the note, so both sides go by the comment ids and
 * the note, marked apart from a plain message of the same words.
 */
export function openerMatchText(content: string, shown: ArtifactCommentsShown | null | undefined): string {
  if (!shown) return content.trim();
  // Two sends can share a note (often none, from a viewer's "Send now"), so
  // the comments they carry tell them apart.
  return `\u0000file-comments\u0000${commentSendKey(commentSendOf(shown))}\u0000${shown.note.trim()}`;
}
