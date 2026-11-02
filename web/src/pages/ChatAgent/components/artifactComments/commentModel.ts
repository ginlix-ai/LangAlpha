import type { TFunction } from 'i18next';
import type { ArtifactComment, ArtifactCommentAnchor, ArtifactCommentMessage } from '@/types/api';
import type { SendLook } from './commentSendStore';

/**
 * How a pin reads at a glance. `sent` is the only one in the annotation
 * colour: the agent has it. `queued` waits for its thread's next send, `note`
 * stays with the owner, `resolved` is done whatever its delivery.
 */
export type PinState = 'sent' | 'queued' | 'note' | 'resolved';

export function pinState(comment: Pick<ArtifactComment, 'status' | 'delivery'>): PinState {
  return comment.status === 'resolved' ? 'resolved' : comment.delivery;
}

/**
 * Whether the agent is on it: the comment reached a turn that is still
 * running, and nothing it said came after the owner last wrote. A resolved
 * comment is done, whatever runs.
 */
export function isWorking(comment: Pick<ArtifactComment, 'status' | 'delivery' | 'messages'>, running: boolean): boolean {
  if (!running || comment.status === 'resolved' || comment.delivery !== 'sent') return false;
  const last = comment.messages[comment.messages.length - 1];
  return last?.author !== 'agent';
}

/** Which comments the list shows. */
export type ListFilter = 'open' | 'unread' | 'resolved' | 'all';

export const LIST_FILTERS: readonly ListFilter[] = ['open', 'unread', 'resolved', 'all'];

type Listed = Pick<ArtifactComment, 'status' | 'unread'>;

/**
 * Open holds a resolved comment while it has a reply the owner has not read,
 * so an answer the agent closed the thread with is still found where the
 * owner looks; once read it goes as resolved comments do.
 */
export function inFilter(comment: Listed, filter: ListFilter): boolean {
  switch (filter) {
    case 'open':
      return comment.status === 'open' || comment.unread > 0;
    case 'unread':
      return comment.unread > 0;
    case 'resolved':
      return comment.status === 'resolved';
    default:
      return true;
  }
}

/** How many comments each filter shows, the counts its menu lists. */
export function filterCounts(comments: readonly Listed[]): Record<ListFilter, number> {
  const counts: Record<ListFilter, number> = { open: 0, unread: 0, resolved: 0, all: 0 };
  for (const c of comments) for (const f of LIST_FILTERS) if (inFilter(c, f)) counts[f] += 1;
  return counts;
}

/** What the Open list holds, the toolbar's count. */
export function openComments<T extends Listed>(comments: readonly T[]): T[] {
  return comments.filter((c) => inFilter(c, 'open'));
}

/** The filter that shows a comment opened from elsewhere: All keeps showing everything. */
export function filterShowing(comment: Listed, filter: ListFilter): ListFilter {
  return inFilter(comment, filter) ? filter : comment.status;
}

/** A comment being written waits among the open ones, so only Open and All show it. */
export function showsDraft(filter: ListFilter): boolean {
  return filter === 'open' || filter === 'all';
}

/** The agent's newest message the owner has not read, what a shut card previews. */
export function newestUnread(comment: Pick<ArtifactComment, 'messages' | 'read_through'>): ArtifactCommentMessage | null {
  let newest: ArtifactCommentMessage | null = null;
  for (const m of comment.messages) {
    if (m.author === 'agent' && m.seq > comment.read_through && (!newest || m.seq > newest.seq)) newest = m;
  }
  return newest;
}

/** What a card's ⋯ menu offers. */
export type CardAction = 'keepNote' | 'sendNow' | 'addToQueue' | 'reopen' | 'delete';

/**
 * The moves that fit where a comment is. Only words the agent has not had can
 * wait, so a sent comment offers none, and a resolved one is reopened before
 * it goes anywhere (the server holds both rules too). A send goes once its
 * destination is known, to a new thread if no other takes it; a queue needs a
 * thread to wait for. A queued card has Send now on its face, so the menu
 * leaves it out.
 */
export function cardActions(state: PinState, { canSend, canQueue }: { canSend: boolean; canQueue: boolean }): CardAction[] {
  switch (state) {
    case 'resolved':
      return ['reopen', 'delete'];
    case 'queued':
      return ['keepNote', 'delete'];
    case 'note': {
      const moves: CardAction[] = [];
      if (canSend) moves.push('sendNow');
      if (canQueue) moves.push('addToQueue');
      return [...moves, 'delete'];
    }
    default:
      return ['delete'];
  }
}

/** The agent has had the owner's words at least once, which the card marks. */
export function reachedAgent(comment: Pick<ArtifactComment, 'delivery' | 'messages'>): boolean {
  return comment.delivery === 'sent' || comment.messages.some((m) => m.author === 'user' && m.sent_at !== null);
}

/** How a comment reads, on its pin and its card alike. */
export interface CommentLook {
  state: PinState;
  /** The agent is on it: the number gives way to the live glyph until it answers. */
  working: boolean;
  /** The agent has had the owner's words at least once, which the card marks. */
  reached: boolean;
  /** The agent has what it was asked, so the owner can answer it; a reply to a resolved one reopens it. */
  canReply: boolean;
  pending: boolean;
  refusal: string | null;
}

/**
 * A comment as its pin and card draw it. One the owner just sent reads as
 * sent while the send settles: the server flips it on
 * the same request, and a pin that showed the old state for that moment would
 * read as refused.
 */
export function describeComment(
  comment: Pick<ArtifactComment, 'status' | 'delivery' | 'messages'>,
  send: SendLook,
  running: boolean,
): CommentLook {
  const seen = send.sending ? { ...comment, delivery: 'sent' as const } : comment;
  return {
    state: pinState(seen),
    working: isWorking(seen, running),
    reached: reachedAgent(seen),
    canReply: seen.delivery === 'sent',
    pending: send.pending,
    refusal: send.refusal,
  };
}

/** Everything said after the owner's first words: the agent's answers and the owner's follow-ups. */
export function laterMessages<T extends Pick<ArtifactComment, 'messages'>>(comment: T): T['messages'] {
  const first = comment.messages.findIndex((m) => m.author === 'user');
  return first < 0 ? comment.messages : comment.messages.filter((_, i) => i !== first);
}

/** The owner's first words in a comment, what a list row shows of it. */
export function firstWords(comment: Pick<ArtifactComment, 'messages'>): string {
  return comment.messages.find((m) => m.author === 'user')?.body ?? '';
}

/** Text squashed to one line and cut to `limit`, for a label or a quote in a row. */
export function oneLine(text: string | undefined, limit: number): string {
  const line = (text ?? '').split(/\s+/).filter(Boolean).join(' ');
  return line.length > limit ? `${line.slice(0, limit - 1).trimEnd()}…` : line;
}

/**
 * A new comment's spot as its box names it, and what it holds under the
 * name. Words are named by the place they are in (the tile's label, else the
 * nearest heading), as an element is by its own name. Selected words are
 * quoted, since they are what the owner means, and an area's parts say what
 * it took; an element's name already says what it is.
 */
export function draftHeading(t: TFunction, anchor: ArtifactCommentAnchor): { label: string; quote: string | null } {
  if (anchor.kind === 'area') return { label: areaTitle(t, anchor), quote: anchor.label ?? null };
  const label = anchor.label
    ? t('pageViewer.comments.onLabel', { label: anchor.label })
    : anchor.kind === 'text'
      ? t('pageViewer.comments.onText')
      : t('pageViewer.comments.onElement');
  const quote = anchor.kind === 'text' && anchor.quote ? t('chat.artifactComments.message.quote', { quote: anchor.quote }) : null;
  return { label, quote };
}

/** An area by its size, the composer's header: "Selected area · 3 elements". */
export function areaTitle(t: TFunction, anchor: Pick<ArtifactCommentAnchor, 'count' | 'elements'>): string {
  return t('pageViewer.comments.areaTitle', { count: anchor.count ?? anchor.elements?.length ?? 0 });
}

/**
 * Where a comment is, as its card's spot line says it: the words it quotes
 * for a selection, else the anchor's label, which for an area names its parts.
 */
export function spotPhrase(t: TFunction, anchor: ArtifactCommentAnchor): string {
  if (anchor.kind === 'text') {
    return anchor.quote ? t('pageViewer.comments.onQuote', { quote: oneLine(anchor.quote, 36) }) : t('pageViewer.comments.onText');
  }
  if (anchor.label) return t('pageViewer.comments.onLabel', { label: oneLine(anchor.label, 60) });
  if (anchor.kind === 'area') return areaTitle(t, anchor);
  return t('pageViewer.comments.onElement');
}
