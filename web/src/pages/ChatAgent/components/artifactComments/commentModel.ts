import type { ArtifactComment, ArtifactCommentMessage } from '@/types/api';
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

/** Whether a person's message is the viewer's own: one naming no writer, as
 *  all are in a personal workspace, or naming them in a shared one. */
export function wroteIt(message: Pick<ArtifactCommentMessage, 'written_by'> | undefined, me: string | null): boolean {
  return !message?.written_by || message.written_by === me;
}

/**
 * The words a card may still change: the newest a person wrote that no send
 * has taken, on a note or a queued comment, where the viewer wrote them (the
 * server changes a message for its writer alone). A sent comment's the agent
 * has read.
 */
export function editableMessage(
  comment: Pick<ArtifactComment, 'delivery' | 'messages'>,
  me: string | null,
): ArtifactCommentMessage | null {
  if (comment.delivery === 'sent') return null;
  for (let i = comment.messages.length - 1; i >= 0; i--) {
    const m = comment.messages[i];
    if (m.author === 'user') return m.sent_at == null && m.request_key == null && wroteIt(m, me) ? m : null;
  }
  return null;
}

/** What a card's ⋯ menu offers. */
export type CardAction = 'edit' | 'keepNote' | 'sendNow' | 'addToQueue' | 'reopen' | 'delete';

/**
 * The moves that fit where a comment is. Only words the agent has not had can
 * wait, so a sent comment offers none, and a resolved one is reopened before
 * it goes anywhere (the server holds both rules too). A send goes once its
 * destination is known, to a new thread if no other takes it; a queue needs a
 * thread to wait for. A queued card has Send now on its face, so the menu
 * leaves it out.
 */
export function cardActions(
  state: PinState,
  { canSend, canQueue, canEdit }: { canSend: boolean; canQueue: boolean; canEdit: boolean },
): CardAction[] {
  const edit: CardAction[] = canEdit ? ['edit'] : [];
  switch (state) {
    case 'resolved':
      return ['reopen', 'delete'];
    case 'queued':
      return [...edit, 'keepNote', 'delete'];
    case 'note': {
      const moves: CardAction[] = [...edit];
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
  /**
   * The thread it went to ended its last run in an error and waits for a
   * Retry there, with the agent's answer still owed: the card says so and
   * the pin asks for attention.
   */
  needsRetry: boolean;
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
  /** The last run on the thread it went to failed. */
  failed: boolean,
): CommentLook {
  const seen = send.sending ? { ...comment, delivery: 'sent' as const } : comment;
  return {
    state: pinState(seen),
    working: isWorking(seen, running),
    reached: reachedAgent(seen),
    canReply: seen.delivery === 'sent',
    pending: send.pending,
    refusal: send.refusal,
    // Owed an answer as a running turn would be, by a run that stopped; a
    // send of it on its way is a new run, which the failure is not about.
    needsRetry: !running && !send.sending && isWorking(seen, failed),
  };
}

/** The pin and the card's badge ask for attention: an answer unread, or one owed by a run that stopped. */
export function asksAttention(comment: Pick<ArtifactComment, 'unread'>, look: Pick<CommentLook, 'needsRetry'>): boolean {
  return comment.unread > 0 || look.needsRetry;
}

/** What a card offers to do with its comment, decided once from how it reads. */
export interface CardMoves {
  /** The ✓ and the ⋯ menu's moves; null where the card only reads, which offers neither. */
  menu: CardAction[] | null;
  /** Send now on a queued card's face: ready, held while where it goes is not known, or not offered. */
  sendNow: 'ready' | 'waiting' | null;
  /** A reply box under the open card. */
  reply: boolean;
}

/**
 * The moves a card offers: none where it only reads (an earlier version's
 * comments), else the menu's (`cardActions`), Send now and a reply. Words a
 * send is taking are not offered to edit.
 */
export function cardMoves(
  look: Pick<CommentLook, 'state' | 'pending' | 'canReply'>,
  { writes, canSend, canQueue, canEdit }: { writes: boolean; canSend: boolean; canQueue: boolean; canEdit: boolean },
): CardMoves {
  if (!writes) return { menu: null, sendNow: null, reply: false };
  return {
    menu: cardActions(look.state, { canSend, canQueue, canEdit: canEdit && !look.pending }),
    sendNow: canSend ? 'ready' : 'waiting',
    reply: look.canReply,
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
