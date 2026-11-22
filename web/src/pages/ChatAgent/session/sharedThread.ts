/**
 * Who sent each turn of a shared workspace's thread, as the view reads it:
 * the caption on each message, which turns a rewrite may start from, which
 * offer a human review, and whose turn is running.
 */
import type { ChatMessage, MessageSender } from '@/types/chat';

/** What a shared workspace's thread needs to say who sent each message.
 *  Absent in every other thread, which then draws no sender at all. */
export interface SharedThreadView {
  me: MessageSender;
  /** The pictures of the people in the workspace, by user id. */
  avatars: Readonly<Record<string, string | null>>;
  /** Their names, by user id, for a message that names its sender by id
   *  alone (a delivered steer). */
  names: Readonly<Record<string, string | null>>;
  /** The latest turn not known to be the viewer's, or -1: an edit,
   *  regenerate or retry from that turn back would replace someone else's
   *  turn, which the server refuses, so none is offered there. */
  lastOthersTurn: number;
  /** The turns the viewer sent. A human review is the sender's to ask for,
   *  which the server holds to, so it is offered on these only. */
  ownTurns: ReadonlySet<number>;
}

/** Who sent a user message, as its caption draws them. */
export interface BubbleSender {
  userId: string;
  name: string | null;
  avatarUrl: string | null;
  mine: boolean;
}

/** What a message in a shared workspace's thread draws: the caption naming
 *  its sender, whether a rewrite may start from its turn, and whether a
 *  thumbs-down on it may ask for a human review. */
export interface SharedBubble {
  sender: BubbleSender | undefined;
  /** Whether an edit, regenerate or retry may start from this turn. Each
   *  rewrites this thread from there, which would drop a later turn another
   *  member sent; a copy into a new chat rewrites nothing and needs no say. */
  canRewrite: boolean;
  humanReview: boolean;
  /** Whether the viewer may answer a pause in this turn (a question, an
   *  approval): only its sender may, which the server holds to. */
  answersPause: boolean;
}

type OwnedMessage = { role: string; sender?: MessageSender; provenance?: unknown; isHistory?: boolean; sentHere?: boolean };

/**
 * Whose a user message is. One that names its sender is theirs or the
 * viewer's by that name. One that names nobody is the viewer's when this
 * view sent it, though the server has not named it yet (a run names its
 * sender only once its computer is up, and the stream's end makes the bubble
 * a history row); any other (a history row, or one another agent sent) is
 * nobody's that the view can tell.
 */
export function turnOwner(message: OwnedMessage, me: string): 'mine' | 'theirs' | 'none' {
  if (message.sender) return message.sender.userId === me ? 'mine' : 'theirs';
  if (message.provenance || (message.isHistory && !message.sentHere)) return 'none';
  return 'mine';
}

/**
 * The turns `messages` show the viewer sent and the latest one they did
 * not, read in one pass, with whoever opened the running turn when
 * `running`. A turn counts as the viewer's only when an opener says so: the
 * server refuses a rewrite from turn n when any turn from n on was sent by
 * someone else or names no sender, so a turn with no opener that names
 * anyone (a wake or report turn) counts as someone else's.
 */
export function threadTurns(
  messages: readonly ChatMessage[], me: string, running: boolean,
): Pick<SharedThreadView, 'ownTurns' | 'lastOthersTurn'> & { othersTurn: MessageSender | null } {
  const mine = new Set<number>();
  const others = new Set<number>();
  const seen = new Set<number>();
  let opener: MessageSender | null = null;
  for (const m of messages) {
    const turn = 'turnIndex' in m ? m.turnIndex : undefined;
    if (typeof turn === 'number') seen.add(turn);
    if (m.role !== 'user' || m.steering || m.queued) continue;
    const owner = turnOwner(m, me);
    // A message another agent sent opens no member's turn: the opener before it stands.
    if (!m.provenance) opener = owner === 'theirs' ? m.sender ?? null : null;
    if (typeof turn !== 'number') continue;
    if (owner === 'mine') mine.add(turn);
    else if (owner === 'theirs') others.add(turn);
  }
  let lastOthersTurn = -1;
  const ownTurns = new Set<number>();
  for (const turn of seen) {
    if (others.has(turn) || !mine.has(turn)) lastOthersTurn = Math.max(lastOthersTurn, turn);
    else ownTurns.add(turn);
  }
  return { ownTurns, lastOthersTurn, othersTurn: running ? opener : null };
}

/** Who sent a user message, as its bubble draws them. A message another
 *  agent sent is captioned by where it came from, so it names no sender. */
export function bubbleSender(view: SharedThreadView, message: OwnedMessage): BubbleSender | undefined {
  const owner = message.role === 'user' && !message.provenance ? turnOwner(message, view.me.userId) : 'none';
  if (owner === 'none') return undefined;
  const { userId, name } = message.sender ?? view.me;
  return { userId, name: name ?? view.names[userId] ?? null, avatarUrl: view.avatars[userId] ?? null, mine: owner === 'mine' };
}

export function sharedBubble(view: SharedThreadView, message: OwnedMessage, turnIndex: number | null): SharedBubble {
  return {
    sender: bubbleSender(view, message),
    canRewrite: turnIndex !== null && turnIndex > view.lastOthersTurn,
    humanReview: turnIndex !== null && view.ownTurns.has(turnIndex),
    answersPause: turnIndex === null || view.ownTurns.has(turnIndex),
  };
}

/** Whether another member's message follows one of theirs with nothing
 *  drawn between (`prev` is the sender of the entry just above), so it joins
 *  their run and shows no name or picture of its own, as a group chat draws
 *  one. The reader's own messages carry neither. */
export function continuesSender(prev: BubbleSender | undefined, sender: BubbleSender | undefined): boolean {
  return !!prev && !!sender && !prev.mine && !sender.mine && prev.userId === sender.userId;
}
