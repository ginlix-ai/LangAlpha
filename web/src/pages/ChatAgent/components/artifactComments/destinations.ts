import type { TFunction } from 'i18next';
import type { ArtifactComment, ArtifactDestinations, ArtifactThread } from '@/types/api';
import type { CommentTarget } from '../../utils/api/artifacts';

/** What the owner asked for: keep the words, queue them for a thread's next send, or send them now. */
export type CommentIntent = 'note' | 'queue' | 'send';

/**
 * Where the owner's words go once a destination is decided. A queue names a
 * thread, since a queued comment waits for that thread's next send, so a
 * queue for a thread not made yet cannot be written.
 */
export type CommentRoute =
  | { kind: 'note' }
  | { kind: 'queue'; threadId: string }
  | { kind: 'send'; to: CommentTarget };

/** One thread a comment can be sent to, or a new one, as the "To:" menu offers it. */
export interface DestinationChoice {
  key: string;
  target: CommentTarget;
  label: string;
  /** The default destination, which a comment takes unless another is picked. */
  isDefault: boolean;
}

/** A target's identity, to tell a pick from the default and to dedupe the menu. */
export function targetKey(target: CommentTarget): string {
  return 'threadId' in target ? `thread:${target.threadId}` : 'new';
}

/** A thread as the menus name it: the one beside as "This thread", else its title. */
export function threadLabel(
  t: TFunction,
  thread: Pick<ArtifactThread, 'thread_id' | 'title'> | { thread_id: string; title: string | null },
  besideThreadId: string | null,
): string {
  if (thread.thread_id === besideThreadId) return t('pageViewer.comments.thisThread');
  return thread.title?.trim() || t('pageViewer.comments.untitledThread');
}

/**
 * Where a file's comments go, read once for every control that sends one:
 * the comment box and its "To:" menu, a card's ⋯ menu and reply box, Send
 * now and Send all, the header's bind, and whether the composer docks under
 * the file. Until the server has answered for the file's record, nothing that
 * needs the default is offered: every file a view shows has its record made
 * as it opens, so that wait is short, and a destination guessed here could
 * disagree with the server's.
 */
export interface CommentDestinations {
  /** Where a comment goes when it names no thread: the bound thread, else what the setting picks. Null while unknown. */
  fallback: CommentTarget | null;
  /** The "To:" menu: the default first, then the thread beside and the bound thread, each usable. Empty while unknown. */
  choices: DestinationChoice[];
  /** Binding the file to the thread beside would change where its comments go. */
  canBind: boolean;
  /**
   * Where `intent` takes the words: a new comment's (`comment` null) to the
   * thread picked in its "To:" menu, else the default; an existing one's to
   * the thread it waits for or went to, unless archived, else the default. A queue needs a
   * thread, so where the default is a new thread there is only a send. Null
   * where it cannot go yet.
   */
  targetFor: (
    comment: Pick<ArtifactComment, 'thread_id' | 'thread_archived'> | null,
    intent: CommentIntent,
    picked?: CommentTarget | null,
  ) => CommentRoute | null;
}

/**
 * Whether the file's comments go to `threadId` unless the owner picks
 * another: what decides that the chat's composer docks under the file, since
 * what it sends goes to that thread.
 */
export function defaultsTo(fallback: CommentTarget | null, threadId: string | undefined): boolean {
  return !!fallback && !!threadId && 'threadId' in fallback && fallback.threadId === threadId;
}

/**
 * The menus and routes from the server's answer for one file, read beside
 * `besideThreadId`. `destinations` undefined is the wait for that answer.
 */
export function commentDestinations(
  t: TFunction,
  destinations: ArtifactDestinations | undefined,
  besideThreadId: string | null,
): CommentDestinations {
  const fallback: CommentTarget | null = !destinations
    ? null
    : destinations.default.thread
      ? { threadId: destinations.default.thread.thread_id }
      : { newThreadIn: destinations.default.workspace_id };

  const choices: DestinationChoice[] = [];
  if (destinations) {
    const add = (target: CommentTarget, label: string, isDefault: boolean) => {
      const key = targetKey(target);
      if (!choices.some((c) => c.key === key)) choices.push({ key, target, label, isDefault });
    };
    const addThread = (thread: ArtifactThread | null, isDefault = false) => {
      if (thread?.usable) add({ threadId: thread.thread_id }, threadLabel(t, thread, besideThreadId), isDefault);
    };
    if (destinations.default.thread) addThread(destinations.default.thread, true);
    else add({ newThreadIn: destinations.default.workspace_id }, t('pageViewer.comments.newThread'), true);
    addThread(destinations.beside);
    addThread(destinations.bound);
  }

  // The server names the thread beside only when the owner may send there.
  const canBind = !!besideThreadId && !!destinations
    && destinations.beside?.usable === true
    && destinations.bound?.thread_id !== besideThreadId;

  const targetFor: CommentDestinations['targetFor'] = (comment, intent, picked = null) => {
    if (intent === 'note') return { kind: 'note' };
    // An archived thread takes nothing, so it is no more a destination than a deleted one.
    const own: CommentTarget | null = comment?.thread_id && !comment.thread_archived ? { threadId: comment.thread_id } : null;
    const to = (comment ? own : picked) ?? fallback;
    if (!to) return null;
    if (intent === 'send') return { kind: 'send', to };
    return 'threadId' in to ? { kind: 'queue', threadId: to.threadId } : null;
  };

  return { fallback, choices, canBind, targetFor };
}
