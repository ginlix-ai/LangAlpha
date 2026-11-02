import type { CommentEnter } from '@/lib/artifactPreferences';
import type { CommentIntent } from '../../../artifactComments/destinations';

/**
 * The comment box's footer: which route the filled button takes, what the
 * quiet button beside it offers, and what each key does. It lives here alone
 * because the design weighed three footers and this one may still be swapped
 * for another; a swap is an edit to this file and the labels it names.
 *
 * This is "queue on the right". Enter always does what the filled button
 * says: adds the comment to the queue, or saves a note once "For the agent"
 * is unticked. Sending one comment by itself, at once, is the quiet button and
 * ⌘/Ctrl+Enter. The owner may swap the two (`enter` 'send'): the filled
 * button and Enter send, the quiet button and ⌘/Ctrl+Enter queue. Where no
 * thread would hold a queue (the comment would start a new one), sending is
 * the filled button itself.
 */
export interface FooterLayout {
  /** The filled button, on the right; Enter presses it. */
  primary: CommentIntent;
  /**
   * The quiet button before it. `shown` false fades it out in place, so the
   * footer does not move as the box is ticked and unticked; null where the
   * box never offers one.
   */
  quiet: { route: CommentIntent; shown: boolean } | null;
  /** What ⌘/Ctrl+Enter does. */
  modEnter: CommentIntent;
}

export function footerLayout(forAgent: boolean, canQueue: boolean, enter: CommentEnter = 'queue'): FooterLayout {
  if (!canQueue) return { primary: forAgent ? 'send' : 'note', quiet: null, modEnter: forAgent ? 'send' : 'note' };
  const [primary, other]: [CommentIntent, CommentIntent] = enter === 'send' ? ['send', 'queue'] : ['queue', 'send'];
  const quiet = { route: other, shown: forAgent };
  if (!forAgent) return { primary: 'note', quiet, modEnter: 'note' };
  return { primary, quiet, modEnter: other };
}

/** Where a key sends the words, or null for one that only edits them, Shift+Enter's newline among them. */
export function routeForKey(
  key: { key: string; shiftKey: boolean; metaKey: boolean; ctrlKey: boolean },
  layout: FooterLayout,
): CommentIntent | null {
  if (key.key !== 'Enter' || key.shiftKey) return null;
  return key.metaKey || key.ctrlKey ? layout.modEnter : layout.primary;
}

/** Each intent as a footer button names it. */
export const INTENT_LABEL: Record<CommentIntent, string> = {
  queue: 'pageViewer.comments.addToQueue',
  note: 'pageViewer.comments.saveNote',
  send: 'pageViewer.comments.sendNow',
};
