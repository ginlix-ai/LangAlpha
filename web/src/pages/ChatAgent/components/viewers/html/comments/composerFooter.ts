import type { CommentEnter } from '@/lib/artifactPreferences';
import type { CommentIntent } from '../../../artifactComments/destinations';
import type { CommentSurface } from './commentSurface';

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

/**
 * What Enter does in a box on `surface`: the surface's own default where it
 * sets one (the Drive sends at once), else the owner's preference.
 */
export function enterFor(surface: CommentSurface, preference: CommentEnter): CommentEnter {
  return surface.enter ?? preference;
}

/**
 * Which Enter a key is in a comment field: plain, with ⌘/Ctrl (`mod`), or
 * null for a key that only edits the words, Shift+Enter's newline among them.
 * An on-screen keyboard's return carries no modifier, so with one up
 * (`onScreenKeyboard`) Enter is always a new line and the buttons send, as in
 * the chat's box.
 */
export function enterKey(
  key: { key: string; shiftKey: boolean; metaKey: boolean; ctrlKey: boolean },
  onScreenKeyboard = false,
): 'plain' | 'mod' | null {
  if (key.key !== 'Enter' || key.shiftKey || onScreenKeyboard) return null;
  return key.metaKey || key.ctrlKey ? 'mod' : 'plain';
}

/** Each intent as a footer button names it. */
export const INTENT_LABEL: Record<CommentIntent, string> = {
  queue: 'pageViewer:comments.addToQueue',
  note: 'pageViewer:comments.saveNote',
  send: 'pageViewer:comments.sendNow',
};
