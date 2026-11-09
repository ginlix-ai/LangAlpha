/**
 * Copy helpers for the Rooms page.
 *
 * Kept out of the page components so the list and the panel agree on what a
 * responder setting is called, and so the locale-key sweep has one place to
 * find every variant.
 */

/** The catalog key naming a responder value ("everyone", "mentions", one bot). */
export function responderLabelKey(responder: string): string {
  if (responder.startsWith('member:')) return 'rooms.responder.single';
  return responder === 'mentions' ? 'rooms.responder.mentions' : 'rooms.responder.everyone';
}
