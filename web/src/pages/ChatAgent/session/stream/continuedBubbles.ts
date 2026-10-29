/**
 * The bubbles one stream of a turn writes to. A stream that drops leaves its
 * bubble where it stopped, and the reconnect that resumes the run writes to a
 * bubble of its own that names the one it continues (`continues`), so a turn
 * read across drops spans a chain of bubbles. What lands on the turn as a
 * whole, its failure above all, belongs on that chain and not on whichever
 * bubble the newest stream holds: that one is often empty, and a reconnect
 * drops its own bubble when it ends empty.
 */

interface Bubble {
  content?: unknown;
  contentSegments?: unknown;
  continues?: unknown;
}

/** Whether a bubble shows anything of its own, the test a reconnect drops its
 *  bubble by when its stream ends. */
export function holdsContent(msg: Bubble): boolean {
  return ((msg.contentSegments as unknown[] | undefined)?.length ?? 0) > 0 || !!msg.content;
}

/** The bubbles from `bubbleId` back through the ones it continues, newest
 *  first, as far as the transcript still holds them. */
export function continuationChain<T extends Bubble>(read: (id: string) => T | undefined, bubbleId: string): T[] {
  const chain: T[] = [];
  const seen = new Set<string>();
  let id: unknown = bubbleId;
  while (typeof id === 'string' && !seen.has(id)) {
    seen.add(id);
    const msg = read(id);
    if (!msg) break;
    chain.push(msg);
    id = msg.continues;
  }
  return chain;
}
