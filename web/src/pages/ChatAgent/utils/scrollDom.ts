// DOM lookups the transcript scroll code shares (the scroll controller and
// the minimap must agree on what "the viewport" is, and both chat surfaces on
// where a reply starts).

// Gap left above a bubble the transcript is pinned to.
export const ANCHOR_OFFSET_PX = 16;

export type AnchorPart = 'reply';

/** The ScrollArea wrapper is outer (overflow-hidden) > inner (overflow-auto); the inner div is the element that actually scrolls. */
export function resolveScrollViewport(root: HTMLElement | null): HTMLElement | null {
  if (!root) return null;
  return (
    root.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]') ??
    root.querySelector<HTMLElement>('.overflow-auto') ??
    root
  );
}

/** The growing content node inside the fixed-height viewport: the viewport itself never changes size as content lands, so this is what a ResizeObserver must watch. */
export function resolveScrollContent(viewport: HTMLElement): HTMLElement {
  return (
    viewport.querySelector<HTMLElement>('.max-w-3xl') ??
    (viewport.firstElementChild as HTMLElement | null) ??
    viewport
  );
}

export function findMessageElement(viewport: HTMLElement, id: string): HTMLElement | null {
  for (const el of viewport.querySelectorAll<HTMLElement>('[data-message-id]')) {
    if (el.dataset.messageId === id) return el;
  }
  return null;
}

/** scrollTop that puts bubble `id` just under the viewport top, or `delta` px
 *  into it, or null once it is no longer in the transcript. */
export function anchorTop(c: HTMLElement, id: string, part?: AnchorPart, delta?: number): number | null {
  const msg = findMessageElement(c, id);
  if (!msg) return null;
  // The reply part is the bubble's last prose block, so a turn that opened with
  // commentary and tool rows lands on the answer; a bubble without prose is
  // its own start.
  const el = (part === 'reply' && msg.querySelector<HTMLElement>('[data-reply-start]')) || msg;
  const gap = delta == null ? ANCHOR_OFFSET_PX : -delta;
  return Math.max(0, c.scrollTop + el.getBoundingClientRect().top - c.getBoundingClientRect().top - gap);
}

/** The floating composer's height, which the transcript keeps clear under its
 *  last line (--composer-h): the band under it is not viewport anything can
 *  be read in. */
export function composerHeight(c: HTMLElement): number {
  return parseFloat(getComputedStyle(c).getPropertyValue('--composer-h')) || 0;
}

// Breathing room left under a deliverables deck brought into view.
const REVEAL_GAP_PX = 12;

/** scrollTop that brings the deliverables deck on bubble `id` into view, capped
 *  so the deck's own top never leaves it: a deck taller than the viewport is
 *  read from its first card rather than chased past it. Null once the deck is
 *  gone, which is what retires the pin. */
export function revealTop(c: HTMLElement, id: string): number | null {
  const deck = findMessageElement(c, id)?.querySelector<HTMLElement>('.turn-files');
  if (!deck) return null;
  const view = c.getBoundingClientRect();
  const rect = deck.getBoundingClientRect();
  // clientHeight, not the rect's own bottom: a horizontal scrollbar is not
  // viewport a card can be read in, and neither is the band under the composer.
  const needed = Math.max(0, rect.bottom + REVEAL_GAP_PX - (view.top + c.clientHeight - composerHeight(c)));
  const headroom = Math.max(0, rect.top - view.top);
  return c.scrollTop + Math.min(needed, headroom);
}
