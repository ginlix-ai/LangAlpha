// DOM lookups the transcript scroll code shares (the scroll controller and
// the minimap must agree on what "the viewport" is).

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
