import { useEffectEvent, useLayoutEffect, useState, type RefObject } from 'react';
import type { BlockAnchor, ImageAnchor, QuoteAnchor, SelectionAnchor } from './quoteAnchor';

const EDGE = 8;
/** How far the block's menu reaches down into it, so that the pointer can get there. */
const EDGE_OVERLAP = 6;
/** Clear of the line the selection ended on, without leaving the pointer behind. */
const END_GAP = 4;

interface Size { width: number; height: number }
/** Where to put a floating element, and the stretch of the page its anchor covers. */
interface Placement { left: number; top: number; span: { top: number; bottom: number } }
/** Null once the anchor is gone from the page. */
export type Place<A> = (anchor: A, size: Size, bounds: DOMRect) => Placement | null;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(value, Math.max(min, max)));
}

const leftWithin = (left: number, { width }: Size, bounds: DOMRect) => clamp(left, bounds.left + EDGE, bounds.right - width - EDGE);
const boxOf = (element: Element) => (element.isConnected ? element.getBoundingClientRect() : null);

export const placeAtCorner: Place<ImageAnchor> = ({ image }, { width }) => {
  const box = boxOf(image);
  return box && { left: box.right - width - EDGE, top: box.top + EDGE, span: box };
};

export const placeOnEdge: Place<BlockAnchor> = ({ block }, size, bounds) => {
  const box = boxOf(block);
  return box && {
    left: leftWithin(box.right - size.width, size, bounds),
    // A block taller than the view keeps its menu at the top of what shows.
    top: Math.max(box.top - size.height + EDGE_OVERLAP, bounds.top + EDGE / 2),
    span: box,
  };
};

/** Centered on `x` over what it belongs to, under it only if there is no room above. */
function aboveOrBelow(x: number, span: { top: number; bottom: number }, size: Size, bounds: DOMRect): Placement {
  const above = span.top - size.height - EDGE;
  const below = span.bottom + EDGE;
  const fitsAbove = above >= bounds.top + EDGE / 2;
  const fitsBelow = below + size.height <= bounds.bottom - EDGE;
  return {
    left: leftWithin(x - size.width / 2, size, bounds),
    top: clamp(fitsAbove || !fitsBelow ? above : below, EDGE, window.innerHeight - size.height - EDGE),
    span,
  };
}

/** Off the selection, on the side the drag was heading, so the pointer is already
 *  there and what was chosen stays in view. Only a lack of room turns it. */
function atSelectionEnd({ range, backward }: SelectionAnchor, size: Size, bounds: DOMRect): Placement | null {
  const lines = Array.from(range.getClientRects()).filter((r) => r.width > 0);
  const end = backward ? lines[0] : lines.at(-1);
  if (!end) return null;
  const over = end.top - size.height - END_GAP;
  const under = end.bottom + END_GAP;
  const fitsOver = over >= bounds.top + EDGE / 2;
  const fitsUnder = under + size.height <= bounds.bottom - EDGE;
  const top = backward ? (fitsOver || !fitsUnder ? over : under) : (fitsUnder || !fitsOver ? under : over);
  return {
    left: leftWithin((backward ? end.left : end.right) - size.width / 2, size, bounds),
    top: clamp(top, EDGE, window.innerHeight - size.height - EDGE),
    span: { top: lines[0].top, bottom: range.getBoundingClientRect().bottom },
  };
}

export const placeBeside: Place<QuoteAnchor> = (anchor, size, bounds) => {
  if (anchor.kind === 'selection') return atSelectionEnd(anchor, size, bounds);
  if (anchor.kind === 'rect') return aboveOrBelow(anchor.rect.left, anchor.rect, size, bounds);
  const box = boxOf(anchor.kind === 'image' ? anchor.image : anchor.block);
  return box && aboveOrBelow(box.left + box.width / 2, box, size, bounds);
};

/** Keeps a floating element beside its anchor as the text under it scrolls. */
export function useAnchoredPosition<A extends QuoteAnchor>(
  anchor: A,
  elementRef: RefObject<HTMLElement | null>,
  boundsRef: RefObject<HTMLElement | null>,
  place: Place<A>,
  onLost?: () => void,
) {
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null);
  const [offscreen, setOffscreen] = useState(false);
  const lost = useEffectEvent(() => onLost?.());

  useLayoutEffect(() => {
    const measure = () => {
      const element = elementRef.current;
      const bounds = boundsRef.current?.getBoundingClientRect();
      if (!element || !bounds) return;
      element.style.maxWidth = `${bounds.width - EDGE * 2}px`;
      const placed = place(anchor, { width: element.offsetWidth, height: element.offsetHeight }, bounds);
      if (!placed) {
        lost();
        return;
      }
      setOffscreen(placed.span.bottom < bounds.top || placed.span.top > bounds.bottom);
      setPosition({ left: placed.left, top: placed.top });
    };
    measure();
    let frame = 0;
    const schedule = () => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(measure);
    };
    window.addEventListener('scroll', schedule, true);
    window.addEventListener('resize', schedule);
    // A reply growing line by line can outgrow the side it opened on.
    const size = new ResizeObserver(schedule);
    if (elementRef.current) size.observe(elementRef.current);
    return () => {
      size.disconnect();
      cancelAnimationFrame(frame);
      window.removeEventListener('scroll', schedule, true);
      window.removeEventListener('resize', schedule);
    };
  }, [anchor, elementRef, boundsRef, place]);

  return { position, offscreen };
}
