import { useLayoutEffect, useRef, useState } from 'react';

function contentWidth(el: HTMLElement): number {
  const style = getComputedStyle(el);
  const width = parseFloat(style.width);
  if (Number.isFinite(width)) return width;
  return el.clientWidth - (parseFloat(style.paddingLeft) || 0) - (parseFloat(style.paddingRight) || 0);
}

/**
 * The grid container's content width, read before the first paint, and 0
 * until then. react-grid-layout's own `useContainerWidth` starts from a
 * guessed 1280 and measures in a passive effect, so the first frame painted
 * every cell for the guess, and the cells' transitions then slid the whole
 * grid to the real width. A width of 0 after a real one is the page taken out
 * of layout, and keeps the last.
 */
export function useGridWidth() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(0);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const update = (next: number) => {
      const rounded = Math.round(next);
      if (rounded > 0) setWidth(rounded);
    };
    update(contentWidth(el));
    if (typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(([entry]) => {
      if (entry) update(entry.contentRect.width);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return { width, containerRef };
}
