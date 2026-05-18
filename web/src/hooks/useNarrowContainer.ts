import { useEffect, useState, type RefObject } from 'react';

/**
 * Returns true when the observed element's width is below `threshold`.
 * Element-based (not viewport) so embedded panels (e.g. MarketView's chat
 * column) can collapse avatars/tight layouts independent of window width.
 */
export function useNarrowContainer(
  ref: RefObject<HTMLElement | null>,
  threshold: number,
): boolean {
  const [narrow, setNarrow] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el || typeof ResizeObserver === 'undefined') return;

    const update = (width: number) => {
      setNarrow((prev) => {
        const next = width > 0 && width < threshold;
        return next === prev ? prev : next;
      });
    };

    update(el.getBoundingClientRect().width);

    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        update(entry.contentRect.width);
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [ref, threshold]);

  return narrow;
}
