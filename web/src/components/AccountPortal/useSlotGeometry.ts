import { useLayoutEffect, useState } from 'react';
import type { PortalLayout } from './portalMessages';

export interface SlotRect {
  left: number;
  top: number;
  width: number;
  height: number;
}

/**
 * Where the slot is on screen, and the padding of the column Settings lays in
 * it (`data-portal-probe`). Both follow the sidebar, the top bar and the
 * window, so both are read again whenever the slot or the window resizes; the
 * frame cannot see the window to read the padding itself.
 */
export function useSlotGeometry(slot: HTMLElement | null): { rect: SlotRect | null; layout: PortalLayout | null } {
  const [rect, setRect] = useState<SlotRect | null>(null);
  const [layout, setLayout] = useState<PortalLayout | null>(null);

  useLayoutEffect(() => {
    if (!slot) return;
    const measure = () => {
      const r = slot.getBoundingClientRect();
      setRect((prev) => (prev && prev.left === r.left && prev.top === r.top && prev.width === r.width && prev.height === r.height
        ? prev
        : { left: r.left, top: r.top, width: r.width, height: r.height }));
      const probe = slot.querySelector<HTMLElement>('[data-portal-probe]');
      if (!probe) return;
      const style = getComputedStyle(probe);
      const next = { top: parseFloat(style.paddingTop) || 0, inline: parseFloat(style.paddingLeft) || 0 };
      setLayout((prev) => (prev && prev.top === next.top && prev.inline === next.inline ? prev : next));
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(slot);
    window.addEventListener('resize', measure);
    return () => {
      observer.disconnect();
      window.removeEventListener('resize', measure);
    };
  }, [slot]);

  return { rect, layout };
}
