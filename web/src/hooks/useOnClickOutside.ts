import { useEffect, useRef, type RefObject } from 'react';

/**
 * Calls `handler` when a pointerdown event fires outside `ref`.
 * Uses `pointerdown` to handle mouse, touch, and stylus uniformly.
 * Handler is stored in a ref so callers don't need `useCallback`.
 * No-ops when `enabled` is false.
 *
 * Taps on elements (or their descendants) with `data-click-outside-ignore`
 * are excluded — use this attribute on portal-rendered dropdowns that
 * logically belong to the guarded component.
 */
export function useOnClickOutside(
  ref: RefObject<HTMLElement | null>,
  handler: () => void,
  enabled = true,
): void {
  const handlerRef = useRef(handler);
  useEffect(() => { handlerRef.current = handler; });

  useEffect(() => {
    if (!enabled) return;
    const handle = (e: PointerEvent) => {
      const target = e.target as Node;
      if (ref.current && !ref.current.contains(target)) {
        // Ignore taps inside portal-rendered elements marked with the ignore attribute
        const el = target instanceof Element ? target : target.parentElement;
        if (el?.closest('[data-click-outside-ignore]')) return;
        handlerRef.current();
      }
    };
    document.addEventListener('pointerdown', handle);
    return () => document.removeEventListener('pointerdown', handle);
  }, [ref, enabled]);
}
