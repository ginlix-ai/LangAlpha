import { useCallback, useEffect, useEffectEvent, useRef, useState, type RefObject } from 'react';
import { isQuoteSurface } from './quoteAnchor';

/**
 * What the pointer rests on within `surfaceRef`, as an anchor. Found by hover on
 * the surface rather than wired into each thing the markdown draws. Touch has no
 * hover, and a press on the controls themselves is on the way to them.
 */
export function useHoverAnchor<E extends Element, A>({ surfaceRef, enabled, delay = 0, resolve, makeAnchor }: {
  surfaceRef: RefObject<HTMLElement | null>;
  enabled: boolean;
  delay?: number;
  /** The element under the pointer that takes an anchor, if any. */
  resolve: (target: Element, event: PointerEvent) => E | null;
  makeAnchor: (element: E) => A;
}) {
  const [anchor, setAnchor] = useState<A | null>(null);
  const shownRef = useRef<E | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const clear = useCallback(() => {
    clearTimeout(timerRef.current);
    shownRef.current = null;
    setAnchor(null);
  }, []);

  const settle = useEffectEvent((element: E | null) => {
    if (shownRef.current === element) return;
    shownRef.current = element;
    setAnchor(element ? makeAnchor(element) : null);
  });

  const onPointerOver = useEffectEvent((event: PointerEvent) => {
    if (event.pointerType === 'touch') return;
    clearTimeout(timerRef.current);
    if (isQuoteSurface(event.target)) return;
    const target = event.target instanceof Element && surfaceRef.current?.contains(event.target) ? event.target : null;
    const element = target && resolve(target, event);
    if (delay > 0) timerRef.current = setTimeout(() => settle(element), delay);
    else settle(element);
  });

  useEffect(() => {
    if (!enabled) return;
    document.addEventListener('pointerover', onPointerOver);
    return () => {
      document.removeEventListener('pointerover', onPointerOver);
      clear();
    };
  }, [enabled, clear]);

  return { anchor, clear };
}
