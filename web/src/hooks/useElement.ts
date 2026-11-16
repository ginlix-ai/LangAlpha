import { useCallback, useState, type RefObject } from 'react';

/**
 * An element as state, for what must render once it is drawn, kept in `ref`
 * too for the handlers and effects that read it as they run. Pass the
 * returned callback as the element's `ref`.
 */
export function useElement<T extends Element>(ref: RefObject<T | null>): [T | null, (el: T | null) => void] {
  const [element, setElement] = useState<T | null>(null);
  const place = useCallback((el: T | null) => {
    ref.current = el;
    setElement(el);
  }, [ref]);
  return [element, place];
}
