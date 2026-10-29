import { useCallback, useEffect, useRef } from 'react';
import type React from 'react';

/** Marks a scroll container `data-scrolling` while it moves and for a beat
 *  after, so its scrollbar can show only then. The flag is written straight
 *  to the element: state set on every scroll event would re-render the pane. */
export function useScrollReveal() {
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  return useCallback((e: React.UIEvent<HTMLElement>) => {
    const el = e.currentTarget;
    el.dataset.scrolling = '';
    clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      delete el.dataset.scrolling;
    }, 900);
  }, []);
}
