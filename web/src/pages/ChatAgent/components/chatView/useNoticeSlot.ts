import { useCallback, useState } from 'react';
import { flushSync } from 'react-dom';

/**
 * The notices above the composer, held in one row of their own: whether any
 * shows. Each notice decides on its own whether it renders, so the row's
 * children are the one place that knows, and a notice that comes or goes on
 * its own read (a dismissal, a query landing) changes them without the host
 * rendering. Empty, the row is hidden so it takes no gap in the stack.
 */
export function useNoticeSlot(): [attach: (el: HTMLDivElement | null) => (() => void) | undefined, filled: boolean] {
  const [filled, setFilled] = useState(false);
  const attach = useCallback((el: HTMLDivElement | null) => {
    if (!el) return undefined;
    const read = () => setFilled(el.childElementCount > 0);
    read();
    // Before the next paint, so an emptied row never keeps its gap for a frame.
    const observer = new MutationObserver(() => flushSync(read));
    observer.observe(el, { childList: true });
    return () => {
      observer.disconnect();
      setFilled(false);
    };
  }, []);
  return [attach, filled];
}
