import { useContext, useLayoutEffect, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { TopBarSlotContext } from './topBarSlot';

/**
 * The page's controls, drawn at the right end of the top bar, which goes up
 * over the page for them while they are mounted. Outside the desktop shell's
 * layout (a test, a phone) there is no bar and nothing is drawn.
 */
export function TopBarActions({ children }: { children: ReactNode }) {
  const bar = useContext(TopBarSlotContext);
  const claim = bar?.claim;
  useLayoutEffect(() => claim?.(), [claim]);
  return bar?.slot ? createPortal(children, bar.slot) : null;
}
