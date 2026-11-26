import { createContext, useCallback, useMemo, useState } from 'react';

export interface TopBarSlot {
  /** The bar's right end; null while the bar does not span the page. */
  slot: HTMLElement | null;
  /** Puts the bar up over the page while the returned release is unheld. */
  claim: () => () => void;
}

/** Where a page puts its own controls in the top bar (TopBarActions). */
export const TopBarSlotContext = createContext<TopBarSlot | null>(null);

/**
 * The shell's side of the slot: whether a page on screen wants the bar, the
 * ref the bar's right end takes, and the value pages read. The shell learns
 * only that some page asked, never which one.
 */
export function useTopBarHost() {
  const [claims, setClaims] = useState(0);
  const [slot, slotRef] = useState<HTMLElement | null>(null);
  const claim = useCallback(() => {
    setClaims((n) => n + 1);
    return () => setClaims((n) => n - 1);
  }, []);
  const context = useMemo<TopBarSlot>(() => ({ slot, claim }), [slot, claim]);
  return { claimed: claims > 0, slotRef, context };
}
