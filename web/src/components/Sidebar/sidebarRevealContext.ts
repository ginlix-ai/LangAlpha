import { createContext } from 'react';
import type { SidebarPeek } from './useSidebarPeek';

/**
 * The toggle a page draws at the start of its own top row (SidebarReveal)
 * while the sidebar is hidden and no top bar is up to hold it, so hiding the
 * sidebar moves nothing down. Null whenever the toggle has another home.
 */
export interface SidebarRevealValue {
  onToggle: () => void;
  hover: SidebarPeek['toggleHover'];
}

export const SidebarRevealContext = createContext<SidebarRevealValue | null>(null);
