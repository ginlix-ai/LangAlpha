import { createContext, useContext } from 'react';

/** The experiment that turns the top bar into a strip of open pages. */
export const PAGE_TABS_FEATURE = 'page_tabs';

export interface PageTab {
  id: string;
  /** Pathname plus search: where switching back to the tab goes. */
  path: string;
  /** What the page at `path` called itself. */
  title?: string;
}

export interface PageTabsState {
  tabs: PageTab[];
  activeId: string;
}

/**
 * What a page or a menu does to the tabs. Apart from the strip's own state, so
 * the many rows that only offer "Open in new tab" do not render again each
 * time the strip changes.
 */
export interface PageTabActions {
  select: (id: string) => void;
  close: (id: string) => void;
  /** A new tab in front, showing `path`. */
  open: (path: string) => void;
  /** What the page on screen at `path` calls itself. */
  publishTitle: (title: string, path: string) => void;
  /** Asks `guard` before the tabs take the page away; returns its removal. */
  guardLeave: (guard: (go: () => void) => void) => () => void;
}

export const PageTabsContext = createContext<PageTabsState | null>(null);
export const PageTabActionsContext = createContext<PageTabActions | null>(null);

/** The open pages, or null while the experiment is off. */
export function usePageTabs(): PageTabsState | null {
  return useContext(PageTabsContext);
}

/** Null while the experiment is off. */
export function usePageTabActions(): PageTabActions | null {
  return useContext(PageTabActionsContext);
}
