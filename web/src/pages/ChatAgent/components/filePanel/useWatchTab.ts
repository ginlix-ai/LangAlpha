import { useEffect, useEffectEvent, useRef } from 'react';
import type { FileTabsApi } from './useFileTabs';

/**
 * The Status tab follows the market watch and the thread's wakeups: it joins
 * the strip as a watch starts or a wakeup is set, without taking focus from
 * the tab being read. Only that transition acts. A panel mounted under a
 * watch already running leaves the reader on what they opened it for, and the
 * chip in the chat is the way in; a stamp that changes nothing about the watch
 * never pulls the strip back to a tab the reader closed. A watch ending leaves
 * the tab where it is, showing that nothing is watched: closing it would take
 * a tab the reader may be looking at, and a watch resumed a moment later would
 * bring it back unfocused, as though it were new.
 */
export function useWatchTab(tabs: FileTabsApi, watching: boolean): void {
  const wasWatching = useRef(watching);
  const openStatusUnfocused = useEffectEvent(() => tabs.openStatus({ focus: false }));
  useEffect(() => {
    if (watching === wasWatching.current) return;
    wasWatching.current = watching;
    if (watching) openStatusUnfocused();
  }, [watching]);
}
