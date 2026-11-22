import { useSyncExternalStore } from 'react';
import { onPageUnseenChange } from '@/lib/pageVisibility';

function read(): boolean {
  return document.visibilityState === 'visible';
}

/**
 * Whether the page is on screen, for a read that should poll or stream only
 * while someone can see it. Visibility rather than focus: a page can be in
 * front without focus (the desktop menu bar's panel never takes it). Told of
 * each change by the page's one visibility listener (`pageVisibility`).
 */
export function usePageVisible(): boolean {
  return useSyncExternalStore(onPageUnseenChange, read, () => true);
}
