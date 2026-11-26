import { useEffect } from 'react';
import { useLatestRef } from '@/hooks/useLatestRef';
import { usePageTabActions } from './pageTabsContext';

/**
 * Has the tabs ask `guard` before a switch, an open or a close takes the page
 * away: a route change fires no beforeunload, so a page holding drafts would
 * lose them unasked. Null while the page has no say (a chat kept mounted
 * behind the one on screen). Read through a ref, since a page's guard changes
 * as its drafts do. A no-op while tabs are off.
 */
export function usePageTabLeaveGuard(guard: ((go: () => void) => void) | null) {
  const guardLeave = usePageTabActions()?.guardLeave;
  const latest = useLatestRef(guard);
  const on = guard !== null;
  useEffect(() => {
    if (!guardLeave || !on) return;
    return guardLeave((go) => (latest.current ? latest.current(go) : go()));
  }, [guardLeave, on, latest]);
}
