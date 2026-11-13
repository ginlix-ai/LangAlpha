/**
 * Marks the mounted chat view's thread as ACTIVE in the lifecycle store
 * (active thread never shows the unseen dot) and stamps the durable seen
 * cursor once the thread's newest run is finished and unseen, while a person
 * can be looking at the page.
 *
 * Subscribes to the thread rather than sampling it once on mount: on a cold
 * URL open the store hasn't hydrated yet (the snapshot frame lands after
 * mount), so a mount-time check misses the terminal state and the dot
 * survives the visit. A finish that lands while the page is away (another app
 * in front, a background tab) waits for the user to come back, because
 * stamping it then would tell the server they watched it and suppress the
 * notification that should reach them. Only while a stamp is owed does the
 * page listen for that return. markThreadSeen dedups in flight and the cursor
 * is monotonic, so overlapping stamps (feed client, local stream end) are safe.
 */
import { useEffect, useSyncExternalStore } from 'react';
import { isPageWatched, isSeenUnconfirmed, markThreadSeen, subscribePageWatched } from './seen';
import {
  getEffectiveObservation,
  getSeenThroughSeq,
  setActiveThread,
  subscribeThreadLifecycle,
  TERMINAL_FAMILY,
} from './store';

/** The run a stamp is owed for: the newest, finished, past the seen cursor. */
function owedRunId(threadId: string | null): string | null {
  if (!threadId) return null;
  const eff = getEffectiveObservation(threadId);
  if (!eff?.runId || !TERMINAL_FAMILY.has(eff.status)) return null;
  // A seq-less settle (this tab's own stream) is owed until the feed names its
  // seq, and a stamp the server never confirmed stays owed.
  if (
    typeof eff.runSeq === 'number'
    && getSeenThroughSeq(threadId) >= eff.runSeq
    && !isSeenUnconfirmed(threadId)
  ) return null;
  return eff.runId;
}

const noSubscribe = () => () => {};

export function useActiveThreadPublisher(threadId: string | null | undefined): void {
  const tid = threadId && threadId !== '__default__' ? threadId : null;
  const owed = useSyncExternalStore(subscribeThreadLifecycle, () => owedRunId(tid));
  const watched = useSyncExternalStore(owed ? subscribePageWatched : noSubscribe, isPageWatched);

  useEffect(() => {
    setActiveThread(tid);
    return () => setActiveThread(null);
  }, [tid]);

  useEffect(() => {
    if (tid && owed && watched) markThreadSeen(tid, owed);
  }, [tid, owed, watched]);
}
