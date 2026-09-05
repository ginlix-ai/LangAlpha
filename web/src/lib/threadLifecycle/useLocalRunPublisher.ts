/**
 * Publishes the mounted chat view's OWN run-liveness into the lifecycle
 * store's LOCAL layer, so every surface (nav panel, gallery) reacts instantly,
 * ahead of the user feed.
 *
 * Only the body's running→idle transition counts as a settle — cleanup alone
 * (unmount, LRU eviction, the `__default__` → real id flip mid-run) just drops
 * the local observation; the feed layer persists underneath. A natural end
 * while this thread is active also stamps the durable seen cursor (watched
 * finishes never grow a dot).
 *
 * Sibling of useActiveThreadPublisher: that one owns "the user is looking at
 * this thread", this one owns "this tab is running this thread".
 */
import { useEffect, useRef } from 'react';
import { markThreadSeen } from './seen';
import {
  clearLocalObservation,
  getActiveThreadId,
  publishLocalRunning,
  publishLocalSettled,
} from './store';

export function useLocalRunPublisher(
  threadId: string,
  isLoading: boolean,
  runIdRef: { current: string | null },
): void {
  // Holds WHICH thread this tab observed running (not a bare boolean), so a
  // threadId swap without an unmount can never settle the new thread on the
  // strength of the old thread's run.
  const runningThreadRef = useRef<string | null>(null);
  useEffect(() => {
    const running = !!threadId && threadId !== '__default__' && isLoading;
    const wasRunningThisThread = runningThreadRef.current === threadId;
    runningThreadRef.current = running ? threadId : null;
    if (running) {
      publishLocalRunning(threadId, runIdRef.current ?? undefined);
      // The effect fires on the isLoading flip, BEFORE the response-header
      // latch fills the ref — so the first publish has no runId, and a
      // runId-less observation can't be superseded by the feed's settle nor
      // matched by a snapshot. Converge here (the latch happens outside
      // React): re-publish once the ref fills, polling only in that window.
      let timer: ReturnType<typeof setTimeout> | undefined;
      if (runIdRef.current == null) {
        const poll = () => {
          if (runIdRef.current != null) {
            publishLocalRunning(threadId, runIdRef.current);
          } else {
            timer = setTimeout(poll, 500);
          }
        };
        timer = setTimeout(poll, 500);
      }
      return () => {
        if (timer !== undefined) clearTimeout(timer);
        clearLocalObservation(threadId);
      };
    }
    if (wasRunningThisThread && threadId && threadId !== '__default__') {
      const runId = runIdRef.current ?? undefined;
      if (runId === undefined) {
        // The send ended before response headers delivered a run id (network
        // or admission failure), so no run row was committed — a seq-less
        // terminal observation could never be reconciled away by snapshots or
        // list seeding and would sit as a permanent phantom dot. Drop it.
        clearLocalObservation(threadId);
      } else {
        publishLocalSettled(threadId, runId);
        if (getActiveThreadId() === threadId) {
          markThreadSeen(threadId, runId);
        }
      }
    }
    // runIdRef is a stable ref container — read at effect time, never a dep.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoading, threadId]);
}
