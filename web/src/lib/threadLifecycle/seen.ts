/**
 * Durable seen stamping: POST /threads/{id}/seen with the OBSERVED run_id —
 * the causal cursor (server refuses cross-thread / non-terminal runs and
 * never sweeps later settlements the client hasn't rendered).
 *
 * Fire-and-forget by design: the store's cursor is raised optimistically so
 * the dot clears instantly for this session; a lost POST self-heals on the
 * next open (thread-lifecycle v6 §3 failure matrix).
 */
import type { QueryClient } from '@tanstack/react-query';
import { api } from '@/api/client';
import { apiErrorStatus } from '@/api/errors';
import { registerAuthReset } from '@/lib/authResets';
import { createEmitter } from '@/lib/emitter';
import { patchThreadSeen } from '@/lib/threadListCache';
import { applySeenCursors, raiseSeenToEffective } from './store';

/**
 * The lifecycle package's one QueryClient handle. Set by the feed mount and
 * read by every ambient caller (`markThreadSeen` is invoked from three call
 * sites that hold no client), so the feed client reads it back rather than
 * keeping a second copy.
 */
let queryClientRef: QueryClient | null = null;

export function setLifecycleQueryClient(qc: QueryClient): void {
  queryClientRef = qc;
}

export function getLifecycleQueryClient(): QueryClient | null {
  return queryClientRef;
}

const inflight = new Set<string>();
const askedAgain = new Set<string>();

/**
 * Threads whose last stamp the server never answered, with the order of that
 * stamp. The optimistic raise already cleared their dot, so the publisher
 * would see nothing owed; this is what makes the next open send the stamp
 * again. Only a stamp sent after the failed one settles it: an older one's
 * answer says nothing of the newer run.
 */
const unconfirmed = new Map<string, number>();
let stampOrder = 0;

export function isSeenUnconfirmed(threadId: string): boolean {
  return unconfirmed.has(threadId);
}

/** Moves on sign-out, so a stamp the old account sent changes nothing once it lands. */
let authEpoch = 0;

registerAuthReset(() => {
  authEpoch += 1;
  unconfirmed.clear();
  askedAgain.clear();
});

/**
 * Stamp the thread seen as of `runId`, unconditionally: an explicit act of the
 * user's (a notification's Mark as read), or a page known to be watched. A
 * stamp that follows from the thread merely being open goes through
 * {@link markWatchedThreadSeen}.
 */
export function markThreadSeen(threadId: string, runId: string): void {
  if (!threadId || !runId) return;
  stamp(threadId, runId, ++stampOrder);
}

/**
 * `order` places the stamp among the thread's others; a repeat of an ask keeps
 * the order of the one it repeats, since it confirms no newer run than that.
 */
function stamp(threadId: string, runId: string, order: number, deferred = false): void {
  const epoch = authEpoch;
  // Keyed by sign-in too: a stamp the old session left out must not hold back
  // the same stamp made after signing back in.
  const key = `${epoch}:${threadId}:${runId}`;
  // A second ask while the first is out may know more (the first went before
  // the run's end committed and is refused), so it goes once the first is back.
  if (inflight.has(key)) {
    askedAgain.add(key);
    return;
  }
  inflight.add(key);
  const settled = () => {
    if ((unconfirmed.get(threadId) ?? 0) <= order) unconfirmed.delete(threadId);
  };
  // A deferred repeat leaves the dot alone: the ask it repeats already cleared
  // it for the run it saw, and a run that finished since is not what it names.
  if (!deferred) raiseSeenToEffective(threadId);
  void (async () => {
    try {
      const { data } = await api.post<{
        last_seen_run_seq: number;
        latest_run_seq: number;
      }>(`/api/v1/threads/${threadId}/seen`, { run_id: runId });
      if (epoch !== authEpoch) return;
      settled();
      applySeenCursors(threadId, data);
      const qc = getLifecycleQueryClient();
      if (qc && typeof data?.last_seen_run_seq === 'number') {
        patchThreadSeen(qc, threadId, data.last_seen_run_seq);
        announceSeen(threadId, data.last_seen_run_seq);
      }
    } catch (err) {
      if (epoch !== authEpoch) return;
      // 409 seen_not_applicable (an already-newer cursor, or a run not over)
      // needs nothing more. Anything else was lost, so the next open retries.
      if (apiErrorStatus(err) === 409) settled();
      else unconfirmed.set(threadId, Math.max(unconfirmed.get(threadId) ?? 0, order));
    } finally {
      inflight.delete(key);
      if (askedAgain.delete(key) && epoch === authEpoch) stamp(threadId, runId, order, true);
    }
  })();
}

/**
 * Whether a person can be looking at this page now: visible, and the window
 * focused. A thread open in a window behind another app, or in a background
 * tab, is on screen to nobody, and stamping it seen there tells the server
 * the user watched a finish they never saw, which suppresses the very
 * notification that would have told them.
 *
 * `hasFocus()` is true while focus is in a frame inside the page, of any
 * origin (it walks the focus chain down from the top document), so a user
 * typing in an artifact counts as watching.
 */
export function isPageWatched(): boolean {
  if (typeof document === 'undefined') return false;
  if (document.visibilityState !== 'visible') return false;
  return typeof document.hasFocus !== 'function' || document.hasFocus();
}

/**
 * While the page is away, how often it is checked for the user's return.
 * Coming back to a window whose focus was last inside a frame (an artifact, an
 * embedded chart) focuses that frame: the top window gets no `focus`, its
 * document no `focusin`, and visibility never changed, and a click inside the
 * frame reaches only the frame. No event marks that return, so it is polled.
 */
export const RETURN_POLL_MS = 1_000;

const watchers = createEmitter();
let watchedNow = false;
let watcherCount = 0;
let detachReturn: (() => void) | null = null;

function recheckWatched(): void {
  const now = isPageWatched();
  if (now === watchedNow) return;
  watchedNow = now;
  syncReturnWatch();
  watchers.emit();
}

/** The return listeners and the poll run only while someone waits on a page that is away. */
function syncReturnWatch(): void {
  const want = watcherCount > 0 && !watchedNow;
  if (want === (detachReturn !== null)) return;
  if (!want) {
    detachReturn?.();
    detachReturn = null;
    return;
  }
  const capture = { capture: true } as const;
  window.addEventListener('focus', recheckWatched);
  // Returns into the page itself that raise no window `focus` (a shell that
  // refocuses a view, a click on a window that was already key).
  document.addEventListener('focusin', recheckWatched, capture);
  document.addEventListener('pointerdown', recheckWatched, capture);
  const poll = setInterval(recheckWatched, RETURN_POLL_MS);
  detachReturn = () => {
    clearInterval(poll);
    window.removeEventListener('focus', recheckWatched);
    document.removeEventListener('focusin', recheckWatched, capture);
    document.removeEventListener('pointerdown', recheckWatched, capture);
  };
}

/** {@link isPageWatched} as an external store. Leaving is heard from `blur` and visibility. */
export function subscribePageWatched(onChange: () => void): () => void {
  const off = watchers.subscribe(onChange);
  if (watcherCount++ === 0) {
    watchedNow = isPageWatched();
    window.addEventListener('blur', recheckWatched);
    document.addEventListener('visibilitychange', recheckWatched);
  }
  syncReturnWatch();
  return () => {
    off();
    if (--watcherCount === 0) {
      window.removeEventListener('blur', recheckWatched);
      document.removeEventListener('visibilitychange', recheckWatched);
    }
    syncReturnWatch();
  };
}

/**
 * The stamp a thread being on screen makes, as a run settles: now while the
 * page is watched, so the server's "seen during grace" check still skips the
 * push for a watched finish. Away, nothing; `useActiveThreadPublisher` stamps
 * the open thread when the user comes back.
 */
export function markWatchedThreadSeen(threadId: string, runId: string): void {
  if (isPageWatched()) markThreadSeen(threadId, runId);
}

/**
 * Where a page tells this browser's other pages that a thread was read. The
 * desktop shell's menu bar page is one of them: it takes the thread's
 * notification and badge away now rather than at the feed's next reconcile.
 */
const SEEN_CHANNEL = 'thread-seen';

interface SeenMessage {
  type: 'seen';
  threadId: string;
  lastSeenRunSeq: number;
}

function announceSeen(threadId: string, lastSeenRunSeq: number): void {
  if (typeof BroadcastChannel === 'undefined') return;
  try {
    const chan = new BroadcastChannel(SEEN_CHANNEL);
    chan.postMessage({ type: 'seen', threadId, lastSeenRunSeq } satisfies SeenMessage);
    chan.close();
  } catch {
    // A page that cannot post still has the feed's reconcile.
  }
}

/** Take the seen cursors other pages announce. Attached with the feed. */
export function listenForSeenElsewhere(): () => void {
  if (typeof BroadcastChannel === 'undefined') return () => {};
  const chan = new BroadcastChannel(SEEN_CHANNEL);
  chan.onmessage = (e: MessageEvent) => {
    const msg = e.data as Partial<SeenMessage> | null;
    if (msg?.type !== 'seen' || typeof msg.threadId !== 'string' || typeof msg.lastSeenRunSeq !== 'number') return;
    applySeenCursors(msg.threadId, { last_seen_run_seq: msg.lastSeenRunSeq });
  };
  return () => chan.close();
}
