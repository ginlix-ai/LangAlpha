import { flushSync } from 'react-dom';
import { onPageReturn, onPageUnseenChange } from '@/lib/pageVisibility';

type Update<T> = (prev: T) => T;

/**
 * Per-token state updates, applied once per animation frame.
 *
 * A model streams one SSE event per chunk, and each one rendered the whole
 * transcript, although the screen can only show one state per frame. Queued
 * here, every chunk that lands between two frames is composed into a single
 * update. The batch follows the display up to 60 Hz (see
 * MIN_FLUSH_INTERVAL_MS), and a main thread too busy to produce frames drains
 * it less often, which is when batching saves the most. A hidden page paints
 * nothing, so there it drains on a timer, about once a second, and all at
 * once when the page comes back.
 *
 * Everything else must apply the queue before its own write, in the same task
 * (`flush`), so no other state ever gets ahead of the text.
 */
export interface FrameQueue<T> {
  /** Apply on the next frame, after every update queued before it. */
  queue: (update: Update<T>) => void;
  /** Apply what is queued now. */
  flush: () => void;
  /** Drop what is queued. */
  cancel: () => void;
}

// One frame callback for every queue, so two streaming views cost one render.
const waiting = new Set<() => void>();
let frame = 0;

function flushWaiting(): void {
  const flushes = [...waiting];
  waiting.clear();
  for (const flush of flushes) flush();
}

// A plain default-priority update, not flushSync: it can render in the same
// pass as the typewriter's own per-frame update. Forced to render inside the
// frame callback, the two always rendered apart, and a stream slower than the
// display took 44% more commits and 38% more script time than no queue at all.
//
// At most once per 60 Hz frame, though: on a 120 Hz screen every other frame
// rendered the transcript again for text the typewriter reveals at the same
// pace on both. The bound sits between one 120 Hz frame and two, so a 60 Hz
// screen still drains every frame and a 120 Hz one every other.
const MIN_FLUSH_INTERVAL_MS = 12;
let lastFlush = -Infinity;

function onFrame(now: number): void {
  frame = 0;
  if (!waiting.size) return;
  if (now - lastFlush < MIN_FLUSH_INTERVAL_MS) {
    frame = requestAnimationFrame(onFrame);
    return;
  }
  lastFlush = now;
  flushWaiting();
}

// A hidden page waits on a timer instead: one render a second rather than one
// per network read, which was 2 to 2.6 times what the same stream costs in
// view. A hidden page's timers wake at most once a second anyway, so the delay
// only has to stay under that. The read that queues arms it, never a timer
// callback, which keeps it out of intensive throttling's once-a-minute budget.
const HIDDEN_FLUSH_MS = 500;
let hiddenTimer: ReturnType<typeof setTimeout> | undefined;

function onHiddenTimer(): void {
  hiddenTimer = undefined;
  flushWaiting();
}

// A hidden document runs no frame callbacks: hand over what is waiting now
// rather than when the page comes back.
onPageUnseenChange(() => {
  if (document.hidden) flushWaiting();
});

// What the hidden page held renders synchronously, while the page still counts
// as unseen (lib/pageVisibility), so the first frame back is as current as if
// every read had rendered, and nothing it brings animates in.
onPageReturn(() => {
  clearTimeout(hiddenTimer);
  hiddenTimer = undefined;
  if (waiting.size) flushSync(flushWaiting);
});

export function createFrameQueue<T>(apply: (update: Update<T>) => void): FrameQueue<T> {
  let pending: Update<T>[] = [];

  function flush(): void {
    waiting.delete(flush);
    if (pending.length === 0) return;
    const batch = pending;
    pending = [];
    apply(batch.length === 1 ? batch[0] : (prev) => batch.reduce((acc, update) => update(acc), prev));
  }

  function queue(update: Update<T>): void {
    pending.push(update);
    waiting.add(flush);
    if (document.hidden) {
      hiddenTimer ??= setTimeout(onHiddenTimer, HIDDEN_FLUSH_MS);
      return;
    }
    if (!frame) frame = requestAnimationFrame(onFrame);
  }

  function cancel(): void {
    waiting.delete(flush);
    pending = [];
  }

  return { queue, flush, cancel };
}
