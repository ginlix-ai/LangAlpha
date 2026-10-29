import { useEffect, useState, type SetStateAction } from 'react';
import { createFrameQueue } from './frameQueue';

/**
 * The transcript with every streamed chunk applied.
 *
 * A chunk only grows a message that is already there, and nearly everything
 * that reads the transcript needs only which messages there are. So chunk
 * flushes land here alone, and reach the few readers that show the text
 * (subscribed through `useLiveMessages`) without rendering the view that holds
 * the transcript. Every other write lands here first and then in that view's
 * state, so the state has the structure at once and the text as of its last
 * write.
 */
export interface LiveMessages<T> {
  get: () => T;
  subscribe: (listener: () => void) => () => void;
}

/** The store with its writes: the only way into it, so no write can get ahead of a chunk. */
export interface LiveTranscript<T> extends LiveMessages<T> {
  /** A streamed chunk, applied on the next frame (see createFrameQueue). */
  queue: (update: (prev: T) => T) => void;
  /** Applies the queued chunks now. */
  flush: () => void;
  /** Any other write. An update applies the queued chunks first and is
   *  computed on the result; a value replaces the transcript and drops them.
   *  Either way it lands here and in `commit` in the same task. */
  write: (next: SetStateAction<T>) => void;
  dispose: () => void;
}

export function createLiveTranscript<T extends readonly unknown[]>(initial: T, commit: (value: T) => void): LiveTranscript<T> {
  let value = initial;
  const listeners = new Set<() => void>();
  const publish = (next: T) => {
    if (Object.is(next, value)) return;
    value = next;
    for (const listener of [...listeners]) listener();
  };
  const chunks = createFrameQueue<T>((update) => publish(update(value)));
  return {
    get: () => value,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    queue: chunks.queue,
    flush: chunks.flush,
    dispose: chunks.cancel,
    write: (next) => {
      if (typeof next === 'function') {
        chunks.flush();
        next = next(value);
      } else {
        chunks.cancel();
      }
      publish(next);
      commit(next);
    },
  };
}

/**
 * The store's value, rendered on every change.
 *
 * State and a subscription rather than useSyncExternalStore: a store update
 * renders on the sync lane, apart from the typewriter's default-lane update of
 * the same frame, which took 40% more commits. As state it batches with them,
 * and with whatever else the same task sets, so one commit never shows the
 * new transcript beside the old loading state.
 */
export function useLiveMessages<T>(store: LiveMessages<T>): T {
  const [value, setValue] = useState(store.get);
  useEffect(() => {
    const update = () => setValue(store.get());
    // A chunk can land between this render and the subscription.
    update();
    return store.subscribe(update);
  }, [store]);
  return value;
}
