/**
 * Minimal external value store for `useSyncExternalStore` containment: state
 * that updates at input rate (e.g. per mousemove) lives here so only the leaf
 * component that displays it re-renders, never the owning component tree.
 */
import { createEmitter } from '@/lib/emitter';

export interface ValueStore<T> {
  get(): T;
  set(next: T): void;
  subscribe(listener: () => void): () => void;
}

export function createValueStore<T>(initial: T): ValueStore<T> {
  let value = initial;
  const emitter = createEmitter();
  return {
    get: () => value,
    set: (next: T) => {
      if (Object.is(next, value)) return;
      value = next;
      emitter.emit();
    },
    subscribe: emitter.subscribe,
  };
}
