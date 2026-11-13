/**
 * The listener-set half of every external store: `subscribe` returns an
 * unsubscribe closure shaped for `useSyncExternalStore`, `emit` fans out.
 * Extracted so stores own only their state, never a hand-rolled Set. An event
 * stream gives `T` for the value each emit carries.
 */
export interface Emitter<T = void> {
  subscribe(listener: (value: T) => void): () => void;
  emit(value: T): void;
}

export function createEmitter<T = void>(): Emitter<T> {
  const listeners = new Set<(value: T) => void>();
  return {
    subscribe(listener: (value: T) => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    emit(value: T): void {
      listeners.forEach((l) => l(value));
    },
  };
}
