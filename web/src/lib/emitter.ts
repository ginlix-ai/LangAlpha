/**
 * The listener-set half of every external store: `subscribe` returns an
 * unsubscribe closure shaped for `useSyncExternalStore`, `emit` fans out.
 * Extracted so stores own only their state, never a hand-rolled Set.
 */
export interface Emitter {
  subscribe(listener: () => void): () => void;
  emit(): void;
}

export function createEmitter(): Emitter {
  const listeners = new Set<() => void>();
  return {
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    emit(): void {
      listeners.forEach((l) => l());
    },
  };
}
