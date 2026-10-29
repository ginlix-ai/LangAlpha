import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';

/**
 * Threads deleted in this tab. A history entry and a cached chat view both
 * outlive their thread, and Back to one would show it as if it still existed,
 * so the chat route drops their views and leaves their routes.
 */
let deleted: ReadonlySet<string> = new Set();
const listeners = new Set<() => void>();

function notify(): void {
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function markThreadDeleted(threadId: string): void {
  if (deleted.has(threadId)) return;
  deleted = new Set(deleted).add(threadId);
  notify();
}

registerAuthReset(() => {
  deleted = new Set();
  notify();
});

export function useDeletedThreads(): ReadonlySet<string> {
  return useSyncExternalStore(subscribe, () => deleted);
}
