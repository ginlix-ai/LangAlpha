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

// Threads known archived: archived in this tab, or by another one as the
// lifecycle feed says, until one of them brings it back.
const archived = new Set<string>();

export function markThreadArchived(threadId: string, isArchived: boolean): void {
  if (isArchived) archived.add(threadId);
  else archived.delete(threadId);
}

registerAuthReset(() => {
  deleted = new Set();
  archived.clear();
  notify();
});

/** Outside React: whether this tab deleted the thread. */
export function isThreadDeleted(threadId: string): boolean {
  return deleted.has(threadId);
}

/**
 * Outside React: whether the thread was deleted or archived as far as this tab
 * knows, so its row has left the lists and no page of them holds it. The nav
 * tree's reveal reads it: the open thread's row leaves before the navigation
 * off it lands, and paging for it would walk the whole list.
 */
export function isThreadRemoved(threadId: string): boolean {
  return deleted.has(threadId) || archived.has(threadId);
}

export function useDeletedThreads(): ReadonlySet<string> {
  return useSyncExternalStore(subscribe, () => deleted);
}
