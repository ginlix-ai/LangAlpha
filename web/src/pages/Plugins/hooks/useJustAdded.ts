import { useSyncExternalStore } from 'react';

/**
 * What the user added a moment ago, so the list it lands in can point at it.
 *
 * Held outside React rather than in a URL param or a parent's state: an add
 * finishes inside a dialog, a wizard or the landing after a sign-in, and the
 * list that should mark it is often not mounted yet (a detail page covers
 * it) or belongs to another section. The mark is brief by design, a cue that
 * says "it went here", not a state the item keeps.
 */

export type JustAddedKind = 'plugin' | 'server' | 'skill' | 'secret' | 'subagent';

const HOLD_MS = 6000;

let snapshot: ReadonlySet<string> = new Set();
const timers = new Map<string, ReturnType<typeof setTimeout>>();
const listeners = new Set<() => void>();

function key(kind: JustAddedKind, name: string): string {
  return `${kind}:${name}`;
}

// A new Set per change: the snapshot's identity is what tells React (and the
// compiler's cache) that something moved.
function emit(next: ReadonlySet<string>) {
  snapshot = next;
  for (const listener of listeners) listener();
}

export function markJustAdded(kind: JustAddedKind, name: string): void {
  const k = key(kind, name);
  const pending = timers.get(k);
  if (pending) clearTimeout(pending);
  if (!snapshot.has(k)) emit(new Set(snapshot).add(k));
  timers.set(
    k,
    setTimeout(() => {
      timers.delete(k);
      const next = new Set(snapshot);
      next.delete(k);
      emit(next);
    }, HOLD_MS),
  );
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

const getSnapshot = () => snapshot;

export function useJustAdded(): (kind: JustAddedKind, name: string) => boolean {
  const marked = useSyncExternalStore(subscribe, getSnapshot);
  return (kind, name) => marked.has(key(kind, name));
}
