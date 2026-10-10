import { useState, useCallback, useMemo, useRef } from 'react';

const MAX_ENTRIES = 5;

export interface CacheEntry {
  key: string;           // `${workspaceId}-${threadId}`
  instanceId: number;    // stable React key — never changes after creation
  workspaceId: string;
  threadId: string;
  workspaceName: string;
  initialTaskId?: string;
}

export type TouchParams = Omit<CacheEntry, 'key' | 'instanceId'>;

function makeKey(workspaceId: string, threadId: string): string {
  return `${workspaceId}-${threadId}`;
}

export function useChatViewCache() {
  const [entries, setEntries] = useState<CacheEntry[]>([]);
  const nextIdRef = useRef(1);
  // Each key's instanceId, given once. touch runs in a state updater, which
  // React may run again: an entry the route makes during render is dropped
  // when a later update is rebased past it, and that update makes it again.
  // A fresh id there was a new React key, so the view remounted and loaded
  // its thread a second time.
  const idsRef = useRef(new Map<string, number>());

  // Idempotent: if entry already exists with same key and is already active, no state update.
  const touch = useCallback((params: TouchParams) => {
    const key = makeKey(params.workspaceId, params.threadId);

    setEntries(prev => {
      const idx = prev.findIndex(e => e.key === key);
      if (idx === 0) {
        // Already MRU — update fields if changed
        const existing = prev[0];
        if (
          existing.workspaceName === params.workspaceName &&
          existing.initialTaskId === params.initialTaskId
        ) {
          return prev; // truly no change — skip re-render
        }
        const updated = [...prev];
        updated[0] = { ...existing, ...params, key, instanceId: existing.instanceId };
        return updated;
      }

      let newEntries: CacheEntry[];
      if (idx > 0) {
        // Promote to front
        const entry = prev[idx];
        newEntries = [{ ...entry, ...params, key, instanceId: entry.instanceId }, ...prev.slice(0, idx), ...prev.slice(idx + 1)];
      } else {
        // New entry. One view per thread: a thread lives in one workspace, so
        // a view of it under another is a stray that loads it a second time.
        // `__default__` is a new thread, one per workspace.
        let instanceId = idsRef.current.get(key);
        if (instanceId === undefined) {
          instanceId = nextIdRef.current++;
          idsRef.current.set(key, instanceId);
        }
        const others = params.threadId === '__default__' ? prev : prev.filter(e => e.threadId !== params.threadId);
        newEntries = [{ ...params, key, instanceId }, ...others];
      }

      // Evict LRU if over cap
      if (newEntries.length > MAX_ENTRIES) {
        newEntries = newEntries.slice(0, MAX_ENTRIES);
      }

      return newEntries;
    });
  }, []);

  // Update key in-place (e.g., __default__ → real threadId).
  // instanceId is preserved so React doesn't remount.
  const updateKey = useCallback((oldKey: string, newKey: string, updates: Partial<Omit<CacheEntry, 'key' | 'instanceId'>>) => {
    setEntries(prev => {
      const idx = prev.findIndex(e => e.key === oldKey);
      if (idx === -1) return prev;
      const updated = [...prev];
      updated[idx] = { ...updated[idx], ...updates, key: newKey };
      // The id goes with the view: one made under the old key again is new.
      idsRef.current.set(newKey, updated[idx].instanceId);
      idsRef.current.delete(oldKey);
      return updated;
    });
  }, []);

  // The order to render the views in: creation order, which a promotion never
  // changes. Rendered in MRU order, a promotion moves the views that were ahead
  // of the promoted one, and React moves a node by re-inserting it, which
  // resets every scroll position inside it: a cached thread came back at the
  // top of its transcript.
  const mounted = useMemo(() => [...entries].sort((a, b) => a.instanceId - b.instanceId), [entries]);

  return { entries, mounted, touch, updateKey };
}
