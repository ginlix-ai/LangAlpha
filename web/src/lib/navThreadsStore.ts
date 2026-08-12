/**
 * Session-shared store of loaded thread lists per workspace ("Show more"
 * rows), used by every cached ChatView's nav panel — hook-local state would
 * let a panel that mounted before a folder was opened render it open-but-empty
 * until its own fetch landed. Page-session scoped: a reload starts fresh.
 *
 * Lives in lib (not the nav hook) so `threadRowActions.patchThreadRows` can
 * compose it: a thread row removed or rewritten in the query caches must not
 * survive here as a stale "Show more" extra.
 */
import { createEmitter } from '@/lib/emitter';

export interface ThreadRecord {
  thread_id: string;
  [key: string]: unknown;
}

export interface ThreadsData {
  threads: ThreadRecord[];
  loading: boolean;
  total?: number;
}

let _sharedWorkspaceThreads: Record<string, ThreadsData> = {};
const navThreadsEmitter = createEmitter();

// Cap on how many workspaces' thread lists the shared store retains. A long
// session paging through many workspaces would otherwise grow this unbounded;
// far above any realistic count of workspaces a user navigates in one session.
const MAX_WORKSPACE_THREAD_LISTS = 50;

/** Subscribe to store changes (useSyncExternalStore-compatible). */
export const subscribeNavThreads = navThreadsEmitter.subscribe;

// Stable snapshot: the same ref until a write replaces it, so useSyncExternalStore
// only re-renders on a real change.
export function getNavThreadsSnapshot(): Record<string, ThreadsData> {
  return _sharedWorkspaceThreads;
}

export function setSharedWorkspaceThreads(
  updater: (prev: Record<string, ThreadsData>) => Record<string, ThreadsData>,
): void {
  let next = updater(_sharedWorkspaceThreads);
  const keys = Object.keys(next);
  if (keys.length > MAX_WORKSPACE_THREAD_LISTS) {
    // Evict oldest (insertion-order) entries to bound a long multi-workspace
    // session; evicted lists are re-fetched on demand when revisited.
    next = { ...next };
    for (const k of keys.slice(0, keys.length - MAX_WORKSPACE_THREAD_LISTS)) delete next[k];
  }
  _sharedWorkspaceThreads = next;
  navThreadsEmitter.emit();
}

// Drop all loaded thread lists. Mirrors resetStableNavOrder's session-reset role
// (page-session scoped); used on auth changes and by tests to isolate the store.
export function resetSharedWorkspaceThreads(): void {
  _sharedWorkspaceThreads = {};
  navThreadsEmitter.emit();
}

// Forget one workspace's threads so a deleted workspace doesn't linger in the
// shared store. Called from the workspace delete path (alongside forgetStableNavOrder).
export function forgetSharedWorkspaceThreads(workspaceId: string): void {
  if (!(workspaceId in _sharedWorkspaceThreads)) return;
  const next = { ..._sharedWorkspaceThreads };
  delete next[workspaceId];
  _sharedWorkspaceThreads = next;
  navThreadsEmitter.emit();
}

/** Pre-patch entries touched by patchNavThreadsRows, for restore. */
export type NavThreadsStoreSnapshot = Array<[string, ThreadsData]>;

/**
 * Apply a row mapper to every retained workspace's list — the store-side half
 * of `patchThreadRows`. Mappers must be id-targeted (return the same array
 * when no row matches): the store isn't scoped by query key, so an
 * unconditional mapper would rewrite every workspace it retains. The entry's
 * total tracks row-count deltas the same way the cache pages' totals do.
 */
export function patchNavThreadsRows(
  mapRows: (rows: ThreadRecord[]) => ThreadRecord[],
): NavThreadsStoreSnapshot {
  const snapshot: NavThreadsStoreSnapshot = [];
  let next: Record<string, ThreadsData> | null = null;
  for (const [wsId, data] of Object.entries(_sharedWorkspaceThreads)) {
    const rows = data.threads;
    if (!Array.isArray(rows)) continue;
    const mapped = mapRows(rows);
    if (mapped === rows) continue;
    snapshot.push([wsId, data]);
    const delta = mapped.length - rows.length;
    const total = typeof data.total === 'number' && delta !== 0
      ? Math.max(0, data.total + delta)
      : data.total;
    (next ??= { ..._sharedWorkspaceThreads })[wsId] = { ...data, threads: mapped, total };
  }
  if (next) {
    _sharedWorkspaceThreads = next;
    navThreadsEmitter.emit();
  }
  return snapshot;
}

/** Restore entries captured by patchNavThreadsRows. */
export function restoreNavThreadsRows(snapshot: NavThreadsStoreSnapshot): void {
  if (!snapshot.length) return;
  const next = { ..._sharedWorkspaceThreads };
  for (const [wsId, data] of snapshot) next[wsId] = data;
  _sharedWorkspaceThreads = next;
  navThreadsEmitter.emit();
}
