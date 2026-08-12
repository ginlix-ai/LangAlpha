import { useSyncExternalStore } from 'react';

/**
 * Navigation panel display preferences, persisted to localStorage.
 * Module-level store (like the panel's pin state) so every panel instance —
 * one mounts per cached ChatView — sees the same values.
 */
/** Workspace ordering — mirrors the workspace gallery's sort modes. */
export type NavOrderBy = 'activity' | 'name' | 'custom';

export interface NavDisplayPrefs {
  /** Workspaces visible before the "Load all" row; 'all' shows everything. */
  workspaceLimit: number | 'all';
  /** Threads fetched/shown per workspace page ("Show more" loads another page). */
  threadPageSize: number;
  /** Workspace order: recency ('activity'), alphabetical ('name'), or the
   *  manual/pinned arrangement ('custom', synced with the gallery). */
  orderBy: NavOrderBy;
}

const STORAGE_KEY = 'nav.display';

export const NAV_PREFS_DEFAULTS: NavDisplayPrefs = {
  // 'all' by default so the nav reads as a browsable workspace tree. Only names
  // are paged (100 at a time beyond the first 20) and threads stay lazy per
  // expand, so the first-load cost is metadata-only — intentional, not a cap bug.
  workspaceLimit: 'all',
  threadPageSize: 10,
  orderBy: 'custom',
};

function sanitize(raw: unknown): NavDisplayPrefs {
  const prefs = { ...NAV_PREFS_DEFAULTS };
  if (raw && typeof raw === 'object') {
    const { workspaceLimit, threadPageSize, orderBy } = raw as Record<string, unknown>;
    if (workspaceLimit === 'all' || (typeof workspaceLimit === 'number' && Number.isInteger(workspaceLimit) && workspaceLimit > 0)) {
      prefs.workspaceLimit = workspaceLimit;
    }
    if (typeof threadPageSize === 'number' && Number.isInteger(threadPageSize) && threadPageSize > 0) {
      prefs.threadPageSize = threadPageSize;
    }
    if (orderBy === 'activity' || orderBy === 'name' || orderBy === 'custom') {
      prefs.orderBy = orderBy;
    }
  }
  return prefs;
}

function load(): NavDisplayPrefs {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? sanitize(JSON.parse(raw)) : { ...NAV_PREFS_DEFAULTS };
  } catch {
    return { ...NAV_PREFS_DEFAULTS };
  }
}

let _prefs: NavDisplayPrefs = load();
const _listeners = new Set<() => void>();

export function getNavPrefs(): NavDisplayPrefs {
  return _prefs;
}

export function setNavPrefs(update: Partial<NavDisplayPrefs>): void {
  _prefs = sanitize({ ..._prefs, ...update });
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(_prefs));
  } catch {
    // Storage unavailable (private mode) — prefs still apply for the session.
  }
  _listeners.forEach((fn) => fn());
}

export function resetNavPrefs(): void {
  _prefs = { ...NAV_PREFS_DEFAULTS };
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    // ignore
  }
  _listeners.forEach((fn) => fn());
}

function subscribe(fn: () => void): () => void {
  _listeners.add(fn);
  return () => _listeners.delete(fn);
}

export function useNavPrefs(): NavDisplayPrefs {
  return useSyncExternalStore(subscribe, getNavPrefs);
}
