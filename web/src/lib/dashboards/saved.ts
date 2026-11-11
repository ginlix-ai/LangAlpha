import type { QueryClient } from '@tanstack/react-query';
import * as z from 'zod/mini';
import { readOtherPreference } from '@/lib/otherPreference';
import { queryKeys } from '@/lib/queryKeys';
import type { UserPreferences } from '@/types/api';

interface Entry {
  /** Its private `/a/` code, which names it on the list and keys what it may read. */
  code: string;
  title: string;
  /** An app's page to open at (null for its entry); a page's file, workspace-relative. */
  path: string | null;
  /** Where it was built, so an edit goes back to the agent that knows it. */
  workspaceId: string | null;
  threadId: string | null;
}

/**
 * An HTML file kept for its owner (`setArtifactKept`). The server holds a
 * version of it, so it opens with its computer off, and the end of each turn
 * in its workspace cuts a new one. What the dashboard skill builds.
 */
export interface PageDashboard extends Entry {
  kind: 'page';
  artifactId: string;
}

/**
 * A running app. Resolving its code re-signs the URL, starts a stopped
 * computer and reruns the app's command, so it needs the computer.
 */
export interface AppDashboard extends Entry {
  kind: 'app';
}

/** A dashboard the user's agent built and the user set in place of the built-in one. */
export type SavedDashboard = PageDashboard | AppDashboard;

/**
 * Every dashboard the user has set, and the one shown. `current` null is the
 * built-in layout: switching back keeps the list, so trying another dashboard
 * never costs the one before it.
 */
export interface SavedDashboards {
  current: string | null;
  list: SavedDashboard[];
}

/** A sibling of `other_preference.dashboard` (the built-in layout), whose writer replaces that key whole. */
export const SAVED_DASHBOARDS_KEY = 'custom_dashboards';
export const MAX_DASHBOARDS = 8;

// Per-field recovery, and unknown fields ride through (`looseObject`), so a
// tab on an older build writing the list back keeps what a newer one added.
const id = z.string().check(z.minLength(1));
const text = z.catch(z.nullable(id), null);
const entry = { code: id, title: z.catch(z.string(), ''), path: text, workspaceId: text, threadId: text };
const Page = z.looseObject({ ...entry, kind: z.literal('page'), artifactId: id });
const App = z.looseObject({ ...entry, kind: z.literal('app') });
const EMPTY = { current: null, list: [] as unknown[] };
const Stored = z.catch(z.looseObject({ current: text, list: z.catch(z.array(z.unknown()), []) }), () => EMPTY);

// A page with no record fails both shapes and is dropped, rather than opened
// as an app that would wait forever on a port.
function readEntry(raw: unknown): SavedDashboard | null {
  const page = Page.safeParse(raw);
  if (page.success) return page.data;
  const app = App.safeParse(raw);
  return app.success ? app.data : null;
}

export function readSavedDashboards(raw: unknown): SavedDashboards {
  const parsed = Stored.safeParse(raw);
  const stored = parsed.success ? parsed.data : EMPTY;
  const seen = new Set<string>();
  const list: SavedDashboard[] = [];
  for (const item of stored.list.map(readEntry)) {
    if (!item || seen.has(item.code)) continue;
    seen.add(item.code);
    list.push(item);
  }
  const current = stored.current && seen.has(stored.current) ? stored.current : null;
  return { ...stored, current, list };
}

/** The list as the latest cached row holds it, for a decision made outside render. */
export function readCachedDashboards(qc: QueryClient): SavedDashboards {
  const row = qc.getQueryData<UserPreferences | null>(queryKeys.user.preferences());
  return readSavedDashboards(readOtherPreference(row, SAVED_DASHBOARDS_KEY));
}

// The changes to the list, each built on the latest value at write time
// (`usePreferenceKey`), and each saying why when it leaves the list alone.

/** Add it, or refresh what is saved for it in place at the front, and show it. */
export function addDashboard(saved: SavedDashboards, dashboard: SavedDashboard): { next: SavedDashboards } | { refuse: 'full' } {
  const before = saved.list.find((d) => d.code === dashboard.code);
  if (!before && saved.list.length >= MAX_DASHBOARDS) return { refuse: 'full' };
  return {
    next: {
      ...saved,
      current: dashboard.code,
      list: [{ ...before, ...dashboard }, ...saved.list.filter((d) => d.code !== dashboard.code)],
    },
  };
}

/** What a removal took off the list, enough to put it back as it was. */
export interface Removed {
  gone: SavedDashboard;
  index: number;
  wasCurrent: boolean;
}

export function removeDashboard(saved: SavedDashboards, code: string): { next: SavedDashboards; removed: Removed } | { refuse: 'absent' } {
  const index = saved.list.findIndex((d) => d.code === code);
  if (index < 0) return { refuse: 'absent' };
  const wasCurrent = saved.current === code;
  return {
    next: { ...saved, current: wasCurrent ? null : saved.current, list: saved.list.filter((d) => d.code !== code) },
    removed: { gone: saved.list[index], index, wasCurrent },
  };
}

/**
 * Put a removal back where it was. It is shown again only if it was and
 * nothing else has been chosen since.
 */
export function restoreDashboard(saved: SavedDashboards, { gone, index, wasCurrent }: Removed): { next: SavedDashboards } | { refuse: 'listed' | 'full' } {
  if (saved.list.some((d) => d.code === gone.code)) return { refuse: 'listed' };
  if (saved.list.length >= MAX_DASHBOARDS) return { refuse: 'full' };
  const list = [...saved.list];
  list.splice(Math.min(index, list.length), 0, gone);
  return { next: { ...saved, list, current: wasCurrent && saved.current === null ? gone.code : saved.current } };
}

/** Show this one, or the built-in dashboard for null. */
export function selectDashboard(saved: SavedDashboards, code: string | null): { next: SavedDashboards } | { refuse: 'absent' } {
  if (code !== null && !saved.list.some((d) => d.code === code)) return { refuse: 'absent' };
  return { next: { ...saved, current: code } };
}

export function isPageListed(saved: SavedDashboards, artifactId: string): boolean {
  return saved.list.some((d) => d.kind === 'page' && d.artifactId === artifactId);
}
