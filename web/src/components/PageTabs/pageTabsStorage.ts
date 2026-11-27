import * as z from 'zod/mini';
import { getUserLocalItem, userLocalStorage, userSessionStorage } from '@/lib/userStorage';
import type { PageTab } from './pageTabsContext';

// The tabs name threads and workspaces, so they are kept per user.
const STORAGE_KEY = 'app-page-tabs';
export const MAX_TABS = 20;
// What a saved strip may hold: a closed tab brought back by Back reopens even
// past the limit, so a strip can reach the limit plus the closed list's.
const MAX_SAVED = MAX_TABS * 2;

/**
 * A tab as this provider keeps it. A tab left behind also keeps the history
 * entry it was left on, so Back onto that entry goes back to the tab, and the
 * route state it was showing, which a page such as a new chat needs to come
 * back as it was (its workspace rides in the state, not the address).
 */
export interface Tab extends PageTab {
  key?: string;
  state?: unknown;
}

export interface Tabs {
  tabs: Tab[];
  activeId: string;
}

// Persisted input, so read leniently: a bad title drops the title, a bad tab
// drops the tab, and anything worse starts over with one tab.
const optionalText = z.catch(z.optional(z.string()), undefined);
const tabSchema = z.object({
  id: z.string().check(z.minLength(1)),
  // An app path: `//host` or `/\host` would be another origin to the router
  // and to the URL parser.
  path: z.string().check(z.regex(/^\/(?![/\\])/)),
  title: optionalText,
  key: optionalText,
});
const stateSchema = z.object({
  tabs: z.array(z.unknown()),
  activeId: z.string(),
});

/** A tab closed in this browser tab, and where it stood in the strip. */
export interface Closed {
  tab: Tab;
  index: number;
}

/**
 * What this browser tab keeps of the tabs' past, for Back and Forward: which
 * tab each history entry was visited in, every entry and not only the one a
 * tab was left on, and the tabs closed here, newest first, so Back onto one of
 * their entries reopens them. Kept per browser tab and only as long as it
 * lives, which is how long its history does, so in session storage, and
 * together, since one is read against the other.
 */
export interface TabHistory {
  owners: Record<string, string>;
  closed: Closed[];
}

const HISTORY_KEY = 'app-page-tab-history';
// Where the owners were kept before the closed tabs joined them. Read while
// there is no record yet, so a browser tab open across the update keeps its
// way back to the tabs it switched between.
const OWNERS_KEY = 'app-page-tab-entries';
// More than a browser keeps of one tab's history.
const MAX_OWNERS = 100;
const closedSchema = z.object({
  tab: tabSchema,
  index: z.number().check(z.int(), z.nonnegative()),
});
const historySchema = z.object({
  owners: z.catch(z.record(z.string(), z.string()), {}),
  closed: z.catch(z.array(z.unknown()), []),
});

function readRecord(): unknown {
  const raw = userSessionStorage.getItem(HISTORY_KEY);
  if (raw) return JSON.parse(raw);
  const owners = userSessionStorage.getItem(OWNERS_KEY);
  return owners ? { owners: JSON.parse(owners) } : null;
}

/** This browser tab's history of tabs, as last saved here. */
export function loadHistory(): TabHistory {
  try {
    const record = readRecord();
    const parsed = record ? historySchema.safeParse(record) : null;
    if (!parsed?.success) return { owners: {}, closed: [] };
    // As in the strip, a closed tab is found by its id.
    const ids = new Set<string>();
    const closed = parsed.data.closed.flatMap((entry) => {
      const one = closedSchema.safeParse(entry);
      if (!one.success || ids.has(one.data.tab.id)) return [];
      ids.add(one.data.tab.id);
      return [one.data];
    }).slice(0, MAX_TABS);
    return { owners: parsed.data.owners, closed };
  } catch {
    return { owners: {}, closed: [] };
  }
}

/** Saves the newest entries' tabs, in the order they were visited, and the
 *  closed tabs. */
export function saveHistory({ owners, closed }: TabHistory): void {
  const kept: TabHistory = {
    owners: Object.fromEntries(Object.entries(owners).slice(-MAX_OWNERS)),
    closed: closed.map(({ tab, index }) => ({ tab: persisted(tab), index })),
  };
  userSessionStorage.setItem(HISTORY_KEY, JSON.stringify(kept));
}

/** A tab as it is saved. Route state is the router's, and may not survive a
 *  trip through JSON; the history entry a tab comes back on carries its own. */
function persisted({ state: _routeState, ...tab }: Tab): Tab {
  return tab;
}

/** The strip as it is saved, compared by the caller to save only a change. */
export function tabsRecord(tabs: Tabs): string {
  return JSON.stringify({ ...tabs, tabs: tabs.tabs.map(persisted) });
}

export function saveTabs(record: string): void {
  userLocalStorage.setItem(STORAGE_KEY, record);
}

export const newId = () => (crypto.randomUUID?.() ?? `${Date.now()}-${Math.random()}`);

/** `user`'s strip as it was last saved, or null when none was or it is
 *  unreadable. The account is an argument rather than whoever is signed in
 *  when it runs, so a caller reading it in render names what it depends on. */
function readSaved(user: string | null): Tabs | null {
  if (!user) return null;
  try {
    const raw = getUserLocalItem(user, STORAGE_KEY);
    const parsed = raw ? stateSchema.safeParse(JSON.parse(raw)) : null;
    if (!parsed?.success) return null;
    // A tab is found by its id, so a second one with the same id is dropped.
    const ids = new Set<string>();
    const tabs = parsed.data.tabs.flatMap((tab) => {
      const one = tabSchema.safeParse(tab);
      if (!one.success || ids.has(one.data.id)) return [];
      ids.add(one.data.id);
      return [one.data];
    }).slice(0, MAX_SAVED);
    if (tabs.length === 0) return null;
    const active = tabs.find((tab) => tab.id === parsed.data.activeId) ?? tabs[0];
    return { tabs, activeId: active.id };
  } catch {
    return null;
  }
}

/** `user`'s saved strip, or a fresh one with a single tab at `path`. */
export function loadTabs(path: string, user: string | null): Tabs {
  const saved = readSaved(user);
  if (saved) return saved;
  const id = newId();
  return { tabs: [{ id, path }], activeId: id };
}

/** The page the tab in front was showing when the strip was last saved. */
export function savedFrontTabPath(user: string | null): string | null {
  const saved = readSaved(user);
  return saved?.tabs.find((tab) => tab.id === saved.activeId)?.path ?? null;
}
