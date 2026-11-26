import * as z from 'zod/mini';
import { getUserLocalItem, userSessionStorage } from '@/lib/userStorage';
import type { PageTab } from './pageTabsContext';

// The tabs name threads and workspaces, so they are kept per user.
export const STORAGE_KEY = 'app-page-tabs';
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

// Which tab each history entry was visited in. Per browser tab and only as
// long as it lives, which is how long its history does, so session storage.
const OWNERS_KEY = 'app-page-tab-entries';
// More than a browser keeps of one tab's history.
const MAX_OWNERS = 100;
const ownersSchema = z.record(z.string(), z.string());

/** Which tab each history entry was visited in, as last saved here. */
export function loadOwners(): Record<string, string> {
  try {
    const raw = userSessionStorage.getItem(OWNERS_KEY);
    const parsed = raw ? ownersSchema.safeParse(JSON.parse(raw)) : null;
    return parsed?.success ? parsed.data : {};
  } catch {
    return {};
  }
}

/** Saves the newest entries' tabs, in the order they were visited. */
export function saveOwners(owners: Record<string, string>): void {
  const kept = Object.entries(owners).slice(-MAX_OWNERS);
  userSessionStorage.setItem(OWNERS_KEY, JSON.stringify(Object.fromEntries(kept)));
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
