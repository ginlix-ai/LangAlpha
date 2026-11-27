import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { NavigationType, useLocation, useNavigate, useNavigationType } from 'react-router';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { useIsMobile } from '@/hooks/useIsMobile';
import { storageUser, useStorageUser } from '@/lib/userStorage';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import {
  loadHistory,
  loadTabs,
  MAX_TABS,
  newId,
  saveHistory,
  saveTabs,
  tabsRecord,
  type Tab,
  type TabHistory,
  type Tabs,
} from './pageTabsStorage';
import {
  PAGE_TABS_FEATURE,
  PageTabActionsContext,
  PageTabsContext,
  type PageTabActions,
} from './pageTabsContext';

/** Where the tab in front is, and what to keep of it when it is left. */
interface Spot {
  path: string;
  title?: string;
  key?: string;
  state?: unknown;
}

/** What the page at `at` is called: what it said if it is on screen, else
 *  what the tab in front last kept for it. */
function titleAt(at: string, named: { title: string; path: string } | null, active: Tab | undefined): string | undefined {
  if (named?.path === at) return named.title;
  return active?.path === at ? active.title : undefined;
}

/** Whether two route states show the same page. History hands back a clone,
 *  never the object a navigation passed, so they are compared by content. */
function sameState(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}

const NO_HISTORY: TabHistory = { owners: {}, closed: [] };

/**
 * What is known of the tabs' past on a load: what this browser tab saved, of
 * tabs still open or closed here, then the entry each tab was left on, and the
 * one on screen, which is the tab in front's. A closed tab open again is left
 * out: the strip is shared by the account's browser tabs, so another kept it.
 */
function historyOf(tabs: Tabs, entry: string, saved: TabHistory): TabHistory {
  const open = new Set(tabs.tabs.map((tab) => tab.id));
  const closed = saved.closed.filter((c) => !open.has(c.tab.id));
  const known = new Set([...open, ...closed.map((c) => c.tab.id)]);
  const owners = Object.fromEntries(Object.entries(saved.owners).filter(([, id]) => known.has(id)));
  for (const tab of tabs.tabs) if (tab.key) owners[tab.key] = tab.id;
  delete owners[entry];
  owners[entry] = tabs.activeId;
  return { owners, closed };
}

/** The tabs with the one in front at `spot`. */
function placeActive(tabs: Tabs, spot: Spot): Tab[] {
  return tabs.tabs.map((tab) => (tab.id === tabs.activeId ? { ...tab, ...spot } : tab));
}

/**
 * The open pages, one of which is the page on screen. There is one router and
 * one history: a tab is a remembered address, and switching to it navigates
 * there, so each page loads as it would from the sidebar.
 *
 * The tab in front stores nothing while it is in front: it is wherever the
 * address is, called whatever the page there calls itself. A tab keeps an
 * address and a name only once it is left, so a reload or a pasted link lands
 * in the tab that was in front, showing that page.
 */
export function PageTabsProvider({ children }: { children: React.ReactNode }) {
  // The strip is drawn in the desktop layout's top bar alone, so a phone,
  // where nothing would show the tabs, opens none.
  const isMobile = useIsMobile();
  const enabled = useFeatureEnabled(PAGE_TABS_FEATURE) && !isMobile;
  const user = useStorageUser();
  const location = useLocation();
  const navigate = useNavigate();
  const navigationType = useNavigationType();
  const path = `${location.pathname}${location.search}`;
  const [state, setState] = useState<Tabs | null>(null);
  // The tabs share one history, so each entry is remembered with the tab it
  // was visited in: a tab switched to twice has entries on both sides of the
  // other tab's. History keeps a closed tab's entries too, so Back onto one
  // brings the tab back rather than showing its page in the tab in front.
  const [history, setHistory] = useState<TabHistory>(NO_HISTORY);

  // Loaded once the flag reads on, not at mount, since the flags arrive after
  // the first paint; again for another account signed in in place; and dropped
  // if the flag is turned off.
  const [loadedFor, setLoadedFor] = useState(user);

  // A switch, a new tab or a close puts the tab in front at once and
  // navigates to its page, and the router lands the new address in a
  // transition that renders after it. Until it lands, the tab in front shows
  // where it is going rather than the page it left. Watched on the history
  // entry, since a tab at the same address with other route state is a
  // navigation that leaves the address as it was. `replace` marks the one
  // navigation this provider makes from an effect rather than an action.
  const [pending, setPending] = useState<{ from: string; to: string; replace?: { state?: unknown } } | null>(null);

  const loading = enabled && (!state || loadedFor !== user);
  if (loading) {
    const loaded = loadTabs(path, user);
    setState(loaded);
    setHistory(historyOf(loaded, location.key, loadHistory()));
    setLoadedFor(user);
    // Another account signed in in place: the address is still the last
    // one's page, so go to the front tab this account left rather than
    // keeping that page in it. On a first load the address is the one asked
    // for, and lands in the front tab.
    const front = loaded.tabs.find((tab) => tab.id === loaded.activeId);
    if (state && front && front.path !== path) {
      setPending({ from: location.key, to: front.path, replace: { state: front.state } });
    }
  }
  if (!enabled && state) {
    setState(null);
    setHistory(NO_HISTORY);
  }

  // What the page on screen calls itself, and at which address: its effects
  // can run before the address it names has reached this provider.
  const [named, setNamed] = useState<{ title: string; path: string } | null>(null);

  const waiting = pending !== null && location.key === pending.from;
  if (pending && !waiting) setPending(null);

  const active = state?.tabs.find((tab) => tab.id === state.activeId);
  // While a switch is on its way the history entry and its state are still the
  // page it left, so they are not this tab's to keep.
  const herePath = waiting ? pending.to : path;
  const hereTitle = titleAt(herePath, named, active);
  const here: Spot = waiting
    ? { path: herePath, title: hereTitle }
    : { path: herePath, title: hereTitle, key: location.key, state: location.state };

  // Back or Forward onto an entry another tab visited goes back to that tab,
  // which undoes a switch rather than copying a tab, and onto one a closed tab
  // visited reopens it. Matched on the entry, not the address, so a page two
  // tabs both visited still steps back within the tab you are in.
  const [seen, setSeen] = useState<Spot>(here);
  // Not in the render that loads, which still reads the strip and history
  // it is replacing: an account switched in place in the same render as a
  // step would keep the last account's. The step is taken on the next one.
  if (seen.key !== location.key && !waiting && !loading) {
    setSeen(here);
    let owner = state?.activeId;
    let { owners, closed } = history;
    if (state && navigationType === NavigationType.Pop) {
      const tabs = placeActive(state, { ...seen, title: titleAt(seen.path, named, active) });
      const id = owners[location.key];
      const other = id === state.activeId ? undefined : state.tabs.find((tab) => tab.id === id);
      // Past the limit too: the page is on screen already, and the tab in
      // front taking it would lose that tab's own.
      const back = other ? undefined : closed.find((c) => c.tab.id === id);
      if (other) {
        setState({ tabs, activeId: other.id });
        owner = other.id;
      }
      if (back) {
        setState({ tabs: tabs.toSpliced(back.index, 0, back.tab), activeId: back.tab.id });
        closed = closed.filter((c) => c !== back);
        owner = back.tab.id;
      }
    }
    if (owner && owners[location.key] !== owner) {
      // Moved to the end, so the newest entries are the ones saved.
      const { [location.key]: _was, ...rest } = owners;
      owners = { ...rest, [location.key]: owner };
    }
    if (owners !== history.owners || closed !== history.closed) setHistory({ owners, closed });
  }

  // Memoized by the compiler on its inputs, so the context changes only when
  // the strip does.
  const shown: Tabs | null = state ? { tabs: placeActive(state, here), activeId: state.activeId } : null;

  const saved = shown && tabsRecord(shown);
  const handoff = pending?.replace ? pending : null;
  useEffect(() => {
    if (handoff) navigate(handoff.to, { replace: true, state: handoff.replace?.state });
  }, [handoff, navigate]);
  // Only for the account this render read: an effect still pending when
  // another signs in in place would put one account's tabs under the next's.
  useEffect(() => {
    if (state && storageUser() === user) saveHistory(history);
  }, [history, state, user]);
  useEffect(() => {
    if (saved && storageUser() === user) saveTabs(saved);
  }, [saved, user]);

  // The actions read the strip as it is when they run, so they keep one
  // identity for the provider's life and the rows offering them stay put.
  const { t } = useTranslation();
  const latest = useRef({ shown, entry: location.key, here: herePath, navigate, t });
  // What the pages on screen ask before the tabs take them away (their
  // drafts). Run in turn, each going on only once the one before says so.
  const guards = useRef(new Set<(go: () => void) => void>());
  useLayoutEffect(() => {
    latest.current = { shown, entry: location.key, here: herePath, navigate, t };
  });
  const actions = useMemo<PageTabActions>(() => {
    const askLeave = (go: () => void) =>
      [...guards.current].reduceRight<() => void>((then, guard) => () => guard(then), go)();
    // `also` is the rest of the change, made only if the page is left.
    const show = (next: Tabs, target: Tab, also?: () => void) => {
      const { shown: now, entry: from, here: at } = latest.current;
      const onScreen = now?.tabs.find((t) => t.id === now.activeId)?.state;
      // A tab bringing route state of its own still goes there at the same
      // address: two new chats sit at one address in different workspaces.
      if (target.path === at && (target.state === undefined || sameState(target.state, onScreen))) {
        // Nothing navigates, so the entry on screen is the new tab's now, and
        // Back onto the one before it stays in this tab.
        also?.();
        setState({ ...next, tabs: next.tabs.map((t) => (t.id === now?.activeId ? { ...t, key: undefined } : t)) });
        setHistory((prev) => ({ ...prev, owners: { ...prev.owners, [from]: target.id } }));
        return;
      }
      // The strip `next` was read for the account on screen now, so if another
      // signs in while the page asks, the answer comes too late to apply.
      const who = storageUser();
      askLeave(() => {
        if (storageUser() !== who) return;
        also?.();
        setState(next);
        setPending({ from: latest.current.entry, to: target.path });
        latest.current.navigate(target.path, { state: target.state });
      });
    };
    return {
      select: (id) => {
        const now = latest.current.shown;
        const tab = now?.tabs.find((t) => t.id === id);
        if (!now || !tab || id === now.activeId) return;
        show({ ...now, activeId: id }, tab);
      },
      open: (to) => {
        const now = latest.current.shown;
        if (!now) return;
        // Said rather than dropped: the command that asked shows no state.
        if (now.tabs.length >= MAX_TABS) {
          toast({ title: latest.current.t('pageTabs.full', { count: MAX_TABS }) });
          return;
        }
        const tab = { id: newId(), path: to };
        show({ tabs: [...now.tabs, tab], activeId: tab.id }, tab);
      },
      close: (id) => {
        const now = latest.current.shown;
        if (!now || now.tabs.length <= 1) return;
        const index = now.tabs.findIndex((t) => t.id === id);
        if (index < 0) return;
        const tabs = now.tabs.filter((t) => t.id !== id);
        const remember = () =>
          setHistory((prev) => ({ ...prev, closed: [{ tab: now.tabs[index], index }, ...prev.closed].slice(0, MAX_TABS) }));
        if (id !== now.activeId) {
          remember();
          setState({ ...now, tabs });
          return;
        }
        // The neighbour to the right takes its place, or the one to the left at the end.
        const next = tabs[Math.min(index, tabs.length - 1)];
        show({ tabs, activeId: next.id }, next, remember);
      },
      publishTitle: (title, at) =>
        setNamed((prev) => (prev?.title === title && prev.path === at ? prev : { title, path: at })),
      guardLeave: (guard) => {
        guards.current.add(guard);
        return () => guards.current.delete(guard);
      },
    };
  }, []);

  return (
    <PageTabActionsContext.Provider value={shown ? actions : null}>
      <PageTabsContext.Provider value={shown}>{children}</PageTabsContext.Provider>
    </PageTabActionsContext.Provider>
  );
}
