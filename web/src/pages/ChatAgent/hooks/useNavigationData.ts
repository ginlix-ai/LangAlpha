import { useState, useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { useQueries, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { CACHE_ONLY_META, queryKeys } from '../../../lib/queryKeys';
import {
  getNavThreadsSnapshot,
  setSharedWorkspaceThreads,
  subscribeNavThreads,
  type ThreadRecord,
  type ThreadsData,
} from '@/lib/navThreadsStore';
import type { ResourceTier, ThreadsResponse, Workspace, WorkspacesResponse } from '@/types/api';
import { getWorkspaces, getWorkspaceThreads, reorderWorkspaces } from '../utils/api';
import { workspaceThreadsQuery } from '../utils/threadQueries';
import { pinWorkspaceRow, rankCachedWorkspaces, renameWorkspaceRow, unrankCachedWorkspaces } from './workspaceRowActions';
import {
  absorbThreadOrder,
  bumpNavOrder,
  frozenThreadOrder,
  getFrozenWorkspaceOrder,
  getNavOrderVersion,
  lastWorkspaceArrangement,
  setFrozenWorkspaceOrder,
  subscribeNavOrder,
} from './navOrderFreeze';
import { getNavPrefs, useNavPrefs } from '../utils/navPrefs';
import { denialMessage } from '../utils/denialMessage';

/**
 * Workspace row as the nav tree consumes it: the fields the tree and its
 * options menu render, plus the loose tail the list endpoint carries. Exported
 * so hosts render the tree without re-casting the hook's output.
 */
export interface NavWorkspace extends Pick<Workspace, 'is_home'> {
  workspace_id: string;
  name?: string;
  status?: string;
  is_pinned?: boolean;
  is_always_on?: boolean;
  resource_tier?: ResourceTier;
  sort_order?: number | null;
  [key: string]: unknown;
}

// Re-exported so nav-tree consumers keep importing the hook's row types from
// the hook rather than reaching into the store module.
export type { ThreadRecord, ThreadsData } from '@/lib/navThreadsStore';

const NAV_WS_PARAMS = { limit: 20, includeHome: true };

// Reorder `items` to a frozen id sequence. Unseen ids appearing before any
// known id are genuinely new (the server lists them first) and surface on top;
// unseen ids after a known id are paginated-in older entries and stay below
// the stable block. Ids missing from `items` (deleted) drop out via map-miss.
export function applyStableOrderBy<T>(
  frozen: string[] | null | undefined,
  items: T[],
  getId: (item: T) => string,
): { order: string[]; items: T[] } {
  const ids = items.map(getId);
  if (!frozen) return { order: ids, items };
  const byId = new Map(items.map((item) => [getId(item), item]));
  const frozenSet = new Set(frozen);
  const firstKnownIdx = ids.findIndex((id) => frozenSet.has(id));
  const newIds: string[] = [];
  const trailingIds: string[] = [];
  ids.forEach((id, idx) => {
    if (frozenSet.has(id)) return;
    if (firstKnownIdx === -1 || idx < firstKnownIdx) newIds.push(id);
    else trailingIds.push(id);
  });
  const order = [...newIds, ...frozen, ...trailingIds];
  const merged = order
    .map((id) => byId.get(id))
    .filter((item): item is T => item !== undefined);
  return { order, items: merged };
}

export function applyStableOrder(
  frozen: string[] | undefined,
  serverThreads: ThreadRecord[],
): { order: string[]; threads: ThreadRecord[] } {
  const { order, items } = applyStableOrderBy(frozen, serverThreads, (thread) => thread.thread_id);
  return { order, threads: items };
}

// The workspace ordering rule: an unpinned workspace never renders above a
// pinned one, and Home counts as always-pinned. It can't be assumed to
// carry is_pinned in every deployment, and pinned workspaces may still
// outrank it inside the pinned block (by sort_order/recency).
export function isEffectivelyPinned(ws: Pick<Workspace, 'is_home'> & { is_pinned?: unknown }): boolean {
  return Boolean(ws.is_pinned) || isHomeWorkspace(ws);
}

/**
 * Put the unpinned workspaces the server lists above every known unpinned one
 * at the top of the frozen order. `applyStableOrderBy` tops only rows above
 * every known row, and the pinned block (Home included) always leads the
 * list, so a workspace created this session sank below the rest instead of
 * opening the unpinned block, where a reload shows it. Rows paged in later
 * still come below the known ones.
 */
function admitNewWorkspaces(frozen: string[], workspaces: NavWorkspace[]): string[] {
  const known = new Set(frozen);
  const unpinned = workspaces.filter((ws) => !isEffectivelyPinned(ws));
  const firstKnown = unpinned.findIndex((ws) => known.has(ws.workspace_id));
  const fresh = (firstKnown === -1 ? unpinned : unpinned.slice(0, firstKnown)).map((ws) => ws.workspace_id);
  return fresh.length ? [...fresh, ...frozen] : frozen;
}

// Pinned block first, preserving relative (frozen) order within each block.
// A render-time partition rather than part of the frozen order itself: a pin
// toggle repositions the row instantly via the optimistic cache patch, without
// dropping the session freeze, and a chat bump (unshift to index 0) can only
// hoist a thread to the top of its OWN block — never above the pinned set.
export function partitionPinnedFirst(threads: ThreadRecord[]): ThreadRecord[] {
  if (!threads.some((t) => t.is_pinned)) return threads;
  const pinned: ThreadRecord[] = [];
  const rest: ThreadRecord[] = [];
  for (const t of threads) (t.is_pinned ? pinned : rest).push(t);
  return [...pinned, ...rest];
}

/** A workspace's threads in the tree's order: the frozen order, then the pinned block first. */
function orderNavThreads(wsId: string, rows: ThreadRecord[]): { order: string[]; threads: ThreadRecord[] } {
  const { order, threads } = applyStableOrder(frozenThreadOrder(wsId), rows);
  return { order, threads: partitionPinnedFirst(threads) };
}

/** Page 0 with the "Show more" rows beneath it; page 0's copy wins on overlap. */
function withShownMore(page: ThreadRecord[], stored: ThreadRecord[] | undefined): ThreadRecord[] {
  const pageIds = new Set(page.map((t) => t.thread_id));
  return [...page, ...(stored ?? []).filter((t) => !pageIds.has(t.thread_id))];
}

/**
 * A workspace's threads as the nav tree lists them, read from the cache and
 * the "Show more" store, so a surface that mounts no tree (the command
 * palette, a phone's chat) still knows which thread sits next to which.
 */
export function navThreadRows(queryClient: QueryClient, wsId: string): ThreadRecord[] {
  const stored = getNavThreadsSnapshot()[wsId]?.threads;
  const page = queryClient.getQueryData(workspaceThreadsQuery(wsId, getNavPrefs().threadPageSize).queryKey);
  if (!page) return stored ?? [];
  return orderNavThreads(wsId, withShownMore(page.threads ?? [], stored)).threads;
}

export function useNavigationData(currentWorkspaceId: string) {
  const queryClient = useQueryClient();
  const { t } = useTranslation();
  const { workspaceLimit, threadPageSize, orderBy } = useNavPrefs();
  const orderVersion = useSyncExternalStore(subscribeNavOrder, getNavOrderVersion);

  // Pure: assembles the frozen order without writing it. The commit-phase
  // effect below owns every write to the snapshot (see absorbThreadOrder).
  const orderThreads = useCallback((wsId: string, serverThreads: ThreadRecord[]) => {
    return orderNavThreads(wsId, serverThreads);
    // orderVersion isn't read in the body, but a bump rewrites the frozen
    // orders this callback closes over — its identity must change so the
    // memos downstream re-apply the new order.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [orderVersion]);

  // Workspace list via React Query. sortBy mirrors the gallery's order-by
  // selection; changing it swaps the query key and refetches in the new order.
  const wsParams = useMemo(() => ({ ...NAV_WS_PARAMS, sortBy: orderBy }), [orderBy]);
  const { data: wsData, isLoading } = useWorkspaces(wsParams);
  // Memoized so the `|| []` fallback doesn't hand every dependent memo/callback
  // a fresh array identity on each render.
  const allFetched = useMemo<NavWorkspace[]>(
    () => wsData?.workspaces || [],
    [wsData],
  );
  const totalCount = wsData?.total || 0;

  // Loaded thread lists, read from the session-global shared store so every
  // cached panel sees the same data (see lib/navThreadsStore). Writes go
  // through setSharedWorkspaceThreads, which notifies all subscribed panels.
  const workspaceThreads = useSyncExternalStore(subscribeNavThreads, getNavThreadsSnapshot);
  // "Load all" clicked this session — overrides a numeric workspaceLimit pref.
  const [showAllWorkspaces, setShowAllWorkspaces] = useState(false);
  const showAll = workspaceLimit === 'all' || showAllWorkspaces;

  // When showing all workspaces, page in the remainder beyond the first fetch.
  // Each completed page grows allFetched, re-running the effect until total is
  // reached. A failure stops the loop for the session (avoids a retry storm).
  const wsFetchRef = useRef({ inflight: false, failed: false });
  // Per-workspace single-flight for "Show more": the page offset is snapshotted
  // before the await, so two rapid taps would otherwise fetch the same page.
  const loadMoreInflightRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    if (!showAll || isLoading) return;
    if (!totalCount || allFetched.length >= totalCount) return;
    if (wsFetchRef.current.inflight || wsFetchRef.current.failed) return;
    wsFetchRef.current.inflight = true;
    getWorkspaces(100, allFetched.length, orderBy, true)
      .then((page) => {
        queryClient.setQueryData<WorkspacesResponse>(queryKeys.workspaces.list({ ...NAV_WS_PARAMS, sortBy: orderBy, offset: 0 }), (oldData) => {
          if (!oldData) return page;
          const existingIds = new Set(oldData.workspaces.map(w => w.workspace_id));
          const unique = (page.workspaces || []).filter(w => !existingIds.has(w.workspace_id));
          return { ...oldData, workspaces: [...oldData.workspaces, ...unique], total: page.total || oldData.total };
        });
      })
      .catch((e: unknown) => {
        wsFetchRef.current.failed = true;
        console.warn('[useNavigationData] Failed to load all workspaces:', e);
      })
      .finally(() => {
        wsFetchRef.current.inflight = false;
      });
  }, [showAll, isLoading, allFetched.length, totalCount, queryClient, orderBy]);

  // Workspace list in server order (pinned first, then manual sort_order, then
  // recency), frozen to its first-session arrangement so the active workspace's
  // updated_at bumps (which reorder the server response) don't hoist it.
  //
  // NOTE: this memo intentionally reads AND writes the module-level frozen-order
  // stores during render (an impure useMemo). That is safe ONLY because this
  // tree renders synchronously — no StrictMode, no useTransition/Suspense on the
  // nav path — so the factory runs once per render and can't interleave or be
  // discarded. If concurrent features are ever adopted on this path, rework the
  // ordering subsystem (this memo + reorderWorkspace) to pure-compute + an
  // effect-commit before they can tear the snapshot; the THREAD tier already
  // works that way (orderThreads + absorbThreadOrder).
  const workspaces = useMemo(() => {
    if (!allFetched.length) return [];

    let ordered: NavWorkspace[];
    if (orderBy === 'custom') {
      // If an existing workspace's manual arrangement (sort_order or pin state)
      // changed since the last render, a reorder/pin happened — here or in the
      // workspace gallery — so drop the frozen order and re-snapshot from the
      // fresh server order, keeping the nav in sync with the gallery's 'custom'
      // sort. Recency bumps and paged-in workspaces don't change these fields.
      let manualOrderChanged = false;
      for (const ws of allFetched) {
        const sortOrder = (ws.sort_order as number | null | undefined) ?? null;
        const pinned = Boolean(ws.is_pinned);
        const prev = lastWorkspaceArrangement.get(ws.workspace_id);
        if (prev && (prev.sortOrder !== sortOrder || prev.pinned !== pinned)) manualOrderChanged = true;
        lastWorkspaceArrangement.set(ws.workspace_id, { sortOrder, pinned });
      }
      if (manualOrderChanged) setFrozenWorkspaceOrder(null);
      const previous = getFrozenWorkspaceOrder();
      const frozen = previous && admitNewWorkspaces(previous, allFetched);

      const { order, items: stable } = applyStableOrderBy(frozen, allFetched, (ws) => ws.workspace_id);
      setFrozenWorkspaceOrder(order);
      ordered = stable;
    } else {
      // 'activity' / 'name': the server already returned the list in this order
      // (the query's sortBy). Trust it and don't freeze — recency hoisting is
      // the point of 'activity'. Drop any custom snapshot so switching back to
      // 'custom' re-snapshots fresh from the server's manual order.
      setFrozenWorkspaceOrder(null);
      ordered = allFetched;
    }

    // Pinned block first (Home counts as always-pinned), preserving relative
    // order within each block — the workspace-tier partitionPinnedFirst. The
    // sources can't guarantee the invariant on their own: the frozen snapshot
    // may predate a pin flip, and Home isn't guaranteed is_pinned in
    // the DB. Render-time (not baked into the freeze) so an optimistic pin
    // patch repositions the row instantly, like the thread partition.
    const pinnedBlock: NavWorkspace[] = [];
    const unpinnedBlock: NavWorkspace[] = [];
    for (const ws of ordered) (isEffectivelyPinned(ws) ? pinnedBlock : unpinnedBlock).push(ws);
    if (pinnedBlock.length && unpinnedBlock.length) ordered = [...pinnedBlock, ...unpinnedBlock];

    if (showAll) return ordered;

    const sliced = ordered.slice(0, workspaceLimit as number);
    if (currentWorkspaceId && !sliced.some((ws) => ws.workspace_id === currentWorkspaceId)) {
      const currentWs = allFetched.find((ws) => ws.workspace_id === currentWorkspaceId);
      // Keep the current workspace in view without hoisting it to the top —
      // it joins at the bottom of the visible slice, holding the list stable.
      if (currentWs) sliced.push(currentWs);
    }
    return sliced;
    // orderVersion isn't read in the body, but drag-reorder rewrites the frozen
    // workspace order this memo reads — it must recompute when the version ticks.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allFetched, showAll, workspaceLimit, currentWorkspaceId, orderVersion, orderBy]);

  // Drag-reorder a workspace in the nav list. Optimistically rewrites the
  // frozen nav order, then persists sequential sort_order values so the
  // workspace gallery's 'custom' sort shows the same arrangement. Mirrors the
  // gallery grid's drag rules: crossing the pinned/unpinned boundary is
  // refused (the server sort would undo it — is_pinned DESC dominates
  // sort_order); Home counts as pinned and reorders within that block.
  const reorderWorkspace = useCallback(async (activeId: string, overId: string) => {
    const frozen = getFrozenWorkspaceOrder();
    if (!frozen || !activeId || !overId || activeId === overId) return;
    const byId = new Map(allFetched.map((ws) => [ws.workspace_id, ws]));
    const active = byId.get(activeId);
    const over = byId.get(overId);
    if (!active || !over) return;
    // No Home special case: it counts as pinned, so this boundary guard
    // both contains it in the pinned block and keeps unpinned rows out.
    if (isEffectivelyPinned(active) !== isEffectivelyPinned(over)) return;
    const fromIdx = frozen.indexOf(activeId);
    const toIdx = frozen.indexOf(overId);
    if (fromIdx === -1 || toIdx === -1) return;

    const snapshot = [...frozen];
    frozen.splice(fromIdx, 1);
    frozen.splice(toIdx, 0, activeId);
    bumpNavOrder();

    // Home is included: it's DB-pinned with a real sort_order, and writing
    // its slot is what makes "pinned workspace above/below Home" stick.
    // Omitting it leaves a sort_order tie decided by updated_at, so the
    // pinned block would reshuffle whenever Home is used.
    const items = frozen
      .map((id) => byId.get(id))
      .filter((ws): ws is NavWorkspace => !!ws)
      .map((ws, i) => ({ workspace_id: ws.workspace_id, sort_order: i }));
    // Every cached list in the custom order takes the new order at once, the
    // gallery's included, as a drop there does: a drag in the gallery before
    // the save lands then moves from this order, not the one before it.
    const previous = rankCachedWorkspaces(queryClient, items);
    try {
      await reorderWorkspaces(items);
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    } catch (e) {
      console.warn('[useNavigationData] Failed to persist workspace order:', e);
      unrankCachedWorkspaces(queryClient, previous);
      setFrozenWorkspaceOrder(snapshot);
      bumpNavOrder();
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    }
  }, [allFetched, queryClient]);

  // Pin/unpin a workspace through the canonical row action — the gallery calls
  // the same one, so an unpin made there releases this tree's freeze too.
  const pinWorkspace = useCallback((wsId: string, pinned: boolean) => {
    return pinWorkspaceRow(queryClient, wsId, pinned);
  }, [queryClient]);

  // Rename a workspace. No-op on blank/unchanged names is enforced by the caller.
  // The inline field closes before the server answers, so a refusal (most
  // often a taken name) can only be told in a toast.
  const renameWorkspace = useCallback((wsId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) return Promise.resolve(false);
    return renameWorkspaceRow(queryClient, wsId, trimmed, {
      onError: (err) => toast({
        variant: 'destructive',
        title: t('workspace.renameFailed'),
        description: denialMessage(err, t),
      }),
    });
  }, [queryClient, t]);

  const hasMore = useMemo(() => {
    if (showAll) return false;
    if ((workspaceLimit as number) < allFetched.length) return true;
    if (allFetched.length < totalCount) return true;
    return false;
  }, [showAll, workspaceLimit, allFetched.length, totalCount]);

  const { isLoading: currentWsThreadsLoading } = useQuery({
    ...workspaceThreadsQuery(currentWorkspaceId, threadPageSize),
    enabled: !!currentWorkspaceId,
  });

  // Every workspace whose page-0 rows this tree renders: the current one plus
  // whatever the shared "Show more" store retains. Sorted so the observer list
  // is stable across renders.
  const observedWsIds = useMemo(() => {
    const ids = new Set(Object.keys(workspaceThreads));
    if (currentWorkspaceId) ids.add(currentWorkspaceId);
    return [...ids].sort();
  }, [workspaceThreads, currentWorkspaceId]);

  // Cache-ONLY observers over those exact page-0 keys. A patch to a row this
  // panel isn't otherwise subscribed to (a generated title landing for another
  // workspace's thread) must repaint immediately, but ENABLED queries would
  // refetch every retained workspace on every mounted panel — so these observe
  // without ever fetching. `combine` returns the data refs, which
  // replaceEqualDeep keeps identity-stable while nothing changes.
  // Every id here is a real workspace, so these may be thawed after a
  // lifecycle-feed resync — see CACHE_ONLY_META for what that opt-in attests to.
  const pageData = useQueries({
    queries: observedWsIds.map((wsId) => ({
      ...workspaceThreadsQuery(wsId, threadPageSize),
      enabled: false,
      meta: CACHE_ONLY_META,
    })),
    combine: (results) => results.map((r) => r.data),
  });

  // Each workspace keeps its previous ThreadsData object while its rows are
  // the same records: the sidebar rows are memoized on that identity, and a
  // refetch that changed one thread used to rebuild every workspace's block.
  const prevThreadsByWsRef = useRef<Record<string, ThreadsData>>({});
  const merged = useMemo(() => {
    const threadsByWs: Record<string, ThreadsData> = {};
    const orders: Record<string, string[]> = {};
    const prev = prevThreadsByWsRef.current;
    const keep = (wsId: string, next: ThreadsData): ThreadsData => {
      const last = prev[wsId];
      const same = !!last && last.loading === next.loading && last.total === next.total
        && last.threads.length === next.threads.length && last.threads.every((t, i) => t === next.threads[i]);
      return same ? last : next;
    };

    // Every workspace group (not just the current one) renders page 0 read
    // through the query cache, with any "Show more" pages from the shared
    // store beneath it (cache wins on overlap). The cache is the single
    // authority for row content, so a patched row can never be shadowed by a
    // stale store snapshot; the store's page-0 copy only paints when the
    // cache entry is missing (never fetched, or gc'd while unobserved).
    observedWsIds.forEach((wsId, index) => {
      const stored = workspaceThreads[wsId];
      const isCurrent = wsId === currentWorkspaceId;
      const loading = (isCurrent && currentWsThreadsLoading) || stored?.loading || false;
      const page = pageData[index];
      if (page === undefined) {
        threadsByWs[wsId] = keep(wsId, { threads: stored?.threads || [], loading, total: stored?.total });
        return;
      }
      const { order, threads } = orderThreads(wsId, withShownMore(page.threads || [], stored?.threads));
      orders[wsId] = order;
      threadsByWs[wsId] = keep(wsId, { threads, loading, total: page.total ?? stored?.total });
    });

    prevThreadsByWsRef.current = threadsByWs;
    return { threadsByWs, orders };
  }, [observedWsIds, pageData, workspaceThreads, currentWorkspaceId, currentWsThreadsLoading, orderThreads]);

  // The one writer for the thread tier's frozen order (see absorbThreadOrder).
  useEffect(() => {
    for (const [wsId, order] of Object.entries(merged.orders)) absorbThreadOrder(wsId, order);
  }, [merged]);

  const mergedThreads = merged.threadsByWs;
  // Read by the handlers below at call time, so their identity does not
  // follow the thread data: each is a prop of every memoized sidebar row.
  const mergedThreadsRef = useRef(mergedThreads);
  mergedThreadsRef.current = mergedThreads;

  const expandWorkspace = useCallback((wsId: string) => {
    const mergeFetched = (data: ThreadsResponse) => {
      // Keep already-paged-in threads; the fetched first page wins on overlap.
      const have = getNavThreadsSnapshot()[wsId]?.threads || [];
      const pageIds = new Set((data.threads || []).map((t) => t.thread_id));
      const extras = have.filter((t) => !pageIds.has(t.thread_id));
      const { order, threads } = orderThreads(wsId, [...(data.threads || []), ...extras]);
      absorbThreadOrder(wsId, order);
      setSharedWorkspaceThreads(prev => ({
        ...prev,
        [wsId]: { threads, loading: false, total: data.total ?? prev[wsId]?.total },
      }));
    };

    const threadsKey = workspaceThreadsQuery(wsId, threadPageSize).queryKey;
    const cached = queryClient.getQueryData(threadsKey);
    if (cached) {
      mergeFetched(cached);
      return;
    }

    setSharedWorkspaceThreads(prev => ({
      ...prev,
      [wsId]: { threads: prev[wsId]?.threads || [], loading: true, total: prev[wsId]?.total },
    }));

    queryClient.fetchQuery(workspaceThreadsQuery(wsId, threadPageSize)).then(mergeFetched).catch(() => {
      setSharedWorkspaceThreads(prev => ({
        ...prev,
        [wsId]: { threads: prev[wsId]?.threads || [], loading: false, total: prev[wsId]?.total },
      }));
    });
  }, [queryClient, orderThreads, threadPageSize]);

  // Show more: grow the prefix (limit = shown + page, offset 0) rather than
  // offset-paging the tail. `offset = shown.length` is only correct while the
  // local list is an exact prefix of the live server order — threads created
  // above a stale head or archived from another surface break that silently,
  // and the page at the offset comes back all-duplicates or empty (dead button
  // until reload). Re-requesting the current top costs a few small rows and
  // can't desync; when nothing new exists, the fresh total retires the button.
  const loadMoreThreads = useCallback(async (wsId: string) => {
    if (loadMoreInflightRef.current.has(wsId)) return;
    loadMoreInflightRef.current.add(wsId);
    const shown = mergedThreadsRef.current[wsId]?.threads || [];
    setSharedWorkspaceThreads(prev => ({
      ...prev,
      [wsId]: {
        threads: prev[wsId]?.threads || shown,
        loading: true,
        total: prev[wsId]?.total ?? mergedThreadsRef.current[wsId]?.total,
      },
    }));
    try {
      const data = await getWorkspaceThreads(wsId, shown.length + threadPageSize, 0) as ThreadsResponse;
      const stored = getNavThreadsSnapshot()[wsId];
      const have = stored?.threads?.length ? stored.threads : shown;
      // Server copies win on overlap (same rule as expandWorkspace's merge).
      // Local rows the response no longer carries stay beneath the stable
      // block ONLY while the response is a full page (the row may merely have
      // sunk below the requested prefix). A short response is the complete
      // list — a row missing from it is genuinely gone (archived/deleted in
      // another tab), and keeping it would pin a ghost until reload.
      const pageRows = data.threads || [];
      const pageIds = new Set(pageRows.map((t) => t.thread_id));
      const isCompleteList = pageRows.length < shown.length + threadPageSize;
      const extras = isCompleteList ? [] : have.filter((t) => !pageIds.has(t.thread_id));
      const { order, threads } = orderThreads(wsId, [...pageRows, ...extras]);
      // The response head IS the current page-0 — write it and the fresh total
      // back to the cache entry, which the render path treats as the authority
      // for both. Without this a stale cached total keeps the button alive
      // after the server list shrank (rows archived from another surface).
      queryClient.setQueryData(workspaceThreadsQuery(wsId, threadPageSize).queryKey, (prev) => ({
        limit: threadPageSize, offset: 0, ...prev, threads: pageRows.slice(0, threadPageSize), total: data.total ?? prev?.total,
      }));
      absorbThreadOrder(wsId, order);
      setSharedWorkspaceThreads(prev => ({
        ...prev,
        [wsId]: { threads, loading: false, total: data.total ?? prev[wsId]?.total },
      }));
    } catch (e) {
      console.warn('[useNavigationData] Failed to load more threads:', e);
      setSharedWorkspaceThreads(prev => ({
        ...prev,
        [wsId]: {
          threads: prev[wsId]?.threads || shown,
          loading: false,
          total: prev[wsId]?.total,
        },
      }));
    } finally {
      loadMoreInflightRef.current.delete(wsId);
    }
  }, [threadPageSize, orderThreads, queryClient]);

  const loadAll = useCallback(() => {
    // The page-in effect above fetches the remainder once this flips.
    setShowAllWorkspaces(true);
    wsFetchRef.current.failed = false;
  }, []);

  // `canReorderWorkspaces` is false under activity/name: drag-reorder is a
  // 'custom'-order action (a drop persists sort_order the view wouldn't
  // reflect), so the consumer withholds the handler to disable the affordance.
  // Mirrors the gallery, where reordering only applies to the custom arrangement.
  return { workspaces, workspaceThreads: mergedThreads, loading: isLoading, hasMore, loadAll, expandWorkspace, loadMoreThreads, reorderWorkspace, canReorderWorkspaces: orderBy === 'custom', pinWorkspace, renameWorkspace };
}
