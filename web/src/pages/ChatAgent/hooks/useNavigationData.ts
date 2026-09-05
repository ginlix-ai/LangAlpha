import { useState, useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { CACHE_ONLY_META, queryKeys } from '../../../lib/queryKeys';
import { createEmitter } from '@/lib/emitter';
import { isArchivedThreadsKey, patchThreadRows, rollbackThreadRows } from '@/lib/threadRowActions';
import type { ThreadRowMapper } from '@/lib/threadRowActions';
import {
  getNavThreadsSnapshot,
  setSharedWorkspaceThreads,
  subscribeNavThreads,
  type ThreadRecord,
  type ThreadsData,
} from '@/lib/navThreadsStore';
import type { ResourceTier, WorkspacesResponse } from '@/types/api';
import { getWorkspaces, getWorkspaceThreads, reorderWorkspaces, updateThread } from '../utils/api';
import { pinWorkspaceRow, renameWorkspaceRow } from './workspaceRowActions';
import { useNavPrefs } from '../utils/navPrefs';

/**
 * Workspace row as the nav tree consumes it: the fields the tree and its
 * options menu render, plus the loose tail the list endpoint carries. Exported
 * so hosts render the tree without re-casting the hook's output.
 */
export interface NavWorkspace {
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

interface ThreadsResponse {
  threads: ThreadRecord[];
  total?: number;
  [key: string]: unknown;
}

const NAV_WS_PARAMS = { limit: 20, includeFlash: true };

// Page size is part of the key (mirrors the dashboard widgets' thread keys):
// the queryFn fetches `pageSize` rows, so changing that pref must miss the
// cache and refetch instead of replaying the previous page size.
const threadPageKey = (wsId: string, pageSize: number) => [...queryKeys.threads.byWorkspace(wsId), pageSize, 0];

// Session-stable nav ordering. The server sorts threads (and, within the
// 'custom' workspace sort, unpinned workspaces) by updated_at DESC, so the item
// being chatted in would hoist to the top on every refetch. We snapshot the
// order seen first in this page session and reorder later responses to match.
// The stores are module-level because ChatAgent caches one ChatView instance
// per thread — hook-local refs would re-snapshot on every thread switch,
// defeating the freeze. Reload starts a fresh session (fresh recency order).
const _frozenThreadOrders: Record<string, string[]> = {};
let _frozenWorkspaceOrder: string[] | null = null;

// Last-seen manual arrangement (sort_order + pin state) per workspace. The
// frozen order above neutralizes recency hoisting, but a reorder/pin made in
// the workspace gallery also lands as a refetch — we must NOT freeze that out.
// When an existing workspace's sort_order or pin state changes between
// refetches, we re-snapshot so the nav tracks the gallery's 'custom' sort.
// Recency (updated_at) bumps and newly paged-in workspaces don't change these
// values, so the active workspace still never hoists itself.
const _lastWorkspaceArrangement = new Map<string, { sortOrder: number | null; pinned: boolean }>();

export function resetStableNavOrder() {
  for (const key of Object.keys(_frozenThreadOrders)) delete _frozenThreadOrders[key];
  _frozenWorkspaceOrder = null;
  _lastWorkspaceArrangement.clear();
}

/** Parked subscription for a hook instance running with `enabled: false`. */
const subscribeNever = (): (() => void) => () => {};

// Drop a deleted workspace from the frozen-order stores so they don't retain
// ghost ids for the rest of the session. Deleted ids are already filtered out
// of the rendered list (applyStableOrderBy drops map-misses), so this is
// housekeeping, not a correctness fix — call it from the workspace delete path.
export function forgetStableNavOrder(workspaceId: string) {
  delete _frozenThreadOrders[workspaceId];
  _lastWorkspaceArrangement.delete(workspaceId);
  if (_frozenWorkspaceOrder) {
    const idx = _frozenWorkspaceOrder.indexOf(workspaceId);
    if (idx !== -1) _frozenWorkspaceOrder.splice(idx, 1);
  }
}

// Bump notifications: chatting in a thread moves it to the top of its
// workspace's list (like normal chat apps), while clicking around never
// reorders. Subscribed hooks re-apply the frozen orders when the version ticks.
let _navOrderVersion = 0;
const navOrderEmitter = createEmitter();

/** Announce a deliberate reordering; every mounted nav panel re-applies the frozen orders. */
function bumpNavOrder(): void {
  _navOrderVersion++;
  navOrderEmitter.emit();
}

/**
 * Release the WORKSPACE tier's session freeze and re-render every nav panel.
 * The pin path needs exactly this after its refetch: `resetStableNavOrder`
 * also clears the thread tier, `forgetStableNavOrder` is workspace-scoped, and
 * neither notifies — nothing re-renders on a bare module-store write.
 */
export function resetWorkspaceOrderFreeze(): void {
  _frozenWorkspaceOrder = null;
  bumpNavOrder();
}

/**
 * Move a thread to the top of its workspace's frozen order. Called when the
 * user sends a message (new turn, steering, edit/regenerate/retry). No-op if
 * the workspace's order hasn't been snapshotted yet — the initial snapshot is
 * recency-sorted, so the thread lands on top anyway.
 */
export function bumpThreadNavOrder(wsId: string, threadId: string | null | undefined) {
  if (!wsId || !threadId || threadId === '__default__') return;
  const frozen = _frozenThreadOrders[wsId];
  if (!frozen) return;
  const idx = frozen.indexOf(threadId);
  if (idx === 0) return;
  if (idx > 0) frozen.splice(idx, 1);
  frozen.unshift(threadId);
  bumpNavOrder();
}

/**
 * Commit-phase writer for the thread tier's frozen order: store the assembled
 * order whenever it carries ids the snapshot doesn't have (or there is no
 * snapshot yet). Silent — no version bump — because this is absorption, not a
 * reordering. It covers three roles the old render-phase write served: the
 * first-load snapshot (`bumpThreadNavOrder` no-ops without one), the re-freeze
 * after a pin deletes the snapshot, and continuous absorption of new ids —
 * without the last, `applyStableOrderBy` re-classifies an unfrozen id
 * positionally every render and sinks a fresh thread to the bottom of the tree
 * as soon as an older frozen thread sorts above it.
 */
export function absorbThreadOrder(wsId: string, order: string[]): void {
  const frozen = _frozenThreadOrders[wsId];
  if (frozen) {
    const known = new Set(frozen);
    if (order.every((id) => known.has(id))) return;
  }
  _frozenThreadOrders[wsId] = order;
}

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
// pinned one, and Flash counts as always-pinned — it can't be assumed to
// carry is_pinned in every deployment, and pinned workspaces may still
// outrank it inside the pinned block (by sort_order/recency).
export function isEffectivelyPinned(ws: { workspace_id?: unknown; is_pinned?: unknown; status?: unknown }): boolean {
  return Boolean(ws.is_pinned) || ws.status === 'flash';
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

export interface UseNavigationDataOptions {
  /** Desktop ChatViews keep a mobile-only nav drawer mounted but never shown;
   *  `false` parks this hook's queries and store subscriptions so five cached
   *  views stop paying for a tree nobody renders. */
  enabled?: boolean;
}

export function useNavigationData(currentWorkspaceId: string, { enabled = true }: UseNavigationDataOptions = {}) {
  const queryClient = useQueryClient();
  const { workspaceLimit, threadPageSize, orderBy } = useNavPrefs();
  const orderVersion = useSyncExternalStore(
    enabled ? navOrderEmitter.subscribe : subscribeNever,
    () => _navOrderVersion,
  );

  // Pure: assembles the frozen order without writing it. The commit-phase
  // effect below owns every write to the snapshot (see absorbThreadOrder).
  const orderThreads = useCallback((wsId: string, serverThreads: ThreadRecord[]) => {
    const { order, threads } = applyStableOrder(_frozenThreadOrders[wsId], serverThreads);
    return { order, threads: partitionPinnedFirst(threads) };
    // orderVersion isn't read in the body, but a bump rewrites the frozen
    // orders this callback closes over — its identity must change so the
    // memos downstream re-apply the new order.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [orderVersion]);

  // Workspace list via React Query. sortBy mirrors the gallery's order-by
  // selection; changing it swaps the query key and refetches in the new order.
  const wsParams = useMemo(() => ({ ...NAV_WS_PARAMS, sortBy: orderBy, enabled }), [orderBy, enabled]);
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
  const workspaceThreads = useSyncExternalStore(
    enabled ? subscribeNavThreads : subscribeNever,
    getNavThreadsSnapshot,
  );
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
    if (!enabled || !showAll || isLoading) return;
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
  }, [enabled, showAll, isLoading, allFetched.length, totalCount, queryClient, orderBy]);

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
        const prev = _lastWorkspaceArrangement.get(ws.workspace_id);
        if (prev && (prev.sortOrder !== sortOrder || prev.pinned !== pinned)) manualOrderChanged = true;
        _lastWorkspaceArrangement.set(ws.workspace_id, { sortOrder, pinned });
      }
      if (manualOrderChanged) _frozenWorkspaceOrder = null;

      const { order, items: stable } = applyStableOrderBy(_frozenWorkspaceOrder, allFetched, (ws) => ws.workspace_id);
      _frozenWorkspaceOrder = order;
      ordered = stable;
    } else {
      // 'activity' / 'name': the server already returned the list in this order
      // (the query's sortBy). Trust it and don't freeze — recency hoisting is
      // the point of 'activity'. Drop any custom snapshot so switching back to
      // 'custom' re-snapshots fresh from the server's manual order.
      _frozenWorkspaceOrder = null;
      ordered = allFetched;
    }

    // Pinned block first (Flash counts as always-pinned), preserving relative
    // order within each block — the workspace-tier partitionPinnedFirst. The
    // sources can't guarantee the invariant on their own: the frozen snapshot
    // may predate a pin flip, and a flash row isn't guaranteed is_pinned in
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
  // gallery's reorder-mode rules: crossing the pinned/unpinned boundary is
  // refused (the server sort would undo it — is_pinned DESC dominates
  // sort_order); flash counts as pinned and reorders within that block.
  const reorderWorkspace = useCallback(async (activeId: string, overId: string) => {
    const frozen = _frozenWorkspaceOrder;
    if (!frozen || !activeId || !overId || activeId === overId) return;
    const byId = new Map(allFetched.map((ws) => [ws.workspace_id, ws]));
    const active = byId.get(activeId);
    const over = byId.get(overId);
    if (!active || !over) return;
    // No flash special case: it counts as pinned, so this boundary guard
    // both contains it in the pinned block and keeps unpinned rows out.
    if (isEffectivelyPinned(active) !== isEffectivelyPinned(over)) return;
    const fromIdx = frozen.indexOf(activeId);
    const toIdx = frozen.indexOf(overId);
    if (fromIdx === -1 || toIdx === -1) return;

    const snapshot = [...frozen];
    frozen.splice(fromIdx, 1);
    frozen.splice(toIdx, 0, activeId);
    bumpNavOrder();

    // Flash is included: it's DB-pinned with a real sort_order, and writing
    // its slot is what makes "pinned workspace above/below Flash" stick —
    // omitting it leaves a sort_order tie decided by updated_at, so the
    // pinned block would reshuffle whenever Flash is used.
    const items = frozen
      .map((id) => byId.get(id))
      .filter((ws): ws is NavWorkspace => !!ws)
      .map((ws, i) => ({ workspace_id: ws.workspace_id, sort_order: i }));
    try {
      await reorderWorkspaces(items);
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    } catch (e) {
      console.warn('[useNavigationData] Failed to persist workspace order:', e);
      _frozenWorkspaceOrder = snapshot;
      bumpNavOrder();
    }
  }, [allFetched, queryClient]);

  // Pin/unpin a workspace through the canonical row action — the gallery calls
  // the same one, so an unpin made there releases this tree's freeze too.
  const pinWorkspace = useCallback((wsId: string, pinned: boolean) => {
    return pinWorkspaceRow(queryClient, wsId, pinned);
  }, [queryClient]);

  // Rename a workspace. No-op on blank/unchanged names is enforced by the caller.
  const renameWorkspace = useCallback((wsId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) return Promise.resolve(false);
    return renameWorkspaceRow(queryClient, wsId, trimmed);
  }, [queryClient]);

  // Optimistically patch (or, with `remove`, drop) one thread row across every
  // cached list for the workspace — finite sidebar pages, the gallery's
  // infinite entries, and the shared "Show more" store (patchThreadRows covers
  // all three) — persist via PATCH /threads/{id}, then invalidate so the
  // server truth lands. Rolls everything back on failure. Shared by pin and archive.
  const patchThread = useCallback(async (
    wsId: string,
    threadId: string,
    updates: { is_pinned?: boolean; archived?: boolean },
    { remove = false }: { remove?: boolean } = {},
  ) => {
    const applyRows: ThreadRowMapper<ThreadRecord> = (rows) => {
      if (!rows.some((t) => t.thread_id === threadId)) return rows;
      return remove
        ? rows.filter((t) => t.thread_id !== threadId)
        : rows.map((t) => (t.thread_id === threadId ? { ...t, ...updates } : t));
    };

    const previousRows = patchThreadRows<ThreadRecord>(
      queryClient,
      queryKeys.threads.byWorkspace(wsId),
      applyRows,
      // Archiving drops the row from the default lists but it BELONGS in the
      // archived view, so that entry is left to the invalidate below.
      remove ? { skipKey: isArchivedThreadsKey } : {},
    );
    try {
      await updateThread(threadId, updates);
    } catch (e) {
      rollbackThreadRows(queryClient, previousRows);
      console.warn('[useNavigationData] Failed to update thread:', e);
      // Surface failure to callers (archive gates navigation on it) without
      // rejecting — pin/archive buttons call this fire-and-forget.
      return false;
    }
    // Past this point the server has applied the change: a failure in the
    // refresh below must NOT roll back the optimistic patch (it matches the
    // durable state) and must still release the frozen order.
    if ('is_pinned' in updates) {
      // Pin-state changes drop the workspace's frozen order (same rule as
      // the workspace tree's manualOrderChanged re-snapshot) so an unpinned
      // row falls back to its recency slot instead of squatting at the top
      // for the session. The reset must land AFTER a real refetch: any
      // render while the cache is merely marked stale would re-freeze the
      // OLD order and swallow the reset. The page-0 entry is fetched
      // explicitly — this tree's cache-only observers make that query
      // "disabled", and refetchQueries skips disabled queries — while the
      // prefix invalidate covers the gallery views, which own live
      // observers of their own.
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(wsId) });
      try {
        await queryClient.fetchQuery({
          queryKey: threadPageKey(wsId, threadPageSize),
          queryFn: () => getWorkspaceThreads(wsId, threadPageSize, 0),
          staleTime: 0,
        });
      } catch (e) {
        // Refetch failure: release the freeze anyway — re-snapshotting from
        // the optimistically patched cache beats a stuck frozen order.
        console.warn('[useNavigationData] Post-pin refetch failed:', e);
      }
      delete _frozenThreadOrders[wsId];
      bumpNavOrder();
    } else {
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(wsId) });
    }
    return true;
  }, [queryClient, threadPageSize]);

  // Pin/unpin a thread. The pinned-first partition in orderThreads repositions
  // the row from the optimistic is_pinned patch alone — no freeze reset needed.
  const pinThread = useCallback((wsId: string, threadId: string, pinned: boolean) => {
    return patchThread(wsId, threadId, { is_pinned: pinned });
  }, [patchThread]);

  // Archive a thread: the row leaves every default list immediately (archived
  // rows only exist in the gallery's explicit archived view).
  const archiveThread = useCallback((wsId: string, threadId: string) => {
    return patchThread(wsId, threadId, { archived: true }, { remove: true });
  }, [patchThread]);

  const hasMore = useMemo(() => {
    if (showAll) return false;
    if ((workspaceLimit as number) < allFetched.length) return true;
    if (allFetched.length < totalCount) return true;
    return false;
  }, [showAll, workspaceLimit, allFetched.length, totalCount]);

  const { isLoading: currentWsThreadsLoading } = useQuery({
    queryKey: threadPageKey(currentWorkspaceId, threadPageSize),
    queryFn: () => getWorkspaceThreads(currentWorkspaceId, threadPageSize, 0),
    enabled: enabled && !!currentWorkspaceId,
    staleTime: 30_000,
  });

  // Every workspace whose page-0 rows this tree renders: the current one plus
  // whatever the shared "Show more" store retains. Sorted so the observer list
  // is stable across renders.
  const observedWsIds = useMemo(() => {
    if (!enabled) return [];
    const ids = new Set(Object.keys(workspaceThreads));
    if (currentWorkspaceId) ids.add(currentWorkspaceId);
    return [...ids].sort();
  }, [enabled, workspaceThreads, currentWorkspaceId]);

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
      queryKey: threadPageKey(wsId, threadPageSize),
      queryFn: () => getWorkspaceThreads(wsId, threadPageSize, 0),
      enabled: false,
      meta: CACHE_ONLY_META,
      staleTime: 30_000,
    })),
    combine: (results) => results.map((r) => r.data as ThreadsResponse | undefined),
  });

  const merged = useMemo(() => {
    const threadsByWs: Record<string, ThreadsData> = {};
    const orders: Record<string, string[]> = {};

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
        threadsByWs[wsId] = { threads: stored?.threads || [], loading, total: stored?.total };
        return;
      }
      const pageThreads = page.threads || [];
      const pageIds = new Set(pageThreads.map((t) => t.thread_id));
      const extras = (stored?.threads || []).filter((t) => !pageIds.has(t.thread_id));
      const { order, threads } = orderThreads(wsId, [...pageThreads, ...extras]);
      orders[wsId] = order;
      threadsByWs[wsId] = { threads, loading, total: page.total ?? stored?.total };
    });

    return { threadsByWs, orders };
  }, [observedWsIds, pageData, workspaceThreads, currentWorkspaceId, currentWsThreadsLoading, orderThreads]);

  // The one writer for the thread tier's frozen order (see absorbThreadOrder).
  useEffect(() => {
    for (const [wsId, order] of Object.entries(merged.orders)) absorbThreadOrder(wsId, order);
  }, [merged]);

  const mergedThreads = merged.threadsByWs;

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

    const cached = queryClient.getQueryData(threadPageKey(wsId, threadPageSize)) as ThreadsResponse | undefined;
    if (cached) {
      mergeFetched(cached);
      return;
    }

    setSharedWorkspaceThreads(prev => ({
      ...prev,
      [wsId]: { threads: prev[wsId]?.threads || [], loading: true, total: prev[wsId]?.total },
    }));

    queryClient.fetchQuery({
      queryKey: threadPageKey(wsId, threadPageSize),
      queryFn: () => getWorkspaceThreads(wsId, threadPageSize, 0),
      staleTime: 30_000,
    }).then((data: unknown) => {
      mergeFetched(data as ThreadsResponse);
    }).catch(() => {
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
    const shown = mergedThreads[wsId]?.threads || [];
    setSharedWorkspaceThreads(prev => ({
      ...prev,
      [wsId]: {
        threads: prev[wsId]?.threads || shown,
        loading: true,
        total: prev[wsId]?.total ?? mergedThreads[wsId]?.total,
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
      queryClient.setQueryData(threadPageKey(wsId, threadPageSize), (old: unknown) => {
        const prev = old as ThreadsResponse | undefined;
        return { ...(prev ?? {}), threads: pageRows.slice(0, threadPageSize), total: data.total ?? prev?.total };
      });
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
  }, [mergedThreads, threadPageSize, orderThreads, queryClient]);

  const loadAll = useCallback(() => {
    // The page-in effect above fetches the remainder once this flips.
    setShowAllWorkspaces(true);
    wsFetchRef.current.failed = false;
  }, []);

  // `canReorderWorkspaces` is false under activity/name: drag-reorder is a
  // 'custom'-order action (a drop persists sort_order the view wouldn't
  // reflect), so the consumer withholds the handler to disable the affordance.
  // Mirrors the gallery, where reordering only applies to the custom arrangement.
  return { workspaces, workspaceThreads: mergedThreads, loading: isLoading, hasMore, loadAll, expandWorkspace, loadMoreThreads, reorderWorkspace, canReorderWorkspaces: orderBy === 'custom', pinWorkspace, renameWorkspace, pinThread, archiveThread };
}
