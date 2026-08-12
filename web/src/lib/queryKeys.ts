import type { QueryMeta } from '@tanstack/react-query';

/**
 * Hierarchical query key factory for React Query, plus the query `meta`
 * contract that rides alongside it (see {@link CACHE_ONLY_META}).
 *
 * Each level builds on its parent to enable prefix-based invalidation:
 *   invalidateQueries({ queryKey: queryKeys.user.all })
 *     → invalidates me, preferences, apiKeys
 *   invalidateQueries({ queryKey: queryKeys.workspaces.lists() })
 *     → invalidates all workspace list queries (any page/sort)
 */
export const queryKeys = {
  user: {
    all:         ['user'],
    me:          () => [...queryKeys.user.all, 'me'],
    preferences: () => [...queryKeys.user.all, 'preferences'],
    apiKeys:     () => [...queryKeys.user.all, 'api-keys'],
  },
  models: {
    all: ['models'],
  },
  features: {
    all:  ['features'],
    list: () => [...queryKeys.features.all, 'list'],
  },
  platform: {
    all:    ['platform'],
    models: () => [...queryKeys.platform.all, 'models'],
  },
  oauth: {
    all:    ['oauth'],
    codex:  () => [...queryKeys.oauth.all, 'codex'],
    claude: () => [...queryKeys.oauth.all, 'claude'],
  },
  workspaces: {
    all:    ['workspaces'],
    lists:  () => [...queryKeys.workspaces.all, 'list'],
    list:   (params: Record<string, unknown>) => [...queryKeys.workspaces.lists(), params],
    detail: (id: string) => [...queryKeys.workspaces.all, 'detail', id],
    flash:  () => [...queryKeys.workspaces.all, 'flash'],
    quota:  () => [...queryKeys.workspaces.all, 'quota'],
  },
  threads: {
    all:         ['threads'],
    byWorkspace: (wsId: string) => [...queryKeys.threads.all, 'workspace', wsId],
    // ThreadGallery's infinite list. Deliberately UNDER the byWorkspace prefix
    // so the lifecycle feed's prefix invalidation and the gallery's own
    // self-invalidations keep reaching it; the suffix keeps it distinct from
    // the sidebar's finite page entries, which cannot hold InfiniteData.
    gallery:     (wsId: string, archived: boolean) => [...queryKeys.threads.byWorkspace(wsId), { view: 'gallery', archived }],
    detail:      (threadId: string) => [...queryKeys.threads.all, 'detail', threadId],
    // Base for every recent-list variant — invalidation targets this prefix.
    recentAll:   () => [...queryKeys.threads.all, 'recent'],
    recent:      (limit: number) => [...queryKeys.threads.recentAll(), limit],
    status:      (threadId: string) => [...queryKeys.threads.all, 'status', threadId],
    // Batched dispatch-liveness read for a turn's PTC cards. The base key
    // targets every id-set variant for invalidation; the concrete key is stable
    // on the SORTED id set so registration order never churns the cache entry.
    dispatchLivenessAll: () => [...queryKeys.threads.all, 'dispatch-liveness'],
    dispatchLiveness: (ids: string[]) => [...queryKeys.threads.dispatchLivenessAll(), [...ids].sort()],
  },
  workspaceFiles: {
    all:  ['workspaceFiles'],
    byWs: (wsId: string, opts?: Record<string, unknown>) => [...queryKeys.workspaceFiles.all, wsId, opts],
  },
  memory: {
    all:       ['memory'],
    user:      () => [...queryKeys.memory.all, 'user'],
    userRead:  (key: string) => [...queryKeys.memory.user(), 'read', key],
    workspace: (wsId: string) => [...queryKeys.memory.all, 'workspace', wsId],
    workspaceRead: (wsId: string, key: string) => [...queryKeys.memory.workspace(wsId), 'read', key],
  },
  memo: {
    all:  ['memo'],
    list: () => [...queryKeys.memo.all, 'list'],
    read: (key: string) => [...queryKeys.memo.all, 'read', key],
  },
  mcp: {
    all:       ['mcp'],
    // User-level catalog of MCP templates (not workspace-scoped).
    catalog:   () => [...queryKeys.mcp.all, 'catalog'],
    // Effective per-workspace server list (builtins + workspace servers).
    workspace: (wsId: string) => [...queryKeys.mcp.all, 'workspace', wsId],
  },
  marketData: {
    all:  ['marketData'],
    bars: (symbol: string, interval: string) => [...queryKeys.marketData.all, 'bars', symbol, interval],
  },
  // Per-symbol quote cache — the unified snapshot layer (see lib/quotes/).
  // Key = uppercase legacy symbol spelling (indexes stripped of a leading '^').
  // Interim keying until Phase 4 re-keys on the canonical instrument_key.
  quote: {
    all:    ['quote'],
    detail: (symbol: string) => [...queryKeys.quote.all, symbol],
  },
};

/**
 * Marks a query as observed cache-only *by choice* — its arguments are complete
 * and its queryFn would succeed, it simply must not fetch on its own schedule.
 * Carrying it is what lets `refetchCacheOnlyLists`
 * (lib/threadLifecycle/feedClient.ts) fetch a query behind `enabled: false`.
 *
 * Never put it on a query that is disabled because an argument is missing:
 * those queryFns throw on the absent id, and the parked error is then read as a
 * real failure by whatever watches the query — which is how a thread lookup
 * with no id once evicted the user from every /chat route.
 */
export const CACHE_ONLY_META = { cacheOnly: true } as const;

/** Reader for {@link CACHE_ONLY_META} — keeps the flag's name in one module. */
export function isCacheOnlyMeta(meta: QueryMeta | undefined): boolean {
  return meta?.cacheOnly === true;
}
