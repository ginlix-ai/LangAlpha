/**
 * Hierarchical query key factory for React Query.
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
  },
  threads: {
    all:         ['threads'],
    byWorkspace: (wsId: string) => [...queryKeys.threads.all, 'workspace', wsId],
    detail:      (threadId: string) => [...queryKeys.threads.all, 'detail', threadId],
    recent:      (limit: number) => [...queryKeys.threads.all, 'recent', limit],
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
};
