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
    // Its own entry, not part of preferences: the server keeps it apart so the
    // agent, which can write preferences, cannot raise it.
    tradingPermission: () => [...queryKeys.user.all, 'trading-permission'],
  },
  models: {
    all: ['models'],
  },
  features: {
    all:  ['features'],
    list: () => [...queryKeys.features.all, 'list'],
    /** What a read keyed under one flag is keyed by: toggling the flag rereads it. */
    of: (key: string) => [...queryKeys.features.all, key],
    claudeCodeStatus: () => [...queryKeys.features.of('claude_code_runtime'), 'status'],
  },
  platform: {
    all:    ['platform'],
    models: () => [...queryKeys.platform.all, 'models'],
  },
  oauth: {
    all:    ['oauth'],
    /** One subscription's status, by its short name (`SubscriptionKey`). */
    status: (key: string) => [...queryKeys.oauth.all, key],
  },
  // What each connected key or subscription can run, read from the provider's
  // own list. `q` narrows a searched list (OpenRouter); '' is the whole list.
  providerModels: {
    all:       ['providerModels'],
    /** Every answer for one provider, searched or whole: a refresh replaces them all. */
    availableFor: (provider: string) => [...queryKeys.providerModels.all, 'available', provider],
    available: (provider: string, q = '') => [...queryKeys.providerModels.availableFor(provider), q],
    local:     (provider: string) => [...queryKeys.providerModels.all, 'local', provider],
    /** A custom endpoint not yet saved: what it lists for this key. */
    endpoint:  (format: string, baseUrl: string, apiKey: string) => [...queryKeys.providerModels.all, 'endpoint', format, baseUrl, apiKey],
  },
  workspaces: {
    all:    ['workspaces'],
    lists:  () => [...queryKeys.workspaces.all, 'list'],
    list:   (params: Record<string, unknown>) => [...queryKeys.workspaces.lists(), params],
    details: () => [...queryKeys.workspaces.all, 'detail'],
    detail: (id: string) => [...queryKeys.workspaces.details(), id],
    home:   () => [...queryKeys.workspaces.all, 'home'],
    quota:  () => [...queryKeys.workspaces.all, 'quota'],
  },
  // A workspace's latest move to another computer, polled while it runs. Its
  // own root, not under `workspaces`: the read that sees a move end refreshes
  // every workspace query, which would cancel that read before it lands.
  workspaceMoves: {
    all:       ['workspaceMoves'],
    workspace: (wsId: string) => [...queryKeys.workspaceMoves.all, wsId],
  },
  // The signed prefix the owner's report iframes load under, one per workspace
  // because the grant covers the whole workspace and nothing narrower. Its own
  // root, not under `workspaces`: every workspace invalidation would re-mint
  // it, and a new prefix reloads every open report.
  fileGrants: {
    all:       ['fileGrants'],
    workspace: (wsId: string) => [...queryKeys.fileGrants.all, wsId],
  },
  // Stable `/a/<code>` links for files and apps, grouped by workspace. A share
  // or stop rewrites the cached entries holding that code under `links`, then
  // re-reads the link's file list and the workspace's shared list.
  shareLinks: {
    all:         ['shareLinks'],
    byWorkspace: (wsId: string) => [...queryKeys.shareLinks.all, wsId],
    links:       (wsId: string) => [...queryKeys.shareLinks.byWorkspace(wsId), 'link'],
    // `key` is the normalized entry path for a file, the port for an app.
    link:        (wsId: string, kind: string, key: string) => [...queryKeys.shareLinks.links(wsId), kind, key],
    files:       (wsId: string, code: string) => [...queryKeys.shareLinks.byWorkspace(wsId), 'files', code],
    shared:      (wsId: string) => [...queryKeys.shareLinks.byWorkspace(wsId), 'shared'],
  },
  // What an `/s/` or `/a/` page resolves to. The visitor flag is part of the
  // question: the owner gets a different answer for the same code.
  share: {
    all:      ['share'],
    metadata: (code: string, viewer: string | null, asVisitor: boolean, path: string | null) =>
      [...queryKeys.share.all, 'metadata', code, viewer, asVisitor, path],
  },
  // One projection of a machine: the list. A detail entry would be a second
  // place for a status to disagree with itself, and every surface that shows a
  // machine already reads the list.
  computers: {
    all:   ['computers'],
    lists: () => [...queryKeys.computers.all, 'list'],
    // The per-folder breakdown is a separate, expensive reading (a `du` on
    // the machine), not a projection of the row, so it keys apart from it.
    storage: (computerId: string) => [...queryKeys.computers.all, 'storage', computerId],
    // What a new machine can run on, per user: the plan decides `allowed`.
    providers: () => [...queryKeys.computers.all, 'providers'],
    sshKey: () => [...queryKeys.computers.all, 'ssh-key'],
  },
  // A pairing is minted by a POST and then polled; the mint is cached so a
  // render never mints twice, and each token's status keys apart from it.
  // Not under `computers`: it is no machine yet, and refreshing the machines
  // must never mint a new command under the one the user is running.
  sshPairings: {
    all:    ['ssh-pairings'],
    // One per computer it reconnects, and one for a new machine, so a create
    // and a reconnect never show each other's command.
    mint:   (computerId?: string | null) => [...queryKeys.sshPairings.all, 'mint', computerId ?? 'new'],
    status: (token: string) => [...queryKeys.sshPairings.all, 'status', token],
  },
  // Read from the desktop shell, not the server: what this Mac's own helper
  // says, so it is not under `computers`, whose refreshes go to the server.
  desktop: {
    all:           ['desktop'],
    remoteControl: () => [...queryKeys.desktop.all, 'remote-control'],
  },
  threads: {
    all:         ['threads'],
    byWorkspace: (wsId: string) => [...queryKeys.threads.all, 'workspace', wsId],
    // ThreadGallery's infinite list. Deliberately UNDER the byWorkspace prefix
    // so the lifecycle feed's prefix invalidation and the gallery's own
    // self-invalidations keep reaching it; the suffix keeps it distinct from
    // the sidebar's finite page entries, which cannot hold InfiniteData.
    gallery:     (wsId: string, archived: boolean) => [...queryKeys.threads.byWorkspace(wsId), { view: 'gallery', archived }],
    // The first `limit` rows, a finite list. The size is in the key because the
    // entry holds exactly that many rows, so a new size must miss the cache.
    page:        (wsId: string, limit: number) => [...queryKeys.threads.byWorkspace(wsId), limit, 0],
    detail:      (threadId: string) => [...queryKeys.threads.all, 'detail', threadId],
    // Base for every recent-list variant — invalidation targets this prefix.
    recentAll:   () => [...queryKeys.threads.all, 'recent'],
    recent:      (limit: number) => [...queryKeys.threads.recentAll(), limit],
    status:      (threadId: string) => [...queryKeys.threads.all, 'status', threadId],
    // The thread's public link: whether it is shared, its URL, what visitors
    // may open. The chat header and every share panel read this one entry.
    share:       (threadId: string) => [...queryKeys.threads.all, 'share', threadId],
    // The wakeups the agent set in the thread, and its price targets from
    // before them. Under the prefix, so a feed reconnect's resync re-reads it.
    wakeups:     (threadId: string) => [...queryKeys.threads.all, 'wakeups', threadId],
    // The commands that outlasted their tool's wait and ran on in the
    // background. Under the prefix for the same reason as the wakeups.
    commands:    (threadId: string) => [...queryKeys.threads.all, 'commands', threadId],
    // Batched dispatch-liveness read for a turn's PTC cards: their threads,
    // the hand-offs' own runs, and the steers of those steered into a run
    // already going. The base key targets every id-set variant for
    // invalidation; the concrete key is stable on the SORTED id sets so
    // registration order never churns the cache entry.
    dispatchLivenessAll: () => [...queryKeys.threads.all, 'dispatch-liveness'],
    dispatchLiveness: ({ threads, runs, steers }: { threads: readonly string[]; runs: readonly string[]; steers: readonly string[] }) =>
      [...queryKeys.threads.dispatchLivenessAll(), { threads: [...threads].sort(), runs: [...runs].sort(), steers: [...steers].sort() }],
    // The same read keyed on the threads' run states as well, so a run that
    // starts or settles is a fresh read of how long each has been quiet.
    dispatchActivity: (ids: string[], revision: string) =>
      [...queryKeys.threads.dispatchLivenessAll(), 'activity', [...ids].sort(), revision],
  },
  workspaceFiles: {
    all:  ['workspaceFiles'],
    // Every listing and body of one workspace, for a change that rewrites
    // its whole folder (a restore from its backup).
    workspace: (wsId: string) => [...queryKeys.workspaceFiles.all, wsId],
    byWs: (wsId: string, opts?: Record<string, unknown>) => [...queryKeys.workspaceFiles.all, wsId, opts],
    // One file's bytes. `mode` rides in the key because the same path answers
    // in a different shape per reader — paginated text, full source, an
    // ArrayBuffer — and a viewer handed the wrong one renders nothing.
    // `scope` is the workspace id, or, for a share that has no workspace of
    // its own, the panel's mount id, so two shares never read each other's
    // bytes out of one cache. Deliberately under `all` but past the list's
    // options slot: dropping a workspace drops its bodies with it.
    bodies: (scope: string) => [...queryKeys.workspaceFiles.all, scope, 'body'],
    body: (scope: string, path: string, mode: string) => [...queryKeys.workspaceFiles.bodies(scope), path, mode],
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
    // The account's servers (not workspace-scoped).
    catalog:   () => [...queryKeys.mcp.all, 'catalog'],
    // Process-global builtins with the user's account-wide toggles.
    builtins:  () => [...queryKeys.mcp.all, 'builtins'],
    // Effective per-workspace server list (builtins + account servers).
    workspace: (wsId: string) => [...queryKeys.mcp.all, 'workspace', wsId],
    // Discovered tool snapshot for one catalog server (the detail view).
    serverTools: (name: string) => [...queryKeys.mcp.all, 'serverTools', name],
    // A builtin's tools, read from the frozen process registry. Its own family
    // rather than a child of `builtins()`: schemas are fixed for the process
    // lifetime, so a toggle there has nothing to tell this.
    builtinServerTools: (name: string) => [
      ...queryKeys.mcp.all,
      'builtinServerTools',
      name,
    ],
    // Every pre-save check. Under `mcp` so a vault mutation, which answers
    // `missing_secrets` for all of them, drops them with the fan-out.
    probes: () => [...queryKeys.mcp.all, 'probe'],
    // The add form's pre-save check of one address with one set of headers.
    // Both go in the key because the verdict is about the pair: the same URL
    // answers differently once a credential rides along. `headers` is a digest
    // of the map rather than the map itself: a key is cached state, and saying
    // which credential was in hand needs no more than that, while a row the
    // user is still filling in still never collides with the finished one.
    probe: (url: string, headers: string) => [
      ...queryKeys.mcp.probes(),
      url,
      headers,
    ],
  },
  // The brokerage connectors this build ships. Deliberately its own family
  // rather than a child of `mcp`: it is static and user-independent, so the
  // MCP fan-out has nothing to tell it, and sitting under that prefix meant
  // every server toggle refetched a list whose `staleTime: Infinity` says it
  // can never have changed.
  brokerages: {
    all:  ['brokerages'],
    list: () => [...queryKeys.brokerages.all, 'list'],
  },
  // Skills are per-user and mutable; the mode variant is what the slash menu
  // reads, the manage variant is the full list including disabled rows. A
  // workspace id keys the workspace-effective view (shadowing + disables).
  skills: {
    all:  ['skills'],
    // The scope slot is one of: 'user' (user view), a workspace id
    // (workspace-effective view), or 'all-scopes' (the Plugins inventory) —
    // allScopes and workspaceId are mutually exclusive on the wire.
    list: (
      includeDisabled = false,
      workspaceId: string | null = null,
      allScopes = false,
    ) =>
      [
        ...queryKeys.skills.all, 'list', includeDisabled,
        allScopes ? 'all-scopes' : (workspaceId ?? 'user'),
      ],
    // One file of a skill, SKILL.md by default (the detail view).
    content: (name: string, workspaceId: string | null = null, path = 'SKILL.md') =>
      [...queryKeys.skills.all, 'content', name, workspaceId ?? 'user', path],
    // The server's upload caps; deployment configuration, so they hold for
    // the life of the page.
    limits: () => [...queryKeys.skills.all, 'limits'],
  },
  // One read carries every subagent plus what the pane needs to edit them
  // (connector names, brokerage flags, limits), so a write lands in one entry.
  subagents: {
    all:  ['subagents'],
    list: () => [...queryKeys.subagents.all, 'list'],
  },
  userVault: {
    all:        ['userVault'],
    secrets:    () => [...queryKeys.userVault.all, 'secrets'],
    blueprints: () => [...queryKeys.userVault.all, 'blueprints'],
  },
  // Installed Agent Plugins packages. Their components live in the mcp and
  // skills caches; mutations here invalidate the whole fan-out.
  plugins: {
    all:    ['plugins'],
    list:   () => [...queryKeys.plugins.all, 'list'],
    detail: (name: string) => [...queryKeys.plugins.all, 'detail', name],
    // Under `all` so an install anywhere refreshes each entry's `installed`.
    marketplace: () => [...queryKeys.plugins.all, 'marketplace'],
  },
  // The user's order attempts. The list is paged by the server on a keyset
  // cursor, so a filter set is its own list rather than a client-side view of
  // one: the key carries the filters, and changing one starts a new page 1.
  orders: {
    all:    ['orders'],
    lists:  () => [...queryKeys.orders.all, 'list'],
    any:    () => [...queryKeys.orders.all, 'any'],
    list:   (filters: Record<string, string | string[]>) => [...queryKeys.orders.lists(), filters],
    detail: (attemptId: string) => [...queryKeys.orders.all, 'detail', attemptId],
  },
  // One family so a mutation invalidates the list, every run history and the
  // run feed together: pausing or triggering an automation changes all three.
  automations: {
    all:        ['automations'],
    lists:      () => [...queryKeys.automations.all, 'list'],
    list:       (params: Record<string, unknown>) => [...queryKeys.automations.lists(), params],
    executions: (automationId: string) => [...queryKeys.automations.all, 'executions', automationId],
    runs:       () => [...queryKeys.automations.all, 'runs'],
    waiting:    (threadId: string) => [...queryKeys.automations.all, 'waiting', threadId],
  },
  // The chats an automation can deliver to, per workspace (null: an
  // automation with no workspace). Its own root: an
  // automation write changes none of them, and each read asks the messaging
  // service to list every chat again.
  automationDelivery: {
    all:     ['automationDelivery'],
    options: (workspaceId: string | null) => [...queryKeys.automationDelivery.all, 'options', workspaceId],
  },
  // Per-symbol quote cache — the unified snapshot layer (see lib/quotes/).
  // Key = uppercase legacy symbol spelling (indexes stripped of a leading '^').
  // Interim keying until Phase 4 re-keys on the canonical instrument_key.
  quote: {
    all:    ['quote'],
    detail: (symbol: string) => [...queryKeys.quote.all, symbol],
  },
  insights: {
    all:         ['insights'],
    // Today's market-wide brief, as markdown for the agent.
    marketBrief: () => [...queryKeys.insights.all, 'market-brief'],
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
