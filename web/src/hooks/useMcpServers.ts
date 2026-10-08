import { useEffect, useRef, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { FAIL_FAST_OFFLINE } from '../lib/network';
import { needsDiscoveryProbe, PROBE_KICK_WINDOW_MS } from '../pages/ChatAgent/components/mcp/mcpState';
import {
  getWorkspaceMcpServers,
  addWorkspaceMcpServer,
  updateWorkspaceMcpServer,
  setWorkspaceMcpServerEnabled,
  discoverWorkspaceMcpServer,
  importWorkspaceMcpServers,
  getMcpCatalog,
  getMcpCatalogServerTools,
  getBuiltinMcpServers,
  getBuiltinMcpServerTools,
  setBuiltinMcpServerEnabled,
  createMcpCatalogServer,
  updateMcpCatalogServer,
  deleteMcpCatalogServer,
  setMcpCatalogServerEnabled,
  setMcpCatalogServerNewWorkspaces,
  setMcpCatalogServerBinding,
  mergeToolBinding,
  mergeOrderApproval,
  importMcpCatalogServers,
  disconnectMcpOauth,
  refreshMcpOauthSchemas,
  getBrokerages,
  setBrokerageEnabled,
  type CatalogServer,
  type CatalogServerList,
  type EffectiveServerList,
  type McpServerBindingPatch,
  type McpServerInput,
} from '../pages/ChatAgent/utils/api';

/**
 * React Query hooks for MCP server config — mirror `useWorkspaces` /
 * `useApiKeys` patterns. A mutation bumps `config_version` in the DB and the
 * backend kicks a background apply that warms the sandbox if needed and brings
 * the live agent up to the new version; the GET reports the session's
 * `applied_config_version` so the row's lifecycle reflects real verify + apply
 * progress. Here we just invalidate the relevant caches.
 *
 * The enabled toggle is OPTIMISTIC with rollback on error (plan requirement):
 * the row flips instantly, and reverts if the PATCH fails.
 */

// ---------------------------------------------------------------------------
// Invalidation
// ---------------------------------------------------------------------------

/**
 * One blast radius for every mutation that changes what an MCP row looks like:
 * `queryKeys.mcp.all`.
 *
 * A server lives on the account and every workspace lists it, so creating,
 * editing, deleting or disconnecting one, from either surface, changes the
 * catalog and each workspace's effective list at once. Invalidating only the
 * surface that asked leaves the other showing the pre-edit definition, which
 * is the drift these three different radii had already produced.
 *
 * Exported because callers outside this module need the same radius without
 * re-deciding it: a bulk MCP action on the Plugins page reaching for the
 * plugin-wide fan-out instead would drop the skills and vault caches too, which
 * an MCP change cannot have altered.
 *
 * The radius covers the cached probe verdicts (`queryKeys.mcp.probes`) on
 * purpose: a vault mutation answers their `missing_secrets`.
 */
export function invalidateMcpFanout(qc: QueryClient) {
  qc.invalidateQueries({ queryKey: queryKeys.mcp.all });
  // Plugin cards list their still-owned components as chips, so deleting or
  // customizing a server changes what the Plugins tab should show. The
  // dependency runs one way only — the plugin fanout already invalidates this
  // key — so naming it here is what keeps the two tabs from disagreeing.
  qc.invalidateQueries({ queryKey: queryKeys.plugins.all });
}

// ---------------------------------------------------------------------------
// Anti-flicker
// ---------------------------------------------------------------------------

/**
 * Returns `value`, but suppresses a sub-`delayMs` dip from `true` to `false`:
 * once true, it stays true through a brief drop and only flips false if `value`
 * is still false after `delayMs`. An initial-mount false (or a value that goes
 * true) propagates immediately — only the true→false edge is debounced.
 *
 * Used for the MCP **apply axis** (`synced`). Every config mutation bumps the
 * workspace-wide `config_version`, so the instant you toggle ANY server, every
 * connected row's `applied >= config` check goes false for a frame until the
 * background apply catches up — flashing "Applying to agent…" on rows you never
 * touched (and churning the toggled row through Verifying→Applying→Connected).
 * Holding the last `true` across that fast apply keeps the pills steady; an
 * apply that genuinely lags past `delayMs` still surfaces "Applying…" honestly.
 */
export function useDelayedFalse(value: boolean, delayMs: number): boolean {
  const [shown, setShown] = useState(value);
  const latest = useRef(value);
  useEffect(() => {
    latest.current = value;
    if (value) {
      setShown(true);
      return;
    }
    const timer = setTimeout(() => {
      if (!latest.current) setShown(false);
    }, delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return shown;
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/**
 * Why the per-workspace poll is still running — and whether that reason is safe
 * to poll indefinitely. Three outcomes:
 *  - `{ poll: false }` — settled (or nothing to watch): stop.
 *  - `{ poll: true, bounded: false }` — a *backend-driven* wait (sandbox warming,
 *    or a numeric apply still catching up). These always resolve on their own
 *    (an apply can even defer behind a long agent turn), so poll freely.
 *  - `{ poll: true, bounded: true, sig }` — the *only* thing outstanding is
 *    discovery of `pending` rows. That advances via McpTab's auto-probe, which
 *    fires at most once per mount and never retries; if a probe can't move a row
 *    off `pending` (a thrown probe, or discovery that never settles server-side)
 *    the row stays `pending` forever. `sig` fingerprints the pending set so the
 *    caller can stop once it has been static too long instead of spinning.
 */
type SettleState =
  | { poll: false }
  | { poll: true; bounded: false }
  | { poll: true; bounded: true; sig: string };

function settleState(data: EffectiveServerList | undefined): SettleState {
  if (!data) return { poll: false };
  if (data.sandbox_warming) return { poll: true, bounded: false };
  if (!data.sandbox_running) return { poll: false };
  // `applied_config_version == null` means no warm session has applied MCP config
  // yet — that's a *settled* state for an idle running sandbox, NOT "behind".
  // (An in-flight apply surfaces as `sandbox_warming` above, or as a numeric
  // applied version that lags `config_version`.) Treating null as behind would
  // poll forever while the panel is open; only a numeric lag counts as applying.
  const applyingBehind =
    data.applied_config_version != null &&
    data.applied_config_version < data.config_version;
  if (applyingBehind) return { poll: true, bounded: false };
  // Exactly the rows McpTab will auto-probe (`needsDiscoveryProbe`).
  const pending = data.servers.filter(needsDiscoveryProbe).map((s) => s.name).sort();
  if (pending.length === 0) return { poll: false };
  return { poll: true, bounded: true, sig: pending.join(',') };
}

// A verify-only poll whose pending set hasn't changed in this long has stalled:
// the mount-once auto-probe has already run and won't retry, so continuing to
// poll just spins. Comfortably clears the backend's ~15s discovery debounce, so
// a slow-but-resolving probe is never cut short. Warming/applying are exempt
// (they're backend-bounded) — only the stuck-`pending` case is capped.
const MAX_VERIFY_STALL_MS = 30_000;

export function useWorkspaceMcpServers(workspaceId: string | null | undefined, enabled = true) {
  // Tracks how long the verify-only poll has seen the same pending set, so a
  // row the auto-probe couldn't resolve stops the poll instead of hanging it.
  const verifyStall = useRef<{ sig: string; since: number } | null>(null);
  return useQuery({
    queryKey: queryKeys.mcp.workspace(workspaceId ?? ''),
    queryFn: () => getWorkspaceMcpServers(workspaceId!),
    enabled: enabled && !!workspaceId,
    staleTime: 15_000,
    // Self-stopping poll: ~2.5s while settling, off once verified + applied — or
    // once a stuck verify-only wait exceeds MAX_VERIFY_STALL_MS.
    refetchInterval: (query) => {
      const state = settleState(query.state.data);
      if (!state.poll || !state.bounded) {
        verifyStall.current = null;
        return state.poll ? 2_500 : false;
      }
      const now = Date.now();
      if (!verifyStall.current || verifyStall.current.sig !== state.sig) {
        verifyStall.current = { sig: state.sig, since: now };
      }
      if (now - verifyStall.current.since > MAX_VERIFY_STALL_MS) return false;
      return 2_500;
    },
  });
}

/**
 * How long the catalog keeps asking while an `http` row's verdict is
 * outstanding. The host probes a row right after it is saved, imported or
 * enabled and lands the verdict a few seconds later; nothing pushes that to the
 * page, so the list re-asks while a verdict is outstanding and stops once every
 * probeable row has one, or once a slow vendor has clearly stalled.
 */
const CATALOG_PROBE_POLL_MS = 3_000;
// How long a single outstanding set is waited on. Counting evaluations instead
// counted renders: React Query re-runs this callback on every option change,
// so the budget was spent by the page rather than by the wait, and any row
// that never got a verdict left the poll permanently exhausted for the next
// one. Elapsed time is what the sentence above actually describes.
//
// The same window a row renders "checking" for (`PROBE_KICK_WINDOW_MS`): the
// copy promises a verdict is coming, and this poll is the thing that would go
// and get it, so the two stopping at different moments is the promise outliving
// the effort behind it.
const CATALOG_PROBE_POLL_MAX_MS = PROBE_KICK_WINDOW_MS;
// The host re-kicks a row whose last verdict was `unreachable`, but at most
// once per row per this long (`SELF_HEAL_INTERVAL_S`, the list route's
// throttle). It is deliberately longer than the window above: one window holds
// exactly one kick, so the poll can never outrun the retry it waits for, and a
// list fetch landing a whole throttle after a window opened is one the host
// answered with a fresh kick.
const CATALOG_KICK_THROTTLE_MS = 120_000;

/** The account's MCP servers, as the Plugins page lists them. */
export function useMcpCatalog(enabled = true) {
  // The outstanding set and when the wait on it opened. A change to the set
  // restarts the clock; the same set running long stops the poll. The signature
  // is names only, so a row that lands on `unreachable` does not buy itself a
  // second window: the kick this one is waiting on is the one that answered.
  const outstanding = useRef<{ sig: string; since: number } | null>(null);
  return useQuery({
    queryKey: queryKeys.mcp.catalog(),
    queryFn: getMcpCatalog,
    enabled,
    staleTime: 60_000,
    refetchInterval: (query) => {
      const rows = query.state.data?.servers ?? [];
      // `http` only, the same set the host will actually probe: it dials
      // streamable HTTP, so an `sse` row never gets a verdict from here and
      // counting one kept this poll running for the whole window on every
      // mount, asking after an answer nothing was going to send.
      //
      // An `unreachable` verdict is outstanding too, because the host treats it
      // that way: the list route re-kicks exactly those rows, so the GET that
      // renders the failure is the same one that went and asked again. Waiting
      // only on rows with no verdict at all left the retry's answer (a
      // recovered server, its grants resynced) to arrive on the next mount or
      // tab refocus.
      //
      // A plugin-disabled row is refused the same way a switched-off one is:
      // `plugin_enabled: false` suppresses it everywhere regardless of its own
      // `enabled`, so no verdict is ever coming and the wait runs the whole
      // window on every mount.
      const sig = rows
        .filter(
          (s) =>
            s.enabled &&
            s.plugin_enabled !== false &&
            s.transport === 'http' &&
            (s.probe == null || s.probe.verdict === 'unreachable'),
        )
        .map((s) => s.name)
        .sort()
        .join(',');
      if (!sig) {
        outstanding.current = null;
        return false;
      }
      const now = Date.now();
      // A fetch landing a whole kick throttle after this window opened is one
      // the host answered by re-kicking, so the wait starts over. The poll
      // cannot produce that gap itself, the window being far shorter; a remount
      // or a tab refocus can, and that refetch is exactly the one whose retry
      // nobody would otherwise be waiting for.
      let active = outstanding.current;
      const reKicked =
        active != null &&
        query.state.dataUpdatedAt - active.since > CATALOG_KICK_THROTTLE_MS;
      if (!active || active.sig !== sig || reKicked) {
        active = { sig, since: now };
        outstanding.current = active;
      }
      if (now - active.since > CATALOG_PROBE_POLL_MAX_MS) return false;
      return CATALOG_PROBE_POLL_MS;
    },
  });
}

/**
 * Re-read the catalog until `name` reports discovered tools, or `timeoutMs`
 * passes. Resolves whether the tools arrived; never rejects.
 *
 * For a caller about to start an agent turn that should be able to use a
 * server just connected: a thread's tool roster is fixed when it starts, so a
 * turn started before discovery lands runs without the server. Each read goes
 * through the shared catalog entry, so every list showing the row sees it too.
 */
export async function waitForServerTools(
  qc: QueryClient,
  name: string,
  { timeoutMs, intervalMs = 1_500, signal }: { timeoutMs: number; intervalMs?: number; signal?: AbortSignal },
): Promise<boolean> {
  // One switch for the whole wait: the caller's abort, the deadline, and the
  // return all flip it, and every pending timer goes with it.
  const stop = new AbortController();
  const cancel = () => stop.abort();
  if (signal?.aborted) stop.abort();
  signal?.addEventListener('abort', cancel, { once: true });
  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const id = setTimeout(resolve, ms);
      stop.signal.addEventListener('abort', () => {
        clearTimeout(id);
        resolve();
      }, { once: true });
    });
  // Raced against every read as well as every pause, so a slow request cannot
  // hold the caller past the cap.
  const expired = sleep(timeoutMs).then(() => {
    stop.abort();
    return false;
  });
  const hasTools = () =>
    qc
      .fetchQuery({ queryKey: queryKeys.mcp.catalog(), queryFn: getMcpCatalog, staleTime: 0 })
      .then((list) => (list.servers.find((s) => s.name === name)?.tool_count ?? 0) > 0)
      .catch(() => false);
  try {
    while (!stop.signal.aborted) {
      if (await Promise.race([hasTools(), expired])) return true;
      if (stop.signal.aborted) break;
      await Promise.race([sleep(intervalMs), expired]);
    }
    return false;
  } finally {
    stop.abort();
    signal?.removeEventListener('abort', cancel);
  }
}

/** Discovered tools for one catalog server — powers the detail overlay. */
export function useMcpCatalogServerTools(name: string | null) {
  return useQuery({
    queryKey: queryKeys.mcp.serverTools(name ?? ''),
    queryFn: () => getMcpCatalogServerTools(name!),
    enabled: !!name,
    staleTime: 60_000,
  });
}

/** A builtin's tools, cached for as long as the answer can be trusted.
 *
 *  A connected builtin really is frozen: its tool list is fixed when the
 *  worker connects it and only a restart moves it. `connected: false` is a
 *  different kind of answer -- the worker that replied is one of several, and
 *  a builtin it failed to connect at startup stays dropped for that process
 *  alone. Freezing that reply is what turns one worker's gap into a permanent
 *  "tools unavailable" for the session, so it is left stale and the next
 *  remount or refocus gets another draw. */
export function useBuiltinMcpServerTools(name: string | null) {
  return useQuery({
    queryKey: queryKeys.mcp.builtinServerTools(name ?? ''),
    queryFn: () => getBuiltinMcpServerTools(name!),
    enabled: !!name,
    staleTime: (query) => (query.state.data?.connected ? Infinity : 0),
  });
}

/** Process-global builtin servers with the user's account-wide toggles. */
export function useBuiltinMcpServers() {
  return useQuery({
    queryKey: queryKeys.mcp.builtins(),
    queryFn: getBuiltinMcpServers,
    staleTime: 60_000,
  });
}

export function useToggleBuiltinMcpServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setBuiltinMcpServerEnabled(name, enabled),
    onSuccess: () => {
      // The toggle changes every workspace's effective list, not just this page.
      invalidateMcpFanout(queryClient);
    },
  });
}

// ---------------------------------------------------------------------------
// Per-workspace mutations
// ---------------------------------------------------------------------------

// Add, edit and import from a workspace write the ACCOUNT server (and an add
// switches it off in every other workspace), so they take the shared radius
// rather than this workspace's key alone.

export function useAddWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: (body: McpServerInput) => addWorkspaceMcpServer(workspaceId, body),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

export function useUpdateWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: ({ name, body }: { name: string; body: McpServerInput }) =>
      updateWorkspaceMcpServer(workspaceId, name, body),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

/** Optimistic enabled toggle with rollback on error. */
export function useToggleWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  const key = queryKeys.mcp.workspace(workspaceId);
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setWorkspaceMcpServerEnabled(workspaceId, name, enabled),
    onMutate: async ({ name, enabled }) => {
      await queryClient.cancelQueries({ queryKey: key });
      const previous = queryClient.getQueryData<EffectiveServerList>(key);
      if (previous) {
        queryClient.setQueryData<EffectiveServerList>(key, {
          ...previous,
          servers: previous.servers.map((s) =>
            s.name === name
              // Reconcile status with the new enabled state in the SAME optimistic
              // write so the row never churns through transient labels:
              //  - Disabling → 'disabled' (a clean muted pill).
              //  - Enabling → optimistic 'connected'. Toggling `enabled` doesn't
              //    change the discovery fingerprint, so re-enabling a server that
              //    was set up before reconnects from the cached schema with no
              //    re-verify — jump straight to the steady pill instead of flashing
              //    "Verifying…/Applying…". If it turns out unhealthy (missing
              //    secret / config changed while off), the refetch corrects it
              //    within a poll. Paired with the apply-axis anti-flicker
              //    (useDelayedFalse on `synced`) so the version bump this mutation
              //    triggers doesn't immediately bounce it back out of 'connected'.
              ? { ...s, enabled, status: enabled ? 'connected' : 'disabled' }
              : s,
          ),
        });
      }
      return { previous };
    },
    onError: (_err, _vars, context) => {
      if (context?.previous) queryClient.setQueryData(key, context.previous);
    },
    onSettled: () => {
      // The switch writes this workspace's marker, which the Plugins scope
      // checklist reads off the catalog too.
      invalidateMcpFanout(queryClient);
    },
  });
}

/**
 * Bulk-import a standard `mcpServers` blob (parsed JSON object). The backend
 * auto-extracts inline literal credentials into the account vault, so that
 * list is invalidated too; otherwise the freshly created secrets stay
 * invisible (and the server modal's picker keeps offering to re-create them)
 * until the staleTime lapses. Same rule as the catalog import.
 */
export function useImportWorkspaceMcpServers(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: (payload: unknown) => importWorkspaceMcpServers(workspaceId, payload),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
    },
  });
}

/** Per-workspace enable toggle with the workspace id in the vars, for the
 * Plugins scope checklist, which addresses many workspaces from one row
 * (useToggleWorkspaceMcpServer serves the single-workspace pages). Writes
 * tombstones / builtin markers, so the catalog and builtin views change too:
 * whole-prefix invalidation. */
export function useSetMcpServerEnabledInWorkspace() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      workspaceId,
      name,
      enabled,
    }: {
      workspaceId: string;
      name: string;
      enabled: boolean;
    }) => setWorkspaceMcpServerEnabled(workspaceId, name, enabled),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

/**
 * Discovery probe. Callers render the returned result inline; the fan-out
 * brings the probed status and tool count to every list, because a remote
 * row's probe lands on the account row every workspace and Plugins read.
 */
export function useDiscoverWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => discoverWorkspaceMcpServer(workspaceId, name),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

// ---------------------------------------------------------------------------
// Catalog mutations
// ---------------------------------------------------------------------------

/**
 * The optimistic half of a mutation that edits one catalog row in place:
 * patch the row at once, put the previous list back if the write fails, and
 * take the shared radius either way.
 */
function optimisticCatalogRow<V extends { name: string }>(
  queryClient: QueryClient,
  patch: (row: CatalogServer, vars: V) => CatalogServer,
) {
  const key = queryKeys.mcp.catalog();
  return {
    onMutate: async (vars: V) => {
      await queryClient.cancelQueries({ queryKey: key });
      const previous = queryClient.getQueryData<CatalogServerList>(key);
      if (previous) {
        queryClient.setQueryData<CatalogServerList>(key, {
          ...previous,
          servers: previous.servers.map((s) => (s.name === vars.name ? patch(s, vars) : s)),
        });
      }
      return { previous };
    },
    onError: (
      _err: unknown,
      _vars: V,
      context: { previous?: CatalogServerList } | undefined,
    ) => {
      if (context?.previous) queryClient.setQueryData(key, context.previous);
    },
    onSettled: () => {
      invalidateMcpFanout(queryClient);
    },
  };
}

/** Catalog mutations all take the shared radius (`invalidateMcpFanout`). */
export function useCreateMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: (body: McpServerInput) => createMcpCatalogServer(body),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

export function useUpdateMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: ({ name, body }: { name: string; body: McpServerInput }) =>
      updateMcpCatalogServer(name, body),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

export function useDeleteMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteMcpCatalogServer(name),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

type NameEnabled = { name: string; enabled: boolean };

/** Optimistic user-level enabled toggle (Plugins page). */
export function useToggleMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: NameEnabled) => setMcpCatalogServerEnabled(name, enabled),
    ...optimisticCatalogRow(queryClient, (s, { enabled }: NameEnabled) => ({ ...s, enabled })),
  });
}

/**
 * The scope menu's "On in new workspaces" on a user server. Optimistic on the
 * catalog row because the item keeps its menu open: until the refetch lands
 * the check would still show the old value, and a second click would resend
 * the one just written rather than undo it.
 */
export function useSetMcpServerNewWorkspaces() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: NameEnabled) =>
      setMcpCatalogServerNewWorkspaces(name, enabled),
    ...optimisticCatalogRow(queryClient, (s, { enabled }: NameEnabled) => ({
      ...s,
      enabled_in_new_workspaces: enabled,
    })),
  });
}

type BindingVars = { name: string; body: McpServerBindingPatch };

/**
 * Change how a catalog server's tools reach the model (Plugins detail panel).
 * Optimistic on the three stored fields only: the effective per-tool answer
 * lives on the tools query, which the fan-out refetches once the server has
 * resolved the new precedence.
 */
export function useSetMcpServerBinding() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: BindingVars) => setMcpCatalogServerBinding(name, body),
    ...optimisticCatalogRow(queryClient, (s, { body }: BindingVars) => ({
      ...s,
      ...((body.tool_binding_set !== undefined || body.tool_binding_unset !== undefined) && {
        tool_binding: mergeToolBinding(s.tool_binding ?? {}, body),
      }),
      ...(body.binding_preset !== undefined && { binding_preset: body.binding_preset }),
      ...(body.order_approval !== undefined && {
        order_approval: mergeOrderApproval(s.order_approval, body.order_approval),
      }),
    })),
  });
}

/**
 * Bulk-import a standard `mcpServers` blob into the user catalog (Plugins page).
 * The backend also auto-extracts inline literal credentials into the USER
 * vault, so the vault list is invalidated too — otherwise the freshly created
 * secrets stay invisible (and the server modal's picker keeps offering to
 * re-create them) until the 30s staleTime lapses.
 */
export function useImportMcpCatalogServers() {
  const queryClient = useQueryClient();
  return useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: (payload: unknown) => importMcpCatalogServers(payload),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
    },
  });
}

/** Disconnect a server's OAuth connection (marks it revoked server-side). */
export function useDisconnectMcpOauth() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => disconnectMcpOauth(name),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

/** Host-side schema re-discovery for an OAuth-connected server. */
export function useRefreshMcpOauthSchemas() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => refreshMcpOauthSchemas(name),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

// --- Brokerage connectors ---

export function useBrokerages() {
  return useQuery({
    queryKey: queryKeys.brokerages.list(),
    queryFn: getBrokerages,
    // What this build ships cannot change under a running page.
    staleTime: Infinity,
  });
}

export function useToggleBrokerage() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setBrokerageEnabled(name, enabled),
    // The shared catalog radius, not just the MCP keys: this writes a catalog
    // row, and a plugin card lists the rows it still owns.
    onSuccess: () => invalidateMcpFanout(queryClient),
  });
}
