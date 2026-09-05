import { useEffect, useRef, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { needsDiscoveryProbe } from '../pages/ChatAgent/components/mcp/mcpState';
import {
  getWorkspaceMcpServers,
  addWorkspaceMcpServer,
  adoptMcpServerToWorkspace,
  updateWorkspaceMcpServer,
  setWorkspaceMcpServerEnabled,
  deleteWorkspaceMcpServer,
  discoverWorkspaceMcpServer,
  importWorkspaceMcpServers,
  promoteWorkspaceMcpServerToTemplate,
  getMcpCatalog,
  getMcpCatalogServerTools,
  getBuiltinMcpServers,
  getBuiltinMcpServerTools,
  setBuiltinMcpServerEnabled,
  createMcpCatalogServer,
  updateMcpCatalogServer,
  deleteMcpCatalogServer,
  setMcpCatalogServerEnabled,
  importMcpCatalogServers,
  disconnectMcpOauth,
  refreshMcpOauthSchemas,
  getBrokerages,
  setBrokerageEnabled,
  type CatalogServerList,
  type EffectiveServerList,
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
 * A catalog row that is enabled is inherited by EVERY workspace of the user, so
 * creating, editing, deleting or disconnecting one changes each workspace's
 * effective list exactly as toggling it does. Invalidating only the catalog
 * leaves an open workspace panel showing the pre-edit definition, which is the
 * drift these three different radii had already produced.
 *
 * Exported because callers outside this module need the same radius without
 * re-deciding it: a bulk MCP action on the Plugins page reaching for the
 * plugin-wide fan-out instead would drop the skills and vault caches too, which
 * an MCP change cannot have altered.
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

/** The user's MCP template catalog. */
export function useMcpCatalog(enabled = true) {
  return useQuery({
    queryKey: queryKeys.mcp.catalog(),
    queryFn: getMcpCatalog,
    enabled,
    staleTime: 60_000,
  });
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

export function useAddWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: McpServerInput) => addWorkspaceMcpServer(workspaceId, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}

export function useUpdateWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: McpServerInput }) =>
      updateWorkspaceMcpServer(workspaceId, name, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
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
      queryClient.invalidateQueries({ queryKey: key });
    },
  });
}

export function useDeleteWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteWorkspaceMcpServer(workspaceId, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}

/**
 * Bulk-import a standard `mcpServers` blob (parsed JSON object). The backend
 * auto-extracts inline literal credentials into the WORKSPACE vault, so that
 * list is invalidated too — otherwise the freshly created secrets stay
 * invisible (and the server modal's picker keeps offering to re-create them)
 * until the staleTime lapses. Same rule as the catalog import.
 */
export function useImportWorkspaceMcpServers(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: unknown) => importWorkspaceMcpServers(workspaceId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaceVault.byWorkspace(workspaceId) });
    },
  });
}

/**
 * Promote a workspace server up into the user template catalog. Invalidates the
 * catalog so the new/updated template appears in the Templates view; the
 * workspace list is untouched (promotion doesn't change the workspace set).
 *
 * The workspace id rides in the vars because the all-scopes Plugins list mixes
 * rows from many workspaces in one list; a fixed-workspace page passes the
 * same id every call. `removeSource` turns a copy into a move.
 */
export function usePromoteMcpServerToTemplate() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      workspaceId,
      name,
      overwrite,
      removeSource,
    }: {
      workspaceId: string;
      name: string;
      overwrite?: boolean;
      removeSource?: boolean;
    }) =>
      promoteWorkspaceMcpServerToTemplate(
        workspaceId, name, overwrite ?? false, removeSource ?? false,
      ),
    onSuccess: (_data, vars) => {
      // With removeSource the workspace set changes too (the fork is gone),
      // so the whole prefix goes; a plain copy touches only the catalog.
      queryClient.invalidateQueries({
        queryKey: vars.removeSource ? queryKeys.mcp.all : queryKeys.mcp.catalog(),
      });
    },
  });
}

/**
 * Move a user-level server INTO one workspace (the down direction). The
 * catalog row disappears and every workspace's effective list changes, so the
 * whole `mcp` prefix is invalidated.
 */
export function useAdoptMcpServerToWorkspace() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ workspaceId, name }: { workspaceId: string; name: string }) =>
      adoptMcpServerToWorkspace(workspaceId, name),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

/** Per-workspace enable toggle with the workspace id in the vars — for the
 * all-scopes Plugins view where one list mixes rows from many workspaces
 * (useToggleWorkspaceMcpServer serves the single-workspace pages). Writes
 * tombstones / builtin markers for inherited names, so the catalog and
 * builtin views change too: whole-prefix invalidation. */
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
 * Discovery probe. Invalidates the workspace list on success so the freshly
 * probed status + tool count surface on the row immediately; callers also
 * render the returned result inline.
 */
export function useDiscoverWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => discoverWorkspaceMcpServer(workspaceId, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}

// ---------------------------------------------------------------------------
// Catalog mutations
// ---------------------------------------------------------------------------

/** Catalog mutations all take the shared radius (`invalidateMcpFanout`). */
export function useCreateMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: McpServerInput) => createMcpCatalogServer(body),
    onSuccess: () => {
      invalidateMcpFanout(queryClient);
    },
  });
}

export function useUpdateMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
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

/** Optimistic user-level enabled toggle (Plugins page). */
export function useToggleMcpCatalogServer() {
  const queryClient = useQueryClient();
  const key = queryKeys.mcp.catalog();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setMcpCatalogServerEnabled(name, enabled),
    onMutate: async ({ name, enabled }) => {
      await queryClient.cancelQueries({ queryKey: key });
      const previous = queryClient.getQueryData<CatalogServerList>(key);
      if (previous) {
        queryClient.setQueryData<CatalogServerList>(key, {
          ...previous,
          servers: previous.servers.map((s) =>
            s.name === name ? { ...s, enabled } : s,
          ),
        });
      }
      return { previous };
    },
    onError: (_err, _vars, context) => {
      if (context?.previous) queryClient.setQueryData(key, context.previous);
    },
    onSettled: () => {
      invalidateMcpFanout(queryClient);
    },
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
