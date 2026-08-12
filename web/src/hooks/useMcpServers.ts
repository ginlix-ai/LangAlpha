import { useEffect, useRef, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import {
  getWorkspaceMcpServers,
  addWorkspaceMcpServer,
  updateWorkspaceMcpServer,
  setWorkspaceMcpServerEnabled,
  deleteWorkspaceMcpServer,
  discoverWorkspaceMcpServer,
  importWorkspaceMcpServers,
  promoteWorkspaceMcpServerToTemplate,
  getMcpCatalog,
  createMcpCatalogServer,
  updateMcpCatalogServer,
  deleteMcpCatalogServer,
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
 * Effective per-workspace MCP list (builtins + workspace servers + status).
 *
 * Polls while the workspace is still *settling* so the lifecycle UI advances on
 * its own. A workspace is settling when:
 *  - the sandbox is *warming* (a proactive apply / workspace entry kicked a
 *    cold start) — keep polling so the row advances when it lands on running and
 *    discovery runs, rather than freezing on a stale "stopped"; or
 *  - the sandbox is running AND a workspace server is still `pending` (discovery
 *    hasn't resolved) or the session's `applied_config_version` hasn't caught up
 *    to the saved `config_version` (the change is still applying).
 *
 * Once the sandbox is running and every server is resolved AND applied — or it
 * settles into a steady non-running state (stopped / error) — polling stops
 * (steady state = no network).
 */
function isSettling(data: EffectiveServerList | undefined): boolean {
  if (!data) return false;
  if (data.sandbox_warming) return true;
  if (!data.sandbox_running) return false;
  // `applied_config_version == null` means no warm session has applied MCP config
  // yet — that's a *settled* state for an idle running sandbox, NOT "behind".
  // (An in-flight apply surfaces as `sandbox_warming` above, or as a numeric
  // applied version that lags `config_version`.) Treating null as behind would
  // poll forever while the panel is open; only a numeric lag counts as applying.
  const applyingBehind =
    data.applied_config_version != null &&
    data.applied_config_version < data.config_version;
  const verifying = data.servers.some(
    (s) => s.origin === 'workspace' && s.enabled && s.status === 'pending',
  );
  return applyingBehind || verifying;
}

export function useWorkspaceMcpServers(workspaceId: string | null | undefined, enabled = true) {
  return useQuery({
    queryKey: queryKeys.mcp.workspace(workspaceId ?? ''),
    queryFn: () => getWorkspaceMcpServers(workspaceId!),
    enabled: enabled && !!workspaceId,
    staleTime: 15_000,
    // Self-stopping poll: ~2.5s while settling, off once verified + applied.
    refetchInterval: (query) => (isSettling(query.state.data) ? 2_500 : false),
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

// ---------------------------------------------------------------------------
// Per-workspace mutations
// ---------------------------------------------------------------------------

/** Add a workspace server — a full def OR `{ from_template }`. */
export function useAddWorkspaceMcpServer(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: McpServerInput | { from_template: string }) =>
      addWorkspaceMcpServer(workspaceId, body),
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

/** Bulk-import a standard `mcpServers` blob (parsed JSON object). */
export function useImportWorkspaceMcpServers(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: unknown) => importWorkspaceMcpServers(workspaceId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}

/**
 * Promote a workspace server up into the user template catalog. Invalidates the
 * catalog so the new/updated template appears in the Templates view; the
 * workspace list is untouched (promotion doesn't change the workspace set).
 */
export function usePromoteMcpServerToTemplate(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, overwrite }: { name: string; overwrite?: boolean }) =>
      promoteWorkspaceMcpServerToTemplate(workspaceId, name, overwrite ?? false),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.catalog() });
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

export function useCreateMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: McpServerInput) => createMcpCatalogServer(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.catalog() });
    },
  });
}

export function useUpdateMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: McpServerInput }) =>
      updateMcpCatalogServer(name, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.catalog() });
    },
  });
}

export function useDeleteMcpCatalogServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteMcpCatalogServer(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.catalog() });
    },
  });
}
