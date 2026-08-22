/**
 * MCP server config: per-workspace servers + user catalog.
 */
import { api } from '@/api/client';

//
// Per-workspace effective list mixes built-in servers with workspace-added
// ones; the catalog holds reusable user templates managed on the Plugins page.
// Env/header literal values are never echoed by
// the backend — only `${vault:NAME}` reference names surface (as `*_refs`).

/** The wire transports a user-configured server can speak. */
export type McpTransport = 'stdio' | 'sse' | 'http';

/** A full MCP server definition payload (matches backend `McpServerInput`). */
export interface McpServerInput {
  name: string;
  transport: McpTransport;
  command?: string | null;
  args?: string[];
  url?: string | null;
  env?: Record<string, string>;
  headers?: Record<string, string>;
  description?: string;
  instruction?: string;
  tool_exposure_mode?: 'summary' | 'detailed';
  discovery_uses_secrets?: boolean;
}

/** One discovered tool (sanitized snapshot from the discovery cache). */
export interface McpToolSummary {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
}

export type McpStatus =
  | 'connected'
  | 'error'
  | 'needs_secret'
  | 'disabled'
  | 'pending'
  | 'unknown';

/** Lifecycle of a server's OAuth connection (absent = never connected). */
export type McpOauthStatus =
  | 'connected'
  | 'needs_reauth'
  | 'refresh_ambiguous'
  | 'revoked';

/** One row in the effective per-workspace MCP list. */
export interface EffectiveServer {
  name: string;
  /** 'user' = inherited from the user's servers (manage at /plugins). */
  origin: 'builtin' | 'workspace' | 'user';
  /** Workspace-local fork that overrides a same-named inherited server. */
  shadows_inherited?: boolean;
  transport: McpTransport;
  enabled: boolean;
  editable: boolean;
  deletable: boolean;
  status: McpStatus;
  error: string;
  tool_count: number;
  tools: McpToolSummary[];
  missing_secrets: string[];
  env_refs: string[];
  header_refs: string[];
  /**
   * The stored env/header reference maps for workspace-origin servers — keys are
   * the real var/header names, values are the configured `${vault:NAME}` ref
   * strings or plain literals (never resolved secrets). Empty/absent for builtin
   * rows and on older backends that only return `env_refs`/`header_refs`.
   */
  env?: Record<string, string>;
  headers?: Record<string, string>;
  description: string;
  instruction: string;
  tool_exposure_mode: string | null;
  discovery_uses_secrets?: boolean;
  command: string | null;
  args: string[];
  url: string | null;
  config_version: number;
  /**
   * Inherited (origin='user') rows only: the owner's OAuth connection status,
   * including 'revoked'. Absent/null = the server has no OAuth connection.
   * OAuth rows are discovered host-side, never probed from the workspace.
   */
  oauth_status?: McpOauthStatus | null;
  /**
   * Disabled built-ins only: which tier switched it off. 'user' means the
   * account-level disable, which a workspace cannot undo — the row renders
   * read-only here and points at Plugins.
   */
  disabled_scope?: 'workspace' | 'user' | null;
  /** Set when the row was installed by an Agent Plugins package (badge only;
   * the row is managed at /plugins). */
  plugin_name?: string | null;
}

/**
 * The config-shaped half of a server row: everything the create/edit modal
 * reads, and nothing it doesn't. Both list surfaces hand their own row straight
 * to the modal — an `EffectiveServer` and a `CatalogServer` are each
 * structurally assignable to this — so neither has to fabricate the runtime
 * fields (status, tool counts, permissions) it doesn't have.
 */
export type McpServerDraft = Pick<
  EffectiveServer,
  | 'name'
  | 'transport'
  | 'command'
  | 'args'
  | 'url'
  | 'env'
  | 'env_refs'
  | 'headers'
  | 'header_refs'
  | 'description'
  | 'instruction'
  | 'tool_exposure_mode'
  | 'discovery_uses_secrets'
>;

export interface EffectiveServerList {
  servers: EffectiveServer[];
  sandbox_running: boolean;
  max_servers: number;
  config_version: number;
  /**
   * The MCP config version the *running* session has actually applied (loaded
   * into the live agent), or null when no warm session exists. When this has
   * caught up to `config_version`, the latest config is live — the
   * version-accurate "synced" signal. Null/behind ⇒ "applying / will apply".
   */
  applied_config_version?: number | null;
  /**
   * True while the sandbox is transitioning *up* toward running (a proactive
   * MCP apply, or workspace entry, kicked a warm). Lets the UI keep polling and
   * show "Starting workspace…" through the stopped→running gap.
   */
  sandbox_warming?: boolean;
}

/** A user catalog row, as returned to its owner. */
export interface CatalogServer {
  name: string;
  transport: McpTransport;
  command: string | null;
  args: string[];
  url: string | null;
  env_refs: string[];
  header_refs: string[];
  /**
   * The stored env/header reference maps — keys are the real var/header names,
   * values the configured `${vault:NAME}` ref strings or plain literals (never
   * resolved secrets). The edit form round-trips them; absent on older backends
   * that returned only `env_refs`/`header_refs`.
   */
  env?: Record<string, string>;
  headers?: Record<string, string>;
  description: string;
  instruction: string;
  tool_exposure_mode: string;
  discovery_uses_secrets?: boolean;
  /** True = inherited by every workspace of the user (Plugins page toggle). */
  enabled?: boolean;
  /** OAuth connection status, when one exists for this server. */
  oauth_status?: McpOauthStatus | null;
  /** Host-side discovered tool count for the current config (OAuth servers). */
  tool_count?: number | null;
  /** Non-blocking policy nudges — present on create/update responses only. */
  warnings?: string[] | null;
  created_at: string | null;
  updated_at: string | null;
  /** Workspaces holding a tombstone for this name (deny-list); populated in
   * the all-scopes view only. */
  disabled_workspace_ids?: string[];
  /** Set when the row was installed by an Agent Plugins package. Editing a
   * plugin-owned row detaches it (the badge clears; updates skip it). */
  plugin_name?: string | null;
  /** The owning plugin's enabled state; false = the row is suppressed from
   * every workspace regardless of its own `enabled`. */
  plugin_enabled?: boolean | null;
}

/** A workspace-local server surfaced in the all-scopes catalog view — a
 * summary, not an editable config (editing stays on the workspace endpoints). */
export interface WorkspaceScopedMcpServer {
  name: string;
  workspace_id: string;
  transport: McpTransport;
  enabled: boolean;
  description: string;
  shadows_inherited: boolean;
}

/** Result of a discovery probe (POST /discover). */
export interface McpDiscoveryResult {
  server_name?: string;
  status: McpStatus;
  tools: McpToolSummary[];
  error: string;
  /** The per-server config fingerprint this snapshot was discovered under. */
  config_hash?: string;
  discovered_at?: string | null;
}

/** Response shape of GET /api/v1/mcp/servers (the user catalog list). */
export interface CatalogServerList {
  servers: CatalogServer[];
  max_servers: number;
  /** all_scopes=true only: workspace-local servers across the user's workspaces. */
  workspace_servers?: WorkspaceScopedMcpServer[];
}

// --- Per-workspace MCP ---

export async function getWorkspaceMcpServers(workspaceId: string): Promise<EffectiveServerList> {
  const { data } = await api.get<EffectiveServerList>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers`,
  );
  return data;
}

export async function addWorkspaceMcpServer(
  workspaceId: string,
  body: McpServerInput,
) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/mcp/servers`, body);
  return data as { name: string; source: string; enabled: boolean; warnings?: string[] };
}

export async function updateWorkspaceMcpServer(
  workspaceId: string,
  name: string,
  body: McpServerInput,
) {
  const { data } = await api.put(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}`,
    body,
  );
  return data as { name: string; source: string; enabled: boolean; warnings?: string[] };
}

export async function setWorkspaceMcpServerEnabled(
  workspaceId: string,
  name: string,
  enabled: boolean,
) {
  const { data } = await api.patch(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/enabled`,
    { enabled },
  );
  return data as { name: string; enabled: boolean };
}

export async function deleteWorkspaceMcpServer(workspaceId: string, name: string) {
  const { data } = await api.delete(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}`,
  );
  return data as { ok: boolean };
}

export async function discoverWorkspaceMcpServer(
  workspaceId: string,
  name: string,
): Promise<McpDiscoveryResult> {
  const { data } = await api.post<{ server: McpDiscoveryResult }>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/discover`,
  );
  return data.server;
}

/** One per-server outcome from a bulk import. */
export interface McpImportResultRow {
  name: string;
  original_name: string;
  renamed: boolean;
  status: 'created' | 'exists' | 'skipped' | 'invalid' | 'error';
  reason?: string;
  error?: string;
}

export interface McpImportResult {
  results: McpImportResultRow[];
  created: number;
  /** Vault secret names auto-created from inline literal credentials. */
  secrets_created: string[];
  config_version: number;
}

/**
 * Bulk-import a standard `mcpServers` JSON blob. The backend coerces names,
 * maps transports, and auto-extracts inline literal secrets into the vault.
 * `payload` is the parsed JSON object (e.g. `{ mcpServers: { … } }`).
 */
export async function importWorkspaceMcpServers(
  workspaceId: string,
  payload: unknown,
): Promise<McpImportResult> {
  const { data } = await api.post<McpImportResult>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/import`,
    payload,
  );
  return data;
}

/**
 * Promote a workspace server UP into the user's reusable template catalog.
 * Only `${vault:NAME}` reference names travel —
 * secret values are workspace-scoped, so the template surfaces `needs_secret`
 * when later added to another workspace. `overwrite` replaces an existing
 * same-named template; without it a clash is a 409.
 */
export async function promoteWorkspaceMcpServerToTemplate(
  workspaceId: string,
  name: string,
  overwrite = false,
  removeSource = false,
): Promise<CatalogServer> {
  const { data } = await api.post<CatalogServer>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/promote`,
    { overwrite, remove_source: removeSource },
  );
  return data;
}

/**
 * Move a user-level (Connectors) server INTO one workspace — the inverse of
 * promote-with-removeSource. The catalog row becomes a workspace-local fork
 * and is then deleted; OAuth-connected servers refuse with a 409 (connections
 * exist only at the user tier).
 */
export async function adoptMcpServerToWorkspace(workspaceId: string, name: string) {
  const { data } = await api.post(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/adopt`,
  );
  return data as { name: string; source: string; enabled: boolean };
}

// --- User catalog (templates) ---

/** Always fetches the all-scopes shape: one cache key serves both the Plugins
 * scope view and plain catalog reads (the extra fields are two cheap queries
 * server-side, and a per-scope key would break the optimistic toggle). */
export async function getMcpCatalog(): Promise<CatalogServerList> {
  const { data } = await api.get<CatalogServerList>('/api/v1/mcp/servers', {
    params: { all_scopes: true },
  });
  return {
    servers: data.servers ?? [],
    max_servers: data.max_servers ?? 20,
    workspace_servers: data.workspace_servers ?? [],
  };
}

export async function createMcpCatalogServer(body: McpServerInput): Promise<CatalogServer> {
  const { data } = await api.post<CatalogServer>('/api/v1/mcp/servers', body);
  return data;
}

export async function updateMcpCatalogServer(
  name: string,
  body: McpServerInput,
): Promise<CatalogServer> {
  const { data } = await api.put<CatalogServer>(`/api/v1/mcp/servers/${name}`, body);
  return data;
}

export async function deleteMcpCatalogServer(name: string) {
  const { data } = await api.delete(`/api/v1/mcp/servers/${name}`);
  return data as { ok: boolean };
}

/** Enable/disable a catalog server for ALL the user's workspaces (inheritance). */
export async function setMcpCatalogServerEnabled(name: string, enabled: boolean) {
  const { data } = await api.patch(`/api/v1/mcp/servers/${name}/enabled`, { enabled });
  return data as { name: string; enabled: boolean; warnings?: string[] };
}

/**
 * Bulk-import a standard `mcpServers` JSON blob into the user catalog. Inline
 * literal secrets are auto-extracted into the USER vault; imported rows land
 * disabled (inert) until the user flips them live.
 */
export async function importMcpCatalogServers(payload: unknown): Promise<McpImportResult> {
  const { data } = await api.post<McpImportResult>('/api/v1/mcp/servers/import', payload);
  return data;
}

// --- Builtin servers (process-global, per-user account-wide toggle) ---

export interface BuiltinMcpServer {
  name: string;
  description: string;
  transport: string;
  enabled: boolean;
  /** Workspaces with a disable-marker for this builtin — all-scopes view only. */
  disabled_workspace_ids?: string[];
}

export async function getBuiltinMcpServers(): Promise<{ servers: BuiltinMcpServer[] }> {
  const { data } = await api.get('/api/v1/mcp/builtin-servers', {
    params: { all_scopes: true },
  });
  return data;
}

/** Account-wide toggle: applies to every workspace, no workspace re-enable. */
export async function setBuiltinMcpServerEnabled(name: string, enabled: boolean) {
  const { data } = await api.patch(`/api/v1/mcp/builtin-servers/${name}/enabled`, { enabled });
  return data as { name: string; enabled: boolean };
}

// --- User-level OAuth (Plugins page) ---

/** Phase 1 of the connect flow; the caller navigates to `authorize_url`. */
export async function startMcpOauth(name: string, returnTo = '/plugins') {
  const { data } = await api.post<{ authorize_url: string }>(
    `/api/v1/mcp/servers/${name}/oauth/start`,
    { return_to: returnTo },
  );
  return data;
}

export async function disconnectMcpOauth(name: string) {
  const { data } = await api.delete(`/api/v1/mcp/servers/${name}/oauth`);
  return data as { ok: boolean };
}

export interface McpOauthSchemaRefreshResult {
  server_name: string;
  status: string;
  error: string;
  tool_count: number;
  discovered_at: string | null;
}

/** Host-side re-discovery of an OAuth server's tool schemas. */
export async function refreshMcpOauthSchemas(
  name: string,
): Promise<McpOauthSchemaRefreshResult> {
  const { data } = await api.post<McpOauthSchemaRefreshResult>(
    `/api/v1/mcp/servers/${name}/oauth/refresh-schemas`,
  );
  return data;
}
