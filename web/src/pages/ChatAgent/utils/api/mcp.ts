/**
 * MCP server config: per-workspace servers + user catalog.
 */
import { api } from '@/api/client';

//
// Per-workspace effective list mixes built-in servers with workspace-added
// ones; the catalog holds reusable user templates that get copied into a
// workspace via `from_template`. Env/header literal values are never echoed by
// the backend — only `${vault:NAME}` reference names surface (as `*_refs`).

/** A full MCP server definition payload (matches backend `McpServerInput`). */
export interface McpServerInput {
  name: string;
  transport: 'stdio' | 'sse' | 'http';
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

/** One row in the effective per-workspace MCP list. */
export interface EffectiveServer {
  name: string;
  origin: 'builtin' | 'workspace';
  transport: string;
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
}

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

/** A user catalog template row (masked — only vault refs surfaced). */
export interface CatalogServer {
  name: string;
  transport: string;
  command: string | null;
  args: string[];
  url: string | null;
  env_refs: string[];
  header_refs: string[];
  description: string;
  instruction: string;
  tool_exposure_mode: string;
  discovery_uses_secrets?: boolean;
  created_at: string | null;
  updated_at: string | null;
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
}

// --- Per-workspace MCP ---

export async function getWorkspaceMcpServers(workspaceId: string): Promise<EffectiveServerList> {
  const { data } = await api.get<EffectiveServerList>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers`,
  );
  return data;
}

/** Add a server to a workspace — either a full def or `{ from_template }`. */
export async function addWorkspaceMcpServer(
  workspaceId: string,
  body: McpServerInput | { from_template: string },
) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/mcp/servers`, body);
  return data as { name: string; source: string; enabled: boolean };
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
  return data as { name: string; source: string; enabled: boolean };
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
 * Promote a workspace server UP into the user's reusable template catalog (the
 * inverse of `from_template`). Only `${vault:NAME}` reference names travel —
 * secret values are workspace-scoped, so the template surfaces `needs_secret`
 * when later added to another workspace. `overwrite` replaces an existing
 * same-named template; without it a clash is a 409.
 */
export async function promoteWorkspaceMcpServerToTemplate(
  workspaceId: string,
  name: string,
  overwrite = false,
): Promise<CatalogServer> {
  const { data } = await api.post<CatalogServer>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}/promote`,
    { overwrite },
  );
  return data;
}

// --- User catalog (templates) ---

export async function getMcpCatalog(): Promise<CatalogServerList> {
  const { data } = await api.get<CatalogServerList>('/api/v1/mcp/servers');
  return { servers: data.servers ?? [], max_servers: data.max_servers ?? 20 };
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
