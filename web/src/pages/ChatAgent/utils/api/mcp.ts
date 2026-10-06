/**
 * MCP server config: the account's servers, and each workspace's view of them.
 */
import { api } from '@/api/client';
import { foldToolName } from '@/pages/ChatAgent/utils/directTools';
import type { OrderAction, OrderMode } from '@/types/orders';
import type { TradingLevel } from '@/types/api';

//
// Servers are installed on the account and selected per workspace: the
// workspace list is the built-ins plus the account's servers, each with this
// workspace's on/off state. Resolved secret values are never echoed by the
// backend, only `${vault:NAME}` refs and literals as stored.

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
  /**
   * Which brokerage capability group reaches this tool, null when none does.
   * Discovery is unfiltered on purpose -- it is what the vendor offers, not
   * what a connection may call -- so this is what lets a surface say which of
   * them the user actually granted. Absent outside the catalog tools route,
   * and on every server that is not a brokerage.
   */
  capability?: string | null;
  /**
   * Whether this tool is refused at every grant. A null `capability` alone does
   * not say: one we deliberately withheld is always refused, one we have simply
   * not classified is always permitted. Reading null as "unavailable" is how the
   * detail view came to promise the agent could not reach tools it could.
   */
  always_denied?: boolean;
  /**
   * How the agent reaches this tool right now: `ptc` from Python in the
   * sandbox, `direct` as one tool call the app can show, `both`. Effective,
   * not stored: `binding_source` says which layer decided it, and `policy`
   * means the server pins it and refuses any other value.
   */
  binding?: McpToolBinding;
  binding_source?: McpBindingSource;
  /**
   * The bindings the server accepts for this tool. A live order tool lists
   * only `direct`, so every order is one visible call rather than a line of
   * Python. Absent means unrestricted.
   */
  allowed?: McpToolBinding[];
  /** The call stops for the user's confirmation. Such a tool cannot be `both`. */
  approval?: boolean;
  /**
   * What this tool does to an order, or null for every tool that touches none.
   * The kind of order is the whole cost of the call, so it is what the row
   * badges and what the gate above it is keyed by.
   */
  order?: McpToolOrder | null;
}

export type McpToolBinding = 'ptc' | 'direct' | 'both';

/** The catalog's names for the order vocabulary, which is one vocabulary: the
 *  gate a row configures here and the interrupt a stopped call raises have to
 *  agree on what `staged` means, so both read the wire types. */
export type McpOrderAction = OrderAction;
export type McpOrderMode = OrderMode;

export interface McpToolOrder {
  action: McpOrderAction;
  mode: McpOrderMode;
}

/**
 * Whether an order of each kind stops for the user, one answer per mode.
 * Three and not one because the three cost different things: a live order
 * spends real money, a staged one writes into the real account without placing
 * anything, and a paper one spends nothing. A response carries all three keys.
 */
export interface McpOrderApproval {
  live: boolean;
  paper: boolean;
  staged: boolean;
}

/** The gate on a backend that predates the map: live orders and staged
 *  instructions asked, paper did not. A current server always sends the
 *  whole map, already folded under the trading permission. */
export const ORDER_APPROVAL_DEFAULTS: Readonly<McpOrderApproval> = {
  live: true,
  paper: false,
  staged: true,
};

/** The gate as it stands, filling in a map an older backend did not send. */
export function orderApprovalOf(
  stored: McpOrderApproval | null | undefined,
): McpOrderApproval {
  return { ...ORDER_APPROVAL_DEFAULTS, ...(stored ?? {}) };
}

/** The one row-wide override: send everything the row may move through the
 * sandbox. Null, the only other state, leaves each group's own default in force. */
export type McpBindingPreset = 'ptc_only';

/** Precedence, highest first: override > preset > config > group > default. */
export type McpBindingSource = 'override' | 'preset' | 'group' | 'default' | 'policy';

/** Partial: only the fields present change. Bindings are sent per tool, not
 * as the whole map, so two tabs editing different tools cannot overwrite
 * each other.
 * A `binding_preset` of `null` clears it back to the group default. */
export interface McpServerBindingPatch {
  tool_binding_set?: Record<string, McpToolBinding>;
  tool_binding_unset?: string[];
  binding_preset?: McpBindingPreset | null;
  /** Only the modes that changed; the server merges them onto the stored map.
   *  Sending the whole map would let one switch write back the other two as
   *  this tab last read them. */
  order_approval?: Partial<McpOrderApproval>;
}

/** The server's merge, mirrored for the optimistic view. */
export function mergeOrderApproval(
  stored: McpOrderApproval | null | undefined,
  patch: Partial<McpOrderApproval>,
): McpOrderApproval {
  return { ...orderApprovalOf(stored), ...patch };
}

/** The server's merge, mirrored for the optimistic view: a delta replaces
 * every stored key that folds to the name it addresses. */
export function mergeToolBinding(
  stored: Record<string, McpToolBinding>,
  patch: McpServerBindingPatch,
): Record<string, McpToolBinding> {
  const touched = new Set(
    [...Object.keys(patch.tool_binding_set ?? {}), ...(patch.tool_binding_unset ?? [])].map(
      foldToolName,
    ),
  );
  const kept = Object.entries(stored).filter(([name]) => !touched.has(foldToolName(name)));
  return { ...Object.fromEntries(kept), ...(patch.tool_binding_set ?? {}) };
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
  /** 'user' = one of the account's servers (removed at /plugins). */
  origin: 'builtin' | 'user';
  transport: McpTransport;
  enabled: boolean;
  /** True on every account row: an edit here changes the account server, so
   *  it applies in every workspace where that server is on. */
  editable: boolean;
  status: McpStatus;
  error: string;
  tool_count: number;
  tools: McpToolSummary[];
  /** Account vault names the server references that hold no value yet. */
  missing_secrets: string[];
  env_refs: string[];
  header_refs: string[];
  /**
   * The stored env/header maps of an account row: keys are the real var/header
   * names, values the configured `${vault:NAME}` ref strings or plain literals
   * (never resolved secrets), so the edit form round-trips them. Absent on
   * builtin rows.
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
   * Account (origin='user') rows only: the owner's OAuth connection status,
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
  /** The per-account server cap. Not a count to render against this list,
   *  which leaves out servers switched off account-wide; an add past the cap
   *  answers 409. */
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
  /** The account switch on the Plugins page: false = off in every workspace,
   * true = on wherever no workspace has switched it off. */
  enabled?: boolean;
  /** OAuth connection status, when one exists for this server. */
  oauth_status?: McpOauthStatus | null;
  /**
   * The capability groups this connection was granted, in the order they were
   * stored. `null`/absent means no connection, or one for a server we curate
   * no groups for; `[]` means a brokerage the user granted nothing. The two
   * are different answers and the gap between them is a broker that can do
   * nothing, so they stay distinguishable here too.
   */
  granted_capabilities?: string[] | null;
  /**
   * The same keys, but what the user last chose rather than what is in force.
   * Survives a `needs_reauth` or `revoked` status, where `granted_capabilities`
   * is deliberately withheld so nothing badges a dead connection as able to
   * trade. Only the consent dialog reads this, and only to open on the user's
   * own last answer instead of the product defaults.
   */
  remembered_capabilities?: string[] | null;
  /** Per-tool binding overrides; a name here beats the preset and the group. */
  tool_binding?: Record<string, McpToolBinding>;
  /** Whether any tool on this row binds directly, and so whether the row can
   * reach Flash at all. */
  has_direct_tools?: boolean;
  /**
   * The host-side probe's last word on the row under its current config, or
   * null when nothing has probed it yet (a stdio row never is, and stays
   * null). `tools` is empty here: a catalog row's tools are its cached
   * discovery snapshot's, which outlive a probe that starts failing.
   */
  probe?: McpProbeResult | null;
  /**
   * When the host last sent a probe after this row, or null when nothing ever
   * has. It is what separates a verdict still on its way from a row that is
   * simply never going to have one, which is the difference between saying
   * "checking" and saying nothing at all.
   */
  probe_kicked_at?: string | null;
  binding_preset?: McpBindingPreset | null;
  /** Whether an order stops for confirmation, per kind of order, with every
   *  mode answered under `trading_permission`. Absent on a backend that
   *  predates the map; `ORDER_APPROVAL_DEFAULTS` is the answer then. */
  order_approval?: McpOrderApproval;
  /** The user's trading permission as it stood when this row was read. On the
   *  row so its order switches are drawn from the same answer as the
   *  `order_approval` the server folded with it, never from a second read
   *  that may not have landed. Absent on a backend that predates the level;
   *  the row then reads as the default. */
  trading_permission?: TradingLevel;
  /** Host-side discovered tool count for the current config (OAuth servers). */
  tool_count?: number | null;
  /** Path on this origin to the mark the server declared in its handshake.
   * Absent when it declared none, which is most of them. */
  icon_url?: string | null;
  /** Non-blocking policy nudges — present on create/update responses only. */
  warnings?: string[] | null;
  created_at: string | null;
  updated_at: string | null;
  /** Workspaces holding a tombstone for this name (deny-list); populated in
   * the all-scopes view only. */
  disabled_workspace_ids?: string[];
  /** Whether a workspace created later starts with this server on. False for
   * a server added from a workspace: it was wanted there, not everywhere, so
   * the next workspace should not start with it on. Changing it touches no
   * existing workspace. Builtins and skills have no such setting and always
   * start on. */
  enabled_in_new_workspaces?: boolean;
  /** Set when the row was installed by an Agent Plugins package. Editing a
   * plugin-owned row detaches it (the badge clears; updates skip it). */
  plugin_name?: string | null;
  /** The owning plugin's enabled state; false = the row is suppressed from
   * every workspace regardless of its own `enabled`. */
  plugin_enabled?: boolean | null;
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

/** Response of an add or edit from a workspace. */
export interface WorkspaceMcpServerSaved {
  name: string;
  source: 'user';
  enabled: boolean;
  warnings?: string[];
}

/**
 * Create the server on the account and turn it on in this workspace only; it
 * starts off everywhere else, Flash included. A taken name or the account cap
 * answers 409 with a human `detail`.
 */
export async function addWorkspaceMcpServer(
  workspaceId: string,
  body: McpServerInput,
): Promise<WorkspaceMcpServerSaved> {
  const { data } = await api.post<WorkspaceMcpServerSaved>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers`,
    body,
  );
  return data;
}

/** Edits the account server, so the change reaches every workspace where it
 *  is on. `warnings` may say the edit detached it from its plugin. */
export async function updateWorkspaceMcpServer(
  workspaceId: string,
  name: string,
  body: McpServerInput,
): Promise<WorkspaceMcpServerSaved> {
  const { data } = await api.put<WorkspaceMcpServerSaved>(
    `/api/v1/workspaces/${workspaceId}/mcp/servers/${name}`,
    body,
  );
  return data;
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

/**
 * What one probe concluded, in one word. `ok` is an open server that listed
 * its tools and `ok_authed` one that accepted the credential we sent; the two
 * 401/403 arms differ by whether a credential was sent at all, which is the
 * difference between "connect this" and "the key is wrong". The host computes
 * it, because only that side knows the last of those three. Re-deriving it
 * from `http_status` here gets the two 401 arms backwards.
 */
export type ProbeVerdict =
  | 'ok'
  | 'ok_authed'
  | 'needs_credential'
  | 'credential_rejected'
  | 'oauth'
  | 'missing_secrets'
  | 'unreachable';

/** One tool a probe saw. Name and description only: the form is deciding
 *  whether to save an address, not rendering a schema. */
export interface ProbeTool {
  name: string;
  description: string;
}

/** Refs in `headers` resolve against the account vault. The route refuses any
 *  other field. */
export interface McpProbeInput {
  url: string;
  headers?: Record<string, string>;
}

/** What a probe of a remote address learned. Nothing is persisted by the
 *  ad-hoc route; the same shape is stored on a catalog row's `probe`. */
export interface McpProbeResult {
  verdict: ProbeVerdict;
  /** The ad-hoc route's preview, empty on a catalog row's stored verdict. */
  tools: ProbeTool[];
  server_info: { name?: string; version?: string } | null;
  error: string;
  http_status: number | null;
  /** Vault names the headers referenced that have no value yet. */
  missing_secrets: string[];
  probed_at: string | null;
}

/**
 * Ask a remote address what it offers, with the headers the form holds, before
 * anything is saved. The add form calls this as the user types.
 *
 * `signal` is the caller's: a check the user has already typed past holds one
 * of the host's few concurrent probe slots for as long as the dial-out takes,
 * so a superseded one is dropped rather than waited out.
 */
export async function probeMcpServer(
  body: McpProbeInput,
  signal?: AbortSignal,
): Promise<McpProbeResult> {
  const { data } = await api.post<McpProbeResult>('/api/v1/mcp/servers/probe', body, { signal });
  return data;
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
 * maps transports, and auto-extracts inline literal secrets into the account
 * vault. Each created server lands on the account, on in this workspace only.
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

// --- Account servers (the Plugins page) ---

/** Always fetches the all-scopes shape: one cache key serves both the Plugins
 * scope view and plain catalog reads (the extra `disabled_workspace_ids` is
 * cheap server-side, and a per-scope key would break the optimistic toggle). */
export async function getMcpCatalog(): Promise<CatalogServerList> {
  const { data } = await api.get<CatalogServerList>('/api/v1/mcp/servers', {
    params: { all_scopes: true },
  });
  return {
    servers: data.servers ?? [],
    max_servers: data.max_servers ?? 20,
  };
}

/** The discovered tool snapshot for one catalog server (hash-gated server
 * side to the row's current config — empty until a discovery has run). */
export async function getMcpCatalogServerTools(name: string): Promise<{
  server_name: string;
  /** The kinds of order this vendor has at all, which is not the same as the
   *  kinds its snapshot shows: the curation is the vendor's shape and the
   *  snapshot is one moment of discovery, so a row whose tools have never been
   *  read still gets the gates that will govern them. Empty on a server that
   *  places no orders, and on a backend that predates the field. */
  order_modes: McpOrderMode[];
  tools: McpToolSummary[];
  discovered_at: string | null;
}> {
  const { data } = await api.get(
    `/api/v1/mcp/servers/${encodeURIComponent(name)}/tools`,
  );
  return {
    server_name: data.server_name ?? name,
    order_modes: data.order_modes ?? [],
    tools: data.tools ?? [],
    discovered_at: data.discovered_at ?? null,
  };
}

/** What a builtin reported to this process at startup. Same shape as the
 * catalog's snapshot so the detail view has one way to render tools, minus a
 * `discovered_at`: a builtin is discovered once per process, not at a moment
 * the user acted. */
export async function getBuiltinMcpServerTools(name: string): Promise<{
  server_name: string;
  /** False when this worker never connected the server, which is not the same
   *  as the server having no tools. Absent on an older backend, so it defaults
   *  to true and the view reads as it did before. */
  connected: boolean;
  tools: McpToolSummary[];
  discovered_at: string | null;
}> {
  const { data } = await api.get(
    `/api/v1/mcp/builtin-servers/${encodeURIComponent(name)}/tools`,
  );
  return {
    server_name: data.server_name ?? name,
    connected: data.connected ?? true,
    tools: data.tools ?? [],
    discovered_at: data.discovered_at ?? null,
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

/** The account switch: off turns the server off in every workspace, and on
 *  turns it back on wherever no workspace has switched it off. */
export async function setMcpCatalogServerEnabled(name: string, enabled: boolean) {
  const { data } = await api.patch(`/api/v1/mcp/servers/${name}/enabled`, { enabled });
  return data as { name: string; enabled: boolean; warnings?: string[] };
}

/** Whether workspaces created later start with this server on. 404 once the
 *  server is gone. Brokerage rows live in the same table and take it too. */
export async function setMcpCatalogServerNewWorkspaces(name: string, enabled: boolean) {
  const { data } = await api.patch(`/api/v1/mcp/servers/${name}/new-workspaces`, { enabled });
  return data as { name: string; enabled_in_new_workspaces: boolean };
}

/** Change how a catalog server's tools reach the model. 422 when a tool that
 *  asks first is set to `both`; the detail names it. */
export async function setMcpCatalogServerBinding(
  name: string,
  body: McpServerBindingPatch,
): Promise<CatalogServer> {
  const { data } = await api.patch<CatalogServer>(
    `/api/v1/mcp/servers/${name}/binding`,
    body,
  );
  return data;
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
