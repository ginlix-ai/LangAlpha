/**
 * MCP server config: per-workspace servers + user catalog.
 */
import { api } from '@/api/client';
import {
  beginMcpOAuth,
  bindMcpOAuth,
  canBeginMcpOAuth,
  cancelMcpOAuth,
  isDesktopShell,
  type McpOAuthFlow,
} from '@/lib/desktop';

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
  /** Set when the row was installed by an Agent Plugins package. Editing a
   * plugin-owned row detaches it (the badge clears; updates skip it). */
  plugin_name?: string | null;
  /** The owning plugin's enabled state; false = the row is suppressed from
   * every workspace regardless of its own `enabled`. */
  plugin_enabled?: boolean | null;
}

// --- Brokerage connectors ---

/**
 * One shipped brokerage connector, as the backend describes it.
 *
 * The registry arrives over the wire rather than living in the client: a build
 * that hard-coded a broker's host would be a second place for it to be wrong,
 * and the host is where an OAuth token ends up. The quirks arrive as booleans
 * and the sentence each becomes is translated, so it lives with the copy.
 */
export interface Brokerage {
  name: string;
  label: string;
  url: string;
  /** The broker's own website, which is not the endpoint's host: an MCP
   *  endpoint sits on an API subdomain with no page behind it. */
  site: string;
  description: string;
  /**
   * The vendor's authorization server refuses a hosted callback, so only the
   * desktop shell's loopback listener can finish the flow. A browser tab gets
   * no error back either: the refusal happens on the vendor's own page.
   */
  native_callback_only: boolean;
  /**
   * The vendor allows one connected AI platform per account and drops the
   * previous one, so connecting is destructive to a connection elsewhere.
   */
  exclusive_connection: boolean;
  /**
   * What this vendor's tools can be granted in, in display order. Empty for a
   * vendor nothing is curated for, which reads as "nothing to choose" rather
   * than "everything": the backend answers the same way, and a connect that
   * names no group is granted none of them.
   */
  capabilities: CapabilityGroup[];
}

/**
 * One consent toggle offered when connecting a brokerage.
 *
 * The key is the fact and also the translation key; the words are this client's,
 * the same contract the quirk booleans above keep. `tone` is how loudly to draw
 * the row -- `neutral` is public or personal data, `caution` is the user's own
 * positions and money, `danger` places real orders -- and is left a plain string
 * because a tone this build has no styling for must still render.
 */
export interface CapabilityGroup {
  key: string;
  tone: string;
  /**
   * One of the steps between reading and placing an order (paper, preview,
   * staged, live). A fact about the group rather than a reading of its key, so
   * a group added later reaches the badges with no release here. Absent on a
   * backend that predates it, which reads as "not a rung" and costs a badge.
   */
  rung?: boolean;
}

export async function getBrokerages(): Promise<Brokerage[]> {
  const { data } = await api.get<{ brokerages: Brokerage[] }>('/api/v1/mcp/brokerages');
  return data.brokerages ?? [];
}

/** Turn one on or off; the backend creates its catalog row the first time. */
export async function setBrokerageEnabled(
  name: string,
  enabled: boolean,
): Promise<CatalogServer> {
  const { data } = await api.patch<CatalogServer>(
    `/api/v1/mcp/brokerages/${name}/enabled`,
    { enabled },
  );
  return data;
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

/** The discovered tool snapshot for one catalog server (hash-gated server
 * side to the row's current config — empty until a discovery has run). */
export async function getMcpCatalogServerTools(name: string): Promise<{
  server_name: string;
  tools: McpToolSummary[];
  discovered_at: string | null;
}> {
  const { data } = await api.get(
    `/api/v1/mcp/servers/${encodeURIComponent(name)}/tools`,
  );
  return {
    server_name: data.server_name ?? name,
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
  /** Path on this origin to the mark the server declared in its handshake. */
  icon_url?: string | null;
  /** The bundle that ships this server, and whether that bundle is on — the
   *  same provenance pair a catalog row carries, so both group and explain
   *  themselves the same way. */
  plugin_name?: string | null;
  plugin_enabled?: boolean | null;
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

const OAUTH_CALLBACK_PATH = '/api/v1/mcp/oauth/callback';

/**
 * Offer the desktop shell's loopback listener as this flow's redirect target.
 *
 * Some authorization servers allowlist only the native-app profile and reject a
 * hosted callback outright, which is a wall a browser cannot get past. The shell
 * can, so it is asked first and its answer travels with the request: the backend
 * binds the redirect target into the flow at this moment and checks it again at
 * the token exchange, which is why this has to be settled here rather than
 * rewritten in the navigation later.
 *
 * Undefined means there is no listener on offer, and the flow proceeds the way
 * it does in a browser.
 */
async function loopbackFlow(): Promise<McpOAuthFlow | undefined> {
  // Built outside the ask, because the two failures are not the same fault and
  // must not answer alike: a base URL this cannot parse is a misconfigured
  // deployment, and reporting it as "no listener" sends whoever is debugging it
  // to the shell, which was fine. `beginMcpOAuth` swallows its own.
  const callback = new URL(
    OAUTH_CALLBACK_PATH,
    api.defaults.baseURL || window.location.origin,
  ).toString();
  return beginMcpOAuth(callback);
}

/** Phase 1's answer. `state` and `redirect_uri` are for the desktop path. */
export interface McpOauthStart {
  authorize_url: string;
  /** This flow's OAuth `state`; absent from a build older than the field. */
  state?: string;
  /** The callback the flow was really minted against; see `startMcpOauth`. */
  redirect_uri?: string;
}

/**
 * A connect that had to come home through the shell could not be set up that way.
 *
 * Carries the reason rather than a sentence because the four are not the same
 * fault and do not have the same remedy, and the words belong to whoever is
 * rendering them: this module has no translator. `shell-outdated` and
 * `no-listener` are the pair most worth keeping apart -- one is answered by
 * updating the app and the other by quitting a second copy of it, and a reader
 * given the wrong one of those goes looking for a window that is not open.
 */
export class LoopbackRequiredError extends Error {
  constructor(
    readonly reason: 'shell-outdated' | 'no-listener' | 'not-minted' | 'not-bound',
  ) {
    super(`loopback callback unavailable: ${reason}`);
    this.name = 'LoopbackRequiredError';
  }
}

/** What a connect needs to know beyond which row it is for. */
export interface StartMcpOauthOptions {
  /**
   * This vendor's authorization server will not accept the hosted callback at
   * all, so the flow can only come home through the desktop shell's listener.
   */
  vendorRefusesHostedCallback?: boolean;
  /**
   * The address the caller drew the row from, checked against the row before
   * anything starts.
   *
   * The row is the user's to edit from any tab, so what the page asked them
   * about and what the connect would do can be two different servers -- and a
   * vendor that allows one connected AI platform per account makes that
   * difference expensive. Naming the address is what turns the question the
   * page asked into a gate. A caller that names none has nothing to be wrong
   * about and is let through as before.
   */
  expectedUrl?: string | null;
  /**
   * Whether the caller still wants this connect, asked once there is a URL to
   * navigate to. Answering false gives back whatever this armed and returns
   * null.
   *
   * The question belongs here rather than to the caller because the thing that
   * has to be released is not the caller's to see: arming happens inside this
   * function, before either round trip below, and the flow id never leaves it.
   * A caller that took the connect back in the meantime would otherwise be
   * holding a listener it has no handle on, and the shell would run the flow's
   * full timeout and then raise the window over a connect the user stopped ten
   * minutes earlier.
   */
  stillWanted?: () => boolean;
  /**
   * The capability groups the user agreed this connection may carry, or
   * undefined for a connect that was never asked.
   *
   * The empty array and undefined are different answers and both travel as they
   * are: `[]` is a brokerage granted nothing, undefined is a server we curate no
   * groups for and hold no policy over. Collapsing them would make "declined
   * everything" and "not a brokerage" the same request, and only one of those
   * may reach a vendor's whole tool list.
   */
  grantedCapabilities?: string[];
}

/**
 * Phase 1 of the connect flow; the caller navigates to `authorize_url`.
 *
 * Everything past `returnTo` is named rather than positional. Three of the four
 * are optional and two of them are a boolean beside a callback, which is the
 * shape an argument gets silently passed in the wrong slot.
 */
export async function startMcpOauth(
  name: string,
  returnTo = '/plugins',
  {
    vendorRefusesHostedCallback = false,
    expectedUrl,
    stillWanted,
    grantedCapabilities,
  }: StartMcpOauthOptions = {},
): Promise<McpOauthStart | null> {
  // Asked unconditionally, because asking is free: `loopbackFlow` answers
  // undefined wherever there is no shell, so this arms a listener exactly where
  // one exists and changes nothing in a browser.
  const flow = await loopbackFlow();
  // Whether its absence is fatal is the real question, and it is fatal in two
  // separate situations.
  //
  // A vendor that refuses the hosted callback has no other way home anywhere.
  //
  // And inside the shell, NOTHING has another way home -- whatever the vendor
  // accepts, and whatever this shell is old enough to offer. The authorize URL
  // is an external navigation, so it leaves for the system browser, and the
  // callback then lands in a browser that never received this flow's nonce
  // cookie -- the cookie was set on the request this window made. The backend
  // refuses it as a state mismatch, in a browser the user may not even be
  // signed into, while this window sits on "connecting" because its navigation
  // was cancelled and it never unloaded. Degrading to the hosted callback there
  // is a guaranteed failure nobody is told about, which is worse than not
  // starting.
  if (!flow) {
    // Being IN the shell is the question, not what this shell can do. A build
    // that predates the loopback channel has no method to find, and reading
    // that as "not the desktop app" is how it ends up on the browser path --
    // the one path that cannot work here.
    if (isDesktopShell() && !canBeginMcpOAuth()) {
      throw new LoopbackRequiredError('shell-outdated');
    }
    if (vendorRefusesHostedCallback || isDesktopShell()) {
      throw new LoopbackRequiredError('no-listener');
    }
  }
  try {
    const { data } = await api.post<McpOauthStart>(
      `/api/v1/mcp/servers/${name}/oauth/start`,
      // One guard, and it is `loopbackFlow` above: every way of not having a
      // URI already arrives here as undefined. A second truthiness check would
      // read as belt and braces and is really a place for the two to disagree.
      {
        return_to: returnTo,
        ...(flow ? { redirect_uri: flow.redirectUri } : {}),
        ...(expectedUrl ? { expected_url: expectedUrl } : {}),
        // Presence, not truthiness, unlike the two above: an empty selection is
        // a decision the user made and has to be sent, and it is the one value
        // here that a shorthand check would drop.
        ...(grantedCapabilities !== undefined
          ? { granted_capabilities: grantedCapabilities }
          : {}),
      },
    );
    if (flow) {
      // Did the flow actually get minted against the listener we armed? A build
      // that predates the field ignores it and mints the hosted callback, and
      // so does this one when the value fails its loopback check -- both answer
      // 200, so without asking, the only symptom is a listener holding for five
      // minutes on a code that was never coming, and then raising the window.
      // For the vendors this path exists for there is no error even then: their
      // consent screen simply never redirects anywhere we can hear.
      if (data.redirect_uri !== flow.redirectUri || !data.state) {
        throw new LoopbackRequiredError('not-minted');
      }
      // Only now can the shell recognise this flow's callback. Before it, an
      // armed listener accepts nothing at all -- and a second connect started
      // in this window while the first was still minting takes the slot, so a
      // refusal here is this flow losing that race, not a broken shell.
      if (!(await bindMcpOAuth(flow.flowId, data.state))) {
        throw new LoopbackRequiredError('not-bound');
      }
    }
    // Asked last, with the listener armed and bound and the URL in hand, since
    // that is the widest the window gets: everything above is a round trip the
    // user could have pressed Cancel through. Not raised as a failure -- there
    // is nothing wrong here and nothing to tell them about a connect they are
    // the one who stopped.
    if (stillWanted && !stillWanted()) {
      if (flow) await cancelMcpOAuth(flow.flowId);
      return null;
    }
    return data;
  } catch (err) {
    // Asking armed a listener, and this is the answer that it was for nothing.
    // The order is forced -- the redirect_uri has to be in hand before the
    // request that binds it -- so the only way to keep arming honest is to say
    // when the flow it was armed for did not happen. By id: this window may
    // have started another connect since, and that one is still live.
    if (flow) await cancelMcpOAuth(flow.flowId);
    throw err;
  }
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
