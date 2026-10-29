/**
 * Agent Plugins API — install/manage/export packages conforming to the
 * Agent Plugins open standard (agent-plugins.org).
 *
 * A plugin is a wrapper: installing fans its components into the existing
 * MCP catalog and skill tiers stamped with the plugin's identity, so every
 * other surface keeps reading the lists it already reads. These endpoints
 * own only identity and lifecycle (install, update, toggle, export, remove).
 */
import { api } from '@/api/client';
import { apiErrorCoded, apiErrorObject } from '@/api/errors';

export interface PluginDiagnostic {
  /** info: how the package was read, not a problem with it. */
  level: 'info' | 'warning' | 'error';
  /** Where the finding isolates: the whole bundle, the MCP component, one
   * mcp.json entry, one skills/ directory, one agents/ file, or one archive
   * member. */
  scope: 'plugin' | 'mcp' | 'entry' | 'skill' | 'subagent' | 'file';
  target: string;
  code: string;
  message: string;
  spec_ref?: string | null;
}

export type PluginComponentStatus =
  | 'created'
  | 'exists'
  | 'skipped'
  | 'invalid'
  | 'error'
  | 'upgradable'
  | 'updated'
  | 'deleted'
  | 'unchanged'
  | 'detached';

export interface PluginComponentResult {
  kind: 'mcp' | 'skill' | 'subagent';
  /** Package identity: the mcp.json key, the skills/ directory name or the
   * agents/ file name. */
  key: string;
  /** Installed row name (post name-coercion); '' when nothing landed. */
  name: string;
  renamed: boolean;
  status: PluginComponentStatus;
  reason: string;
  warnings: string[];
}

/**
 * One credential this package asked for and the vault doesn't hold yet.
 *
 * Deliberately not a `UserVaultBlueprint`: that one is the merged catalog,
 * where a name several packages declare collapses to a single card. This is
 * one package's request in its own words, which is what a consent screen has
 * to show.
 */
export interface PluginSecretRequest {
  name: string;
  label: string;
  description: string;
  docs_url: string | null;
  regex: string | null;
}

export interface PluginInstallReport {
  components: PluginComponentResult[];
  diagnostics: PluginDiagnostic[];
  /** Vault secrets auto-extracted from embedded literals. */
  secrets_created: string[];
  /** Blueprints the package declared that the user still has to fill in. */
  secrets_required: PluginSecretRequest[];
  /** Package entries not modelled (README, LICENSE, extension dirs). */
  dropped_files: string[];
  servers_created: number;
  skills_created: number;
  /** Absent from a server that predates plugin subagents. */
  subagents_created?: number;
}

export interface PluginComponentRef {
  kind: 'mcp' | 'skill' | 'subagent';
  name: string;
  key: string;
  /** Subagents only: the user changed it. */
  edited?: boolean;
}

export interface PluginInfo {
  name: string;
  version: string | null;
  description: string;
  author: string | null;
  homepage: string | null;
  source_type: string;
  source_ref: string | null;
  enabled: boolean;
  /** Where a bundle's vendor art is proxied from. Null means the mark is a
   *  file the frontend already ships, not that there is none. */
  icon_url?: string | null;
  repository?: string | null;
  license?: string | null;
  keywords?: string[];
  installed_at: string | null;
  updated_at: string | null;
  components: PluginComponentRef[];
}

export interface PluginListResponse {
  plugins: PluginInfo[];
  max_plugins: number;
  remaining_slots: number;
}

export interface PluginInstallResponse {
  plugin: PluginInfo;
  report: PluginInstallReport;
}

/** One plugin discovered inside a multi-plugin archive (a marketplace repo). */
export interface PluginCandidate {
  /** Subtree root inside the archive; re-request the install with this. */
  path: string;
  /** canonical, the vendor layout it was found in (claude/codex/cursor), or
   * 'external' for a marketplace entry living in another repo. */
  dialect: string;
  name: string | null;
  description: string | null;
  version: string | null;
  /** Set for external marketplace entries: install via this URL, not via
   * subdir. */
  source_url: string | null;
}

/**
 * The chooser payload from a 422 whose detail carries
 * `code: "multiple_plugins"` — the source holds several plugins and the
 * caller must pick one. Null for every other error.
 */
export function multiplePluginCandidates(
  err: unknown,
): PluginCandidate[] | null {
  const discovered = apiErrorCoded(err, 'multiple_plugins')?.discovered;
  return Array.isArray(discovered) ? discovered : null;
}

/**
 * The keys a 422 `plugin_secrets_required` asks for, or null for any other
 * error: declared keys the vault does not hold and the install did not send.
 * The install asks again with them in `secrets`.
 */
export function pluginSecretsRequired(err: unknown): PluginSecretRequest[] | null {
  const secrets = apiErrorCoded(err, 'plugin_secrets_required')?.secrets;
  if (!Array.isArray(secrets)) return null;
  return secrets.map((s: Partial<Record<keyof PluginSecretRequest, unknown>>) => ({
    name: String(s.name ?? ''),
    label: String(s.label || s.name || ''),
    description: typeof s.description === 'string' ? s.description : '',
    docs_url: typeof s.docs_url === 'string' ? s.docs_url : null,
    regex: typeof s.regex === 'string' ? s.regex : null,
  }));
}

/**
 * The diagnostics riding on an install/update failure — the per-component
 * findings the wizard renders beside the error. Empty for a failure that
 * carries none.
 */
export function fatalDiagnostics(err: unknown): PluginDiagnostic[] {
  const diags = apiErrorObject(err)?.diagnostics;
  return Array.isArray(diags) ? diags : [];
}

function uploadConfig(onProgress: ((percent: number) => void) | null) {
  return {
    onUploadProgress: onProgress
      ? (e: { loaded: number; total?: number }) => {
          if (e.total) onProgress(Math.round((e.loaded / e.total) * 100));
        }
      : undefined,
  };
}

export async function getPlugins(): Promise<PluginListResponse> {
  const { data } = await api.get<PluginListResponse>('/api/v1/plugins');
  return data;
}

export async function getPlugin(name: string): Promise<PluginInfo> {
  const { data } = await api.get<PluginInfo>(
    `/api/v1/plugins/${encodeURIComponent(name)}`,
  );
  return data;
}

/** Values for the keys a plugin declares, which its servers are checked with
 *  before anything is installed. Sent only when there are any. */
type PluginSecrets = Record<string, string> | null;

function nonEmpty(secrets: PluginSecrets): secrets is Record<string, string> {
  return !!secrets && Object.keys(secrets).length > 0;
}

export async function installPluginFromUrl(
  sourceUrl: string,
  subdir: string | null = null,
  secrets: PluginSecrets = null,
): Promise<PluginInstallResponse> {
  const { data } = await api.post<PluginInstallResponse>('/api/v1/plugins', {
    source_url: sourceUrl,
    ...(subdir !== null ? { subdir } : {}),
    ...(nonEmpty(secrets) ? { secrets } : {}),
  });
  return data;
}

export async function installPluginFromZip(
  file: File,
  onProgress: ((percent: number) => void) | null = null,
  subdir: string | null = null,
  secrets: PluginSecrets = null,
): Promise<PluginInstallResponse> {
  const form = new FormData();
  form.append('file', file);
  if (subdir !== null) form.append('subdir', subdir);
  if (nonEmpty(secrets)) form.append('secrets', JSON.stringify(secrets));
  const { data } = await api.post<PluginInstallResponse>(
    '/api/v1/plugins/upload',
    form,
    uploadConfig(onProgress),
  );
  return data;
}

/** Re-fetch a remote-sourced plugin and reconcile components. `secrets`
 *  answers a `plugin_secrets_required` refusal: keys the new version
 *  declares, checked with its servers before anything is written.
 *  `sourceUrl` moves it to another revision of its own source, the catalog
 *  entry's when the catalog holds a newer one; the server refuses any other
 *  package with `plugin_source_mismatch`. */
export async function updatePlugin(
  name: string,
  secrets: PluginSecrets = null,
  sourceUrl: string | null = null,
): Promise<PluginInstallResponse> {
  const body: { source_url?: string; secrets?: Record<string, string> } = {};
  if (sourceUrl) body.source_url = sourceUrl;
  if (nonEmpty(secrets)) body.secrets = secrets;
  const { data } = await api.post<PluginInstallResponse>(
    `/api/v1/plugins/${encodeURIComponent(name)}/update`,
    body.source_url || body.secrets ? body : undefined,
  );
  return data;
}

export async function updatePluginFromZip(
  name: string,
  file: File,
  onProgress: ((percent: number) => void) | null = null,
  secrets: PluginSecrets = null,
): Promise<PluginInstallResponse> {
  const form = new FormData();
  form.append('file', file);
  if (nonEmpty(secrets)) form.append('secrets', JSON.stringify(secrets));
  const { data } = await api.post<PluginInstallResponse>(
    `/api/v1/plugins/${encodeURIComponent(name)}/update/upload`,
    form,
    uploadConfig(onProgress),
  );
  return data;
}

export async function setPluginEnabled(name: string, enabled: boolean) {
  const { data } = await api.patch<{ name: string; enabled: boolean }>(
    `/api/v1/plugins/${encodeURIComponent(name)}/enabled`,
    { enabled },
  );
  return data;
}

/** Consent to installing held-back sse entries as streamable-http. */
export async function upgradePluginSseEntries(
  name: string,
  keys: string[],
): Promise<PluginInstallResponse> {
  const { data } = await api.post<PluginInstallResponse>(
    `/api/v1/plugins/${encodeURIComponent(name)}/sse-upgrades`,
    { keys },
  );
  return data;
}

export async function deletePlugin(name: string) {
  const { data } = await api.delete<{
    ok: boolean;
    /** `subagents` is absent from a server that predates plugin subagents. */
    deleted: { servers: string[]; skills: string[]; subagents?: string[] };
  }>(`/api/v1/plugins/${encodeURIComponent(name)}`);
  return data;
}

/**
 * Export as a spec-compliant zip. Fetched as a blob because the bearer token
 * rides the API client; the caller revokes the URL after the click.
 */
export async function exportPluginBlobUrl(name: string): Promise<string> {
  try {
    const response = await api.get(
      `/api/v1/plugins/${encodeURIComponent(name)}/export`,
      { responseType: 'blob' },
    );
    return URL.createObjectURL(response.data as Blob);
  } catch (err) {
    // A refused export still answers JSON, but responseType pins the parse to
    // Blob for the error body too, so the backend's detail reaches
    // formatApiErrorDetail as an opaque object and every failure reads
    // "Request failed with status code N". Re-parse it in place rather than
    // teaching the shared formatter about blobs.
    const response = (err as { response?: { data?: unknown } })?.response;
    if (response?.data instanceof Blob) {
      try {
        response.data = JSON.parse(await response.data.text());
      } catch {
        // Not JSON after all; leave the blob and let the fallback speak.
      }
    }
    throw err;
  }
}

/**
 * Trigger a browser download of the exported package. Same blob + anchor
 * pattern as `triggerUserMemoDownload`: the anchor is attached before the
 * click and the URL revoked only after it, because a detached anchor and a
 * same-tick revoke each drop the download in some browsers.
 */
export async function triggerPluginExportDownload(name: string): Promise<void> {
  const blobUrl = await exportPluginBlobUrl(name);
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = `${name}.zip`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(blobUrl);
}
