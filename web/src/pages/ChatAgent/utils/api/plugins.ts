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

export interface PluginDiagnostic {
  level: 'warning' | 'error';
  /** Where the finding isolates: the whole bundle, the MCP component, one
   * mcp.json entry, one skills/ directory, or one archive member. */
  scope: 'plugin' | 'mcp' | 'entry' | 'skill' | 'file';
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
  kind: 'mcp' | 'skill';
  /** Package identity: the mcp.json key or the skills/ directory name. */
  key: string;
  /** Installed row name (post name-coercion); '' when nothing landed. */
  name: string;
  renamed: boolean;
  status: PluginComponentStatus;
  reason: string;
  warnings: string[];
}

export interface PluginInstallReport {
  components: PluginComponentResult[];
  diagnostics: PluginDiagnostic[];
  /** Vault secrets auto-extracted from embedded literals. */
  secrets_created: string[];
  /** Declared blueprint names the user still has to fill in the vault. */
  secrets_required: string[];
  /** Package entries not modelled (README, LICENSE, extension dirs). */
  dropped_files: string[];
  servers_created: number;
  skills_created: number;
}

export interface PluginComponentRef {
  kind: 'mcp' | 'skill';
  name: string;
  key: string;
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

function uploadConfig(onProgress: ((percent: number) => void) | null) {
  return {
    headers: { 'Content-Type': 'multipart/form-data' },
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

export async function installPluginFromUrl(
  sourceUrl: string,
): Promise<PluginInstallResponse> {
  const { data } = await api.post<PluginInstallResponse>('/api/v1/plugins', {
    source_url: sourceUrl,
  });
  return data;
}

export async function installPluginFromZip(
  file: File,
  onProgress: ((percent: number) => void) | null = null,
): Promise<PluginInstallResponse> {
  const form = new FormData();
  form.append('file', file);
  const { data } = await api.post<PluginInstallResponse>(
    '/api/v1/plugins/upload',
    form,
    uploadConfig(onProgress),
  );
  return data;
}

/** Re-fetch a remote-sourced plugin and reconcile components. */
export async function updatePlugin(name: string): Promise<PluginInstallResponse> {
  const { data } = await api.post<PluginInstallResponse>(
    `/api/v1/plugins/${encodeURIComponent(name)}/update`,
  );
  return data;
}

export async function updatePluginFromZip(
  name: string,
  file: File,
  onProgress: ((percent: number) => void) | null = null,
): Promise<PluginInstallResponse> {
  const form = new FormData();
  form.append('file', file);
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

/** Fill declared secret blueprints; values land in the user vault. */
export async function bindPluginSecrets(
  name: string,
  secrets: Record<string, string>,
) {
  const { data } = await api.post<{ set: string[] }>(
    `/api/v1/plugins/${encodeURIComponent(name)}/bindings`,
    { secrets },
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
    deleted: { servers: string[]; skills: string[] };
  }>(`/api/v1/plugins/${encodeURIComponent(name)}`);
  return data;
}

/**
 * Export as a spec-compliant zip. Fetched as a blob because the bearer token
 * rides the axios interceptor; the caller revokes the URL after the click.
 */
export async function exportPluginBlobUrl(name: string): Promise<string> {
  const response = await api.get(
    `/api/v1/plugins/${encodeURIComponent(name)}/export`,
    { responseType: 'blob' },
  );
  return URL.createObjectURL(response.data as Blob);
}
