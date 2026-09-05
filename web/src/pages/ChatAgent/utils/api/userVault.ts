/**
 * User-level vault secrets (Plugins backing store). Mirrors the workspace
 * vault API but scoped to the signed-in user: these secrets back inherited
 * (user-level) MCP servers and are merged into every sandbox push, with
 * workspace secrets winning on name collision.
 */
import { api } from '@/api/client';

export interface UserVaultSecret {
  user_vault_secret_id: string;
  name: string;
  description: string;
  masked_value: string;
  created_at: string;
  updated_at: string;
}

export interface UserVaultSecretList {
  secrets: UserVaultSecret[];
  remaining_slots: number;
}

export async function getUserVaultSecrets(): Promise<UserVaultSecretList> {
  const { data } = await api.get<UserVaultSecretList>('/api/v1/mcp/vault/secrets');
  return { secrets: data.secrets ?? [], remaining_slots: data.remaining_slots ?? 0 };
}

/** A user-tier blueprint; plugin-declared entries carry the plugin's name. */
export interface UserVaultBlueprint {
  name: string;
  label: string;
  description: string;
  docs_url: string | null;
  regex: string | null;
  sources: string[];
  plugin_name?: string | null;
}

export interface UserVaultBlueprintsResponse {
  blueprints: UserVaultBlueprint[];
  remaining_slots: number;
}

/** Credentials builtin servers and enabled plugins declare but the user
 * vault doesn't hold yet — the install wizard's bindings data source. */
export async function getUserVaultBlueprints(): Promise<UserVaultBlueprintsResponse> {
  const { data } = await api.get<UserVaultBlueprintsResponse>(
    '/api/v1/mcp/vault/blueprints',
  );
  return { blueprints: data.blueprints ?? [], remaining_slots: data.remaining_slots ?? 0 };
}

export async function createUserVaultSecret(body: {
  name: string;
  value: string;
  description?: string;
}) {
  const { data } = await api.post('/api/v1/mcp/vault/secrets', body);
  return data as { name: string };
}

export async function updateUserVaultSecret(
  name: string,
  body: { value?: string; description?: string },
) {
  const { data } = await api.put(`/api/v1/mcp/vault/secrets/${name}`, body);
  return data as { name: string };
}

export async function revealUserVaultSecret(name: string): Promise<string> {
  const { data } = await api.get<{ value: string }>(
    `/api/v1/mcp/vault/secrets/${name}/reveal`,
  );
  return data.value;
}

export async function deleteUserVaultSecret(name: string) {
  const { data } = await api.delete(`/api/v1/mcp/vault/secrets/${name}`);
  return data as { ok: boolean };
}
