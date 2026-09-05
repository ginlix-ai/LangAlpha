/**
 * Vault secrets + credential blueprints.
 */
import { api } from '@/api/client';

/** Workspace-tier vault secret. The user tier's twin is {@link UserVaultSecret}. */
export interface WorkspaceVaultSecret {
  workspace_vault_secret_id: string;
  name: string;
  description: string;
  masked_value: string;
  created_at: string;
  updated_at: string;
}

export async function getVaultSecrets(workspaceId: string): Promise<WorkspaceVaultSecret[]> {
  const { data } = await api.get<{ secrets?: WorkspaceVaultSecret[] }>(
    `/api/v1/workspaces/${workspaceId}/vault/secrets`,
  );
  return data.secrets ?? [];
}

export async function createVaultSecret(
  workspaceId: string,
  body: { name: string; value: string; description?: string },
): Promise<{ name: string }> {
  const { data } = await api.post<{ name: string }>(
    `/api/v1/workspaces/${workspaceId}/vault/secrets`,
    body,
  );
  return data;
}

export async function updateVaultSecret(
  workspaceId: string,
  name: string,
  body: { value?: string; description?: string },
): Promise<{ name: string }> {
  const { data } = await api.put<{ name: string }>(
    `/api/v1/workspaces/${workspaceId}/vault/secrets/${name}`,
    body,
  );
  return data;
}

export async function revealVaultSecret(workspaceId: string, name: string): Promise<string> {
  const { data } = await api.get<{ value: string }>(
    `/api/v1/workspaces/${workspaceId}/vault/secrets/${name}/reveal`,
  );
  return data.value;
}

export async function deleteVaultSecret(
  workspaceId: string,
  name: string,
): Promise<{ ok: boolean }> {
  const { data } = await api.delete<{ ok: boolean }>(
    `/api/v1/workspaces/${workspaceId}/vault/secrets/${name}`,
  );
  return data;
}

// --- Vault Blueprints (credentials recommended but not yet set) ---

export interface VaultBlueprint {
  name: string;
  label: string;
  description: string;
  docs_url: string | null;
  regex: string | null;
  sources: string[];
}

export interface VaultBlueprintsResponse {
  blueprints: VaultBlueprint[];
  remaining_slots: number;
}

export async function getVaultBlueprints(workspaceId: string): Promise<VaultBlueprintsResponse> {
  const { data } = await api.get<VaultBlueprintsResponse>(
    `/api/v1/workspaces/${workspaceId}/vault/blueprints`,
  );
  return data;
}

// --- Memory (agent long-term memory in LangGraph store) ---
