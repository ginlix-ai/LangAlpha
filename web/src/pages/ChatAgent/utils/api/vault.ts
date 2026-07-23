/**
 * Vault secrets + credential blueprints.
 */
import { api } from '@/api/client';

export async function getVaultSecrets(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/vault/secrets`);
  return data.secrets;
}

export async function createVaultSecret(workspaceId: string, body: { name: string; value: string; description?: string }) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/vault/secrets`, body);
  return data;
}

export async function updateVaultSecret(workspaceId: string, name: string, body: { value?: string; description?: string }) {
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}/vault/secrets/${name}`, body);
  return data;
}

export async function revealVaultSecret(workspaceId: string, name: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/vault/secrets/${name}/reveal`);
  return data.value as string;
}

export async function deleteVaultSecret(workspaceId: string, name: string) {
  const { data } = await api.delete(`/api/v1/workspaces/${workspaceId}/vault/secrets/${name}`);
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
