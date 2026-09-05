import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import {
  createVaultSecret,
  deleteVaultSecret,
  getVaultBlueprints,
  getVaultSecrets,
  updateVaultSecret,
} from '../pages/ChatAgent/utils/api';

/** React Query hooks for the workspace-level vault (sandbox settings → Vault). */

export function useWorkspaceVaultSecrets(workspaceId: string, enabled = true) {
  return useQuery({
    queryKey: queryKeys.workspaceVault.secrets(workspaceId),
    queryFn: () => getVaultSecrets(workspaceId),
    enabled: enabled && !!workspaceId,
    staleTime: 30_000,
  });
}

/**
 * Recommended credentials declared by the workspace's enabled MCP servers.
 * Secondary to the list: `retry: false` keeps a broken blueprints endpoint from
 * holding the secrets UI in its loading state — callers degrade to no
 * recommendations rather than to an error.
 */
export function useVaultBlueprints(workspaceId: string, enabled = true) {
  return useQuery({
    queryKey: queryKeys.workspaceVault.blueprints(workspaceId),
    queryFn: () => getVaultBlueprints(workspaceId),
    enabled: enabled && !!workspaceId,
    staleTime: 30_000,
    retry: false,
  });
}

export function useCreateWorkspaceVaultSecret(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: { name: string; value: string; description?: string }) =>
      createVaultSecret(workspaceId, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaceVault.byWorkspace(workspaceId) });
      // Server rows derive needs_secret from vault state, and a settled MCP
      // list stops polling — without this the pill outlives the fix.
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}

export function useUpdateWorkspaceVaultSecret(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      body,
    }: {
      name: string;
      body: { value?: string; description?: string };
    }) => updateVaultSecret(workspaceId, name, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaceVault.byWorkspace(workspaceId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}

export function useDeleteWorkspaceVaultSecret(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteVaultSecret(workspaceId, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaceVault.byWorkspace(workspaceId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    },
  });
}
