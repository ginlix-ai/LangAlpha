import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import {
  createUserVaultSecret,
  deleteUserVaultSecret,
  getUserVaultBlueprints,
  getUserVaultSecrets,
  updateUserVaultSecret,
} from '../pages/ChatAgent/utils/api';

/** React Query hooks for the user-level vault (Plugins → Secrets). */

export function useUserVaultSecrets(enabled = true) {
  return useQuery({
    queryKey: queryKeys.userVault.secrets(),
    queryFn: getUserVaultSecrets,
    enabled,
    staleTime: 30_000,
  });
}

/** Credentials builtin servers and enabled plugins declare but the vault
 * doesn't hold yet; any secret/plugin mutation invalidates it by prefix. */
export function useUserVaultBlueprints(enabled = true) {
  return useQuery({
    queryKey: queryKeys.userVault.blueprints(),
    queryFn: getUserVaultBlueprints,
    enabled,
    staleTime: 30_000,
  });
}

export function useCreateUserVaultSecret() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: { name: string; value: string; description?: string }) =>
      createUserVaultSecret(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
      // User-tier secrets feed needs_secret on the catalog AND on inherited
      // rows in every workspace list, and settled MCP queries stop polling.
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.all });
    },
  });
}

export function useUpdateUserVaultSecret() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      body,
    }: {
      name: string;
      body: { value?: string; description?: string };
    }) => updateUserVaultSecret(name, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.all });
    },
  });
}

export function useDeleteUserVaultSecret() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteUserVaultSecret(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
      queryClient.invalidateQueries({ queryKey: queryKeys.mcp.all });
    },
  });
}
