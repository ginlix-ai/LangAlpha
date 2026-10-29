import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { invalidateMcpFanout } from './useMcpServers';
import {
  apiErrorStatus,
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

/**
 * A 404 or 409 means the list the user acted on is out of date (the key was
 * added or deleted from another tab, or the account filled up there), so the
 * list is read again and the form can point at what is really there.
 */
function refreshIfStale(queryClient: ReturnType<typeof useQueryClient>, err: unknown) {
  const status = apiErrorStatus(err);
  if (status === 404 || status === 409) {
    queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
  }
}

export function useCreateUserVaultSecret() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: { name: string; value: string; description?: string }) =>
      createUserVaultSecret(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
      // Account secrets feed needs_secret on the catalog AND on every workspace
      // list, and settled MCP queries stop polling.
      invalidateMcpFanout(queryClient);
    },
    onError: (err) => refreshIfStale(queryClient, err),
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
      invalidateMcpFanout(queryClient);
    },
    onError: (err) => refreshIfStale(queryClient, err),
  });
}

export function useDeleteUserVaultSecret() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteUserVaultSecret(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.userVault.all });
      invalidateMcpFanout(queryClient);
    },
    onError: (err) => refreshIfStale(queryClient, err),
  });
}
