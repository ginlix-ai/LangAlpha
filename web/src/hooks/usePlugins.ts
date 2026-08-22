import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { invalidateMcpFanout } from './useMcpServers';
import { invalidateSkillFanout } from './useSkills';
import {
  bindPluginSecrets,
  deletePlugin,
  getPlugins,
  installPluginFromUrl,
  installPluginFromZip,
  setPluginEnabled,
  updatePlugin,
  updatePluginFromZip,
  upgradePluginSseEntries,
} from '../pages/ChatAgent/utils/api';

/** React Query hooks for installed Agent Plugins packages. */

/**
 * One blast radius for every plugin mutation. Install fans components into
 * the MCP catalog and the skill tier and can create vault secrets, and each
 * of those feeds a query that has already stopped polling.
 *
 * This is the widest of the three and belongs only to a mutation that really
 * is plugin-wide. An action confined to one tier takes that tier's fan-out
 * instead; both are re-exported here so the choice is visible in one place,
 * and each is defined beside the mutations that share its reasoning.
 */
export function invalidatePluginFanout(qc: QueryClient) {
  qc.invalidateQueries({ queryKey: queryKeys.plugins.all });
  invalidateMcpFanout(qc);
  invalidateSkillFanout(qc);
  qc.invalidateQueries({ queryKey: queryKeys.userVault.all });
}

export { invalidateMcpFanout, invalidateSkillFanout };

export function usePlugins(enabled = true) {
  return useQuery({
    queryKey: queryKeys.plugins.list(),
    queryFn: getPlugins,
    enabled,
    staleTime: 60_000,
  });
}

export function useInstallPluginFromUrl() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      sourceUrl,
      subdir,
    }: {
      sourceUrl: string;
      subdir?: string;
    }) => installPluginFromUrl(sourceUrl, subdir ?? null),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}

export function useInstallPluginFromZip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      file,
      onProgress,
      subdir,
    }: {
      file: File;
      onProgress?: (percent: number) => void;
      subdir?: string;
    }) => installPluginFromZip(file, onProgress ?? null, subdir ?? null),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}

export function useUpdatePlugin() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => updatePlugin(name),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}

export function useUpdatePluginFromZip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, file }: { name: string; file: File }) =>
      updatePluginFromZip(name, file),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}

export function useTogglePlugin() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setPluginEnabled(name, enabled),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}

export function useBindPluginSecrets() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      secrets,
    }: {
      name: string;
      secrets: Record<string, string>;
    }) => bindPluginSecrets(name, secrets),
    onSuccess: () => invalidatePluginFanout(queryClient),
    // The mutation's variables ARE the plaintext credentials, and React Query
    // keeps them in the MutationCache for gcTime after settling — five minutes
    // by default, and unaffected by the wizard closing. Nothing reads them
    // again, so drop them the moment the request settles.
    gcTime: 0,
  });
}

export function useUpgradePluginSseEntries() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, keys }: { name: string; keys: string[] }) =>
      upgradePluginSseEntries(name, keys),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}

export function useDeletePlugin() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deletePlugin(name),
    onSuccess: () => invalidatePluginFanout(queryClient),
  });
}
