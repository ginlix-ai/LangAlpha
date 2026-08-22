import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
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
 */
export function invalidatePluginFanout(qc: QueryClient) {
  qc.invalidateQueries({ queryKey: queryKeys.plugins.all });
  qc.invalidateQueries({ queryKey: queryKeys.mcp.all });
  qc.invalidateQueries({ queryKey: queryKeys.skills.all });
  qc.invalidateQueries({ queryKey: queryKeys.userVault.all });
}

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
