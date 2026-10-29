import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { invalidateMcpFanout } from './useMcpServers';
import { invalidateSkillFanout } from './useSkills';
import {
  deletePlugin,
  getMarketplace,
  getPlugins,
  installPluginFromUrl,
  installPluginFromZip,
  setPluginEnabled,
  updatePlugin,
  updatePluginFromZip,
  upgradePluginSseEntries,
  type MarketplaceResponse,
} from '../pages/ChatAgent/utils/api';

/** React Query hooks for installed Agent Plugins packages. */

/**
 * One blast radius for every plugin mutation. Install fans components into
 * the MCP catalog, the skill tier and the subagent roster and can create
 * vault secrets, and each of those feeds a query that has already stopped
 * polling.
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
  qc.invalidateQueries({ queryKey: queryKeys.subagents.all });
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

/** The curated catalog the Discover tab lists. The server caches the listing
 *  for ten minutes, so a shorter staleTime would only re-read its cache;
 *  `installed` still refreshes on any install through the plugin fan-out. */
export function useMarketplace(enabled = true) {
  return useQuery({
    queryKey: queryKeys.plugins.marketplace(),
    queryFn: getMarketplace,
    enabled,
    staleTime: 5 * 60_000,
  });
}

/**
 * Flip one entry to installed, as `plugin`, in the cached listing. The
 * fan-out refetch that confirms it lands after the install resolves, so
 * without this the card reads "Install" again for that gap.
 */
export function markMarketplaceInstalled(qc: QueryClient, sourceUrl: string, plugin: string) {
  qc.setQueryData<MarketplaceResponse>(queryKeys.plugins.marketplace(), (old) =>
    old && {
      ...old,
      entries: old.entries.map((e) =>
        e.source_url === sourceUrl ? { ...e, installed: true, plugin } : e,
      ),
    },
  );
}

export function useInstallPluginFromUrl() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      sourceUrl,
      subdir,
      secrets,
    }: {
      sourceUrl: string;
      subdir?: string;
      /** The declared keys' values, checked before anything installs. */
      secrets?: Record<string, string>;
    }) => installPluginFromUrl(sourceUrl, subdir ?? null, secrets ?? null),
    onSuccess: () => invalidatePluginFanout(queryClient),
    // The variables can carry plaintext keys, and React Query keeps them in
    // the MutationCache for gcTime after settling -- five minutes by default,
    // unaffected by the dialog closing. Nothing reads them again, so drop them
    // the moment the request settles.
    gcTime: 0,
  });
}

export function useInstallPluginFromZip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      file,
      onProgress,
      subdir,
      secrets,
    }: {
      file: File;
      onProgress?: (percent: number) => void;
      subdir?: string;
      secrets?: Record<string, string>;
    }) => installPluginFromZip(file, onProgress ?? null, subdir ?? null, secrets ?? null),
    onSuccess: () => invalidatePluginFanout(queryClient),
    gcTime: 0,
  });
}

export function useUpdatePlugin() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      secrets,
      sourceUrl,
    }: {
      name: string;
      secrets?: Record<string, string>;
      /** Another revision of the plugin's own source: the catalog's newer one. */
      sourceUrl?: string | null;
    }) => updatePlugin(name, secrets ?? null, sourceUrl ?? null),
    onSuccess: () => invalidatePluginFanout(queryClient),
    // Plaintext keys in the variables; see the install hooks.
    gcTime: 0,
  });
}

export function useUpdatePluginFromZip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      file,
      secrets,
    }: {
      name: string;
      file: File;
      secrets?: Record<string, string>;
    }) => updatePluginFromZip(name, file, null, secrets ?? null),
    onSuccess: () => invalidatePluginFanout(queryClient),
    gcTime: 0,
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
