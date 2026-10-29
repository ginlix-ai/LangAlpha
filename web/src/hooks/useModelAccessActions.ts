import { useCallback } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { disconnectOAuth, isSubscriptionProvider, subscriptionKey } from '@/api/oauth';
import { deleteUserApiKey } from '@/api/model';
import { useReadStoredPreferences } from './useStoredPreferences';
import { useUpdatePreferences } from './useUpdatePreferences';
import { useModels } from './useModels';
import { useProviderCatalog } from './useProviderCatalog';
import { useConfiguredProviders, type ConfiguredProvider } from './useConfiguredProviders';
import { modelPrefs, pruneModelReferences, splitPreferenceWrite, type PreferencePatch } from '@/lib/modelPreferences';
import { withoutProvider } from '@/lib/customModels';
import { modelsLeavingWith } from '@/lib/connectionModels';

/**
 * Everything a new, changed or removed connection makes stale. Awaited, so a
 * caller that moves on next reads the new connection. The models catalog
 * (`queryKeys.models.all`) is not among it: the server builds that from the
 * deployment's manifest alone, the same for every user, and a user's own
 * models and providers reach the client through their preferences.
 */
export async function refreshAfterAccessChange(queryClient: QueryClient, provider: string): Promise<void> {
  const oauthKey = isSubscriptionProvider(provider) ? queryKeys.oauth.status(subscriptionKey(provider)) : null;
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: queryKeys.user.me() }),
    queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() }),
    queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() }),
    // The server drops its cached list on every change; ours follows.
    queryClient.invalidateQueries({ queryKey: queryKeys.providerModels.all }),
    ...(oauthKey ? [queryClient.invalidateQueries({ queryKey: oauthKey })] : []),
  ]);
}

export function useRefreshAfterAccessChange() {
  const queryClient = useQueryClient();
  return useCallback((provider: string) => refreshAfterAccessChange(queryClient, provider), [queryClient]);
}

/**
 * Takes a connection's models out of everything that names them (defaults,
 * fallbacks, the picked list, per-model settings), except a model another of
 * `connected` still serves, once its key or sign-in is gone.
 */
function usePruneReferences() {
  const readStored = useReadStoredPreferences();
  const updatePreferences = useUpdatePreferences();
  const { models: modelsData } = useModels();
  const { connectionOf } = useProviderCatalog();

  return useCallback(async (name: string, connected: ReadonlySet<string>) => {
    // From what is stored now, so a change another tab saved since is kept.
    const preferences = await readStored();
    const modelPref = modelPrefs(preferences);
    const customProviders = modelPref.custom_providers ?? [];
    const customModels = modelPref.custom_models ?? [];
    const remainingProviders = customProviders.filter((cp) => cp.name !== name);
    const remainingModels = withoutProvider(customModels, name, connectionOf);
    const removed = modelsLeavingWith(name, {
      customModels,
      manifest: modelsData?.model_metadata ?? {},
      connected,
      keyCandidates: modelsData?.key_candidates,
      connectionOf,
    });
    // One flat patch: splitPreferenceWrite routes each key to its column. A
    // key left as it was stays out of it.
    const patch: PreferencePatch = {
      ...(remainingProviders.length !== customProviders.length
        ? { custom_providers: remainingProviders.length > 0 ? remainingProviders : null }
        : {}),
      ...(remainingModels.length !== customModels.length
        ? { custom_models: remainingModels.length > 0 ? remainingModels : null }
        : {}),
      ...pruneModelReferences(preferences, (m) => removed.has(m)),
    };

    if (Object.keys(patch).length > 0) await updatePreferences.mutateAsync(splitPreferenceWrite(patch));
  }, [readStored, updatePreferences, modelsData, connectionOf]);
}

/**
 * Disconnects a subscription or removes a key, then prunes its models.
 * Rejects on failure, so the caller can say so. Once the connection is gone
 * the views refresh even if the cleanup fails, so none shows it still there.
 */
export function useRemoveAccess() {
  const queryClient = useQueryClient();
  const pruneReferences = usePruneReferences();
  const { configuredSet } = useConfiguredProviders();

  return useCallback(async (provider: ConfiguredProvider) => {
    const name = provider.provider;
    if (isSubscriptionProvider(name)) await disconnectOAuth(name);
    else await deleteUserApiKey(name);
    await pruneReferences(name, configuredSet).finally(() => refreshAfterAccessChange(queryClient, name));
  }, [pruneReferences, configuredSet, queryClient]);
}

/**
 * Prunes the models of the regions a coding plan's key just replaced (their
 * keys went in the same save). The plan's own models run on in the new region,
 * so only what the old regions alone served leaves the lists. A region that
 * offers the same models (MiniMax's two hosts) holds its picks as entries
 * under itself, which move to the new region first, keeping the defaults and
 * settings that name them; a region with models of its own (GLM's China
 * rows) has picks the new one does not list, and they leave.
 *
 * `stranded` are the plan's regions with no key at all. One holds entries only
 * where an earlier switch failed to move them, and since an entry lists itself
 * they would stay on the lists and fail on use, so they go with the replaced
 * regions' here, whichever save of the plan's key comes next.
 */
export function usePruneReplacedRegions() {
  const pruneReferences = usePruneReferences();
  const readStored = useReadStoredPreferences();
  const updatePreferences = useUpdatePreferences();
  const { optionFor } = useProviderCatalog();
  const { configuredSet } = useConfiguredProviders();

  return useCallback(async (replaced: readonly string[], by: string, stranded: readonly string[] = []) => {
    const customModels = modelPrefs(await readStored()).custom_models ?? [];
    const holding = new Set(customModels.map((cm) => cm.provider));
    const regions = [...new Set([...replaced, ...stranded.filter((p) => holding.has(p))])];
    const offered = optionFor(by)?.models_from;
    const alike = new Set(regions.filter((p) => offered && optionFor(p)?.models_from === offered));
    if (customModels.some((cm) => alike.has(cm.provider))) {
      // A name an entry elsewhere already holds is that entry's: one row each.
      const held = new Set(customModels.filter((cm) => !alike.has(cm.provider)).map((cm) => cm.name));
      const moved = customModels.flatMap((cm) => (
        !alike.has(cm.provider) ? [cm] : held.has(cm.name) ? [] : [{ ...cm, provider: by }]
      ));
      await updatePreferences.mutateAsync(splitPreferenceWrite({ custom_models: moved }));
    }
    const connected = new Set([...configuredSet].filter((p) => !regions.includes(p)));
    connected.add(by);
    for (const name of regions) await pruneReferences(name, connected);
  }, [pruneReferences, readStored, updatePreferences, optionFor, configuredSet]);
}
