/**
 * Hook + utility for fetching the user's model-access tier from the platform
 * service.
 *
 * Fail-open: when the endpoint is unavailable (network error, auth failure),
 * `usePlatformModels()` returns null and `getModelAccess()` treats every model
 * as accessible.
 */
import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/api/client';
import { queryKeys } from '@/lib/queryKeys';
import { isPlatformMode } from '@/config/hostMode';
import type { ProviderModelsData } from '@/components/model/types';
import { customPairKey, type ModelMetadataEntry } from './useFilteredModels';
import type { PlatformModelsResponse, ModelAccess } from '@/types/platform';

/**
 * Fetch the authenticated user's model-access info from the platform service.
 *
 * Returns `null` on any error (fail-open when the endpoint is unavailable).
 * Data is cached for 5 minutes to avoid repeated calls.
 */
export function usePlatformModels(): PlatformModelsResponse | null {
  const { data } = useQuery<PlatformModelsResponse>({
    queryKey: queryKeys.platform.models(),
    queryFn: async () => {
      const res = await api.get<PlatformModelsResponse>('/api/auth/models');
      return res.data;
    },
    // Skip entirely in OSS mode — there's no platform auth service.
    enabled: isPlatformMode,
    staleTime: 5 * 60_000,
    gcTime: Infinity,
    retry: false,
    // Fail-open: swallow errors so the hook returns undefined (-> null below).
    meta: { suppressError: true },
    throwOnError: false,
  });
  // Validate shape — in OSS mode the endpoint may not exist and Vite can
  // return HTML (status 200) which Axios happily hands back as a string.
  if (!data || !Array.isArray(data.byok_providers)) return null;
  return data;
}

/**
 * Determine a model's access status given the platform response.
 *
 * Priority: BYOK > OAuth > tier check > locked.
 *
 * When `platform` is null (endpoint unavailable), every model is treated as
 * accessible so the UI never blocks anything.
 *
 * @param modelTier - The tier required by the model (from model_metadata.tier).
 * @param provider  - The parent/brand provider slug (e.g. "anthropic").
 * @param platform  - Platform response, or null if unavailable.
 */
export function getModelAccess(
  modelTier: number,
  provider: string,
  platform: PlatformModelsResponse | null,
): ModelAccess {
  if (!platform) return 'platform'; // No platform endpoint — everything accessible
  if (platform.byok_providers.includes(provider)) return 'byok';
  if (platform.oauth_providers.includes(provider)) return 'oauth';
  if (modelTier <= platform.model_tier) return 'platform';
  return 'locked';
}

/**
 * Build a model-name → ModelAccess map for badge display.
 *
 * Returns undefined when platform is null, so callers can skip badge
 * rendering entirely.
 *
 * For custom-pair entries (a model added under a user-configured provider
 * that shadows a built-in name), badges resolve against `groupKey` rather
 * than the built-in's metadata provider — the runtime resolver routes
 * shadowed names through the custom provider's credentials, so the badge
 * should reflect that ("byok"), not the platform availability of the
 * shadowed built-in.
 */
export function useModelAccessMap(
  models: Record<string, ProviderModelsData>,
  metadata: Record<string, ModelMetadataEntry>,
  platform: PlatformModelsResponse | null,
  customPairs?: Set<string>,
): Record<string, ModelAccess> | undefined {
  return useMemo(() => {
    if (!platform) return undefined;
    const map: Record<string, ModelAccess> = {};
    for (const [groupKey, group] of Object.entries(models)) {
      for (const m of group.models ?? []) {
        const meta = metadata[m];
        const isCustomPair = customPairs?.has(customPairKey(groupKey, m)) ?? false;
        const provider = isCustomPair ? groupKey : (meta?.provider ?? groupKey);
        const tier = typeof meta?.tier === 'number' ? meta.tier : 0;
        map[m] = getModelAccess(tier, provider, platform);
      }
    }
    return map;
  }, [models, metadata, platform, customPairs]);
}
