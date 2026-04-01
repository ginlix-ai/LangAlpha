import { useMemo } from 'react';
import type { ProviderModelsData } from '@/components/model/types';
import type { ConfiguredProvider } from './useConfiguredProviders';
import type { PlatformModelsResponse } from '@/types/platform';
import { getModelAccess } from './usePlatformModels';

/**
 * Model metadata entry as returned by the `/api/v1/models` endpoint.
 */
export interface ModelMetadataEntry {
  provider?: string;
  sdk?: string;
  access_type?: string;
  /** "true" when variant needs its own API key (different env_key from parent). */
  requires_own_key?: string;
  /** Numeric tier for platform-served access. Absent = 0. */
  tier?: number;
}

/**
 * Pure function: filter a grouped models map so only models the user
 * has access to remain.
 *
 * Filter logic per model:
 *   1. Direct match: configuredSet has the model's own provider -> include
 *   2. GroupKey fallback: configuredSet has the groupKey AND the configured
 *      provider's type matches the model's access_type -> include
 *   3. Otherwise -> exclude
 *   4. If configuredSet is empty -> no filtering (all pass)
 */
export function filterModelsByAccess(
  providerMap: Record<string, ProviderModelsData>,
  metadata: Record<string, ModelMetadataEntry>,
  configuredSet: Set<string>,
  configuredTypeMap: Map<string, string>,
): Record<string, ProviderModelsData> {
  const hasFilter = configuredSet.size > 0;
  if (!hasFilter) return providerMap;

  const out: Record<string, ProviderModelsData> = {};

  for (const [groupKey, data] of Object.entries(providerMap)) {
    if (!data || typeof data !== 'object') continue;
    const allModels = data.models ?? [];

    const filtered = allModels.filter((m) => {
      const meta = metadata[m];
      const modelProvider = meta?.provider;
      if (!modelProvider) return false;

      // 1. Direct match on model's own provider
      if (configuredSet.has(modelProvider)) return true;

      // 2. GroupKey fallback — only if access_type matches AND variant
      //    shares credentials with parent (no independent env_key).
      if (configuredSet.has(groupKey)) {
        if (meta?.requires_own_key === 'true') return false;
        const configuredType = configuredTypeMap.get(groupKey);
        const modelAccessType = meta?.access_type ?? 'api_key';
        return configuredType === modelAccessType;
      }

      return false;
    });

    if (filtered.length > 0) {
      out[groupKey] = {
        models: filtered,
        display_name: data.display_name ?? groupKey,
      };
    }
  }

  return out;
}

/**
 * Build a type map from configured providers: provider key -> type.
 */
export function buildConfiguredTypeMap(
  providers: ConfiguredProvider[],
): Map<string, string> {
  return new Map(providers.map((p) => [p.provider, p.type]));
}

/**
 * Augment platform response with locally-known providers.
 *
 * The local app is the authority on BYOK keys and OAuth tokens, but
 * the platform service may not reflect recent connections (OAuth tokens
 * live in the local DB). Merging ensures the tier filter recognizes
 * locally-configured providers.
 */
export function augmentPlatformWithLocal(
  platform: PlatformModelsResponse,
  configuredProviders: ConfiguredProvider[],
): PlatformModelsResponse {
  const localByok: string[] = [];
  const localOAuth: string[] = [];
  for (const p of configuredProviders) {
    if (p.type === 'oauth') localOAuth.push(p.provider);
    else localByok.push(p.provider);
  }
  return {
    ...platform,
    byok_providers: [...new Set([...platform.byok_providers, ...localByok])],
    oauth_providers: [...new Set([...platform.oauth_providers, ...localOAuth])],
  };
}

/**
 * Pure function: remove models the user's tier doesn't cover.
 *
 * When `platform` is null (endpoint unavailable), no filtering is applied —
 * all models pass through.
 */
export function filterByPlatformTier(
  providerMap: Record<string, ProviderModelsData>,
  metadata: Record<string, ModelMetadataEntry>,
  platform: PlatformModelsResponse | null,
): Record<string, ProviderModelsData> {
  if (!platform) return providerMap;

  const out: Record<string, ProviderModelsData> = {};

  for (const [groupKey, data] of Object.entries(providerMap)) {
    if (!data || typeof data !== 'object') continue;
    const allModels = data.models ?? [];

    const filtered = allModels.filter((m) => {
      const meta = metadata[m];
      const provider = meta?.provider ?? groupKey;
      const hasTier = typeof meta?.tier === 'number';
      const tier = hasTier ? meta!.tier! : 0;
      const access = getModelAccess(tier, provider, platform);

      // No explicit tier → not platform-supplied. Only show for BYOK/OAuth users.
      if (!hasTier && access !== 'byok' && access !== 'oauth') return false;

      return access !== 'locked';
    });

    if (filtered.length > 0) {
      out[groupKey] = {
        models: filtered,
        display_name: data.display_name ?? groupKey,
      };
    }
  }

  return out;
}

/**
 * React hook: filters models using `filterModelsByAccess`.
 *
 * Takes the grouped models map, model metadata, and configured providers.
 * Returns only models the user has access to.
 */
export function useFilteredModels(
  providerMap: Record<string, ProviderModelsData>,
  metadata: Record<string, ModelMetadataEntry>,
  configuredProviders: ConfiguredProvider[],
): Record<string, ProviderModelsData> {
  return useMemo(() => {
    const configuredSet = new Set(configuredProviders.map((p) => p.provider));
    const configuredTypeMap = buildConfiguredTypeMap(configuredProviders);
    return filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);
  }, [providerMap, metadata, configuredProviders]);
}
