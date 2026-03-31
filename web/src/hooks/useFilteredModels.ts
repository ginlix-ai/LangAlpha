import { useMemo } from 'react';
import type { ProviderModelsData } from '@/components/model/types';
import type { ConfiguredProvider } from './useConfiguredProviders';

/**
 * Model metadata entry as returned by the `/api/v1/models` endpoint.
 */
export interface ModelMetadataEntry {
  provider?: string;
  sdk?: string;
  access_type?: string;
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

      // 2. GroupKey fallback — only if access_type matches
      if (configuredSet.has(groupKey)) {
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
