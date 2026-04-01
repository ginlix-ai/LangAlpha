import { useMemo } from 'react';
import { useModels } from './useModels';
import { usePreferences } from './usePreferences';
import { usePlatformModels } from './usePlatformModels';
import { filterByPlatformTier, augmentPlatformWithLocal } from './useFilteredModels';
import { useConfiguredProviders } from './useConfiguredProviders';
import type { ProviderModelsData } from '@/components/model/types';

interface CustomModelEntry {
  name: string;
  model_id: string;
  provider: string;
}

/**
 * Returns the full models response with custom models merged in.
 * Custom models (from other_preference.custom_models) are appended
 * to the models map under their custom provider key.
 *
 * Also returns model_metadata with custom model entries added.
 */
export function useAllModels() {
  const { models: modelsData, isLoading: modelsLoading } = useModels();
  const { preferences, isLoading: prefsLoading } = usePreferences();
  const rawPlatform = usePlatformModels();
  const { providers: configuredProviders } = useConfiguredProviders();

  const customModels = useMemo<CustomModelEntry[]>(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const other = (prefs.other_preference ?? {}) as Record<string, unknown>;
    const cm = other.custom_models;
    if (!Array.isArray(cm)) return [];
    return cm as CustomModelEntry[];
  }, [preferences]);

  /** Models map with custom models merged in */
  const mergedModels = useMemo<Record<string, ProviderModelsData>>(() => {
    if (!modelsData) return {};
    const raw = modelsData as Record<string, unknown>;
    const providerMap = (raw.models ?? raw) as Record<string, Record<string, unknown>>;

    const out: Record<string, ProviderModelsData> = {};
    for (const [provider, data] of Object.entries(providerMap)) {
      if (!data || typeof data !== 'object') continue;
      out[provider] = {
        models: (data.models as string[]) ?? [],
        display_name: (data.display_name as string) ?? provider,
      };
    }

    // Append custom models grouped by their provider
    for (const cm of customModels) {
      const key = cm.provider;
      if (!out[key]) {
        out[key] = { models: [], display_name: key };
      }
      if (!out[key].models!.includes(cm.name)) {
        out[key].models!.push(cm.name);
      }
    }

    return out;
  }, [modelsData, customModels]);

  /** Model metadata with custom models added */
  const mergedMetadata = useMemo<Record<string, Record<string, unknown>>>(() => {
    if (!modelsData) return {};
    const raw = modelsData as Record<string, unknown>;
    const metadata = { ...((raw.model_metadata ?? {}) as Record<string, Record<string, unknown>>) };

    for (const cm of customModels) {
      if (!metadata[cm.name]) {
        metadata[cm.name] = { provider: cm.provider, sdk: 'custom' };
      }
    }

    return metadata;
  }, [modelsData, customModels]);

  // Augment platform with locally-known BYOK/OAuth providers so the
  // tier filter recognises connections that the platform service may not know about.
  const platform = useMemo(
    () => rawPlatform ? augmentPlatformWithLocal(rawPlatform, configuredProviders) : null,
    [rawPlatform, configuredProviders],
  );

  /** Apply platform tier filter (no-op when platform is null / OSS mode) */
  const filteredModels = useMemo(
    () => filterByPlatformTier(mergedModels, mergedMetadata, platform),
    [mergedModels, mergedMetadata, platform],
  );

  /** Raw models response with custom models merged + platform tier filtered */
  const mergedData = useMemo(() => {
    if (!modelsData) return null;
    const raw = modelsData as Record<string, unknown>;
    return {
      ...raw,
      models: Object.fromEntries(
        Object.entries(filteredModels).map(([k, v]) => [k, { display_name: v.display_name, models: v.models }]),
      ),
      model_metadata: mergedMetadata,
    };
  }, [modelsData, filteredModels, mergedMetadata]);

  return {
    models: mergedData,
    mergedModels: filteredModels,
    mergedMetadata,
    customModels,
    platform,
    isLoading: modelsLoading || prefsLoading,
  };
}
