import { useMemo } from 'react';
import { useModels } from './useModels';
import { usePreferences } from './usePreferences';
import { usePlatformModels, useModelAccessMap } from './usePlatformModels';
import { buildVisibleModels, augmentPlatformWithLocal } from './useFilteredModels';
import type { BuildVisibleModelsResult, ModelMetadataEntry } from './useFilteredModels';
import { useConfiguredProviders } from './useConfiguredProviders';
import type { ProviderModelsData, CustomModelEntry } from '@/components/model/types';
import type { PlatformModelsResponse, ModelAccess } from '@/types/platform';

interface SystemDefaults {
  default_model?: string;
  flash_model?: string;
  compaction_model?: string;
  fetch_model?: string;
  fallback_models?: string[];
}

export interface CompactionProfilePreset {
  token_threshold: number;
  truncate_args_trigger_messages: number;
  keep_messages: number;
}

export type CompactionProfileName = 'aggressive' | 'moderate' | 'extended' | 'relaxed';

export type CompactionProfileCatalog = Record<CompactionProfileName, CompactionProfilePreset>;

export interface UseAllModelsResult {
  /** Filtered models the user can access (grouped by provider). */
  models: Record<string, ProviderModelsData>;
  /** Full metadata (including custom model entries). */
  metadata: Record<string, ModelMetadataEntry>;
  /** Pre-filter models (all models before access/tier gating). */
  rawModels: Record<string, ProviderModelsData>;
  /** Model-name → access status map for badge display (undefined when no platform). */
  modelAccessMap: Record<string, ModelAccess> | undefined;
  /** Platform response (null in OSS mode or when endpoint unavailable). */
  platform: PlatformModelsResponse | null;
  /** System-level default model selections from the API. */
  systemDefaults: SystemDefaults | null;
  /** User-defined custom models from preferences. */
  customModels: CustomModelEntry[];
  /** Flat set of all model names in the filtered result. */
  validModelNames: Set<string>;
  /** Compaction profile catalog from the models API (name → preset values). */
  compactionProfiles: CompactionProfileCatalog | null;
  /** Raw models API response (for callers that need the full shape). */
  rawApiResponse: Record<string, unknown> | null;
  /** True while any upstream query is still loading. */
  isLoading: boolean;
}

/**
 * Single source of truth for "which models does the user see?"
 *
 * Fetches models, preferences, platform tier, and configured providers,
 * then runs the full normalize → merge custom → filter pipeline via
 * `buildVisibleModels()`.
 */
export function useAllModels(): UseAllModelsResult {
  const { models: modelsData, isLoading: modelsLoading } = useModels();
  const { preferences, isLoading: prefsLoading } = usePreferences();
  const rawPlatform = usePlatformModels();
  const { providers: configuredProviders, isLoading: configuredLoading } = useConfiguredProviders();

  const customModels = useMemo<CustomModelEntry[]>(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const other = (prefs.other_preference ?? {}) as Record<string, unknown>;
    const cm = other.custom_models;
    if (!Array.isArray(cm)) return [];
    return cm as CustomModelEntry[];
  }, [preferences]);

  /** Provider catalog for resolving SDK of custom models */
  const providerCatalog = useMemo<Record<string, { sdk?: string; parent_provider?: string }>>(() => {
    if (!modelsData) return {};
    const raw = modelsData as Record<string, unknown>;
    const catalog = (raw.provider_catalog ?? []) as Array<{ provider: string; sdk?: string }>;
    const map: Record<string, { sdk?: string }> = {};
    for (const entry of catalog) {
      map[entry.provider] = entry;
    }
    // Also index custom providers from preferences
    if (preferences) {
      const prefs = preferences as Record<string, unknown>;
      const other = (prefs.other_preference ?? {}) as Record<string, unknown>;
      const customProviders = (other.custom_providers ?? []) as Array<{ name: string; parent_provider?: string }>;
      for (const cp of customProviders) {
        if (!map[cp.name] && cp.parent_provider && map[cp.parent_provider]) {
          map[cp.name] = { sdk: map[cp.parent_provider].sdk };
        }
      }
    }
    return map;
  }, [modelsData, preferences]);

  // Augment platform with locally-known BYOK/OAuth providers so the
  // tier filter recognises connections that the platform service may not know about.
  const platform = useMemo(
    () => rawPlatform ? augmentPlatformWithLocal(rawPlatform, configuredProviders) : null,
    [rawPlatform, configuredProviders],
  );

  /** System defaults from the models API response */
  const systemDefaults = useMemo<SystemDefaults | null>(() => {
    if (!modelsData) return null;
    const raw = modelsData as Record<string, unknown>;
    return (raw.system_defaults as SystemDefaults) ?? null;
  }, [modelsData]);

  /** Compaction profile catalog from the models API */
  const compactionProfiles = useMemo<CompactionProfileCatalog | null>(() => {
    if (!modelsData) return null;
    const raw = modelsData as Record<string, unknown>;
    return (raw.compaction_profiles as CompactionProfileCatalog) ?? null;
  }, [modelsData]);

  /** Run the full pipeline: normalize → merge custom → filter */
  const visible = useMemo<BuildVisibleModelsResult>(() => {
    if (!modelsData) {
      return {
        models: {},
        metadata: {},
        rawModels: {},
        validModelNames: new Set(),
        customPairs: new Set(),
      };
    }
    const raw = modelsData as Record<string, unknown>;
    const rawApiModels = (raw.models ?? raw) as Record<string, Record<string, unknown>>;
    const rawMetadata = (raw.model_metadata ?? {}) as Record<string, ModelMetadataEntry>;

    return buildVisibleModels(
      rawApiModels,
      rawMetadata,
      customModels,
      providerCatalog,
      platform,
      configuredProviders,
    );
  }, [modelsData, customModels, providerCatalog, platform, configuredProviders]);

  const modelAccessMap = useModelAccessMap(
    visible.models,
    visible.metadata,
    platform,
    visible.customPairs,
  );

  return {
    models: visible.models,
    metadata: visible.metadata,
    rawModels: visible.rawModels,
    modelAccessMap,
    platform,
    systemDefaults,
    customModels,
    validModelNames: visible.validModelNames,
    compactionProfiles,
    rawApiResponse: modelsData ? (modelsData as Record<string, unknown>) : null,
    isLoading: modelsLoading || prefsLoading || configuredLoading,
  };
}
