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
  /** True for user-added custom models — bypasses all access filters. */
  is_custom_model?: boolean;
}

/**
 * Pure function: filter a grouped models map so only models the user
 * has access to remain.
 *
 * Filter logic per model:
 *   0. Custom models (is_custom_model) always pass — user explicitly added them
 *   1. Direct match: configuredSet has the model's own provider -> include
 *   2. GroupKey fallback: configuredSet has the groupKey AND the configured
 *      provider's type matches the model's access_type -> include
 *   3. Otherwise -> exclude
 */
export function filterModelsByAccess(
  providerMap: Record<string, ProviderModelsData>,
  metadata: Record<string, ModelMetadataEntry>,
  configuredSet: Set<string>,
  configuredTypeMap: Map<string, string>,
): Record<string, ProviderModelsData> {
  const out: Record<string, ProviderModelsData> = {};

  for (const [groupKey, data] of Object.entries(providerMap)) {
    if (!data || typeof data !== 'object') continue;
    const allModels = data.models ?? [];

    const filtered = allModels.filter((m) => {
      const meta = metadata[m];
      const modelProvider = meta?.provider;
      if (!modelProvider) return false;

      // 0. Custom models are self-authorizing — user explicitly added them.
      if (meta?.is_custom_model) return true;

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
 * Build a type map from configured providers: provider key -> access_type.
 */
export function buildConfiguredTypeMap(
  providers: ConfiguredProvider[],
): Map<string, string> {
  return new Map(providers.map((p) => [p.provider, p.access_type]));
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
    if (p.access_type === 'oauth') localOAuth.push(p.provider);
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

      // Custom models are self-authorizing — user explicitly added them.
      if (meta?.is_custom_model) return true;

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

// ---------------------------------------------------------------------------
// buildVisibleModels — single pipeline: normalize → merge custom → filter
// ---------------------------------------------------------------------------

interface CustomModelEntry {
  name: string;
  model_id: string;
  provider: string;
}

interface ProviderCatalogEntry {
  sdk?: string;
  parent_provider?: string;
}

export interface BuildVisibleModelsResult {
  /** Filtered models the user can access. */
  models: Record<string, ProviderModelsData>;
  /** Full metadata (including custom model entries). */
  metadata: Record<string, ModelMetadataEntry>;
  /** Pre-filter models (all models before access/tier gating). */
  rawModels: Record<string, ProviderModelsData>;
  /** Flat set of all model names in the filtered result. */
  validModelNames: Set<string>;
}

/**
 * Pure function: normalize raw API models, merge custom models, then apply
 * the appropriate filter (platform tier or configured-provider access).
 *
 * This is the single source of truth for "which models does the user see?"
 */
export function buildVisibleModels(
  rawApiModels: Record<string, Record<string, unknown>>,
  rawMetadata: Record<string, ModelMetadataEntry>,
  customModels: CustomModelEntry[],
  providerCatalog: Record<string, ProviderCatalogEntry>,
  platform: PlatformModelsResponse | null,
  configuredProviders: ConfiguredProvider[],
): BuildVisibleModelsResult {
  // 1. Normalize raw API shape → Record<string, ProviderModelsData>
  const normalized: Record<string, ProviderModelsData> = {};
  for (const [groupKey, data] of Object.entries(rawApiModels)) {
    if (!data || typeof data !== 'object') continue;
    normalized[groupKey] = {
      models: [...((data.models as string[]) ?? [])],
      display_name: (data.display_name as string) ?? groupKey,
    };
  }

  // 2. Merge custom models
  const metadata: Record<string, ModelMetadataEntry> = { ...rawMetadata };
  for (const cm of customModels) {
    const key = cm.provider;
    if (!normalized[key]) {
      normalized[key] = { models: [], display_name: key };
    }
    if (!normalized[key].models!.includes(cm.name)) {
      normalized[key].models!.push(cm.name);
    }
    if (!metadata[cm.name]) {
      const sdk = providerCatalog[cm.provider]?.sdk;
      metadata[cm.name] = {
        provider: cm.provider,
        is_custom_model: true,
        ...(sdk ? { sdk } : {}),
      };
    }
  }

  // Snapshot before filtering
  const rawModels: Record<string, ProviderModelsData> = {};
  for (const [k, v] of Object.entries(normalized)) {
    rawModels[k] = { models: [...(v.models ?? [])], display_name: v.display_name };
  }

  // 3. Filter
  let filtered: Record<string, ProviderModelsData>;
  if (platform) {
    filtered = filterByPlatformTier(normalized, metadata, platform);
  } else {
    const configuredSet = new Set(configuredProviders.map((p) => p.provider));
    const configuredTypeMap = buildConfiguredTypeMap(configuredProviders);
    filtered = filterModelsByAccess(normalized, metadata, configuredSet, configuredTypeMap);
  }

  // 4. Build validModelNames set
  const validModelNames = new Set<string>();
  for (const group of Object.values(filtered)) {
    for (const m of group.models ?? []) {
      validModelNames.add(m);
    }
  }

  return { models: filtered, metadata, rawModels, validModelNames };
}

