import { useMemo } from 'react';
import { useModels } from './useModels';
import { usePreferences } from './usePreferences';
import { getModelAccess, usePlatformModels } from './usePlatformModels';
import { buildVisibleModels, augmentPlatformWithLocal, customPairKey, planKeyHolders } from './useFilteredModels';
import type { BuildVisibleModelsResult, ModelMetadataEntry, ProviderRow } from './useFilteredModels';
import { indexCatalog, ownProviderNames } from '@/lib/providerCatalog';
import { useConfiguredProviders } from './useConfiguredProviders';
import { useMissingModels } from './useProviderModels';
import { isPlatformMode } from '@/config/hostMode';
import type { ProviderModelsData, CustomModelEntry } from '@/components/model/types';
import type { ModelsResponse } from '@/api/model';
import type { PlatformModelsResponse } from '@/types/platform';
import { modelPrefs } from '@/lib/modelPreferences';
import { ownKeyRule, payerResolver, type Payer } from '@/lib/modelPayer';

export interface SystemDefaults {
  default_model?: string;
  background_model?: string;
  compaction_model?: string;
  fetch_model?: string;
  fallback_models?: string[];
  /** Deployment-pinned `prompt.guidance`; absent or null on the `auto` default. */
  prompt_guidance?: string | null;
}

export interface CompactionProfilePreset {
  token_threshold: number;
}

export type CompactionProfileName =
  | 'aggressive'
  | 'moderate'
  | 'extended'
  | 'economy'
  | 'relaxed'
  | 'spacious'
  | 'expansive'
  | 'maximal';

/** Whose compaction a model takes, on unless the user turns it off. The twin
 *  of `NativeKind` in `src/llms/provider_compaction.py`. */
export type NativeCompactionKind = 'anthropic' | 'openai';

export type CompactionProfileCatalog = Record<CompactionProfileName, CompactionProfilePreset>;

export interface SearchDepthInfo {
  name: string;
  display_name: string;
  min_tier: number;
}

export interface SearchProviderInfo {
  display_name: string;
  min_tier: number;
  default_depth: string;
  /** Ordered fastest → deepest, exactly as the manifest declares. */
  depths: SearchDepthInfo[];
}

export type SearchProviderCatalog = Record<string, SearchProviderInfo>;

export interface UseAllModelsResult {
  /** Filtered models the user can access (grouped by provider). */
  models: Record<string, ProviderModelsData>;
  /** Full metadata (including custom model entries). */
  metadata: Record<string, ModelMetadataEntry>;
  /** Pre-filter models (all models before access/tier gating). */
  rawModels: Record<string, ProviderModelsData>;
  /** Who pays for a turn on each listed model, the badge beside its name.
   *  A model nothing serves, or a local server, has none. */
  payers: Record<string, Payer>;
  /** Listed models the user added that no key or sign-in of theirs serves
   *  now (none saved, or their keys switched off). Unlike a local server's,
   *  which also has no payer, a turn on one is refused until a key is back. */
  keylessModels: Set<string>;
  /** The models the plan holds back, by provider: listed in a picker, but
   *  not pickable. */
  lockedModels: Record<string, ProviderModelsData>;
  /** Every listed model the server refuses when a turn names it: outside the
   *  plan and the user's own keys. Empty without a platform. */
  offPlanModels: Set<string>;
  /** Platform response (null in OSS mode or when endpoint unavailable). */
  platform: PlatformModelsResponse | null;
  /** System-level default model selections from the API. */
  systemDefaults: SystemDefaults | null;
  /** User-defined custom models from preferences. */
  customModels: CustomModelEntry[];
  /** Flat set of all model names in the filtered result. */
  validModelNames: Set<string>;
  /** Every model the catalog carries, access aside, custom models and custom
   *  provider names included: what the server's "does this model still
   *  exist" test (`model_resolves`) reads. Empty until
   *  the catalog and the custom models have both loaded, so a name missing
   *  from it means gone, never not loaded yet. */
  catalogModelNames: Set<string>;
  /** Compaction profile catalog from the models API (name → preset values). */
  compactionProfiles: CompactionProfileCatalog | null;
  /** Web-search provider catalog from the models API (name → tiers + depths). */
  searchProviders: SearchProviderCatalog | null;
  /** Raw models API response (for callers that need the full shape). */
  rawApiResponse: ModelsResponse | null;
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
  const { preferences, isLoading: prefsLoading, isLoaded: prefsLoaded } = usePreferences();
  const { platform: rawPlatform, isLoading: platformLoading } = usePlatformModels();
  const { providers: configuredProviders, byokEnabled, isLoading: configuredLoading } = useConfiguredProviders();

  const customModels = useMemo<CustomModelEntry[]>(() => {
    if (!preferences) return [];
    const cm = modelPrefs(preferences).custom_models;
    if (!Array.isArray(cm)) return [];
    return cm as CustomModelEntry[];
  }, [preferences]);

  /** What each provider a custom model can name says about it: its SDK, the
   *  name its group is listed under, and its connection. A region and another
   *  API of a connection are listed under the connection's entry's name. */
  const providerCatalog = useMemo<Record<string, ProviderRow>>(() => {
    const map: Record<string, ProviderRow> = {};
    const customProviders = modelPrefs(preferences).custom_providers;
    for (const [provider, row] of indexCatalog(modelsData?.provider_catalog ?? [], ownProviderNames(customProviders)).rows) {
      map[provider] = { sdk: row.sdk, display_name: row.entry.display_name, connection: row.connection };
    }
    // The user's own providers, which speak their parent's SDK.
    for (const cp of customProviders ?? []) {
      if (!map[cp.name] && cp.parent_provider && map[cp.parent_provider]) {
        map[cp.name] = { sdk: map[cp.parent_provider].sdk, connection: cp.name };
      }
    }
    return map;
  }, [modelsData, preferences]);

  // What the access filters count as connected: a coding plan's key also
  // under the plan's other slugs, whose models it runs.
  const planHolders = useMemo(
    () => planKeyHolders(configuredProviders, modelsData?.key_candidates ?? {}),
    [configuredProviders, modelsData],
  );
  const accessProviders = useMemo(
    () => (planHolders.size === 0
      ? configuredProviders
      : [...configuredProviders, ...[...planHolders].map(([provider, holder]) => ({ ...holder, provider }))]),
    [configuredProviders, planHolders],
  );

  // Augment platform with locally-known BYOK/OAuth providers so the
  // tier filter recognises connections that the platform service may not know about.
  const platform = useMemo(
    () => rawPlatform ? augmentPlatformWithLocal(rawPlatform, accessProviders, byokEnabled) : null,
    [rawPlatform, accessProviders, byokEnabled],
  );

  const systemDefaults = modelsData?.system_defaults ?? null;
  const compactionProfiles = modelsData?.compaction_profiles ?? null;
  const searchProviders = modelsData?.search_providers ?? null;

  // Each connected credential's own list, read to hide the manifest models it
  // cannot run. A key the server would not use (BYOK off on a hosted account)
  // and a local or user-defined provider have no list here.
  const listedProviders = useMemo(() => configuredProviders
    .filter((p) => !p.is_custom && p.access_type !== 'local')
    .filter((p) => (p.access_type === 'oauth' ? p.state === 'connected' : !isPlatformMode || byokEnabled))
    .map((p) => p.provider), [configuredProviders, byokEnabled]);
  const { missingByProvider: listedMissing, isLoading: listsLoading } = useMissingModels(listedProviders);
  // A plan slug run on a key held under another answers to that key's list.
  const missingByProvider = useMemo(() => {
    const out = new Map(listedMissing);
    for (const [provider, holder] of planHolders) {
      const missing = listedMissing.get(holder.provider);
      if (missing && !out.has(provider)) out.set(provider, missing);
    }
    return out;
  }, [listedMissing, planHolders]);

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
    // Read as the network sent it: a response without the map must still
    // render, not crash every view that lists models.
    return buildVisibleModels(
      modelsData.models ?? {},
      modelsData.model_metadata ?? {},
      customModels,
      providerCatalog,
      platform,
      accessProviders,
      missingByProvider,
      modelsData.gateway_index,
    );
  }, [modelsData, customModels, providerCatalog, platform, accessProviders, missingByProvider]);

  const catalogModelNames = useMemo(() => {
    const names = new Set<string>();
    if (!prefsLoaded) return names;
    for (const data of Object.values(visible.rawModels)) {
      for (const m of data.models ?? []) names.add(m);
    }
    // The server runs a custom provider's own name as a model too, through
    // the user's key for that provider, so it stays a live model here.
    const customProviders = preferences ? modelPrefs(preferences).custom_providers : null;
    for (const cp of Array.isArray(customProviders) ? customProviders : []) {
      if (typeof cp?.name === 'string' && cp.name) names.add(cp.name);
    }
    return names;
  }, [visible.rawModels, prefsLoaded, preferences]);

  const { payers, keylessModels } = useMemo(() => {
    const payerOf = payerResolver({
      platform,
      configured: configuredProviders,
      byokEnabled,
      keyCandidates: modelsData?.key_candidates ?? {},
    });
    const locals = new Set(
      configuredProviders.filter((p) => p.access_type === 'local').map((p) => p.provider),
    );
    const out: Record<string, Payer> = {};
    const keyless = new Set<string>();
    for (const [group, data] of Object.entries(visible.models)) {
      for (const m of data.models ?? []) {
        const meta = visible.metadata[m];
        // A model the user added under a provider of theirs runs through that
        // provider's key, whatever built-in it shares a name with.
        const custom = visible.customPairs.has(customPairKey(group, m));
        const payer = custom ? payerOf(meta, group, m) : payerOf(meta, meta?.provider ?? group);
        if (payer) out[m] = payer;
        else if (custom && !locals.has(group)) keyless.add(m);
      }
    }
    return { payers: out, keylessModels: keyless };
  }, [visible, platform, configuredProviders, byokEnabled, modelsData]);

  const lockedModels = useMemo(() => {
    const { rawModels, validModelNames, metadata } = visible;
    const out: Record<string, ProviderModelsData> = {};
    for (const [provider, data] of Object.entries(rawModels)) {
      const locked = (data.models ?? []).filter(
        (m) =>
          !validModelNames.has(m) &&
          getModelAccess(metadata[m]?.tier ?? 0, metadata[m]?.provider ?? provider, platform) === 'locked',
      );
      if (locked.length > 0) out[provider] = { ...data, models: locked };
    }
    return out;
  }, [visible, platform]);

  // Hidden by the plan filter, a tier-less model without the user's key
  // included, unless a key the server finds for it serves it: the filter reads
  // only a key under the model's own provider. An OAuth model is left out, as
  // the server leaves it: a lapsed connection is refused with its reconnect
  // prompt, not swapped for another. A custom model under a built-in's name
  // passes that filter as the user's own, but without a key for it the server
  // judges it as the built-in, which is what runs in its place. Whether a key
  // serves is the payer's rule, so no model is tagged "Your key" and refused.
  const offPlanModels = useMemo(() => {
    const out = new Set<string>();
    if (!platform || !modelsData) return out;
    const ownKey = ownKeyRule({
      configured: configuredProviders,
      byokEnabled,
      keyCandidates: modelsData.key_candidates ?? {},
    });
    const { rawModels, validModelNames, metadata } = visible;
    for (const [group, data] of Object.entries(rawModels)) {
      for (const m of data.models ?? []) {
        const meta = metadata[m];
        if (validModelNames.has(m) || meta?.access_type === 'oauth') continue;
        if (!ownKey.serves(meta?.provider ?? group)) out.add(m);
      }
    }
    const builtIns = modelsData.model_metadata ?? {};
    // One added on a sign-in runs on it, like that provider's built-ins, so no
    // plan gates it (the server's `on_subscription`).
    const signIns = new Set(
      (modelsData.provider_catalog ?? []).filter((p) => p.access_type === 'oauth').map((p) => p.provider),
    );
    for (const cm of customModels) {
      const builtIn = builtIns[cm.name];
      if (!builtIn || signIns.has(cm.provider)) continue;
      const covered = typeof builtIn.tier === 'number' && builtIn.tier <= platform.model_tier;
      if (!covered && !ownKey.holds(cm.name) && !ownKey.serves(cm.provider, true)) out.add(cm.name);
    }
    return out;
  }, [visible, platform, modelsData, customModels, configuredProviders, byokEnabled]);

  return {
    models: visible.models,
    metadata: visible.metadata,
    rawModels: visible.rawModels,
    payers,
    keylessModels,
    lockedModels,
    offPlanModels,
    platform,
    systemDefaults,
    customModels,
    validModelNames: visible.validModelNames,
    catalogModelNames,
    compactionProfiles,
    searchProviders,
    rawApiResponse: modelsData,
    isLoading: modelsLoading || prefsLoading || configuredLoading || platformLoading || listsLoading,
  };
}
