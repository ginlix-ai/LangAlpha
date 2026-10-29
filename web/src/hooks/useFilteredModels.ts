import type { CustomModelEntry, KeyCandidates, ProviderModelsData, ReasoningDeclaration } from '@/components/model/types';
import type { ConfiguredProvider } from './useConfiguredProviders';
import type { PlatformModelsResponse } from '@/types/platform';
import type { NativeCompactionKind } from './useAllModels';
import { getModelAccess } from './usePlatformModels';

/**
 * Model metadata entry as returned by the `/api/v1/models` endpoint.
 */
export interface ModelMetadataEntry {
  /** Name the manifest authors for people to read. Absent = print the key. */
  display_name?: string;
  provider?: string;
  /** The provider's name for the model, where it is not the key. */
  model_id?: string;
  sdk?: string;
  access_type?: string;
  /** "true" when variant needs its own API key (different env_key from parent). */
  requires_own_key?: string;
  /** Numeric tier for platform-served access. Absent = 0. */
  tier?: number;
  /** OAuth-plan allowlist — the connected subscription's plan_type must be
   * one of these for the model to show. Absent = no plan gate. */
  oauth_plans?: string[];
  /** True for user-added custom models — bypasses all access filters. */
  is_custom_model?: boolean;
  /** Reasoning levels this model actually honors, weakest first. Absent means
   * the model has no reasoning control — render no selector rather than
   * assuming a ladder. */
  reasoning_efforts?: string[];
  /** Level used when the user has not picked one. */
  reasoning_effort_default?: string;
  /** Guidance level the model declares for itself. Absent = the detailed fail-safe. */
  prompt_guidance?: string;
  /** Compaction preset the model resolves to on its own — its declaration, else
   * the band its context window falls in. Absent = the deployment default. */
  compaction_profile?: string;
  /** Whose compaction the model takes, on by default. Absent or null = its
   * provider cannot compact for it. */
  native_compaction?: NativeCompactionKind | null;
  /** What a typical task costs at each level the model offers, as a multiple
   *  of the models API's `task_cost_baseline`; "any" for a model measured
   *  without a level. Absent = never measured. */
  task_cost?: Record<string, TaskCost>;
}

export interface TaskCost {
  value: number;
  /** False for a level the benchmark skipped, which carries the figure of the
   *  nearest level above it: no more than this, rather than about this. */
  measured: boolean;
}

/** Key for the (group, name) pair in customPairs — '::' can't collide with any provider or model name. */
export function customPairKey(group: string, name: string): string {
  return `${group}::${name}`;
}

/**
 * Pure function: filter a grouped models map so only models the user
 * has access to remain.
 *
 * Filter logic per model:
 *   0a. (groupKey, name) is in customPairs -> include (user explicitly added
 *       this model under this group, e.g. starred a parent-catalog model
 *       under a variant provider)
 *   0b. metadata[name].is_custom_model -> include (user-added model with
 *       no parent-catalog collision)
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
  customPairs?: Set<string>,
): Record<string, ProviderModelsData> {
  const out: Record<string, ProviderModelsData> = {};

  for (const [groupKey, data] of Object.entries(providerMap)) {
    if (!data || typeof data !== 'object') continue;
    const allModels = data.models ?? [];

    const filtered = allModels.filter((m) => {
      // 0a. Group-scoped custom entry — user explicitly added this model
      // under this specific group (variant). The global metadata may still
      // point to the parent, so we key off the pair.
      if (customPairs?.has(customPairKey(groupKey, m))) return true;

      const meta = metadata[m];
      const modelProvider = meta?.provider;
      if (!modelProvider) return false;

      // 0b. Custom models are self-authorizing — user explicitly added them.
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
 *
 * Only exact provider slugs are registered. We do NOT promote ``brand_key``
 * here because a variant key (e.g. ``moonshot-coding``) does not grant access
 * to the parent brand's endpoint (backend BYOK resolves by exact slug + parent
 * walk, not sibling walk). Unlocking the brand in UI would show models the
 * backend can't route.
 */
export function buildConfiguredTypeMap(
  providers: ConfiguredProvider[],
): Map<string, string> {
  const map = new Map<string, string>();
  for (const p of providers) {
    map.set(p.provider, p.access_type);
  }
  return map;
}

/**
 * Build the "configured" set for filter purposes. Only exact provider slugs —
 * see ``buildConfiguredTypeMap`` for the rationale against brand_key
 * promotion.
 */
export function buildConfiguredSet(
  providers: ConfiguredProvider[],
): Set<string> {
  return new Set(providers.map((p) => p.provider));
}

/**
 * Each coding plan slug the user holds no key under itself whose key one of
 * their connections holds under another of the plan's slugs (a region of it,
 * or another protocol), mapped to that connection, as the server's walk finds
 * it there. A plan's models run on its key whichever of its slugs the key was
 * saved under, and as far as that key's own list goes; a brand's do not, which
 * is why only a coding plan's key is read this way.
 */
export function planKeyHolders(
  providers: ConfiguredProvider[],
  keyCandidates: Readonly<Record<string, KeyCandidates>>,
): Map<string, ConfiguredProvider> {
  const held = new Map(providers.filter((p) => p.access_type === 'coding_plan').map((p) => [p.provider, p]));
  const have = new Set(providers.map((p) => p.provider));
  const holders = new Map<string, ConfiguredProvider>();
  for (const [provider, { shared }] of Object.entries(keyCandidates)) {
    if (have.has(provider)) continue;
    const holder = shared.map((s) => held.get(s)).find(Boolean);
    if (holder) holders.set(provider, holder);
  }
  return holders;
}

/**
 * Augment platform response with locally-known providers.
 *
 * The local app is the authority on BYOK keys and OAuth tokens, but
 * the platform service may not reflect recent connections (OAuth tokens
 * live in the local DB). Merging ensures the tier filter recognizes
 * locally-configured providers.
 *
 * A stored key counts only while `byokEnabled`: with the switch off the
 * server runs no turn on it, so its models fall back to what the plan covers.
 */
export function augmentPlatformWithLocal(
  platform: PlatformModelsResponse,
  configuredProviders: ConfiguredProvider[],
  byokEnabled: boolean,
): PlatformModelsResponse {
  const localByok: string[] = [];
  const localOAuth: string[] = [];
  for (const p of configuredProviders) {
    if (p.access_type === 'oauth') localOAuth.push(p.provider);
    else if (byokEnabled) localByok.push(p.provider);
    // No brand_key promotion. A configured variant (e.g. moonshot-coding)
    // does not imply the parent brand (moonshot) is accessible — their keys
    // and endpoints are distinct. Promoting the brand would show models the
    // backend can't route.
  }
  return {
    ...platform,
    byok_providers: [...new Set([...platform.byok_providers, ...localByok])],
    oauth_providers: [...new Set([...platform.oauth_providers, ...localOAuth])],
  };
}

/** Whether a model limited to some of a subscription's plans (`oauth_plans`)
 *  is in `plan`, compared case-blind. An unknown plan fails open: the server
 *  enforces the same gate when a turn is admitted. */
export function planIncludes(meta: ModelMetadataEntry | undefined, plan: string | null | undefined): boolean {
  const plans = meta?.oauth_plans;
  if (!plans || plans.length === 0 || !plan) return true;
  return plans.some((allowed) => allowed.toLowerCase() === plan.toLowerCase());
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
  customPairs?: Set<string>,
): Record<string, ProviderModelsData> {
  if (!platform) return providerMap;

  const out: Record<string, ProviderModelsData> = {};

  for (const [groupKey, data] of Object.entries(providerMap)) {
    if (!data || typeof data !== 'object') continue;
    const allModels = data.models ?? [];

    const filtered = allModels.filter((m) => {
      // Group-scoped custom entry — same bypass as in filterModelsByAccess.
      if (customPairs?.has(customPairKey(groupKey, m))) return true;

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

/**
 * Pure function: drop the manifest models a connected credential's own list
 * lacks (`missing`, by the model's provider), so a key or plan never offers a
 * model it cannot run. A provider absent from `missingByProvider` keeps every
 * model: its list was not read, and an unread list hides nothing. A model the
 * user added themselves is theirs to keep; a manifest model they picked under
 * another provider (`routedPairs`) answers to that provider's list.
 */
export function dropMissingModels(
  providerMap: Record<string, ProviderModelsData>,
  metadata: Record<string, ModelMetadataEntry>,
  missingByProvider: ReadonlyMap<string, ReadonlySet<string>>,
  customPairs?: Set<string>,
  routedPairs?: ReadonlySet<string>,
): Record<string, ProviderModelsData> {
  if (missingByProvider.size === 0) return providerMap;
  const out: Record<string, ProviderModelsData> = {};
  for (const [groupKey, data] of Object.entries(providerMap)) {
    const models = (data.models ?? []).filter((m) => {
      const pair = customPairKey(groupKey, m);
      if (routedPairs?.has(pair)) return !missingByProvider.get(groupKey)?.has(m);
      if (customPairs?.has(pair)) return true;
      const meta = metadata[m];
      if (meta?.is_custom_model) return true;
      const provider = meta?.provider;
      return !provider || !missingByProvider.get(provider)?.has(m);
    });
    out[groupKey] = { ...data, models };
  }
  return out;
}

// ---------------------------------------------------------------------------
// buildVisibleModels — single pipeline: normalize → merge custom → filter
// ---------------------------------------------------------------------------

/** A custom entry as stored, which may still carry the flat keys entries
 *  were saved with before the `reasoning` block existed. */
type StoredCustomEntry = Omit<CustomModelEntry, 'reasoning'> & {
  reasoning?: Partial<ReasoningDeclaration>;
  reasoning_efforts?: string[];
  reasoning_effort_default?: string;
  compaction_profile?: string;
};

// The ladder an entry declares, from either shape it may be stored in. Mirrors
// `reasoning_block` on the server, precedence included: a `reasoning` object
// answers for the whole declaration, so an entry carrying both does not get
// its flat keys read as a fallback for the block's missing ones. An empty
// object is not such an answer, and `{}` being truthy here is what would have
// made this side read one anyway.
function declaredLadder(cm: StoredCustomEntry): {
  efforts?: string[];
  effortDefault?: string;
} {
  if (
    cm.reasoning &&
    typeof cm.reasoning === 'object' &&
    Object.keys(cm.reasoning).length > 0
  ) {
    return {
      ...('efforts' in cm.reasoning ? { efforts: cm.reasoning.efforts } : {}),
      ...('default' in cm.reasoning ? { effortDefault: cm.reasoning.default } : {}),
    };
  }
  return {
    ...('reasoning_efforts' in cm ? { efforts: cm.reasoning_efforts } : {}),
    ...('reasoning_effort_default' in cm ? { effortDefault: cm.reasoning_effort_default } : {}),
  };
}

/** `GET /api/v1/models`' `gateway_index`: the routes whose model ids meet the
 *  manifest's by slug, the manifest model each slug names, and per route the
 *  manifest models whose effort ladder a pick on it borrows. */
export interface GatewayIndex {
  routes: string[];
  models: Record<string, string>;
  borrows: Record<string, string[]>;
}

/** `vendor/name.with.dots:suffix` -> `name-with-dots`, lowercased: the
 *  server's `gateway_slug`, which the index is keyed by. */
export function gatewaySlug(modelId: string): string {
  const id = modelId.split(':', 1)[0];
  return id.slice(id.lastIndexOf('/') + 1).toLowerCase().replaceAll('.', '-');
}

/** The tuning defaults a gateway's pick takes from the built-in its id matches
 *  (`inherited_declarations` on the server), where the entry declares none:
 *  its guidance and compaction, and its effort ladder where the route's API
 *  carries that model's effort control. */
function gatewayTuning(
  cm: StoredCustomEntry,
  rawMetadata: Record<string, ModelMetadataEntry>,
  gateway: GatewayIndex | undefined,
): Partial<ModelMetadataEntry> {
  if (!gateway?.routes.includes(cm.provider) || !cm.model_id) return {};
  const key = gateway.models[gatewaySlug(cm.model_id)];
  const builtIn = key ? rawMetadata[key] : undefined;
  const borrows = !!key && (gateway.borrows?.[cm.provider] ?? []).includes(key);
  return {
    ...(builtIn?.prompt_guidance ? { prompt_guidance: builtIn.prompt_guidance } : {}),
    ...(builtIn?.compaction_profile ? { compaction_profile: builtIn.compaction_profile } : {}),
    ...(borrows && builtIn?.reasoning_efforts ? { reasoning_efforts: builtIn.reasoning_efforts } : {}),
    ...(borrows && builtIn?.reasoning_effort_default
      ? { reasoning_effort_default: builtIn.reasoning_effort_default }
      : {}),
  };
}

/** What a custom model's provider says about it (`useAllModels`' map). */
export interface ProviderRow {
  sdk?: string;
  /** Names the group of the user's models under this provider. */
  display_name?: string;
  /** The connection the provider's models are listed under: its own name,
   *  or for another API of a connection, that connection's. */
  connection: string;
}

/**
 * The manifest's metadata with the user's own models laid over it. What a
 * model declares about itself is the entry's answer where it gives one, and
 * the shadowed built-in's where it does not: a shadow reaches the same model
 * through the user's own key, so the ladder carries over. The server resolves
 * it identically (`with_inherited_declarations`), and on a route that speaks
 * another API, which borrows nothing, it sends the entry with an empty ladder
 * spelled out. Presence, not truthiness -- an entry declaring an empty ladder
 * is saying the model honors no levels, which the built-in's must not
 * overwrite.
 *
 * A pick from a gateway's list (OpenRouter, OpenCode Go) is named its own way,
 * so it stands for the built-in its id matches through the server's index, as
 * a turn on it does (`shadowed_model`), and takes only that one's tuning
 * defaults: its provider and access stay the entry's.
 *
 * The label is the exception. The entry's name is the label its author typed,
 * or the name a gateway's catalog gives the model, while the built-in's
 * display_name names the manifest's model, which this entry may not route to.
 * Provider compaction does not carry over either: it rides on the manifest's
 * own request settings, which a custom entry never inherits.
 */
export function withCustomModels(
  rawMetadata: Record<string, ModelMetadataEntry>,
  customModels: readonly StoredCustomEntry[],
  sdkOf: (provider: string) => string | undefined,
  gateway?: GatewayIndex,
): Record<string, ModelMetadataEntry> {
  const metadata: Record<string, ModelMetadataEntry> = { ...rawMetadata };
  for (const cm of customModels) {
    const sdk = sdkOf(cm.provider);
    const {
      display_name: _shadowedLabel,
      native_compaction: _shadowedNative,
      ...declared
    } = metadata[cm.name] ?? {
      provider: cm.provider,
      is_custom_model: true,
      ...(sdk ? { sdk } : {}),
      ...gatewayTuning(cm, rawMetadata, gateway),
    };
    const ladder = declaredLadder(cm);
    metadata[cm.name] = {
      ...declared,
      ...('efforts' in ladder ? { reasoning_efforts: ladder.efforts } : {}),
      ...('effortDefault' in ladder ? { reasoning_effort_default: ladder.effortDefault } : {}),
      ...('prompt_guidance' in cm ? { prompt_guidance: cm.prompt_guidance } : {}),
      ...('compaction_profile' in cm ? { compaction_profile: cm.compaction_profile } : {}),
      ...(cm.display_name ? { display_name: cm.display_name } : {}),
    };
  }
  return metadata;
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
  /** (group, name) pairs the user explicitly added via custom_models. Used by
   *  the badge map to render "byok" instead of "platform" for custom-pair
   *  entries that shadow a built-in name. */
  customPairs: Set<string>;
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
  customModels: StoredCustomEntry[],
  providerCatalog: Record<string, ProviderRow>,
  platform: PlatformModelsResponse | null,
  configuredProviders: ConfiguredProvider[],
  missingByProvider: ReadonlyMap<string, ReadonlySet<string>> = new Map(),
  gateway?: GatewayIndex,
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

  // 2. Merge custom models, each listed under its connection.
  const connectionOf = (provider: string) => providerCatalog[provider]?.connection ?? provider;
  // (group, name) pairs the user explicitly added. Bypasses filters for that
  // specific pair without polluting the global metadata — a parent-catalog
  // model starred under a variant must appear under the variant, not under
  // the parent group whose API metadata is still authoritative there.
  const customPairs = new Set<string>();
  // Of those, the manifest's own models picked under another provider (a
  // coding plan's pick of its brand's model): the same model, so that
  // provider's own list still decides it.
  const routedPairs = new Set<string>();
  for (const cm of customModels) {
    const key = connectionOf(cm.provider);
    const manifestEntry = rawMetadata[cm.name];
    if (manifestEntry && (cm.model_id || cm.name) === (manifestEntry.model_id ?? cm.name)) {
      routedPairs.add(customPairKey(key, cm.name));
    }
    if (!normalized[key]) {
      normalized[key] = { models: [], display_name: providerCatalog[key]?.display_name ?? key };
    }
    if (!normalized[key].models!.includes(cm.name)) {
      normalized[key].models!.push(cm.name);
    }
    customPairs.add(customPairKey(key, cm.name));
  }
  const metadata = withCustomModels(rawMetadata, customModels, (p) => providerCatalog[p]?.sdk, gateway);

  // Snapshot before filtering — preserves the FULL pre-filter picture for
  // consumers like ModelPickStep that need to show built-ins under their
  // parent group regardless of shadowing.
  const rawModels: Record<string, ProviderModelsData> = {};
  for (const [k, v] of Object.entries(normalized)) {
    rawModels[k] = { models: [...(v.models ?? [])], display_name: v.display_name };
  }

  // 3. Hide shadowed built-ins. When a custom_models entry shadows a
  // built-in of the same name, the resolver's classify_model returns
  // CUSTOM first (src/server/handlers/chat/llm_config.py:455-483) — the
  // platform path is unreachable as long as the shadow exists. Drop the
  // name from every group except the custom-pair owner so the picker
  // doesn't show two identically-routed rows.
  const shadowOwner: Record<string, string> = {};
  for (const cm of customModels) {
    if (!Object.hasOwn(shadowOwner, cm.name)) shadowOwner[cm.name] = connectionOf(cm.provider);
  }
  for (const [groupKey, data] of Object.entries(normalized)) {
    if (!data.models) continue;
    data.models = data.models.filter(
      (m) => !Object.hasOwn(shadowOwner, m) || shadowOwner[m] === groupKey,
    );
  }

  // 3b. OAuth plan gate — drop models whose `oauth_plans` allowlist excludes
  // the connected subscription's plan. Unknown plan fails open; the backend
  // enforces the same gate at turn admission either way.
  const oauthPlanByProvider = new Map<string, string>();
  for (const p of configuredProviders) {
    if (p.access_type === 'oauth' && p.plan_type) {
      oauthPlanByProvider.set(p.provider, p.plan_type.toLowerCase());
    }
  }
  for (const data of Object.values(normalized)) {
    if (!data.models) continue;
    data.models = data.models.filter((m) => {
      const provider = metadata[m]?.provider;
      return planIncludes(metadata[m], provider ? oauthPlanByProvider.get(provider) : undefined);
    });
  }

  // 3c. What the connected credentials' own lists say they cannot run.
  const listed = dropMissingModels(normalized, metadata, missingByProvider, customPairs, routedPairs);

  // 4. Filter
  let filtered: Record<string, ProviderModelsData>;
  if (platform) {
    filtered = filterByPlatformTier(listed, metadata, platform, customPairs);
  } else {
    const configuredSet = buildConfiguredSet(configuredProviders);
    const configuredTypeMap = buildConfiguredTypeMap(configuredProviders);
    filtered = filterModelsByAccess(listed, metadata, configuredSet, configuredTypeMap, customPairs);
  }

  // 5. Build validModelNames set
  const validModelNames = new Set<string>();
  for (const group of Object.values(filtered)) {
    for (const m of group.models ?? []) {
      validModelNames.add(m);
    }
  }

  return { models: filtered, metadata, rawModels, validModelNames, customPairs };
}

