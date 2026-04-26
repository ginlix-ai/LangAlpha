import { describe, it, expect } from 'vitest';
import {
  filterModelsByAccess,
  filterByPlatformTier,
  augmentPlatformWithLocal,
  buildConfiguredTypeMap,
  buildConfiguredSet,
  buildVisibleModels,
} from '../useFilteredModels';
import type { ModelMetadataEntry } from '../useFilteredModels';
import type { ConfiguredProvider } from '../useConfiguredProviders';
import type { ProviderModelsData } from '@/components/model/types';
import type { PlatformModelsResponse } from '@/types/platform';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeProviderMap(
  entries: Record<string, string[]>,
): Record<string, ProviderModelsData> {
  const out: Record<string, ProviderModelsData> = {};
  for (const [key, models] of Object.entries(entries)) {
    out[key] = { models, display_name: key };
  }
  return out;
}

function makeConfigured(
  providers: ConfiguredProvider[],
): { configuredSet: Set<string>; configuredTypeMap: Map<string, string> } {
  return {
    configuredSet: new Set(providers.map((p) => p.provider)),
    configuredTypeMap: buildConfiguredTypeMap(providers),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('filterModelsByAccess', () => {
  it('includes models with a direct provider match', () => {
    const providerMap = makeProviderMap({
      openai: ['gpt-4o', 'gpt-4o-mini'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', access_type: 'api_key' },
      'gpt-4o-mini': { provider: 'openai', access_type: 'api_key' },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    expect(result.openai?.models).toEqual(['gpt-4o', 'gpt-4o-mini']);
  });

  it('includes groupKey fallback when access_type matches and no own key required', () => {
    // DeepInfra models grouped under openrouter — same api_key, shares credentials
    const providerMap = makeProviderMap({
      openrouter: ['deepinfra-model-1'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'deepinfra-model-1': { provider: 'deepinfra', access_type: 'api_key' },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'openrouter', display_name: 'OpenRouter', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    expect(result.openrouter?.models).toEqual(['deepinfra-model-1']);
  });

  it('excludes groupKey fallback when access_type differs (OAuth leak)', () => {
    // codex-oauth models grouped under openai — user only has api_key for openai
    const providerMap = makeProviderMap({
      openai: ['gpt-4o', 'gpt-5.4-oauth'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', access_type: 'api_key' },
      'gpt-5.4-oauth': { provider: 'codex-oauth', access_type: 'oauth' },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    // gpt-4o should be included (direct match), gpt-5.4-oauth excluded (different access_type)
    expect(result.openai?.models).toEqual(['gpt-4o']);
  });

  it('returns empty map when configuredSet is empty (zero keys = zero models)', () => {
    const providerMap = makeProviderMap({
      openai: ['gpt-4o'],
      anthropic: ['claude-sonnet'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai' },
      'claude-sonnet': { provider: 'anthropic' },
    };
    const configuredSet = new Set<string>();
    const configuredTypeMap = new Map<string, string>();

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    // Zero configured providers = zero visible models
    expect(Object.keys(result)).toEqual([]);
  });

  it('excludes models with no metadata entry', () => {
    const providerMap = makeProviderMap({
      openai: ['gpt-4o', 'unknown-model'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', access_type: 'api_key' },
      // 'unknown-model' has no metadata
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    expect(result.openai?.models).toEqual(['gpt-4o']);
  });

  it('excludes groupKey fallback when access_type differs (coding_plan leak)', () => {
    // dashscope-coding models grouped under dashscope — user only has api_key
    const providerMap = makeProviderMap({
      dashscope: ['qwen3-turbo', 'qwen3.5-plus-coding'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'qwen3-turbo': { provider: 'dashscope', access_type: 'api_key' },
      'qwen3.5-plus-coding': { provider: 'dashscope-coding', access_type: 'coding_plan' },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'dashscope', display_name: 'DashScope', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    expect(result.dashscope?.models).toEqual(['qwen3-turbo']);
  });

  it('excludes groupKey fallback when variant requires own key (regional variant)', () => {
    // z-ai-cn models grouped under z-ai — both api_key but different env_key
    const providerMap = makeProviderMap({
      'z-ai': ['glm-5.1', 'glm-5-turbo-cn'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'glm-5.1': { provider: 'z-ai', access_type: 'api_key' },
      'glm-5-turbo-cn': { provider: 'z-ai-cn', access_type: 'api_key', requires_own_key: 'true' },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'z-ai', display_name: 'Zhipu AI', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    // glm-5.1 included (direct match), glm-5-turbo-cn excluded (requires own key)
    expect(result['z-ai']?.models).toEqual(['glm-5.1']);
  });

  it('includes model when metadata is missing access_type (defaults to api_key)', () => {
    const providerMap = makeProviderMap({
      openai: ['gpt-4o'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai' }, // no access_type field
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    // Should include — access_type defaults to 'api_key', matches configured type
    expect(result.openai?.models).toEqual(['gpt-4o']);
  });

  it('always includes custom models (is_custom_model bypass)', () => {
    const providerMap = makeProviderMap({
      openai: ['gpt-4o'],
      'my-ollama': ['my-llama'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai' },
      'my-llama': { provider: 'my-ollama', is_custom_model: true },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    expect(result.openai?.models).toEqual(['gpt-4o']);
    expect(result['my-ollama']?.models).toEqual(['my-llama']);
  });

  it('custom models pass even with empty configuredSet', () => {
    const providerMap = makeProviderMap({
      openai: ['gpt-4o'],
      'my-ollama': ['my-llama'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai' },
      'my-llama': { provider: 'my-ollama', is_custom_model: true },
    };
    const configuredSet = new Set<string>();
    const configuredTypeMap = new Map<string, string>();

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    // Only custom model passes — no configured providers
    expect(result.openai).toBeUndefined();
    expect(result['my-ollama']?.models).toEqual(['my-llama']);
  });

  it('includes coding_plan model when coding_plan provider is configured', () => {
    const providerMap = makeProviderMap({
      dashscope: ['qwen3-turbo'],
      'dashscope-coding': ['qwen3.5-plus-coding'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'qwen3-turbo': { provider: 'dashscope', access_type: 'api_key' },
      'qwen3.5-plus-coding': { provider: 'dashscope-coding', access_type: 'coding_plan' },
    };
    const { configuredSet, configuredTypeMap } = makeConfigured([
      { provider: 'dashscope-coding', display_name: 'DashScope Coding', access_type: 'coding_plan' },
    ]);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);

    expect(result['dashscope-coding']?.models).toEqual(['qwen3.5-plus-coding']);
    expect(result.dashscope).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// filterByPlatformTier
// ---------------------------------------------------------------------------

function makePlatform(overrides: Partial<PlatformModelsResponse> = {}): PlatformModelsResponse {
  return {
    model_tier: 0,
    byok_providers: [],
    oauth_providers: [],
    ...overrides,
  };
}

describe('filterByPlatformTier', () => {
  it('passes all models through when platform is null', () => {
    const providerMap = makeProviderMap({ openai: ['gpt-4o'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', tier: 2 },
    };

    const result = filterByPlatformTier(providerMap, metadata, null);

    expect(result.openai?.models).toEqual(['gpt-4o']);
  });

  it('includes model when user tier >= model tier', () => {
    const providerMap = makeProviderMap({ openai: ['gpt-4o'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', tier: 1 },
    };

    const result = filterByPlatformTier(providerMap, metadata, makePlatform({ model_tier: 1 }));

    expect(result.openai?.models).toEqual(['gpt-4o']);
  });

  it('excludes model when user tier < model tier (locked)', () => {
    const providerMap = makeProviderMap({ openai: ['gpt-4o'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', tier: 2 },
    };

    const result = filterByPlatformTier(providerMap, metadata, makePlatform({ model_tier: 0 }));

    expect(result.openai).toBeUndefined();
  });

  it('includes model when user has BYOK for provider (regardless of tier)', () => {
    const providerMap = makeProviderMap({ openai: ['gpt-4o'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', tier: 2 },
    };

    const result = filterByPlatformTier(
      providerMap, metadata,
      makePlatform({ model_tier: 0, byok_providers: ['openai'] }),
    );

    expect(result.openai?.models).toEqual(['gpt-4o']);
  });

  it('includes model when user has OAuth for provider (regardless of tier)', () => {
    const providerMap = makeProviderMap({ anthropic: ['claude-sonnet'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'claude-sonnet': { provider: 'anthropic', tier: 2 },
    };

    const result = filterByPlatformTier(
      providerMap, metadata,
      makePlatform({ model_tier: 0, oauth_providers: ['anthropic'] }),
    );

    expect(result.anthropic?.models).toEqual(['claude-sonnet']);
  });

  it('excludes model with no tier and no BYOK/OAuth', () => {
    const providerMap = makeProviderMap({ openai: ['gpt-4o'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai' }, // no tier field
    };

    const result = filterByPlatformTier(providerMap, metadata, makePlatform({ model_tier: 0 }));

    expect(result.openai).toBeUndefined();
  });

  it('always includes custom models (is_custom_model bypass)', () => {
    const providerMap = makeProviderMap({ 'my-ollama': ['my-llama'] });
    const metadata: Record<string, ModelMetadataEntry> = {
      'my-llama': { provider: 'my-ollama', is_custom_model: true },
    };

    const result = filterByPlatformTier(providerMap, metadata, makePlatform({ model_tier: 0 }));

    expect(result['my-ollama']?.models).toEqual(['my-llama']);
  });
});

// ---------------------------------------------------------------------------
// augmentPlatformWithLocal
// ---------------------------------------------------------------------------

describe('augmentPlatformWithLocal', () => {
  it('merges BYOK providers without duplicates', () => {
    const platform = makePlatform({ byok_providers: ['openai'] });
    const providers: ConfiguredProvider[] = [
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
      { provider: 'anthropic', display_name: 'Anthropic', access_type: 'api_key' },
    ];

    const result = augmentPlatformWithLocal(platform, providers);

    expect(result.byok_providers).toEqual(['openai', 'anthropic']);
    expect(result.oauth_providers).toEqual([]);
  });

  it('merges OAuth providers without duplicates', () => {
    const platform = makePlatform({ oauth_providers: ['codex-oauth'] });
    const providers: ConfiguredProvider[] = [
      { provider: 'codex-oauth', display_name: 'Codex', access_type: 'oauth' },
      { provider: 'claude-oauth', display_name: 'Claude', access_type: 'oauth' },
    ];

    const result = augmentPlatformWithLocal(platform, providers);

    expect(result.oauth_providers).toEqual(['codex-oauth', 'claude-oauth']);
  });

  it('returns unchanged platform when no local providers', () => {
    const platform = makePlatform({ byok_providers: ['openai'], oauth_providers: ['codex-oauth'] });

    const result = augmentPlatformWithLocal(platform, []);

    expect(result.byok_providers).toEqual(['openai']);
    expect(result.oauth_providers).toEqual(['codex-oauth']);
  });
});

// ---------------------------------------------------------------------------
// buildVisibleModels
// ---------------------------------------------------------------------------

describe('buildVisibleModels', () => {
  it('OSS mode: filters by configured providers, custom models pass', () => {
    const rawApiModels = {
      openai: { models: ['gpt-4o'], display_name: 'OpenAI' },
      anthropic: { models: ['claude-sonnet'], display_name: 'Anthropic' },
    };
    const rawMetadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai' },
      'claude-sonnet': { provider: 'anthropic' },
    };
    const customModels = [{ name: 'my-llama', model_id: 'my-llama', provider: 'ollama' }];
    const configuredProviders: ConfiguredProvider[] = [
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' },
    ];

    const result = buildVisibleModels(rawApiModels, rawMetadata, customModels, {}, null, configuredProviders);

    // openai passes (configured), anthropic filtered out, custom model passes
    expect(result.models.openai?.models).toEqual(['gpt-4o']);
    expect(result.models.anthropic).toBeUndefined();
    expect(result.models.ollama?.models).toEqual(['my-llama']);
    expect(result.metadata['my-llama']?.is_custom_model).toBe(true);
    expect(result.validModelNames).toEqual(new Set(['gpt-4o', 'my-llama']));
    // rawModels has all models (pre-filter)
    expect(result.rawModels.anthropic?.models).toEqual(['claude-sonnet']);
  });

  it('variant bypass: parent-catalog model starred under variant shows under variant group', () => {
    // User has ``z-ai-coding`` configured (access_type=coding_plan). They
    // starred ``glm-5.1`` (a parent-catalog model, provider=z-ai) under the
    // variant in ModelPickStep, which writes ``{name:'glm-5.1', provider:
    // 'z-ai-coding'}`` to custom_models. The variant group must list it.
    //
    // Before the ``customPairs`` fix the variant entry was dropped by the
    // access_type mismatch (metadata.access_type='api_key' vs variant's
    // 'coding_plan'), so the user saw an empty group.
    const rawApiModels = {
      'z-ai': { models: ['glm-5.1', 'glm-5-turbo', 'glm-5v-turbo'], display_name: 'Zhipu AI' },
    };
    const rawMetadata: Record<string, ModelMetadataEntry> = {
      'glm-5.1': { provider: 'z-ai', access_type: 'api_key' },
      'glm-5-turbo': { provider: 'z-ai', access_type: 'api_key' },
      'glm-5v-turbo': { provider: 'z-ai', access_type: 'api_key' },
    };
    const customModels = [
      { name: 'glm-5.1', model_id: 'glm-5.1', provider: 'z-ai-coding' },
      { name: 'glm-5-turbo', model_id: 'glm-5-turbo', provider: 'z-ai-coding' },
    ];
    const configuredProviders: ConfiguredProvider[] = [
      { provider: 'z-ai-coding', display_name: 'Zhipu AI', access_type: 'coding_plan' },
    ];

    const result = buildVisibleModels(rawApiModels, rawMetadata, customModels, {}, null, configuredProviders);

    expect(result.models['z-ai-coding']?.models?.sort()).toEqual(['glm-5.1', 'glm-5-turbo'].sort());
    // Parent group stays hidden — the user didn't configure z-ai itself.
    expect(result.models['z-ai']).toBeUndefined();
    expect(result.validModelNames.has('glm-5.1')).toBe(true);
    expect(result.validModelNames.has('glm-5-turbo')).toBe(true);
    // Unstarred parent model stays gone.
    expect(result.validModelNames.has('glm-5v-turbo')).toBe(false);
  });

  it('shadow hide: a custom_models entry shadowing a built-in name removes the built-in from other groups', () => {
    // The runtime resolver's classify_model returns CUSTOM first whenever a
    // user's custom_models entry shares a name with a built-in
    // (src/server/handlers/chat/llm_config.py:455-483). The built-in path
    // becomes unreachable, so showing the built-in row in the picker is
    // misleading. This test pins the UI behavior: the shadowed row is
    // dropped from the parent group, and only the custom-pair owner shows.
    const rawApiModels = {
      'parent-prov': { models: ['model-shared', 'model-other'], display_name: 'Parent' },
    };
    const rawMetadata: Record<string, ModelMetadataEntry> = {
      'model-shared': { provider: 'parent-prov', tier: 1 },
      'model-other': { provider: 'parent-prov' },
    };
    const customModels = [
      { name: 'model-shared', model_id: 'model-shared', provider: 'byok-variant' },
    ];
    const platform = makePlatform({
      model_tier: 1,
      byok_providers: ['byok-variant'],
    });

    const result = buildVisibleModels(rawApiModels, rawMetadata, customModels, {}, platform, []);

    // model-shared must NOT appear under its parent group — the custom
    // shadow makes that row functionally unreachable.
    expect(result.models['parent-prov']?.models ?? []).not.toContain('model-shared');
    // model-shared lives only under the custom-pair owner.
    expect(result.models['byok-variant']?.models).toEqual(['model-shared']);
    // rawModels keeps the pre-shadow snapshot so ModelPickStep still shows
    // the built-in under the parent catalog when configuring it directly.
    expect(result.rawModels['parent-prov']?.models).toContain('model-shared');
    // customPairs is exposed so the badge map can render "byok" instead
    // of "platform" for shadowing rows.
    expect(result.customPairs.has('byok-variant::model-shared')).toBe(true);
  });

  it('platform mode: filters by tier, custom models pass', () => {
    const rawApiModels = {
      openai: { models: ['gpt-4o', 'gpt-4o-mini'], display_name: 'OpenAI' },
    };
    const rawMetadata: Record<string, ModelMetadataEntry> = {
      'gpt-4o': { provider: 'openai', tier: 2 },
      'gpt-4o-mini': { provider: 'openai', tier: 0 },
    };
    const customModels = [{ name: 'my-model', model_id: 'my-model', provider: 'custom-prov' }];
    const platform = makePlatform({ model_tier: 0 });

    const result = buildVisibleModels(rawApiModels, rawMetadata, customModels, {}, platform, []);

    // gpt-4o locked (tier 2 > user tier 0), gpt-4o-mini accessible
    expect(result.models.openai?.models).toEqual(['gpt-4o-mini']);
    expect(result.models['custom-prov']?.models).toEqual(['my-model']);
    expect(result.validModelNames.has('gpt-4o')).toBe(false);
    expect(result.validModelNames.has('gpt-4o-mini')).toBe(true);
    expect(result.validModelNames.has('my-model')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Variant isolation — configuring a coding-plan variant does NOT unlock the
// parent brand's catalog. Variant keys and endpoints are distinct from the
// parent's, so promoting the brand would show models the backend can't route.
// ---------------------------------------------------------------------------

describe('variant isolation (no brand_key promotion)', () => {
  it('buildConfiguredSet excludes brand_key for variant entries', () => {
    const set = buildConfiguredSet([
      {
        provider: 'z-ai-coding',
        display_name: 'Zhipu AI',
        access_type: 'coding_plan',
        brand_key: 'z-ai',
      },
    ]);
    expect(set.has('z-ai-coding')).toBe(true);
    expect(set.has('z-ai')).toBe(false);
  });

  it('buildConfiguredTypeMap does not register brand_key', () => {
    const map = buildConfiguredTypeMap([
      {
        provider: 'z-ai-coding',
        display_name: 'Zhipu AI',
        access_type: 'coding_plan',
        brand_key: 'z-ai',
      },
    ]);
    expect(map.get('z-ai-coding')).toBe('coding_plan');
    expect(map.has('z-ai')).toBe(false);
  });

  it('filterModelsByAccess hides the parent brand catalog when only a variant is configured', () => {
    const providerMap = makeProviderMap({
      'z-ai': ['glm-5.1', 'glm-5v-turbo'],
    });
    const metadata: Record<string, ModelMetadataEntry> = {
      'glm-5.1': { provider: 'z-ai', access_type: 'api_key' },
      'glm-5v-turbo': { provider: 'z-ai', access_type: 'api_key' },
    };
    const providers: ConfiguredProvider[] = [
      {
        provider: 'z-ai-coding',
        display_name: 'Zhipu AI',
        access_type: 'coding_plan',
        brand_key: 'z-ai',
      },
    ];
    const configuredSet = buildConfiguredSet(providers);
    const configuredTypeMap = buildConfiguredTypeMap(providers);

    const result = filterModelsByAccess(providerMap, metadata, configuredSet, configuredTypeMap);
    // Only z-ai-coding is configured. Brand models (provider=z-ai) must not
    // appear — the user has no z-ai key and the coding-plan key routes to a
    // different endpoint.
    expect(result['z-ai']).toBeUndefined();
  });

  it('augmentPlatformWithLocal does not promote brand_key', () => {
    const platform: PlatformModelsResponse = {
      tier: 0,
      model_tier: 0,
      byok_providers: [],
      oauth_providers: [],
    } as unknown as PlatformModelsResponse;
    const augmented = augmentPlatformWithLocal(platform, [
      {
        provider: 'z-ai-coding',
        display_name: 'Zhipu AI',
        access_type: 'coding_plan',
        brand_key: 'z-ai',
      },
    ]);
    expect(augmented.byok_providers).toContain('z-ai-coding');
    expect(augmented.byok_providers).not.toContain('z-ai');
  });
});
