/**
 * `offPlanModels` is what the composer checks a saved Default against, so it
 * has to name every model the server refuses on the platform's key, and none
 * it runs: the composer names what it starts on, and a refused one blocks
 * every send until the user changes Settings.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';
import type { ConfiguredProvider } from '../useConfiguredProviders';
import type { PlatformModelsResponse } from '@/types/platform';

const h = vi.hoisted(() => ({
  platform: null as PlatformModelsResponse | null,
  providers: [] as ConfiguredProvider[],
  byokEnabled: true,
  customModels: [] as Array<{ name: string; model_id: string; provider: string }>,
  features: [],
}));

// The providers' own lists are read through React Query; none is read here.
vi.mock('../useProviderModels', () => ({ useMissingModels: () => ({ missingByProvider: new Map(), isLoading: false }) }));
vi.mock('../useModels', () => ({
  useModels: () => ({
    models: {
      models: {
        'prov-a': { models: ['m-free', 'm-pro', 'm-untiered'], display_name: 'A' },
        'codex-oauth': { models: ['m-oauth'], display_name: 'Codex' },
      },
      model_metadata: {
        'm-free': { provider: 'prov-a', access_type: 'api_key', tier: 0 },
        'm-pro': { provider: 'prov-a', access_type: 'api_key', tier: 2 },
        'm-untiered': { provider: 'prov-a', access_type: 'api_key' },
        'm-oauth': { provider: 'codex-oauth', access_type: 'oauth' },
      },
      provider_catalog: [
        { provider: 'prov-a', access_type: 'api_key' },
        { provider: 'codex-oauth', access_type: 'oauth' },
      ],
      key_candidates: {
        'prov-a': { slugs: ['prov-a', 'prov-a-cn', 'prov-a-coding'], shared: ['prov-a', 'prov-a-cn'] },
        'prov-b-cn': { slugs: ['prov-b-cn', 'prov-b', 'prov-b-coding'], shared: ['prov-b-cn', 'prov-b'] },
      },
    },
    isLoading: false,
  }),
}));
vi.mock('../usePreferences', () => ({
  usePreferences: () => ({
    preferences: { model_preference: { custom_models: h.customModels } },
    isLoading: false,
    isLoaded: true,
  }),
}));
vi.mock('../useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: h.providers, byokEnabled: h.byokEnabled, isLoading: false }),
}));
// Flags are read through React Query; settled here, so only the hook's own
// reads are in flight.
vi.mock('../useFeatures', async (importActual) => ({
  ...(await importActual<typeof import('../useFeatures')>()),
  useFeatures: () => ({ data: h.features, isLoading: false }),
}));
vi.mock('../usePlatformModels', async (importActual) => ({
  ...(await importActual<typeof import('../usePlatformModels')>()),
  usePlatformModels: () => ({ platform: h.platform, isLoading: false }),
}));

import { useAllModels } from '../useAllModels';

// A key the user saved, as the server masks it.
const key = (provider: string, extra: Partial<ConfiguredProvider> = {}): ConfiguredProvider => ({
  provider, display_name: provider, access_type: 'api_key', masked_key: 'sk-...1234', ...extra,
});
const KEY_A = key('prov-a');
const KEY_B = key('prov-b');

function offPlan(): string[] {
  const { result } = renderHookWithProviders(() => useAllModels());
  return [...result.current.offPlanModels].sort();
}

describe('useAllModels — offPlanModels', () => {
  beforeEach(() => {
    h.platform = { model_tier: 1, byok_providers: [], oauth_providers: [] };
    h.providers = [];
    h.byokEnabled = true;
    h.customModels = [];
  });

  it('names a model above the plan and one with no tier, which no plan includes', () => {
    expect(offPlan()).toEqual(['m-pro', 'm-untiered']);
  });

  it("names none of a provider the user holds a key for", () => {
    h.providers = [KEY_A];
    expect(offPlan()).toEqual([]);
  });

  it('counts a stored key for nothing while the BYOK switch is off', () => {
    h.providers = [KEY_A];
    h.byokEnabled = false;
    expect(offPlan()).toEqual(['m-pro', 'm-untiered']);
  });

  it('leaves an OAuth model out, so a lapsed connection keeps its reconnect refusal', () => {
    expect(offPlan()).not.toContain('m-oauth');
  });

  it("judges a custom model under a built-in's name as the built-in until a key is held for it", () => {
    h.customModels = [
      { name: 'm-pro', model_id: 'x', provider: 'prov-b' },
      { name: 'm-free', model_id: 'y', provider: 'prov-b' },
      { name: 'my-own', model_id: 'z', provider: 'prov-b' },
    ];
    expect(offPlan()).toEqual(['m-pro', 'm-untiered']);

    h.providers = [KEY_B];
    expect(offPlan()).toEqual(['m-untiered']);

    h.byokEnabled = false;
    expect(offPlan()).toEqual(['m-pro', 'm-untiered']);
  });

  // The server's `on_subscription`: it runs on the sign-in, like that
  // provider's built-ins, so no plan gates it.
  it("leaves out a custom model added on a sign-in, whatever built-in's name it takes", () => {
    h.customModels = [{ name: 'm-pro', model_id: 'x', provider: 'codex-oauth' }];
    expect(offPlan()).not.toContain('m-pro');
  });

  it('counts a key the server finds under a related slug when its vendor issues the two as one', () => {
    h.providers = [key('prov-a-cn')];
    expect(offPlan()).toEqual([]);

    h.providers = [key('prov-a-coding', { access_type: 'coding_plan' })];
    expect(offPlan()).toEqual(['m-pro', 'm-untiered']);

    h.providers = [{ ...h.providers[0], base_url: 'https://gateway.example.com/v1' }];
    expect(offPlan()).toEqual([]);
  });

  it("takes any key the server finds for a custom model, which is built against the slug holding it", () => {
    h.customModels = [{ name: 'm-pro', model_id: 'x', provider: 'prov-b-cn' }];
    h.providers = [key('prov-c')];
    expect(offPlan()).toContain('m-pro');

    h.providers = [key('prov-b-coding', { access_type: 'coding_plan' })];
    expect(offPlan()).not.toContain('m-pro');
  });

  it('is empty without a platform, which gates no model', () => {
    h.platform = null;
    expect(offPlan()).toEqual([]);
  });
});
