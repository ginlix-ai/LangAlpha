/**
 * Whether the app has confirmed a model cannot run for the user, which is when
 * the default step may replace a saved Background: a subscription's status
 * that failed to read leaves its models unlisted but confirms nothing, while a
 * plan read as excluding a model, or a model gone from the catalog, does.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  providers: [] as object[],
  missing: new Map<string, Set<string>>(),
  customModels: [] as object[],
}));

vi.mock('../useProviderModels', () => ({ useMissingModels: () => ({ missingByProvider: h.missing, isLoading: false }) }));
vi.mock('../useModels', () => ({
  useModels: () => ({
    models: {
      models: {
        'codex-oauth': { models: ['gpt-sol', 'gpt-luna'], display_name: 'ChatGPT' },
        'grok-oauth': { models: ['grok-fast'], display_name: 'Grok' },
        'z-ai': { models: ['glm-big'], display_name: 'Zhipu AI' },
      },
      model_metadata: {
        'gpt-sol': { provider: 'codex-oauth', access_type: 'oauth', oauth_plans: ['plus', 'pro'] },
        'gpt-luna': { provider: 'codex-oauth', access_type: 'oauth' },
        'grok-fast': { provider: 'grok-oauth', access_type: 'oauth' },
        'glm-big': { provider: 'z-ai', access_type: 'api_key' },
      },
    },
    isLoading: false,
  }),
}));
vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: { model_preference: { custom_models: h.customModels } }, isLoading: false, isLoaded: true }),
}));
vi.mock('../useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: h.providers, byokEnabled: false, isLoading: false }),
}));
vi.mock('../useClaudeCode', () => ({ useClaudeCodeStatus: () => ({ enabled: false }) }));
vi.mock('../usePlatformModels', () => ({
  usePlatformModels: () => ({ platform: null, isLoading: false }),
  useModelAccessMap: () => ({}),
  getModelAccess: () => 'platform',
}));

import { useAllModels } from '../useAllModels';

const signedIn = (provider: string, plan: string | null) => ({ provider, access_type: 'oauth', state: 'connected', plan_type: plan });
const listed = (result: { current: ReturnType<typeof useAllModels> }) =>
  Object.values(result.current.models).flatMap((p) => p.models ?? []);

describe('useAllModels().cannotRun', () => {
  beforeEach(() => { h.providers = []; h.missing = new Map(); h.customModels = []; });

  it('confirms nothing for a model whose subscription\'s status could not be read', () => {
    // ChatGPT's status failed, so its sign-in is not among the connections, while Grok's read.
    h.providers = [signedIn('grok-oauth', null)];
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(listed(result)).not.toContain('gpt-sol');
    expect(result.current.cannotRun('gpt-sol')).toBe(false);
  });

  it('confirms a model the plan read for its subscription excludes', () => {
    h.providers = [signedIn('codex-oauth', 'free'), signedIn('grok-oauth', null)];
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.cannotRun('gpt-sol')).toBe(true);
    expect(result.current.cannotRun('gpt-luna')).toBe(false);
  });

  it('confirms nothing for a model a coding plan runs that the parent key\'s list lacks', () => {
    h.providers = [
      { provider: 'z-ai', access_type: 'api_key' },
      { provider: 'z-ai-coding', access_type: 'coding_plan' },
    ];
    h.customModels = [{ name: 'glm-big', model_id: 'glm-big', provider: 'z-ai-coding' }];
    h.missing = new Map([['z-ai', new Set(['glm-big'])], ['z-ai-coding', new Set()]]);
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.models['z-ai-coding']?.models).toContain('glm-big');
    expect(result.current.cannotRun('glm-big')).toBe(false);
  });

  it('confirms a model every connection that runs it leaves off its list', () => {
    h.providers = [{ provider: 'z-ai', access_type: 'api_key' }];
    h.missing = new Map([['z-ai', new Set(['glm-big'])]]);
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.cannotRun('glm-big')).toBe(true);
  });

  it('confirms a model the catalog no longer carries', () => {
    h.providers = [signedIn('codex-oauth', 'pro')];
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.cannotRun('gpt-retired')).toBe(true);
    expect(result.current.cannotRun('gpt-sol')).toBe(false);
  });
});
