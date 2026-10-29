/**
 * Removing a connection takes its models out of every preference that names
 * them, a subscription's as much as a key's, and leaves a model another
 * connection still serves: a custom entry that took a built-in's name, or a
 * coding plan's pick that the brand's key runs too.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

const h = vi.hoisted(() => ({
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  deleteUserApiKey: vi.fn(async (_provider: string) => ({})),
  disconnectOAuth: vi.fn(async (_provider: string) => undefined),
  preferences: null as Record<string, unknown> | null,
  manifest: {} as Record<string, { provider: string }>,
  keyCandidates: {} as Record<string, { slugs: string[]; shared: string[] }>,
  connected: [] as string[],
  catalog: [] as unknown[],
}));

vi.mock('@/api/model', () => ({ deleteUserApiKey: h.deleteUserApiKey }));
vi.mock('@/api/oauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/oauth')>()),
  disconnectOAuth: h.disconnectOAuth,
}));
vi.mock('@/hooks/useModels', () => ({
  useModels: () => ({
    models: { model_metadata: h.manifest, key_candidates: h.keyCandidates, provider_catalog: h.catalog },
  }),
}));
// What the server holds when the removal reads it, the cleanup's starting point.
vi.mock('@/hooks/useStoredPreferences', () => ({ useReadStoredPreferences: () => async () => h.preferences }));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: h.mutateAsync }) }));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ configuredSet: new Set(h.connected) }),
}));

import { usePruneReplacedRegions, useRemoveAccess } from '../useModelAccessActions';
import type { ConfiguredProvider } from '../useConfiguredProviders';

const written = () => h.mutateAsync.mock.calls.at(-1)![0] as {
  model_preference?: Record<string, unknown>;
  other_preference?: Record<string, unknown>;
};

const remove = async (provider: ConfiguredProvider) => {
  const { result } = renderHookWithProviders(() => useRemoveAccess());
  await act(async () => { await result.current(provider); });
};

afterEach(() => {
  vi.clearAllMocks();
  h.preferences = null;
  h.connected = [];
  h.keyCandidates = {};
  h.catalog = [];
});

describe('useRemoveAccess', () => {
  it('leaves a model a custom provider serves under the same name', async () => {
    h.manifest = { 'gpt-terra': { provider: 'openai' }, 'gpt-sol': { provider: 'openai' } };
    h.connected = ['openai', 'my-azure'];
    h.preferences = {
      model_preference: {
        preferred_model: 'gpt-terra',
        fallback_models: ['gpt-terra', 'gpt-sol'],
        profiles: { 'gpt-terra': { reasoning_effort: 'high' }, 'gpt-sol': { reasoning_effort: 'low' } },
        custom_providers: [{ name: 'my-azure', parent_provider: 'openai' }],
        custom_models: [{ name: 'gpt-terra', model_id: 'terra-deploy', provider: 'my-azure' }],
      },
      other_preference: { starred_models: ['gpt-terra', 'gpt-sol'] },
    };

    await remove({ provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' });

    expect(h.deleteUserApiKey).toHaveBeenCalledWith('openai');
    const { model_preference, other_preference } = written();
    expect(model_preference).not.toHaveProperty('preferred_model');
    expect(model_preference?.fallback_models).toEqual(['gpt-terra']);
    expect(model_preference?.profiles).toEqual({ 'gpt-sol': null });
    expect(model_preference).not.toHaveProperty('custom_models');
    expect(other_preference?.starred_models).toEqual(['gpt-terra']);
  });

  it("keeps a coding plan's pick that the brand's key still runs", async () => {
    h.manifest = { 'glm-a': { provider: 'z-ai' } };
    h.connected = ['z-ai', 'z-ai-coding'];
    h.preferences = {
      model_preference: {
        preferred_model: 'glm-a',
        custom_models: [{ name: 'glm-a', model_id: 'glm-a', provider: 'z-ai-coding' }],
      },
      other_preference: { starred_models: ['glm-a'] },
    };

    await remove({ provider: 'z-ai-coding', display_name: 'Zhipu AI', access_type: 'coding_plan' });

    expect(written()).toEqual({ model_preference: { custom_models: null } });
  });

  it("takes a disconnected subscription's models off the defaults and the list", async () => {
    h.manifest = { 'gpt-sub': { provider: 'codex-oauth' }, 'gpt-sol': { provider: 'openai' } };
    h.connected = ['openai', 'codex-oauth'];
    h.preferences = {
      model_preference: { preferred_model: 'gpt-sub', preferred_background_model: 'gpt-sol' },
      other_preference: { starred_models: ['gpt-sub', 'gpt-sol'] },
    };

    await remove({ provider: 'codex-oauth', display_name: 'ChatGPT', access_type: 'oauth' });

    expect(h.disconnectOAuth).toHaveBeenCalledWith('codex-oauth');
    expect(written()).toEqual({
      model_preference: { preferred_model: null },
      other_preference: { starred_models: ['gpt-sol'] },
    });
  });

  it("keeps a model a region's key still runs, the vendor issuing the two as one", async () => {
    h.manifest = { 'glm-a': { provider: 'z-ai' } };
    h.connected = ['z-ai', 'z-ai-cn'];
    h.keyCandidates = { 'z-ai': { slugs: ['z-ai', 'z-ai-cn'], shared: ['z-ai', 'z-ai-cn'] } };
    h.preferences = {
      model_preference: { preferred_model: 'glm-a' },
      other_preference: { starred_models: ['glm-a'] },
    };

    await remove({ provider: 'z-ai', display_name: 'Zhipu AI', access_type: 'api_key' });

    expect(h.deleteUserApiKey).toHaveBeenCalledWith('z-ai');
    expect(h.mutateAsync).not.toHaveBeenCalled();
  });

  it('shows the connection gone even when its models could not be taken out', async () => {
    // The key is deleted by then, so a view still listing it offers a removal
    // that can no longer succeed.
    h.manifest = { 'gpt-sol': { provider: 'openai' } };
    h.connected = ['openai'];
    h.preferences = { model_preference: { preferred_model: 'gpt-sol' } };
    h.mutateAsync.mockRejectedValueOnce(new Error('write failed'));
    const { result, queryClient } = renderHookWithProviders(() => useRemoveAccess());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await act(async () => {
      await expect(result.current({ provider: 'openai', display_name: 'OpenAI', access_type: 'api_key' }))
        .rejects.toThrow('write failed');
    });

    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.user.apiKeys() });
  });
});

describe('usePruneReplacedRegions', () => {
  const plan = (provider: string, modelsFrom: string, cnModelsFrom: string) => ({
    provider,
    display_name: 'Plan',
    access_type: 'coding_plan',
    brand_key: modelsFrom,
    byok_eligible: true,
    models_from: modelsFrom,
    region_variants: [{ provider: `${provider}-cn`, display_name: 'Plan', region: 'cn', models_from: cnModelsFrom }],
  });

  const replace = async (replaced: string[], by: string, stranded: string[] = []) => {
    // What a write leaves stored, so the prune after the move reads it.
    h.mutateAsync.mockImplementation(async (payload) => {
      const pref = (payload as { model_preference?: Record<string, unknown> }).model_preference ?? {};
      const stored = (h.preferences?.model_preference ?? {}) as Record<string, unknown>;
      h.preferences = { ...h.preferences, model_preference: { ...stored, ...pref } };
      return {};
    });
    const { result } = renderHookWithProviders(() => usePruneReplacedRegions());
    await act(async () => { await result.current(replaced, by, stranded); });
  };

  it("moves a region's picks to the region that replaced it when both offer the same models", async () => {
    h.catalog = [plan('mm-coding', 'mm', 'mm')];
    h.manifest = { 'mm-a': { provider: 'mm' }, 'mm-b': { provider: 'mm' } };
    h.connected = ['mm-coding', 'mm-coding-cn'];
    h.preferences = {
      model_preference: {
        preferred_model: 'mm-a',
        profiles: { 'mm-a': { reasoning_effort: 'high' } },
        custom_models: [
          { name: 'mm-a', model_id: 'mm-a', provider: 'mm-coding' },
          { name: 'mm-b', model_id: 'mm-b', provider: 'mm-coding' },
        ],
      },
      other_preference: { starred_models: ['mm-a', 'mm-b'] },
    };

    await replace(['mm-coding'], 'mm-coding-cn');

    expect(h.mutateAsync).toHaveBeenCalledTimes(1);
    expect(written()).toEqual({
      model_preference: {
        custom_models: [
          { name: 'mm-a', model_id: 'mm-a', provider: 'mm-coding-cn' },
          { name: 'mm-b', model_id: 'mm-b', provider: 'mm-coding-cn' },
        ],
      },
    });
  });

  it("takes a region's picks out when the new region offers models of its own", async () => {
    h.catalog = [plan('glm-coding', 'glm', 'glm-cn')];
    h.manifest = { 'glm-a': { provider: 'glm' } };
    h.connected = ['glm-coding', 'glm-coding-cn'];
    h.preferences = {
      model_preference: {
        preferred_model: 'glm-a',
        custom_models: [{ name: 'glm-a', model_id: 'glm-a', provider: 'glm-coding' }],
      },
      other_preference: { starred_models: ['glm-a'] },
    };

    await replace(['glm-coding'], 'glm-coding-cn');

    expect(written()).toEqual({
      model_preference: { custom_models: null, preferred_model: null },
      other_preference: { starred_models: null },
    });
  });

  it('moves the picks a failed switch left under a region that now has no key', async () => {
    h.catalog = [plan('mm-coding', 'mm', 'mm')];
    h.manifest = { 'mm-a': { provider: 'mm' } };
    h.connected = ['mm-coding-cn'];
    h.preferences = {
      model_preference: {
        preferred_model: 'mm-a',
        custom_models: [{ name: 'mm-a', model_id: 'mm-a', provider: 'mm-coding' }],
      },
    };

    await replace([], 'mm-coding-cn', ['mm-coding']);

    expect(written()).toEqual({
      model_preference: { custom_models: [{ name: 'mm-a', model_id: 'mm-a', provider: 'mm-coding-cn' }] },
    });
  });

  it('leaves the preferences alone when no keyless region holds a pick', async () => {
    h.catalog = [plan('mm-coding', 'mm', 'mm')];
    h.manifest = { 'mm-a': { provider: 'mm' } };
    h.connected = ['mm-coding-cn'];
    h.preferences = { other_preference: { starred_models: ['mm-a'] } };

    await replace([], 'mm-coding-cn', ['mm-coding']);

    expect(h.mutateAsync).not.toHaveBeenCalled();
  });
});
