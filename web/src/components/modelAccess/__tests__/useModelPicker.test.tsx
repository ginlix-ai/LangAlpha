/**
 * The picker acts on one connection, and preferences name models by name only:
 * a name another connection serves is not this one's to offer, pre-tick or
 * clear. A coding plan finds its brand's models by each model's own provider,
 * so the China plan finds the China models the catalog files under the
 * top-level brand. Only a connection just made opens with everything ticked.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  preferences: null as Record<string, unknown> | null,
  // What the server holds when the save reads it; the rendered preferences
  // unless a test has another tab write in between.
  stored: undefined as Record<string, unknown> | null | undefined,
  manifest: {} as Record<string, { provider: string; model_id?: string; oauth_plans?: string[] }>,
  rawModels: {} as Record<string, { models: string[] }>,
  connected: [] as string[],
  plans: {} as Record<string, string>,
  // The provider's own list, when a test reads one.
  available: undefined as Record<string, unknown> | undefined,
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({ rawModels: h.rawModels, metadata: h.manifest, isLoading: false }),
}));
vi.mock('@/hooks/useModels', () => ({ useModels: () => ({ models: { model_metadata: h.manifest } }) }));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: h.preferences, isLoaded: true }) }));
vi.mock('@/hooks/useStoredPreferences', () => ({
  useReadStoredPreferences: () => async () => (h.stored === undefined ? h.preferences : h.stored),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: h.mutateAsync }) }));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({
    configuredSet: new Set(h.connected),
    providers: Object.entries(h.plans).map(([provider, plan_type]) => ({ provider, access_type: 'oauth', plan_type })),
  }),
}));
vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ entryFor: (provider: string) => ({ provider }), connectionOf: (provider: string) => provider }),
}));
vi.mock('@/hooks/useProviderModels', () => ({
  useAvailableModels: () => ({ data: h.available, isLoading: false, isFetching: false }),
  useLocalProviderModels: () => ({ data: undefined, isLoading: false, isError: false }),
  useRefreshAvailableModels: () => vi.fn(),
}));

import { useModelPicker } from '../useModelPicker';

// The catalog files a region's models under the top-level brand, keyed apart
// from the name the provider knows them by.
const zhipu = () => {
  h.manifest = {
    'glm-a': { provider: 'z-ai' },
    'glm-b': { provider: 'z-ai' },
    'glm-a-cn': { provider: 'z-ai-cn', model_id: 'glm-a' },
  };
  h.rawModels = { 'z-ai': { models: ['glm-a', 'glm-b', 'glm-a-cn'] } };
};

const lastWrite = () => h.mutateAsync.mock.calls.at(-1)![0] as {
  model_preference?: Record<string, unknown>;
  other_preference?: Record<string, unknown>;
};

afterEach(() => {
  h.mutateAsync.mockClear();
  h.preferences = null;
  h.stored = undefined;
  h.connected = [];
  h.plans = {};
  h.available = undefined;
});

describe('useModelPicker on a coding plan', () => {
  it("gives the China plan's picks the entry that routes them through it, under the provider's name", async () => {
    zhipu();
    h.connected = ['z-ai-cn-coding'];
    const { result } = renderHookWithProviders(() =>
      useModelPicker({ provider: 'z-ai-cn-coding', modelsFrom: 'z-ai-cn', justConnected: true }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect([...result.current.selected]).toEqual(['glm-a-cn']);
    await act(async () => { await result.current.save(); });

    expect(lastWrite().model_preference?.custom_models).toEqual([
      { name: 'glm-a-cn', model_id: 'glm-a', provider: 'z-ai-cn-coding' },
    ]);
  });

  it("does not tick, reroute or clear the brand's own picks", async () => {
    zhipu();
    h.connected = ['z-ai', 'z-ai-coding'];
    h.preferences = {
      model_preference: { preferred_model: 'glm-a' },
      other_preference: { starred_models: ['glm-a'] },
    };
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'z-ai-coding', modelsFrom: 'z-ai' }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.selected.size).toBe(0);
    await act(async () => { await result.current.save(); });

    expect(lastWrite().other_preference?.starred_models).toEqual(['glm-a']);
    expect(lastWrite().model_preference).not.toHaveProperty('preferred_model');
    expect(lastWrite().model_preference?.custom_models ?? null).toBeNull();
  });

  it('keeps a pick it drops on the list when the brand key still runs it', async () => {
    zhipu();
    h.connected = ['z-ai', 'z-ai-coding'];
    h.preferences = {
      model_preference: {
        preferred_model: 'glm-a',
        custom_models: [{ name: 'glm-a', model_id: 'glm-a', provider: 'z-ai-coding' }],
      },
      other_preference: { starred_models: ['glm-a'] },
    };
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'z-ai-coding', modelsFrom: 'z-ai' }));

    await waitFor(() => expect(result.current.selected.has('glm-a')).toBe(true));
    act(() => result.current.toggle('glm-a'));
    await act(async () => { await result.current.save(); });

    expect(lastWrite().other_preference?.starred_models).toEqual(['glm-a']);
    expect(lastWrite().model_preference).not.toHaveProperty('preferred_model');
    expect(lastWrite().model_preference?.custom_models).toBeNull();
  });
});

describe('useModelPicker saving beside another tab', () => {
  it("keeps another provider's models and stars saved since the picker opened", async () => {
    zhipu();
    h.connected = ['z-ai-cn-coding', 'my-gw'];
    const theirs = { name: 'tab-model', model_id: 'tab-model', provider: 'my-gw' };
    h.stored = {
      model_preference: { custom_models: [theirs] },
      other_preference: { starred_models: ['tab-model'] },
    };
    const { result } = renderHookWithProviders(() =>
      useModelPicker({ provider: 'z-ai-cn-coding', modelsFrom: 'z-ai-cn', justConnected: true }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    await act(async () => { await result.current.save(); });

    expect(lastWrite().model_preference?.custom_models).toEqual([
      theirs,
      { name: 'glm-a-cn', model_id: 'glm-a', provider: 'z-ai-cn-coding' },
    ]);
    expect(lastWrite().other_preference?.starred_models).toEqual(['tab-model', 'glm-a-cn']);
  });
});

describe('useModelPicker beside a custom entry that took a built-in name', () => {
  it("does not offer it, so the key's picker cannot clear it", async () => {
    h.manifest = { 'gpt-terra': { provider: 'openai' }, 'gpt-sol': { provider: 'openai' } };
    h.rawModels = { openai: { models: ['gpt-terra', 'gpt-sol'] } };
    h.connected = ['openai', 'my-azure'];
    h.preferences = {
      model_preference: {
        preferred_model: 'gpt-terra',
        custom_models: [{ name: 'gpt-terra', model_id: 'terra-deploy', provider: 'my-azure' }],
      },
      other_preference: { starred_models: ['gpt-terra', 'gpt-sol'] },
    };
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'openai' }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.ready.map((r) => r.name)).toEqual(['gpt-sol']);
    act(() => result.current.toggle('gpt-sol'));
    await act(async () => { await result.current.save(); });

    expect(lastWrite().other_preference?.starred_models).toEqual(['gpt-terra']);
    expect(lastWrite().model_preference).not.toHaveProperty('preferred_model');
  });
});

describe('useModelPicker seed', () => {
  const openai = () => {
    h.manifest = { 'gpt-terra': { provider: 'openai' }, 'gpt-sol': { provider: 'openai' } };
    h.rawModels = { openai: { models: ['gpt-terra', 'gpt-sol'] } };
    h.connected = ['openai'];
    h.preferences = { other_preference: { starred_models: ['other-model'] } };
  };

  it('opens a connection saved with no models as it was left', async () => {
    openai();
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'openai' }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.selected.size).toBe(0);
  });

  it('starts a connection just made on every model it offers', async () => {
    openai();
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'openai', justConnected: true }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect([...result.current.selected].sort()).toEqual(['gpt-sol', 'gpt-terra']);
  });

  it("leaves out a subscription's models its plan does not include", async () => {
    h.manifest = {
      'gpt-terra-oauth': { provider: 'codex-oauth' },
      'gpt-spark-oauth': { provider: 'codex-oauth', oauth_plans: ['pro'] },
    };
    h.rawModels = { 'codex-oauth': { models: ['gpt-terra-oauth', 'gpt-spark-oauth'] } };
    h.connected = ['codex-oauth'];
    h.plans = { 'codex-oauth': 'plus' };
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'codex-oauth', justConnected: true }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect([...result.current.selected]).toEqual(['gpt-terra-oauth']);
    expect(result.current.ready.map((r) => r.name)).not.toContain('gpt-spark-oauth');
  });
});

describe("useModelPicker on a gateway's catalog", () => {
  // OpenCode Go: no manifest models of its own; the catalog describes two,
  // one the manifest knows, and lists one it does not describe.
  const gateway = () => {
    h.manifest = {};
    h.rawModels = {};
    h.connected = ['go'];
    h.available = {
      provider: 'go',
      status: 'ok',
      ready: [],
      missing: [],
      others: [
        { id: 'kimi-k9', display_name: 'Kimi K9', described: true, known: true, route: 'go-anthropic', parameters: { a: 1 } },
        { id: 'qwen-z', display_name: 'Qwen Z', described: true, route: 'go-chat' },
        { id: 'raw-model', route: 'go-chat' },
      ],
    };
  };

  it('lists the described models as ready, starts on the known ones, and saves them under their route', async () => {
    gateway();
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'go', justConnected: true }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.ready.map((r) => r.name)).toEqual(['go/kimi-k9', 'go/qwen-z']);
    expect([...result.current.selected]).toEqual(['go/kimi-k9']);
    expect(result.current.others.map((m) => m.id)).toEqual(['raw-model']);

    act(() => result.current.selectAllReady());
    await act(async () => { await result.current.save(); });

    expect(lastWrite().model_preference?.custom_models).toEqual([
      { name: 'go/kimi-k9', model_id: 'kimi-k9', provider: 'go-anthropic', parameters: { a: 1 }, display_name: 'Kimi K9' },
      { name: 'go/qwen-z', model_id: 'qwen-z', provider: 'go-chat', display_name: 'Qwen Z' },
    ]);
    expect(lastWrite().other_preference?.starred_models).toEqual(['go/kimi-k9', 'go/qwen-z']);
  });

  it("puts an untuned model it ticks on the user's own, where it stays unticked until removed", async () => {
    gateway();
    const { result } = renderHookWithProviders(() => useModelPicker({ provider: 'go' }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.yours).toEqual([]);
    const name = result.current.nameOf({ id: 'raw-model' });
    act(() => result.current.toggle(name));
    expect(result.current.yours).toEqual([name]);
    act(() => result.current.toggle(name));
    expect(result.current.yours).toEqual([name]);
    expect(result.current.selected.has(name)).toBe(false);
    act(() => result.current.removeYours(name));
    expect(result.current.yours).toEqual([]);
  });
});
