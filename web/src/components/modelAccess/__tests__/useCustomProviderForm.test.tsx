/**
 * A provider of the user's own is stored before its key, since the server
 * takes a key only for a provider it knows. When the key is refused, the
 * definition goes back to what it was, so its models are never listed with
 * nothing to run them on, and what another tab saved meanwhile stays.
 */
import { describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

const before = {
  custom_providers: [{ name: 'other-gw', parent_provider: 'openai' }],
  custom_models: [{ name: 'kept', model_id: 'kept', provider: 'other-gw' }],
};

const h = vi.hoisted(() => ({
  updatePreferences: vi.fn(async (_payload: unknown) => ({})),
  updateApiKeys: vi.fn(async (_payload: unknown): Promise<unknown> => {
    throw new Error('refused');
  }),
}));

// Stored by the time the key is refused: this save's provider, and another
// tab's, saved in between.
const stored = {
  custom_providers: [
    ...before.custom_providers,
    { name: 'my-gateway', parent_provider: 'openai' },
    { name: 'tab-gw', parent_provider: 'openai' },
  ],
  custom_models: [
    ...before.custom_models,
    { name: 'gpt-x', model_id: 'gpt-x', provider: 'my-gateway' },
    { name: 'tab-model', model_id: 'tab-model', provider: 'tab-gw' },
  ],
};

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: { model_preference: before } }),
}));
// The save reads before writing, then again before taking its provider back.
const reads = [{ model_preference: before }, { model_preference: stored }];
vi.mock('@/hooks/useStoredPreferences', () => ({
  useReadStoredPreferences: () => async () => reads.shift(),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: h.updatePreferences }) }));
vi.mock('@/hooks/useApiKeys', () => ({ useUpdateApiKeys: () => ({ mutateAsync: h.updateApiKeys }) }));
vi.mock('@/hooks/useModelAccessActions', () => ({ useRefreshAfterAccessChange: () => vi.fn() }));
vi.mock('@/hooks/useProviderModels', () => ({
  useEndpointModels: () => ({
    data: { status: 'ok', recognized: [{ name: 'gpt-x', model_id: 'gpt-x' }], others: [] },
    settled: true,
    isFetching: false,
    isError: false,
    error: null,
  }),
}));

import { useCustomProviderForm } from '../useCustomProviderForm';

describe('useCustomProviderForm', () => {
  it('takes the provider back out when its key is refused', async () => {
    const { result } = renderHookWithProviders(() => useCustomProviderForm());
    act(() => {
      result.current.setName('My Gateway');
      result.current.setBaseUrl('https://gateway.example/v1');
      result.current.setApiKey('sk-test');
    });
    let saved: string | null = 'unset';
    await act(async () => { saved = await result.current.save(); });

    expect(saved).toBeNull();
    expect(h.updatePreferences).toHaveBeenCalledTimes(2);
    expect(h.updatePreferences).toHaveBeenLastCalledWith({ model_preference: {
      custom_providers: [before.custom_providers[0], stored.custom_providers[2]],
      custom_models: [before.custom_models[0], stored.custom_models[2]],
    } });
  });
});
