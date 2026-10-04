/**
 * `useAllModels().isLoading` gates the composer's model picker, so it has to
 * cover every input to "which models can this account reach". The platform
 * access answer is one of them: while it is pending, a locked model reads as
 * reachable and a pick would save it as the account default.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';

const h = vi.hoisted(() => ({
  platformLoading: false,
  prefsLoaded: true,
  preferences: {} as Record<string, unknown>,
}));

vi.mock('../useModels', () => ({
  useModels: () => ({
    models: {
      models: { 'prov-x': { models: ['model-x'], display_name: 'X' } },
      model_metadata: { 'model-x': { provider: 'prov-x' } },
    },
    isLoading: false,
  }),
}));
vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false, isLoaded: h.prefsLoaded }),
}));
vi.mock('../useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], isLoading: false }),
}));
vi.mock('../usePlatformModels', () => ({
  usePlatformModels: () => ({ platform: null, isLoading: h.platformLoading }),
  useModelAccessMap: () => ({}),
}));

import { useAllModels } from '../useAllModels';

describe('useAllModels — isLoading', () => {
  beforeEach(() => { h.platformLoading = false; h.prefsLoaded = true; });

  it('stays loading while only the platform access answer is pending', () => {
    h.platformLoading = true;
    const { result } = renderHook(() => useAllModels());
    expect(result.current.isLoading).toBe(true);
  });

  it('settles once every input has answered', () => {
    const { result } = renderHook(() => useAllModels());
    expect(result.current.isLoading).toBe(false);
  });
});

describe('useAllModels — catalogModelNames', () => {
  beforeEach(() => { h.platformLoading = false; h.prefsLoaded = true; h.preferences = {}; });

  it('carries a custom provider\'s own name, which the server runs as a model', () => {
    h.preferences = {
      model_preference: { custom_providers: [{ name: 'my-gateway', parent_provider: 'prov-x' }] },
    };
    const { result } = renderHook(() => useAllModels());
    expect(result.current.catalogModelNames.has('my-gateway')).toBe(true);
  });

  it('carries a model the user cannot reach, since only the catalog decides it is gone', () => {
    // No provider is configured, so the model is out of reach but still exists.
    const { result } = renderHook(() => useAllModels());
    expect(result.current.validModelNames.has('model-x')).toBe(false);
    expect(result.current.catalogModelNames.has('model-x')).toBe(true);
  });

  it('stays empty until the custom models are read, so no name reads as gone early', () => {
    h.prefsLoaded = false;
    const { result } = renderHook(() => useAllModels());
    expect(result.current.catalogModelNames.size).toBe(0);
  });
});
