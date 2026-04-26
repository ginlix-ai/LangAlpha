import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '../../test/utils';
import { usePlatformModels, getModelAccess, useModelAccessMap } from '../usePlatformModels';
import { customPairKey, type ModelMetadataEntry } from '../useFilteredModels';
import type { ProviderModelsData } from '@/components/model/types';
import type { PlatformModelsResponse } from '@/types/platform';

// ---------------------------------------------------------------------------
// Mock the shared API client + host mode
// ---------------------------------------------------------------------------

vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn(),
  },
}));

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
}));

import { api } from '@/api/client';
import type { Mock } from 'vitest';

const mockGet = api.get as Mock;

// ---------------------------------------------------------------------------
// usePlatformModels hook tests
// ---------------------------------------------------------------------------

describe('usePlatformModels', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns platform data on successful fetch', async () => {
    const payload: PlatformModelsResponse = {
      model_tier: 1,
      byok_providers: ['openai'],
      oauth_providers: ['anthropic'],
    };
    mockGet.mockResolvedValue({ data: payload });

    const { result } = renderHookWithProviders(() => usePlatformModels());

    await waitFor(() => {
      expect(result.current).toEqual(payload);
    });

    expect(mockGet).toHaveBeenCalledWith('/api/auth/models');
  });

  it('returns null on network error (fail-open)', async () => {
    mockGet.mockRejectedValue(new Error('Network Error'));

    const { result } = renderHookWithProviders(() => usePlatformModels());

    // Should stay null — the hook never throws
    await waitFor(() => {
      // Give React Query time to process the error
      expect(result.current).toBeNull();
    });
  });

  it('returns null on 401 (fail-open)', async () => {
    const error = Object.assign(new Error('Unauthorized'), {
      response: { status: 401, data: { detail: 'Not authenticated' } },
    });
    mockGet.mockRejectedValue(error);

    const { result } = renderHookWithProviders(() => usePlatformModels());

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });
});

// ---------------------------------------------------------------------------
// getModelAccess pure function tests
// ---------------------------------------------------------------------------

describe('getModelAccess', () => {
  const basePlatform: PlatformModelsResponse = {
    model_tier: 1,
    byok_providers: ['openai'],
    oauth_providers: ['anthropic'],
  };

  it('returns "byok" when provider is in byok_providers (takes priority)', () => {
    expect(getModelAccess(2, 'openai', basePlatform)).toBe('byok');
  });

  it('returns "oauth" when provider is in oauth_providers', () => {
    expect(getModelAccess(2, 'anthropic', basePlatform)).toBe('oauth');
  });

  it('returns "platform" when model tier <= user tier', () => {
    // model tier 0 <= user tier 1
    expect(getModelAccess(0, 'google', basePlatform)).toBe('platform');
    // model tier 1 <= user tier 1
    expect(getModelAccess(1, 'google', basePlatform)).toBe('platform');
  });

  it('returns "locked" when model tier > user tier', () => {
    // model tier 2 > user tier 1
    expect(getModelAccess(2, 'google', basePlatform)).toBe('locked');
  });

  it('returns "platform" when platform is null (no filtering)', () => {
    expect(getModelAccess(0, 'openai', null)).toBe('platform');
    expect(getModelAccess(1, 'anthropic', null)).toBe('platform');
    expect(getModelAccess(2, 'google', null)).toBe('platform');
    expect(getModelAccess(99, 'whatever', null)).toBe('platform');
  });

  it('prioritizes BYOK over OAuth when provider is in both lists', () => {
    const platform: PlatformModelsResponse = {
      model_tier: 0,
      byok_providers: ['anthropic'],
      oauth_providers: ['anthropic'],
    };
    expect(getModelAccess(2, 'anthropic', platform)).toBe('byok');
  });

  it('prioritizes BYOK over plan tier', () => {
    // Even though model tier 0 <= user tier 1, BYOK takes priority
    expect(getModelAccess(0, 'openai', basePlatform)).toBe('byok');
  });
});

// ---------------------------------------------------------------------------
// useModelAccessMap — custom-pair badge resolution
// ---------------------------------------------------------------------------

describe('useModelAccessMap', () => {
  it('badges a custom-pair row against groupKey ("byok") instead of the built-in metadata provider', () => {
    // User has tier 1 (granting platform access to the built-in) AND a
    // configured BYOK variant with a custom_models entry shadowing the
    // built-in name. The runtime resolver routes through the variant
    // regardless, so the badge must reflect "byok", not "platform".
    const models: Record<string, ProviderModelsData> = {
      'byok-variant': { models: ['model-shared'], display_name: 'Variant' },
    };
    const metadata: Record<string, ModelMetadataEntry> = {
      // Built-in metadata still points at the parent provider — the merge
      // step in buildVisibleModels intentionally does not overwrite it.
      'model-shared': { provider: 'parent-prov', tier: 1 },
    };
    const platform: PlatformModelsResponse = {
      model_tier: 1,
      byok_providers: ['byok-variant'],
      oauth_providers: [],
    };
    const customPairs = new Set([customPairKey('byok-variant', 'model-shared')]);

    const { result } = renderHook(() =>
      useModelAccessMap(models, metadata, platform, customPairs),
    );

    expect(result.current).toEqual({ 'model-shared': 'byok' });
  });

  it('falls back to built-in metadata provider when the row is NOT a custom-pair', () => {
    // OAuth-served models live under the parent's group but their
    // meta.provider is the OAuth slug — the meta lookup must win when
    // there's no custom-pair shadow.
    const models: Record<string, ProviderModelsData> = {
      'parent-prov': { models: ['oauth-model'] },
    };
    const metadata: Record<string, ModelMetadataEntry> = {
      'oauth-model': { provider: 'oauth-prov' },
    };
    const platform: PlatformModelsResponse = {
      model_tier: 0,
      byok_providers: [],
      oauth_providers: ['oauth-prov'],
    };

    const { result } = renderHook(() =>
      useModelAccessMap(models, metadata, platform, new Set()),
    );

    expect(result.current).toEqual({ 'oauth-model': 'oauth' });
  });
});
