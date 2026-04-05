import { describe, it, expect, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '../../test/utils';
import { usePlatformModels, getModelAccess } from '../usePlatformModels';
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
