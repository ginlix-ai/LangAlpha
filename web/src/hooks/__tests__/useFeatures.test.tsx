import { describe, it, expect, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import type { FeatureState } from '@/types/api';

vi.mock('@/api/features', () => ({
  getFeatures: vi.fn(),
  setFeatureOverride: vi.fn(),
}));

import { getFeatures, setFeatureOverride } from '@/api/features';
import { useFeatures, useFeatureEnabled, useSetFeatureOverride } from '../useFeatures';

const mockGetFeatures = getFeatures as unknown as ReturnType<typeof vi.fn>;
const mockSetFeatureOverride = setFeatureOverride as unknown as ReturnType<typeof vi.fn>;

function feature(overrides: Partial<FeatureState> & { key: string }): FeatureState {
  return {
    label: overrides.key,
    description: '',
    tradeoffs: null,
    enabled: false,
    gate: 'opt_out',
    min_tier: null,
    user_override: null,
    ...overrides,
  };
}

const FEATURES: FeatureState[] = [
  feature({ key: 'market_watch', label: 'Market watch', enabled: true }),
  feature({ key: 'beta_thing', label: 'Beta thing', enabled: false }),
];

function freshQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
}

describe('useFeatures', () => {
  beforeEach(() => {
    mockGetFeatures.mockReset();
    mockSetFeatureOverride.mockReset();
  });

  it('returns the feature list via getFeatures()', async () => {
    mockGetFeatures.mockResolvedValue(FEATURES);
    const { result } = renderHookWithProviders(() => useFeatures());
    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data).toEqual(FEATURES);
  });
});

describe('useFeatureEnabled', () => {
  beforeEach(() => {
    mockGetFeatures.mockReset();
  });

  it('returns true for an enabled flag', async () => {
    mockGetFeatures.mockResolvedValue(FEATURES);
    const { result } = renderHookWithProviders(() => useFeatureEnabled('market_watch'));
    await waitFor(() => expect(result.current).toBe(true));
  });

  it('returns false for a disabled flag', async () => {
    mockGetFeatures.mockResolvedValue(FEATURES);
    const { result } = renderHookWithProviders(() => useFeatureEnabled('beta_thing'));
    // Wait for the query to settle, then confirm the disabled flag reads false.
    const features = renderHookWithProviders(() => useFeatures());
    await waitFor(() => expect(features.result.current.data).toBeDefined());
    expect(result.current).toBe(false);
  });

  it('returns false while loading (fails closed)', () => {
    // Never-resolving fetch keeps the query pending; the gate must read false.
    mockGetFeatures.mockReturnValue(new Promise<FeatureState[]>(() => {}));
    const { result } = renderHookWithProviders(() => useFeatureEnabled('market_watch'));
    expect(result.current).toBe(false);
  });

  it('returns false for an unknown flag', async () => {
    mockGetFeatures.mockResolvedValue(FEATURES);
    const { result } = renderHookWithProviders(() => useFeatureEnabled('does_not_exist'));
    const features = renderHookWithProviders(() => useFeatures());
    await waitFor(() => expect(features.result.current.data).toBeDefined());
    expect(result.current).toBe(false);
  });
});

describe('useSetFeatureOverride', () => {
  beforeEach(() => {
    mockGetFeatures.mockReset();
    mockSetFeatureOverride.mockReset();
  });

  it('updates the features cache from the mutation response', async () => {
    const queryClient = freshQueryClient();
    mockGetFeatures.mockResolvedValue(FEATURES);
    const updated: FeatureState[] = [
      feature({ key: 'market_watch', label: 'Market watch', enabled: false, user_override: false }),
      FEATURES[1],
    ];
    mockSetFeatureOverride.mockResolvedValue(updated);

    const { result } = renderHookWithProviders(
      () => ({ features: useFeatures(), setOverride: useSetFeatureOverride() }),
      { queryClient },
    );
    await waitFor(() => expect(result.current.features.data).toEqual(FEATURES));

    await result.current.setOverride.mutateAsync({ key: 'market_watch', enabled: false });

    expect(mockSetFeatureOverride).toHaveBeenCalledWith('market_watch', false);
    // setQueryData in onSuccess propagates the returned list without a refetch.
    await waitFor(() => expect(result.current.features.data).toEqual(updated));
    expect(mockGetFeatures).toHaveBeenCalledTimes(1);
  });
});
