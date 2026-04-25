import { describe, it, expect, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: vi.fn(),
}));

import { getPreferences } from '@/pages/Dashboard/utils/api';
import { usePreferences } from '../usePreferences';

const mockGetPreferences = getPreferences as unknown as ReturnType<typeof vi.fn>;

describe('usePreferences', () => {
  beforeEach(() => {
    mockGetPreferences.mockReset();
  });

  it('fetches preferences via getPreferences()', async () => {
    mockGetPreferences.mockResolvedValue({ theme: 'dark', other_preference: { dashboard: { mode: 'classic' } } });
    const { result } = renderHookWithProviders(() => usePreferences());
    await waitFor(() => expect(result.current.preferences).not.toBeNull());
    expect(result.current.preferences).toMatchObject({ theme: 'dark' });
  });

  it('uses staleTime: 0 so a returning tab refetches immediately', async () => {
    // staleTime: 0 means a query is *always* stale on subscribe — combined
    // with the global refetchOnWindowFocus: true (main.tsx), an alt-tab user
    // gets fresh prefs without polling.
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity } },
    });
    mockGetPreferences.mockResolvedValue({ theme: 'light' });
    const { result, unmount } = renderHookWithProviders(() => usePreferences(), { queryClient });
    await waitFor(() => expect(result.current.preferences).not.toBeNull());
    expect(mockGetPreferences).toHaveBeenCalledTimes(1);
    unmount();
    // Re-mount immediately. With staleTime: 0, the cache is stale and the
    // next observer triggers a refetch instead of returning the cached value.
    mockGetPreferences.mockResolvedValue({ theme: 'dark' });
    const { result: r2 } = renderHookWithProviders(() => usePreferences(), { queryClient });
    await waitFor(() => expect(mockGetPreferences).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(r2.current.preferences).toMatchObject({ theme: 'dark' }));
  });
});
