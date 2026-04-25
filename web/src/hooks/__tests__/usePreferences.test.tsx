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

  it('uses 60s staleTime when BroadcastChannel is available (modern browsers)', async () => {
    // Vitest's Node runtime exposes BroadcastChannel, so this is the default
    // branch under test. Immediate remount stays cached; no extra GET.
    expect(typeof BroadcastChannel).toBe('function');
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity } },
    });
    mockGetPreferences.mockResolvedValue({ theme: 'light' });
    const { result, unmount } = renderHookWithProviders(() => usePreferences(), { queryClient });
    await waitFor(() => expect(result.current.preferences).not.toBeNull());
    expect(mockGetPreferences).toHaveBeenCalledTimes(1);
    unmount();
    // Immediate remount: cache is still fresh (well within 60s), so no refetch.
    mockGetPreferences.mockResolvedValue({ theme: 'dark' });
    const { result: r2 } = renderHookWithProviders(() => usePreferences(), { queryClient });
    await waitFor(() => expect(r2.current.preferences).toMatchObject({ theme: 'light' }));
    expect(mockGetPreferences).toHaveBeenCalledTimes(1);
  });

  it('falls back to staleTime: 0 when BroadcastChannel is unavailable (Safari < 15.4)', async () => {
    // Delete the global and re-import so the module-level const re-evaluates.
    vi.resetModules();
    const original = globalThis.BroadcastChannel;
    delete (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
    try {
      const apiMod = await import('@/pages/Dashboard/utils/api');
      const localGet = apiMod.getPreferences as unknown as ReturnType<typeof vi.fn>;
      vi.mocked(localGet).mockReset();
      const { usePreferences: freshUsePrefs } = await import('../usePreferences');
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, gcTime: Infinity } },
      });
      vi.mocked(localGet).mockResolvedValue({ theme: 'light' });
      const { result, unmount } = renderHookWithProviders(() => freshUsePrefs(), { queryClient });
      await waitFor(() => expect(result.current.preferences).not.toBeNull());
      expect(localGet).toHaveBeenCalledTimes(1);
      unmount();
      // Immediate remount under staleTime: 0 → cache is stale → refetch.
      vi.mocked(localGet).mockResolvedValue({ theme: 'dark' });
      const { result: r2 } = renderHookWithProviders(() => freshUsePrefs(), { queryClient });
      await waitFor(() => expect(localGet).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(r2.current.preferences).toMatchObject({ theme: 'dark' }));
    } finally {
      globalThis.BroadcastChannel = original;
    }
  });
});
