/**
 * The optimistic cache has to predict the row the server will store, because
 * every control that writes a preference reads it straight back out of that one
 * entry. The prediction that matters here is the one a clear triggers: the
 * server drops a cleared model key from its pre-move copy as well, and a
 * prediction that dropped only the new column would show the user the old value
 * coming back for as long as the request is in flight.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { modelPrefs } from '@/lib/modelPreferences';

vi.mock('@/pages/Dashboard/utils/api', () => ({
  updatePreferences: vi.fn(),
}));

import { updatePreferences } from '@/pages/Dashboard/utils/api';
import { useUpdatePreferences } from '../useUpdatePreferences';

const mockUpdate = updatePreferences as unknown as ReturnType<typeof vi.fn>;

/** A write that never answers, so what is asserted is the prediction alone. */
function inFlight() {
  mockUpdate.mockImplementation(() => new Promise(() => {}));
}

/** The shared test client garbage-collects at once, and nothing here mounts a
 *  reader of the preferences query, so the seeded row has to outlive its lack
 *  of observers for the rollback to have something to roll back to. */
function seeded(row: Record<string, unknown>) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity },
      mutations: { retry: false },
    },
  });
  queryClient.setQueryData(queryKeys.user.preferences(), row);
  return queryClient;
}

describe('useUpdatePreferences optimistic merge', () => {
  beforeEach(() => {
    mockUpdate.mockReset();
  });

  it('clears a moved key out of the pre-move column too', async () => {
    inFlight();
    const queryClient = seeded({
      other_preference: { preferred_model: 'old' },
      model_preference: { preferred_model: 'new' },
    });
    const { result } = renderHookWithProviders(() => useUpdatePreferences(), { queryClient });

    await act(async () => {
      result.current.mutate({ model_preference: { preferred_model: null } });
    });

    const row = queryClient.getQueryData(queryKeys.user.preferences());
    expect(modelPrefs(row).preferred_model).toBeUndefined();
  });

  it('leaves the pre-move column alone when the key is only being set', async () => {
    inFlight();
    const queryClient = seeded({
      other_preference: { preferred_model: 'old', starred_models: ['a'] },
      model_preference: {},
    });
    const { result } = renderHookWithProviders(() => useUpdatePreferences(), { queryClient });

    await act(async () => {
      result.current.mutate({ model_preference: { preferred_model: 'new' } });
    });

    const row = queryClient.getQueryData(queryKeys.user.preferences()) as Record<string, unknown>;
    expect(modelPrefs(row).preferred_model).toBe('new');
    // A key another surface owns is not collateral of a model write.
    expect((row.other_preference as Record<string, unknown>).starred_models).toEqual(['a']);
  });

  it('rolls the whole prediction back when the write fails', async () => {
    mockUpdate.mockImplementation(() => Promise.reject(new Error('nope')));
    const previous = {
      other_preference: { preferred_model: 'old' },
      model_preference: { preferred_model: 'new' },
    };
    const queryClient = seeded(previous);
    const { result } = renderHookWithProviders(() => useUpdatePreferences(), { queryClient });

    await act(async () => {
      await result.current
        .mutateAsync({ model_preference: { preferred_model: null } })
        .catch(() => {});
    });

    expect(queryClient.getQueryData(queryKeys.user.preferences())).toEqual(previous);
  });
});
