import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { AccountTuningDefaults } from '../AccountTuningDefaults';
import { queryKeys } from '@/lib/queryKeys';

/**
 * Summarizing a long thread after a break is on until the user turns it off,
 * so only "off" is stored, and turning it back on deletes the key rather than
 * storing true.
 */

const server = vi.hoisted(() => {
  const state = { stored: {} as Record<string, unknown> };
  return {
    state,
    getPreferences: vi.fn(async () => ({ model_preference: state.stored })),
    updatePreferences: vi.fn(async (patch: { model_preference?: Record<string, unknown> }) => {
      const next = { ...state.stored };
      for (const [key, value] of Object.entries(patch.model_preference ?? {})) {
        if (value === null) delete next[key];
        else next[key] = value;
      }
      state.stored = next;
      return { model_preference: next };
    }),
  };
});

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: server.getPreferences,
  updatePreferences: server.updatePreferences,
}));

const LABEL = 'Summarize long threads after a break';

function renderDefaults() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  queryClient.setQueryData(queryKeys.user.preferences(), { model_preference: server.state.stored });
  return render(
    <QueryClientProvider client={queryClient}>
      <AccountTuningDefaults metadata={{}} validModelNames={new Set()} compactionProfiles={null} />
    </QueryClientProvider>,
  );
}

describe('AccountTuningDefaults: summarizing after a break', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    server.state.stored = {};
  });

  it('is on by default, stores off as false, and on again as a deleted key', async () => {
    renderDefaults();
    const toggle = screen.getByRole('switch', { name: LABEL });
    expect(toggle).toHaveAccessibleDescription(/costs less than processing all of it again/);
    expect(toggle).toHaveAttribute('aria-checked', 'true');

    fireEvent.click(toggle);
    await waitFor(() => expect(server.updatePreferences).toHaveBeenCalledTimes(1));
    expect(server.updatePreferences.mock.lastCall?.[0]).toEqual({ model_preference: { cold_compaction: false } });
    await waitFor(() => expect(toggle).toHaveAttribute('aria-checked', 'false'));

    fireEvent.click(toggle);
    await waitFor(() => expect(server.updatePreferences).toHaveBeenCalledTimes(2));
    expect(server.updatePreferences.mock.lastCall?.[0]).toEqual({ model_preference: { cold_compaction: null } });
    await waitFor(() => expect(toggle).toHaveAttribute('aria-checked', 'true'));
  });
});
