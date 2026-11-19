import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHookWithProviders } from '../../test/utils';
import { readUserAfterPlanChange, useUser } from '../useUser';
import { createTestQueryClient } from '../../test/utils';
import { queryKeys } from '../../lib/queryKeys';
import { waitFor } from '@testing-library/react';

vi.mock('../../pages/Dashboard/utils/api', () => ({
  getCurrentUser: vi.fn(),
}));

import { getCurrentUser } from '../../pages/Dashboard/utils/api';

const mockGetCurrentUser = getCurrentUser as Mock;

describe('useUser', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns the user from the API response (unwraps .user field)', async () => {
    const mockUser = { id: 'u-1', name: 'Alice', email: 'alice@test.com' };
    mockGetCurrentUser.mockResolvedValue({ user: mockUser });

    const { result } = renderHookWithProviders(() => useUser());

    await waitFor(() => expect(result.current.user).toEqual(mockUser));
  });

  it('falls back to raw response when .user field is absent', async () => {
    const raw = { id: 'u-2', name: 'Bob' };
    mockGetCurrentUser.mockResolvedValue(raw);

    const { result } = renderHookWithProviders(() => useUser());

    await waitFor(() => expect(result.current.user).toEqual(raw));
  });

  it('reads the account past the cached plan after a plan change, then has every reader read it again', async () => {
    mockGetCurrentUser.mockResolvedValue({});
    const queryClient = createTestQueryClient();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    await readUserAfterPlanChange(queryClient);
    expect(mockGetCurrentUser).toHaveBeenCalledWith({ refresh_tier: true });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.user.me() });
  });

  it('leaves the account as it was where that read fails', async () => {
    mockGetCurrentUser.mockRejectedValue(new Error('down'));
    const queryClient = createTestQueryClient();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    await expect(readUserAfterPlanChange(queryClient)).resolves.toBeUndefined();
    expect(invalidate).not.toHaveBeenCalled();
  });

  it('returns null as user while loading', () => {
    mockGetCurrentUser.mockReturnValue(new Promise(() => {})); // never resolves

    const { result } = renderHookWithProviders(() => useUser());

    expect(result.current.user).toBeNull();
    expect(result.current.isLoading).toBe(true);
  });

  it('does not retry a failure other than a 503', async () => {
    mockGetCurrentUser.mockRejectedValue(new Error('Unauthorized'));

    const { result } = renderHookWithProviders(() => useUser());

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(mockGetCurrentUser).toHaveBeenCalledTimes(1);
    expect(result.current.user).toBeNull();
  });

  it('asks again after Retry-After when the plan cannot be read (503)', async () => {
    // The server's answer while the platform is down and no plan is on record:
    // not "no plan", so the user must stay loading rather than reach setup.
    const unavailable = { response: { status: 503, headers: { 'retry-after': '1' } } };
    const mockUser = { id: 'u-3', access_tier: 2 };
    mockGetCurrentUser.mockRejectedValueOnce(unavailable).mockResolvedValue({ user: mockUser });

    const { result } = renderHookWithProviders(() => useUser());

    await waitFor(() => expect(mockGetCurrentUser).toHaveBeenCalledTimes(1));
    expect(result.current.isLoading).toBe(true);
    expect(result.current.user).toBeNull();
    await waitFor(() => expect(result.current.user).toEqual(mockUser), { timeout: 3000 });
    expect(mockGetCurrentUser).toHaveBeenCalledTimes(2);
  });
});
