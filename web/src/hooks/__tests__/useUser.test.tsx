import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHookWithProviders } from '../../test/utils';
import { useUser } from '../useUser';
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
