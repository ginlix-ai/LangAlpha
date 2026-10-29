import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHookWithProviders } from '../../test/utils';
import { useWorkspaces } from '../useWorkspaces';
import { useAllWorkspaces } from '../useAllWorkspaces';
import { waitFor } from '@testing-library/react';

vi.mock('../../pages/ChatAgent/utils/api', () => ({
  getWorkspaces: vi.fn(),
}));

import { getWorkspaces } from '../../pages/ChatAgent/utils/api';

const mockGetWorkspaces = getWorkspaces as Mock;

describe('useWorkspaces', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches workspaces with default parameters', async () => {
    const mockData = { workspaces: [{ workspace_id: 'ws-1' }], total: 1 };
    mockGetWorkspaces.mockResolvedValue(mockData);

    const { result } = renderHookWithProviders(() => useWorkspaces());

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(mockData);
    expect(mockGetWorkspaces).toHaveBeenCalledWith(20, 0, 'custom', false);
  });

  it('passes custom limit, offset, and sortBy', async () => {
    mockGetWorkspaces.mockResolvedValue({ workspaces: [], total: 0 });

    const { result } = renderHookWithProviders(
      () => useWorkspaces({ limit: 10, offset: 5, sortBy: 'name' })
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetWorkspaces).toHaveBeenCalledWith(10, 5, 'name', false);
  });

  it('asks for Home under its own key when includeHome is set', async () => {
    mockGetWorkspaces.mockResolvedValue({ workspaces: [], total: 0 });

    const { result, queryClient } = renderHookWithProviders(
      () => useWorkspaces({ includeHome: true })
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetWorkspaces).toHaveBeenCalledWith(20, 0, 'custom', true);
    expect(queryClient.getQueryData([
      'workspaces', 'list', { limit: 20, offset: 0, sortBy: 'custom', includeHome: true },
    ])).toEqual({ workspaces: [], total: 0 });
  });

  it('does not fetch when enabled is false', () => {
    const { result } = renderHookWithProviders(
      () => useWorkspaces({ enabled: false })
    );

    expect(result.current.isFetching).toBe(false);
    expect(mockGetWorkspaces).not.toHaveBeenCalled();
  });

  it('handles API errors gracefully', async () => {
    mockGetWorkspaces.mockRejectedValue(new Error('Network error'));

    const { result } = renderHookWithProviders(() => useWorkspaces());

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toBeInstanceOf(Error);
  });

  it('uses the correct query key structure', async () => {
    mockGetWorkspaces.mockResolvedValue({ workspaces: [], total: 0 });

    const { result, queryClient } = renderHookWithProviders(
      () => useWorkspaces({ limit: 5, offset: 0, sortBy: 'custom' })
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    // The query key should match queryKeys.workspaces.list(params)
    const cachedData = queryClient.getQueryData([
      'workspaces', 'list', { limit: 5, offset: 0, sortBy: 'custom', includeHome: false },
    ]);
    expect(cachedData).toEqual({ workspaces: [], total: 0 });
  });
});

describe('useAllWorkspaces', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('reads past the first page', async () => {
    // A reach counted against the first 100 alone misses a switch in the rest.
    const rows = Array.from({ length: 105 }, (_, i) => ({ workspace_id: `ws-${i}` }));
    mockGetWorkspaces.mockImplementation(async (limit: number, offset: number) => ({
      workspaces: rows.slice(offset, offset + limit),
      total: rows.length,
    }));

    const { result } = renderHookWithProviders(() => useAllWorkspaces());

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.workspaces).toHaveLength(105);
    expect(mockGetWorkspaces).toHaveBeenNthCalledWith(2, 100, 100, 'custom', false);
  });

  it('keeps the first copy of a row that moved up between pages', async () => {
    // ws-150 is used between the two reads and jumps to the top, so the
    // second page, read at offset 100, starts one row later and repeats it.
    const before = Array.from({ length: 205 }, (_, i) => ({ workspace_id: `ws-${i}`, name: `before-${i}` }));
    const after = [before[150], ...before.filter((_, i) => i !== 150)].map((ws) => ({ ...ws, name: 'after' }));
    mockGetWorkspaces.mockImplementation(async (limit: number, offset: number) => ({
      workspaces: (offset === 0 ? before : after).slice(offset, offset + limit),
      total: 205,
    }));

    const { result } = renderHookWithProviders(() => useAllWorkspaces({ sortBy: 'activity' }));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const ids = result.current.data!.workspaces.map((ws) => ws.workspace_id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toHaveLength(204);
    expect(result.current.data!.workspaces.find((ws) => ws.workspace_id === 'ws-99')?.name).toBe('before-99');
    // Paged by what the server sent, not by what was kept.
    expect(mockGetWorkspaces).toHaveBeenNthCalledWith(3, 100, 200, 'activity', false);
  });
});
