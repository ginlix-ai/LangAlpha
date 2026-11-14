/**
 * Back from the plan page, the one quota read that follows asks the server to
 * read the plan again; every other read answers from what it holds.
 */
import { describe, expect, it, vi } from 'vitest';
import { act, waitFor } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, xlarge: null, always_on: null })),
}));

import { getWorkspaceQuota } from '../../utils/api';
import { readQuotaAfterPlanChange, useTierQuota } from '../useTierQuota';

describe('useTierQuota, back from the plan page', () => {
  it('asks for the plan again on the next read only', async () => {
    const { result, queryClient } = renderHookWithProviders(() => useTierQuota({ enabled: true }));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    await act(() => readQuotaAfterPlanChange(queryClient));
    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() }));
    expect(vi.mocked(getWorkspaceQuota).mock.calls).toEqual([[false], [true], [false]]);
  });

  it('asks again on the read after one that failed', async () => {
    vi.mocked(getWorkspaceQuota).mockClear();
    const { result, queryClient } = renderHookWithProviders(() => useTierQuota({ enabled: true }));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    vi.mocked(getWorkspaceQuota).mockRejectedValueOnce(new Error('timeout'));
    await act(() => readQuotaAfterPlanChange(queryClient));
    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() }));
    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() }));
    expect(vi.mocked(getWorkspaceQuota).mock.calls).toEqual([[false], [true], [true], [false]]);
  });
});
