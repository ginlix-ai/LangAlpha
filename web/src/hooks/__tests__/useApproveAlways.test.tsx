/**
 * "Always allow" on an approval card saves the setting, then answers the card.
 * The answer goes out even when the save fails (the next card just asks
 * again), and `saving` follows the write so the card's buttons come back once
 * it settles, whether or not the answer reached anything.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('@/pages/Dashboard/utils/api', () => ({
  updatePreferences: vi.fn(),
}));

import { updatePreferences } from '@/pages/Dashboard/utils/api';
import { AUTO_APPROVE, useApproveAlways } from '../useAutoApprove';

const mockUpdate = updatePreferences as unknown as ReturnType<typeof vi.fn>;

function deferred() {
  let resolve!: (value: unknown) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

describe('useApproveAlways', () => {
  beforeEach(() => {
    mockUpdate.mockReset();
  });

  it('saves the setting, then answers, and is saving only while the write is out', async () => {
    const write = deferred();
    mockUpdate.mockReturnValue(write.promise);
    const then = vi.fn();
    const { result } = renderHookWithProviders(() => useApproveAlways(AUTO_APPROVE.handoffs));
    expect(result.current.saving).toBe(false);

    act(() => result.current.approveAlways(then));

    await waitFor(() => expect(result.current.saving).toBe(true));
    expect(mockUpdate.mock.calls[0][0]).toEqual({ other_preference: { auto_approve_handoffs: true } });
    expect(then).not.toHaveBeenCalled();

    await act(async () => write.resolve({}));

    await waitFor(() => expect(then).toHaveBeenCalledOnce());
    await waitFor(() => expect(result.current.saving).toBe(false));
  });

  it('still answers when the save fails, and comes back from saving', async () => {
    mockUpdate.mockRejectedValue(new Error('network down'));
    const then = vi.fn();
    const { result } = renderHookWithProviders(() => useApproveAlways(AUTO_APPROVE.workspaceCreation));

    act(() => result.current.approveAlways(then));

    await waitFor(() => expect(then).toHaveBeenCalledOnce());
    expect(mockUpdate.mock.calls[0][0]).toEqual({ other_preference: { auto_approve_workspace_creation: true } });
    await waitFor(() => expect(result.current.saving).toBe(false));
  });
});
