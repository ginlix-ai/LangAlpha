import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { toast } from '@/components/ui/use-toast';
import i18n from '@/i18n';
import { queryKeys } from '@/lib/queryKeys';
import { renderHookWithProviders } from '@/test/utils';
import * as api from '../../utils/api';
import { useAutomationMutations } from '../useAutomationMutations';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  createAutomation: vi.fn(),
  pauseAutomation: vi.fn(),
  triggerAutomation: vi.fn(),
  skipRun: vi.fn(),
}));

afterEach(() => vi.clearAllMocks());

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<never>((res) => {
    resolve = () => res({} as never);
  });
  return { promise, resolve };
}

describe('useAutomationMutations', () => {
  it('holds busy until the last of two overlapping writes settles', async () => {
    const first = deferred();
    const second = deferred();
    vi.mocked(api.pauseAutomation).mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result, queryClient } = renderHookWithProviders(() => useAutomationMutations());

    act(() => {
      result.current.pause.mutate('auto-1');
      result.current.pause.mutate('auto-2');
    });
    await waitFor(() => expect(result.current.busy).toBe(true));

    // The later write settles first: the one a verb's own `isPending` tracks.
    await act(async () => second.resolve());
    await waitFor(() => expect(queryClient.isMutating()).toBe(1));
    expect(result.current.busy).toBe(true);

    await act(async () => first.resolve());
    await waitFor(() => expect(result.current.busy).toBe(false));
  });

  it('announces a started run and refetches every automation list', async () => {
    vi.mocked(api.triggerAutomation).mockResolvedValue({} as never);
    const { result, queryClient } = renderHookWithProviders(() => useAutomationMutations());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await act(async () => {
      await result.current.trigger.mutateAsync('auto-1');
    });

    expect(api.triggerAutomation).toHaveBeenCalledWith('auto-1');
    expect(toast).toHaveBeenCalledWith({ description: i18n.t('automation.runStarted') });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.automations.all });
  });

  it('treats a skip refused with 409 as nothing left to skip', async () => {
    vi.mocked(api.skipRun).mockRejectedValue({ response: { status: 409 } });
    const { result, queryClient } = renderHookWithProviders(() => useAutomationMutations());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await act(async () => {
      await expect(result.current.skip.mutateAsync({ automationId: 'auto-1', executionId: 'exec-1' })).resolves.toBeNull();
    });

    expect(api.skipRun).toHaveBeenCalledWith('auto-1', 'exec-1');
    expect(toast).not.toHaveBeenCalled();
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.automations.all });
  });

  it('toasts a refused write and still rejects for a caller that branches on it', async () => {
    const refusal = { response: { status: 422, data: { detail: 'Name is required' } } };
    vi.mocked(api.createAutomation).mockRejectedValue(refusal);
    const { result } = renderHookWithProviders(() => useAutomationMutations());

    await act(async () => {
      await expect(result.current.create.mutateAsync({} as never)).rejects.toBe(refusal);
    });

    expect(toast).toHaveBeenCalledWith({ variant: 'destructive', description: 'Name is required' });
  });
});
