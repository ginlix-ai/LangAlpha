import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '../../../../../test/utils';

// The real preference hooks over a mocked API: a user with no preferences row
// reads as null, and what their first writes leave in the cache is decided by
// the mutation's own optimistic patch and rollback.
const getPreferences = vi.fn();
const updatePreferences = vi.fn();
vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/Dashboard/utils/api')>()),
  getPreferences: () => getPreferences(),
  updatePreferences: (prefs: unknown) => updatePreferences(prefs),
}));
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: vi.fn(), dismiss: vi.fn(), toasts: [] }),
}));

import { useDashboardPrefs } from '../useDashboardPrefs';
import { defaultLayout } from '../../presets';

function deferred() {
  let resolve: (value: unknown) => void = () => {};
  let reject: (err: unknown) => void = () => {};
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function renderForUserWithNoRow() {
  getPreferences.mockResolvedValueOnce(null);
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
  const view = renderHookWithProviders(() => useDashboardPrefs(), { queryClient });
  await waitFor(() => expect(view.result.current.editable).toBe(true));
  // The read after a write settles never answers, so what shows is what the
  // write left in the cache.
  getPreferences.mockImplementation(() => new Promise(() => {}));
  return { ...view, queryClient };
}

/** Waits until no write is saving and the page has seen it settle: TanStack
 *  tells its observers a tick after the mutation cache goes idle. */
async function settled(queryClient: QueryClient) {
  await waitFor(() => expect(queryClient.isMutating()).toBe(0));
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

describe('useDashboardPrefs for a user with no preferences row', () => {
  beforeEach(() => {
    getPreferences.mockReset();
    updatePreferences.mockReset();
  });

  it('keeps the first edit once its write lands', async () => {
    const write = deferred();
    updatePreferences.mockReturnValueOnce(write.promise);
    const { result, queryClient } = await renderForUserWithNoRow();
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    await act(async () => {
      write.resolve({});
    });
    await settled(queryClient);
    expect(result.current.prefs.lastBreakpoint).toBe('md');
  });

  it('takes the default back when the first write is rejected', async () => {
    const write = deferred();
    updatePreferences.mockReturnValueOnce(write.promise);
    const { result, queryClient } = await renderForUserWithNoRow();
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    await act(async () => {
      write.reject(new Error('rejected'));
    });
    await settled(queryClient);
    expect(result.current.prefs).toEqual(defaultLayout());
  });

  it('keeps a first write that landed when the one queued behind it is rejected', async () => {
    const first = deferred();
    const second = deferred();
    updatePreferences.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result, queryClient } = await renderForUserWithNoRow();
    act(() => {
      result.current.update({ lastBreakpoint: 'md' }, { immediate: true });
    });
    act(() => {
      result.current.update((prev) => ({ ...prev, layouts: { ...prev.layouts, md: [] } }), { immediate: true });
    });
    await act(async () => {
      first.resolve({});
    });
    await waitFor(() => expect(updatePreferences).toHaveBeenCalledTimes(2));
    await act(async () => {
      second.reject(new Error('rejected'));
    });
    await settled(queryClient);
    expect(result.current.prefs.lastBreakpoint).toBe('md');
    expect(result.current.prefs.layouts.md).not.toEqual([]);
  });
});
