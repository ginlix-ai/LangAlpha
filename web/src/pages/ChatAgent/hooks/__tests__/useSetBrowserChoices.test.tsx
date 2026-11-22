/**
 * A browser save shows at once on that computer's `browser`, and puts back
 * only what it changed, with a toast saying why, when the server refuses it.
 * A save of sign-ins adds its sign-out toast.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { act, waitFor } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { toast } from '@/components/ui/use-toast';
import type { ComputerBrowser } from '@/types/api';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  setComputerBrowser: vi.fn(),
}));

import { setComputerBrowser } from '../../utils/api';
import { useSetBrowserChoices } from '../useSetBrowserChoices';

const ID = 'c-1';
const OTHER = 'c-2';
const LISTS = queryKeys.computers.lists();
const BROWSER: ComputerBrowser = { quality: null, signins: true, window: false, window_available: true };

function seeded(browser: Partial<ComputerBrowser> = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } } });
  client.setQueryData(LISTS, {
    computers: [
      { computer_id: ID, name: 'Mac', status: 'running', browser: { ...BROWSER, ...browser } },
      { computer_id: OTHER, name: 'Box', status: 'running', browser: BROWSER },
    ],
    total: 2,
  });
  return client;
}

const browserOf = (client: QueryClient, id = ID) =>
  client.getQueryData<{ computers: { computer_id: string; browser: ComputerBrowser }[] }>(LISTS)?.computers
    .find((c) => c.computer_id === id)?.browser;

/** Each call to the server waits until the test answers it. */
function held() {
  const calls: { resolve: (value: unknown) => void; reject: (err: Error) => void }[] = [];
  (setComputerBrowser as Mock).mockImplementation(
    () => new Promise((resolve, reject) => { calls.push({ resolve, reject }); }),
  );
  return calls;
}

describe('saving a computer\'s browser choices', () => {
  beforeEach(() => vi.clearAllMocks());

  it('shows the choice at once and puts it back when refused', async () => {
    const calls = held();
    const client = seeded();
    const { result } = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client });
    act(() => result.current.mutate({ computerId: ID, patch: { window: true } }));
    await waitFor(() => expect(browserOf(client)?.window).toBe(true));
    expect(setComputerBrowser).toHaveBeenCalledWith(ID, { window: true });
    expect(result.current.busyFor(ID)).toBe(true);
    expect(result.current.busyFor(OTHER)).toBe(false);
    act(() => calls[0].reject(new Error('no')));
    await waitFor(() => expect(browserOf(client)).toEqual(BROWSER));
    expect(toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive', title: "Couldn't change the browser window" }));
    expect(result.current.busyFor(ID)).toBe(false);
  });

  it('puts back only its own keys when refused, leaving a save that landed meanwhile and other computers', async () => {
    const calls = held();
    const client = seeded();
    const windowSave = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client }).result;
    const qualitySave = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client }).result;
    const otherSave = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client }).result;
    act(() => windowSave.current.mutate({ computerId: ID, patch: { window: true } }));
    act(() => qualitySave.current.mutate({ computerId: ID, patch: { quality: 'sharp' } }));
    act(() => otherSave.current.mutate({ computerId: OTHER, patch: { signins: false } }));
    await waitFor(() => expect(calls).toHaveLength(3));
    expect(browserOf(client)).toEqual({ ...BROWSER, window: true, quality: 'sharp' });

    // The quality lands; its answer, read before the window's save, still says no window.
    act(() => calls[1].resolve({ browser: { ...BROWSER, quality: 'sharp' }, applied: true }));
    act(() => calls[2].resolve({ browser: { ...BROWSER, signins: false }, applied: true }));
    await waitFor(() => expect(qualitySave.current.busyFor(ID)).toBe(false));
    expect(browserOf(client)).toEqual({ ...BROWSER, window: true, quality: 'sharp' });

    act(() => calls[0].reject(new Error('no')));
    await waitFor(() => expect(browserOf(client)?.window).toBe(false));
    expect(browserOf(client)).toEqual({ ...BROWSER, quality: 'sharp' });
    expect(browserOf(client, OTHER)).toEqual({ ...BROWSER, signins: false });
  });

  it('leaves a later save of the same key standing when an earlier one is refused', async () => {
    const calls = held();
    const client = seeded();
    const first = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client }).result;
    const second = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client }).result;
    act(() => first.current.mutate({ computerId: ID, patch: { quality: 'light' } }));
    act(() => second.current.mutate({ computerId: ID, patch: { quality: 'retina' } }));
    await waitFor(() => expect(calls).toHaveLength(2));
    act(() => calls[0].reject(new Error('no')));
    await waitFor(() => expect(first.current.busyFor(ID)).toBe(false));
    expect(browserOf(client)?.quality).toBe('retina');
  });

  it('leaves the last save standing when an earlier save of the same value is refused after it', async () => {
    const calls = held();
    const client = seeded();
    const { result } = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client });
    act(() => result.current.mutate({ computerId: ID, patch: { quality: 'light' } }));
    act(() => result.current.mutate({ computerId: ID, patch: { quality: 'retina' } }));
    act(() => result.current.mutate({ computerId: ID, patch: { quality: 'light' } }));
    await waitFor(() => expect(calls).toHaveLength(3));
    act(() => calls[2].resolve({ browser: { ...BROWSER, quality: 'light' }, applied: true }));
    await waitFor(() => expect(browserOf(client)?.quality).toBe('light'));
    act(() => calls[0].reject(new Error('no')));
    await waitFor(() => expect(toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive' })));
    expect(browserOf(client)?.quality).toBe('light');
    // A stale answer from the middle save does not overwrite it either.
    act(() => calls[1].resolve({ browser: { ...BROWSER, quality: 'retina' }, applied: true }));
    await waitFor(() => expect(result.current.busyFor(ID)).toBe(false));
    expect(browserOf(client)?.quality).toBe('light');
  });

  it('says it signed out when sign-ins were saved on while already kept', async () => {
    (setComputerBrowser as Mock).mockResolvedValue({ browser: BROWSER, applied: true });
    const client = seeded();
    const { result } = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client });
    act(() => result.current.mutate({ computerId: ID, patch: { signins: true } }));
    await waitFor(() => expect(toast).toHaveBeenCalledWith({ title: 'Signed out of every site' }));
  });

  it('says the sign-out waits when the running machine did not take it', async () => {
    (setComputerBrowser as Mock).mockResolvedValue({ browser: BROWSER, applied: false });
    const client = seeded();
    const { result } = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client });
    act(() => result.current.mutate({ computerId: ID, patch: { signins: true } }));
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(expect.objectContaining({ title: "Saved, but this computer didn't answer" })),
    );
    expect(toast).not.toHaveBeenCalledWith({ title: 'Signed out of every site' });
  });

  it('says nothing more when sign-ins are turned back on', async () => {
    (setComputerBrowser as Mock).mockResolvedValue({ browser: BROWSER, applied: true });
    const client = seeded({ signins: false });
    const { result } = renderHookWithProviders(() => useSetBrowserChoices(), { queryClient: client });
    act(() => result.current.mutate({ computerId: ID, patch: { signins: true } }));
    await waitFor(() => expect(setComputerBrowser).toHaveBeenCalled());
    await waitFor(() => expect(result.current.busyFor(ID)).toBe(false));
    expect(toast).not.toHaveBeenCalled();
    expect(browserOf(client)?.signins).toBe(true);
  });
});
