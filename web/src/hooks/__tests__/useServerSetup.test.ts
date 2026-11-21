/** Warming Home ends the setup step, so it waits for an answer, but never for long. */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClient } from '@tanstack/react-query';

const { get } = vi.hoisted(() => ({ get: vi.fn() }));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss', APP_ENTRY_PATH: '/' }));
vi.mock('@/api/client', () => ({ api: { get } }));

import { queryKeys } from '@/lib/queryKeys';
import { readSetupStatusForWarm } from '../useServerSetup';

const client = () => new QueryClient({ defaultOptions: { queries: { retry: false } } });

beforeEach(() => {
  vi.useFakeTimers();
  get.mockReset();
});

afterEach(() => vi.useRealTimers());

describe('readSetupStatusForWarm', () => {
  it('asks nothing once a read has answered', async () => {
    const queryClient = client();
    queryClient.setQueryData(queryKeys.setup.status(), { needs_setup: false });
    await expect(readSetupStatusForWarm(queryClient)).resolves.toMatchObject({ needs_setup: false });
    expect(get).not.toHaveBeenCalled();
  });

  it('retries a failed read and puts the answer where the gate reads it', async () => {
    get.mockRejectedValueOnce(new Error('Network Error')).mockResolvedValue({ data: { needs_setup: true } });
    const queryClient = client();
    const reading = readSetupStatusForWarm(queryClient);
    await vi.advanceTimersByTimeAsync(10_000);
    await expect(reading).resolves.toMatchObject({ needs_setup: true });
    expect(get).toHaveBeenCalledTimes(2);
    expect(queryClient.getQueryData(queryKeys.setup.status())).toMatchObject({ needs_setup: true });
  });

  it('gives up after three attempts, so the app is not stuck', async () => {
    get.mockRejectedValue(new Error('Network Error'));
    const reading = readSetupStatusForWarm(client());
    await vi.advanceTimersByTimeAsync(10_000);
    await expect(reading).resolves.toBeNull();
    expect(get).toHaveBeenCalledTimes(3);
  });
});
