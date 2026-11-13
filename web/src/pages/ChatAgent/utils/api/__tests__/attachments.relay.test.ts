/**
 * A relay upload that stops moving is abandoned as a failed send, which the
 * composer shows with Retry: no bytes for a minute, or no answer for two
 * minutes after the last one. A cancel by the caller stays a cancel.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('@/lib/authToken', () => ({
  authGeneration: vi.fn(() => 0),
  bearerTokenOf: vi.fn(() => null),
  getAccessToken: vi.fn(async () => null),
  getAuthHeaders: vi.fn(async () => ({})),
  refreshAccessToken: vi.fn(async () => null),
}));

vi.mock('@/api/client', async (importOriginal) => {
  const real = await importOriginal<typeof import('@/api/client')>();
  return { ...real, api: { post: vi.fn() } };
});

import { api, ApiError, type TransferProgress } from '@/api/client';
import { PART_IDLE_MS, UploadSendError } from '../../partUpload';
import { RELAY_ANSWER_MS, sendAttachmentContent } from '../attachments';

const post = api.post as Mock;
const file = new File([new Uint8Array(100)], 'data.bin');

/** A request that answers only when aborted; its progress callback is the test's. */
function hangingPost() {
  const calls: Array<{ signal: AbortSignal; progress: (e: TransferProgress) => void }> = [];
  post.mockImplementation((_url: string, _body: FormData, config: { signal: AbortSignal; onUploadProgress: (e: TransferProgress) => void }) =>
    new Promise((_resolve, reject) => {
      calls.push({ signal: config.signal, progress: config.onUploadProgress });
      config.signal.addEventListener('abort', () => reject(new ApiError('canceled', 'ERR_CANCELED')));
    }));
  return calls;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('sendAttachmentContent', () => {
  it('gives up as a failed send once no bytes have gone for a minute', async () => {
    const calls = hangingPost();
    const sending = sendAttachmentContent('att-1', file, () => {});
    sending.catch(() => {});

    await vi.advanceTimersByTimeAsync(PART_IDLE_MS - 1);
    expect(calls[0].signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await expect(sending).rejects.toBeInstanceOf(UploadSendError);
  });

  it('keeps going while bytes move, then waits longer for the answer after the last one', async () => {
    const calls = hangingPost();
    const seen: number[] = [];
    const sending = sendAttachmentContent('att-1', file, (p) => seen.push(p));
    sending.catch(() => {});

    await vi.advanceTimersByTimeAsync(PART_IDLE_MS - 1000);
    calls[0].progress({ loaded: 50, total: 100 });
    await vi.advanceTimersByTimeAsync(PART_IDLE_MS - 1000);
    calls[0].progress({ loaded: 100, total: 100 });
    expect(seen).toEqual([50, 100]);

    // Past a minute with nothing moving: the server is storing it.
    await vi.advanceTimersByTimeAsync(RELAY_ANSWER_MS - 1);
    expect(calls[0].signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await expect(sending).rejects.toBeInstanceOf(UploadSendError);
  });

  it("stays a cancel when the caller aborts", async () => {
    const calls = hangingPost();
    const ctrl = new AbortController();
    const sending = sendAttachmentContent('att-1', file, () => {}, ctrl.signal);
    sending.catch(() => {});

    ctrl.abort();
    await expect(sending).rejects.toMatchObject({ name: 'CanceledError' });
    expect(calls[0].signal.aborted).toBe(true);
    // No watchdog left behind.
    expect(vi.getTimerCount()).toBe(0);
  });
});
