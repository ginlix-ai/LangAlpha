import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError } from '@/api/client';
import { runAuthResets } from '@/lib/authResets';
import { busyRetryDelay, clearHistory, saveFile } from '../drive';

function refusal(status: number, headers: Record<string, string> = {}) {
  const data = { detail: { type: status === 503 ? 'unavailable' : 'changed', message: 'x' } };
  return new ApiError(`Request failed with status code ${status}`, status >= 500 ? 'ERR_BAD_RESPONSE' : 'ERR_BAD_REQUEST', {
    data, status, statusText: '', headers,
  });
}

const entry = { id: 'e1', path: 'a.md', sha256: 'f'.repeat(64) };

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('busyRetryDelay', () => {
  it('waits as long as Retry-After says, within a bound', () => {
    expect(busyRetryDelay(refusal(503, { 'retry-after': '2' }))).toBe(2000);
    expect(busyRetryDelay(refusal(503, { 'retry-after': '600' }))).toBe(15_000);
    expect(busyRetryDelay(refusal(503))).toBe(2000);
  });

  it('never retries a refusal a retry would not change', () => {
    expect(busyRetryDelay(refusal(412))).toBeNull();
    expect(busyRetryDelay(refusal(507))).toBeNull();
    expect(busyRetryDelay(new Error('Network Error'))).toBeNull();
  });
});

describe('saveFile on a busy server', () => {
  it('sends the same save again after each 503 and answers the entry', async () => {
    vi.useFakeTimers();
    const put = vi.spyOn(api, 'put')
      .mockRejectedValueOnce(refusal(503, { 'retry-after': '1' }))
      .mockRejectedValueOnce(refusal(503, { 'retry-after': '1' }))
      .mockResolvedValueOnce({ data: entry, status: 200, statusText: 'OK', headers: {} });
    const onBusy = vi.fn();
    const saving = saveFile('personal', 'a.md', new Blob(['x']), { ifMatch: 'a'.repeat(64), sitting: 'web-1', onBusy });
    await vi.runAllTimersAsync();
    await expect(saving).resolves.toEqual(entry);
    expect(put).toHaveBeenCalledTimes(3);
    expect(onBusy).toHaveBeenCalledTimes(2);
    // Every attempt is the one save: same If-Match, same sitting.
    for (const call of put.mock.calls) {
      expect(call[2]?.headers).toMatchObject({ 'If-Match': `"${'a'.repeat(64)}"`, 'X-Drive-Sitting': 'web-1' });
    }
  });

  it('gives up after a few tries and hands the 503 to the caller', async () => {
    vi.useFakeTimers();
    const put = vi.spyOn(api, 'put').mockRejectedValue(refusal(503, { 'retry-after': '1' }));
    const saving = saveFile('personal', 'a.md', new Blob(['x']));
    const settled = expect(saving).rejects.toMatchObject({ response: { status: 503 } });
    await vi.runAllTimersAsync();
    await settled;
    expect(put).toHaveBeenCalledTimes(4);
  });

  it('does not send again under an account signed in while it waited', async () => {
    vi.useFakeTimers();
    const put = vi.spyOn(api, 'put').mockRejectedValue(refusal(503, { 'retry-after': '1' }));
    const saving = saveFile('personal', 'a.md', new Blob(['x']));
    const settled = expect(saving).rejects.toMatchObject({ code: 'ERR_CANCELED' });
    await vi.advanceTimersByTimeAsync(0);
    runAuthResets();
    await vi.runAllTimersAsync();
    await settled;
    expect(put).toHaveBeenCalledTimes(1);
  });

  it('does not retry a conflict', async () => {
    const put = vi.spyOn(api, 'put').mockRejectedValue(refusal(412));
    await expect(saveFile('personal', 'a.md', new Blob(['x']))).rejects.toMatchObject({ response: { status: 412 } });
    expect(put).toHaveBeenCalledTimes(1);
  });
});

describe('clearHistory', () => {
  it('clears only below the head the caller saw, so a version saved since stays', async () => {
    const post = vi.spyOn(api, 'post').mockResolvedValue({ data: { dropped: 2, freed_bytes: 10 }, status: 200, statusText: 'OK', headers: {} });
    await clearHistory('personal', 'e1', { head: 7, final: 5 });
    expect(post).toHaveBeenLastCalledWith(expect.stringContaining('versions/clear'), { id: 'e1', before: 7, final_seq: 5 });
    // No pinned final is said too: the clear is refused if one has appeared.
    await clearHistory('personal', 'e1', { head: 7, final: null });
    expect(post).toHaveBeenLastCalledWith(expect.stringContaining('versions/clear'), { id: 'e1', before: 7, final_seq: null });
  });
});
