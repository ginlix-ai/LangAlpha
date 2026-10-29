/**
 * The upload orchestrator against a mocked backend and store: which path the
 * backend's answer sends a file down, and how the parts path retries, re-sends
 * what the store did not get, and gives up or cancels without leaving the
 * upload open.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('@/lib/authToken', () => ({
  authGeneration: vi.fn(() => 0),
  bearerTokenOf: vi.fn(() => null),
  getAccessToken: vi.fn(async () => null),
  getAuthHeaders: vi.fn(),
  refreshAccessToken: vi.fn(async () => null),
}));

vi.mock('@/api/client', async (importOriginal) => {
  const real = await importOriginal<typeof import('@/api/client')>();
  return { ...real, api: { putSigned: vi.fn() } };
});

vi.mock('../api/files', () => ({
  beginWorkspaceUpload: vi.fn(),
  signWorkspaceUploadParts: vi.fn(),
  completeWorkspaceUpload: vi.fn(),
  getWorkspaceUpload: vi.fn(),
  cancelWorkspaceUpload: vi.fn(),
  cancelWorkspaceUploadOnUnload: vi.fn(),
  uploadWorkspaceFile: vi.fn(),
}));

import { api, ApiError } from '@/api/client';
import { getAuthHeaders } from '@/lib/authToken';
import {
  beginWorkspaceUpload,
  cancelWorkspaceUpload,
  cancelWorkspaceUploadOnUnload,
  completeWorkspaceUpload,
  getWorkspaceUpload,
  signWorkspaceUploadParts,
  uploadWorkspaceFile,
} from '../api/files';
import { PART_IDLE_MS, uploadToWorkspace, UploadSendError, type UploadProgress } from '../workspaceUpload';

const begin = beginWorkspaceUpload as Mock;
const sign = signWorkspaceUploadParts as Mock;
const complete = completeWorkspaceUpload as Mock;
const status = getWorkspaceUpload as Mock;
const cancel = cancelWorkspaceUpload as Mock;
const unload = cancelWorkspaceUploadOnUnload as Mock;
const authHeaders = getAuthHeaders as Mock;
const relay = uploadWorkspaceFile as Mock;
const put = api.putSigned as Mock;

const WS = 'ws-1';
const UPLOAD = '00000000-0000-4000-8000-000000000001';
const PART = 16;
// Three parts: 16, 16 and the 8 that remain.
const file = new File([new Uint8Array(40)], 'big.bin');

const refusal = (status: number, detail: unknown) =>
  new ApiError(`Request failed with status code ${status}`, status < 500 ? 'ERR_BAD_REQUEST' : 'ERR_BAD_RESPONSE', {
    data: { detail }, status, statusText: '', headers: {},
  });
const networkError = () => new ApiError('Network Error', 'ERR_NETWORK');
const row = (state: string, extra: Record<string, unknown> = {}) => ({
  upload_id: UPLOAD, state, path: 'big.bin', size: 40, error: null, ...extra,
});

function signedParts(numbers: number[]) {
  return {
    expires_in: 900,
    parts: numbers.map((n) => ({
      part_number: n,
      offset: (n - 1) * PART,
      size: n < 3 ? PART : 40 - 2 * PART,
      url: `https://store.test/part/${n}`,
    })),
  };
}

const partOf = (url: string) => Number(url.split('/').pop());

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
  begin.mockResolvedValue({
    mode: 'direct', upload_id: UPLOAD, path: 'big.bin', part_size: PART, part_count: 3,
    expires_at: '2026-01-01T00:00:00Z',
  });
  sign.mockImplementation(async (_ws: string, _id: string, numbers: number[]) => signedParts(numbers));
  put.mockResolvedValue({ status: 200 });
  complete.mockResolvedValue(row('pulling'));
  status.mockResolvedValue(row('done'));
  cancel.mockResolvedValue(undefined);
  authHeaders.mockResolvedValue({ Authorization: 'Bearer t' });
});

afterEach(() => {
  vi.useRealTimers();
});

/** A PUT that sends nothing and ends only when aborted. */
const quiet = (_url: string, _body: Blob, { signal }: { signal: AbortSignal }) =>
  new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => reject(new ApiError('canceled', 'ERR_CANCELED')));
  });

async function settle<T>(promise: Promise<T>): Promise<T> {
  promise.catch(() => {});
  await vi.runAllTimersAsync();
  return promise;
}

describe('uploadToWorkspace', () => {
  it('sends the file in one request when the backend answers relay', async () => {
    begin.mockResolvedValue({ mode: 'relay' });
    relay.mockImplementation(async (_ws, _file, _path, onProgress: (p: number) => void) => {
      onProgress(40);
      onProgress(100);
      onProgress(100);
      return { path: 'big.bin' };
    });
    const seen: UploadProgress[] = [];

    await expect(settle(uploadToWorkspace(WS, file, { onProgress: (p) => seen.push(p) })))
      .resolves.toEqual({ path: 'big.bin' });

    expect(begin).toHaveBeenCalledWith(WS, { path: 'big.bin', size: 40 });
    expect(relay.mock.calls[0].slice(0, 3)).toEqual([WS, file, null]);
    expect(seen).toEqual([{ phase: 'sending', percent: 40 }, { phase: 'saving' }]);
    expect(put).not.toHaveBeenCalled();
  });

  it('sends each part once on a signed URL, completes, and waits for the file to be placed', async () => {
    status.mockResolvedValueOnce(row('pulling')).mockResolvedValueOnce(row('done'));
    const seen: UploadProgress[] = [];

    await expect(settle(uploadToWorkspace(WS, file, { path: 'data/big.bin', onProgress: (p) => seen.push(p) })))
      .resolves.toEqual({ path: 'big.bin' });

    expect(begin).toHaveBeenCalledWith(WS, { path: 'data/big.bin', size: 40 });
    // Signed ahead in one batch, not one request per part.
    expect(sign).toHaveBeenCalledTimes(1);
    expect(sign.mock.calls[0][2]).toEqual([1, 2, 3]);
    const sent = put.mock.calls.map(([url, body]) => [partOf(url), (body as Blob).size]);
    expect(sent.sort()).toEqual([[1, 16], [2, 16], [3, 8]]);
    // Signed URLs carry none of the app's headers, so nothing but the body goes.
    expect(put.mock.calls[0][2]).toEqual(expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(complete).toHaveBeenCalledTimes(1);
    expect(status).toHaveBeenCalledTimes(2);
    expect(seen.at(-1)).toEqual({ phase: 'saving' });
    expect(seen).toContainEqual({ phase: 'sending', percent: 100 });
    expect(cancel).not.toHaveBeenCalled();
  });

  it('retries a failed part on a freshly signed URL', async () => {
    put.mockImplementation(async (url: string) => {
      if (partOf(url) === 2 && put.mock.calls.filter(([u]) => partOf(u) === 2).length === 1) throw networkError();
      return { status: 200 };
    });

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });

    expect(put.mock.calls.filter(([url]) => partOf(url) === 2)).toHaveLength(2);
    expect(sign.mock.calls.map((c) => c[2])).toContainEqual([2]);
  });

  it('sends again the parts the store says it does not hold, then completes', async () => {
    complete
      .mockRejectedValueOnce(refusal(409, { message: 'Some parts of the file did not arrive.', missing_parts: [3] }))
      .mockResolvedValueOnce(row('pulling'));

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });

    expect(put.mock.calls.filter(([url]) => partOf(url) === 3)).toHaveLength(2);
    expect(complete).toHaveBeenCalledTimes(2);
  });

  it('stops every part when the backend refuses to sign, and ends the upload', async () => {
    const ended = refusal(409, 'This upload has ended. Start it again.');
    sign.mockRejectedValue(ended);

    await expect(settle(uploadToWorkspace(WS, file))).rejects.toBe(ended);

    expect(put).not.toHaveBeenCalled();
    expect(complete).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledWith(WS, UPLOAD);
  });

  it('gives up on a part after its retries, with a send error, and ends the upload', async () => {
    put.mockRejectedValue(networkError());

    await expect(settle(uploadToWorkspace(WS, file))).rejects.toBeInstanceOf(UploadSendError);

    expect(complete).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledWith(WS, UPLOAD);
  });

  it('cancels mid-send: aborts the parts in flight and ends the upload', async () => {
    const ctrl = new AbortController();
    put.mockImplementation((_url: string, _body: Blob, { signal }: { signal: AbortSignal }) =>
      new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => reject(new ApiError('canceled', 'ERR_CANCELED')));
      }));

    const upload = uploadToWorkspace(WS, file, { signal: ctrl.signal });
    upload.catch(() => {});
    await vi.advanceTimersByTimeAsync(0);
    expect(put).toHaveBeenCalled();
    ctrl.abort();

    await expect(settle(upload)).rejects.toMatchObject({ name: 'CanceledError' });
    expect(put.mock.calls.every(([, , config]) => config.signal.aborted)).toBe(true);
    expect(complete).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledWith(WS, UPLOAD);
  });

  it('ends an upload the backend started after the user had already cancelled', async () => {
    let started!: (value: unknown) => void;
    begin.mockReturnValue(new Promise((resolve) => { started = resolve; }));
    const ctrl = new AbortController();

    const upload = uploadToWorkspace(WS, file, { signal: ctrl.signal });
    upload.catch(() => {});
    ctrl.abort();
    await expect(settle(upload)).rejects.toMatchObject({ name: 'CanceledError' });

    started({ mode: 'direct', upload_id: UPLOAD, path: 'big.bin', part_size: PART, part_count: 3, expires_at: '' });
    await vi.runAllTimersAsync();
    expect(cancel).toHaveBeenCalledWith(WS, UPLOAD);
    expect(put).not.toHaveBeenCalled();
  });

  it("rejects with the server's reason when the workspace could not take the file", async () => {
    status.mockResolvedValue(row('failed', { error: 'Not enough free disk space in this workspace for this file (10 MB free).' }));

    await expect(settle(uploadToWorkspace(WS, file)))
      .rejects.toThrow('Not enough free disk space in this workspace for this file (10 MB free).');
    // Past completing, the parts are the workspace's to clean up.
    expect(cancel).not.toHaveBeenCalled();
  });

  it('gives a part up once no bytes have gone for a minute, and sends it again', async () => {
    put.mockImplementation((url: string, body: Blob, config: { signal: AbortSignal }) =>
      partOf(url) === 1 && put.mock.calls.filter(([u]) => partOf(u) === 1).length === 1
        ? quiet(url, body, config)
        : Promise.resolve({ status: 200 }));

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });

    expect(put.mock.calls.filter(([url]) => partOf(url) === 1)).toHaveLength(2);
    expect(cancel).not.toHaveBeenCalled();
  });

  it('keeps a slow part going for as long as its bytes still move', async () => {
    // Ten minutes for one part, never a minute without progress.
    put.mockImplementation((_url: string, _body: Blob, { onUploadProgress }: { onUploadProgress?: (e: { loaded: number }) => void }) =>
      new Promise((resolve) => {
        let loaded = 0;
        const tick = () => {
          onUploadProgress?.({ loaded: ++loaded });
          if (loaded < 12) setTimeout(tick, PART_IDLE_MS - 10_000);
          else resolve({ status: 200 });
        };
        setTimeout(tick, PART_IDLE_MS - 10_000);
      }));

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });

    expect(put).toHaveBeenCalledTimes(3);
    expect(put.mock.calls.every(([, , config]) => config.timeout === undefined)).toBe(true);
  });

  it('says the file is saving before it asks the backend to complete, so no cancel races the assembly', async () => {
    const order: string[] = [];
    complete.mockImplementation(async () => {
      order.push('complete');
      return row('pulling');
    });

    await settle(uploadToWorkspace(WS, file, {
      onProgress: (p) => { if (p.phase === 'saving') order.push('saving'); },
    }));

    expect(order.slice(0, 2)).toEqual(['saving', 'complete']);
  });

  it('says the file is saving again before completing after the missing parts are sent again', async () => {
    const order: string[] = [];
    complete.mockImplementation(async () => {
      order.push('complete');
      if (order.filter((o) => o === 'complete').length === 1) {
        throw refusal(409, { message: 'Some parts of the file did not arrive.', missing_parts: [3] });
      }
      return row('pulling');
    });

    await settle(uploadToWorkspace(WS, file, {
      onProgress: (p) => order.push(p.phase),
    }));

    const second = order.lastIndexOf('complete');
    expect(order.slice(0, second)).toContain('sending');
    expect(order[second - 1]).toBe('saving');
  });

  it.each([404, 405])('takes the one-request path when the backend has no parts route (%s)', async (code) => {
    begin.mockRejectedValue(refusal(code, code === 404 ? 'Not Found' : 'Method Not Allowed'));
    relay.mockResolvedValue({ path: 'big.bin' });

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });
    expect(relay.mock.calls[0].slice(0, 3)).toEqual([WS, file, null]);
  });

  it.each(['Workspace not found', 'File not found'])(
    'surfaces a 404 the route itself gave (%s) without sending the file the other way',
    async (detail) => {
      const refused = refusal(404, detail);
      begin.mockRejectedValue(refused);
      // Where it would end up: the one-request path refuses the same way, after the whole file.
      relay.mockRejectedValue(refusal(404, detail));

      await expect(settle(uploadToWorkspace(WS, file))).rejects.toBe(refused);
      expect(relay).not.toHaveBeenCalled();
    },
  );

  it('sends the missing parts again when the upload is handed back after the answer to complete was lost', async () => {
    complete
      // Reached the backend, which took the upload; the answer never came back.
      .mockRejectedValueOnce(networkError())
      .mockResolvedValueOnce(row('completing'))
      .mockRejectedValueOnce(refusal(409, { message: 'Some parts of the file did not arrive.', missing_parts: [2] }))
      .mockResolvedValueOnce(row('pulling'));
    // Assembling found part 2 short and handed the upload back.
    status.mockResolvedValueOnce(row('uploading')).mockResolvedValue(row('done'));

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });

    expect(put.mock.calls.filter(([url]) => partOf(url) === 2)).toHaveLength(2);
    expect(complete).toHaveBeenCalledTimes(4);
    expect(cancel).not.toHaveBeenCalled();
  });

  it('ends an upload handed back after complete when its missing parts cannot be sent', async () => {
    complete.mockResolvedValueOnce(row('completing'))
      .mockRejectedValueOnce(refusal(409, { message: 'Some parts of the file did not arrive.', missing_parts: [2] }));
    status.mockResolvedValueOnce(row('uploading'));
    put.mockImplementation(async (url: string) => {
      if (partOf(url) === 2 && put.mock.calls.filter(([u]) => partOf(u) === 2).length > 1) throw networkError();
      return { status: 200 };
    });

    await expect(settle(uploadToWorkspace(WS, file))).rejects.toBeInstanceOf(UploadSendError);
    expect(cancel).toHaveBeenCalledWith(WS, UPLOAD);
  });

  it('tells the backend the upload is alive while slow parts are in flight', async () => {
    // Each part takes ten minutes, its bytes moving all along.
    put.mockImplementation((_url: string, _body: Blob, { onUploadProgress }: { onUploadProgress?: (e: { loaded: number }) => void }) =>
      new Promise((resolve) => {
        let loaded = 0;
        const tick = () => {
          onUploadProgress?.({ loaded: ++loaded });
          if (loaded < 20) setTimeout(tick, 30_000);
          else resolve({ status: 200 });
        };
        setTimeout(tick, 30_000);
      }));
    const signedAt: number[] = [];
    sign.mockImplementation(async (_ws: string, _id: string, numbers: number[]) => {
      signedAt.push(Date.now());
      return signedParts(numbers);
    });
    const start = Date.now();

    await expect(settle(uploadToWorkspace(WS, file))).resolves.toEqual({ path: 'big.bin' });

    // A minute at most, well inside the five a computer may sit idle at its
    // shortest, with room for the backend counting it once a minute.
    const gaps = [...signedAt.slice(1), start + 10 * 60_000].map((t, i) => t - signedAt[i]);
    expect(Math.max(...gaps)).toBeLessThanOrEqual(60_000);
    expect(sign.mock.calls.slice(1).every(([, , numbers]) => numbers.length === 1 && numbers[0] <= 3)).toBe(true);
    expect(put).toHaveBeenCalledTimes(3);
  });

  it('ends the upload with a request that outlives the page when the page goes away mid-send', async () => {
    put.mockImplementation(quiet);
    const ctrl = new AbortController();
    const upload = uploadToWorkspace(WS, file, { signal: ctrl.signal });
    upload.catch(() => {});
    await vi.advanceTimersByTimeAsync(0);

    // Kept for the back-forward cache, the page may come back and carry on.
    window.dispatchEvent(Object.assign(new Event('pagehide'), { persisted: true }));
    expect(unload).not.toHaveBeenCalled();
    window.dispatchEvent(new Event('pagehide'));
    expect(unload).toHaveBeenCalledWith(WS, UPLOAD, { Authorization: 'Bearer t' });

    ctrl.abort();
    await expect(settle(upload)).rejects.toMatchObject({ name: 'CanceledError' });
  });
});
