/**
 * One file into the Drive against a mocked server and store: the way each
 * begin answer sends it, a part whose direct PUT never arrives going through
 * the server, an upload continued from the parts the store holds, and a
 * connection failure leaving the upload open for that.
 */
import { createHash } from 'node:crypto';
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
  return { ...real, api: { putSigned: vi.fn(), delete: vi.fn() } };
});
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  saveFile: vi.fn(),
}));
vi.mock('@/api/driveUploads', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/driveUploads')>()),
  beginDriveUpload: vi.fn(),
  signDriveUploadParts: vi.fn(),
  relayDriveUploadPart: vi.fn(),
  completeDriveUpload: vi.fn(),
  getDriveUpload: vi.fn(),
  cancelDriveUpload: vi.fn(),
}));

import { api, ApiError } from '@/api/client';
import { saveFile } from '@/api/drive';
import {
  beginDriveUpload,
  cancelDriveUpload,
  completeDriveUpload,
  DriveUploadFailed,
  getDriveUpload,
  relayDriveUploadPart,
  signDriveUploadParts,
} from '@/api/driveUploads';
import { UploadSendError } from '@/lib/partUpload';
import type { DriveEntry } from '@/types/drive';
import { continuable, sendToDrive } from '../driveUpload';

const begin = beginDriveUpload as Mock;
const sign = signDriveUploadParts as Mock;
const relay = relayDriveUploadPart as Mock;
const complete = completeDriveUpload as Mock;
const status = getDriveUpload as Mock;
const cancel = cancelDriveUpload as Mock;
const put = api.putSigned as Mock;
const save = saveFile as Mock;

const ID = 'u-1';
const PART = 16;
// Three parts: 16, 16 and the 8 that remain.
const bytes = Uint8Array.from({ length: 40 }, (_, i) => i);
const file = new File([bytes], 'prices.parquet');
/** Part `n`'s MD5 as the store reports it (hex) and as a part is sent with it (base64). */
const digest = (n: number, of: Uint8Array = bytes) => createHash('md5').update(of.subarray((n - 1) * PART, n * PART));
const hex = (n: number, of?: Uint8Array) => digest(n, of).digest('hex');
const b64 = (n: number) => digest(n).digest('base64');
type Signed = Array<{ part_number: number; md5: string }>;
const entry = { id: 'e-1', path: 'data/prices.parquet', name: 'prices.parquet' } as DriveEntry;
const open = (mode: 'direct' | 'relay', parts_held: Array<{ part_number: number; size: number; md5?: string | null }> = []) => ({
  mode, upload_id: ID, path: entry.path, size: 40, part_size: PART, part_count: 3, parts_held, expires_at: '', resumed: false,
});
const row = (state: string, extra: Record<string, unknown> = {}) => ({ ...open('relay'), state, ...extra });
const networkError = () => new ApiError('Network Error', 'ERR_NETWORK');

function events() {
  return { onProgress: vi.fn(), onBusy: vi.fn(), onVerifying: vi.fn(), onOpen: vi.fn() };
}

async function send(uploadId: string | null = null, ev = events()) {
  const sent = sendToDrive('personal', entry.path, file, { ...ev, uploadId, signal: new AbortController().signal });
  // Lets the retries' backoffs and the verifying polls run.
  const settled = sent.then((v) => ({ v }), (e) => ({ e }));
  await vi.runAllTimersAsync();
  const out = await settled;
  if ('e' in out) throw out.e;
  return out.v;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
  sign.mockImplementation(async (_s: string, _id: string, asked: Signed) => ({
    expires_in: 900,
    parts: asked.map(({ part_number: n }) => ({ part_number: n, offset: (n - 1) * PART, size: n < 3 ? PART : 8, url: `https://store.test/${n}`, headers: { 'x-amz-checksum': `c${n}` } })),
  }));
  put.mockResolvedValue({ status: 200 });
  relay.mockResolvedValue(undefined);
  complete.mockResolvedValue(row('verifying'));
  status.mockResolvedValue(row('done', { entry }));
  cancel.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.useRealTimers();
});

describe('sendToDrive', () => {
  it('sends a file the server answers `single` for in one request', async () => {
    begin.mockResolvedValue({ mode: 'single', max_bytes: 50 });
    save.mockResolvedValue(entry);
    expect(await send()).toBe(entry);
    expect(begin).toHaveBeenCalledWith('personal', { path: entry.path, size: 40, content_type: undefined });
    expect(save).toHaveBeenCalledWith('personal', entry.path, file, expect.anything());
    expect(relay).not.toHaveBeenCalled();
  });

  it('sends nothing for bytes the server already holds', async () => {
    begin.mockResolvedValue({ mode: 'held', entry });
    expect(await send()).toBe(entry);
    expect(save).not.toHaveBeenCalled();
    expect(relay).not.toHaveBeenCalled();
  });

  it('relays every part, then waits out the check before the entry', async () => {
    begin.mockResolvedValue(open('relay'));
    const ev = events();
    expect(await send(null, ev)).toBe(entry);
    expect(relay.mock.calls.map((c) => [c[2], (c[3] as Blob).size])).toEqual([[1, 16], [2, 16], [3, 8]]);
    expect(sign).not.toHaveBeenCalled();
    expect(ev.onOpen).toHaveBeenCalledWith(ID);
    expect(ev.onVerifying).toHaveBeenCalled();
    expect(ev.onProgress).toHaveBeenLastCalledWith(40);
  });

  it("sends each part with its MD5, for the store to refuse bytes that changed on the way", async () => {
    begin.mockResolvedValue(open('direct'));
    put.mockImplementation(async (url: string) => {
      if (url.endsWith('/2')) throw networkError();
      return { status: 200 };
    });
    expect(await send()).toBe(entry);
    expect(sign.mock.calls.flatMap((c) => c[2] as Signed)).toEqual([1, 2, 3].map((n) => ({ part_number: n, md5: b64(n) })));
    // The part relayed after its PUT failed carries its digest too.
    expect(relay.mock.calls.map((c) => [c[2], c[4]])).toEqual([[2, b64(2)]]);
  });

  it('puts direct parts with the headers they were signed with, and relays a part whose PUT never arrived', async () => {
    begin.mockResolvedValue(open('direct'));
    put.mockImplementation(async (url: string) => {
      if (url.endsWith('/2')) throw networkError();
      return { status: 200 };
    });
    expect(await send()).toBe(entry);
    expect(put).toHaveBeenCalledWith('https://store.test/1', expect.any(Blob), expect.objectContaining({ headers: { 'x-amz-checksum': 'c1' } }));
    expect(put.mock.calls.filter((c) => (c[0] as string).endsWith('/2'))).toHaveLength(1);
    expect(relay.mock.calls.map((c) => c[2])).toEqual([2]);
  });

  it('sends the rest through the server once the store is out of reach', async () => {
    const big = new File([new Uint8Array(6 * PART)], 'prices.parquet');
    begin.mockResolvedValue({ ...open('direct'), size: 6 * PART, part_count: 6 });
    sign.mockImplementation(async (_s: string, _id: string, asked: Signed) => ({
      expires_in: 900,
      parts: asked.map(({ part_number: n }) => ({ part_number: n, offset: (n - 1) * PART, size: PART, url: `https://store.test/${n}` })),
    }));
    put.mockRejectedValue(networkError());
    const sent = sendToDrive('personal', entry.path, big, { ...events(), uploadId: null, signal: new AbortController().signal });
    const settled = sent.then((v) => ({ v }), (e) => ({ e }));
    await vi.runAllTimersAsync();
    expect(await settled).toEqual({ v: entry });
    // Only the parts already on their way tried the store.
    expect(put.mock.calls.length).toBeLessThanOrEqual(4);
    expect(relay.mock.calls.map((c) => c[2]).sort()).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it('continues an open upload from the parts the store holds', async () => {
    status.mockResolvedValueOnce(row('uploading', { parts_held: [{ part_number: 1, size: 16, md5: hex(1) }, { part_number: 3, size: 8, md5: hex(3).toUpperCase() }] }));
    expect(await send(ID)).toBe(entry);
    expect(begin).not.toHaveBeenCalled();
    expect(relay.mock.calls.map((c) => c[2])).toEqual([2]);
  });

  it('sends again a held part whose store digest differs from the file, rather than refusing the resume', async () => {
    const other = bytes.slice();
    other[20] ^= 1;
    status.mockResolvedValueOnce(row('uploading', { parts_held: [{ part_number: 1, size: 16, md5: hex(1) }, { part_number: 2, size: 16, md5: hex(2, other) }] }));
    expect(await send(ID)).toBe(entry);
    // Every part the store keeps is now the file's own: the one proven, the rest sent from it.
    expect(relay.mock.calls.map((c) => c[2])).toEqual([2, 3]);
    expect(cancel).not.toHaveBeenCalled();
  });

  it('continues on a store whose part tags are not MD5s, sending every held part again', async () => {
    // Server-side encryption with a managed key: 32 hex characters, never the part's MD5.
    const tag = (n: number) => `${n}`.padStart(32, 'e');
    status.mockResolvedValueOnce(row('uploading', { parts_held: [1, 2, 3].map((n) => ({ part_number: n, size: n < 3 ? 16 : 8, md5: tag(n) })) }));
    expect(await send(ID)).toBe(entry);
    expect(relay.mock.calls.map((c) => c[2])).toEqual([1, 2, 3]);
  });

  it('sends again a held part the store names no digest for, since it cannot be proven', async () => {
    status.mockResolvedValueOnce(row('uploading', { parts_held: [{ part_number: 1, size: 16 }, { part_number: 3, size: 8, md5: hex(3) }] }));
    expect(await send(ID)).toBe(entry);
    expect(relay.mock.calls.map((c) => c[2])).toEqual([1, 2]);
  });

  it('says the file is being saved once the one-request body is all sent, when a cancel no longer stops it', async () => {
    begin.mockResolvedValue({ mode: 'single', max_bytes: 50 });
    save.mockImplementation(async (_s: string, _p: string, _f: File, { onProgress }: { onProgress: (e: { loaded: number }) => void }) => {
      onProgress({ loaded: 20 });
      onProgress({ loaded: 40 });
      return entry;
    });
    const ev = events();
    expect(await send(null, ev)).toBe(entry);
    expect(ev.onProgress.mock.calls).toEqual([[20]]);
    expect(ev.onVerifying).toHaveBeenCalledTimes(1);
  });

  it('begins again when the upload it had is gone', async () => {
    status.mockResolvedValueOnce(row('aborted'));
    begin.mockResolvedValue({ mode: 'held', entry });
    expect(await send(ID)).toBe(entry);
    expect(begin).toHaveBeenCalled();
  });

  it('leaves the upload open when the connection fails, for a retry to continue', async () => {
    begin.mockResolvedValue(open('relay'));
    relay.mockRejectedValue(networkError());
    await expect(send()).rejects.toBeInstanceOf(UploadSendError);
    expect(cancel).not.toHaveBeenCalled();
  });

  it('ends the upload when the server refuses a part', async () => {
    begin.mockResolvedValue(open('relay'));
    relay.mockRejectedValue(new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
      data: { detail: { type: 'upload_ended', message: 'ended' } }, status: 409, statusText: '', headers: {},
    }));
    await expect(send()).rejects.toMatchObject({ status: 409 });
    expect(cancel).toHaveBeenCalledWith('personal', ID);
  });

  it("says why the server did not keep the file, in its status's own type", async () => {
    begin.mockResolvedValue(open('relay'));
    status.mockResolvedValue(row('failed', { error: { type: 'mismatch', message: 'bytes differ' } }));
    const err = await send().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(DriveUploadFailed);
    expect(err).toMatchObject({ type: 'mismatch' });
  });

  it('reads a refusal for room or for too many open uploads as an answer, not as passing trouble', () => {
    const refused = (status: number, type: string) => new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_RESPONSE', {
      data: { detail: { type, message: '' } }, status, statusText: '', headers: {},
    });
    expect(continuable(refused(507, 'over_quota'))).toBe(false);
    expect(continuable(refused(507, 'too_many_entries'))).toBe(false);
    expect(continuable(refused(429, 'too_many_uploads'))).toBe(false);
    expect(continuable(refused(503, 'unavailable'))).toBe(true);
    expect(continuable(networkError())).toBe(true);
  });

  it('says how an upload ended when cancelling it: stopped, or saved before the cancel arrived', async () => {
    const real = await vi.importActual<typeof import('@/api/driveUploads')>('@/api/driveUploads');
    const del = api.delete as Mock;
    del.mockResolvedValueOnce({ data: { state: 'aborted' }, status: 200, headers: {} });
    expect(await real.cancelDriveUpload('personal', ID)).toBe('aborted');
    del.mockRejectedValueOnce(new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
      data: { detail: { type: 'upload_ended', message: 'ended', state: 'done' } }, status: 409, statusText: '', headers: {},
    }));
    expect(await real.cancelDriveUpload('personal', ID)).toBe('done');
  });

  it('relays every part, each with its MD5, when the store refuses the header in its preflight', async () => {
    begin.mockResolvedValue(open('direct'));
    sign.mockImplementation(async (_s: string, _id: string, asked: Signed) => ({
      expires_in: 900,
      parts: asked.map(({ part_number: n, md5 }) => ({
        part_number: n, offset: (n - 1) * PART, size: n < 3 ? PART : 8, url: `https://store.test/${n}`, headers: { 'Content-MD5': md5 },
      })),
    }));
    // A preflight the bucket's CORS refuses never reaches the store: the browser reports no answer.
    put.mockRejectedValue(networkError());
    expect(await send()).toBe(entry);
    expect(relay.mock.calls.map((c) => [c[2], c[4]]).sort()).toEqual([[1, b64(1)], [2, b64(2)], [3, b64(3)]]);
  });

  it('relays a part the store keeps refusing once signed with its digest', async () => {
    begin.mockResolvedValue(open('direct'));
    put.mockImplementation(async (url: string) => {
      if (url.endsWith('/2')) throw new ApiError('Request failed with status code 403', 'ERR_BAD_REQUEST', { data: '', status: 403, statusText: '', headers: {} });
      return { status: 200 };
    });
    expect(await send()).toBe(entry);
    expect(relay.mock.calls.map((c) => c[2])).toEqual([2]);
  });

  it('takes a full space at completion as the upload failing, without asking again', async () => {
    begin.mockResolvedValue(open('relay'));
    complete.mockRejectedValue(new ApiError('Request failed with status code 507', 'ERR_BAD_RESPONSE', {
      data: { detail: { type: 'over_quota', message: 'full' } }, status: 507, statusText: '', headers: {},
    }));
    const err = await send().catch((e: unknown) => e);
    expect(err).toMatchObject({ type: 'over_quota' });
    expect(continuable(err)).toBe(false);
    expect(complete).toHaveBeenCalledTimes(1);
  });

  it('sends again a held part whose store tag is not an MD5, and says how far the check of the rest is', async () => {
    status.mockResolvedValueOnce(row('uploading', { parts_held: [{ part_number: 1, size: 16, md5: null }, { part_number: 2, size: 16, md5: hex(2) }, { part_number: 3, size: 8, md5: hex(3) }] }));
    const ev = { ...events(), onChecking: vi.fn() };
    expect(await send(ID, ev)).toBe(entry);
    expect(relay.mock.calls.map((c) => c[2])).toEqual([1]);
    expect(ev.onChecking.mock.calls).toEqual([[0, 24], [16, 24], [24, 24]]);
  });
});
