// @vitest-environment node
/**
 * The ranged reader against a stand-in for the download route's contract: one
 * `bytes=a-b` range answered with a 206 of at most `cap` bytes, the version in
 * the ETag, and a 416 past the end. A server that ignores Range sends a 200.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>();
  return { ...actual, api: { get: vi.fn() } };
});
vi.mock('@/lib/supabase', () => ({ supabase: null }));
vi.mock('@/lib/authToken', () => ({
  authGeneration: vi.fn(() => 0),
  bearerTokenOf: vi.fn(() => null),
  getAccessToken: vi.fn(async () => null),
  refreshAccessToken: vi.fn(async () => null),
}));

import { api, ApiError, type RequestConfig } from '@/api/client';
import { FileChangedError, isRangedFile, isStale, type RangedFile } from '@/lib/rangedFile';
import { openWorkspaceFileRanged } from '../files';

const mockGet = api.get as unknown as Mock;
const MIB = 1024 * 1024;

interface Server {
  bytes: Uint8Array;
  /** Null sends no ETag. */
  etag: string | null;
  honor: boolean;
  exposeRange: boolean;
  cap: number;
  /** Bytes each partial answer arrives short of what its Content-Range says. */
  short: number;
  /** Failures to answer before serving, oldest first. */
  failures: ApiError[];
  ranges: (string | undefined)[];
}

function fileOf(length: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) out[i] = (i * 31 + (i >> 11)) & 0xff;
  return out;
}

function statusError(status: number, headers: Record<string, string> = {}) {
  return new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_REQUEST', { status, statusText: '', data: {}, headers });
}

/**
 * What the real client rejects a range request with, given what fetch does.
 * Its error body comes back in the type the request asked for, so a range's
 * is bytes, not the JSON `statusError` fakes.
 */
async function clientFailure(
  fetchImpl: (url: string, init: RequestInit) => Promise<Response>,
  config: RequestConfig = {},
): Promise<ApiError> {
  const { api: real } = await vi.importActual<typeof import('@/api/client')>('@/api/client');
  vi.stubGlobal('fetch', fetchImpl);
  try {
    await real.get('/download', { ...config, responseType: 'arraybuffer' });
  } catch (err) {
    return err as ApiError;
  } finally {
    vi.unstubAllGlobals();
  }
  throw new Error('expected the request to fail');
}

const refusal = (status: number, body: unknown) =>
  clientFailure(async () => new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', 'retry-after': '1' },
  }));
const FILE_CHANGED = { detail: { code: 'file_changed', message: 'File changed while it was read' } };

/** A request the real client gave up on when its answer went silent. Real timers: fake ones would never fire it. */
const wentSilent = () =>
  clientFailure((_url, init) => new Promise((_resolve, reject) => {
    init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
  }), { idleTimeout: 1 });

function serve(length: number, overrides: Partial<Server> = {}): Server {
  const server: Server = {
    bytes: fileOf(length), etag: '"v1"', honor: true, exposeRange: true, cap: 8 * MIB, short: 0, failures: [], ranges: [],
    ...overrides,
  };
  mockGet.mockImplementation(async (_url: string, config: RequestConfig) => {
    const range = config.headers?.Range;
    server.ranges.push(range);
    const failure = server.failures.shift();
    if (failure) throw failure;
    const total = server.bytes.length;
    const match = range && /^bytes=(\d+)-(\d+)$/.exec(range);
    if (!server.honor || !match) {
      // As the client does, it leaves the body of a status the call did not expect unread.
      const read = config.expectStatus === undefined || config.expectStatus === 200;
      return { status: 200, data: read ? server.bytes.slice().buffer : null, headers: {} };
    }
    const first = Number(match[1]);
    if (first >= total) throw statusError(416, { 'content-range': `bytes */${total}` });
    const last = Math.min(Number(match[2]), total - 1, first + server.cap - 1);
    const headers: Record<string, string> = {};
    if (server.etag !== null) headers.etag = server.etag;
    if (server.exposeRange) headers['content-range'] = `bytes ${first}-${last}/${total}`;
    return { status: 206, data: server.bytes.slice(first, last + 1 - server.short).buffer, headers };
  });
  return server;
}

/** Byte equality; the matcher's element-by-element walk is far too slow at MiBs. */
function same(actual: ArrayBuffer | Uint8Array, expected: Uint8Array) {
  const bytes = actual instanceof Uint8Array ? actual : new Uint8Array(actual);
  expect(bytes.byteLength).toBe(expected.byteLength);
  expect(Buffer.from(bytes).equals(Buffer.from(expected))).toBe(true);
}

async function openRanged(): Promise<RangedFile> {
  const opened = await openWorkspaceFileRanged('ws-1', 'reports/big.pdf');
  if (!isRangedFile(opened)) throw new Error('expected a ranged file');
  return opened;
}

beforeEach(() => {
  mockGet.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('openWorkspaceFileRanged', () => {
  it('returns a file that fits in the opening slice as bytes, from the one request', async () => {
    const server = serve(300_000);
    const opened = await openWorkspaceFileRanged('ws-1', 'a.pdf');
    same(opened as ArrayBuffer, server.bytes);
    expect(server.ranges).toEqual(['bytes=0-4194303']);
  });

  it('returns the whole body when the server ignores the range', async () => {
    const server = serve(6 * MIB, { honor: false });
    const opened = await openWorkspaceFileRanged('ws-1', 'a.pdf');
    same(opened as ArrayBuffer, server.bytes);
    expect(server.ranges).toHaveLength(1);
  });

  it('reads an empty file whole, since a range has no first byte to start at', async () => {
    const server = serve(0);
    const opened = await openWorkspaceFileRanged('ws-1', 'a.pdf');
    expect((opened as ArrayBuffer).byteLength).toBe(0);
    expect(server.ranges).toEqual(['bytes=0-4194303', undefined]);
  });

  it('reads the file whole when a partial answer does not say where it sits', async () => {
    const server = serve(6 * MIB, { exposeRange: false });
    const opened = await openWorkspaceFileRanged('ws-1', 'a.pdf');
    same(opened as ArrayBuffer, server.bytes);
    expect(server.ranges).toEqual(['bytes=0-4194303', undefined]);
  });

  it('reads the file whole when a partial answer names no version to hold the slices to', async () => {
    const server = serve(6 * MIB, { etag: null });
    const opened = await openWorkspaceFileRanged('ws-1', 'a.pdf');
    same(opened as ArrayBuffer, server.bytes);
    expect(server.ranges).toEqual(['bytes=0-4194303', undefined]);
  });

  it('reads the file whole when the opening answer is not the bytes it says it is', async () => {
    const server = serve(6 * MIB, { short: 1 });
    const opened = await openWorkspaceFileRanged('ws-1', 'a.pdf');
    same(opened as ArrayBuffer, server.bytes);
    expect(server.ranges).toEqual(['bytes=0-4194303', undefined]);
  });

  it('returns a file of exactly the opening slice as bytes, and one byte more as a ranged file', async () => {
    const fits = serve(4 * MIB);
    same(await openWorkspaceFileRanged('ws-1', 'a.pdf') as ArrayBuffer, fits.bytes);
    serve(4 * MIB + 1);
    expect((await openRanged()).length).toBe(4 * MIB + 1);
  });

  it('retries the opening request lost in transit or to a server error', async () => {
    vi.useFakeTimers();
    const server = serve(6 * MIB);
    server.failures.push(new ApiError('Network Error', 'ERR_NETWORK'), statusError(503));
    const opening = openRanged();
    await vi.advanceTimersByTimeAsync(2500);
    expect((await opening).length).toBe(6 * MIB);
    expect(server.ranges).toEqual(['bytes=0-4194303', 'bytes=0-4194303', 'bytes=0-4194303']);
  });

  it('asks past the browser cache for every range', async () => {
    serve(6 * MIB);
    const file = await openRanged();
    await file.read(5 * MIB, 5 * MIB + 10);
    expect(mockGet).toHaveBeenCalledTimes(2);
    for (const [, config] of mockGet.mock.calls) expect(config).toMatchObject({ cache: 'no-store' });
  });

  it('opens a larger file from its first slice and reads any span on request', async () => {
    const server = serve(6 * MIB + 123, { etag: '"6291579-1791343164.8"' });
    const file = await openRanged();
    expect(file.length).toBe(6 * MIB + 123);
    expect(file.etag).toBe('"6291579-1791343164.8"');
    same(file.initial, server.bytes.subarray(0, 4 * MIB));

    const span = await file.read(5 * MIB, 5 * MIB + 65_536);
    same(span, server.bytes.subarray(5 * MIB, 5 * MIB + 65_536));
    expect(server.ranges.at(-1)).toBe(`bytes=${5 * MIB}-${5 * MIB + 65_535}`);
  });

  it('asks again from where a capped answer stopped', async () => {
    const server = serve(6 * MIB, { cap: MIB });
    const file = await openRanged();
    const before = server.ranges.length;
    const span = await file.read(MIB / 2, 3 * MIB);
    same(span, server.bytes.subarray(MIB / 2, 3 * MIB));
    expect(server.ranges.slice(before)).toEqual([
      `bytes=${MIB / 2}-${3 * MIB - 1}`,
      `bytes=${MIB / 2 + MIB}-${3 * MIB - 1}`,
      `bytes=${MIB / 2 + 2 * MIB}-${3 * MIB - 1}`,
    ]);
  });

  it('throws FileChangedError for a slice of another version', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    server.etag = '"v2"';
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
  });

  it('throws FileChangedError for a slice of a file that changed length', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    server.bytes = fileOf(7 * MIB);
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
  });

  it('throws FileChangedError for a slice past the end of a file that shrank, and asks once', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    const before = server.ranges.length;
    server.bytes = fileOf(5 * MIB);
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
    expect(server.ranges.length - before).toBe(1);
    expect(isStale(file)).toBe(true);
  });

  it('throws FileChangedError for a slice of a file that is gone, and asks once', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    const before = server.ranges.length;
    server.failures.push(statusError(404));
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
    expect(server.ranges.length - before).toBe(1);
    expect(isStale(file)).toBe(true);
  });

  it('throws FileChangedError when the server answers a slice whole', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    server.honor = false;
    const before = server.ranges.length;
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
    expect(server.ranges.length - before).toBe(1);
  });

  it('never downloads the whole file a later slice is answered with', async () => {
    serve(6 * MIB);
    const file = await openRanged();
    const { api: real } = await vi.importActual<typeof import('@/api/client')>('@/api/client');
    mockGet.mockImplementation((url: string, config: RequestConfig) => real.get(url, config));
    const getReader = vi.fn();
    const arrayBuffer = vi.fn();
    const cancel = vi.fn(async () => {});
    const fetchMock = vi.fn(async () => ({
      ok: true, status: 200, statusText: 'OK', headers: new Headers(), body: { getReader, cancel }, arrayBuffer,
    }) as unknown as Response);
    vi.stubGlobal('fetch', fetchMock);
    try {
      await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
    } finally {
      vi.unstubAllGlobals();
    }
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(getReader).not.toHaveBeenCalled();
    expect(arrayBuffer).not.toHaveBeenCalled();
    expect(isStale(file)).toBe(true);
  });

  it('retries a slice lost in transit or to a server error, then reads it', async () => {
    vi.useFakeTimers();
    const server = serve(6 * MIB);
    const file = await openRanged();
    server.failures.push(new ApiError('Network Error', 'ERR_NETWORK'), statusError(503));
    const read = file.read(5 * MIB, 5 * MIB + 10);
    await vi.advanceTimersByTimeAsync(2500);
    same(await read, server.bytes.subarray(5 * MIB, 5 * MIB + 10));
  });

  it('gives up on a slice that keeps failing', async () => {
    vi.useFakeTimers();
    const server = serve(6 * MIB);
    const file = await openRanged();
    server.failures.push(statusError(503), statusError(503), statusError(503));
    const read = file.read(5 * MIB, 5 * MIB + 10);
    const outcome = expect(read).rejects.toMatchObject({ status: 503 });
    await vi.advanceTimersByTimeAsync(2500);
    await outcome;
    expect(server.failures).toHaveLength(0);
  });

  it('throws FileChangedError at once for a slice the route found changing as it read', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    const before = server.ranges.length;
    server.failures.push(await refusal(503, FILE_CHANGED));
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toBeInstanceOf(FileChangedError);
    expect(server.ranges.length - before).toBe(1);
    expect(isStale(file)).toBe(true);
  });

  it('reads the file whole when the opening slice finds it changing, without asking again', async () => {
    const server = serve(6 * MIB);
    server.failures.push(await refusal(503, FILE_CHANGED));
    same(await openWorkspaceFileRanged('ws-1', 'a.pdf') as ArrayBuffer, server.bytes);
    expect(server.ranges).toEqual(['bytes=0-4194303', undefined]);
  });

  it('still retries a 503 that is not the file changing', async () => {
    const unavailable = await refusal(503, { detail: 'Sandbox unavailable' });
    vi.useFakeTimers();
    const server = serve(6 * MIB);
    server.failures.push(unavailable);
    const opening = openRanged();
    await vi.advanceTimersByTimeAsync(500);
    const file = await opening;
    expect(file.length).toBe(6 * MIB);
    server.failures.push(unavailable);
    const read = file.read(5 * MIB, 5 * MIB + 10);
    await vi.advanceTimersByTimeAsync(500);
    same(await read, server.bytes.subarray(5 * MIB, 5 * MIB + 10));
    expect(server.ranges.filter((r) => r === `bytes=${5 * MIB}-${5 * MIB + 9}`)).toHaveLength(2);
  });

  it('bounds later slices by silence alone, and the opening and whole downloads not at all', async () => {
    serve(14 * MIB, { cap: 8 * MIB });
    const file = await openRanged();
    await file.read(5 * MIB, 5 * MIB + 65_536);
    await file.read(4 * MIB, 14 * MIB);
    // The opening 4 MiB, a 64 KiB slice, then 10 MiB asked of an answer
    // capped at 8 MiB, and the 2 MiB left.
    const configs = mockGet.mock.calls.map(([, config]) => config as RequestConfig);
    expect(configs.map((config) => config.timeout)).toEqual([undefined, undefined, undefined, undefined]);
    expect(configs.map((config) => config.idleTimeout)).toEqual([undefined, 30_000, 30_000, 30_000]);
    // The opening takes a whole answer as one; only a later slice needs a part.
    expect(configs.map((config) => config.expectStatus)).toEqual([undefined, 206, 206, 206]);

    serve(6 * MIB, { exposeRange: false });
    mockGet.mockClear();
    await openWorkspaceFileRanged('ws-1', 'a.pdf');
    const whole = mockGet.mock.calls.map(([, config]) => config as RequestConfig);
    expect(whole.map((config) => [config.timeout, config.idleTimeout])).toEqual([[undefined, undefined], [undefined, undefined]]);
  });

  it('retries a slice whose answer went silent', async () => {
    const late = await wentSilent();
    expect(late.status).toBeUndefined();
    expect(late.name).not.toBe('CanceledError');
    vi.useFakeTimers();
    const server = serve(6 * MIB);
    const file = await openRanged();
    server.failures.push(late);
    const read = file.read(5 * MIB, 5 * MIB + 10);
    await vi.advanceTimersByTimeAsync(500);
    same(await read, server.bytes.subarray(5 * MIB, 5 * MIB + 10));
  });

  it('does not retry a refusal', async () => {
    const server = serve(6 * MIB);
    const file = await openRanged();
    const before = server.ranges.length;
    server.failures.push(statusError(403));
    await expect(file.read(5 * MIB, 5 * MIB + 10)).rejects.toMatchObject({ status: 403 });
    expect(server.ranges.length - before).toBe(1);
    expect(isStale(file)).toBe(false);
  });
});
