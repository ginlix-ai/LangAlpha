/**
 * How a chat attachment and a workspace upload end on each failure the
 * backend or the store can give, pinned to what they did before the part
 * sender was shared with the Drive: what the caller sees, and which requests
 * went (signs, PUTs, completes, cancels). The Drive's own reading of an
 * answer (a full space is final) must not reach these two.
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
  return { ...real, api: { post: vi.fn(), get: vi.fn(), delete: vi.fn(), put: vi.fn(), putSigned: vi.fn() } };
});

import { api, ApiError } from '@/api/client';
import { uploadAttachment } from '../attachmentUpload';
import { uploadToWorkspace } from '../workspaceUpload';

const post = api.post as Mock;
const get = api.get as Mock;
const del = api.delete as Mock;
const put = api.putSigned as Mock;

const PART = 16;
const file = new File([new Uint8Array(40)], 'big.bin');

const answer = (status: number, type = 'x') =>
  new ApiError(`Request failed with status code ${status}`, status < 500 ? 'ERR_BAD_REQUEST' : 'ERR_BAD_RESPONSE', {
    data: { detail: { type, message: type } }, status, statusText: '', headers: {},
  });
const noAnswer = () => new ApiError('Network Error', 'ERR_NETWORK');

type Step = 'begin' | 'sign' | 'put' | 'complete';
/** Fails `step` with `error` on its first `times` calls (every call when absent). */
interface Fault {
  step: Step;
  error: () => unknown;
  times?: number;
}

const signed = (numbers: number[]) => ({
  expires_in: 900,
  parts: numbers.map((n) => ({ part_number: n, offset: (n - 1) * PART, size: n < 3 ? PART : 8, url: `https://store.test/${n}` })),
});

function serve(caller: 'attachment' | 'workspace', fault: Fault | null, abort?: AbortController) {
  const seen: Record<Step, number> = { begin: 0, sign: 0, put: 0, complete: 0 };
  const hit = (step: Step) => {
    seen[step] += 1;
    if (fault?.step === step && (fault.times === undefined || seen[step] <= fault.times)) throw fault.error();
  };
  post.mockImplementation(async (url: string, body: { part_numbers?: number[] }) => {
    if (url.endsWith('/parts')) {
      hit('sign');
      return { data: signed(body.part_numbers!) };
    }
    if (url.endsWith('/complete')) {
      hit('complete');
      return caller === 'attachment'
        ? { data: { attachment_id: 'a1', state: 'ready' } }
        : { data: { upload_id: 'u1', state: 'done', path: 'big.bin', size: 40, error: null } };
    }
    hit('begin');
    return caller === 'attachment'
      ? { data: { attachment_id: 'a1', mode: 'direct', part_count: 3, part_size: PART } }
      : { data: { mode: 'direct', upload_id: 'u1', path: 'big.bin', part_size: PART, part_count: 3, expires_at: '' } };
  });
  get.mockResolvedValue({ data: { upload_id: 'u1', state: 'done', path: 'big.bin', size: 40, error: null } });
  del.mockResolvedValue({ data: null });
  put.mockImplementation(async (_url: string, _body: Blob, config: { signal?: AbortSignal }) => {
    hit('put');
    if (!abort) return { status: 200 };
    // The person cancels while the first part is on its way, which hangs until the cancel stops it.
    if (seen.put === 1) setTimeout(() => abort.abort(), 10);
    return new Promise((_resolve, reject) => {
      config.signal?.addEventListener('abort', () => reject(new ApiError('canceled', 'ERR_CANCELED')));
    });
  });
  return seen;
}

async function outcome(caller: 'attachment' | 'workspace', fault: Fault | null, aborting = false) {
  const abort = aborting ? new AbortController() : undefined;
  const seen = serve(caller, fault, abort);
  const sent = caller === 'attachment'
    ? uploadAttachment(file, { signal: abort?.signal })
    : uploadToWorkspace('ws-1', file, { signal: abort?.signal });
  const settled = sent.then(() => ({ ok: true as const }), (e: unknown) => ({ ok: false as const, e }));
  await vi.runAllTimersAsync();
  const out = await settled;
  const error = out.ok ? null : out.e instanceof ApiError ? `${out.e.name}:${out.e.status ?? out.e.code}` : (out.e as Error).name;
  return { error, ...seen, cancels: del.mock.calls.length };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

const FAULTS: Record<string, Fault | null> = {
  none: null,
  'begin 507': { step: 'begin', error: () => answer(507, 'over_quota') },
  'sign 401': { step: 'sign', error: () => answer(401) },
  'sign 403': { step: 'sign', error: () => answer(403) },
  'sign 503 once': { step: 'sign', error: () => answer(503), times: 1 },
  'sign 507': { step: 'sign', error: () => answer(507) },
  'put 403 once': { step: 'put', error: () => answer(403), times: 1 },
  'put 500 always': { step: 'put', error: () => answer(500) },
  'put no answer': { step: 'put', error: noAnswer },
  'complete 401': { step: 'complete', error: () => answer(401) },
  'complete 503 once': { step: 'complete', error: () => answer(503), times: 1 },
  'complete 507': { step: 'complete', error: () => answer(507, 'disk_full') },
  'complete no answer': { step: 'complete', error: noAnswer },
};

/** What each caller did on main, for each fault, and for a cancel mid-send. */
const MAIN: Record<'attachment' | 'workspace', Record<string, unknown>> = {
  attachment: {
    'none': { error: null, begin: 1, sign: 1, put: 3, complete: 1, cancels: 0 },
    'begin 507': { error: 'ApiError:507', begin: 1, sign: 0, put: 0, complete: 0, cancels: 0 },
    'sign 401': { error: 'ApiError:401', begin: 1, sign: 1, put: 0, complete: 0, cancels: 1 },
    'sign 403': { error: 'ApiError:403', begin: 1, sign: 1, put: 0, complete: 0, cancels: 1 },
    'sign 503 once': { error: null, begin: 1, sign: 4, put: 3, complete: 1, cancels: 0 },
    'sign 507': { error: 'UploadSendError', begin: 1, sign: 11, put: 0, complete: 0, cancels: 1 },
    'put 403 once': { error: null, begin: 1, sign: 2, put: 4, complete: 1, cancels: 0 },
    'put 500 always': { error: 'UploadSendError', begin: 1, sign: 11, put: 13, complete: 0, cancels: 1 },
    'put no answer': { error: 'UploadSendError', begin: 1, sign: 11, put: 13, complete: 0, cancels: 1 },
    'complete 401': { error: 'ApiError:401', begin: 1, sign: 1, put: 3, complete: 1, cancels: 1 },
    'complete 503 once': { error: null, begin: 1, sign: 1, put: 3, complete: 2, cancels: 0 },
    'complete 507': { error: 'ApiError:507', begin: 1, sign: 1, put: 3, complete: 3, cancels: 1 },
    'complete no answer': { error: 'ApiError:ERR_NETWORK', begin: 1, sign: 1, put: 3, complete: 3, cancels: 1 },
    'cancel': { error: 'CanceledError:ERR_CANCELED', begin: 1, sign: 1, put: 3, complete: 0, cancels: 1 },
  },
  workspace: {
    'none': { error: null, begin: 1, sign: 1, put: 3, complete: 1, cancels: 0 },
    'begin 507': { error: 'ApiError:507', begin: 1, sign: 0, put: 0, complete: 0, cancels: 0 },
    'sign 401': { error: 'ApiError:401', begin: 1, sign: 1, put: 0, complete: 0, cancels: 1 },
    'sign 403': { error: 'ApiError:403', begin: 1, sign: 1, put: 0, complete: 0, cancels: 1 },
    'sign 503 once': { error: null, begin: 1, sign: 4, put: 3, complete: 1, cancels: 0 },
    'sign 507': { error: 'UploadSendError', begin: 1, sign: 11, put: 0, complete: 0, cancels: 1 },
    'put 403 once': { error: null, begin: 1, sign: 2, put: 4, complete: 1, cancels: 0 },
    'put 500 always': { error: 'UploadSendError', begin: 1, sign: 11, put: 13, complete: 0, cancels: 1 },
    'put no answer': { error: 'UploadSendError', begin: 1, sign: 11, put: 13, complete: 0, cancels: 1 },
    'complete 401': { error: 'ApiError:401', begin: 1, sign: 1, put: 3, complete: 1, cancels: 1 },
    'complete 503 once': { error: null, begin: 1, sign: 1, put: 3, complete: 2, cancels: 0 },
    'complete 507': { error: 'ApiError:507', begin: 1, sign: 1, put: 3, complete: 3, cancels: 1 },
    'complete no answer': { error: 'ApiError:ERR_NETWORK', begin: 1, sign: 1, put: 3, complete: 3, cancels: 1 },
    'cancel': { error: 'CanceledError:ERR_CANCELED', begin: 1, sign: 1, put: 3, complete: 0, cancels: 1 },
  },
};

describe.each(['attachment', 'workspace'] as const)('a %s upload, on every failure', (caller) => {
  it.each(Object.keys(FAULTS))('%s', async (name) => {
    const got = await outcome(caller, FAULTS[name]);
    expect(got).toEqual(MAIN[caller][name]);
  });

  it('cancelled mid-send', async () => {
    const got = await outcome(caller, null, true);
    expect(got).toEqual(MAIN[caller].cancel);
  });
});
