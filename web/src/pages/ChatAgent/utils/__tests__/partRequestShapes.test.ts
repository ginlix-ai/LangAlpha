/**
 * The part requests a chat attachment and a workspace upload make, pinned:
 * the signing body names part numbers only, and each signed PUT carries no
 * header the store did not sign. Only the Drive binds a part to its MD5, so a
 * bucket whose CORS does not list `Content-MD5` refuses no preflight here.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
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
  return { ...real, api: { post: vi.fn(), get: vi.fn(), delete: vi.fn(), putSigned: vi.fn() } };
});

import { api } from '@/api/client';
import { uploadAttachment } from '../attachmentUpload';
import { uploadToWorkspace } from '../workspaceUpload';

const post = api.post as Mock;
const get = api.get as Mock;
const put = api.putSigned as Mock;

const PART = 16;
const file = new File([new Uint8Array(40)], 'big.bin');

/** Signs as the server does for these callers: no `headers` in the answer, since no digest was asked for. */
function signed(numbers: number[]) {
  return {
    expires_in: 900,
    parts: numbers.map((n) => ({ part_number: n, offset: (n - 1) * PART, size: n < 3 ? PART : 8, url: `https://store.test/${n}` })),
  };
}

const signBodies = () => post.mock.calls.filter(([url]) => String(url).endsWith('/parts')).map(([, body]) => body);

beforeEach(() => {
  vi.resetAllMocks();
  put.mockResolvedValue({ status: 200 });
});

describe('part requests outside the Drive', () => {
  it('a chat attachment signs by part number and puts each part with no header of its own', async () => {
    post.mockImplementation(async (url: string, body: { part_numbers?: number[] }) => {
      if (url === '/api/v1/attachments') return { data: { attachment_id: 'a1', mode: 'direct', part_count: 3, part_size: PART } };
      if (url.endsWith('/parts')) return { data: signed(body.part_numbers!) };
      return { data: { attachment_id: 'a1', state: 'ready' } };
    });
    expect(await uploadAttachment(file)).toBe('a1');
    expect(signBodies()).toEqual([{ part_numbers: [1, 2, 3] }]);
    expect(put).toHaveBeenCalledTimes(3);
    for (const [, , config] of put.mock.calls) expect(config.headers).toBeUndefined();
  });

  it('a workspace upload signs by part number and puts each part with no header of its own', async () => {
    post.mockImplementation(async (url: string, body: { part_numbers?: number[] }) => {
      if (url.endsWith('/uploads')) {
        return { data: { mode: 'direct', upload_id: 'u1', path: 'big.bin', part_size: PART, part_count: 3, expires_at: '' } };
      }
      if (url.endsWith('/parts')) return { data: signed(body.part_numbers!) };
      return { data: { upload_id: 'u1', state: 'done', path: 'big.bin', size: 40, error: null } };
    });
    get.mockResolvedValue({ data: { upload_id: 'u1', state: 'done', path: 'big.bin', size: 40, error: null } });
    await uploadToWorkspace('ws-1', file);
    expect(signBodies()).toEqual([{ part_numbers: [1, 2, 3] }]);
    expect(put).toHaveBeenCalledTimes(3);
    for (const [, , config] of put.mock.calls) expect(config.headers).toBeUndefined();
  });
});
