// @vitest-environment node
/**
 * A file answered from the backup in place of its computer. A text read says
 * so in its body, and bytes, which have no field for it, by header; both come
 * out of the API as the same `from`, which the file panel's notice reads.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
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

import { api } from '@/api/client';
import { readFileBody } from '../../../components/filePanel/fileBody';
import { downloadWorkspaceFileAsArrayBuffer, openWorkspaceFileRanged, readWorkspaceFileFull } from '../files';

const mockGet = api.get as unknown as Mock;

function answer(headers: Record<string, string>, status = 200) {
  return { status, data: new Uint8Array([1, 2, 3]).buffer, headers };
}

const readers = {
  readFile: vi.fn(),
  readFileFull: vi.fn(),
  downloadFileAsArrayBuffer: downloadWorkspaceFileAsArrayBuffer,
  openRanged: openWorkspaceFileRanged,
};

beforeEach(() => { mockGet.mockReset(); });

describe('a binary file answered from the backup', () => {
  it.each([
    ['an image', 'chart.png'],
    ['a PDF opened by range', 'report.pdf'],
  ])('carries the computer\'s code on %s, and asks to be answered from the backup', async (_label, path) => {
    mockGet.mockResolvedValue(answer({ 'x-computer-unreachable': 'not_connected' }));

    const body = await readFileBody(readers, 'ws', path);

    expect(body.from).toEqual({ kind: 'backup', unreachable: 'not_connected' });
    expect(mockGet.mock.calls[0][1].params).toMatchObject({ path, backup_ok: true });
  });

  it('is live when the answer carries no code', async () => {
    mockGet.mockResolvedValue(answer({}));

    const body = await readFileBody(readers, 'ws', 'chart.png');

    expect(body.from).toEqual({ kind: 'live' });
  });
});

describe('a text read answered from the backup', () => {
  it.each([
    ['an away computer\'s', { source: 'database', unreachable: 'timeout' }, { kind: 'backup', unreachable: 'timeout' }],
    ['a stopped workspace\'s', { source: 'database' }, { kind: 'backup', unreachable: null }],
    ['no', { source: 'sandbox', unreachable: 'timeout' }, { kind: 'live' }],
  ])('reads as %s backup', async (_label, marks, from) => {
    mockGet.mockResolvedValue({ status: 200, data: { content: 'x', version: null, ...marks }, headers: {} });

    expect((await readWorkspaceFileFull('ws', 'a.md')).from).toEqual(from);
  });

  it('says a live read that names no version caught the file mid-rewrite, unless the server names none at all', async () => {
    mockGet.mockResolvedValue({ status: 200, data: { content: 'x', version: null }, headers: {} });
    expect(await readWorkspaceFileFull('ws', 'a.md')).toMatchObject({ version: null, rewriting: true });

    mockGet.mockResolvedValue({ status: 200, data: { content: 'x' }, headers: {} });
    expect(await readWorkspaceFileFull('ws', 'a.md')).toMatchObject({ version: null, rewriting: false });
  });
});
