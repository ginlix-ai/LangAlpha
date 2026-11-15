// @vitest-environment node
/**
 * An image or PDF answered from the backup while its computer is away. Bytes
 * have no field for the marks a text read carries, so the download says so by
 * header, and the file panel's body takes the code from there to show the
 * same backup notice a text file gets.
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
import { downloadWorkspaceFileAsArrayBuffer, openWorkspaceFileRanged } from '../files';

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

    expect(body.unreachable).toBe('not_connected');
    expect(mockGet.mock.calls[0][1].params).toMatchObject({ path, backup_ok: true });
  });

  it('is live when the answer carries no code', async () => {
    mockGet.mockResolvedValue(answer({}));

    const body = await readFileBody(readers, 'ws', 'chart.png');

    expect(body.unreachable).toBeUndefined();
  });
});
