// @vitest-environment node
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('@/lib/authToken', () => ({ getAuthHeaders: vi.fn(async () => ({ Authorization: 'Bearer owner-jwt' })) }));

import {
  downloadSharedFile,
  getSharedMetadata,
  servedBytes,
  servedObjectUrl,
  servedReaders,
  sharedServePrefix,
} from '../api';

const fetchMock = vi.fn();

describe('shared byte-access goes through the serve endpoint (allow_files)', () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal('fetch', fetchMock);
    vi.stubGlobal('URL', { ...URL, createObjectURL: vi.fn(() => 'blob:served'), revokeObjectURL: vi.fn() });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // Regression: inline images / previews on a copy-link share (allow_files only)
  // must NOT hit /files/download (allow_download) or they 403.
  it('object-URL fetch hits /files/serve, not /files/download', async () => {
    fetchMock.mockResolvedValue({ ok: true, blob: () => Promise.resolve(new Blob(['x'])) });

    const url = await servedObjectUrl(sharedServePrefix('tok123'), 'results/chart.png');

    const calledWith = String(fetchMock.mock.calls[0][0]);
    expect(calledWith).toContain('/api/v1/public/shared/tok123/files/serve/results/chart.png');
    expect(calledWith).not.toContain('/files/download');
    expect(url).toBe('blob:served');
  });

  it('throws "File access not permitted" on a 403, with the status the panel classifies', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 403 });
    const read = servedObjectUrl(sharedServePrefix('tok'), 'results/x.png');
    await expect(read).rejects.toThrow('File access not permitted');
    await expect(read).rejects.toMatchObject({ response: { status: 403 } });
  });

  it('bytes also come from the serve endpoint', async () => {
    const buf = new ArrayBuffer(8);
    fetchMock.mockResolvedValue({ ok: true, arrayBuffer: () => Promise.resolve(buf) });

    const out = await servedBytes(sharedServePrefix('tok'), 'data/x.bin');

    expect(String(fetchMock.mock.calls[0][0])).toContain('/files/serve/data/x.bin');
    expect(out).toBe(buf);
  });

  it("a shared file's readers read under its frame_base", async () => {
    fetchMock.mockResolvedValue({ ok: true, text: () => Promise.resolve('# Notes') });
    const readers = servedReaders('/api/v1/wsfiles/s/abc/');

    await expect(readers.readFile('', 'notes/my notes.md')).resolves.toEqual({ content: '# Notes' });
    expect(String(fetchMock.mock.calls[0][0])).toMatch(/\/api\/v1\/wsfiles\/s\/abc\/notes\/my%20notes\.md$/);

    fetchMock.mockResolvedValue({ ok: false, status: 404 });
    await expect(readers.readFileFull('', 'gone.md')).rejects.toMatchObject({ response: { status: 404 } });
  });

  it('the explicit save uses the download endpoint, whose 403 names its own permission', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 403 });
    await expect(downloadSharedFile('tok', 'results/x.md')).rejects.toThrow('File download not permitted');
    expect(String(fetchMock.mock.calls[0][0])).toContain('/api/v1/public/shared/tok/files/download?path=results%2Fx.md');
  });
});

describe('getSharedMetadata', () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('reads /public/shared/<code> with the bearer, so the owner of a private link is recognised', async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 200, json: async () => ({ kind: 'file', name: 'r.html', path: 'r.html', access: 'owner', frame_base: '/api/v1/wsfiles/g/x/' }) });

    await expect(getSharedMetadata('k7f2m9q1x4z8')).resolves.toMatchObject({ kind: 'file', access: 'owner' });

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain('/api/v1/public/shared/k7f2m9q1x4z8');
    expect(url).not.toContain('?');
    expect(init.headers).toEqual({ Authorization: 'Bearer owner-jwt' });
  });

  it('asks as a visitor with ?as=visitor', async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 200, json: async () => ({ kind: 'thread' }) });
    await getSharedMetadata('k7f2m9q1x4z8', { asVisitor: true });
    expect(String(fetchMock.mock.calls[0][0])).toContain('/api/v1/public/shared/k7f2m9q1x4z8?as=visitor');
  });

  it('forwards the page an old preview URL named as ?path=', async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 200, json: async () => ({ kind: 'app' }) });
    await getSharedMetadata('k7f2m9q1x4z8', { path: 'reports/q3.html' });
    expect(String(fetchMock.mock.calls[0][0])).toContain('/api/v1/public/shared/k7f2m9q1x4z8?path=reports%2Fq3.html');
  });

  it('an answer without a kind predates file links and is a chat', async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 200, json: async () => ({ thread_id: 't1', title: 'Old' }) });
    await expect(getSharedMetadata('k7f2m9q1x4z8')).resolves.toMatchObject({ kind: 'thread', thread_id: 't1' });
  });

  it('a 404 carries its status on response.status, where the page reads it', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 404 });
    await expect(getSharedMetadata('nope')).rejects.toMatchObject({ response: { status: 404 } });
  });
});
