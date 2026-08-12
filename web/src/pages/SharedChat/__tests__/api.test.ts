import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { fetchSharedServeObjectUrl, fetchSharedServeArrayBuffer } from '../api';

const fetchMock = vi.fn();

describe('shared byte-access goes through the serve endpoint (allow_files)', () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal('fetch', fetchMock);
    vi.stubGlobal('URL', { ...URL, createObjectURL: vi.fn(() => 'blob:served') });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // Regression: inline images / previews on a copy-link share (allow_files only)
  // must NOT hit /files/download (allow_download) or they 403.
  it('object-URL fetch hits /files/serve, not /files/download', async () => {
    fetchMock.mockResolvedValue({ ok: true, blob: () => Promise.resolve(new Blob(['x'])) });

    const url = await fetchSharedServeObjectUrl('tok123', 'results/chart.png');

    const calledWith = String(fetchMock.mock.calls[0][0]);
    expect(calledWith).toContain('/api/v1/public/shared/tok123/files/serve/results/chart.png');
    expect(calledWith).not.toContain('/files/download');
    expect(url).toBe('blob:served');
  });

  it('throws "File access not permitted" on a 403 (allow_files not granted)', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 403 });
    await expect(fetchSharedServeObjectUrl('tok', 'results/x.png')).rejects.toThrow(
      'File access not permitted',
    );
  });

  it('arraybuffer variant also uses the serve endpoint and returns raw bytes', async () => {
    const buf = new ArrayBuffer(8);
    fetchMock.mockResolvedValue({ ok: true, arrayBuffer: () => Promise.resolve(buf) });

    const out = await fetchSharedServeArrayBuffer('tok', 'data/x.bin');

    expect(String(fetchMock.mock.calls[0][0])).toContain('/files/serve/data/x.bin');
    expect(out).toBe(buf);
  });
});
