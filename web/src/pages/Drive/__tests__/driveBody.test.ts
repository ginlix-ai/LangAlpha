import { waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readBytes, readText } from '@/api/drive';
import { openRangedRead } from '@/lib/rangedRead';
import { renderHookWithProviders } from '@/test/utils';
import type { DriveEntry } from '@/types/drive';
import { driveBodyKey, PREVIEW_MAX_BYTES, readDriveBody, useDriveBody } from '../driveBody';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  readText: vi.fn(),
  readBytes: vi.fn(),
}));
vi.mock('@/lib/rangedRead', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/rangedRead')>()),
  openRangedRead: vi.fn(),
}));

function file(name: string, size: number, version = 3): DriveEntry {
  return {
    id: 'e1', path: name, name, kind: 'file', size, sha256: 'a'.repeat(64), content_type: null, is_text: name.endsWith('.md'),
    version, created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null,
  };
}

beforeEach(() => {
  vi.mocked(readText).mockReset();
  vi.mocked(readBytes).mockReset();
  vi.mocked(openRangedRead).mockReset();
});

describe('driveBodyKey', () => {
  it('keeps the bytes of a file renamed to another kind its own reader reads apart', () => {
    // A PDF and a workbook both read as a buffer, but each opens in its own viewer.
    const sha = 'a'.repeat(64);
    const pdf = driveBodyKey('s', { id: 'e1', name: 'r.pdf', is_text: false, size: 10 }, sha);
    const xlsx = driveBodyKey('s', { id: 'e1', name: 'r.xlsx', is_text: false, size: 10 }, sha);
    expect(pdf).not.toEqual(xlsx);
  });
});

describe('useDriveBody: the head', () => {
  it('reads the version its key names, not whatever the head is by the time the read lands', async () => {
    vi.mocked(readText).mockResolvedValue({ content: 'x', sha: 'a'.repeat(64) });
    const { result } = renderHookWithProviders(() => useDriveBody('s', file('n.md', 1, 7), null));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(readText).toHaveBeenCalledWith('s', 'e1', 7);
  });
});

describe('readDriveBody: a PDF past the preview bound', () => {
  it('opens as a download card when it cannot be read by range', async () => {
    vi.mocked(openRangedRead).mockResolvedValue({ kind: 'whole' });
    const body = await readDriveBody('s', file('big.pdf', PREVIEW_MAX_BYTES + 1), 3);
    expect(body.mode).toBe('none');
    expect(readBytes).not.toHaveBeenCalled();
  });

  it('still reads a small one whole', async () => {
    vi.mocked(openRangedRead).mockResolvedValue({ kind: 'whole' });
    vi.mocked(readBytes).mockResolvedValue({ buffer: new ArrayBuffer(4), sha: null });
    const body = await readDriveBody('s', file('small.pdf', 4), 3);
    expect(body.mode).toBe('buffer');
    expect(readBytes).toHaveBeenCalled();
  });
});
