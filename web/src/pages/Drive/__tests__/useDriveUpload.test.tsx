import { act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { getSpaces, listVersions, makeFolder } from '@/api/drive';
import { DriveUploadFailed } from '@/api/driveUploads';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import { toast } from '@/components/ui/use-toast';
import { renderHookWithProviders } from '@/test/utils';
import { driveUploads } from '../driveUploadStore';
import { useDriveUpload } from '../useDriveUpload';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  makeFolder: vi.fn(),
  listVersions: vi.fn(),
  getSpaces: vi.fn(),
}));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

beforeEach(() => {
  vi.mocked(makeFolder).mockReset();
  vi.mocked(toast).mockReset();
  // The files' own sends are the store's concern.
  vi.spyOn(driveUploads, 'add').mockImplementation(() => {});
});

describe('useDriveUpload: a dropped tree', () => {
  it('says once when an empty folder in it could not be made, there being no file to carry the failure', async () => {
    vi.mocked(makeFolder).mockRejectedValue(new Error('Network Error'));
    const { result } = renderHookWithProviders(() => useDriveUpload({ space: 'personal', usage: null }));
    await act(() => result.current.upload([], 'in', ['a', 'b']));
    expect(toast).toHaveBeenCalledTimes(1);
    expect(toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive', title: 'Could not create the folder' }));
  });

  it('leaves a folder that holds files to fail with its files', async () => {
    vi.mocked(makeFolder).mockRejectedValue(new Error('Network Error'));
    const { result } = renderHookWithProviders(() => useDriveUpload({ space: 'personal', usage: null }));
    const file = new File(['x'], 'x.md');
    await act(() => result.current.upload([{ file, relPath: 'a/x.md' }], 'in', ['a']));
    expect(toast).not.toHaveBeenCalled();
  });
});

describe('useDriveUpload: replacing a file too large to keep a copy of', () => {
  it('finds the file it replaces under another case, as the server does, so its bytes leave the counts', async () => {
    const MB = 1024 ** 2;
    const usage = { bytes: 90 * MB, entries: 1, max_bytes: 100 * MB, max_entries: 100, max_file_bytes: 200 * MB, max_upload_bytes: 200 * MB };
    const existing = {
      id: 'e1', path: 'in/Data.parquet', name: 'Data.parquet', kind: 'file', size: 80 * MB, sha256: 'a'.repeat(64), content_type: null,
      is_text: false, version: 1, created_at: '', modified_at: '', modified_by: null, deleted_at: null, memo: null, note: null, undo_until: null,
    };
    vi.mocked(listVersions).mockResolvedValue({ versions: [], history_bytes: 0, too_large_for_history: true } as never);
    const { result, queryClient } = renderHookWithProviders(() => useDriveUpload({ space: 'personal', usage }));
    queryClient.setQueryData(queryKeys.drive.list('personal', 'in'), [existing]);
    const file = new File([new Uint8Array(1)], 'data.parquet');
    Object.defineProperty(file, 'size', { value: 50 * MB });
    await act(() => result.current.upload([{ file, relPath: 'data.parquet' }], 'in'));
    // 90 MB used, 80 MB of it freed by the replacement: 50 MB fits.
    expect(driveUploads.add).toHaveBeenCalledWith([expect.objectContaining({ path: 'in/data.parquet', failure: undefined, replacesUnkept: true })]);
  });
});

describe('useDriveUpload: Retry on a file refused for the files before it', () => {
  const MB = 1024 ** 2;
  const usage = (bytes: number) => ({ bytes, entries: 1, max_bytes: 100 * MB, max_entries: 100, max_file_bytes: 200 * MB, max_upload_bytes: 200 * MB });
  const space = (bytes: number) => [{ id: 'sp-1', kind: 'personal', created_at: '', change_seq: 0, trash_days: 14, usage: usage(bytes) }];

  /** The second of two 40 MB files with 50 MB free at selection: refused for the first. */
  async function refusedBehind() {
    runAuthResets();
    vi.mocked(driveUploads.add).mockRestore();
    const recheck = vi.spyOn(driveUploads, 'recheck').mockImplementation(() => {});
    recheck.mockClear();
    const { result } = renderHookWithProviders(() => useDriveUpload({ space: 'personal', usage: usage(50 * MB) }));
    const file = new File([new Uint8Array(1)], 'b.bin');
    Object.defineProperty(file, 'size', { value: 40 * MB });
    driveUploads.add([{ space: 'personal', name: 'b.bin', path: 'in/b.bin', file, handle: null, replacesUnkept: false,
      failure: { error: new DriveUploadFailed('over_quota', ''), usage: usage(90 * MB), recheck: true } }]);
    const row = driveUploads.snapshot().find((i) => i.name === 'b.bin')!;
    return { result, recheck, id: row.id };
  }

  it('sends it once the space, read again, has room: the file before it failed', async () => {
    vi.mocked(getSpaces).mockResolvedValue(space(50 * MB) as never);
    const { result, recheck, id } = await refusedBehind();
    await act(() => result.current.retry(id));
    expect(getSpaces).toHaveBeenCalled();
    expect(recheck).toHaveBeenCalledWith(id, undefined);
  });

  it('refuses it again, for good, when the file before it landed', async () => {
    vi.mocked(getSpaces).mockResolvedValue(space(90 * MB) as never);
    const { result, recheck, id } = await refusedBehind();
    await act(() => result.current.retry(id));
    expect(recheck).toHaveBeenCalledWith(id, expect.objectContaining({ error: expect.objectContaining({ type: 'over_quota' }) }));
    expect(recheck.mock.calls[0][1]).not.toHaveProperty('recheck');
  });
});
