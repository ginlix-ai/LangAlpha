import i18n from 'i18next';
import { describe, expect, it } from 'vitest';
import { DriveUploadFailed } from '@/api/driveUploads';
import type { DriveUsage } from '@/types/drive';
import { driveRefusal } from '../driveRefusal';
import { refusedUpFront } from '../useDriveUpload';

const GB = 1024 ** 3;
const usage = (over: Partial<DriveUsage> = {}): DriveUsage => ({
  bytes: 3 * GB, entries: 10, max_bytes: 5 * GB, max_entries: 1000, max_file_bytes: 50 * 1024 ** 2, max_upload_bytes: 5 * GB, ...over,
});

const ctx = (u: DriveUsage) => ({ subject: 'big.bin', failed: 'Upload failed', locale: 'en-US', usage: u });

describe('refusedUpFront', () => {
  it('refuses a file past what is left, counting the files before it, and lets the rest go', () => {
    expect(refusedUpFront([1.5 * GB, 1 * GB, 0.4 * GB], usage())).toEqual([
      undefined,
      // Judged against the room left once the first file is counted; alone it fits, so it is judged again on Retry.
      { error: new DriveUploadFailed('over_quota', ''), usage: usage({ bytes: 4.5 * GB }), recheck: true },
      undefined,
    ]);
  });

  it('makes final a file that does not fit on its own, and judges again one refused only for the files before it', () => {
    const [alone] = refusedUpFront([2.5 * GB], usage());
    expect(alone).toEqual({ error: new DriveUploadFailed('over_quota', ''), usage: usage() });
    const [, behind] = refusedUpFront([1.5 * GB, 1 * GB], usage());
    expect(behind?.recheck).toBe(true);
  });

  it('counts the bytes a replaced file not kept as a version frees, as the server does', () => {
    // 3 GB used of 5: a 2.5 GB file over a 1 GB one that is too large to keep ends at 4.5 GB.
    expect(refusedUpFront([2.5 * GB], usage(), [1 * GB])).toEqual([undefined]);
    // The room it leaves is what the next file in the drop is judged against.
    expect(refusedUpFront([2.5 * GB, 1 * GB], usage(), [1 * GB, 0])[1]).toEqual(
      { error: new DriveUploadFailed('over_quota', ''), usage: usage({ bytes: 4.5 * GB }), recheck: true },
    );
  });

  it('measures a file against the upload limit, not what one request carries', () => {
    expect(refusedUpFront([100 * 1024 ** 2], usage({ bytes: 0 }))).toEqual([undefined]);
    expect(refusedUpFront([2 * GB], usage({ bytes: 0, max_upload_bytes: GB }))[0]?.error).toEqual(new DriveUploadFailed('too_large', ''));
  });

  it('falls back to the per-request limit where the server names no upload limit', () => {
    expect(driveRefusal(new DriveUploadFailed('too_large', ''), i18n.t, ctx(usage({ bytes: 0, max_upload_bytes: undefined }))).toast.title)
      .toBe('Larger than the 50 MB file limit.');
  });

  it('words a file too large the same whether the page or the server caught it', () => {
    const u = usage({ max_upload_bytes: GB });
    const [page] = refusedUpFront([2 * GB], u);
    const server = Object.assign(new Error('Request failed with status code 413'), {
      name: 'ApiError', status: 413, response: { status: 413, data: { detail: { type: 'too_large', message: 'Too large' } } },
    });
    const said = driveRefusal(page!.error, i18n.t, ctx(page!.usage!)).toast.title;
    expect(said).toBe('Larger than the 1 GB file limit.');
    expect(driveRefusal(server, i18n.t, ctx(u)).toast.title).toBe(said);
  });

  it('names no Drive limit for a 413 from something in front of the server, which may refuse less', () => {
    const proxy = Object.assign(new Error('Request failed with status code 413'), {
      name: 'ApiError', status: 413, response: { status: 413, data: '<html>Request Entity Too Large</html>' },
    });
    expect(driveRefusal(proxy, i18n.t, ctx(usage())).toast.title).toBe('Too large to upload.');
  });

  it('says how much room is left when there is not enough', () => {
    const [, second] = refusedUpFront([1.5 * GB, 1 * GB], usage());
    expect(driveRefusal(second!.error, i18n.t, { ...ctx(second!.usage!), size: GB }).toast.title)
      .toBe('big.bin needs 1 GB, and only 512 MB of your 5 GB is free. Delete files to make room.');
  });
});
