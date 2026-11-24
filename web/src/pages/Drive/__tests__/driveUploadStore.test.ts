/**
 * Uploads that outlive their page: what a remembered upload becomes on the
 * next page by the server's answer, and that continuing one takes only the
 * file it was sent from, whether the person picks it or drops it again.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/driveUploads', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/driveUploads')>()),
  getDriveUpload: vi.fn(),
  cancelDriveUpload: vi.fn(async () => undefined),
}));
vi.mock('../driveUpload', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../driveUpload')>()),
  sendToDrive: vi.fn(),
}));
vi.mock('@/lib/fileHandles', () => ({
  keepHandle: vi.fn(async () => undefined),
  loadHandle: vi.fn(async () => null),
  forgetHandle: vi.fn(async () => undefined),
  readHandle: vi.fn(async () => null),
}));

import { cancelDriveUpload, DriveUploadFailed, getDriveUpload } from '@/api/driveUploads';
import { UploadSendError } from '@/lib/partUpload';
import { runAuthResets } from '@/lib/authResets';
import { userLocalStorage } from '@/lib/userStorage';
import { FileNeeded, sendToDrive } from '../driveUpload';
import { canRetry, driveUploads } from '../driveUploadStore';
import { fingerprint, rememberedUploads, rememberUpload, type RememberedUpload } from '../rememberedUploads';

const status = getDriveUpload as Mock;
const send = sendToDrive as Mock;

const file = new File([new Uint8Array(40).fill(7)], 'prices.parquet', { lastModified: 1000 });

async function record(id: string, over: Partial<RememberedUpload> = {}): Promise<RememberedUpload> {
  return {
    space: 'personal', uploadId: id, path: `data/${id}.parquet`, name: file.name, size: file.size,
    lastModified: file.lastModified, fingerprint: await fingerprint(file), ...over,
  };
}

/** The store as a new page finds it: no rows, nothing restored yet. */
async function freshStore() {
  runAuthResets();
  // The sends the last page left settle and give their turns back.
  await settle();
  return driveUploads;
}

const settle = () => new Promise((r) => setTimeout(r, 0));

beforeEach(() => {
  vi.resetAllMocks();
  localStorage.clear();
  // Sends hang until cancelled, as one does whose page signs out.
  send.mockImplementation((_s: string, _p: string, _f: File | null, { signal }: { signal: AbortSignal }) =>
    new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')))));
});

describe('remembered Drive uploads', () => {
  it('shows each by what the server says of it, and forgets the ones that are over', async () => {
    for (const id of ['open', 'checking', 'landed', 'gone']) rememberUpload(await record(id));
    status.mockImplementation(async (_space: string, id: string) => {
      if (id === 'gone') throw Object.assign(new Error('nf'), { response: { status: 404 } });
      return {
        open: { state: 'uploading', parts_held: [{ part_number: 1, size: 16 }] },
        checking: { state: 'verifying' },
        landed: { state: 'done', entry: {} },
      }[id];
    });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(3));
    const byPath = Object.fromEntries(store.snapshot().map((i) => [i.path, i]));
    expect(byPath['data/open.parquet']).toMatchObject({ status: 'paused', loaded: 16 });
    // Followed to its end without its file.
    expect(byPath['data/checking.parquet'].status).toBe('sending');
    expect(send).toHaveBeenCalledWith('personal', 'data/checking.parquet', null, expect.objectContaining({ uploadId: 'checking' }));
    expect(byPath['data/landed.parquet'].status).toBe('done');
    expect(rememberedUploads().map((r) => r.uploadId).sort()).toEqual(['checking', 'open']);
  });

  it('refuses a different file to continue one, and keeps it paused', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(1));
    const [row] = store.snapshot();
    const other = new File([new Uint8Array(40).fill(8)], 'prices.parquet', { lastModified: 1000 });
    expect(await store.resume(row.id, { file: other })).toBe('refused');
    expect(store.snapshot()[0]).toMatchObject({ status: 'paused', wrongFile: 'prices.parquet' });
    expect(send).not.toHaveBeenCalled();
    expect(await store.resume(row.id)).toBe('pick');
    expect(await store.resume(row.id, { file })).toBe('sent');
    await vi.waitFor(() => expect(send).toHaveBeenCalledWith('personal', 'data/open.parquet', file, expect.objectContaining({ uploadId: 'open' })));
  });

  it('continues a paused upload when its file is dropped on the same folder again, never a second one', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(1));
    store.add([{ space: 'personal', name: file.name, path: 'data/open.parquet', file, handle: null, replacesUnkept: false }]);
    await vi.waitFor(() => expect(send).toHaveBeenCalled());
    expect(store.snapshot()).toHaveLength(1);
    expect(send).toHaveBeenCalledWith('personal', 'data/open.parquet', file, expect.objectContaining({ uploadId: 'open' }));
  });

  it('starts a new upload for a different file dropped under a paused one\'s name, and keeps that one paused', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(1));
    // Same name, size and time; other bytes.
    const other = new File([new Uint8Array(40).fill(8)], 'prices.parquet', { lastModified: 1000 });
    store.add([{ space: 'personal', name: other.name, path: 'data/open.parquet', file: other, handle: null, replacesUnkept: false }]);
    await vi.waitFor(() => expect(send).toHaveBeenCalled());
    expect(send).toHaveBeenCalledWith('personal', 'data/open.parquet', other, expect.objectContaining({ uploadId: null }));
    const rows = store.snapshot();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ status: 'paused', wrongFile: 'prices.parquet' });
  });

  it('a retried upload cancelled while it waits its turn ends on the server and is not remembered', async () => {
    const store = await freshStore();
    send.mockImplementationOnce(async (_s: string, _p: string, _f: File, { onOpen }: { onOpen: (id: string) => void }) => {
      onOpen('u1');
      await vi.waitFor(() => expect(rememberedUploads()).toHaveLength(1));
      throw new UploadSendError();
    });
    store.add([{ space: 'personal', name: file.name, path: 'data/a.parquet', file, handle: null, replacesUnkept: false }]);
    await vi.waitFor(() => expect(store.snapshot()[0].status).toBe('failed'));
    expect(rememberedUploads().map((r) => r.uploadId)).toEqual(['u1']);
    // Three others take every turn, so the retry waits.
    store.add(['b', 'c', 'd'].map((n) => ({ space: 'personal', name: n, path: `data/${n}`, file, handle: null, replacesUnkept: false })));
    const id = store.snapshot()[0].id;
    store.retry(id);
    expect(store.snapshot().find((i) => i.id === id)?.status).toBe('queued');
    store.cancel(id);
    await settle();
    expect(store.snapshot().find((i) => i.id === id)?.status).toBe('canceled');
    expect(cancelDriveUpload).toHaveBeenCalledWith('personal', 'u1');
    // The next page finds nothing to bring back.
    expect(rememberedUploads()).toEqual([]);
  });

  it('discarding ends the upload on the server and forgets it', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(1));
    store.discard(store.snapshot()[0].id);
    await settle();
    expect(cancelDriveUpload).toHaveBeenCalledWith('personal', 'open');
    expect(store.snapshot()).toHaveLength(0);
    expect(rememberedUploads()).toEqual([]);
  });

  it('keeps a remembered upload it could not ask about, for the next page', async () => {
    rememberUpload(await record('open'));
    status.mockRejectedValue(new Error('Network Error'));
    const store = await freshStore();
    store.restore();
    await settle();
    await settle();
    expect(store.snapshot()).toHaveLength(0);
    expect(rememberedUploads().map((r) => r.uploadId)).toEqual(['open']);
  });

  it('drops a damaged record and keeps the rest', () => {
    userLocalStorage.setItem('drive.openUploads', JSON.stringify([{ uploadId: 1 }, { space: 's', uploadId: 'u', path: 'p', name: 'n', size: 1, lastModified: 1, fingerprint: '' }]));
    expect(rememberedUploads().map((r) => r.uploadId)).toEqual(['u']);
  });
});

type Events = { signal: AbortSignal; uploadId: string | null; onOpen: (id: string) => void; onVerifying: () => void; onProgress: (n: number) => void; onChecking: (done: number, of: number) => void };
const hang = (signal: AbortSignal) => new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted'))));
const newFile = (name: string) => ({ space: 'personal', name, path: `data/${name}`, file, handle: null, replacesUnkept: false });
const rowFor = (name: string) => driveUploads.snapshot().find((i) => i.name === name)!;

/** A failed upload left open under `uploadId`, remembered, as a dropped connection leaves one. */
async function interrupted(store: typeof driveUploads, name: string, uploadId: string) {
  send.mockImplementationOnce(async (_s: string, _p: string, _f: File, { onOpen }: Events) => {
    onOpen(uploadId);
    await vi.waitFor(() => expect(rememberedUploads().some((r) => r.uploadId === uploadId)).toBe(true));
    throw new UploadSendError();
  });
  store.add([newFile(name)]);
  await vi.waitFor(() => expect(rowFor(name).status).toBe('failed'));
}

describe('Drive uploads beside each other', () => {
  it('a file refused for room stops only itself: the others still go, and none is ended', async () => {
    const store = await freshStore();
    send.mockImplementationOnce(async () => { throw new DriveUploadFailed('over_quota', ''); });
    store.add(['a', 'b', 'c', 'd'].map(newFile));
    await vi.waitFor(() => expect(rowFor('a').status).toBe('failed'));
    // The fourth was waiting its turn; it still gets one, and the server decides whether it fits.
    await vi.waitFor(() => expect(send).toHaveBeenCalledTimes(4));
    expect(['b', 'c', 'd'].map((n) => rowFor(n).status)).toEqual(['sending', 'sending', 'sending']);
    expect(cancelDriveUpload).not.toHaveBeenCalled();
  });

  it('dismissing a failed upload ends what the server holds open for it', async () => {
    const store = await freshStore();
    await interrupted(store, 'a', 'u-a');
    store.dismiss();
    await settle();
    expect(cancelDriveUpload).toHaveBeenCalledWith('personal', 'u-a');
    expect(rememberedUploads()).toEqual([]);
  });

  it('the same file dropped again after a failure continues its upload, not a new one', async () => {
    const store = await freshStore();
    await interrupted(store, 'a', 'u-a');
    store.add([newFile('a')]);
    await vi.waitFor(() => expect(send).toHaveBeenCalledTimes(2));
    expect(send.mock.calls[1][3]).toMatchObject({ uploadId: 'u-a' });
    expect(store.snapshot().filter((i) => i.name === 'a')).toHaveLength(1);
  });

  it('goes back to sending, with its cancel, while parts the server found missing go again', async () => {
    const store = await freshStore();
    send.mockImplementationOnce(async (_s: string, _p: string, _f: File, { onVerifying, onProgress, signal }: Events) => {
      onVerifying();
      onProgress(10);
      return hang(signal);
    });
    store.add([newFile('a')]);
    await vi.waitFor(() => expect(rowFor('a')).toMatchObject({ status: 'sending', loaded: 10 }));
    store.cancel(rowFor('a').id);
    await vi.waitFor(() => expect(rowFor('a').status).toBe('canceled'));
  });
});

describe('Drive uploads an earlier page left', () => {
  it('shows one whose check failed as failed, with the reason, not paused', async () => {
    rememberUpload(await record('checking'));
    status.mockResolvedValue({ state: 'verifying' });
    send.mockImplementationOnce(async () => { throw new DriveUploadFailed('mismatch', 'bytes differ'); });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('failed'));
    expect(store.snapshot()[0]).toMatchObject({ failure: { error: { type: 'mismatch' } } });
  });
});

describe('Drive uploads and other tabs', () => {
  /** The browser's locks, with another tab holding `other`. */
  function otherTabHolds(other: string) {
    const held = new Set([other]);
    const freed: Array<() => void> = [];
    const locks = {
      query: async () => ({ held: [...held].map((name) => ({ name })) }),
      request: async (name: string, opts: { ifAvailable?: boolean } | (() => unknown), cb?: (lock: { name: string } | null) => unknown) => {
        const callback = (typeof opts === 'function' ? opts : cb)!;
        const ifAvailable = typeof opts === 'object' && opts.ifAvailable;
        if (held.has(name)) {
          if (ifAvailable) return callback(null);
          await new Promise<void>((r) => freed.push(r));
        }
        held.add(name);
        try {
          return await callback({ name });
        } finally {
          held.delete(name);
        }
      },
    };
    Object.defineProperty(navigator, 'locks', { value: locks, configurable: true });
    return () => {
      held.delete(other);
      for (const f of freed.splice(0)) f();
    };
  }

  it('shows an upload another tab is sending as going on there, with nothing to continue or discard', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [{ part_number: 1, size: 16, md5: '' }] });
    const otherTabDone = otherTabHolds('langalpha-drive-upload:open');
    try {
      const store = await freshStore();
      store.restore();
      await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('elsewhere'));
      const { id } = store.snapshot()[0];
      expect(await store.resume(id, { file })).toBe('refused');
      store.discard(id);
      await settle();
      expect(cancelDriveUpload).not.toHaveBeenCalled();
      expect(send).not.toHaveBeenCalled();
      // The other tab finishes: this one reads where it stands again.
      status.mockResolvedValue({ state: 'done', entry: {} });
      otherTabDone();
      await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('done'));
    } finally {
      Object.defineProperty(navigator, 'locks', { value: undefined, configurable: true });
    }
  });

  it('does not send an upload another tab took first, showing it going on there', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('paused'));
    otherTabHolds('langalpha-drive-upload:open');
    try {
      expect(await store.resume(store.snapshot()[0].id, { file })).toBe('sent');
      await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('elsewhere'));
      expect(send).not.toHaveBeenCalled();
    } finally {
      Object.defineProperty(navigator, 'locks', { value: undefined, configurable: true });
    }
  });
});

describe('Drive uploads and their lock', () => {
  it('lets go of a continued upload once it stops, so the next Continue in this tab can send it', async () => {
    const held = new Set<string>();
    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: {
        query: async () => ({ held: [...held].map((name) => ({ name })) }),
        request: async (name: string, opts: object | (() => unknown), cb?: (l: { name: string } | null) => unknown) => {
          const callback = (typeof opts === 'function' ? opts : cb)! as (l: { name: string } | null) => unknown;
          if (held.has(name)) return callback(null);
          held.add(name);
          try { return await callback({ name }); } finally { held.delete(name); }
        },
      },
    });
    try {
      rememberUpload(await record('open'));
      status.mockResolvedValue({ state: 'uploading', parts_held: [] });
      const store = await freshStore();
      store.restore();
      await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('paused'));
      // The send names the upload it continues, as sendParts does, then hands it back for its file.
      send.mockImplementation(async (_s: string, _p: string, _f: File, { onOpen }: Events) => {
        onOpen('open');
        await settle();
        throw new FileNeeded();
      });
      await store.resume(store.snapshot()[0].id, { file });
      await vi.waitFor(() => expect(store.snapshot()[0]).toMatchObject({ status: 'paused' }));
      await vi.waitFor(() => expect(held.size).toBe(0));
      await store.resume(store.snapshot()[0].id, { file });
      await vi.waitFor(() => expect(send).toHaveBeenCalledTimes(2));
      expect(store.snapshot()[0].status).not.toBe('elsewhere');
    } finally {
      Object.defineProperty(navigator, 'locks', { value: undefined, configurable: true });
    }
  });
});

describe('Drive uploads stopped by the person', () => {
  async function stopped(ended: 'aborted' | 'done') {
    (cancelDriveUpload as Mock).mockResolvedValue(ended);
    const store = await freshStore();
    send.mockImplementationOnce(async (_s: string, _p: string, _f: File, { onOpen, signal }: Events) => {
      onOpen('u-c');
      return hang(signal);
    });
    store.add([newFile('c')]);
    await vi.waitFor(() => expect(rowFor('c')).toMatchObject({ status: 'sending', uploadId: 'u-c' }));
    store.cancel(rowFor('c').id);
    await vi.waitFor(() => expect(cancelDriveUpload).toHaveBeenCalledWith('personal', 'u-c'));
    await settle();
    return rowFor('c').status;
  }

  it('shows a file the server had already saved when the cancel reached it as uploaded', async () => {
    expect(await stopped('done')).toBe('done');
  });

  it('shows one the cancel did stop as cancelled', async () => {
    expect(await stopped('aborted')).toBe('canceled');
  });
});

describe('Drive uploads checking what was sent', () => {
  it('shows how far the check of the parts already sent is, then the bytes going', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('paused'));
    let go!: () => void;
    send.mockImplementationOnce(async (_s: string, _p: string, _f: File, { onChecking, onProgress, signal }: Events) => {
      onChecking(16, 32);
      await new Promise<void>((r) => (go = r));
      onProgress(32);
      return hang(signal);
    });
    await store.resume(store.snapshot()[0].id, { file });
    await vi.waitFor(() => expect(store.snapshot()[0]).toMatchObject({ status: 'sending', checked: { done: 16, of: 32 } }));
    go();
    await vi.waitFor(() => expect(store.snapshot()[0]).toMatchObject({ status: 'sending', checked: null, loaded: 32 }));
  });
});

describe('Drive uploads cleared while another tab sends them', () => {
  it('dismissing leaves an upload another tab has taken up open, and remembered', async () => {
    const store = await freshStore();
    await interrupted(store, 'a', 'u-a');
    // Another tab continues it, holding its lock.
    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: {
        query: async () => ({ held: [{ name: 'langalpha-drive-upload:u-a' }] }),
        request: async (_name: string, opts: object | (() => unknown), cb?: (l: null) => unknown) =>
          (typeof opts === 'function' ? opts : cb)!(null),
      },
    });
    try {
      store.dismiss();
      await settle();
      await settle();
      expect(cancelDriveUpload).not.toHaveBeenCalled();
      expect(rememberedUploads().map((r) => r.uploadId)).toEqual(['u-a']);
    } finally {
      Object.defineProperty(navigator, 'locks', { value: undefined, configurable: true });
    }
  });
});

describe('Drive uploads and the server not hearing the cancel', () => {
  it('keeps a discarded upload remembered when the cancel did not reach the server, for a later page to end', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    (cancelDriveUpload as Mock).mockRejectedValue(new Error('Network Error'));
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(1));
    store.discard(store.snapshot()[0].id);
    await settle();
    await settle();
    expect(cancelDriveUpload).toHaveBeenCalledWith('personal', 'open');
    expect(rememberedUploads().map((r) => r.uploadId)).toEqual(['open']);
  });

  it('forgets one the server no longer has', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    (cancelDriveUpload as Mock).mockRejectedValue(Object.assign(new Error('nf'), { response: { status: 404 } }));
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()).toHaveLength(1));
    store.discard(store.snapshot()[0].id);
    await vi.waitFor(() => expect(rememberedUploads()).toEqual([]));
  });
});

describe('Drive uploads brought back across a sign-out', () => {
  it('drops a row whose answer arrives after the account changed', async () => {
    rememberUpload(await record('open'));
    let answer!: (v: unknown) => void;
    status.mockReturnValueOnce(new Promise((r) => (answer = r)));
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(status).toHaveBeenCalled());
    runAuthResets();
    answer({ state: 'uploading', parts_held: [] });
    await settle();
    await settle();
    expect(store.snapshot()).toEqual([]);
  });

  it('asks again on the next page visit about one the server could not answer for', async () => {
    rememberUpload(await record('open'));
    status.mockRejectedValueOnce(Object.assign(new Error('busy'), { response: { status: 503 } }));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(status).toHaveBeenCalledTimes(1));
    await settle();
    expect(store.snapshot()).toEqual([]);
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('paused'));
  });
});

describe('Drive uploads cancelled while another tab sends them', () => {
  it('leaves an upload waiting its turn here, and taken up by another tab, open and remembered', async () => {
    rememberUpload(await record('open'));
    status.mockResolvedValue({ state: 'uploading', parts_held: [] });
    const store = await freshStore();
    store.restore();
    await vi.waitFor(() => expect(store.snapshot()[0]?.status).toBe('paused'));
    const { id } = store.snapshot()[0];
    // Three others take every turn, so the continued one waits.
    store.add(['b', 'c', 'd'].map(newFile));
    expect(await store.resume(id, { file })).toBe('sent');
    expect(store.snapshot().find((i) => i.id === id)?.status).toBe('queued');
    // Meanwhile another tab takes it up.
    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: {
        query: async () => ({ held: [{ name: 'langalpha-drive-upload:open' }] }),
        request: async (_name: string, opts: object | (() => unknown), cb?: (l: null) => unknown) =>
          (typeof opts === 'function' ? opts : cb)!(null),
      },
    });
    try {
      store.cancel(id);
      await settle();
      await settle();
      expect(cancelDriveUpload).not.toHaveBeenCalled();
      expect(rememberedUploads().map((r) => r.uploadId)).toEqual(['open']);
    } finally {
      Object.defineProperty(navigator, 'locks', { value: undefined, configurable: true });
    }
  });
});

describe('Drive uploads refused for the files before them', () => {
  const quota = (recheck?: boolean) => ({ error: new DriveUploadFailed('over_quota', ''), ...(recheck && { recheck }) });

  it('offers Retry on a refusal only the files before it caused, never on one for its own size', async () => {
    const store = await freshStore();
    store.add([{ ...newFile('own'), failure: quota() }, { ...newFile('behind'), failure: quota(true) }]);
    expect(canRetry(rowFor('own'))).toBe(false);
    expect(canRetry(rowFor('behind'))).toBe(true);
    // The page judges it again: Retry alone never sends it unjudged.
    store.retry(rowFor('behind').id);
    expect(send).not.toHaveBeenCalled();
  });

  it('sends one judged again to fit', async () => {
    const store = await freshStore();
    store.add([{ ...newFile('behind'), failure: quota(true) }]);
    store.recheck(rowFor('behind').id);
    await vi.waitFor(() => expect(send).toHaveBeenCalledWith('personal', 'data/behind', file, expect.anything()));
  });

  it('makes final one judged again not to fit on its own', async () => {
    const store = await freshStore();
    store.add([{ ...newFile('behind'), failure: quota(true) }]);
    store.recheck(rowFor('behind').id, quota());
    expect(rowFor('behind')).toMatchObject({ status: 'failed', failure: { error: { type: 'over_quota' } } });
    expect(canRetry(rowFor('behind'))).toBe(false);
    expect(send).not.toHaveBeenCalled();
  });
});
