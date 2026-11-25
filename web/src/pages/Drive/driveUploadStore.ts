/**
 * The Drive's uploads, outside any component: a file of several GB takes
 * longer than a person stays on one route, so moving about the app neither
 * stops it nor loses its row. An open upload is also remembered in the
 * browser (`rememberedUploads`), so a reload or a closed tab leaves it open on
 * the server and the next page shows it paused, to continue from the parts
 * the store holds or to discard. Only the person's cancel or discard ends one.
 * A tab closed or reloaded while bytes are moving asks first.
 */
import { driveErrorType } from '@/api/drive';
import { cancelDriveUpload, DriveUploadFailed, getDriveUpload } from '@/api/driveUploads';
import { apiErrorStatus } from '@/api/errors';
import { authSessionCheck, registerAuthReset } from '@/lib/authResets';
import { keepHandle, loadHandle, readHandle } from '@/lib/fileHandles';
import { holdReload } from '@/lib/reloadHolds';
import type { DriveUsage } from '@/types/drive';
import { continuable, FileNeeded, sendToDrive } from './driveUpload';
import {
  fingerprint,
  forgetUpload,
  isSameFile,
  looksSame,
  rememberedUploads,
  rememberUpload,
  type RememberedUpload,
} from './rememberedUploads';

/**
 * Why a file is not in the Drive, put into words where it is shown
 * (`driveRefusal`). A refusal the page makes before a byte goes carries the
 * same error the server would have sent, with the space as it judged it: the
 * room left once the files before it in the same drop are counted.
 */
export interface UploadFailure {
  error: unknown;
  usage?: DriveUsage;
  /**
   * Refused only because the files before it in the same drop were counted:
   * Retry has the page judge it again against the space as it is then.
   */
  recheck?: true;
}

/** What every row has, whatever has become of its upload. */
interface Row {
  id: number;
  space: string;
  name: string;
  /** Where it lands, inside the space. */
  path: string;
  total: number;
  /** Where the browser gave one, a handle to read the file again with a click. */
  handle: FileSystemFileHandle | null;
  /** It replaces a file too large to keep an earlier copy of: the current one is not kept. */
  replacesUnkept: boolean;
}

/** The upload the server may hold open for a row, which a retry or a later page continues. */
interface Opened {
  uploadId: string | null;
  /** What a file given to continue it must match, once remembered. */
  remembered: RememberedUpload | null;
}

/**
 * A row by where its upload stands. `file` is null on a row an earlier page
 * left, which a later page follows to its end (`queued` while the server
 * checks it) or offers to continue once the person gives the file (`paused`).
 * A row that has ended (`done`, `canceled`) holds no upload, so nothing is
 * left open on the server behind it.
 */
export type UploadItem =
  | (Row & Opened & { status: 'queued'; file: File | null; loaded: number })
  /** `busy`: the server asked the one-request save to wait while it holds other uploads. */
  /** `checked`: continuing, how far the parts the store holds are checked against the file, before bytes go. */
  | (Row & Opened & { status: 'sending'; file: File | null; loaded: number; busy: boolean; checked: { done: number; of: number } | null })
  | (Row & Opened & { status: 'verifying' })
  /** `wrongFile`: the name of a file given to continue it that is not the one it was sent from. */
  | (Row & { status: 'paused'; uploadId: string; remembered: RememberedUpload; loaded: number; wrongFile: string | null })
  | (Row & Opened & { status: 'failed'; file: File | null; failure: UploadFailure })
  /** Another tab of this browser is sending it: that tab continues or discards it, not this one. */
  | (Row & { status: 'elsewhere'; uploadId: string; remembered: RememberedUpload })
  | (Row & { status: 'done' })
  | (Row & { status: 'canceled'; file: File | null });

type Status = UploadItem['status'];
type In<S extends Status> = Extract<UploadItem, { status: S }>;

/** Files sent at once; the rest wait their turn. */
const CONCURRENCY = 3;

/** Going, or about to: what the tray counts as running. */
export const ACTIVE: ReadonlySet<Status> = new Set<Status>(['queued', 'sending', 'verifying']);
/** Bytes are moving, or about to: what leaving the page would interrupt. */
const MOVING = new Set<Status>(['queued', 'sending']);
/** Refused for the file itself or the space: sending again would be refused the same way. */
const FINAL = new Set(['too_large', 'over_quota', 'too_many_entries', 'exists', 'is_folder', 'not_folder', 'invalid_path', 'forbidden']);

let items: UploadItem[] = [];
let nextId = 1;
let restored = false;
const listeners = new Set<() => void>();
const controllers = new Map<number, AbortController>();
let running = 0;
let releaseHold: (() => void) | null = null;

function emit(): void {
  const moving = items.some((i) => MOVING.has(i.status));
  // The reload a stale build takes, and the browser's own leave question, both wait on bytes moving.
  if (moving && !releaseHold) {
    releaseHold = holdReload();
    window.addEventListener('beforeunload', askBeforeLeaving);
  } else if (!moving && releaseHold) {
    releaseHold();
    releaseHold = null;
    window.removeEventListener('beforeunload', askBeforeLeaving);
  }
  for (const listener of listeners) listener();
}

function askBeforeLeaving(e: BeforeUnloadEvent): void {
  e.preventDefault();
}

const itemOf = (id: number) => items.find((i) => i.id === id);

const rowOf = ({ id, space, name, path, total, handle, replacesUnkept }: UploadItem): Row =>
  ({ id, space, name, path, total, handle, replacesUnkept });

/**
 * Moves row `id` to what `to` makes of it, only from the states `from`
 * names; answers whether it moved. Every change of a row goes through here.
 */
function move<S extends Status>(id: number, from: readonly S[], to: (item: In<S>) => UploadItem): boolean {
  const item = itemOf(id);
  if (!item || !(from as readonly Status[]).includes(item.status)) return false;
  const next = to(item as In<S>);
  items = items.map((i) => (i.id === id ? next : i));
  emit();
  return true;
}

/**
 * Asks the server to end `uploadId`, forgetting it once the server says it is
 * over. A cancel that never arrived leaves it remembered, so a later page
 * shows it again to discard. With `id`, a row whose upload the server had
 * already saved when the cancel reached it shows as uploaded: the file stands.
 */
function cancelOnServer(space: string, uploadId: string, id?: number): Promise<void> {
  return cancelDriveUpload(space, uploadId).then(
    (ended) => {
      forgetUpload(uploadId);
      if (ended === 'done' && id !== undefined && move(id, ['canceled', 'failed'], (item) => ({ ...rowOf(item), status: 'done' }))) {
        onSettled(space);
      }
    },
    (e) => {
      if (apiErrorStatus(e) === 404) forgetUpload(uploadId);
    },
  );
}

/** Ends what the server holds open for a row this tab is sending and will not send. */
function endUpload(space: string, uploadId: string | null, id?: number): void {
  if (uploadId) void cancelOnServer(space, uploadId, id);
}

/**
 * Ends an upload this tab is not sending (a row discarded, cleared, or
 * cancelled while it waits its turn), unless another tab has taken it up
 * meanwhile: that tab sends it, and its record stays.
 */
function endUnlessTaken(space: string, uploadId: string | null, id?: number): void {
  if (!uploadId) return;
  void holdUpload(uploadId).then((release) => {
    if (!release) return;
    // Held until the server has it ended, so no tab takes it up in between.
    void cancelOnServer(space, uploadId, id).finally(release);
  });
}

// The transitions, one per move a row makes.

/** A file given again, a retry, or a paused upload's file found: it waits its turn. */
const toQueued = (id: number, file: File, handle?: FileSystemFileHandle | null) =>
  move(id, ['failed', 'canceled', 'paused'], (item) => ({
    ...rowOf(item),
    handle: handle === undefined ? item.handle : handle,
    status: 'queued',
    file,
    // A paused upload shows what the store holds until its parts are counted again.
    loaded: item.status === 'paused' ? item.loaded : 0,
    // A failure that left the upload open, or a paused one, goes on from what the store holds.
    uploadId: item.status === 'canceled' ? null : item.uploadId,
    remembered: item.status === 'canceled' ? null : item.remembered,
  }));

const toSending = (id: number) =>
  move(id, ['queued'], (item) => ({ ...item, status: 'sending', busy: false, checked: null }));

/** Bytes moving: also a row being checked whose check found parts missing, which go again. */
const toProgress = (id: number, file: File | null, loaded: number) =>
  move(id, ['sending', 'verifying'], (item) => ({ ...rowOf(item), uploadId: item.uploadId, remembered: item.remembered, status: 'sending', file, loaded, busy: false, checked: null }));

const toBusy = (id: number, file: File | null) =>
  move(id, ['sending', 'verifying'], (item) => ({ ...rowOf(item), uploadId: item.uploadId, remembered: item.remembered, status: 'sending', file, loaded: 0, busy: true, checked: null }));

const toChecking = (id: number, done: number, of: number) =>
  move(id, ['sending'], (item) => ({ ...item, checked: { done, of } }));

const toOpened = (id: number, uploadId: string) =>
  move(id, ['sending'], (item) => ({ ...item, uploadId }));

const toRemembered = (id: number, record: RememberedUpload) =>
  move(id, ['queued', 'sending', 'verifying', 'failed'], (item) =>
    item.uploadId === record.uploadId ? { ...item, remembered: record } : item);

const toVerifying = (id: number) =>
  move(id, ['sending'], ({ file: _file, loaded: _loaded, busy: _busy, checked: _checked, ...item }) => ({ ...item, status: 'verifying' }));

const toDone = (id: number) =>
  move(id, ['sending', 'verifying'], (item) => {
    if (item.uploadId) forgetUpload(item.uploadId);
    return { ...rowOf(item), status: 'done' };
  });

/** Failed with `failure`; `keepOpen` leaves its upload open for a retry, any other is ended. */
const toFailed = (id: number, file: File | null, failure: UploadFailure, keepOpen: boolean) =>
  move(id, ['queued', 'sending', 'verifying'], (item) => {
    if (!keepOpen) endUpload(item.space, item.uploadId, id);
    return {
      ...rowOf(item),
      status: 'failed',
      file,
      failure,
      uploadId: keepOpen ? item.uploadId : null,
      remembered: keepOpen ? item.remembered : null,
    };
  });

/** Followed from an earlier page without its file, and handed back for parts: it waits for the file. */
const toPaused = (id: number) => {
  const item = itemOf(id);
  if (!item || item.status === 'paused' || !('uploadId' in item) || !item.uploadId || !item.remembered) return false;
  const { uploadId, remembered } = item;
  return move(id, ['queued', 'sending', 'verifying'], (from) => ({
    ...rowOf(from), status: 'paused', uploadId, remembered, loaded: 'loaded' in from ? from.loaded : 0, wrongFile: null,
  }));
};

/** Found being sent by another tab, which owns it until it lets go. */
const toElsewhere = (id: number) => {
  const item = itemOf(id);
  if (!item || !('uploadId' in item) || !item.uploadId || !item.remembered) return false;
  const { uploadId, remembered } = item;
  const moved = move(id, ['queued', 'sending', 'paused'], (from) => ({ ...rowOf(from), status: 'elsewhere', uploadId, remembered }));
  if (moved) void rejoin(id, remembered);
  return moved;
};

const toWrongFile = (id: number, name: string) =>
  move(id, ['paused'], (item) => ({ ...item, wrongFile: name }));

/**
 * Stopped by the person: whatever the server holds open for it ends, and no
 * later page brings it back. One still waiting its turn holds no lock here,
 * so another tab may be sending it: that one is left to that tab.
 */
const toCanceled = (id: number) =>
  move(id, ['queued', 'sending'], (item) => {
    if (item.status === 'queued') endUnlessTaken(item.space, item.uploadId, id);
    else endUpload(item.space, item.uploadId, id);
    return { ...rowOf(item), status: 'canceled', file: item.file };
  });

/**
 * Each upload being sent holds a lock of this browser's (Web Locks), so a
 * second tab never sends the same upload's parts beside the first, which
 * would race their completes. Where the browser has no locks, tabs are
 * trusted not to.
 */
const lockName = (uploadId: string) => `langalpha-drive-upload:${uploadId}`;
const locks = (): LockManager | undefined => (typeof navigator === 'undefined' ? undefined : navigator.locks);

/** Takes `uploadId`'s lock if no tab holds it: its release, or null when another tab is sending it. */
function holdUpload(uploadId: string): Promise<(() => void) | null> {
  const manager = locks();
  if (!manager) return Promise.resolve(() => {});
  return new Promise((resolve) => {
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    manager
      .request(lockName(uploadId), { ifAvailable: true }, (lock) => {
        if (!lock) return resolve(null);
        resolve(release);
        return held;
      })
      .catch(() => resolve(() => {}));
  });
}

async function heldElsewhere(uploadId: string): Promise<boolean> {
  const manager = locks();
  if (!manager) return false;
  const { held = [] } = await manager.query().catch(() => ({ held: [] }));
  return held.some((l) => l.name === lockName(uploadId));
}

/** Once the other tab lets go of the upload, shows it here as the server then has it. */
async function rejoin(id: number, r: RememberedUpload): Promise<void> {
  for (;;) {
    // Granted once no tab holds it.
    await locks()?.request(lockName(r.uploadId), () => {}).catch(() => {});
    if (itemOf(id)?.status !== 'elsewhere') return;
    const row = await restoreOne(r);
    if (row === undefined) {
      await new Promise((ok) => setTimeout(ok, 5000));
      continue;
    }
    if (row === null || row.status === 'done' || row.status === 'failed') forgetUpload(r.uploadId);
    // Taken again by the other tab meanwhile: wait for it once more.
    if (row?.status === 'elsewhere') continue;
    if (itemOf(id)?.status !== 'elsewhere') return;
    items = row ? items.map((i) => (i.id === id ? { ...row, id } : i)) : items.filter((i) => i.id !== id);
    emit();
    pump();
    return;
  }
}

function newRow(fields: Omit<Row, 'id'>): Row {
  return { ...fields, id: nextId++ };
}

/** Whether `item`'s failure is one sending again could change. */
export function canRetry(item: UploadItem): boolean {
  if (item.status === 'canceled') return !!item.file;
  if (item.status !== 'failed' || !item.file) return false;
  return !!item.failure.recheck || !FINAL.has(driveErrorType(item.failure.error) ?? '');
}

/** Remembers the upload `uploadId` sent from `file`, for a page opened later to continue. */
async function remember(id: number, file: File, uploadId: string): Promise<void> {
  const item = itemOf(id);
  if (!item || !('remembered' in item) || item.remembered?.uploadId === uploadId) return;
  const record: RememberedUpload = {
    space: item.space,
    uploadId,
    path: item.path,
    name: file.name,
    size: file.size,
    lastModified: file.lastModified,
    fingerprint: await fingerprint(file).catch(() => ''),
  };
  // Ended while the fingerprint was read: nothing left to remember.
  const now = itemOf(id);
  if (!now || !('uploadId' in now) || now.uploadId !== uploadId) return;
  rememberUpload(record, item.handle ?? undefined);
  toRemembered(id, record);
}

async function run(item: In<'queued'>): Promise<void> {
  const { id, file } = item;
  const ctrl = new AbortController();
  controllers.set(id, ctrl);
  toSending(id);
  let ended = false;
  let heldFor: string | null = null;
  let release: (() => void) | null = null;
  /** Holds `uploadId`'s lock for this send, once: false when another tab holds it. */
  const hold = async (uploadId: string): Promise<boolean> => {
    if (heldFor === uploadId) return true;
    // Claimed before the wait, so the send naming the same upload does not ask again.
    heldFor = uploadId;
    const r = await holdUpload(uploadId);
    if (!r) return false;
    if (ended) r();
    else {
      // A new upload begun in place of one gone: the old one's lock goes.
      release?.();
      release = r;
    }
    return true;
  };
  try {
    if (item.uploadId && !(await hold(item.uploadId))) {
      toElsewhere(id);
      return;
    }
    await sendToDrive(item.space, item.path, file, {
      uploadId: item.uploadId,
      signal: ctrl.signal,
      onProgress: (loaded) => toProgress(id, file, loaded),
      onBusy: () => toBusy(id, file),
      onVerifying: () => toVerifying(id),
      onChecking: (done, of) => toChecking(id, done, of),
      onOpen: (uploadId) => {
        toOpened(id, uploadId);
        // The upload it continues is held already; a new one no other tab knows yet.
        void hold(uploadId);
        if (file) void remember(id, file, uploadId);
      },
    });
    toDone(id);
    onSettled(item.space);
  } catch (error) {
    // The person's cancel: the upload has already been ended.
    if (ctrl.signal.aborted) {
      toCanceled(id);
      return;
    }
    // An upload followed from an earlier page without its file, which the server handed back for parts.
    if (error instanceof FileNeeded && toPaused(id)) return;
    // Each file meets its own refusal: one too large for the space left may leave room for a smaller one.
    // An upload left open is continued by a retry, here or from a later page; any other ended with the failure.
    toFailed(id, file, { error }, continuable(error));
    onSettled(item.space);
  } finally {
    ended = true;
    (release as (() => void) | null)?.();
    controllers.delete(id);
  }
}

function pump(): void {
  while (running < CONCURRENCY) {
    const next = items.find((i): i is In<'queued'> => i.status === 'queued');
    if (!next) return;
    running += 1;
    // Marked sending inside, before its first await, so the next turn of this loop takes another file.
    void run(next).finally(() => {
      running -= 1;
      pump();
    });
  }
}

/** Whether `file` could be the one a failed row was sending, by what is known of it without reading. */
function looksLikeIts(file: File, item: In<'failed'>): boolean {
  if (item.remembered) return looksSame(file, item.remembered);
  const was = item.file;
  return !!was && was.name === file.name && was.size === file.size && was.lastModified === file.lastModified;
}

const settledListeners = new Set<(space: string) => void>();
function onSettled(space: string): void {
  for (const listener of settledListeners) listener(space);
}

/** One remembered upload as the server now has it, as a row; null to forget it, undefined to ask again later. */
async function restoreOne(r: RememberedUpload): Promise<UploadItem | null | undefined> {
  let status;
  try {
    status = await getDriveUpload(r.space, r.uploadId);
  } catch (e) {
    return apiErrorStatus(e) === 404 ? null : undefined;
  }
  const row = newRow({ space: r.space, name: r.name, path: r.path, total: r.size, handle: null, replacesUnkept: false });
  switch (status.state) {
    case 'uploading': {
      if (await heldElsewhere(r.uploadId)) return { ...row, status: 'elsewhere', uploadId: r.uploadId, remembered: r };
      const held = status.parts_held.reduce((sum, p) => sum + p.size, 0);
      const handle = await loadHandle(r.uploadId);
      return { ...row, handle, status: 'paused', uploadId: r.uploadId, remembered: r, loaded: held, wrongFile: null };
    }
    case 'verifying':
      // Followed to its end like any upload being checked; it needs no file.
      return { ...row, status: 'queued', file: null, uploadId: r.uploadId, remembered: r, loaded: r.size };
    case 'done':
      return { ...row, status: 'done' };
    case 'failed': {
      const error = new DriveUploadFailed(status.error?.type ?? null, status.error?.message ?? '');
      return { ...row, status: 'failed', file: null, failure: { error }, uploadId: null, remembered: null };
    }
    case 'aborted':
      return null;
  }
}

/** A row for `f` that waits its turn, or has failed already where the page refused it. */
function freshRow(f: NewUpload): UploadItem {
  const row = newRow({ space: f.space, name: f.name, path: f.path, total: f.file.size, handle: f.handle, replacesUnkept: f.replacesUnkept });
  return f.failure
    ? { ...row, status: 'failed', file: f.file, failure: f.failure, uploadId: null, remembered: null }
    : { ...row, status: 'queued', file: f.file, loaded: 0, uploadId: null, remembered: null };
}

/** Puts new rows after the ones still going, dropping those that have ended well or been cancelled. */
function append(fresh: UploadItem[]): void {
  if (fresh.length === 0) return;
  items = [...items.filter((i) => i.status !== 'done' && i.status !== 'canceled'), ...fresh];
  emit();
  pump();
}

export interface NewUpload {
  space: string;
  name: string;
  path: string;
  file: File;
  handle: FileSystemFileHandle | null;
  replacesUnkept: boolean;
  /** Refused by the page before a byte goes. */
  failure?: UploadFailure;
}

export type ResumeResult = 'sent' | 'pick' | 'refused';

export const driveUploads = {
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  snapshot: (): UploadItem[] => items,
  /** Called with a space whenever a file in it lands or fails, to read it again. */
  onSettled(listener: (space: string) => void): () => void {
    settledListeners.add(listener);
    return () => settledListeners.delete(listener);
  },
  /** The uploads an earlier page left open, as rows: once per page and person. */
  restore(): void {
    if (restored) return;
    restored = true;
    // An answer that lands after a sign-out belongs to the account before.
    const current = authSessionCheck();
    void (async () => {
      let unanswered = false;
      for (const r of rememberedUploads()) {
        if (items.some((i) => 'uploadId' in i && i.uploadId === r.uploadId)) continue;
        const row = await restoreOne(r);
        if (!current()) return;
        if (row === undefined) {
          unanswered = true;
          continue;
        }
        if (row === null || row.status === 'done' || row.status === 'failed') forgetUpload(r.uploadId);
        if (row) {
          items = [...items, row];
          emit();
          if (row.status === 'elsewhere') void rejoin(row.id, r);
        }
      }
      // One the server could not answer for is asked about again the next time the page opens.
      if (unanswered) restored = false;
      pump();
    })();
  },
  /**
   * Adds files to the queue, those already refused (`failure`) shown as
   * failed without being sent. A file that is the one a paused upload to the
   * same place was sent from continues it rather than starting another; one
   * that only looks like it (same name, size and time, other bytes) starts its
   * own, and the paused one stays, saying the file given was not its own.
   */
  add(files: NewUpload[]): void {
    const fresh: UploadItem[] = [];
    for (const f of files) {
      const same = (i: UploadItem) => i.space === f.space && i.path === f.path;
      // A failed upload the server holds open goes on from its parts; the parts' digests prove the file.
      const open = f.failure
        ? undefined
        : items.find((i): i is In<'failed'> => i.status === 'failed' && !!i.uploadId && same(i) && looksLikeIts(f.file, i));
      if (open) {
        toQueued(open.id, f.file, f.handle);
        continue;
      }
      const paused = f.failure
        ? undefined
        : items.find((i): i is In<'paused'> => i.status === 'paused' && same(i) && looksSame(f.file, i.remembered));
      if (!paused) {
        fresh.push(freshRow(f));
        continue;
      }
      void driveUploads.resume(paused.id, { file: f.file, handle: f.handle }).then((result) => {
        if (result === 'refused') append([freshRow(f)]);
      });
    }
    append(fresh);
    pump();
  },
  /**
   * Continues a paused upload with its file: the one given, else the one its
   * handle reads (asking permission, so call it from a click). Answers `pick`
   * when the person has to choose the file, and `refused` when the file is
   * not the one the upload was sent from.
   */
  async resume(id: number, given?: { file: File; handle?: FileSystemFileHandle | null }): Promise<ResumeResult> {
    const item = itemOf(id);
    if (item?.status !== 'paused') return 'refused';
    const handle = given ? given.handle ?? null : item.handle;
    const file = given?.file ?? (handle ? await readHandle(handle) : null);
    if (!file) return 'pick';
    if (!(await isSameFile(file, item.remembered))) {
      toWrongFile(id, file.name);
      return 'refused';
    }
    if (given?.handle) void keepHandle(item.uploadId, given.handle);
    if (!toQueued(id, file, handle)) return 'refused';
    pump();
    return 'sent';
  },
  /** Ends a paused upload on the server, discarding what the store holds of it. */
  discard(id: number): void {
    const item = itemOf(id);
    if (item?.status !== 'paused') return;
    endUnlessTaken(item.space, item.uploadId);
    items = items.filter((i) => i.id !== id);
    emit();
  },
  /** Stops a file before its bytes are all in; once the server is checking it, it lands regardless. */
  cancel(id: number): void {
    const item = itemOf(id);
    if (item?.status === 'queued') toCanceled(id);
    else if (item?.status === 'sending') controllers.get(id)?.abort();
  },
  retry(id: number): void {
    const item = itemOf(id);
    // One refused for the files before it goes only once the page has judged it again (`recheck`).
    if (!item || !canRetry(item) || !('file' in item) || !item.file || (item.status === 'failed' && item.failure.recheck)) return;
    toQueued(id, item.file);
    pump();
  },
  /**
   * The page's second judgement of a row refused for the files before it:
   * with no `failure` it goes, with one the refusal is its own and final.
   */
  recheck(id: number, failure?: UploadFailure): void {
    const item = itemOf(id);
    if (item?.status !== 'failed' || !item.failure.recheck || !item.file) return;
    if (failure) {
      move(id, ['failed'], (from) => ({ ...from, failure }));
      return;
    }
    toQueued(id, item.file);
    pump();
  },
  /** Clears the ended rows, ending what the server still holds open for a failed one. */
  dismiss(): void {
    for (const i of items) if (i.status === 'failed') endUnlessTaken(i.space, i.uploadId);
    items = items.filter((i) => ACTIVE.has(i.status) || i.status === 'paused' || i.status === 'elsewhere');
    emit();
  },
};

// Another account's uploads are theirs: stopped here, and their rows go. A
// signed-out page cancels what it was sending, as the person's last act there.
registerAuthReset(() => {
  for (const ctrl of controllers.values()) ctrl.abort();
  items = [];
  restored = false;
  emit();
});
