/**
 * One file into the Drive, by whichever way the server answers its begin:
 * nothing to send (it holds those bytes), one request, or parts, to the store
 * on signed URLs or through the server. An upload outlives the page: one
 * that stops on the connection, or whose page goes away, stays open, and
 * sending again with its id continues from the parts the store holds. Only a
 * cancel ends it.
 */
import { delay } from '@/api/client';
import { driveErrorType, saveFile } from '@/api/drive';
import { apiErrorStatus } from '@/api/errors';
import {
  beginDriveUpload,
  cancelDriveUpload,
  completeDriveUpload,
  DriveUploadFailed,
  getDriveUpload,
  relayDriveUploadPart,
  signDriveUploadParts,
  type DriveUploadStatus,
  type OpenUpload,
} from '@/api/driveUploads';
import { beginUnlessAborted, canceled, type RelayPart, retryable, runPartUpload, UploadSendError } from '@/lib/partUpload';
import type { DriveEntry } from '@/types/drive';
import { toBase64, toHex } from './md5';
import { partDigests } from './partDigests';

export interface DriveSendEvents {
  /** Bytes the server or the store holds of the file, or is taking now. */
  onProgress: (loaded: number) => void;
  /** The one-request save waits on a busy server and goes again. */
  onBusy: () => void;
  /** Every byte is in; the server reads the file back to learn its hash. A cancel no longer stops it. */
  onVerifying: () => void;
  /** An upload is open under this id; keep it to continue after a failure. */
  onOpen: (uploadId: string) => void;
  /** Continuing: `done` of the `of` bytes the store holds are checked against the file so far. */
  onChecking?: (done: number, of: number) => void;
}

const POLL_MS = 1500;
// About a minute of polls that got no answer.
const POLL_FAILURES_MAX = 40;

/** The server needs the file to go on, and the row has none: an earlier page's upload handed back for parts. */
export class FileNeeded extends Error {
  constructor() {
    super('the file is needed to send its parts');
    this.name = 'FileNeeded';
  }
}

/**
 * A full space (507) or too many open uploads: answers that hold until the
 * person frees room or ends an upload, though the shared sender would read a
 * 507 as the server's passing trouble and ask again.
 */
function refusedForGood(e: unknown): boolean {
  return apiErrorStatus(e) === 507 || driveErrorType(e) === 'too_many_uploads';
}

/** A failure that leaves the upload open, to be continued: the connection, or the server's passing trouble. */
export function continuable(e: unknown): boolean {
  return e instanceof UploadSendError || (retryable(e) && !refusedForGood(e));
}

/** Completes `id`, a refusal for good arriving as the upload's own failure, which no loop asks again. */
function complete(space: string, id: string, signal?: AbortSignal): Promise<DriveUploadStatus> {
  return completeDriveUpload(space, id, signal).catch((e: unknown) => {
    throw refusedForGood(e) ? new DriveUploadFailed(driveErrorType(e) ?? null, e instanceof Error ? e.message : '') : e;
  });
}

async function untilSettled(
  space: string,
  first: DriveUploadStatus,
  again: () => Promise<DriveUploadStatus>,
  signal: AbortSignal,
): Promise<DriveUploadStatus> {
  let status = first;
  let failures = 0;
  while (status.state === 'verifying' || status.state === 'uploading') {
    await delay(POLL_MS, signal);
    try {
      // Back to uploading: the store was short of parts, which completing again sends.
      status = status.state === 'uploading' ? await again() : await getDriveUpload(space, status.upload_id, signal);
      failures = 0;
    } catch (e) {
      if (signal.aborted || !retryable(e) || ++failures >= POLL_FAILURES_MAX) throw e;
    }
  }
  return status;
}

/** How a settled upload ended: the entry it became (null where the file went before it was read back), or why not. */
function entryOf(status: DriveUploadStatus): DriveEntry | null {
  switch (status.state) {
    case 'done':
      return status.entry ?? null;
    case 'aborted':
      throw canceled();
    case 'failed':
      throw new DriveUploadFailed(status.error?.type ?? null, status.error?.message ?? '');
    default:
      throw new Error(`upload ${status.upload_id} is still ${status.state}`);
  }
}

async function sendParts(
  space: string,
  file: File,
  upload: OpenUpload,
  events: DriveSendEvents,
  signal: AbortSignal,
): Promise<DriveEntry | null> {
  const id = upload.upload_id;
  events.onOpen(id);
  const md5Of = partDigests(file, upload.part_size, signal);
  const base64Of = async (n: number) => {
    const digest = await md5Of(n);
    return digest ? toBase64(digest) : null;
  };
  const held = await provenHeld(upload, md5Of, events.onChecking);
  // A part with no digest (hashing unavailable) goes without one, checked by its length alone.
  const relay: RelayPart = async (n, body, config) => relayDriveUploadPart(space, id, n, body, await base64Of(n), config);
  const sign = async (numbers: number[], s: AbortSignal) => {
    const parts = await Promise.all(numbers.map(async (part_number) => {
      const md5 = await base64Of(part_number);
      return md5 ? { part_number, md5 } : { part_number };
    }));
    return signDriveUploadParts(space, id, parts, s);
  };
  const status = await runPartUpload(
    file,
    { count: upload.part_count, size: upload.part_size, held },
    {
      // A relay upload is never signed; a direct one may still relay a part its PUT could not deliver.
      parts: upload.mode === 'direct'
        ? { kind: 'signedWithFallback', sign, relay }
        : { kind: 'relay', relay },
      complete: (s) => complete(space, id, s),
      cancel: () => cancelDriveUpload(space, id),
      // An upload stopped on the connection stays open, to continue from this page or a later one.
      lifetime: { kind: 'resumable', keepOpen: continuable },
    },
    {
      signal,
      onProgress: (p) => {
        if (p.phase === 'saving') events.onVerifying();
        else events.onProgress(p.loaded);
      },
    },
    (accepted, again) => untilSettled(space, accepted, again, signal),
  );
  return entryOf(status);
}

/**
 * The parts the store holds that are this file's own, proven by hashing each
 * from the file and comparing with the store's MD5 for it: only those are
 * skipped, and every other part goes again from the file, so what the store
 * keeps is the file's whatever it held. A part that differs is not proven
 * rather than a reason to refuse: some stores tag a part with 32 hex
 * characters that are not its MD5 (server-side encryption with a managed
 * key), and there a resume sends everything again. Whether the file is the
 * one the upload was sent from is the fingerprint's call, made before this.
 * The digests come a worker's worth at a time, so `onChecking` says how far
 * the check is.
 */
/** Held parts that all fail to prove before the check stops and sends every part again. */
const GIVE_UP_AFTER = 3;

async function provenHeld(
  upload: OpenUpload,
  md5Of: (n: number) => Promise<Uint8Array | null>,
  onChecking?: (done: number, of: number) => void,
): Promise<OpenUpload['parts_held']> {
  const named = upload.parts_held.filter((p): p is OpenUpload['parts_held'][number] & { md5: string } => !!p.md5);
  const of = named.reduce((sum, p) => sum + p.size, 0);
  if (of === 0) return [];
  let done = 0;
  onChecking?.(0, of);
  const proven: OpenUpload['parts_held'] = [];
  // In order, the next part read while this one is compared.
  for (const [i, p] of named.entries()) {
    const next = named[i + 1];
    if (next) md5Of(next.part_number).catch(() => {});
    const ours = await md5Of(p.part_number);
    if (ours && p.md5.toLowerCase() === toHex(ours)) proven.push(p);
    // The first few all differing: a store whose tags are not MD5s, where hashing the rest only delays sending them.
    else if (i + 1 === GIVE_UP_AFTER && proven.length === 0) return [];
    done += p.size;
    onChecking?.(done, of);
  }
  return proven;
}

/** The upload `uploadId` names, where it can be continued; null when a new one has to begin. */
type Continuable = Extract<DriveUploadStatus, { state: 'uploading' | 'verifying' | 'done' }>;

async function reopen(space: string, uploadId: string, signal: AbortSignal): Promise<Continuable | null> {
  try {
    const status = await getDriveUpload(space, uploadId, signal);
    switch (status.state) {
      case 'uploading':
      case 'verifying':
      case 'done':
        return status;
      default:
        return null;
    }
  } catch (e) {
    if (apiErrorStatus(e) === 404) return null;
    throw e;
  }
}

/**
 * Sends `file` to `path`, continuing `uploadId` when the server still holds
 * it open; resolves with the entry the file became (null where it went
 * before it was read back). With no file, only an upload already being checked
 * can be followed to its end.
 */
export async function sendToDrive(
  space: string,
  path: string,
  given: File | null,
  { uploadId, signal, ...events }: DriveSendEvents & { uploadId: string | null; signal: AbortSignal },
): Promise<DriveEntry | null> {
  const open = uploadId ? await reopen(space, uploadId, signal) : null;
  if (open?.state === 'done') return entryOf(open);
  if (open?.state === 'verifying') {
    events.onVerifying();
    return entryOf(await untilSettled(space, open, () => complete(space, open.upload_id, signal), signal));
  }
  if (!given) throw new FileNeeded();
  const file = given;
  if (open) return sendParts(space, file, open, events, signal);

  // An upload the begin opened holds a place among the person's open ones until it ends.
  const begun = await beginUnlessAborted(
    beginDriveUpload(space, { path, size: file.size, content_type: file.type || undefined }),
    signal,
    (b) => (b.mode === 'direct' || b.mode === 'relay' ? cancelDriveUpload(space, b.upload_id) : undefined),
  );
  switch (begun.mode) {
    case 'held':
      events.onProgress(file.size);
      return begun.entry;
    case 'single':
      return saveFile(space, path, file, {
        contentType: file.type || undefined,
        signal,
        // Once the body is all sent the server is saving it, which a cancel no longer stops.
        onProgress: (e) => (e.loaded >= file.size ? events.onVerifying() : events.onProgress(e.loaded)),
        onBusy: events.onBusy,
      });
    default:
      return sendParts(space, file, begun, events, signal);
  }
}
