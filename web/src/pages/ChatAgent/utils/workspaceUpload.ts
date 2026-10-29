/**
 * Puts one file into a workspace folder, by whichever path the backend picks.
 *
 * A small file, or a computer that cannot pull from object storage, goes in
 * one request through the backend. A larger one goes from here to the store
 * in parts on URLs the backend signs, and the sandbox then pulls the
 * assembled object into place. Starting the upload answers which; this never
 * guesses, since only the backend knows what the computer can do.
 */
import { api, ApiError } from '@/api/client';
import { getAuthHeaders } from '@/lib/authToken';
import {
  beginWorkspaceUpload,
  cancelWorkspaceUpload,
  cancelWorkspaceUploadOnUnload,
  completeWorkspaceUpload,
  getWorkspaceUpload,
  signWorkspaceUploadParts,
  uploadWorkspaceFile,
  type SignedUploadPart,
  type UploadBegin,
  type UploadStatus,
} from './api/files';

export type UploadProgress =
  | { phase: 'sending'; percent: number }
  /** The bytes are in and the workspace is taking the file, which a cancel no longer stops. */
  | { phase: 'saving' };

export interface WorkspaceUploadOptions {
  /** Where in the workspace; the file's own name at the folder's root when absent. */
  path?: string | null;
  onProgress?: (progress: UploadProgress) => void;
  /** Aborting while sending ends the upload and discards what was sent. */
  signal?: AbortSignal;
}

/** The parts could not be sent, retries and all: the connection, not the server, is the likely cause. */
export class UploadSendError extends Error {
  constructor() {
    super('Upload failed');
    this.name = 'UploadSendError';
  }
}

const PART_CONCURRENCY = 4;
const SIGN_BATCH = 8;
const PART_ATTEMPTS = 5;
// A part is given up only when no bytes have gone for this long, so a slow
// link keeps sending while a hung request does not hold its slot.
export const PART_IDLE_MS = 60_000;
// The backend hears from an upload only when it signs parts, and one part on
// a slow link can take longer than its computer may sit idle; signing again
// this often while parts go lets the backend count it as use each minute.
const KEEP_ALIVE_MS = 30_000;
// The token a page going away sends with its last request, kept fresh while
// the parts go; a page that is closing cannot wait for one.
const AUTH_REFRESH_MS = 60_000;
const COMPLETE_ROUNDS = 3;
const POLL_MS = 1500;
// About a minute of polls that got no answer.
const POLL_FAILURES_MAX = 40;
const TERMINAL = new Set<UploadStatus['state']>(['done', 'failed', 'aborted']);

const canceled = () => new ApiError('canceled', 'ERR_CANCELED');

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(canceled());
    const onAbort = () => {
      clearTimeout(timer);
      reject(canceled());
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function untilAborted<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  return new Promise<T>((resolve, reject) => {
    if (signal.aborted) return reject(canceled());
    const onAbort = () => reject(canceled());
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}

const backoff = (attempt: number) => Math.min(8000, 1000 * 2 ** (attempt - 1)) * (0.75 + Math.random() / 2);

/** Worth asking again: no answer, a deadline, or the server's own trouble. */
function retryable(e: unknown): boolean {
  if (!(e instanceof ApiError)) return false;
  if (e.code === 'ERR_NETWORK' || e.code === 'ECONNABORTED') return true;
  return e.status === 429 || (e.status ?? 0) >= 500;
}

/**
 * The router's own answer for a route it lacks. A 404 the route gave (no such
 * workspace, a destination outside it) names what it did not find, and the
 * one-request path would answer the same after the whole file.
 */
function routeMissing(e: unknown): boolean {
  if (!(e instanceof ApiError)) return false;
  const detail = (e.response?.data as { detail?: unknown } | undefined)?.detail;
  return e.status === 405 || (e.status === 404 && detail === 'Not Found');
}

function missingParts(e: unknown): number[] | null {
  if (!(e instanceof ApiError) || e.status !== 409) return null;
  const detail = (e.response?.data as { detail?: { missing_parts?: unknown } } | undefined)?.detail;
  const missing = detail && typeof detail === 'object' ? detail.missing_parts : null;
  return Array.isArray(missing) && missing.length ? (missing as number[]) : null;
}

/** Sends parts of one upload, a few at a time, each retried on a fresh URL. */
class PartSender {
  private readonly signed = new Map<number, { part: SignedUploadPart; until: number }>();
  private readonly signing = new Map<number, Promise<void>>();
  private readonly inFlight = new Map<number, number>();
  private readonly putting = new Set<number>();
  private readonly sent = new Map<number, number>();
  private queue: number[] = [];
  private lastPercent = -1;
  // What stopped the sends, kept because the aborts it causes can settle first.
  private failure: unknown = null;

  constructor(
    private readonly workspaceId: string,
    private readonly uploadId: string,
    private readonly file: File,
    private readonly ctrl: AbortController,
    private readonly onProgress?: (progress: UploadProgress) => void,
  ) {}

  async send(numbers: number[]): Promise<void> {
    // Parts sent again are counted again.
    for (const n of numbers) this.sent.delete(n);
    this.queue = [...numbers];
    this.report();
    const work = async () => {
      for (let n = this.queue.shift(); n !== undefined; n = this.queue.shift()) await this.sendPart(n);
    };
    const alive = setInterval(() => this.keepAlive(), KEEP_ALIVE_MS);
    try {
      await Promise.all(Array.from({ length: Math.min(PART_CONCURRENCY, numbers.length) }, work));
    } catch (e) {
      throw this.failure ?? e;
    } finally {
      clearInterval(alive);
    }
  }

  /**
   * Signs a part in flight again, its URL unused: signing is what keeps the
   * upload open and counts it as use of the computer. Best effort, since a
   * refusal that matters stops the next batch anyway.
   */
  private keepAlive(): void {
    const n = this.putting.values().next().value;
    if (n === undefined) return;
    signWorkspaceUploadParts(this.workspaceId, this.uploadId, [n], this.ctrl.signal).catch(() => {});
  }

  private fail(e: unknown): never {
    this.failure ??= e;
    this.ctrl.abort();
    throw e;
  }

  private async sendPart(n: number): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      if (this.ctrl.signal.aborted) throw canceled();
      let signing = true;
      try {
        // A retry asks for a new URL: the old one may be what failed.
        const part = await this.urlFor(n, attempt > 1);
        signing = false;
        await this.put(n, part);
        this.signed.delete(n);
        this.inFlight.delete(n);
        this.sent.set(n, part.size);
        this.report();
        return;
      } catch (e) {
        this.inFlight.delete(n);
        if (this.ctrl.signal.aborted) throw e;
        this.report();
        // The backend refusing a URL means the upload has ended; no retry brings it back.
        if (signing && e instanceof ApiError && e.status && e.status < 500 && e.status !== 429) this.fail(e);
        if (attempt >= PART_ATTEMPTS) this.fail(new UploadSendError());
        await delay(backoff(attempt), this.ctrl.signal);
      }
    }
  }

  /** One PUT, abandoned once no bytes have gone for PART_IDLE_MS; the retry loop sends it again. */
  private async put(n: number, part: SignedUploadPart): Promise<void> {
    const idle = new AbortController();
    const stop = () => idle.abort();
    if (this.ctrl.signal.aborted) stop();
    else this.ctrl.signal.addEventListener('abort', stop, { once: true });
    let timer = setTimeout(stop, PART_IDLE_MS);
    this.putting.add(n);
    try {
      await api.putSigned(part.url, this.file.slice(part.offset, part.offset + part.size), {
        signal: idle.signal,
        onUploadProgress: ({ loaded }) => {
          clearTimeout(timer);
          timer = setTimeout(stop, PART_IDLE_MS);
          this.report(n, Math.min(loaded, part.size));
        },
      });
    } finally {
      clearTimeout(timer);
      this.putting.delete(n);
      this.ctrl.signal.removeEventListener('abort', stop);
    }
  }

  /** A URL for part `n`, signed with the next few queued parts so most sends wait on no request. */
  private async urlFor(n: number, fresh: boolean): Promise<SignedUploadPart> {
    if (fresh) this.signed.delete(n);
    const pending = this.signing.get(n);
    if (!fresh && pending) await pending;
    const held = this.signed.get(n);
    if (held && held.until > Date.now()) return held.part;
    const batch = [
      n,
      ...this.queue.filter((m) => !this.signed.has(m) && !this.signing.has(m)).slice(0, SIGN_BATCH - 1),
    ];
    const request = this.sign(batch);
    for (const m of batch) this.signing.set(m, request);
    try {
      await request;
    } finally {
      for (const m of batch) if (this.signing.get(m) === request) this.signing.delete(m);
    }
    const got = this.signed.get(n);
    if (!got) throw new Error(`part ${n} was not signed`);
    return got.part;
  }

  private async sign(batch: number[]): Promise<void> {
    const { parts, expires_in } = await signWorkspaceUploadParts(this.workspaceId, this.uploadId, batch, this.ctrl.signal);
    // A minute short of the store's deadline, for a clock that runs behind it.
    const until = Date.now() + Math.max(expires_in - 60, 0) * 1000;
    for (const part of parts) this.signed.set(part.part_number, { part, until });
  }

  private report(n?: number, loaded?: number): void {
    if (n !== undefined && loaded !== undefined) this.inFlight.set(n, loaded);
    let bytes = 0;
    for (const v of this.sent.values()) bytes += v;
    for (const v of this.inFlight.values()) bytes += v;
    const percent = Math.min(100, Math.floor((bytes * 100) / (this.file.size || 1)));
    if (percent === this.lastPercent) return;
    this.lastPercent = percent;
    this.onProgress?.({ phase: 'sending', percent });
  }
}

async function complete(
  workspaceId: string,
  uploadId: string,
  parts: PartSender,
  signal: AbortSignal,
  onProgress: WorkspaceUploadOptions['onProgress'],
): Promise<UploadStatus> {
  for (let round = 1; ; round++) {
    // Said before each request, parts sent again or not: from here a cancel
    // would race the assembly, which a DELETE can no longer stop.
    onProgress?.({ phase: 'saving' });
    try {
      // Safe to repeat: completing again answers where the upload stands.
      return await completeWorkspaceUpload(workspaceId, uploadId, signal);
    } catch (e) {
      if (signal.aborted || round >= COMPLETE_ROUNDS) throw e;
      const missing = missingParts(e);
      if (missing) await parts.send(missing);
      else if (retryable(e)) await delay(2000 * round, signal);
      else throw e;
    }
  }
}

async function waitForEnd(
  workspaceId: string,
  uploadId: string,
  first: UploadStatus,
  signal: AbortSignal | undefined,
  completeAgain: () => Promise<UploadStatus>,
): Promise<UploadStatus> {
  let status = first;
  let failures = 0;
  while (!TERMINAL.has(status.state)) {
    await delay(POLL_MS, signal);
    try {
      // Back to uploading: assembling found the store silent, or short of
      // parts, which completing again sends.
      status = status.state === 'uploading'
        ? await completeAgain()
        : await getWorkspaceUpload(workspaceId, uploadId, signal);
      failures = 0;
    } catch (e) {
      if (signal?.aborted || !retryable(e) || ++failures >= POLL_FAILURES_MAX) throw e;
    }
  }
  return status;
}

async function sendDirect(
  workspaceId: string,
  file: File,
  begun: Extract<UploadBegin, { mode: 'direct' }>,
  onProgress: WorkspaceUploadOptions['onProgress'],
  signal: AbortSignal | undefined,
): Promise<{ path: string }> {
  const uploadId = begun.upload_id;
  const ctrl = new AbortController();
  const stop = () => ctrl.abort();
  if (signal?.aborted) ctrl.abort();
  else signal?.addEventListener('abort', stop, { once: true });
  let sending = true;
  // A page closed or reloaded mid-send takes the upload with it, so it ends
  // the row then rather than when the row would expire.
  let auth: Record<string, string> = {};
  const keepAuth = () => getAuthHeaders().then((h) => { auth = h; }, () => {});
  void keepAuth();
  const authTimer = setInterval(keepAuth, AUTH_REFRESH_MS);
  const onPageHide = (e: Event) => {
    if (sending && !(e as PageTransitionEvent).persisted) cancelWorkspaceUploadOnUnload(workspaceId, uploadId, auth);
  };
  window.addEventListener('pagehide', onPageHide);
  try {
    const parts = new PartSender(workspaceId, uploadId, file, ctrl, onProgress);
    await parts.send(Array.from({ length: begun.part_count }, (_, i) => i + 1));
    // The upload is open again whenever completing is due, here or once the
    // poll finds it handed back, so until it is accepted a failure ends it.
    const finish = async () => {
      sending = true;
      const accepted = await complete(workspaceId, uploadId, parts, ctrl.signal, onProgress);
      sending = false;
      return accepted;
    };
    const status = await waitForEnd(workspaceId, uploadId, await finish(), signal, finish);
    if (status.state !== 'done') throw new Error(status.error || 'Upload failed');
    return { path: status.path };
  } catch (e) {
    // Discards the parts now rather than when the upload would expire.
    if (sending) void cancelWorkspaceUpload(workspaceId, uploadId).catch(() => {});
    if (signal?.aborted) throw canceled();
    throw e;
  } finally {
    signal?.removeEventListener('abort', stop);
    clearInterval(authTimer);
    window.removeEventListener('pagehide', onPageHide);
  }
}

/** Upload `file` into the workspace; resolves once it is in the folder. */
export async function uploadToWorkspace(
  workspaceId: string,
  file: File,
  { path = null, onProgress, signal }: WorkspaceUploadOptions = {},
): Promise<{ path: string }> {
  // Not aborted with the rest: a start the server finished holds room on the
  // computer's disk until it ends, so a cancel that wins the race ends it.
  const starting = beginWorkspaceUpload(workspaceId, { path: path || file.name, size: file.size });
  let begun: UploadBegin;
  try {
    begun = await untilAborted(starting, signal);
  } catch (e) {
    if (signal?.aborted) {
      void starting
        .then((b) => (b.mode === 'direct' ? cancelWorkspaceUpload(workspaceId, b.upload_id) : undefined))
        .catch(() => {});
      throw e;
    }
    // A backend older than this page has no such route, and its one-request
    // path still takes the file.
    if (!routeMissing(e)) throw e;
    begun = { mode: 'relay' };
  }

  if (begun.mode === 'direct') return sendDirect(workspaceId, file, begun, onProgress, signal);

  let saving = false;
  const data = await uploadWorkspaceFile(workspaceId, file, path, (percent) => {
    if (saving) return;
    // The body is all sent; what remains is the backend writing it.
    if (percent >= 100) {
      saving = true;
      onProgress?.({ phase: 'saving' });
    } else {
      onProgress?.({ phase: 'sending', percent });
    }
  }, signal);
  return { path: data.path };
}
