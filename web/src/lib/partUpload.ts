/**
 * Sending a file to the server, shared by workspace uploads, chat attachments
 * and the Drive. Each caller owns its own routes (begin, sign or relay,
 * complete, cancel) and hands them in; what is here is how an upload goes: its
 * start raced with a cancel, the parts a few at a time, each retried on a
 * fresh URL or through the server, given up only when no bytes move, and the
 * upload ended on the server whenever it fails, is cancelled, or its page goes
 * away while it is open.
 */
import { api, ApiError, delay } from '@/api/client';
import { apiErrorDetail, apiErrorDetailMessage, apiErrorStatus } from '@/api/errors';
import { getAuthHeaders } from '@/lib/authToken';

export interface SignedUploadPart {
  part_number: number;
  offset: number;
  size: number;
  url: string;
  /** Sent verbatim with the part, where the store signed them into the URL's terms. */
  headers?: Record<string, string>;
}

/** The bytes are in and the server is taking the file, which a cancel no longer stops. */
type Saving = { phase: 'saving' };

/** An upload in parts, which counts its bytes: those the store holds or is taking. */
export type PartProgress = { phase: 'sending'; percent: number; loaded: number } | Saving;

/** A one-request upload, which knows only how far its body has gone. */
export type RelayProgress = { phase: 'sending'; percent: number } | Saving;

export type UploadProgress = PartProgress | RelayProgress;

/** The parts could not be sent, retries and all: the connection, not the server, is the likely cause. */
export class UploadSendError extends Error {
  constructor() {
    super('Upload failed');
    this.name = 'UploadSendError';
  }
}

/** The server took the upload, then did not keep it; `reason` is its words, when it gave any. */
export class UploadRefusedError extends Error {
  readonly reason: string | null;
  constructor(reason: string | null) {
    super(reason || 'Upload failed');
    this.name = 'UploadRefusedError';
    this.reason = reason;
  }
}

/** Why an upload failed, for each surface to put in its own words. */
export type UploadFailure =
  /** The parts could not be sent: likely the connection. */
  | { kind: 'send' }
  /** A 413 with no words, from something in front of the server. */
  | { kind: 'tooLarge' }
  /** The server said no; `message` is what it said, shown as given. */
  | { kind: 'refused'; message: string | null }
  | { kind: 'unknown' };

export function classifyUploadError(err: unknown): UploadFailure {
  if (err instanceof UploadSendError) return { kind: 'send' };
  if (err instanceof UploadRefusedError) return { kind: 'refused', message: err.reason };
  const detail = apiErrorDetail(err);
  if (typeof detail === 'string' && detail) return { kind: 'refused', message: detail };
  const message = apiErrorDetailMessage(err);
  if (message) return { kind: 'refused', message };
  if (apiErrorStatus(err) === 413) return { kind: 'tooLarge' };
  return { kind: 'unknown' };
}

/** Signs `partNumbers` of one upload. */
type SignParts = (
  partNumbers: number[],
  signal: AbortSignal,
) => Promise<{ parts: SignedUploadPart[]; expires_in: number }>;

/** Sends part `n`'s bytes through the server, which passes them into the store. */
export type RelayPart = (
  n: number,
  body: Blob,
  config: { signal: AbortSignal; onUploadProgress: (e: { loaded: number }) => void },
) => Promise<unknown>;

/**
 * How one upload's parts travel: to the store on signed URLs, through the
 * server, or both, where once a direct PUT gets no answer at all (a bucket
 * without CORS, a firewall, the store's host unreachable) that part and every
 * part after it go through the server instead.
 */
export type PartRoutes =
  | { kind: 'signed'; sign: SignParts }
  | { kind: 'relay'; relay: RelayPart }
  | { kind: 'signedWithFallback'; sign: SignParts; relay: RelayPart };

type SendBody = (body: Blob, config: { signal: AbortSignal; onUploadProgress: (e: { loaded: number }) => void }) => Promise<unknown>;

/** One try at sending one part: which bytes, how, and whether a refusal on the way ends the upload. */
interface PartTrip {
  offset: number;
  size: number;
  send: SendBody;
  /** The upload has ended: a refusal from the backend, which no retry brings back. */
  ends: (e: unknown) => boolean;
}

/** The way parts go for one upload. */
interface PartTransport {
  /** Part `n`'s next try; `fresh` asks for a new address, since the old one may be what failed. A refusal here is the backend's. */
  transportFor(n: number, fresh: boolean): Promise<PartTrip>;
  /** Tells the backend the upload is still going while a part takes long. */
  keepAlive(n: number): void;
}

/** The backend refusing a URL or a part: the upload has ended. */
const backendRefusal = (e: unknown) => e instanceof ApiError && !!e.status && e.status < 500 && e.status !== 429;

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
// A part holds its page-wide turn while its URL is signed, so a signing that
// hangs is given up after this long and retried, giving the turn back between.
export const SIGN_DEADLINE_MS = 30_000;
// Part PUTs going at once across every direct upload on the page. A browser
// runs about six requests to one host over HTTP/1.1 and queues the rest, and a
// queued request reports no progress, so its idle clock would run out before
// it starts and spend a retry. Parts wait their turn here instead, where the
// clock has not started.
export const PART_SLOTS = 6;
let partsGoing = 0;
const partTurns: Array<() => void> = [];

function turnRelease(): () => void {
  let released = false;
  return () => {
    if (released) return;
    released = true;
    partsGoing--;
    partTurns.shift()?.();
  };
}

/** A part's turn to send if one is free now, else null. */
function partTurnNow(): (() => void) | null {
  if (partsGoing >= PART_SLOTS) return null;
  partsGoing++;
  return turnRelease();
}

/** Waits for a part's turn to send; resolves with its release. */
function partTurn(signal: AbortSignal): Promise<() => void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(canceled());
    const now = partTurnNow();
    if (now) return resolve(now);
    const onAbort = () => {
      const at = partTurns.indexOf(go);
      if (at !== -1) partTurns.splice(at, 1);
      reject(canceled());
    };
    const go = () => {
      signal.removeEventListener('abort', onAbort);
      partsGoing++;
      resolve(turnRelease());
    };
    partTurns.push(go);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

export const canceled = () => new ApiError('canceled', 'ERR_CANCELED');


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
export function retryable(e: unknown): boolean {
  if (!(e instanceof ApiError)) return false;
  if (e.code === 'ERR_NETWORK' || e.code === 'ECONNABORTED') return true;
  return e.status === 429 || (e.status ?? 0) >= 500;
}

function missingParts(e: unknown): number[] | null {
  if (!(e instanceof ApiError) || e.status !== 409) return null;
  const detail = (e.response?.data as { detail?: { missing_parts?: unknown } } | undefined)?.detail;
  const missing = detail && typeof detail === 'object' ? detail.missing_parts : null;
  return Array.isArray(missing) && missing.length ? (missing as number[]) : null;
}

/** Part numbers 1 through `count`. */
const allParts = (count: number) => Array.from({ length: count }, (_, i) => i + 1);

/** Parts through the server, each cut from the file by its number. */
function relayTransport(relay: RelayPart, partSize: number, fileSize: number): PartTransport {
  return {
    async transportFor(n) {
      const offset = (n - 1) * partSize;
      return { offset, size: Math.min(partSize, fileSize - offset), send: (body, config) => relay(n, body, config), ends: backendRefusal };
    },
    keepAlive() {},
  };
}

/** Parts to the store on signed URLs, each signed with the next few waiting so most sends wait on no request. */
class SignedTransport implements PartTransport {
  private readonly signed = new Map<number, { part: SignedUploadPart; until: number }>();
  private readonly signing = new Map<number, Promise<void>>();

  constructor(
    private readonly signParts: SignParts,
    private readonly signal: AbortSignal,
    /** Parts waiting their turn or queued, nearest first: signed along with the one asked for. */
    private readonly upcoming: () => number[],
  ) {}

  async transportFor(n: number, fresh: boolean): Promise<PartTrip> {
    const part = await this.urlFor(n, fresh);
    return {
      offset: part.offset,
      size: part.size,
      send: async (body, config) => {
        await api.putSigned(part.url, body, { ...config, headers: part.headers });
        this.signed.delete(n);
      },
      // The store's refusal is a URL gone stale or a passing fault; the retry signs again.
      ends: () => false,
    };
  }

  /** Signs the part again, its URL unused: signing is what keeps the upload open and counts it as use. Best effort. */
  keepAlive(n: number): void {
    this.signParts([n], this.signal).catch(() => {});
  }

  private async urlFor(n: number, fresh: boolean): Promise<SignedUploadPart> {
    if (fresh) this.signed.delete(n);
    const pending = this.signing.get(n);
    if (!fresh && pending) await pending;
    const held = this.signed.get(n);
    if (held && held.until > Date.now()) return held.part;
    const batch = [
      n,
      ...this.upcoming().filter((m) => m !== n && !this.signed.has(m) && !this.signing.has(m)).slice(0, SIGN_BATCH - 1),
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
    const deadline = new AbortController();
    const stop = () => deadline.abort();
    if (this.signal.aborted) stop();
    else this.signal.addEventListener('abort', stop, { once: true });
    let late = false;
    const timer = setTimeout(() => {
      late = true;
      deadline.abort();
    }, SIGN_DEADLINE_MS);
    let signed: Awaited<ReturnType<SignParts>>;
    try {
      signed = await untilAborted(this.signParts(batch, deadline.signal), deadline.signal);
    } catch (e) {
      // A deadline, as the retry loop counts one, not a cancel of the upload.
      if (late && !this.signal.aborted) throw new ApiError(`timeout of ${SIGN_DEADLINE_MS}ms exceeded`, 'ECONNABORTED');
      throw e;
    } finally {
      clearTimeout(timer);
      this.signal.removeEventListener('abort', stop);
    }
    const { parts, expires_in } = signed;
    // A minute short of the store's deadline, for a clock that runs behind it.
    const until = Date.now() + Math.max(expires_in - 60, 0) * 1000;
    for (const part of parts) this.signed.set(part.part_number, { part, until });
  }
}

/**
 * Signed URLs until a PUT gets no answer at all, which is what a store the
 * page may not reach looks like: that part goes again at once through the
 * server, and so does every part after it.
 */
function fallbackTransport(direct: SignedTransport, viaServer: PartTransport, signal: AbortSignal): PartTransport {
  let switched = false;
  return {
    async transportFor(n, fresh) {
      if (switched) return viaServer.transportFor(n, fresh);
      const trip = await direct.transportFor(n, fresh);
      let relayed = false;
      return {
        ...trip,
        send: async (body, config) => {
          try {
            return await trip.send(body, config);
          } catch (e) {
            if (!(e instanceof ApiError) || signal.aborted) throw e;
            // No answer (a firewall, or a preflight the bucket's CORS refuses): the rest goes through the server.
            if (e.code === 'ERR_NETWORK') switched = true;
            // An answer refusing a part signed afresh (a store or proxy that will not take the signed
            // headers): that part goes through the server, which checks the same digest.
            else if (!(fresh && e.status !== undefined && e.status >= 400 && e.status < 500)) throw e;
            relayed = true;
            return (await viaServer.transportFor(n, false)).send(body, config);
          }
        },
        ends: (e) => relayed && backendRefusal(e),
      };
    },
    keepAlive: (n) => direct.keepAlive(n),
  };
}

function transportOf(routes: PartRoutes, partSize: number, fileSize: number, signal: AbortSignal, upcoming: () => number[]): PartTransport {
  switch (routes.kind) {
    case 'relay':
      return relayTransport(routes.relay, partSize, fileSize);
    case 'signed':
      return new SignedTransport(routes.sign, signal, upcoming);
    case 'signedWithFallback':
      return fallbackTransport(new SignedTransport(routes.sign, signal, upcoming), relayTransport(routes.relay, partSize, fileSize), signal);
  }
}

/** Sends parts of one upload, a few at a time, each retried. */
class PartSender {
  private readonly transport: PartTransport;
  private readonly inFlight = new Map<number, number>();
  private readonly putting = new Set<number>();
  // Parts waiting for their turn (`partTurn`), which keep the upload open too.
  private readonly waiting = new Set<number>();
  private readonly sent = new Map<number, number>();
  private queue: number[] = [];
  private lastPercent = -1;
  // What stopped the sends, kept because the aborts it causes can settle first.
  private failure: unknown = null;

  constructor(
    private readonly file: File,
    routes: PartRoutes,
    partSize: number,
    private readonly ctrl: AbortController,
    private readonly onProgress?: (progress: PartProgress) => void,
    /** Parts the store already has, from an upload continued: counted as sent and never sent again. */
    held: ReadonlyArray<{ part_number: number; size: number }> = [],
  ) {
    for (const part of held) this.sent.set(part.part_number, part.size);
    this.transport = transportOf(routes, partSize, file.size, ctrl.signal, () => [...this.waiting, ...this.queue]);
  }

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

  /** A part in flight, or waiting for its turn, keeps the upload open. */
  private keepAlive(): void {
    const n = this.putting.values().next().value ?? this.waiting.values().next().value;
    if (n !== undefined) this.transport.keepAlive(n);
  }

  private fail(e: unknown): never {
    this.failure ??= e;
    this.ctrl.abort();
    throw e;
  }

  private async sendPart(n: number): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      if (this.ctrl.signal.aborted) throw canceled();
      let trip: PartTrip | null = null;
      let release: (() => void) | null = null;
      try {
        // Taken at once when free, so the parts starting together sign together.
        release = partTurnNow();
        if (!release) {
          this.waiting.add(n);
          try {
            release = await partTurn(this.ctrl.signal);
          } finally {
            this.waiting.delete(n);
          }
        }
        // Asked for after the wait, so a long one does not leave a stale URL.
        trip = await this.transport.transportFor(n, attempt > 1);
        await this.put(n, trip);
        this.inFlight.delete(n);
        this.sent.set(n, trip.size);
        this.report();
        return;
      } catch (e) {
        this.inFlight.delete(n);
        if (this.ctrl.signal.aborted) throw e;
        this.report();
        // Before a trip, the backend was being asked; after, the trip says whose refusal it was.
        if (trip ? trip.ends(e) : backendRefusal(e)) this.fail(e);
        if (attempt >= PART_ATTEMPTS) this.fail(new UploadSendError());
      } finally {
        release?.();
      }
      // The backoff gives up the part's turn, so others send meanwhile.
      await delay(backoff(attempt), this.ctrl.signal);
    }
  }

  /** One PUT, abandoned once no bytes have gone for PART_IDLE_MS; the retry loop sends it again. */
  private async put(n: number, { offset, size, send }: PartTrip): Promise<void> {
    const idle = new AbortController();
    const stop = () => idle.abort();
    if (this.ctrl.signal.aborted) stop();
    else this.ctrl.signal.addEventListener('abort', stop, { once: true });
    let timer = setTimeout(stop, PART_IDLE_MS);
    this.putting.add(n);
    try {
      // A slice is a view of the file on disk, so a part's bytes are read only as they go.
      await send(this.file.slice(offset, offset + size), {
        signal: idle.signal,
        onUploadProgress: ({ loaded }) => {
          clearTimeout(timer);
          timer = setTimeout(stop, PART_IDLE_MS);
          this.report(n, Math.min(loaded, size));
        },
      });
    } finally {
      clearTimeout(timer);
      this.putting.delete(n);
      this.ctrl.signal.removeEventListener('abort', stop);
    }
  }

  private report(n?: number, loaded?: number): void {
    if (n !== undefined && loaded !== undefined) this.inFlight.set(n, loaded);
    let bytes = 0;
    for (const v of this.sent.values()) bytes += v;
    for (const v of this.inFlight.values()) bytes += v;
    const percent = Math.min(100, Math.floor((bytes * 100) / (this.file.size || 1)));
    if (percent === this.lastPercent) return;
    this.lastPercent = percent;
    this.onProgress?.({ phase: 'sending', percent, loaded: bytes });
  }
}

/**
 * Asks the backend to assemble the parts, sending again any it says the store
 * lacks, and retrying an answer that did not come.
 */
async function completeParts<T>(
  complete: (signal: AbortSignal) => Promise<T>,
  parts: PartSender,
  signal: AbortSignal,
  onProgress?: (progress: PartProgress) => void,
): Promise<T> {
  for (let round = 1; ; round++) {
    // Said before each request, parts sent again or not: from here a cancel
    // would race the assembly, which a DELETE can no longer stop.
    onProgress?.({ phase: 'saving' });
    try {
      // Safe to repeat: completing again answers where the upload stands.
      return await complete(signal);
    } catch (e) {
      if (signal.aborted || round >= COMPLETE_ROUNDS) throw e;
      const missing = missingParts(e);
      if (missing) await parts.send(missing);
      else if (retryable(e)) await delay(2000 * round, signal);
      else throw e;
    }
  }
}

/**
 * Ends an upload with a request that outlives the page when the page goes away
 * while `open()` holds; one kept for the back-forward cache may come back and
 * carry on. Returns the release.
 */
function endOnPageHide(open: () => boolean, end: (headers: Record<string, string>) => void): () => void {
  let auth: Record<string, string> = {};
  const keepAuth = () => getAuthHeaders().then((h) => { auth = h; }, () => {});
  void keepAuth();
  const authTimer = setInterval(keepAuth, AUTH_REFRESH_MS);
  const onPageHide = (e: Event) => {
    if (open() && !(e as PageTransitionEvent).persisted) end(auth);
  };
  window.addEventListener('pagehide', onPageHide);
  return () => {
    clearInterval(authTimer);
    window.removeEventListener('pagehide', onPageHide);
  };
}

/**
 * Awaits an upload's start, which is not aborted with the rest: a start the
 * server finished holds room until it ends, so a cancel that wins the race
 * hands what began to `end`.
 */
export async function beginUnlessAborted<T>(
  starting: Promise<T>,
  signal: AbortSignal | undefined,
  end: (begun: T) => Promise<unknown> | undefined,
): Promise<T> {
  try {
    return await untilAborted(starting, signal);
  } catch (e) {
    if (signal?.aborted) void starting.then(end).catch(() => {});
    throw e;
  }
}

/** Progress from a one-request upload's percent: once the body is all sent, the server is storing it. */
export function relayProgress(onProgress?: (progress: RelayProgress) => void): (percent: number) => void {
  let saving = false;
  return (percent) => {
    if (saving) return;
    if (percent >= 100) {
      saving = true;
      onProgress?.({ phase: 'saving' });
    } else {
      onProgress?.({ phase: 'sending', percent });
    }
  };
}

/** How long an open upload lives. */
export type UploadLifetime =
  /**
   * Its page's: it ends on any failure, and when the page goes away, by
   * `cancelOnUnload`, a request that outlives the page.
   */
  | { kind: 'page'; cancelOnUnload: (headers: Record<string, string>) => void }
  /**
   * Until it is finished or cancelled, for a caller that remembers it and
   * continues it from the parts the store holds, from this page or another: a
   * failure `keepOpen` names leaves it open. A cancel still ends it.
   */
  | { kind: 'resumable'; keepOpen: (e: unknown) => boolean };

interface OpenUploadRoutes {
  /** Ends the upload on the server, discarding what was sent. */
  cancel: () => Promise<unknown>;
  lifetime: UploadLifetime;
}

/**
 * Runs `send` on an upload the server holds open until it ends. While `send`
 * keeps it open (from the start, and whenever it calls `setOpen(true)`), a
 * failure or cancel ends it now rather than when it would expire, and so does
 * the page going away where the routes say how. `ctrl` aborts with `signal`.
 */
export async function whileOpen<T>(
  routes: OpenUploadRoutes,
  signal: AbortSignal | undefined,
  send: (ctrl: AbortController, setOpen: (open: boolean) => void) => Promise<T>,
): Promise<T> {
  const ctrl = new AbortController();
  const stop = () => ctrl.abort();
  if (signal?.aborted) ctrl.abort();
  else signal?.addEventListener('abort', stop, { once: true });
  let open = true;
  const { lifetime } = routes;
  const releasePageHide = lifetime.kind === 'page' ? endOnPageHide(() => open, lifetime.cancelOnUnload) : () => {};
  try {
    const result = await send(ctrl, (next) => { open = next; });
    open = false;
    return result;
  } catch (e) {
    const keptOpen = lifetime.kind === 'resumable' && lifetime.keepOpen(e);
    if (open && (signal?.aborted || !keptOpen)) void routes.cancel().catch(() => {});
    if (signal?.aborted) throw canceled();
    throw e;
  } finally {
    signal?.removeEventListener('abort', stop);
    releasePageHide();
  }
}

type PartUploadRoutes<T> = OpenUploadRoutes & {
  parts: PartRoutes;
  /** Asks the server to assemble the parts; safe to repeat. */
  complete: (signal: AbortSignal) => Promise<T>;
};

/**
 * Sends `file` in `parts.count` parts of `parts.size` and completes it,
 * skipping those `parts.held` names. `settle` follows the accepted completion
 * with the upload no longer open; its `again` completes once more, open again
 * until accepted, for a server that hands it back.
 */
export function runPartUpload<T, R>(
  file: File,
  parts: { count: number; size: number; held?: ReadonlyArray<{ part_number: number; size: number }> },
  routes: PartUploadRoutes<T>,
  { onProgress, signal }: { onProgress?: (progress: PartProgress) => void; signal?: AbortSignal },
  settle: (accepted: T, again: () => Promise<T>) => Promise<R> | R,
): Promise<R> {
  return whileOpen(routes, signal, async (ctrl, setOpen) => {
    const sender = new PartSender(file, routes.parts, parts.size, ctrl, onProgress, parts.held);
    const held = new Set(parts.held?.map((p) => p.part_number));
    await sender.send(allParts(parts.count).filter((n) => !held.has(n)));
    const finish = async () => {
      setOpen(true);
      const accepted = await completeParts(routes.complete, sender, ctrl.signal, onProgress);
      setOpen(false);
      return accepted;
    };
    return settle(await finish(), finish);
  });
}
