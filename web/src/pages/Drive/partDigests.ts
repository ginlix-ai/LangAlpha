/**
 * The MD5 of each part of a file being sent to the Drive, read from the file
 * on disk and hashed in workers, each part once. A part signed or relayed
 * with its MD5 is refused by the store unless its bytes match, and a part the
 * store already holds is skipped on a resume only when its MD5 there is the
 * one the file gives now: that is how a resume proves it continues the same
 * file, not one that only shares its name, size, time and ends.
 *
 * A digest is null where hashing is not to be had: a worker that failed to
 * load (a tab older than a deploy, its chunk gone) or stopped answering. A
 * part without one goes without it, and a held part without one is sent
 * again, so an upload never waits on a hash that will not come.
 */
import { canceled } from '@/lib/partUpload';
import { md5 } from './md5';

/** Hashing is CPU bound; two workers keep ahead of a few parts in flight without taking every core. */
const WORKERS = 2;
/**
 * How long a worker may go silent before it counts as gone. The clock runs on
 * what it reports, not on the part: a slow disk or a cloud file still being
 * fetched keeps reporting bytes read and is waited on, a dead worker reports
 * nothing. The first bytes may take a while to come (a file the OS fetches
 * first); between reports, and for the hash once the bytes are in (16 MiB in
 * well under a second), far less.
 */
const FIRST_BYTES_MS = 60_000;
const SILENT_MS = 10_000;
/** Workers that fail before one gives a digest, after which parts go without one until the next upload. */
const MAX_STRIKES = 4;

type Reply = { id: number; digest: Uint8Array } | { id: number; read: number } | { id: number; error: string };

interface Job {
  id: number;
  blob: Blob;
  settle: (digest: Uint8Array | null) => void;
}

interface Slot {
  worker: Worker;
  job: Job | null;
  timer: ReturnType<typeof setTimeout> | undefined;
}

let slots: Slot[] | null = null;
let nextId = 1;
let strikes = 0;
/**
 * Parts waiting for a worker. Each worker reads one part at a time, so at
 * most WORKERS parts are in memory however many are asked for at once.
 */
const queue: Job[] = [];

function finish(slot: Slot, digest: Uint8Array | null): void {
  clearTimeout(slot.timer);
  const job = slot.job;
  slot.job = null;
  job?.settle(digest);
}

function watch(slot: Slot, ms: number): void {
  clearTimeout(slot.timer);
  slot.timer = setTimeout(() => retire(slot), ms);
}

/**
 * A worker that failed or went silent: the part it held goes without a
 * digest, and the parts after go to the others, or to new workers once none
 * is left, until MAX_STRIKES failures with no digest between them.
 */
function retire(slot: Slot): void {
  slot.worker.terminate();
  slots = slots?.filter((s) => s !== slot) ?? null;
  strikes += 1;
  finish(slot, null);
  // New workers only for parts waiting; with none, the next ask makes them.
  if (queue.length === 0) return;
  if (pool()?.length === 0) for (const job of queue.splice(0)) job.settle(null);
  else pump();
}

/** The workers, made anew once none is left while failures stay under MAX_STRIKES; empty past that. */
function pool(): Slot[] | null {
  if (typeof Worker === 'undefined') return null;
  if (slots?.length === 0 && strikes < MAX_STRIKES) slots = null;
  slots ??= Array.from({ length: WORKERS }, () => {
    const slot: Slot = {
      worker: new Worker(new URL('./md5.worker.ts', import.meta.url), { type: 'module' }),
      job: null,
      timer: undefined,
    };
    slot.worker.onmessage = (e: MessageEvent<Reply>) => {
      if (slot.job?.id !== e.data.id) return;
      if ('read' in e.data) return watch(slot, SILENT_MS);
      if ('digest' in e.data) strikes = 0;
      // A part the worker could not read (the file moved) has no digest; the next ask reads it again.
      finish(slot, 'digest' in e.data ? e.data.digest : null);
      pump();
    };
    slot.worker.onerror = () => retire(slot);
    slot.worker.onmessageerror = () => retire(slot);
    return slot;
  });
  return slots;
}

function pump(): void {
  for (const slot of slots ?? []) {
    if (slot.job) continue;
    const job = queue.shift();
    if (!job) return;
    slot.job = job;
    watch(slot, FIRST_BYTES_MS);
    slot.worker.postMessage({ id: job.id, blob: job.blob });
  }
}

/**
 * The MD5 of `blob`, in a worker, or null where no worker can give it. On
 * the calling thread only where there are no workers at all (a test).
 */
function digestOf(blob: Blob, signal: AbortSignal): Promise<Uint8Array | null> {
  const all = pool();
  if (!all) return blob.arrayBuffer().then((b) => md5(new Uint8Array(b)));
  if (all.length === 0) return Promise.resolve(null);
  return new Promise((resolve, reject) => {
    const job: Job = {
      id: nextId++,
      blob,
      settle: (digest) => {
        signal.removeEventListener('abort', onAbort);
        resolve(digest);
      },
    };
    // A cancelled upload's parts leave the queue; one being read finishes and is dropped.
    const onAbort = () => {
      const at = queue.indexOf(job);
      if (at >= 0) queue.splice(at, 1);
      reject(canceled());
    };
    if (signal.aborted) return onAbort();
    signal.addEventListener('abort', onAbort, { once: true });
    queue.push(job);
    pump();
  });
}

/**
 * Each part's MD5 for one file cut in `partSize` parts, hashed the first time
 * it is asked for, while `signal` (the upload's) has not fired.
 */
export function partDigests(file: File, partSize: number, signal: AbortSignal): (n: number) => Promise<Uint8Array | null> {
  // Each upload tries the workers afresh: a page whose workers failed for one may hash for the next.
  strikes = 0;
  const known = new Map<number, Promise<Uint8Array | null>>();
  return (n) => {
    let digest = known.get(n);
    if (!digest) {
      const offset = (n - 1) * partSize;
      digest = digestOf(file.slice(offset, Math.min(file.size, offset + partSize)), signal);
      // A part with no digest, or one whose read failed, is tried again on the next ask.
      digest.then((d) => d ?? known.delete(n), () => known.delete(n));
      known.set(n, digest);
    }
    return digest;
  };
}

/** For tests: a page as it loads, before any worker. */
export function resetDigestWorkers(): void {
  for (const slot of slots ?? []) slot.worker.terminate();
  slots = null;
  queue.length = 0;
  strikes = 0;
}
