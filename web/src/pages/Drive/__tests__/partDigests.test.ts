/**
 * Part digests against workers that answer, fail to load, or go quiet: a
 * digest always comes (null where no worker can give it), a cancel stops the
 * wait, and no more parts are read at once than there are workers.
 */
import { createHash } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { partDigests, resetDigestWorkers } from '../partDigests';

type Ask = { id: number; blob: Blob };
type Behavior = (w: FakeWorker, ask: Ask) => void;

let behave: Behavior;
let workers: FakeWorker[];
let reading = 0;
let mostReading = 0;

class FakeWorker {
  onmessage: ((e: MessageEvent) => void) | null = null;
  onerror: ((e: Event) => void) | null = null;
  onmessageerror: ((e: MessageEvent) => void) | null = null;
  terminated = false;
  constructor() {
    workers.push(this);
  }
  postMessage(ask: Ask) {
    behave(this, ask);
  }
  terminate() {
    this.terminated = true;
  }
}

/** Answers each part with its MD5, a tick later, counting the parts being read at once. */
const answers: Behavior = (w, { id, blob }) => {
  reading += 1;
  mostReading = Math.max(mostReading, reading);
  void blob.arrayBuffer().then((b) => {
    setTimeout(() => {
      reading -= 1;
      w.onmessage?.({ data: { id, digest: new Uint8Array(createHash('md5').update(new Uint8Array(b)).digest()) } } as MessageEvent);
    }, 5);
  });
};

const PART = 16;
const bytes = Uint8Array.from({ length: 10 * PART }, (_, i) => i);
const file = new File([bytes], 'f.bin');
const hex = (d: Uint8Array | null) => (d ? Buffer.from(d).toString('hex') : null);
const want = (n: number) => createHash('md5').update(bytes.subarray((n - 1) * PART, n * PART)).digest('hex');

beforeEach(() => {
  workers = [];
  reading = 0;
  mostReading = 0;
  behave = answers;
  resetDigestWorkers();
  vi.stubGlobal('Worker', FakeWorker);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('partDigests', () => {
  it('reads no more parts at once than there are workers, however many are asked for', async () => {
    const md5Of = partDigests(file, PART, new AbortController().signal);
    const all = await Promise.all(Array.from({ length: 10 }, (_, i) => md5Of(i + 1)));
    expect(all.map(hex)).toEqual(Array.from({ length: 10 }, (_, i) => want(i + 1)));
    expect(mostReading).toBeLessThanOrEqual(2);
  });

  it('gives no digest, rather than waiting for ever, when the workers fail to load', async () => {
    behave = (w) => setTimeout(() => w.onerror?.(new Event('error')), 1);
    const md5Of = partDigests(file, PART, new AbortController().signal);
    expect(await Promise.all([md5Of(1), md5Of(2), md5Of(3)])).toEqual([null, null, null]);
    // Later parts do not wait on workers that never load.
    expect(await md5Of(4)).toBeNull();
  });

  it('gives up on a worker that stops answering, and carries on with the other', async () => {
    vi.useFakeTimers();
    behave = (w, ask) => (w === workers[0] ? undefined : answers(w, ask));
    const md5Of = partDigests(file, PART, new AbortController().signal);
    const got = Promise.all([md5Of(1), md5Of(2), md5Of(3)]);
    await vi.advanceTimersByTimeAsync(70_000);
    const [first, second, third] = await got;
    expect(first).toBeNull();
    expect([hex(second), hex(third)]).toEqual([want(2), want(3)]);
    expect(workers[0].terminated).toBe(true);
  });

  it('stops waiting when the upload is cancelled', async () => {
    behave = () => {};
    const ctrl = new AbortController();
    const md5Of = partDigests(file, PART, ctrl.signal);
    const pending = Promise.allSettled([md5Of(1), md5Of(2), md5Of(3)]);
    ctrl.abort();
    const settled = await pending;
    expect(settled.map((s) => s.status)).toEqual(['rejected', 'rejected', 'rejected']);
  });

  it('makes new workers once the old ones stopped answering, so later parts still get their digests', async () => {
    vi.useFakeTimers();
    let dead = true;
    behave = (w, ask) => (dead ? undefined : answers(w, ask));
    const md5Of = partDigests(file, PART, new AbortController().signal);
    const first = Promise.all([md5Of(1), md5Of(2)]);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(await first).toEqual([null, null]);
    dead = false;
    const later = md5Of(3);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(hex(await later)).toBe(want(3));
  });

  it('waits on a slow read that keeps coming, and times only the hash', async () => {
    vi.useFakeTimers();
    // A cloud placeholder or a slow disk: the bytes come in over 40 seconds, then the hash.
    behave = (w, { id, blob }) => {
      for (let s = 5; s <= 40; s += 5) setTimeout(() => w.onmessage?.({ data: { id, read: (blob.size * s) / 40 } } as MessageEvent), s * 1000);
      setTimeout(() => answers(w, { id, blob }), 40_000);
    };
    const md5Of = partDigests(file, PART, new AbortController().signal);
    const got = md5Of(1);
    await vi.advanceTimersByTimeAsync(41_000);
    expect(hex(await got)).toBe(want(1));
    expect(workers.some((w) => w.terminated)).toBe(false);
  });

  it('stops making workers that never load, rather than one per part', async () => {
    behave = (w) => setTimeout(() => w.onerror?.(new Event('error')), 1);
    const md5Of = partDigests(file, PART, new AbortController().signal);
    expect(new Set(await Promise.all(Array.from({ length: 10 }, (_, i) => md5Of(i + 1))))).toEqual(new Set([null]));
    expect(workers.length).toBeLessThanOrEqual(6);
  });
});
