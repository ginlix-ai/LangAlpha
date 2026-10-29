// @vitest-environment node
/**
 * The transport pdf.js reads a ranged file through: at most six reads out at
 * once and the rest sent in the order pdf.js asked, nothing sent or delivered
 * once the document is gone, and pdf.js given a first slice of its own.
 */
import { describe, it, expect, vi } from 'vitest';
import type { RangedFile } from '@/lib/rangedFile';
import { rangeTransport } from '../rangeTransport';

vi.mock('react-pdf', () => ({
  pdfjs: {
    PDFDataRangeTransport: class {
      delivered: [number, Uint8Array][] = [];
      /** What a closing document leaves before pdf.js aborts the transport. */
      readersGone = false;
      constructor(public length: number, public initialData: Uint8Array) {}
      onDataRange(begin: number, chunk: Uint8Array) {
        if (this.readersGone) throw new Error('#onReceiveData - no `PDFDataTransportStreamRangeReader` instance found.');
        this.delivered.push([begin, chunk]);
      }
    },
  },
}));

interface FakeTransport {
  initialData: Uint8Array;
  delivered: [number, Uint8Array][];
  readersGone: boolean;
  requestDataRange: (begin: number, end: number) => void;
  abort: () => void;
}

interface Read {
  begin: number;
  signal?: AbortSignal;
  settled: boolean;
  resolve: (bytes: Uint8Array) => void;
  reject: (error: unknown) => void;
}

/** A file whose reads stay out until the test settles them. */
function setup(initial = new Uint8Array([1, 2, 3])) {
  const reads: Read[] = [];
  const file: RangedFile = {
    length: 1000,
    etag: '"v1"',
    initial,
    read: (begin, _end, signal) => new Promise((resolve, reject) => {
      const read: Read = {
        begin,
        signal,
        settled: false,
        resolve: (bytes) => { read.settled = true; resolve(bytes); },
        reject: (error) => { read.settled = true; reject(error); },
      };
      reads.push(read);
    }),
  };
  const onRead = vi.fn();
  const onFail = vi.fn();
  const transport = rangeTransport(file, { onRead, onFail }) as unknown as FakeTransport;
  const outstanding = () => reads.filter((r) => !r.settled).length;
  return { reads, transport, onRead, onFail, outstanding };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('rangeTransport', () => {
  it('sends at most six reads at once, and the rest in the order pdf.js asked', async () => {
    const { reads, transport, onFail, outstanding } = setup();
    for (let i = 0; i < 10; i++) transport.requestDataRange(i * 10, i * 10 + 10);
    expect(reads.map((r) => r.begin)).toEqual([0, 10, 20, 30, 40, 50]);

    reads[2].resolve(new Uint8Array(10));
    await flush();
    expect(reads.map((r) => r.begin)).toEqual([0, 10, 20, 30, 40, 50, 60]);

    // A failed read frees its place as a delivered one does.
    const failure = new Error('lost');
    reads[0].reject(failure);
    await flush();
    expect(onFail).toHaveBeenCalledWith(failure, 0, 10);

    while (outstanding() > 0) {
      expect(outstanding()).toBeLessThanOrEqual(6);
      reads.find((r) => !r.settled)!.resolve(new Uint8Array(10));
      await flush();
    }
    expect(reads.map((r) => r.begin)).toEqual([0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);
  });

  it('hands each slice to pdf.js where it begins, and reports which bytes it read', async () => {
    const { reads, transport, onRead } = setup();
    transport.requestDataRange(40, 50);
    const bytes = new Uint8Array(10).fill(7);
    reads[0].resolve(bytes);
    await flush();
    expect(transport.delivered).toEqual([[40, bytes]]);
    expect(onRead).toHaveBeenCalledTimes(1);
    expect(onRead).toHaveBeenCalledWith(40, 50);
  });

  it('drops what was queued and what arrives after an abort', async () => {
    const { reads, transport, onRead, onFail } = setup();
    for (let i = 0; i < 8; i++) transport.requestDataRange(i * 10, i * 10 + 10);
    transport.abort();
    expect(reads.every((r) => r.signal?.aborted)).toBe(true);

    reads[0].resolve(new Uint8Array(10));
    reads[1].reject(new Error('canceled'));
    for (const read of reads.slice(2)) read.resolve(new Uint8Array(10));
    await flush();
    expect(reads).toHaveLength(6);
    expect(transport.delivered).toEqual([]);
    expect(onRead).not.toHaveBeenCalled();
    expect(onFail).not.toHaveBeenCalled();
  });

  it('lets a slice go quietly when its reader closed before the abort', async () => {
    const { reads, transport, onFail } = setup();
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    try {
      transport.requestDataRange(40, 50);
      // pdf.js cancels a closing document's readers first, and aborts the
      // transport only once its worker confirms the close.
      transport.readersGone = true;
      reads[0].resolve(new Uint8Array(10));
      await flush();
      await flush();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
    expect(unhandled).not.toHaveBeenCalled();
    expect(onFail).not.toHaveBeenCalled();
    expect(transport.delivered).toEqual([]);
  });

  it('gives pdf.js its own copy of the first slice', () => {
    const initial = new Uint8Array([1, 2, 3]);
    const { transport } = setup(initial);
    expect(transport.initialData).toEqual(initial);
    // pdf.js transfers what it is given to its worker, which detaches it; the
    // file keeps its bytes for the next document.
    structuredClone(transport.initialData, { transfer: [transport.initialData.buffer] });
    expect(transport.initialData.byteLength).toBe(0);
    expect(Array.from(initial)).toEqual([1, 2, 3]);
  });
});
