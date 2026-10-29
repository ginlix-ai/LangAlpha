import { pdfjs } from 'react-pdf';
import type { PDFDataRangeTransport } from 'pdfjs-dist';
import type { RangedFile } from '@/lib/rangedFile';

/**
 * The reads sent at once: a browser holds a host to six connections over
 * HTTP/1.1 and queues the rest, where the panel's own requests would wait
 * behind every range pdf.js asked for. Queued here instead, the ranges a
 * closed document still had waiting are dropped before they are sent.
 */
const MAX_IN_FLIGHT = 6;

/** Each told which bytes, `[begin, end)`, the slice held. */
interface RangeListeners {
  /** A slice arrived. */
  onRead: (begin: number, end: number) => void;
  onFail: (error: unknown, begin: number, end: number) => void;
}

/**
 * How pdf.js reads a ranged file. Its transport has no way to fail a request,
 * so a slice that could not be read would leave the document waiting forever;
 * `onFail` hears of it instead.
 */
export function rangeTransport(file: RangedFile, { onRead, onFail }: RangeListeners): PDFDataRangeTransport {
  // pdf.js hands what it is given to its worker, which detaches it, and the
  // cached file keeps its own bytes for the next document.
  const transport = new pdfjs.PDFDataRangeTransport(file.length, file.initial.slice());
  // pdf.js aborts the transport when it destroys the document, and nothing
  // asked for before then is wanted after.
  const stop = new AbortController();
  const queue: [begin: number, end: number][] = [];
  let inFlight = 0;

  const pump = () => {
    while (inFlight < MAX_IN_FLIGHT && queue.length > 0 && !stop.signal.aborted) {
      const [begin, end] = queue.shift()!;
      inFlight += 1;
      const settle = () => {
        inFlight -= 1;
        pump();
      };
      file.read(begin, end, stop.signal).then(
        (bytes) => {
          settle();
          if (stop.signal.aborted) return;
          onRead(begin, end);
          // pdf.js drops a closing document's readers before it aborts the
          // transport, and a slice handed to a reader that is gone throws.
          try {
            transport.onDataRange(begin, bytes);
          } catch {
            /* the document it was for is closing */
          }
        },
        (error: unknown) => {
          settle();
          if (!stop.signal.aborted) onFail(error, begin, end);
        },
      );
    }
  };

  transport.requestDataRange = (begin: number, end: number) => {
    queue.push([begin, end]);
    pump();
  };
  transport.abort = () => {
    stop.abort();
    queue.length = 0;
  };
  return transport;
}
