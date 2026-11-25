import { api, ApiError, type RequestConfig } from '@/api/client';
import { FileChangedError, markStale, parseContentRange, type RangedFile } from './rangedFile';

// Apart from rangedFile.ts, which holds the file's shape and stays free of the
// API client, so the PDF viewer's range code and its tests do not load it.

/** The opening request's size. A file that fits arrives whole, in the one request it always took. */
const FIRST_SLICE_BYTES = 4 * 1024 * 1024;
/** The wait before each retry of a slice that failed in transit or on a server error. */
const SLICE_RETRY_MS = [500, 2000];
/**
 * How long a later slice may go without a byte. pdf.js puts no deadline on a
 * range, so a connection that stalls without closing would hold the page, and
 * its request slot, for good. A bound on silence rather than on the whole
 * answer leaves a slow link, sharing itself among six slices, to finish; this
 * long covers the server's own 10 s read plus a cold worker taking the request.
 */
const SLICE_IDLE_MS = 30_000;

/**
 * The route's 503 for a file that changed while it was read, as one an agent
 * is still writing does. The client hands an error body back in the type the
 * request asked for, so a range's arrives as bytes.
 */
function isFileChangedRefusal(err: unknown): boolean {
  const body = err instanceof ApiError && err.status === 503 ? err.response?.data : undefined;
  if (!(body instanceof ArrayBuffer)) return false;
  try {
    const { detail } = JSON.parse(new TextDecoder().decode(body)) as { detail?: { code?: unknown } };
    return detail?.code === 'file_changed';
  } catch {
    return false;
  }
}

async function withRetry<T>(request: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await request();
    } catch (err) {
      // A 4xx, an abort and a file that changed under the read are answers;
      // only a dropped request or another 5xx is worth asking again.
      const transient = err instanceof ApiError && err.name !== 'CanceledError'
        && (err.status === undefined || err.status >= 500) && !isFileChangedRefusal(err);
      if (!transient || attempt >= SLICE_RETRY_MS.length || signal?.aborted) throw err;
      await new Promise((resolve) => setTimeout(resolve, SLICE_RETRY_MS[attempt]));
    }
  }
}

/**
 * What opening a file by range found: the whole file in the first answer,
 * a file to read on by range, or `whole` where a range cannot serve it and
 * the caller reads it in one request. `headers` are the first answer's, for
 * whatever the caller reads off them.
 */
export type RangedOpen =
  | { kind: 'bytes'; buffer: ArrayBuffer; headers: Record<string, string> }
  | { kind: 'ranged'; file: RangedFile; headers: Record<string, string> }
  | { kind: 'whole' };

const WHOLE: RangedOpen = { kind: 'whole' };

/**
 * Open the bytes a GET of `url` answers from their first slice, by range,
 * which any route honouring `Range` with an `ETag` serves (a workspace file,
 * a Drive file).
 */
export async function openRangedRead(
  url: string,
  params: Record<string, string | number | boolean>,
): Promise<RangedOpen> {
  const slice = (first: number, last: number, config: RequestConfig = {}) =>
    api.get(url, {
      ...config,
      params,
      headers: { Range: `bytes=${first}-${last}` },
      responseType: 'arraybuffer',
      // A browser holding the whole file from a plain download would make
      // this request conditional, and the server answers a conditional range
      // with the whole file.
      cache: 'no-store',
    });

  let head: Awaited<ReturnType<typeof slice>>;
  try {
    // No deadline, as the whole download this replaces had none, and a 200
    // with the whole file is an answer it takes.
    head = await withRetry(() => slice(0, FIRST_SLICE_BYTES - 1));
  } catch (err) {
    // An empty file has no first byte for a range to start at, and one that
    // changes as it is read has no version for slices to keep to: a plain
    // download takes either as it stands.
    if ((err instanceof ApiError && err.status === 416) || isFileChangedRefusal(err)) return WHOLE;
    throw err;
  }
  const { headers } = head;
  if (head.status !== 206) return { kind: 'bytes', buffer: head.data as ArrayBuffer, headers };
  const bytes = head.data as ArrayBuffer;
  const opened = parseContentRange(head.headers['content-range']);
  // Answered by range but not described (a header the deploy does not
  // expose), or described as other bytes than arrived, so there is no length
  // to read on from: read it again whole.
  if (!opened || opened.first !== 0 || bytes.byteLength !== opened.last + 1) return WHOLE;
  if (opened.last + 1 === opened.total) return { kind: 'bytes', buffer: bytes, headers };
  // With no version to hold the slices to, a change between two would go unseen.
  const etag = headers.etag;
  if (!etag) return WHOLE;

  const length = opened.total;
  const readSpan = async (begin: number, end: number, signal?: AbortSignal) => {
    const out = new Uint8Array(end - begin);
    // A slice answered whole cannot be held to the version that opened, so its
    // body, the entire file, is never read: it comes back empty and the check
    // below has the file opened again.
    const config: RequestConfig = { signal, idleTimeout: SLICE_IDLE_MS, expectStatus: 206 };
    // The server caps one answer, so a short one is asked again from where it stopped.
    for (let at = begin; at < end;) {
      const part = await withRetry(() => slice(at, end - 1, config), signal).catch((err: unknown) => {
        // The offset lay inside the version that opened, so the file has since
        // shrunk or gone, and reading it again says which. The route's own
        // refusal says the version moved while it read.
        const changed = (err instanceof ApiError && (err.status === 416 || err.status === 404)) || isFileChangedRefusal(err);
        throw changed ? new FileChangedError() : err;
      });
      const got = part.status === 206 ? parseContentRange(part.headers['content-range']) : null;
      if (!got || got.first !== at || got.total !== length || part.headers.etag !== etag) {
        throw new FileChangedError();
      }
      const body = part.data as ArrayBuffer;
      if (body.byteLength !== got.last - got.first + 1) throw new Error('A file slice arrived incomplete');
      const take = Math.min(body.byteLength, end - at);
      out.set(new Uint8Array(body, 0, take), at - begin);
      at += take;
    }
    return out;
  };
  const file: RangedFile = {
    length,
    etag,
    initial: new Uint8Array(bytes),
    read: (begin, end, signal) => readSpan(begin, end, signal).catch((err: unknown) => {
      if (err instanceof FileChangedError) markStale(file);
      throw err;
    }),
  };
  return { kind: 'ranged', file, headers };
}
