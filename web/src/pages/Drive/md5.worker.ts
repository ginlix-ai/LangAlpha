/**
 * Hashes one part off the main thread: given its bytes as a Blob, answers its
 * MD5. The bytes are read as a stream and their arrival reported (`read`), so
 * the page can tell a slow disk from a worker that has gone.
 */
import { md5 } from './md5';

interface Ask {
  id: number;
  blob: Blob;
}

// The worker's own scope; the app's types know only a window's.
const scope = self as unknown as {
  onmessage: ((e: MessageEvent<Ask>) => void) | null;
  postMessage(message: unknown, transfer: Transferable[]): void;
};

/** Bytes between two reports of the read, so a part says how it goes a few times a second on a slow disk. */
const REPORT_EVERY = 1024 * 1024;

async function read(id: number, blob: Blob): Promise<Uint8Array> {
  const bytes = new Uint8Array(blob.size);
  const reader = blob.stream().getReader();
  let at = 0;
  let reported = 0;
  for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) {
    bytes.set(chunk.value, at);
    at += chunk.value.length;
    if (at - reported >= REPORT_EVERY) {
      reported = at;
      scope.postMessage({ id, read: at }, []);
    }
  }
  return bytes;
}

scope.onmessage = (e) => {
  const { id, blob } = e.data;
  read(id, blob).then(
    (bytes) => {
      scope.postMessage({ id, read: bytes.length }, []);
      const digest = md5(bytes);
      scope.postMessage({ id, digest }, [digest.buffer]);
    },
    (err: unknown) => scope.postMessage({ id, error: String(err) }, []),
  );
};
