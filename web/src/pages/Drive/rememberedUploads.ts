/**
 * Drive uploads the server holds open, remembered in the browser so a reload
 * or a closed tab does not lose them: a page opened later finds each one and
 * offers to continue it from the parts the store has. Kept per person
 * (`userLocalStorage`), and a store that is blocked or full keeps nothing:
 * the upload then simply ends on the server when nothing touches it for a day.
 */
import * as z from 'zod/mini';
import { forgetHandle, keepHandle } from '@/lib/fileHandles';
import { userLocalStorage } from '@/lib/userStorage';

const KEY = 'drive.openUploads';

const rememberedSchema = z.object({
  space: z.string(),
  uploadId: z.string(),
  path: z.string(),
  name: z.string(),
  size: z.number(),
  lastModified: z.number(),
  /** `fingerprint()` of the file; empty where the page could not compute one. */
  fingerprint: z.string(),
});

export type RememberedUpload = z.infer<typeof rememberedSchema>;

export function rememberedUploads(): RememberedUpload[] {
  let raw: unknown;
  try {
    raw = JSON.parse(userLocalStorage.getItem(KEY) ?? '[]');
  } catch {
    return [];
  }
  if (!Array.isArray(raw)) return [];
  // One damaged record is dropped, not the rest.
  return raw.flatMap((r) => {
    const parsed = z.safeParse(rememberedSchema, r);
    return parsed.success ? [parsed.data] : [];
  });
}

function write(list: RememberedUpload[]): void {
  if (list.length) userLocalStorage.setItem(KEY, JSON.stringify(list));
  else userLocalStorage.removeItem(KEY);
}

export function rememberUpload(upload: RememberedUpload, handle?: FileSystemFileHandle): void {
  write([...rememberedUploads().filter((r) => r.uploadId !== upload.uploadId), upload]);
  if (handle) void keepHandle(upload.uploadId, handle);
}

export function forgetUpload(uploadId: string): void {
  const list = rememberedUploads();
  if (list.some((r) => r.uploadId === uploadId)) write(list.filter((r) => r.uploadId !== uploadId));
  void forgetHandle(uploadId);
}

const EDGE = 1024 * 1024;

/**
 * What tells one file from another of the same name, size and time without
 * reading it whole: the SHA-256 of its first and last MiB and its size. Empty
 * where the page has no WebCrypto (a page served over plain HTTP from another
 * machine), which then compares by name, size and time alone.
 */
export async function fingerprint(file: File): Promise<string> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) return '';
  const head = await file.slice(0, EDGE).arrayBuffer();
  const tail = await file.slice(Math.max(EDGE, file.size - EDGE)).arrayBuffer();
  const size = new TextEncoder().encode(String(file.size));
  const joined = new Uint8Array(head.byteLength + tail.byteLength + size.byteLength);
  joined.set(new Uint8Array(head), 0);
  joined.set(new Uint8Array(tail), head.byteLength);
  joined.set(size, head.byteLength + tail.byteLength);
  const digest = await subtle.digest('SHA-256', joined);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
}

/** Whether `file` looks like the one `upload` was sent from, by what is cheap to read first. */
export function looksSame(file: File, upload: RememberedUpload): boolean {
  return file.name === upload.name && file.size === upload.size && file.lastModified === upload.lastModified;
}

/** Whether `file` is the one `upload` was sent from: its name, size and time, then its fingerprint. */
export async function isSameFile(file: File, upload: RememberedUpload): Promise<boolean> {
  if (!looksSame(file, upload)) return false;
  if (!upload.fingerprint) return true;
  return (await fingerprint(file)) === upload.fingerprint;
}
