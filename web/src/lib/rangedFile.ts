/**
 * A file opened from its first slice, read on by byte range.
 *
 * `etag` names the version the slices belong to: a slice served under another
 * one, or under another length, came from a file that changed in between, and
 * the reader throws {@link FileChangedError} rather than hand back bytes that
 * do not fit the ones already read.
 */
export interface RangedFile {
  length: number;
  etag: string;
  /** The bytes the opening request returned, from offset 0. */
  initial: Uint8Array;
  /** Bytes `[begin, end)`, whole, however many requests that takes. */
  read: (begin: number, end: number, signal?: AbortSignal) => Promise<Uint8Array>;
}

/** The file changed between two slices, so the slices no longer belong together. */
export class FileChangedError extends Error {
  constructor() {
    super('The file changed while it was being read');
    this.name = 'FileChangedError';
  }
}

export function isRangedFile(value: unknown): value is RangedFile {
  return typeof value === 'object' && value !== null && 'read' in value && 'initial' in value;
}

// Files no document can read on from: one a read found changed, marked by the
// read that threw so that every holder can tell nothing more read from it
// fits, and one whose viewer lost a slice that pdf.js never asks for again.
const stale = new WeakSet<RangedFile>();

export function markStale(file: RangedFile): void {
  stale.add(file);
}

export function isStale(file: RangedFile): boolean {
  return stale.has(file);
}

/** `bytes <first>-<last>/<total>`, or null for anything else, including an unknown total. */
export function parseContentRange(header: string | undefined): { first: number; last: number; total: number } | null {
  const match = header ? /^\s*bytes\s+(\d+)-(\d+)\/(\d+)\s*$/i.exec(header) : null;
  if (!match) return null;
  const [first, last, total] = [Number(match[1]), Number(match[2]), Number(match[3])];
  return last >= first && total > last ? { first, last, total } : null;
}
