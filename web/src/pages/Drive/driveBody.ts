import { useQuery } from '@tanstack/react-query';
import { contentParams, contentUrl, readBytes, readText } from '@/api/drive';
import { queryKeys } from '@/lib/queryKeys';
import { openRangedRead } from '@/lib/rangedRead';
import { getFileExtension } from '@/pages/ChatAgent/components/filePanel/fileMeta';
import { bodyMode, readFileBody, type BodyMode, type FileBody, type FileReaders } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { LIVE } from '@/pages/ChatAgent/utils/api/readFrom';
import type { DriveEntry, DriveVersion } from '@/types/drive';

/**
 * Past this a viewer would hold the whole file in the tab, so the file opens
 * as a download card, decided from its size alone. A PDF reads by range and
 * has no such bound.
 */
export const PREVIEW_MAX_BYTES = 32 * 1024 * 1024;

export function tooLargeToPreview(entry: Pick<DriveEntry, 'name' | 'size'>): boolean {
  return (entry.size ?? 0) > PREVIEW_MAX_BYTES && getFileExtension(entry.name) !== 'pdf';
}

/**
 * How a Drive file is read for its viewer. The name picks the viewer as it
 * does in the file panel, but the server knows which files are text: a binary
 * file under a text name opens as a download card rather than as garbled
 * characters (a table the store keeps as data is tried as text first), and
 * text under an unknown name still opens as text.
 */
export function driveBodyMode(entry: Pick<DriveEntry, 'name' | 'is_text' | 'size'>): BodyMode {
  const mode = bodyMode(entry.name);
  if (tooLargeToPreview(entry)) return 'none';
  if (entry.is_text === false && (mode === 'text' || mode === 'html')) return readsAsData(entry) ? 'text' : 'none';
  return mode;
}

/**
 * Tables the store keeps as data rather than text, which still read as text
 * once their bytes are UTF-8. A research CSV is one of the files a person
 * most wants to glance at, so it opens in the table viewer, within a bound
 * the browser parses at once; past the bound, or not UTF-8, it is a download.
 */
const DATA_TEXT_EXTENSIONS = new Set(['csv', 'tsv', 'jsonl', 'ndjson']);
const DATA_PREVIEW_MAX_BYTES = 4 * 1024 * 1024;

function isDataTable(entry: Pick<DriveEntry, 'name' | 'is_text'>): boolean {
  return entry.is_text === false && DATA_TEXT_EXTENSIONS.has(getFileExtension(entry.name));
}

function readsAsData(entry: Pick<DriveEntry, 'name' | 'is_text' | 'size'>): boolean {
  return isDataTable(entry) && (entry.size ?? Infinity) <= DATA_PREVIEW_MAX_BYTES;
}

/** A table past the bound: its card says it is too large, not that it is not text. */
function dataPastBound(entry: Pick<DriveEntry, 'name' | 'is_text' | 'size'>): boolean {
  return isDataTable(entry) && !readsAsData(entry);
}

const DOCX_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
const GENERIC_TYPES = new Set(['', 'application/octet-stream', 'application/zip', 'binary/octet-stream']);

/**
 * Word files: no viewer in the browser, but the server reads their text out
 * (a memo's changes compare it). Known as the server knows them: by the type
 * they were saved with, and by the name only for binary bytes saved with none
 * that says, so a renamed report still reads.
 */
function readsAsExtract(entry: Pick<DriveEntry, 'name' | 'content_type' | 'is_text'>): boolean {
  const media = (entry.content_type ?? '').split(';')[0].trim().toLowerCase();
  if (media === DOCX_TYPE) return true;
  return !entry.is_text && GENERIC_TYPES.has(media) && getFileExtension(entry.name) === 'docx';
}

/**
 * Whether a file shows as its extracted text: the head by its type, a past
 * version by the server's own word on it, since a file overwritten with
 * another kind keeps only the head's type. A PDF among them keeps its viewer.
 */
export function showsExtract(entry: DriveEntry, version: DriveVersion | null): boolean {
  if (!version) return readsAsExtract(entry);
  return version.text === 'extracted' && getFileExtension(entry.name) !== 'pdf';
}

/**
 * Why a file shows the download card in place of a viewer: past the preview
 * bound (or a PDF past it that could not be read by range), or bytes that are
 * not text, a table's that did not decode included.
 */
export function noneCategory(entry: Pick<DriveEntry, 'name' | 'is_text' | 'size'>): 'too_large' | 'binary_file' {
  if (tooLargeToPreview(entry) || dataPastBound(entry)) return 'too_large';
  return driveBodyMode(entry) === 'none' || readsAsData(entry) ? 'binary_file' : 'too_large';
}

/** A data file's bytes that are not UTF-8: it opens as the download card. */
class NotText extends Error {}

function strictUtf8(buffer: ArrayBuffer): string {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buffer);
  } catch {
    throw new NotText();
  }
}

/** A PDF past the preview bound that could not be read by range: it opens as the download card. */
class TooLargeWhole extends Error {}

/**
 * One Drive file's readers, bound to its id and version (its head when `seq`
 * is null). A PDF opens from its first slice and reads on by range, as it
 * does in a workspace; past the preview bound it is never read whole.
 */
function driveReaders(space: string, entry: DriveEntry, seq: number | null): FileReaders {
  const entryId = entry.id;
  // A text body's `version` is the sha256 of its bytes, read off the answer:
  // the `If-Match` of an edit made over it. A table kept as data is read as
  // bytes and decoded here, strictly, since the server keeps no promise they
  // are text.
  const text = readsAsData(entry)
    ? async () => {
      const { buffer, sha } = await readBytes(space, entryId, seq);
      return { content: strictUtf8(buffer), from: LIVE, version: sha };
    }
    : async () => {
      const { content, sha } = await readText(space, entryId, seq);
      return { content, from: LIVE, version: sha };
    };
  const bytes = async () => {
    const { buffer } = await readBytes(space, entryId, seq);
    return { buffer, from: LIVE };
  };
  return {
    readText: text,
    readFull: text,
    readBytes: bytes,
    openRanged: async () => {
      const opened = await openRangedRead(contentUrl(space), contentParams(entryId, seq));
      if (opened.kind === 'whole') {
        if ((entry.size ?? 0) > PREVIEW_MAX_BYTES) throw new TooLargeWhole();
        return bytes();
      }
      return opened.kind === 'bytes' ? { buffer: opened.buffer, from: LIVE } : { ranged: opened.file, from: LIVE };
    },
  };
}

const NONE: FileBody = { mode: 'none', mime: null, content: null, buffer: null, truncated: false, from: LIVE };

export async function readDriveBody(space: string, entry: DriveEntry, seq: number | null): Promise<FileBody> {
  if (driveBodyMode(entry) === 'none') return NONE;
  try {
    return await readFileBody(driveReaders(space, entry, seq), entry.name);
  } catch (err) {
    if (err instanceof TooLargeWhole || err instanceof NotText) return NONE;
    throw err;
  }
}

/**
 * Where a body is cached: by its bytes' hash and by how its name reads them,
 * so a rename to another kind of file with the same bytes reads them again.
 * The extension is part of it, since two kinds read alike (a PDF and a
 * workbook are both a buffer) still open in different viewers.
 */
export function driveBodyKey(space: string, entry: Pick<DriveEntry, 'id' | 'name' | 'is_text' | 'size'>, sha: string) {
  return queryKeys.drive.body(space, entry.id, sha, `${driveBodyMode(entry)}:${getFileExtension(entry.name)}`);
}

/**
 * The file as it was at `version`: its size and, where the server says, its
 * kind of bytes decide how that version opens, not the current one's.
 */
export function atVersion(entry: DriveEntry, version: DriveVersion | null): DriveEntry {
  if (!version) return entry;
  return { ...entry, size: version.size, sha256: version.sha256, is_text: version.is_text };
}

/**
 * The bytes of a file at one version, cached by their hash: the head's moving
 * on is a new key, so the bytes on screen stay until the new ones arrive and
 * the old ones are never read twice. `version` is a seq and its hash, or null
 * for the head.
 */
export function useDriveBody(space: string | null, entry: DriveEntry | null, version: { seq: number; sha: string } | null) {
  const sha = version?.sha ?? entry?.sha256 ?? '';
  const mode = entry ? driveBodyMode(entry) : 'none';
  return useQuery({
    queryKey: driveBodyKey(space ?? '', entry ?? { id: '', name: '', is_text: null, size: null }, sha),
    // The head is read at its seq too, so bytes saved after the entry was
    // read never land under the entry's hash.
    queryFn: () => readDriveBody(space!, entry!, version?.seq ?? entry!.version),
    enabled: !!space && !!entry && entry.kind === 'file' && mode !== 'none',
    staleTime: Infinity,
    gcTime: 5 * 60_000,
    // An ArrayBuffer or a ranged file is kept whole, never merged.
    structuralSharing: false,
    retry: false,
  });
}
