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
 * file under a text name (a data file the store keeps, a `.csv` included)
 * opens as a download card rather than as garbled characters, and text under
 * an unknown name still opens as text.
 */
export function driveBodyMode(entry: Pick<DriveEntry, 'name' | 'is_text' | 'size'>): BodyMode {
  const mode = bodyMode(entry.name);
  if (tooLargeToPreview(entry)) return 'none';
  if (entry.is_text === false && (mode === 'text' || mode === 'html')) return 'none';
  return mode;
}

/** A PDF past the preview bound that could not be read by range: it opens as the download card. */
class TooLargeWhole extends Error {}

/**
 * One Drive file's readers, bound to its id and version (its head when `seq`
 * is null). A PDF opens from its first slice and reads on by range, as it
 * does in a workspace; past the preview bound it is never read whole.
 */
function driveReaders(space: string, entryId: string, seq: number | null, size: number): FileReaders {
  // A text body's `version` is the sha256 of its bytes, read off the answer:
  // the `If-Match` of an edit made over it.
  const text = async () => {
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
        if (size > PREVIEW_MAX_BYTES) throw new TooLargeWhole();
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
    return await readFileBody(driveReaders(space, entry.id, seq, entry.size ?? 0), entry.name);
  } catch (err) {
    if (err instanceof TooLargeWhole) return NONE;
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
