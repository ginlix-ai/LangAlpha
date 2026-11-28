import type { UseQueryResult } from '@tanstack/react-query';
import type { BodyMode, FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import type { DriveEntry, DriveVersion, DriveVersions } from '@/types/drive';
import { atVersion, driveBodyMode, useDriveBody } from './driveBody';
import type { FileView } from './memoMarks';

/** What an open file's document view shows, and how it was read. */
export interface ShownDocument {
  /** The version the address names for the document; null is the head. */
  docVersion: number | null;
  /** That version, once the versions are read and keep it. */
  viewed: DriveVersion | null;
  /** The file as it is on screen: a past version by its own size and kind of bytes. */
  shownEntry: DriveEntry | null;
  /** Its bytes: the head's, or the version's. */
  body: UseQueryResult<FileBody>;
  mode: BodyMode;
  /** Which bytes are on screen, so what is told of them (a viewer that failed on them) goes with them. */
  bodyKey: string | null;
  /**
   * The hash of the bytes on screen, which a comment made on them is stored
   * with: the version's, or the head's, which is the last head's while a new
   * one loads.
   */
  sha: string | null;
}

/**
 * The head, or the version the address names, read by that version's own
 * size and kind of bytes. A version the versions do not hold yet (or any
 * more) is `viewed: null` beside its `docVersion`, never the head instead.
 */
export function useShownDocument(space: string | null, entry: DriveEntry | undefined, fileView: FileView, versions: DriveVersions | undefined, head: UseQueryResult<FileBody>): ShownDocument {
  const docVersion = fileView.view === 'document' ? fileView.version : null;
  const viewed = docVersion !== null ? versions?.versions.find((v) => v.seq === docVersion) ?? null : null;
  const shownEntry = entry ? atVersion(entry, viewed) : null;
  const old = useDriveBody(space, entry && viewed ? shownEntry : null, viewed ? { seq: viewed.seq, sha: viewed.sha256 } : null);
  const body = viewed ? old : head;
  return {
    docVersion,
    viewed,
    shownEntry,
    body,
    mode: shownEntry ? driveBodyMode(shownEntry) : 'none',
    bodyKey: shownEntry ? `${shownEntry.id}:${shownEntry.sha256 ?? ''}:${viewed?.seq ?? 'head'}` : null,
    sha: viewed ? viewed.sha256 : body.isPlaceholderData ? body.data?.version ?? null : entry?.sha256 ?? null,
  };
}
