import { spaceRef, useDriveEntry, useDriveSpaces, useDriveVersions } from '@/hooks/useDrive';
import type { DriveEntry } from '@/types/drive';
import { useDriveBody } from './driveBody';
import { memoState, resolveFileView, type FileAddress } from './memoMarks';
import { useDriveEditor } from './useDriveEditor';

/**
 * The space an address names (`null` is the person's own) as the server
 * knows it. `missing` is a space the person no longer has, once the list is
 * read, so a caller can fall back to their own Drive.
 */
export function useDriveSpace(named: string | null) {
  const spaces = useDriveSpaces();
  const current = spaces.data?.find((s) => (named ? s.id === named || s.kind === named : s.kind === 'personal'))
    ?? (named ? undefined : spaces.data?.[0]);
  return {
    spaces,
    current,
    space: current ? spaceRef(current) : null,
    usage: current?.usage ?? null,
    missing: !!named && !!spaces.data && !current,
  };
}

/**
 * One open Drive file as the page and the panel's Drive tab both show it:
 * its entry, its versions, the view the address names as the file reads it,
 * its head's body and its editor. `listed` seeds the entry from a fresh
 * folder listing.
 */
export function useOpenDriveFile(space: string | null, file: string | null, at: FileAddress, listed?: DriveEntry) {
  const entry = useDriveEntry(space, file, listed);
  const fileEntry = entry.data?.kind === 'file' ? entry.data : undefined;
  // The address as the open file reads it; what it names and the file does
  // not have (a version on a plain file, the head as a version, a tab it has
  // no view for) is dropped from the address by the caller.
  // A memo's versions, which the address may name: one gone is not compared with.
  const versions = useDriveVersions(space, fileEntry?.id ?? null, memoState(fileEntry).kind !== 'plain');
  const kept = versions.data?.versions.map((v) => v.seq);
  const shown = resolveFileView(fileEntry, at, false, kept);
  const head = useDriveBody(space, fileEntry ?? null, null);
  const editor = useDriveEditor(space, file, fileEntry, head, shown.view === 'document' && shown.version === null);
  // An open editor is the document, whatever the address names.
  const fileView = editor.session ? resolveFileView(fileEntry, at, true, kept) : shown;
  // Read, and not a read that failed: a version saved since the last look is
  // not one the address is wrong to name.
  const settled = !entry.isFetching && !versions.isFetching && !entry.isError && !versions.isError;
  return { entry, fileEntry, versions, shown, head, editor, fileView, settled };
}
