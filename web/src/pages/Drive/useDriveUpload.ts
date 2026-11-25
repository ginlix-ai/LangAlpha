import { useEffect, useSyncExternalStore } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { getSpaces, listVersions, makeFolder } from '@/api/drive';
import { toast } from '@/components/ui/use-toast';
import { useLocale } from '@/hooks/useLocale';
import { queryKeys } from '@/lib/queryKeys';
import { invalidateDriveSpace, spaceRef } from '@/hooks/useDrive';
import { dropHandle } from '@/lib/fileHandles';
import type { DriveEntry, DriveUsage } from '@/types/drive';
import { DriveUploadFailed } from '@/api/driveUploads';
import { driveRefusal, uploadLimit } from './driveRefusal';
import { drivePathKey, isWithin, joinDrivePath, nameOf, parentOf } from './drivePaths';
import { driveUploads, type UploadFailure } from './driveUploadStore';

/** A file to send, with its path under the folder it is dropped into (a folder upload keeps its tree). */
export interface PickedFile {
  file: File;
  relPath: string;
  /** Where the browser gave one, a handle to read the file again after the page is gone. */
  handle?: FileSystemFileHandle;
}

/**
 * What was dropped, folders walked into their files. Every item's entry, and
 * its handle where the browser offers one, is taken before the first await:
 * the browser empties the drop's list once the handler returns. Only a file
 * dropped by itself carries a handle; one inside a dropped folder does not.
 */
export async function collectDropped(data: DataTransfer): Promise<{ files: PickedFile[]; folders: string[] }> {
  const items = Array.from(data.items ?? []).filter((item) => item.kind === 'file');
  const handles = items.map(dropHandle);
  const entries = items.map((item) => item.webkitGetAsEntry?.() ?? null);
  if (entries.length === 0 || entries.some((e) => e === null)) {
    return { files: Array.from(data.files).map((file) => ({ file, relPath: file.name })), folders: [] };
  }
  const files: PickedFile[] = [];
  const folders: string[] = [];
  const walk = async (entry: FileSystemEntry, prefix: string, handle?: Promise<FileSystemFileHandle | null>): Promise<void> => {
    const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isFile) {
      const file = await new Promise<File>((resolve, reject) => (entry as FileSystemFileEntry).file(resolve, reject));
      files.push({ file, relPath: rel, handle: (await handle) ?? undefined });
      return;
    }
    folders.push(rel);
    const reader = (entry as FileSystemDirectoryEntry).createReader();
    // A reader hands its entries over in batches until one comes back empty.
    for (;;) {
      const batch = await new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
      if (batch.length === 0) break;
      for (const child of batch) await walk(child, rel);
    }
  };
  for (const [i, entry] of entries.entries()) await walk(entry!, '', handles[i]);
  return { files, folders };
}

/** Files from a picker; a folder picker names each by its path inside the folder. */
export function pickedFiles(list: FileList | null): PickedFile[] {
  return Array.from(list ?? []).map((file) => ({ file, relPath: file.webkitRelativePath || file.name }));
}

/**
 * Each file's refusal said before a byte goes, as the server would refuse it:
 * larger than any upload may be, or more than is left of the allowance once
 * the files before it are counted, which is the room its refusal names.
 * `frees` is what each file's save takes out of the counts at once: the file
 * it replaces, where that is too large to be kept as a version.
 */
export function refusedUpFront(sizes: number[], usage: DriveUsage | null, frees: number[] = []): Array<UploadFailure | undefined> {
  if (!usage) return sizes.map(() => undefined);
  const limit = uploadLimit(usage);
  let bytes = usage.bytes;
  return sizes.map((size, i) => {
    if (limit && size > limit) return { error: new DriveUploadFailed('too_large', ''), usage };
    const grows = size - (frees[i] ?? 0);
    if (usage.max_bytes !== null && bytes + grows > usage.max_bytes) {
      // Room enough for it alone: refused for the files before it, which may yet not land.
      const behind = usage.bytes + grows <= usage.max_bytes;
      return { error: new DriveUploadFailed('over_quota', ''), usage: { ...usage, bytes }, ...(behind && { recheck: true as const }) };
    }
    bytes += grows;
    return undefined;
  });
}

/**
 * The size of the file sending to `path` replaces where the server will not
 * keep it as an earlier copy (0 where it will, or there is none): the person
 * is told on the upload's row, and its bytes leave the counts. Read from
 * the listing the page already holds and, for a file there that is not text
 * (text always keeps its history), the server's own flag on its versions.
 */
async function replacesUnkept(queryClient: QueryClient, space: string, path: string): Promise<number> {
  const listed = queryClient.getQueryData<DriveEntry[]>(queryKeys.drive.list(space, parentOf(path)));
  // Matched as the server matches names, so `data.parquet` replaces `Data.parquet`.
  const key = drivePathKey(path);
  const existing = listed?.find((e) => e.kind === 'file' && drivePathKey(e.path) === key);
  if (!existing || existing.is_text) return 0;
  try {
    const versions = await queryClient.fetchQuery({
      queryKey: queryKeys.drive.versions(space, existing.id),
      queryFn: () => listVersions(space, existing.id),
      staleTime: 15_000,
    });
    return versions.too_large_for_history ? existing.size ?? 0 : 0;
  } catch {
    return 0;
  }
}

/** The page's uploads, and sending more into one space. */
export function useDriveUpload({ space, usage }: { space: string | null; usage: DriveUsage | null }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const queryClient = useQueryClient();
  const items = useSyncExternalStore(driveUploads.subscribe, driveUploads.snapshot);

  // A file landing or failing changes the listing and the usage, wherever the person is now.
  useEffect(() => driveUploads.onSettled((settled) => invalidateDriveSpace(queryClient, settled)), [queryClient]);
  // Uploads an earlier page left open come back as paused rows, once per page and person.
  useEffect(() => driveUploads.restore(), []);

  /** Sends `files` into the folder `dir`, keeping any folder tree they came in. */
  const upload = async (picked: PickedFile[], dir: string, folders: string[] = []) => {
    if (!space || (picked.length === 0 && folders.length === 0)) return;
    // Empty folders in a dropped tree are made too; the others come with their files.
    let told = false;
    for (const folder of folders) {
      try {
        await makeFolder(space, joinDrivePath(dir, folder));
      } catch (err) {
        // A folder that cannot be made fails its files with the reason; an
        // empty one has no file to carry it, so the first of those says so.
        if (told || picked.some(({ relPath }) => isWithin(relPath, folder))) continue;
        told = true;
        const why = driveRefusal(err, t, { subject: nameOf(folder), failed: t('drive:errors.folderFailed'), locale, usage });
        toast({ variant: 'destructive', ...why.toast });
      }
    }
    const paths = picked.map(({ relPath }) => joinDrivePath(dir, relPath));
    const unkept = await Promise.all(paths.map((path) => replacesUnkept(queryClient, space, path)));
    const refused = refusedUpFront(picked.map((p) => p.file.size), usage, unkept);
    driveUploads.add(picked.map(({ file, handle }, i) => ({
      space,
      name: file.name,
      path: paths[i],
      file,
      handle: handle ?? null,
      failure: refused[i],
      replacesUnkept: unkept[i] > 0,
    })));
    if (folders.length) invalidateDriveSpace(queryClient, space);
  };

  /**
   * Sends a failed file again. One refused for the files before it is judged
   * again first, against the space read afresh: it goes, or its refusal is
   * now its own and final.
   */
  const retry = async (id: number) => {
    const item = driveUploads.snapshot().find((i) => i.id === id);
    if (item?.status !== 'failed' || !item.failure.recheck || !item.file) {
      driveUploads.retry(id);
      return;
    }
    const spaces = await queryClient.fetchQuery({ queryKey: queryKeys.drive.spaces(), queryFn: getSpaces, staleTime: 0 }).catch(() => null);
    const now = spaces?.find((s) => spaceRef(s) === item.space)?.usage;
    // Unread, it stays as it is, for another Retry.
    if (!now) return;
    const frees = await replacesUnkept(queryClient, item.space, item.path);
    driveUploads.recheck(id, refusedUpFront([item.file.size], now, [frees])[0]);
  };

  return {
    items,
    upload,
    cancel: driveUploads.cancel,
    retry,
    resume: driveUploads.resume,
    discard: driveUploads.discard,
    dismiss: driveUploads.dismiss,
  };
}
