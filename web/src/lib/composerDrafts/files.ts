import { registerAuthReset } from '../authResets';
import { storageUser } from '../userStorage';
import { deleteDraftFiles, draftFileEntries, getDraftFile, putDraftFile } from './idb';
import type { DraftFileMeta } from './types';

/**
 * The bytes of the files in drafts: in memory while the page lives, and in
 * IndexedDB (`idb`) to come back after a reload. A file larger than
 * `MAX_STORED_FILE_BYTES` stays in memory only. Which bytes are still wanted
 * is the drafts' business (`store`), which sweeps the rest through here. Only
 * a composer and a sweep need this, so it stays off the first load.
 */

export const MAX_STORED_FILE_BYTES = 50 * 1024 * 1024;

/** How long a sweep leaves bytes just written: another tab may have written
 *  them for a draft it has yet to save, which no draft this one reads names. */
const SWEEP_GRACE_MS = 60 * 1000;

/** The Files of drafts this page has seen, by tile id. */
const heldFiles = new Map<string, File>();
/** The IndexedDB keys this page wrote or read, so a file is written once. */
const storedKeys = new Set<string>();
const fileKey = (user: string, tileId: string) => `${user}:${tileId}`;

/** The files a mounted composer holds now, kept so its draft can bring them back. */
export function keepDraftFiles(files: ReadonlyArray<{ id: string; file: File }>): void {
  const user = storageUser();
  if (!user) return;
  for (const { id, file } of files) {
    heldFiles.set(id, file);
    const key = fileKey(user, id);
    if (file.size > MAX_STORED_FILE_BYTES || storedKeys.has(key)) continue;
    storedKeys.add(key);
    void putDraftFile(key, file);
  }
}

/** A saved draft's file, or null once its bytes are gone (too large to keep
 *  past a reload, or cleared by the browser). */
export async function loadDraftFile(meta: DraftFileMeta): Promise<File | null> {
  const held = heldFiles.get(meta.id);
  if (held) return held;
  const user = storageUser();
  if (!user) return null;
  const key = fileKey(user, meta.id);
  const blob = await getDraftFile(key);
  if (!blob) return null;
  const file = blob instanceof File
    ? blob
    : new File([blob], meta.name, { type: meta.mime, lastModified: meta.lastModified });
  heldFiles.set(meta.id, file);
  storedKeys.add(key);
  return file;
}

/** Drops the bytes of every tile not in `wanted`, read once the stored keys
 *  are in, so a tile added meanwhile is kept. */
export async function sweepDraftFiles(wanted: () => Set<string>): Promise<void> {
  const user = storageUser();
  if (!user) return;
  const entries = await draftFileEntries();
  const keep = wanted();
  // Another account came in while the keys were read, and `keep` is its own.
  if (storageUser() !== user) return;
  for (const id of [...heldFiles.keys()]) if (!keep.has(id)) heldFiles.delete(id);
  const prefix = `${user}:`;
  const now = Date.now();
  const doomed = entries
    .filter(({ key, at }) => key.startsWith(prefix) && now - at > SWEEP_GRACE_MS && !keep.has(key.slice(prefix.length)))
    .map(({ key }) => key);
  doomed.forEach((k) => storedKeys.delete(k));
  await deleteDraftFiles(doomed);
}

/** Every file of `user`'s drafts, at sign-out. */
export function clearDraftFiles(user: string): void {
  heldFiles.clear();
  storedKeys.clear();
  const prefix = `${user}:`;
  void draftFileEntries().then((entries) => deleteDraftFiles(entries.map(({ key }) => key).filter((k) => k.startsWith(prefix))));
}

// Per-page memory goes with the account; what is stored stays scoped to it.
registerAuthReset(() => {
  heldFiles.clear();
  storedKeys.clear();
});
