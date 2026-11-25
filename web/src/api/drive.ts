import { api, ApiError, delay, type TransferProgress } from '@/api/client';
import { apiErrorDetailMessage, apiErrorObject, apiErrorStatus } from '@/api/errors';
import { DriveUploadFailed, getContentLink } from '@/api/driveUploads';
import { authSessionCheck } from '@/lib/authResets';
import {
  DRIVE_ERROR_TYPES,
  type DriveChanges,
  type DriveEntry,
  type DriveErrorType,
  type DriveSpace,
  type DriveTrashPage,
  type DriveVersions,
} from '@/types/drive';

const BASE = '/api/v1/drive';

/** Sent with every save from one open editor, so its saves are one version. */
export const SITTING_HEADER = 'X-Drive-Sitting';

const at = (space: string, route: string) => `${BASE}/${encodeURIComponent(space)}/${route}`;

/** The Drive error type a failed call answered, or null for any other failure. */
export function driveErrorType(err: unknown): DriveErrorType | null {
  if (err instanceof DriveUploadFailed) return err.type;
  const type = apiErrorObject(err)?.type;
  return DRIVE_ERROR_TYPES.find((known) => known === type) ?? null;
}

/**
 * The server's own sentence for a refusal (`x already exists`, `cannot be
 * restored beside ...`), shown where the person acted. English only: the API
 * does not localize it.
 */
export function driveErrorMessage(err: unknown): string | null {
  if (err instanceof DriveUploadFailed) return err.message || null;
  return apiErrorDetailMessage(err);
}

/** Whether a save was refused because the file is no longer the one the editor loaded. */
export function isChangedRefusal(err: unknown): boolean {
  return apiErrorStatus(err) === 412;
}

export async function getSpaces(): Promise<DriveSpace[]> {
  const { data } = await api.get<{ spaces: DriveSpace[] }>(`${BASE}/spaces`);
  return data.spaces;
}

export async function listEntries(space: string, path: string, recursive = false): Promise<DriveEntry[]> {
  const params: Record<string, string | boolean> = { path };
  if (recursive) params.recursive = true;
  const { data } = await api.get<{ entries: DriveEntry[] }>(at(space, 'entries'), { params });
  return data.entries;
}

export async function getEntry(space: string, path: string): Promise<DriveEntry> {
  const { data } = await api.get<DriveEntry>(at(space, 'entry'), { params: { path } });
  return data;
}

/** Where a file's bytes are read: its head, or one version of it. */
export function contentUrl(space: string): string {
  return at(space, 'content');
}

export function contentParams(entryId: string, seq?: number | null): Record<string, string | number> {
  return seq ? { id: entryId, seq } : { id: entryId };
}

/** A text file's content and the hash of the bytes it came from (the save's `If-Match`). */
export async function readText(space: string, entryId: string, seq?: number | null): Promise<{ content: string; sha: string | null }> {
  const res = await api.get<string>(contentUrl(space), {
    params: contentParams(entryId, seq),
    responseType: 'text',
    cache: 'no-store',
  });
  return { content: res.data, sha: etagValue(res.headers.etag) };
}

export async function readBytes(space: string, entryId: string, seq?: number | null): Promise<{ buffer: ArrayBuffer; sha: string | null }> {
  const res = await api.get<ArrayBuffer>(contentUrl(space), {
    params: contentParams(entryId, seq),
    responseType: 'arraybuffer',
    cache: 'no-store',
  });
  return { buffer: res.data, sha: etagValue(res.headers.etag) };
}

/** A file by its path, as an object URL: an image a Markdown file in the Drive embeds. */
export async function objectUrlAt(space: string, path: string): Promise<string> {
  const res = await api.get<Blob>(contentUrl(space), { params: { path }, responseType: 'blob' });
  return URL.createObjectURL(res.data);
}

export function etagValue(header: string | undefined): string | null {
  if (!header) return null;
  return header.replace(/^W\//, '').replace(/"/g, '').trim().toLowerCase() || null;
}

export interface SaveOptions {
  /** The hash of the content the editor loaded; the save is refused (412) if the file moved on. */
  ifMatch?: string | null;
  /** The path must be free (409 `exists` otherwise). */
  ifNoneMatch?: boolean;
  sitting?: string | null;
  note?: string | null;
  contentType?: string;
  signal?: AbortSignal;
  onProgress?: (event: TransferProgress) => void;
  /** The server is busy holding other uploads: the save goes again after `ms`. */
  onBusy?: (ms: number) => void;
}

export async function saveFile(space: string, path: string, body: Blob, opts: SaveOptions = {}): Promise<DriveEntry> {
  const headers: Record<string, string> = {
    // Unlabelled, the server names the type from the file's name.
    'Content-Type': opts.contentType || 'application/octet-stream',
  };
  if (opts.ifMatch) headers['If-Match'] = `"${opts.ifMatch}"`;
  if (opts.ifNoneMatch) headers['If-None-Match'] = '*';
  if (opts.sitting) headers[SITTING_HEADER] = opts.sitting;
  const params: Record<string, string> = { path };
  if (opts.note) params.note = opts.note;
  // A save waiting out a busy server is the account's that started it: after a sign-out it never goes again.
  const current = authSessionCheck();
  for (let attempt = 0; ; attempt++) {
    try {
      const { data } = await api.put<DriveEntry>(at(space, 'content'), body, {
        params,
        headers,
        signal: opts.signal,
        onUploadProgress: opts.onProgress,
      });
      return data;
    } catch (err) {
      const wait = busyRetryDelay(err);
      if (wait === null || attempt >= BUSY_RETRIES) throw err;
      opts.onBusy?.(wait);
      await delay(wait, opts.signal);
      if (!current()) throw new ApiError('canceled', 'ERR_CANCELED');
    }
  }
}

/** Saves sent again after a 503 before the refusal reaches the person. */
const BUSY_RETRIES = 3;
const BUSY_WAIT_MAX_MS = 15_000;

/**
 * How long to wait before sending a save again, for a server that answered
 * 503 while it holds other uploads (`Retry-After`, seconds); null for any
 * other failure, which a retry would not change.
 */
export function busyRetryDelay(err: unknown): number | null {
  if (apiErrorStatus(err) !== 503) return null;
  const seconds = err instanceof ApiError ? err.retryAfter : null;
  return Math.min(BUSY_WAIT_MAX_MS, seconds && seconds > 0 ? seconds * 1000 : 2000);
}

export async function makeFolder(space: string, path: string): Promise<DriveEntry> {
  const { data } = await api.post<DriveEntry>(at(space, 'folders'), { path });
  return data;
}

export async function moveEntry(space: string, from: string, to: string): Promise<DriveEntry> {
  const { data } = await api.post<DriveEntry>(at(space, 'move'), { from, to });
  return data;
}

export async function trashEntry(space: string, path: string): Promise<string> {
  const { data } = await api.post<{ trash_id: string }>(at(space, 'trash'), { path });
  return data.trash_id;
}

/** The newest `limit` trash items (the API pages by limit only, at most 1000). */
export async function listTrash(space: string, limit: number): Promise<DriveTrashPage> {
  const { data } = await api.get<DriveTrashPage>(at(space, 'trash'), { params: { limit } });
  return data;
}

export async function restoreTrash(space: string, trashId: string): Promise<DriveEntry[]> {
  const { data } = await api.post<{ entries: DriveEntry[] }>(at(space, 'trash/restore'), { trash_id: trashId });
  return data.entries;
}

/** Delete one trashed item for good, or with no id empty the trash. */
export async function purgeTrash(space: string, trashId?: string): Promise<number> {
  const { data } = await api.delete<{ purged: number }>(at(space, 'trash'), {
    params: trashId ? { trash_id: trashId } : undefined,
  });
  return data.purged;
}

export async function listVersions(space: string, entryId: string): Promise<DriveVersions> {
  const { data } = await api.get<DriveVersions>(at(space, 'versions'), { params: { id: entryId } });
  return { ...data, too_large_for_history: !!data.too_large_for_history };
}

/** Drops a file's old versions, never the current one; answers how many went and the bytes that freed. */
export async function clearHistory(space: string, entryId: string): Promise<{ dropped: number; freed_bytes: number }> {
  const { data } = await api.post<{ dropped: number; freed_bytes: number }>(at(space, 'versions/clear'), { id: entryId });
  return data;
}

export async function restoreVersion(space: string, entryId: string, seq: number): Promise<DriveEntry> {
  const { data } = await api.post<DriveEntry>(at(space, 'versions/restore'), { id: entryId, seq });
  return data;
}

/**
 * Past `limit` changed entries the answer is `resync`: read everything again.
 * Bounded in time, since a poll whose answer never comes (a server restarting
 * mid-request) would otherwise hold the feed for good.
 */
export async function getChanges(space: string, since: number, limit = 1000): Promise<DriveChanges> {
  const { data } = await api.get<DriveChanges>(at(space, 'changes'), { params: { since, limit }, timeout: 15_000 });
  return data;
}

function clickDownload(href: string, name: string): void {
  const a = document.createElement('a');
  a.href = href;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

/**
 * Save a file (or one version of it) to the person's machine under its own
 * name. A file the store holds downloads from its signed address, so the
 * browser streams it to disk, however large, and the store names it; one the
 * server holds (no object storage, so small) comes through the API, which
 * needs the page's credentials.
 */
export async function downloadFile(space: string, entry: DriveEntry, seq?: number | null): Promise<void> {
  const link = await getContentLink(space, entry.id, seq);
  if (link.signed) {
    clickDownload(link.url, entry.name);
    return;
  }
  const res = await api.get<Blob>(contentUrl(space), {
    params: { ...contentParams(entry.id, seq), download: true },
    responseType: 'blob',
  });
  const url = URL.createObjectURL(res.data);
  clickDownload(url, entry.name);
  // Revoked after the click has handed the URL to the download.
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
