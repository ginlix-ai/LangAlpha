/**
 * The Drive's routes for a file sent in parts and for a file's download
 * address (`/api/v1/drive`, "Large files" in the drive contract).
 */
import { api } from '@/api/client';
import type { SignedUploadPart } from '@/lib/partUpload';
import type { DriveEntry, DriveErrorType } from '@/types/drive';

const BASE = '/api/v1/drive';
const uploads = (space: string, id?: string) =>
  `${BASE}/${encodeURIComponent(space)}/uploads${id ? `/${encodeURIComponent(id)}` : ''}`;

/**
 * The server took the parts, then did not keep the file: `type` says why, as
 * the upload's status named it, or null where it named nothing.
 */
export class DriveUploadFailed extends Error {
  constructor(readonly type: DriveErrorType | null, message: string) {
    super(message);
    this.name = 'DriveUploadFailed';
  }
}

export interface HeldPart {
  part_number: number;
  size: number;
  /** The part's MD5 as the store counted it, hex; null where the store's tag for it is not an MD5. */
  md5: string | null;
}

/** What every answer about an upload carries. */
interface UploadRow {
  mode: 'direct' | 'relay';
  upload_id: string;
  path: string;
  size: number;
  part_size: number;
  part_count: number;
  expires_at: string;
}

/** An upload the server holds open for parts. */
export interface OpenUpload extends UploadRow {
  parts_held: HeldPart[];
}

/** What begin answers: nothing to send, one request, or parts. */
export type DriveUploadBegin =
  | { mode: 'held'; entry: DriveEntry }
  | { mode: 'single'; max_bytes: number }
  | (OpenUpload & { resumed: boolean });

/**
 * Where an upload stands. The parts the store holds come with an open one;
 * the entry with one done, unless the file went before it was read back; the
 * reason with one failed, where the server recorded one.
 */
export type DriveUploadStatus =
  | (OpenUpload & { state: 'uploading' })
  | (UploadRow & { state: 'verifying' })
  | (UploadRow & { state: 'aborted' })
  /** `entry` is null once the file it saved has been deleted. */
  | (UploadRow & { state: 'done'; entry: DriveEntry | null })
  | (UploadRow & { state: 'failed'; error?: { type: DriveErrorType; message: string } });

/**
 * Starts an upload of `size` bytes to `path`. No hash goes with it: reading
 * several GB to hash them in a page is slow, and the server hashes what it
 * stores either way.
 */
export async function beginDriveUpload(
  space: string,
  body: { path: string; size: number; content_type?: string },
): Promise<DriveUploadBegin> {
  const { data } = await api.post<DriveUploadBegin>(uploads(space), body);
  return data;
}

/** Signs parts, each bound to its MD5 (base64) where given: the store refuses a part whose bytes do not match it. */
export async function signDriveUploadParts(
  space: string,
  id: string,
  parts: Array<{ part_number: number; md5?: string }>,
  signal?: AbortSignal,
): Promise<{ parts: SignedUploadPart[]; expires_in: number }> {
  const { data } = await api.post(`${uploads(space, id)}/parts`, { parts }, { signal });
  return data;
}

/** One part through the server, which passes it into the store's part as it arrives, refusing it unless it matches `md5` (base64) where given. */
export async function relayDriveUploadPart(
  space: string,
  id: string,
  n: number,
  body: Blob,
  md5: string | null,
  config: { signal: AbortSignal; onUploadProgress: (e: { loaded: number }) => void },
): Promise<void> {
  await api.put(`${uploads(space, id)}/parts/${n}`, body, {
    ...config,
    headers: { 'Content-Type': 'application/octet-stream', ...(md5 ? { 'Content-MD5': md5 } : {}) },
  });
}

export async function completeDriveUpload(space: string, id: string, signal?: AbortSignal): Promise<DriveUploadStatus> {
  const { data } = await api.post<DriveUploadStatus>(`${uploads(space, id)}/complete`, undefined, { signal });
  return data;
}

export async function getDriveUpload(space: string, id: string, signal?: AbortSignal): Promise<DriveUploadStatus> {
  const { data } = await api.get<DriveUploadStatus>(uploads(space, id), { signal });
  return data;
}

/** How an upload ended: `done` when it was saved before the cancel reached it, so the file stands. */
export type UploadEnd = 'aborted' | 'done' | 'failed';

/** Cancels an upload, answering how it ended: by this cancel, or already before it. */
export async function cancelDriveUpload(space: string, id: string): Promise<UploadEnd> {
  try {
    await api.delete(uploads(space, id));
    return 'aborted';
  } catch (e) {
    const detail = (e as { response?: { status?: number; data?: { detail?: { type?: string; state?: UploadEnd } } } })
      .response;
    if (detail?.status === 409 && detail.data?.detail?.type === 'upload_ended') return detail.data.detail.state ?? 'aborted';
    throw e;
  }
}

export interface DriveContentLink {
  url: string;
  /** A signed address on the store, which needs no credentials; else this API's own download route. */
  signed: boolean;
  expires_in: number;
  size: number;
  sha256: string;
  name: string;
}

export async function getContentLink(space: string, entryId: string, seq?: number | null): Promise<DriveContentLink> {
  const { data } = await api.get<DriveContentLink>(`${BASE}/${encodeURIComponent(space)}/content/link`, {
    params: seq ? { id: entryId, seq } : { id: entryId },
  });
  return data;
}
