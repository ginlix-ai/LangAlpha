/**
 * Workspace file endpoints (list/read/write/delete/backup/upload).
 */
import { api } from '@/api/client';
import { isPlatformMode } from '@/config/hostMode';
import type { RangedFile } from '@/lib/rangedFile';
import { openRangedRead } from '@/lib/rangedRead';
import { withDownloadNotice, workspaceDownloadKey, type DownloadProgressReport } from '../downloadNotice';
import type { FileRefResolution } from '../../components/filePanel/types';
import type { BackupResponse, BackupStatusResponse } from '@/types/api';
import type { SignedUploadPart } from '@/lib/partUpload';
import { BACKUP_OK, LIVE, readFromBody, type ReadFrom } from './readFrom';

/** Bytes carry no field for it, so the download says by header, and only for a computer that did not answer. */
function readFromHeaders(headers: Record<string, string> | undefined): ReadFrom {
  const code = headers?.['x-computer-unreachable'];
  return typeof code === 'string' && code ? { kind: 'backup', unreachable: code } : LIVE;
}

export interface TextRead {
  content: string;
  mime?: string;
  truncated?: boolean;
  from: ReadFrom;
  /** The token of the bytes read, where the source names one (the Drive's sha256). */
  version?: string | null;
}

/**
 * A whole file's text, for an edit. `version` names the bytes it came from,
 * null where it names none; `rewriting` says a live read caught the file as
 * it was rewritten, so it has no version a save could be held to. A server
 * older than versions names none and is never `rewriting`.
 */
export interface FullRead {
  content: string;
  version: string | null;
  rewriting: boolean;
  from: ReadFrom;
}

export interface BytesRead {
  buffer: ArrayBuffer;
  from: ReadFrom;
}

/** A file opened from its first slice: whole bytes when it fit, else read on by range. */
export type OpenedRead = BytesRead | { ranged: RangedFile; from: ReadFrom };

/**
 * Resolve a file reference to one workspace path in a single server lookup.
 * @param candidates - readings of the reference, most likely first
 * @param recentWrites - this thread's Write/Edit paths, newest first
 */
export async function resolveWorkspaceFile(
  workspaceId: string,
  candidates: string[],
  recentWrites: string[] = [],
): Promise<FileRefResolution> {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/files/resolve`, {
    candidates,
    recent_writes: recentWrites,
  }, { params: BACKUP_OK });
  return data as FileRefResolution;
}

/**
 * Read a text file from workspace sandbox
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.md"
 */
export async function readWorkspaceFile(workspaceId: string, filePath: string): Promise<TextRead> {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath, ...BACKUP_OK },
  });
  return { content: data?.content ?? '', mime: data?.mime, truncated: data?.truncated, from: readFromBody(data) };
}

/**
 * Download a file from workspace sandbox (returns blob URL)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<string>} Blob URL for the file
 */
export async function downloadWorkspaceFile(
  workspaceId: string,
  filePath: string,
  onProgress?: DownloadProgressReport,
) {
  return (await downloadWorkspaceBlob(workspaceId, filePath, onProgress)).href;
}

async function downloadWorkspaceBlob(
  workspaceId: string,
  filePath: string,
  onProgress?: DownloadProgressReport,
): Promise<{ href: string; from: ReadFrom }> {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath, ...BACKUP_OK },
    responseType: 'blob',
    onDownloadProgress: onProgress
      ? (event) => { if (event.total) onProgress(event.loaded / event.total); }
      : undefined,
  });
  return { href: URL.createObjectURL(response.data as Blob), from: readFromHeaders(response.headers) };
}

/**
 * Download a file from workspace sandbox as ArrayBuffer (for client-side parsing)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function downloadWorkspaceFileAsArrayBuffer(workspaceId: string, filePath: string): Promise<BytesRead> {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath, ...BACKUP_OK },
    responseType: 'arraybuffer',
  });
  return { buffer: response.data as ArrayBuffer, from: readFromHeaders(response.headers) };
}

/**
 * Open a file from its first slice, so a large one is usable before the rest
 * arrives. A file that fits in the slice, or a server that answers with the
 * whole body, comes back as bytes, exactly as a whole download would.
 */
export async function openWorkspaceFileRanged(workspaceId: string, filePath: string): Promise<OpenedRead> {
  const opened = await openRangedRead(`/api/v1/workspaces/${workspaceId}/files/download`, { path: filePath, ...BACKUP_OK });
  if (opened.kind === 'whole') return downloadWorkspaceFileAsArrayBuffer(workspaceId, filePath);
  const from = readFromHeaders(opened.headers);
  return opened.kind === 'bytes' ? { buffer: opened.buffer, from } : { ranged: opened.file, from };
}

/**
 * Trigger file download in browser
 * @param {string} workspaceId
 * @param {string} filePath
 */
export function triggerFileDownload(workspaceId: string, filePath: string): Promise<void> {
  const fileName = filePath.split('/').pop() || 'download';
  return withDownloadNotice(workspaceDownloadKey(workspaceId, filePath), fileName, (report, current) =>
    saveWorkspaceFile(workspaceId, filePath, fileName, report, current),
  );
}

async function saveWorkspaceFile(
  workspaceId: string,
  filePath: string,
  fileName: string,
  report: DownloadProgressReport,
  current: () => boolean,
) {
  // A large file is handed out as a short-lived storage link the browser
  // streams to disk itself; buffering it as a blob would hold the whole file
  // in tab memory. A null url means the file is small enough to fetch here.
  const { data } = await api.get<{ url: string | null; source?: string; unreachable?: string }>(
    `/api/v1/workspaces/${workspaceId}/files/download-url`,
    { params: { path: filePath, ...BACKUP_OK } },
  );
  // Without a bearer token the download route itself is a plain GET, so the
  // browser can stream it to disk too; with one, only a blob can carry it.
  // A download the browser makes never shows its header here, so where it
  // comes from is read off this answer; a blob says by its own header.
  const direct = data?.url ?? (isPlatformMode ? null : directDownloadUrl(workspaceId, filePath));
  const blob = direct ? null : await downloadWorkspaceBlob(workspaceId, filePath, report);
  const href = direct ?? blob!.href;
  const from = blob?.from ?? readFromBody(data);
  // Signed out while this was being prepared: the file is the last account's.
  if (!current()) {
    if (!direct) URL.revokeObjectURL(href);
    return LIVE;
  }
  const a = document.createElement('a');
  a.href = href;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  if (!direct) URL.revokeObjectURL(href);
  return from;
}

function directDownloadUrl(workspaceId: string, filePath: string): string {
  return api.getUri({
    url: `/api/v1/workspaces/${workspaceId}/files/download`,
    params: { path: filePath, attachment: true, ...BACKUP_OK },
  });
}

/** Back up the sandbox's files so they outlive it. */
export async function backupWorkspaceFiles(workspaceId: string): Promise<BackupResponse> {
  const { data } = await api.post<BackupResponse>(`/api/v1/workspaces/${workspaceId}/files/backup`);
  return data;
}

/** Which sandbox files match their backup, differ from it, or have none. */
export async function getBackupStatus(workspaceId: string): Promise<BackupStatusResponse> {
  const { data } = await api.get<BackupStatusResponse>(`/api/v1/workspaces/${workspaceId}/files/backup-status`);
  return data;
}

/**
 * Write full file content to a sandbox file. Given `baseVersion` (the
 * `version` the edit's read returned), the server refuses with 409
 * `file_changed` when the file no longer holds the text the edit began from.
 * `baseSha256` (`textSha256` of that text) lets the save through a version
 * that moved while the text did not. Answers with the version the file holds
 * now, null where the server could not read one.
 */
export async function writeWorkspaceFile(
  workspaceId: string,
  filePath: string,
  content: string,
  baseVersion: string | null = null,
  baseSha256: string | null = null,
): Promise<{ version: string | null }> {
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}/files/write`,
    {
      content,
      ...(baseVersion ? { base_version: baseVersion } : {}),
      ...(baseSha256 ? { base_sha256: baseSha256 } : {}),
    },
    { params: { path: filePath } }
  );
  const version = (data as { version?: unknown } | null)?.version;
  return { version: typeof version === 'string' ? version : null };
}

/** A file's whole text, without the paged read's line limit, for an edit. */
export async function readWorkspaceFileFull(workspaceId: string, filePath: string): Promise<FullRead> {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath, unlimited: true, ...BACKUP_OK },
  });
  const version = typeof data?.version === 'string' && data.version ? data.version : null;
  return {
    content: data?.content ?? '',
    version,
    rewriting: !!data && 'version' in data && !version,
    from: readFromBody(data),
  };
}

export async function deleteWorkspaceFiles(workspaceId: string, paths: string[]) {
  const { data } = await api.delete(`/api/v1/workspaces/${workspaceId}/files`, {
    data: { paths },
  });
  return data;
}

// --- Sandbox ---

/** Sends the file in one request through the backend; `beginWorkspaceUpload` says when this is the path. */
export async function uploadWorkspaceFile(
  workspaceId: string,
  file: File,
  destPath: string | null = null,
  onProgress: ((percent: number) => void) | null = null,
  signal?: AbortSignal,
) {
  const formData = new FormData();
  formData.append('file', file);
  const params = destPath ? { path: destPath } : {};
  const { data } = await api.post(
    `/api/v1/workspaces/${workspaceId}/files/upload`,
    formData,
    {
      params,
      signal,
      onUploadProgress: onProgress
        ? (e) => onProgress(Math.round((e.loaded * 100) / (e.total || 1)))
        : undefined,
    }
  );
  return data;
}

export type UploadBegin =
  | { mode: 'relay' }
  | {
    mode: 'direct';
    upload_id: string;
    /** Where the file will land, relative to the workspace folder. */
    path: string;
    part_size: number;
    part_count: number;
    expires_at: string;
  };

export interface UploadStatus {
  upload_id: string;
  state: 'uploading' | 'completing' | 'pulling' | 'done' | 'failed' | 'aborted';
  path: string;
  size: number;
  error: string | null;
}

const uploadsPath = (workspaceId: string, uploadId?: string) =>
  `/api/v1/workspaces/${workspaceId}/files/uploads${uploadId ? `/${uploadId}` : ''}`;

export async function beginWorkspaceUpload(
  workspaceId: string,
  body: { path: string; size: number; sha256?: string },
  signal?: AbortSignal,
): Promise<UploadBegin> {
  const { data } = await api.post(uploadsPath(workspaceId), body, { signal });
  return data as UploadBegin;
}

export async function signWorkspaceUploadParts(
  workspaceId: string,
  uploadId: string,
  partNumbers: number[],
  signal?: AbortSignal,
): Promise<{ parts: SignedUploadPart[]; expires_in: number }> {
  const { data } = await api.post(`${uploadsPath(workspaceId, uploadId)}/parts`, { part_numbers: partNumbers }, { signal });
  return data;
}

export async function completeWorkspaceUpload(
  workspaceId: string,
  uploadId: string,
  signal?: AbortSignal,
): Promise<UploadStatus> {
  const { data } = await api.post(`${uploadsPath(workspaceId, uploadId)}/complete`, undefined, { signal });
  return data as UploadStatus;
}

export async function getWorkspaceUpload(
  workspaceId: string,
  uploadId: string,
  signal?: AbortSignal,
): Promise<UploadStatus> {
  const { data } = await api.get(uploadsPath(workspaceId, uploadId), { signal });
  return data as UploadStatus;
}

export async function cancelWorkspaceUpload(workspaceId: string, uploadId: string): Promise<void> {
  await api.delete(uploadsPath(workspaceId, uploadId));
}

/**
 * Ends an upload as the page goes away. A keepalive request outlives the page
 * that sent it, where the API client's would be cut off with the page; the
 * caller hands over the auth headers, since there is no time left to fetch them.
 */
export function cancelWorkspaceUploadOnUnload(
  workspaceId: string,
  uploadId: string,
  headers: Record<string, string>,
): void {
  try {
    void fetch(api.getUri({ url: uploadsPath(workspaceId, uploadId) }), {
      method: 'DELETE',
      keepalive: true,
      headers,
    }).catch(() => {});
  } catch {
    /* best effort: the row still expires */
  }
}

// --- Vault Secrets ---
