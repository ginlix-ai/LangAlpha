/**
 * Workspace file endpoints (list/read/write/delete/backup/upload).
 */
import { api, ApiError, type RequestConfig } from '@/api/client';
import { isPlatformMode } from '@/config/hostMode';
import { FileChangedError, markStale, parseContentRange, type RangedFile } from '@/lib/rangedFile';
import { withDownloadNotice, workspaceDownloadKey, type DownloadProgressReport } from '../downloadNotice';
import type { FileRefResolution } from '../../components/filePanel/types';
import type { BackupResponse, BackupStatusResponse } from '@/types/api';

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
  });
  return data as FileRefResolution;
}

/**
 * Read a text file from workspace sandbox
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.md"
 */
export async function readWorkspaceFile(workspaceId: string, filePath: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath },
  });
  return data; // { workspace_id, path, content, mime, truncated }
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
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'blob',
    onDownloadProgress: onProgress
      ? (event) => { if (event.total) onProgress(event.loaded / event.total); }
      : undefined,
  });
  return URL.createObjectURL(response.data as Blob);
}

/**
 * Download a file from workspace sandbox as ArrayBuffer (for client-side parsing)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function downloadWorkspaceFileAsArrayBuffer(workspaceId: string, filePath: string) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'arraybuffer',
  });
  return response.data as ArrayBuffer;
}

/** The opening request's size. A file that fits arrives whole, in the one request it always took. */
const FIRST_SLICE_BYTES = 4 * 1024 * 1024;
/** The wait before each retry of a slice that failed in transit or on a server error. */
const SLICE_RETRY_MS = [500, 2000];
/**
 * How long a later slice may go without a byte. pdf.js puts no deadline on a
 * range, so a connection that stalls without closing would hold the page, and
 * its request slot, for good. A bound on silence rather than on the whole
 * answer leaves a slow link, sharing itself among six slices, to finish; this
 * long covers the server's own 10 s read plus a cold worker taking the request.
 */
const SLICE_IDLE_MS = 30_000;

/**
 * The route's 503 for a file that changed while it was read, as one an agent
 * is still writing does. The client hands an error body back in the type the
 * request asked for, so a range's arrives as bytes.
 */
function isFileChangedRefusal(err: unknown): boolean {
  const body = err instanceof ApiError && err.status === 503 ? err.response?.data : undefined;
  if (!(body instanceof ArrayBuffer)) return false;
  try {
    const { detail } = JSON.parse(new TextDecoder().decode(body)) as { detail?: { code?: unknown } };
    return detail?.code === 'file_changed';
  } catch {
    return false;
  }
}

async function withRetry<T>(request: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await request();
    } catch (err) {
      // A 4xx, an abort and a file that changed under the read are answers;
      // only a dropped request or another 5xx is worth asking again.
      const transient = err instanceof ApiError && err.name !== 'CanceledError'
        && (err.status === undefined || err.status >= 500) && !isFileChangedRefusal(err);
      if (!transient || attempt >= SLICE_RETRY_MS.length || signal?.aborted) throw err;
      await new Promise((resolve) => setTimeout(resolve, SLICE_RETRY_MS[attempt]));
    }
  }
}

/**
 * Open a file from its first slice, so a large one is usable before the rest
 * arrives. A file that fits in the slice, or a server that answers with the
 * whole body, comes back as bytes, exactly as a whole download would.
 */
export async function openWorkspaceFileRanged(
  workspaceId: string,
  filePath: string,
): Promise<ArrayBuffer | RangedFile> {
  const slice = (first: number, last: number, config: RequestConfig = {}) =>
    api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
      ...config,
      params: { path: filePath },
      headers: { Range: `bytes=${first}-${last}` },
      responseType: 'arraybuffer',
      // A browser holding the whole file from a plain download would make
      // this request conditional, and the server answers a conditional range
      // with the whole file.
      cache: 'no-store',
    });
  const whole = () => downloadWorkspaceFileAsArrayBuffer(workspaceId, filePath);

  let head: Awaited<ReturnType<typeof slice>>;
  try {
    // No deadline, as the whole download this replaces had none, and a 200
    // with the whole file is an answer it takes.
    head = await withRetry(() => slice(0, FIRST_SLICE_BYTES - 1));
  } catch (err) {
    // An empty file has no first byte for a range to start at, and one that
    // changes as it is read has no version for slices to keep to: a plain
    // download takes either as it stands.
    if ((err instanceof ApiError && err.status === 416) || isFileChangedRefusal(err)) return whole();
    throw err;
  }
  if (head.status !== 206) return head.data as ArrayBuffer;
  const bytes = head.data as ArrayBuffer;
  const opened = parseContentRange(head.headers['content-range']);
  // Answered by range but not described (a header the deploy does not
  // expose), or described as other bytes than arrived, so there is no length
  // to read on from: read it again whole.
  if (!opened || opened.first !== 0 || bytes.byteLength !== opened.last + 1) return whole();
  if (opened.last + 1 === opened.total) return bytes;
  // With no version to hold the slices to, a change between two would go unseen.
  const etag = head.headers.etag;
  if (!etag) return whole();

  const length = opened.total;
  const readSpan = async (begin: number, end: number, signal?: AbortSignal) => {
    const out = new Uint8Array(end - begin);
    // A slice answered whole cannot be held to the version that opened, so its
    // body, the entire file, is never read: it comes back empty and the check
    // below has the file opened again.
    const config: RequestConfig = { signal, idleTimeout: SLICE_IDLE_MS, expectStatus: 206 };
    // The server caps one answer, so a short one is asked again from where it stopped.
    for (let at = begin; at < end;) {
      const part = await withRetry(() => slice(at, end - 1, config), signal).catch((err: unknown) => {
        // The offset lay inside the version that opened, so the file has since
        // shrunk or gone, and reading it again says which. The route's own
        // refusal says the version moved while it read.
        const changed = (err instanceof ApiError && (err.status === 416 || err.status === 404)) || isFileChangedRefusal(err);
        throw changed ? new FileChangedError() : err;
      });
      const got = part.status === 206 ? parseContentRange(part.headers['content-range']) : null;
      if (!got || got.first !== at || got.total !== length || part.headers.etag !== etag) {
        throw new FileChangedError();
      }
      const body = part.data as ArrayBuffer;
      if (body.byteLength !== got.last - got.first + 1) throw new Error('A file slice arrived incomplete');
      const take = Math.min(body.byteLength, end - at);
      out.set(new Uint8Array(body, 0, take), at - begin);
      at += take;
    }
    return out;
  };
  const file: RangedFile = {
    length,
    etag,
    initial: new Uint8Array(bytes),
    read: (begin, end, signal) => readSpan(begin, end, signal).catch((err: unknown) => {
      if (err instanceof FileChangedError) markStale(file);
      throw err;
    }),
  };
  return file;
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
  const { data } = await api.get<{ url: string | null }>(
    `/api/v1/workspaces/${workspaceId}/files/download-url`,
    { params: { path: filePath } },
  );
  // Without a bearer token the download route itself is a plain GET, so the
  // browser can stream it to disk too; with one, only a blob can carry it.
  const direct = data?.url ?? (isPlatformMode ? null : directDownloadUrl(workspaceId, filePath));
  const href = direct ?? (await downloadWorkspaceFile(workspaceId, filePath, report));
  // Signed out while this was being prepared: the file is the last account's.
  if (!current()) {
    if (!direct) URL.revokeObjectURL(href);
    return;
  }
  const a = document.createElement('a');
  a.href = href;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  if (!direct) URL.revokeObjectURL(href);
}

function directDownloadUrl(workspaceId: string, filePath: string): string {
  return api.getUri({
    url: `/api/v1/workspaces/${workspaceId}/files/download`,
    params: { path: filePath, attachment: true },
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
 * Write full file content to a sandbox file
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.py"
 * @param {string} content - File content to write
 * @returns {Promise<Object>} { workspace_id, path, size }
 */
export async function writeWorkspaceFile(workspaceId: string, filePath: string, content: string) {
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}/files/write`,
    { content },
    { params: { path: filePath } }
  );
  return data;
}

/**
 * Read a file without line-limit pagination (for edit mode)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<Object>} { workspace_id, path, content, mime }
 */
export async function readWorkspaceFileFull(workspaceId: string, filePath: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath, unlimited: true },
  });
  return data;
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

export interface SignedUploadPart {
  part_number: number;
  offset: number;
  size: number;
  url: string;
}

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
