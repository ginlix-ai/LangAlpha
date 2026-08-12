/**
 * Memo (user-managed document store) endpoints.
 */
import { api } from '@/api/client';

export type MemoMetadataStatus = 'pending' | 'ready' | 'failed';

export interface MemoEntry {
  key: string;
  original_filename: string | null;
  mime_type: string | null;
  size_bytes: number;
  description: string | null;
  metadata_status: MemoMetadataStatus | null;
  created_at: string | null;
  modified_at: string | null;
  source_kind: string | null;
  source_workspace_id: string | null;
  source_path: string | null;
  sha256: string | null;
}

export interface MemoListResponse {
  entries: MemoEntry[];
  truncated: boolean;
}

export interface MemoReadResponse {
  key: string;
  original_filename: string | null;
  mime_type: string | null;
  content: string;
  encoding: string;
  description: string | null;
  summary: string | null;
  metadata_status: MemoMetadataStatus | null;
  metadata_error: string | null;
  size_bytes: number;
  created_at: string | null;
  modified_at: string | null;
  source_kind: string | null;
  source_workspace_id: string | null;
  source_path: string | null;
}

export interface MemoUploadResponse {
  key: string;
  original_filename: string;
  metadata_status: MemoMetadataStatus;
  replaced?: boolean;
}

export interface MemoUploadSource {
  source_kind: 'sandbox' | 'upload';
  source_workspace_id?: string;
  source_path?: string;
}

export async function listUserMemos(): Promise<MemoListResponse> {
  const { data } = await api.get<MemoListResponse>('/api/v1/memo/user');
  return data;
}

export async function readUserMemo(key: string): Promise<MemoReadResponse> {
  const { data } = await api.get<MemoReadResponse>('/api/v1/memo/user/read', {
    params: { key },
  });
  return data;
}

export async function uploadUserMemo(
  file: File,
  onProgress: ((percent: number) => void) | null = null,
  source?: MemoUploadSource | null,
): Promise<MemoUploadResponse> {
  const formData = new FormData();
  formData.append('file', file);
  if (source?.source_kind) {
    formData.append('source_kind', source.source_kind);
  }
  if (source?.source_workspace_id) {
    formData.append('source_workspace_id', source.source_workspace_id);
  }
  if (source?.source_path) {
    formData.append('source_path', source.source_path);
  }
  const { data } = await api.post<MemoUploadResponse>(
    '/api/v1/memo/user/upload',
    formData,
    {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress
        ? (e) => onProgress(Math.round((e.loaded * 100) / (e.total || 1)))
        : undefined,
    },
  );
  return data;
}

export async function writeUserMemo(
  key: string,
  content: string,
): Promise<MemoUploadResponse> {
  const { data } = await api.put<MemoUploadResponse>('/api/v1/memo/user/write', {
    key,
    content,
  });
  return data;
}

export async function deleteUserMemo(key: string): Promise<void> {
  await api.delete('/api/v1/memo/user', { params: { key } });
}

export async function regenerateUserMemo(
  key: string,
): Promise<MemoUploadResponse> {
  const { data } = await api.post<MemoUploadResponse>(
    '/api/v1/memo/user/regenerate',
    undefined,
    { params: { key } },
  );
  return data;
}

/**
 * Fetch the original memo bytes via axios (bearer-token auth attached) and
 * return a blob URL suitable for `<object data=...>` or an `<a download>`
 * anchor. Callers are responsible for `URL.revokeObjectURL()` when done.
 *
 * The download endpoint requires the Authorization header, so a plain
 * `<a href="/api/v1/memo/user/download?key=...">` won't work — the same
 * reason `FilePanel` uses the blob-URL pattern.
 */
export async function downloadUserMemoBlobUrl(key: string): Promise<string> {
  const response = await api.get('/api/v1/memo/user/download', {
    params: { key },
    responseType: 'blob',
  });
  return URL.createObjectURL(response.data as Blob);
}

/**
 * Trigger a browser download of the original memo file.
 * Uses the same blob + anchor-click pattern as `triggerFileDownload`.
 */
export async function triggerUserMemoDownload(
  key: string,
  filename: string | null = null,
): Promise<void> {
  const blobUrl = await downloadUserMemoBlobUrl(key);
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = filename || key;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(blobUrl);
}

// --- MCP servers (per-workspace + user catalog) ---
